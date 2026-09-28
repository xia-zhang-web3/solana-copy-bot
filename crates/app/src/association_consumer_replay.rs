//! SQLite restore completes before the scoped recovery task opens its provider connection.
use crate::execution_technical_cohort;
use anyhow::{Context, Result};
use copybot_config::{ExecutionConfig, IngestionConfig};
use copybot_core_types::association_recovery::ReplayScope;
use copybot_ingestion::{DeliveryReceiver, IngestionService};
use copybot_storage_core::association_inbox::{AssociationInbox, InboxLimits};
pub(super) async fn open_and_start(
    ingestion: &mut IngestionService,
    c: &IngestionConfig,
    execution: Option<&ExecutionConfig>,
    path: String,
    limits: InboxLimits,
) -> Result<(
    AssociationInbox,
    DeliveryReceiver,
    bool,
    Option<chrono::DateTime<chrono::Utc>>,
    Option<ReplayScope>,
)> {
    let authority = execution
        .map(execution_technical_cohort::authority)
        .transpose()?
        .flatten();
    let deadline = authority.as_ref().map(|a| a.deadline);
    let mut admission_wallets = authority
        .as_ref()
        .map(|a| {
            execution
                .context("cohort execution config")
                .and_then(|c| execution_technical_cohort::admission_wallets(a, c))
        })
        .transpose()?;
    let mut bot_signer = authority
        .as_ref()
        .and_then(|_| execution.map(|c| c.canary_wallet_pubkey.clone()));
    if !c.yellowstone_replay_wallets.is_empty() {
        anyhow::ensure!(
            authority.is_none(),
            "observation_replay_with_financial_authority"
        );
        admission_wallets = Some(c.yellowstone_replay_wallets.iter().cloned().collect());
        bot_signer = execution
            .map(|e| &e.canary_wallet_pubkey)
            .filter(|bot| c.yellowstone_replay_wallets.contains(bot))
            .cloned();
    }
    let replay_scope = admission_wallets
        .as_ref()
        .map(|wallets| copybot_ingestion::replay_scope(c, wallets))
        .transpose()?;
    let setup_scope = replay_scope.clone();
    let (inbox, checkpoint) = tokio::task::spawn_blocking(move || {
        let mut inbox = AssociationInbox::open_ordered_sell_consumer(path, limits)?;
        if let Some(authority) = authority.as_ref() {
            inbox.register_technical_cohort_authority(authority)?;
        }
        let checkpoint = if let Some(scope) = setup_scope.as_ref() {
            inbox.configure_replay_scope(scope)?;
            inbox.replay_checkpoint(scope)?
        } else {
            None
        };
        Ok::<_, anyhow::Error>((inbox, checkpoint))
    })
    .await??;
    let recovery_pending = inbox.has_sell_preparation_work()?;
    let receiver = match admission_wallets {
        Some(wallets) => ingestion.take_delivery_recovering_labeled(
            AssociationInbox::new_session_id(),
            wallets,
            bot_signer,
            checkpoint,
        )?,
        None => ingestion.take_delivery_scoped_labeled(
            AssociationInbox::new_session_id(),
            None,
            bot_signer,
        )?,
    }
    .context("missing delivery receiver")?;
    Ok((inbox, receiver, recovery_pending, deadline, replay_scope))
}
