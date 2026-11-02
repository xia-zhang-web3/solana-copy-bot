//! Explicit LOCAL registration and cycle scope; no modeled BOT sends or decisions.
#[path = "execution_local_source_receipt.rs"]
pub(crate) mod receipt;
#[path = "execution_local_namespace.rs"]
pub(crate) mod namespace;
use anyhow::{ensure, Result};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    controlled_receipt_lot::Scope, local_bounded_session::Authority, SqliteStore,
};
pub(crate) fn authority(c: &ExecutionConfig) -> Result<Option<Authority>> {
    copybot_config::validate_local_bounded_session(c)?;
    let Some(model) = &c.local_bounded_session else {
        return Ok(None);
    };
    ensure!(
        cfg!(target_os = "linux"),
        "local_session_linux_namespace_required"
    );
    namespace::validate()?;
    let p = c
        .controlled_receipt_lot
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("local_session_scope"))?;
    let scope = Scope {
        policy: p.policy.clone(),
        source_run_id: p.source_run_id.clone(),
        bot_run_id: p.bot_run_id.clone(),
        source_wallet: p.source_wallet.clone(),
        bot_wallet: p.bot_wallet.clone(),
        source_buy_intent_id: p.source_buy_intent_id.clone(),
        source_state_db_path: p.source_state_db_path.clone(),
    };
    Ok(Some(Authority {
        model: serde_json::from_value(serde_json::to_value(model)?)?,
        cohort: crate::execution_technical_cohort::authority(c)?
            .ok_or_else(|| anyhow::anyhow!("local_session_cohort"))?,
        scope,
        genesis: copybot_config::LOCAL_SESSION_GENESIS.into(),
    }))
}
pub(crate) fn register(c: &ExecutionConfig, s: &SqliteStore) -> Result<()> {
    if let Some(a) = authority(c)? {
        s.register_local_bounded_session(&a)?;
    } else {
        ensure!(
            s.local_bounded_scope()?.is_none(),
            "local_session_profile_required"
        );
    }
    Ok(())
}
pub(crate) fn scope(c: &ExecutionConfig) -> Result<Option<Scope>> {
    if c.local_bounded_session.is_none() {
        return Ok(None);
    }
    copybot_config::validate_local_bounded_session(c)?;
    let p = c
        .controlled_receipt_lot
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("local_session_scope"))?;
    let db = SqliteStore::open_read_only(&p.source_state_db_path)?;
    db.local_bounded_scope()
}
pub(crate) fn source_buy(
    c: &ExecutionConfig,
    signature: &str,
    wallet: &str,
    mint: &str,
) -> Result<bool> {
    if c.local_bounded_session.is_none() {
        return Ok(false);
    }
    let scope = scope(c)?.ok_or_else(|| anyhow::anyhow!("local_session_missing"))?;
    let model = c.local_bounded_session.as_ref().unwrap();
    let p = model
        .cycles
        .iter()
        .find(|p| p.cycle_id == scope.source_buy_intent_id)
        .ok_or_else(|| anyhow::anyhow!("local_session_cycle"))?;
    ensure!(
        p.buy_signature == signature
            && wallet == scope.source_wallet
            && mint == copybot_storage_core::OWNER_EXIT_MINT,
        "local_session_source_buy"
    );
    Ok(true)
}
