//! SOURCE's actual tick must retain its BOT_READY session across quote/build stages.
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use copybot_core_types::association_delivery::{
    CandidateGeneration, Delivery, DeliveryEvent, SessionGap,
};
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    native_buy::{NativeBuyFence, TechnicalCohortAuthority, CLASSIC_SPL_MINT_POLICY},
    SqliteStore,
};

#[tokio::test]
#[ignore = "explicit isolated LOCAL fixture, localhost port1 only; no signer/provider"]
async fn source_entry_pinned_session_reset_refuses_actual_source_tick_without_rebinding(
) -> Result<()> {
    for new_fence in [false, true] {
        let input = super::inputs()?;
        let mut c: copybot_config::AppConfig =
            toml::from_str(&std::fs::read_to_string(input.join("LOCAL_SOURCE.toml"))?)?;
        let now = Utc::now();
        let source_path = input.join(format!(
            "source-entry-{}-{new_fence}.sqlite",
            std::process::id()
        ));
        let bot_path = input.join(format!(
            "bot-entry-{}-{new_fence}.sqlite",
            std::process::id()
        ));
        anyhow::ensure!(
            !source_path.exists() && !bot_path.exists(),
            "fresh guard stores"
        );
        let migrations =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut source = SqliteStore::open(&source_path)?;
        source.run_migrations(&migrations)?;
        let mut bot = SqliteStore::open(&bot_path)?;
        bot.run_migrations(&migrations)?;
        let owner = c
            .execution
            .owner_technical_buy
            .as_mut()
            .context("SOURCE owner")?;
        owner.activated_at = (now - Duration::seconds(5)).to_rfc3339();
        owner.expires_at = (now + Duration::seconds(595)).to_rfc3339();
        let gate = owner
            .source_copy_exit_gate
            .as_mut()
            .context("SOURCE exit gate")?;
        gate.bot_state_db_path = bot_path.to_string_lossy().into_owned();
        gate.bot_kill_switch_path = input
            .join("ABSENT-LOCAL-BOT-STOP")
            .to_string_lossy()
            .into_owned();
        gate.expected_native_session = Some("entry-session-0".into());
        let authority = TechnicalCohortAuthority {
            run_id: gate.bot_run_id.clone(),
            wallet_ids: vec![owner.wallet_pubkey.clone()],
            mint_policy: CLASSIC_SPL_MINT_POLICY.into(),
            activated_at: now - Duration::seconds(5),
            deadline: now + Duration::seconds(595),
            max_buy_count: 1,
            policy_identity: gate.bot_policy_identity.clone(),
        };
        let mut inbox = AssociationInbox::open(
            &bot_path,
            InboxLimits {
                count: 1024,
                bytes: 16 << 20,
                busy_ms: 5000,
            },
        )?;
        inbox.register_technical_cohort_authority(&authority)?;
        let mut fence = NativeBuyFence {
            session: "entry-session-0".into(),
            processed_slot: 465326211,
            sampled_at: now - Duration::seconds(1),
            genesis_hash: owner.genesis_hash.clone(),
            policy_identity: authority.policy_identity.clone(),
        };
        inbox.record_native_buy_fence_epoch(&fence)?;
        c.execution.canary_kill_switch_path = input
            .join("ABSENT-LOCAL-SOURCE-STOP")
            .to_string_lossy()
            .into_owned();
        c.execution.submit_adapter_http_url = "http://127.0.0.1:1".into();
        c.execution
            .controlled_receipt_lot
            .as_mut()
            .unwrap()
            .source_state_db_path = source_path.to_string_lossy().into_owned();
        crate::execution_controlled_receipt_lot::register(&c.execution, &source)?;
        let intent = crate::execution_owner_buy_authority::configured(&c.execution)?
            .context("source identity")?;
        source.register_owner_technical_buy_intent(&intent)?;
        crate::execution_owner_buy_authority::current(&c.execution, &source, &intent, now)?;
        // Persist the actual Reset transition. The SOURCE config/intent remains pinned0.
        inbox.persist_at(
            &Delivery {
                session: "entry-session-0".into(),
                sequence: 27,
                arrival_offset_ns: 8989685837,
                event: DeliveryEvent::Session(SessionGap::Reset),
            },
            &CandidateGeneration::Unknown,
            now,
        )?;
        if new_fence {
            fence.session = "entry-session-1".into();
            fence.processed_slot = 465326252;
            fence.sampled_at = now;
            inbox.record_native_buy_fence_epoch(&fence)?;
        }
        let revoked =
            crate::execution_owner_buy_authority::current(&c.execution, &source, &intent, now)
                .expect_err("SOURCE applicability must be revoked after Reset");
        assert_eq!(revoked.to_string(), "source_copy_entry_session_changed");
        let result = crate::execution_owner_buy::tick(&c.execution, &source, now).await?;
        assert_eq!(
            result.last_error.as_deref(),
            Some("source_copy_entry_session_changed")
        );
        let sql = rusqlite::Connection::open(&source_path)?;
        for table in [
            "orders",
            "fills",
            "execution_canary_dispatch",
            "execution_canary_receipt_facts",
        ] {
            assert_eq!(
                super::count(&sql, table)?,
                0,
                "no SOURCE BUY before applicable ready proof: {table}"
            );
        }
        // Even an available new fence cannot silently renew the old SOURCE intent.
        c.execution
            .owner_technical_buy
            .as_mut()
            .unwrap()
            .source_copy_exit_gate
            .as_mut()
            .unwrap()
            .expected_native_session = Some("entry-session-1".into());
        assert!(
            crate::execution_owner_buy_authority::current(&c.execution, &source, &intent, now)
                .is_err()
        );
    }
    Ok(())
}
