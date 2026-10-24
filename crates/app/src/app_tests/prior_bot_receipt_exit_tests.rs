//! LOCAL phase-gate refusals; no old package, financial authority or provider.
use super::{controlled_copy_pipeline_fixture as frames, owner_source_gate_fixture::GateCase};
use anyhow::Result;
use copybot_config::{PriorBotReceiptExitConfig, PRIOR_BOT_RECEIPT_EXIT_V1};
use copybot_storage_core::SqliteStore;
use std::path::Path;

fn prerequisite(path: &Path) -> PriorBotReceiptExitConfig {
    let buy = "exec-canary:native-buy-v1:LOCAL-source-buy".to_string();
    PriorBotReceiptExitConfig {
        policy: PRIOR_BOT_RECEIPT_EXIT_V1.into(),
        state_db_path: path.to_string_lossy().into(),
        exit_intent_id: "bot-receipt-exit:LOCAL-source-buy".into(),
        position_id: format!("exec-canary-pos:{buy}"),
        buy_order_id: buy,
        buy_receipt_signature: bs58::encode([7; 64]).into_string(),
        wallet_pubkey: bs58::encode([19; 32]).into_string(),
        mint: frames::USDC.into(),
        amount_raw: 988_620,
        decimals: 6,
    }
}

#[tokio::test]
async fn prior_bot_exit_empty_snapshot_holds_real_source_buy_runner_without_order_or_send(
) -> Result<()> {
    let mut c = super::owner_roundtrip_fixture::Case::new().await?;
    let previous = c.root.path().join("LOCAL-phase-A.db");
    let mut a = SqliteStore::open(&previous)?;
    a.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    c.config.prior_bot_receipt_exit = Some(prerequisite(&previous));
    let summary = c.tick().await?;
    assert_eq!(
        summary.state_machine_skipped_reason,
        Some("prior_bot_receipt_exit_pending")
    );
    assert_eq!(c.calls("quote"), 0);
    assert_eq!(c.calls("sendTransaction"), 0);
    assert_eq!(
        c.sql()?
            .query_row("SELECT count(*) FROM orders", [], |r| r.get::<_, u64>(0))?,
        0
    );
    assert!(!crate::execution_canary_safety::prior_bot_receipt_exit::ready(&c.config)?);
    let again = c.tick().await?;
    assert_eq!(
        again.state_machine_skipped_reason,
        Some("prior_bot_receipt_exit_pending")
    );
    assert_eq!(c.calls("sendTransaction"), 0);
    Ok(())
}

#[tokio::test]
async fn prior_bot_exit_missing_schema_bad_binding_and_absent_file_never_release_buy() -> Result<()>
{
    let mut c = super::owner_roundtrip_fixture::Case::new().await?;
    let absent = c.root.path().join("LOCAL-absent-A.db");
    c.config.prior_bot_receipt_exit = Some(prerequisite(&absent));
    assert!(crate::execution_canary_safety::prior_bot_receipt_exit::ready(&c.config).is_err());
    assert!(
        !absent.exists(),
        "read-only gate must not create the old DB"
    );
    std::fs::write(&absent, [])?;
    assert!(crate::execution_canary_safety::prior_bot_receipt_exit::ready(&c.config).is_err());
    for change in 0..5 {
        let mut config = c.config.clone();
        let p = config.prior_bot_receipt_exit.as_mut().unwrap();
        match change {
            0 => p.policy.push('x'),
            1 => p.buy_order_id = "exec-canary:owner-buy:LOCAL".into(),
            2 => p.amount_raw = 0,
            3 => p.decimals = 9,
            _ => p.state_db_path = "relative.db".into(),
        }
        assert!(copybot_config::validate_prior_bot_receipt_exit(&config).is_err());
    }
    assert_eq!(c.calls("sendTransaction"), 0);
    Ok(())
}

#[tokio::test]
async fn prior_bot_exit_binding_changes_both_owner_and_native_policy_identity() -> Result<()> {
    let c = GateCase::new().await?;
    let previous = c.source.root.path().join("LOCAL-phase-A.db");
    let mut owner = c.source.config.clone();
    let before = crate::execution_owner_buy_authority::parent_identity(&owner)?;
    owner.prior_bot_receipt_exit = Some(prerequisite(&previous));
    let bound = crate::execution_owner_buy_authority::parent_identity(&owner)?;
    assert_ne!(before.authority_sha256, bound.authority_sha256);
    owner.prior_bot_receipt_exit.as_mut().unwrap().amount_raw += 1;
    assert_ne!(
        bound.authority_sha256,
        crate::execution_owner_buy_authority::parent_identity(&owner)?.authority_sha256
    );
    owner.prior_bot_receipt_exit = None;
    assert_eq!(
        before,
        crate::execution_owner_buy_authority::parent_identity(&owner)?
    );

    let mut native = frames::config(&c)?.execution;
    let before = crate::execution_native_buy_rpc::policy_identity(&native)?;
    native.prior_bot_receipt_exit = Some(prerequisite(&previous));
    let bound = crate::execution_native_buy_rpc::policy_identity(&native)?;
    assert_ne!(before, bound);
    native
        .prior_bot_receipt_exit
        .as_mut()
        .unwrap()
        .buy_receipt_signature = bs58::encode([8; 64]).into_string();
    assert_ne!(
        bound,
        crate::execution_native_buy_rpc::policy_identity(&native)?
    );
    native.prior_bot_receipt_exit = None;
    assert_eq!(
        before,
        crate::execution_native_buy_rpc::policy_identity(&native)?
    );
    Ok(())
}
