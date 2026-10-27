//! New route tests only. Existing receipt/fee/dispatch/restart proofs remain reused.
use anyhow::Result;
use chrono::{Duration, Utc};
use copybot_config::{ExecutionConfig, ExecutionOnlyMode, OwnerExitConfig};
use std::sync::{atomic::AtomicBool, Arc};

fn receipt_only(mut c: ExecutionConfig) -> ExecutionConfig {
    c.enabled = false;
    c.canary_tiny_submit_enabled = true;
    c.canary_entry_submit_enabled = false;
    c.tiny_experiment.activate = false;
    c.technical_cohort.as_mut().unwrap().activate = false;
    c.owner_technical_buy = None;
    c.native_fresh_buy = None;
    c.prior_bot_receipt_exit = None;
    let now = Utc::now();
    let run = c.tiny_experiment.id.clone().unwrap();
    let wallet = c.canary_wallet_pubkey.clone();
    c.owner_exit = Some(OwnerExitConfig {
        policy: copybot_config::OWNER_BOT_RECEIPT_EXIT_V1.into(),
        activate: false,
        source_recovery_parent_intent_id: None,
        run_id: run,
        intent_id: "bot-receipt-exit:SYNTHETIC-SOURCE".into(),
        buy_order_id: "exec-canary:native-buy-v1:SYNTHETIC-SOURCE".into(),
        buy_receipt_signature: "SYNTHETIC-RECEIPT".into(),
        position_id: "exec-canary-pos:exec-canary:native-buy-v1:SYNTHETIC-SOURCE".into(),
        wallet_pubkey: wallet.clone(), signer_pubkey: wallet,
        genesis_hash: crate::app_tests::owner_buy_fixture::GENESIS.into(),
        mint: crate::app_tests::controlled_copy_pipeline_fixture::USDC.into(),
        amount_raw: 988620, decimals: 6, route: c.canary_route.clone(),
        activated_at: now.to_rfc3339(), expires_at: (now + Duration::seconds(60)).to_rfc3339(),
        max_priority_fee_lamports: 22000, min_reserve_lamports: 0,
        max_slippage_bps: 50, max_daily_loss_lamports: 20000000,
    });
    c
}

#[tokio::test]
async fn receipt_exit_only_validated_route_precedes_ingress_hold() -> Result<()> {
    let cycle = crate::app_tests::copy_cycle_fixture::Cycle::new().await?;
    let c = receipt_only(cycle.app.execution.clone());
    assert_eq!(copybot_config::execution_only_mode(&c)?, Some(ExecutionOnlyMode::BotReceiptExit));
    assert!(!copybot_config::owner_execution_roundtrip(&c));
    let runner = crate::execution_canary::ExecutionCanaryRunner::new(c)
        .with_ingress_hold(Some(Arc::new(AtomicBool::new(true))));
    let summary = Box::pin(runner.process_tick(&cycle.case.bot, Utc::now())).await?;
    assert_ne!(summary.skipped_reason, Some("ingress_http_recovery_pending"));
    assert_eq!(cycle.bot.sends(), 0); // inactive receipt policy supplies no authority.
    assert_eq!(cycle.source.sends(), 0);
    Ok(())
}

#[tokio::test]
async fn receipt_exit_only_conflicting_buy_or_active_old_scope_refuses() -> Result<()> {
    let cycle = crate::app_tests::copy_cycle_fixture::Cycle::new().await?;
    let c = receipt_only(cycle.app.execution.clone());
    let mut conflict = c.clone();
    conflict.owner_technical_buy = cycle.case.source.config.owner_technical_buy.clone();
    assert!(copybot_config::execution_only_mode(&conflict).is_err());
    let mut active = c.clone();
    active.tiny_experiment.activate = true;
    assert!(copybot_config::execution_only_mode(&active).is_err());
    let mut amount = c;
    amount.owner_exit.as_mut().unwrap().amount_raw = 0;
    assert!(copybot_config::execution_only_mode(&amount).is_err());
    Ok(())
}

#[tokio::test]
async fn receipt_exit_only_does_not_change_roundtrip_selection() -> Result<()> {
    let cycle = crate::app_tests::copy_cycle_fixture::Cycle::new().await?;
    let mut c = cycle.case.source.config.clone();
    c.owner_exit = None;
    c.owner_technical_buy.as_mut().unwrap().policy =
        copybot_config::OWNER_EXECUTION_ROUNDTRIP_V1.into();
    assert!(copybot_config::owner_execution_roundtrip(&c));
    assert_eq!(copybot_config::execution_only_mode(&c)?, Some(ExecutionOnlyMode::OwnerRoundtrip));
    Ok(())
}

#[tokio::test]
async fn receipt_exit_only_scheduler_stops_without_normal_app_services() -> Result<()> {
    let cycle = crate::app_tests::copy_cycle_fixture::Cycle::new().await?;
    let c = receipt_only(cycle.app.execution.clone());
    assert_eq!(copybot_config::execution_only_mode(&c)?, Some(ExecutionOnlyMode::BotReceiptExit));
    let runner = crate::execution_canary::ExecutionCanaryRunner::new(c);
    let path = cycle.case.path.clone();
    let store = copybot_storage_core::SqliteStore::open(&path)?;
    crate::execution_only_loop::run_until(store, runner, 1, async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        Ok(())
    }).await?;
    assert_eq!(cycle.bot.sends(), 0);
    assert_eq!(cycle.source.sends(), 0);
    Ok(())
}
