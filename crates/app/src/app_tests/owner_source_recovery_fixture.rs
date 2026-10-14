//! SOURCE adapter/signing are real; quotes, chain receipts and RPC are explicit loopback mocks.
use super::owner_source_gate_fixture::GateCase;
use anyhow::{Context, Result};
use copybot_config::{ExecutionConfig, OwnerExitConfig, OWNER_SOURCE_RECOVERY_EXIT_V1};

pub(super) fn recovery(c: &GateCase) -> Result<ExecutionConfig> {
    let mut e = c.source.config.clone();
    let buy = c.source.store.load_execution_canary_receipt_facts(&c.source.order_id())?
        .context("fixture confirmed BUY")?;
    let position = c.source.store.load_execution_canary_open_position(&buy.token)?.unwrap();
    let b = e.owner_technical_buy.as_mut().unwrap();
    b.activate = false;
    e.canary_entry_submit_enabled = false;
    e.owner_exit = Some(OwnerExitConfig {
        native_recovery: None,
        policy: OWNER_SOURCE_RECOVERY_EXIT_V1.into(), activate: true,
        run_id: b.run_id.clone(), intent_id: format!("execution-only:{}", b.intent_id),
        buy_order_id: c.source.order_id(), buy_receipt_signature: buy.tx_signature,
        position_id: position.position_id, wallet_pubkey: b.wallet_pubkey.clone(),
        signer_pubkey: b.signer_pubkey.clone(), genesis_hash: b.genesis_hash.clone(),
        mint: b.mint.clone(), amount_raw: u64::try_from(buy.token_delta.unwrap().raw)?, decimals: 6,
        route: b.route.clone(), activated_at: b.activated_at.clone(), expires_at: b.expires_at.clone(),
        max_priority_fee_lamports: 22_000, min_reserve_lamports: b.min_reserve_lamports,
        max_slippage_bps: b.max_slippage_bps, max_daily_loss_lamports: b.max_daily_loss_lamports,
    });
    copybot_config::validate_owner_technical_buy(&e)?;
    copybot_config::validate_owner_exit(&e)?;
    Ok(e)
}

pub(super) async fn prepared() -> Result<(GateCase, ExecutionConfig)> {
    let c = GateCase::new().await?;
    c.source.tick().await?;
    assert_eq!(c.source.calls("sendTransaction"), 1);
    assert!(!c.ready()?, "normal SOURCE gate must hold without confirmed BOT BUY");
    let e = recovery(&c)?;
    Ok((c, e))
}

pub(super) async fn tick(c: &GateCase, e: &ExecutionConfig) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
    crate::execution_canary::ExecutionCanaryRunner::new(e.clone())
        .process_tick(&c.source.store, chrono::Utc::now()).await
}
