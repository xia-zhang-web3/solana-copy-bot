//! Explicit SELL-only recovery retains the original SOURCE parent and BUY graph.
//! It never removes the normal copy gate or grants authority to BUY or trade BOT.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{OwnerExitIntent, SqliteStore};

pub(super) fn current(
    c: &ExecutionConfig, s: &SqliteStore, exit: &OwnerExitIntent, now: DateTime<Utc>,
) -> Result<()> {
    copybot_config::validate_owner_technical_buy(c)?;
    let p = c.owner_technical_buy.as_ref().context("source_recovery_parent_missing")?;
    let parent = crate::execution_owner_buy_authority::from_config(p)?;
    ensure!(s.load_owner_technical_buy_intent(&p.intent_id)?.as_ref() == Some(&parent),
        "source_recovery_parent_changed");
    ensure!(now >= parent.activated_at && now < parent.expires_at,
        "source_recovery_parent_expired");
    let order = s.load_execution_canary_order(&exit.buy_order_id)?
        .context("source_recovery_buy_missing")?;
    let proof = s.load_execution_canary_receipt_proof(&exit.buy_order_id)?
        .context("source_recovery_buy_proof_missing")?;
    let facts = s.load_execution_canary_receipt_facts(&exit.buy_order_id)?
        .context("source_recovery_buy_facts_missing")?;
    ensure!(order.status == copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
        && order.tx_signature.as_deref() == Some(exit.buy_receipt_signature.as_str())
        && proof.reason == "accounting_complete"
        && matches!(proof.confirmation_status.as_str(), "confirmed" | "finalized")
        && proof.tx_signature == exit.buy_receipt_signature
        && facts.tx_signature == exit.buy_receipt_signature
        && proof.wallet_pubkey == exit.wallet && facts.wallet_pubkey == exit.wallet
        && proof.token == exit.mint && facts.token == exit.mint
        && proof.side == "buy" && facts.side == "buy"
        && proof.slot == Some(facts.slot) && facts.slot > 0
        && facts.token_delta.is_some_and(|q| q.raw == i128::from(exit.amount_raw)
            && q.decimals == exit.decimals)
        && facts.transaction_fee.is_some_and(|f| f.as_u64() <= 100_000)
        && facts.wallet_is_fee_payer() == Some(true)
        && facts.wallet_native_delta.as_i128() < 0,
        "source_recovery_buy_receipt_changed");
    s.validate_owner_source_recovery_buy_cost(exit)?;
    Ok(())
}
