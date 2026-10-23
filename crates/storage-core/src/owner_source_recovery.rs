//! A new exit window never renews the BUY parent or acquires another receipt lot.
use crate::{OwnerExitIntent, SqliteStore};
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection};

pub fn source_recovery_exit_id(id: &str) -> bool { id.starts_with("source-recovery:")||crate::owner_exit_child::is_child(id) }
pub(crate) fn recovery(id: &str) -> bool { source_recovery_exit_id(id) }
pub(crate) fn single_attempt(id: &str) -> bool {
    recovery(id) || id.starts_with("execution-only:")
}
pub(crate) fn validate(c: &Connection, i: &OwnerExitIntent) -> Result<u64> {
    if crate::owner_exit_child::is_child(&i.intent_id){
        crate::owner_exit_child::validate(c,i)?;
        validate_parent(c,i,&crate::owner_exit_child::parent_id(&i.intent_id)?)
    }else{validate_parent(c,i,&i.intent_id)}
}
pub(crate) fn validate_parent(c:&Connection,i:&OwnerExitIntent,parent_id:&str)->Result<u64>{
    let id=parent_id.strip_prefix("source-recovery:").context("source_recovery_origin")?;
    let (wallet,run,scope)=crate::controlled_receipt_lot::load(c)?.context("source_recovery_scope")?;
    ensure!(wallet == i.wallet && run == i.run_id && scope.source_wallet == wallet
        && scope.source_buy_intent_id == id && scope.source_run_id == run,
        "source_recovery_scope_changed");
    let parent: bool=c.query_row("SELECT EXISTS(SELECT 1 FROM owner_technical_buy_intents b
        WHERE b.intent_id=?1 AND b.run_id=?2 AND b.wallet=?3 AND b.signer=?3
        AND b.genesis_hash=?4 AND b.mint=?5 AND b.route=?6
        AND b.amount_lamports=10000000 AND b.min_reserve_lamports=0
        AND b.max_priority_fee_lamports>=?7 AND b.max_slippage_bps>=?8
        AND b.max_daily_loss_lamports>=?9 AND b.max_open_positions=1 AND b.max_buy_count=1)",
        params![id,i.run_id,i.wallet,i.genesis_hash,i.mint,i.route,
            i.max_priority_fee_lamports,i.max_slippage_bps,i.max_daily_loss_lamports],|r|r.get(0))?;
    ensure!(parent && i.buy_order_id == crate::owner_technical_buy_order_id(id)
        && i.max_priority_fee_lamports <= 22_000 && i.max_slippage_bps <= 50
        && i.max_daily_loss_lamports <= 20_000_000 && i.min_reserve_lamports == 0,
        "source_recovery_parent_changed");
    ensure!(i.expires_at.signed_duration_since(i.activated_at) <= chrono::Duration::seconds(300),
        "source_recovery_window_cap");
    crate::owner_exit_binding::ensure_buy_binding(c,i)?;
    let accounted:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM execution_canary_receipt_proofs
        WHERE order_id=?1 AND tx_signature=?2 AND reason='accounting_complete'
        AND confirmation_status='finalized')",params![i.buy_order_id,i.buy_receipt_signature],|r|r.get(0))?;
    let f=crate::receipt_facts_rows::load(c,&i.buy_order_id)?.context("source_recovery_buy_receipt_unresolved")?;
    ensure!(accounted && f.tx_signature == i.buy_receipt_signature
        && f.fee_payer.as_deref() == Some(i.wallet.as_str()) && f.fee_coverage.as_str() == "known",
        "source_recovery_buy_receipt_unresolved");
    let fee=f.transaction_fee.context("source_recovery_buy_fee")?.as_u64();
    // This is the exact owned transaction's debit, never a before/after wallet snapshot.
    let debit=u64::try_from(f.wallet_native_delta.as_i128().checked_neg().context("source_recovery_debit")?)?;
    ensure!(debit >= crate::tiny_experiment::TINY_BUY_LAMPORTS.checked_add(fee)
        .context("source_recovery_debit")?,"source_recovery_debit");
    let remaining=crate::TINY_NATIVE_ALLOWANCE.checked_sub(debit).context("source_recovery_native_cap")?;
    ensure!(remaining >= crate::tiny_experiment::TINY_TRANSACTION_FEE,"source_recovery_native_cap");
    Ok(remaining)
}
impl SqliteStore {
    /// Validates the canonical chain on every authority boundary; no new financial rows.
    pub fn owner_source_recovery_native_remaining(&self, i: &OwnerExitIntent) -> Result<u64> {
        validate(&self.conn,i)
    }
}
