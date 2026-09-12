//! Invoked only inside the canonical successful/failed receipt writer transaction.
use super::*;

pub(crate) fn settle(
    conn: &Connection,
    order: &str,
    signature: &str,
    wallet: &str,
    payer: Option<&str>,
    fee: Option<u64>,
    outcome: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    let reservation:Option<(String,String,String,u64,Option<u64>,Option<String>)>=conn.query_row(
        "SELECT tx_signature,wallet,side,fee_bound,actual_fee,outcome FROM execution_tiny_reservations WHERE order_id=?1",[order],
        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
    let Some((sig, bound_wallet, side, bound, old, old_outcome)) = reservation else {
        return Ok(());
    };
    ensure!(
        signature == sig && wallet == bound_wallet,
        "tiny_budget_receipt_binding"
    );
    let binding:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM execution_canary_dispatch d JOIN orders o ON o.order_id=d.order_id
        JOIN execution_tiny_reservations r ON r.order_id=d.order_id JOIN execution_tiny_experiment e ON e.experiment_id=r.experiment_id
        WHERE d.order_id=?1 AND d.tx_signature=?2 AND o.tx_signature=d.tx_signature AND d.wallet=?3 AND e.wallet=d.wallet
        AND d.signal_id=o.signal_id AND d.client_order_id=o.client_order_id AND d.route=o.route AND d.attempt=o.attempt
        AND d.side=r.side AND d.message_sha256=r.message_sha256 AND d.transaction_sha256=r.transaction_sha256)",
        params![order,signature,wallet],|r|r.get(0))?;
    ensure!(binding, "tiny_budget_receipt_dispatch_conflict");
    // Known payer must match the configured payer. Missing facts cannot release a reserve.
    ensure!(
        payer.is_none_or(|p| p == wallet),
        "tiny_budget_receipt_payer_conflict"
    );
    let Some(fee) = fee.filter(|_| payer == Some(wallet)) else {
        return Ok(());
    };
    if let Some(old) = old {
        ensure!(
            old == fee && old_outcome.as_deref() == Some(outcome),
            "tiny_budget_receipt_fee_conflict"
        );
        return Ok(());
    }
    conn.execute("UPDATE execution_tiny_reservations SET actual_fee=?2,outcome=?3,reconciled_at=?4 WHERE order_id=?1 AND actual_fee IS NULL",
        params![order,i64::try_from(fee)?,outcome,now.to_rfc3339()])?;
    // A violated observation bound is accounted, never hidden by rejecting the receipt.
    if fee > bound {
        stop(conn, "tiny_budget_actual_fee_over_bound")?;
    }
    if side == "buy" && outcome == "failed" {
        stop(conn, "tiny_budget_buy_failed")?;
    }
    refresh(conn, now)?;
    Ok(())
}
