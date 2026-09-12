use crate::{
    receipt_facts_identity::validate_identity, receipt_facts_rows, ExecutionCanaryOrder,
    ExecutionCanaryOwnedPositionRecordResult, ExecutionCanaryPositionRecordOutcome,
    EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
};
use anyhow::{ensure, Result};
use copybot_core_types::{Lamports, TokenQuantity};
use rusqlite::{params, Connection};

/// A generic insert/import is not proof of initial zero. Only a newly inserted
/// actual receipt BUY with matching exact operands can initialize it. Never merge/backfill.
pub(crate) fn initialize(
    conn: &Connection,
    order: &ExecutionCanaryOrder,
    result: &ExecutionCanaryOwnedPositionRecordResult,
    qty: Option<TokenQuantity>,
    actual: Option<Lamports>,
) -> Result<()> {
    if order.status != EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
        || result.outcome != ExecutionCanaryPositionRecordOutcome::Inserted
    {
        return Ok(());
    }
    // Pre-0054 compatibility callers cannot prove receipt facts or an initial zero.
    if !conn.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_canary_receipt_facts'")?.exists([])? {
        return Ok(());
    }
    let Some(facts) = receipt_facts_rows::load(conn, &order.order_id)? else {
        return Ok(());
    };
    validate_identity(conn, &facts)?;
    ensure!(
        facts.side == "buy"
            && qty.is_some_and(|q| facts
                .token_delta
                .is_some_and(|d| d.raw == i128::from(q.raw()) && d.decimals == q.decimals()))
            && actual.is_some_and(|cost| cost.as_u64() > 0
                && facts.wallet_native_delta.as_i128() == -i128::from(cost.as_u64())),
        "initial BUY result requires exact receipt operands"
    );
    let rows = conn.execute(
        "UPDATE positions SET pnl_lamports=0
        WHERE position_id=?1 AND state='open' AND pnl_lamports IS NULL
        AND EXISTS(SELECT 1 FROM fills WHERE order_id=?2)",
        params![result.position.position_id, order.order_id],
    )?;
    ensure!(rows == 1, "initial BUY result position changed");
    Ok(())
}
