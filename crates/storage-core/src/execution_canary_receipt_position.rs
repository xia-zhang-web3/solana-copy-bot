use crate::{
    ExecutionCanaryOrder, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
    EXECUTION_CANARY_POSITION_STATE_OPEN, EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
};
use anyhow::Result;
use copybot_core_types::{Lamports, TokenQuantity};
use rusqlite::{params, Connection, OptionalExtension};

#[derive(Debug)]
pub struct ReceiptAccountingUnsupported(pub &'static str);
impl std::fmt::Display for ReceiptAccountingUnsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ReceiptAccountingUnsupported {}

/// Validate exact inventory on the same connection/transaction that writes the fill.
pub(crate) fn validate_receipt_position(
    conn: &Connection,
    order: &ExecutionCanaryOrder,
    token: &str,
    qty: Option<TokenQuantity>,
    actual_lamports: Option<Lamports>,
    sell: bool,
) -> Result<()> {
    if order.status != EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED {
        return Ok(());
    }
    let qty = qty.ok_or(ReceiptAccountingUnsupported(
        "receipt_exact_quantity_missing",
    ))?;
    if actual_lamports.is_none_or(|v| v.as_u64() == 0) {
        return Err(ReceiptAccountingUnsupported("receipt_exact_cash_flow_missing").into());
    }
    let current: Option<(Option<String>, Option<u8>)> = conn.query_row(
        "SELECT qty_raw, qty_decimals FROM positions WHERE token = ?1 AND accounting_bucket = ?2 AND state = ?3 LIMIT 1",
        params![token,EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,EXECUTION_CANARY_POSITION_STATE_OPEN],
        |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
    let Some((raw, decimals)) = current else {
        return Ok(());
    };
    let Some((raw, decimals)) = raw.and_then(|s| s.parse::<u64>().ok()).zip(decimals) else {
        return Err(ReceiptAccountingUnsupported("receipt_position_exact_quantity_missing").into());
    };
    if decimals != qty.decimals() {
        return Err(ReceiptAccountingUnsupported("receipt_position_decimals_mismatch").into());
    }
    if (sell && qty.raw() > raw) || (!sell && raw.checked_add(qty.raw()).is_none()) {
        return Err(ReceiptAccountingUnsupported("receipt_position_quantity_unsupported").into());
    }
    Ok(())
}
