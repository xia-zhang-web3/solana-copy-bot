use crate::sqlite_retry::is_retryable_sqlite_error;
use crate::{
    merge_position_qty_exact_on_sell, note_sqlite_busy_error, note_sqlite_write_retry,
    shadow_lot_cost_lamports, signed_lamports_to_sol, signed_lamports_to_sql_i64,
    sol_to_lamports_ceil_storage, sol_to_lamports_floor_storage, split_token_quantity_pro_rata,
    token_quantity_from_sql, u64_to_sql_i64, ShadowCloseOutcome, ShadowLotRow, SqliteStore,
    POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER, POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER,
    SHADOW_CLOSE_CONTEXT_MARKET, SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
    SHADOW_RISK_CONTEXT_MARKET, SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY, SQLITE_WRITE_MAX_RETRIES,
    SQLITE_WRITE_RETRY_BACKOFF_MS,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::TokenQuantity;
use rusqlite::{params, OptionalExtension};
use std::collections::HashSet;
use std::io;
use std::time::Duration as StdDuration;

pub(crate) const SHADOW_LOT_OPEN_EPS: f64 = 1e-12;

fn to_sql_conversion_error(error: anyhow::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(io::Error::other(error.to_string())))
}

fn reject_zero_raw_exact_qty(
    qty_exact: Option<TokenQuantity>,
    context: &str,
) -> Result<Option<TokenQuantity>> {
    match qty_exact {
        Some(qty_exact) if qty_exact.raw() == 0 => {
            Err(anyhow!("zero-raw exact quantity is invalid in {}", context))
        }
        other => Ok(other),
    }
}

fn shadow_accounting_bucket_for_qty_exact(qty_exact: Option<TokenQuantity>) -> &'static str {
    if qty_exact.is_some() {
        POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER
    } else {
        POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER
    }
}

fn validate_shadow_risk_context(risk_context: &str) -> Result<()> {
    match risk_context {
        SHADOW_RISK_CONTEXT_MARKET | SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY => Ok(()),
        other => Err(anyhow!("unsupported shadow risk_context: {other}")),
    }
}
