//! Shared insert primitive; callers choose the transaction boundary.
use super::*;
use rusqlite::Connection;
#[allow(clippy::too_many_arguments)]
pub(crate) fn insert_on_conn(
    conn: &Connection,
    wallet_id: &str,
    token: &str,
    qty: f64,
    qty_exact: Option<TokenQuantity>,
    cost_sol: f64,
    risk_context: &str,
    opened_ts: DateTime<Utc>,
) -> rusqlite::Result<i64> {
    let cost_lamports =
        sol_to_lamports_ceil(cost_sol, "shadow lot cost_sol").map_err(to_sql_conversion_error)?;
    let accounting_bucket = shadow_accounting_bucket_for_qty_exact(qty_exact);
    conn.execute(
        "INSERT INTO shadow_lots(
                    wallet_id,
                    token,
                    accounting_bucket,
                    risk_context,
                    qty,
                    qty_raw,
                    qty_decimals,
                    cost_sol,
                    cost_lamports,
                    opened_ts
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            wallet_id,
            token,
            accounting_bucket,
            risk_context,
            qty,
            qty_exact.as_ref().map(|value| value.raw().to_string()),
            qty_exact.as_ref().map(|value| i64::from(value.decimals())),
            cost_sol,
            u64_to_sql_i64("shadow_lots.cost_lamports", cost_lamports.as_u64())
                .map_err(to_sql_conversion_error)?,
            opened_ts.to_rfc3339()
        ],
    )?;
    Ok(conn.last_insert_rowid())
}
