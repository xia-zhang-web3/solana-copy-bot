use crate::money::u64_to_sql_i64;
use anyhow::{Context, Result};
use copybot_core_types::{Lamports, TokenQuantity};
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) fn fill_exists(conn: &Connection, order_id: &str) -> Result<bool> {
    let existing: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM fills WHERE order_id = ?1 LIMIT 1",
            params![order_id],
            |row| row.get(0),
        )
        .optional()
        .context("failed checking execution canary fill marker")?;
    Ok(existing.is_some())
}

pub(crate) fn insert_fill_marker_if_order_exists(
    conn: &Connection,
    order_id: &str,
    token: &str,
    qty: f64,
    qty_exact: Option<TokenQuantity>,
    notional_sol: f64,
    notional_lamports: Lamports,
) -> Result<()> {
    if !order_exists(conn, order_id)? {
        return Ok(());
    }
    let avg_price = if notional_sol > 0.0 && qty > 0.0 {
        notional_sol / qty
    } else {
        0.0
    };
    conn.execute(
        "INSERT OR IGNORE INTO fills(
            order_id,
            token,
            qty,
            avg_price,
            fee,
            slippage_bps,
            notional_lamports,
            fee_lamports,
            qty_raw,
            qty_decimals
        ) VALUES (?1, ?2, ?3, ?4, 0.0, 0.0, ?5, 0, ?6, ?7)",
        params![
            order_id,
            token,
            qty,
            avg_price,
            u64_to_sql_i64("fills.notional_lamports", notional_lamports.as_u64())?,
            qty_exact.map(|value| value.raw().to_string()),
            qty_exact.map(|value| i64::from(value.decimals())),
        ],
    )
    .context("failed inserting execution canary fill marker")?;
    Ok(())
}

fn order_exists(conn: &Connection, order_id: &str) -> Result<bool> {
    let existing: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM orders WHERE order_id = ?1 LIMIT 1",
            params![order_id],
            |row| row.get(0),
        )
        .optional()
        .context("failed checking execution canary fill order")?;
    Ok(existing.is_some())
}
