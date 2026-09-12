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

// Retain the legacy SELL marker contract, including NULL destination and conflict handling.
pub(crate) fn insert_fill_marker_if_order_exists(
    conn: &Connection,
    order_id: &str,
    token: &str,
    qty: f64,
    qty_exact: Option<TokenQuantity>,
    notional_sol: f64,
    notional_lamports: Lamports,
) -> Result<()> {
    insert_buy_fill_marker(
        conn,
        order_id,
        token,
        None,
        qty,
        qty_exact,
        notional_sol,
        notional_lamports,
    )
}

pub(crate) fn insert_buy_fill_marker(
    conn: &Connection,
    order_id: &str,
    token: &str,
    position_id: Option<&str>,
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
    let linked = position_id.is_some() && crate::buy_fill_identity::has_destination(conn)?;
    let conflict = if position_id.is_some() {
        ""
    } else {
        " OR IGNORE"
    };
    let columns = if linked { ",position_id" } else { "" };
    let value = if linked { ",?8" } else { "" };
    let sql = format!(
        "INSERT{conflict} INTO fills(
            order_id,
            token,
            qty,
            avg_price,
            fee,
            slippage_bps,
            notional_lamports,
            fee_lamports,
            qty_raw,
            qty_decimals{columns}
        ) VALUES (?1, ?2, ?3, ?4, 0.0, 0.0, ?5, 0, ?6, ?7{value})"
    );
    let notional = u64_to_sql_i64("fills.notional_lamports", notional_lamports.as_u64())?;
    let raw = qty_exact.map(|value| value.raw().to_string());
    let decimals = qty_exact.map(|value| i64::from(value.decimals()));
    let mut values: Vec<&dyn rusqlite::ToSql> = vec![
        &order_id, &token, &qty, &avg_price, &notional, &raw, &decimals,
    ];
    if linked {
        values.push(&position_id);
    }
    let rows = conn
        .execute(&sql, values.as_slice())
        .context("failed inserting execution canary fill marker")?;
    if position_id.is_some() {
        anyhow::ensure!(rows == 1, "BUY fill marker insertion refused");
    }
    Ok(())
}

pub(crate) fn order_exists(conn: &Connection, order_id: &str) -> Result<bool> {
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
