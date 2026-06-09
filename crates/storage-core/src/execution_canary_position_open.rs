use crate::{
    execution_canary_fill_marker::{fill_exists, insert_fill_marker_if_order_exists},
    execution_canary_positions::execution_canary_position_from_row,
    money::{sol_to_lamports_ceil, u64_to_sql_i64},
    shadow_lots::reject_zero_raw_exact_qty,
    ExecutionCanaryOwnedPosition, ExecutionCanaryOwnedPositionRecordResult,
    ExecutionCanaryPositionRecordOutcome, SqliteDiscoveryStore,
    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET, EXECUTION_CANARY_POSITION_STATE_OPEN,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{Lamports, TokenQuantity};
use rusqlite::{params, Connection, OptionalExtension};

impl SqliteDiscoveryStore {
    pub fn record_execution_canary_open_position(
        &self,
        order_id: &str,
        token: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        cost_sol: f64,
        opened_ts: DateTime<Utc>,
    ) -> Result<ExecutionCanaryOwnedPositionRecordResult> {
        validate_position_inputs(order_id, token, qty, cost_sol)?;
        let qty_exact = reject_zero_raw_exact_qty(qty_exact, "execution canary open position")?;
        let position_id = execution_canary_position_id(order_id);
        let cost_lamports = sol_to_lamports_ceil(cost_sol, "execution canary position cost_sol")?;
        let opened_ts_raw = opened_ts.to_rfc3339();
        let result = self.with_immediate_transaction_retry(
            "execution canary open position record",
            |conn| {
                if fill_exists(conn, order_id)? {
                    let position = load_existing_fill_position(conn, &position_id, token)?
                        .ok_or_else(|| {
                            anyhow!(
                                "missing execution canary position for existing fill {order_id}"
                            )
                        })?;
                    return Ok(ExecutionCanaryOwnedPositionRecordResult {
                        outcome: ExecutionCanaryPositionRecordOutcome::Existing,
                        position,
                    });
                }
                if let Some(position) = load_position_by_id(conn, &position_id)? {
                    insert_fill_marker_if_order_exists(
                        conn,
                        order_id,
                        token,
                        qty,
                        qty_exact,
                        cost_sol,
                        cost_lamports,
                    )?;
                    return Ok(ExecutionCanaryOwnedPositionRecordResult {
                        outcome: ExecutionCanaryPositionRecordOutcome::Existing,
                        position,
                    });
                }
                if let Some(position) = load_open_position_by_token(conn, token)? {
                    merge_open_position(conn, &position, qty, qty_exact, cost_sol, cost_lamports)?;
                    insert_fill_marker_if_order_exists(
                        conn,
                        order_id,
                        token,
                        qty,
                        qty_exact,
                        cost_sol,
                        cost_lamports,
                    )?;
                    let position =
                        load_position_by_id(conn, &position.position_id)?.ok_or_else(|| {
                            anyhow!(
                                "missing merged execution canary position {}",
                                position.position_id
                            )
                        })?;
                    return Ok(ExecutionCanaryOwnedPositionRecordResult {
                        outcome: ExecutionCanaryPositionRecordOutcome::Merged,
                        position,
                    });
                }

                insert_open_position(
                    conn,
                    &position_id,
                    token,
                    qty,
                    qty_exact,
                    cost_sol,
                    cost_lamports,
                    &opened_ts_raw,
                )?;
                insert_fill_marker_if_order_exists(
                    conn,
                    order_id,
                    token,
                    qty,
                    qty_exact,
                    cost_sol,
                    cost_lamports,
                )?;
                let position = load_position_by_id(conn, &position_id)?.ok_or_else(|| {
                    anyhow!("missing execution canary position for order {order_id}")
                })?;
                Ok(ExecutionCanaryOwnedPositionRecordResult {
                    outcome: ExecutionCanaryPositionRecordOutcome::Inserted,
                    position,
                })
            },
        )?;
        Ok(result)
    }
}

fn load_existing_fill_position(
    conn: &Connection,
    position_id: &str,
    token: &str,
) -> Result<Option<ExecutionCanaryOwnedPosition>> {
    if let Some(position) = load_position_by_id(conn, position_id)? {
        return Ok(Some(position));
    }
    if let Some(position) = load_open_position_by_token(conn, token)? {
        return Ok(Some(position));
    }
    load_latest_position_by_token(conn, token)
}

fn validate_position_inputs(order_id: &str, token: &str, qty: f64, cost_sol: f64) -> Result<()> {
    if order_id.trim().is_empty() {
        return Err(anyhow!(
            "execution canary position order_id must not be empty"
        ));
    }
    if token.trim().is_empty() {
        return Err(anyhow!("execution canary position token must not be empty"));
    }
    if !qty.is_finite() || qty <= 0.0 {
        return Err(anyhow!(
            "invalid execution canary position qty={qty} (must be finite and > 0)"
        ));
    }
    if !cost_sol.is_finite() || cost_sol < 0.0 {
        return Err(anyhow!(
            "invalid execution canary position cost_sol={cost_sol}"
        ));
    }
    Ok(())
}

fn load_position_by_id(
    conn: &Connection,
    position_id: &str,
) -> Result<Option<ExecutionCanaryOwnedPosition>> {
    let mut stmt = conn
        .prepare(
            "SELECT
                position_id,
                token,
                qty,
                cost_sol,
                cost_lamports,
                qty_raw,
                qty_decimals,
                opened_ts,
                state,
                accounting_bucket
             FROM positions
             WHERE position_id = ?1
             LIMIT 1",
        )
        .context("failed to prepare execution canary position lookup")?;
    stmt.query_row(params![position_id], execution_canary_position_from_row)
        .optional()
        .context("failed querying execution canary position")
}

fn load_open_position_by_token(
    conn: &Connection,
    token: &str,
) -> Result<Option<ExecutionCanaryOwnedPosition>> {
    let mut stmt = conn
        .prepare(
            "SELECT
                position_id,
                token,
                qty,
                cost_sol,
                cost_lamports,
                qty_raw,
                qty_decimals,
                opened_ts,
                state,
                accounting_bucket
             FROM positions
             WHERE token = ?1
               AND accounting_bucket = ?2
               AND state = ?3
             LIMIT 1",
        )
        .context("failed to prepare execution canary open position merge lookup")?;
    stmt.query_row(
        params![
            token,
            EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
            EXECUTION_CANARY_POSITION_STATE_OPEN,
        ],
        execution_canary_position_from_row,
    )
    .optional()
    .context("failed querying execution canary open position for merge")
}

fn load_latest_position_by_token(
    conn: &Connection,
    token: &str,
) -> Result<Option<ExecutionCanaryOwnedPosition>> {
    let mut stmt = conn
        .prepare(
            "SELECT
                position_id,
                token,
                qty,
                cost_sol,
                cost_lamports,
                qty_raw,
                qty_decimals,
                opened_ts,
                state,
                accounting_bucket
             FROM positions
             WHERE token = ?1
               AND accounting_bucket = ?2
             ORDER BY opened_ts DESC, position_id DESC
             LIMIT 1",
        )
        .context("failed to prepare execution canary latest position lookup")?;
    stmt.query_row(
        params![token, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET],
        execution_canary_position_from_row,
    )
    .optional()
    .context("failed querying execution canary latest position")
}

fn insert_open_position(
    conn: &Connection,
    position_id: &str,
    token: &str,
    qty: f64,
    qty_exact: Option<TokenQuantity>,
    cost_sol: f64,
    cost_lamports: Lamports,
    opened_ts: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO positions(
            position_id,
            token,
            qty,
            cost_sol,
            opened_ts,
            closed_ts,
            pnl_sol,
            state,
            cost_lamports,
            qty_raw,
            qty_decimals,
            pnl_lamports,
            accounting_bucket
        ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, ?6, ?7, ?8, ?9, NULL, ?10)",
        params![
            position_id,
            token,
            qty,
            cost_sol,
            opened_ts,
            EXECUTION_CANARY_POSITION_STATE_OPEN,
            u64_to_sql_i64("positions.cost_lamports", cost_lamports.as_u64())?,
            qty_exact.map(|value| value.raw().to_string()),
            qty_exact.map(|value| i64::from(value.decimals())),
            EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
        ],
    )
    .context("failed inserting execution canary position")?;
    Ok(())
}

fn merge_open_position(
    conn: &Connection,
    position: &ExecutionCanaryOwnedPosition,
    qty: f64,
    qty_exact: Option<TokenQuantity>,
    cost_sol: f64,
    cost_lamports: Lamports,
) -> Result<()> {
    let next_qty = position.qty + qty;
    if !next_qty.is_finite() || next_qty <= 0.0 {
        return Err(anyhow!(
            "invalid merged execution canary position qty={next_qty}"
        ));
    }
    let next_cost_sol = position.cost_sol + cost_sol;
    if !next_cost_sol.is_finite() || next_cost_sol < 0.0 {
        return Err(anyhow!(
            "invalid merged execution canary position cost_sol={next_cost_sol}"
        ));
    }
    let current_cost = match position.cost_lamports {
        Some(value) => value,
        None => sol_to_lamports_ceil(position.cost_sol, "execution canary existing cost_sol")?,
    };
    let next_cost_lamports = current_cost
        .as_u64()
        .checked_add(cost_lamports.as_u64())
        .ok_or_else(|| anyhow!("execution canary position cost_lamports overflow"))?;
    let next_qty_exact = merge_buy_qty_exact(position.qty_exact, qty_exact)?;
    conn.execute(
        "UPDATE positions
         SET qty = ?2,
             qty_raw = ?3,
             qty_decimals = ?4,
             cost_sol = ?5,
             cost_lamports = ?6
         WHERE position_id = ?1
           AND accounting_bucket = ?7
           AND state = ?8",
        params![
            position.position_id.as_str(),
            next_qty,
            next_qty_exact.map(|value| value.raw().to_string()),
            next_qty_exact.map(|value| i64::from(value.decimals())),
            next_cost_sol,
            u64_to_sql_i64("positions.cost_lamports", next_cost_lamports)?,
            EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
            EXECUTION_CANARY_POSITION_STATE_OPEN,
        ],
    )
    .context("failed merging execution canary open position")?;
    Ok(())
}

fn merge_buy_qty_exact(
    current: Option<TokenQuantity>,
    added: Option<TokenQuantity>,
) -> Result<Option<TokenQuantity>> {
    match (current, added) {
        (Some(current), Some(added)) if current.decimals() == added.decimals() => {
            let raw = current
                .raw()
                .checked_add(added.raw())
                .ok_or_else(|| anyhow!("execution canary position qty_raw overflow"))?;
            Ok(Some(TokenQuantity::new(raw, current.decimals())))
        }
        (Some(_), Some(_)) => Err(anyhow!(
            "execution canary position merge exact qty decimals mismatch"
        )),
        _ => Ok(None),
    }
}

fn execution_canary_position_id(order_id: &str) -> String {
    format!("exec-canary-pos:{order_id}")
}
