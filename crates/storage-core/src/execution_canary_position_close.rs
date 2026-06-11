use crate::{
    execution_canary_fill_marker::{fill_exists, insert_fill_marker_if_order_exists},
    execution_canary_positions::execution_canary_position_from_row,
    money::{
        merge_position_qty_exact_on_sell, signed_lamports_to_sol, signed_lamports_to_sql_i64,
        sol_to_lamports_ceil, sol_to_lamports_floor, u64_to_sql_i64,
    },
    shadow_lots::reject_zero_raw_exact_qty,
    ExecutionCanaryOwnedPosition, ExecutionCanaryPositionCloseResult, SqliteDiscoveryStore,
    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET, EXECUTION_CANARY_POSITION_CLOSE_CLOSED,
    EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED, EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION,
    EXECUTION_CANARY_POSITION_CLOSE_PARTIAL, EXECUTION_CANARY_POSITION_STATE_CLOSED,
    EXECUTION_CANARY_POSITION_STATE_OPEN,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{Lamports, SignedLamports, TokenQuantity};
use rusqlite::{params, Connection, OptionalExtension};

const EXECUTION_CANARY_POSITION_CLOSE_EPS: f64 = 1e-12;
const EXECUTION_CANARY_POSITION_RAW_DUST_UNITS: u64 = 1;

impl SqliteDiscoveryStore {
    pub fn close_execution_canary_open_position(
        &self,
        token: &str,
        target_qty: f64,
        target_qty_exact: Option<TokenQuantity>,
        exit_price_sol: f64,
        dust_qty_epsilon: f64,
        closed_ts: DateTime<Utc>,
    ) -> Result<ExecutionCanaryPositionCloseResult> {
        validate_close_inputs(token, target_qty, exit_price_sol, dust_qty_epsilon)?;
        let target_qty_exact =
            reject_zero_raw_exact_qty(target_qty_exact, "execution canary close target qty")?;
        let mut result = self.with_immediate_transaction_retry(
            "execution canary open position close",
            |conn| {
                close_execution_canary_open_position_on_conn(
                    conn,
                    None,
                    token,
                    target_qty,
                    target_qty_exact,
                    exit_price_sol,
                    dust_qty_epsilon,
                    closed_ts,
                )
            },
        )?;
        if result.close_status == EXECUTION_CANARY_POSITION_CLOSE_PARTIAL {
            result.remaining_position = self.load_execution_canary_open_position(token)?;
        }
        Ok(result)
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn close_execution_canary_open_position_on_conn(
    conn: &Connection,
    order_id: Option<&str>,
    token: &str,
    target_qty: f64,
    target_qty_exact: Option<TokenQuantity>,
    exit_price_sol: f64,
    dust_qty_epsilon: f64,
    closed_ts: DateTime<Utc>,
) -> Result<ExecutionCanaryPositionCloseResult> {
    validate_close_inputs(token, target_qty, exit_price_sol, dust_qty_epsilon)?;
    let target_qty_exact =
        reject_zero_raw_exact_qty(target_qty_exact, "execution canary close target qty")?;
    if let Some(order_id) = order_id {
        if fill_exists(conn, order_id)? {
            return Ok(no_position_close_result(token));
        }
    }
    let Some(position) = load_open_position_for_close(conn, token)? else {
        insert_no_position_sell_fill_marker(
            conn,
            order_id,
            token,
            target_qty,
            target_qty_exact,
            exit_price_sol,
        )?;
        return Ok(no_position_close_result(token));
    };
    let plan = plan_position_close(
        &position,
        target_qty,
        target_qty_exact,
        exit_price_sol,
        dust_qty_epsilon,
    )?;
    apply_position_close(conn, &position, &plan, closed_ts)?;
    insert_position_sell_fill_marker(conn, order_id, token, &plan)?;
    Ok(plan.into_result(&position, token))
}

fn validate_close_inputs(
    token: &str,
    target_qty: f64,
    exit_price_sol: f64,
    dust_qty_epsilon: f64,
) -> Result<()> {
    if token.trim().is_empty() {
        return Err(anyhow!("execution canary close token must not be empty"));
    }
    if !target_qty.is_finite() || target_qty <= 0.0 {
        return Err(anyhow!(
            "invalid execution canary close target_qty={target_qty} (must be finite and > 0)"
        ));
    }
    if !exit_price_sol.is_finite() || exit_price_sol < 0.0 {
        return Err(anyhow!(
            "invalid execution canary close exit_price_sol={exit_price_sol}"
        ));
    }
    if !dust_qty_epsilon.is_finite() || dust_qty_epsilon < 0.0 {
        return Err(anyhow!(
            "invalid execution canary close dust_qty_epsilon={dust_qty_epsilon}"
        ));
    }
    Ok(())
}

fn insert_no_position_sell_fill_marker(
    conn: &Connection,
    order_id: Option<&str>,
    token: &str,
    target_qty: f64,
    target_qty_exact: Option<TokenQuantity>,
    exit_price_sol: f64,
) -> Result<()> {
    let Some(order_id) = order_id else {
        return Ok(());
    };
    let exit_value_sol = target_qty * exit_price_sol;
    let exit_value_lamports =
        sol_to_lamports_floor(exit_value_sol, "execution canary sell fill exit_value_sol")?;
    insert_fill_marker_if_order_exists(
        conn,
        order_id,
        token,
        target_qty,
        target_qty_exact,
        exit_value_sol,
        exit_value_lamports,
    )
}

fn insert_position_sell_fill_marker(
    conn: &Connection,
    order_id: Option<&str>,
    token: &str,
    plan: &PlannedPositionClose,
) -> Result<()> {
    let Some(order_id) = order_id else {
        return Ok(());
    };
    insert_fill_marker_if_order_exists(
        conn,
        order_id,
        token,
        plan.closed_qty,
        plan.closed_qty_exact,
        plan.exit_value_sol,
        plan.exit_value_lamports,
    )
}

fn load_open_position_for_close(
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
        .context("failed to prepare execution canary close position query")?;
    stmt.query_row(
        params![
            token,
            EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
            EXECUTION_CANARY_POSITION_STATE_OPEN,
        ],
        execution_canary_position_from_row,
    )
    .optional()
    .context("failed loading execution canary open position for close")
}

fn no_position_close_result(token: &str) -> ExecutionCanaryPositionCloseResult {
    ExecutionCanaryPositionCloseResult {
        close_status: EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION.to_string(),
        position_id: None,
        token: token.to_string(),
        closed_qty: 0.0,
        closed_qty_exact: None,
        remaining_qty: 0.0,
        remaining_qty_exact: None,
        entry_cost_sol: 0.0,
        exit_value_sol: 0.0,
        pnl_sol: 0.0,
        entry_cost_lamports: None,
        exit_value_lamports: None,
        pnl_lamports: None,
        remaining_position: None,
    }
}

struct PlannedPositionClose {
    close_status: &'static str,
    closed_qty: f64,
    closed_qty_exact: Option<TokenQuantity>,
    remaining_qty: f64,
    remaining_qty_exact: Option<TokenQuantity>,
    remaining_cost_sol: f64,
    remaining_cost_lamports: Lamports,
    entry_cost_sol: f64,
    exit_value_sol: f64,
    pnl_sol: f64,
    entry_cost_lamports: Lamports,
    exit_value_lamports: Lamports,
    pnl_lamports: SignedLamports,
}

impl PlannedPositionClose {
    fn into_result(
        self,
        position: &ExecutionCanaryOwnedPosition,
        token: &str,
    ) -> ExecutionCanaryPositionCloseResult {
        ExecutionCanaryPositionCloseResult {
            close_status: self.close_status.to_string(),
            position_id: Some(position.position_id.clone()),
            token: token.to_string(),
            closed_qty: self.closed_qty,
            closed_qty_exact: self.closed_qty_exact,
            remaining_qty: self.remaining_qty,
            remaining_qty_exact: self.remaining_qty_exact,
            entry_cost_sol: self.entry_cost_sol,
            exit_value_sol: self.exit_value_sol,
            pnl_sol: self.pnl_sol,
            entry_cost_lamports: Some(self.entry_cost_lamports),
            exit_value_lamports: Some(self.exit_value_lamports),
            pnl_lamports: Some(self.pnl_lamports),
            remaining_position: None,
        }
    }
}

fn plan_position_close(
    position: &ExecutionCanaryOwnedPosition,
    target_qty: f64,
    target_qty_exact: Option<TokenQuantity>,
    exit_price_sol: f64,
    dust_qty_epsilon: f64,
) -> Result<PlannedPositionClose> {
    if position.qty <= EXECUTION_CANARY_POSITION_CLOSE_EPS {
        return Err(anyhow!(
            "execution canary open position {} has non-positive qty={}",
            position.position_id,
            position.qty
        ));
    }
    let capped_qty = target_qty.min(position.qty);
    let projected_remaining = (position.qty - capped_qty).max(0.0);
    let dust_limit = dust_qty_epsilon.max(EXECUTION_CANARY_POSITION_CLOSE_EPS);
    let dust_close = (projected_remaining > 0.0 && projected_remaining <= dust_limit)
        || exact_raw_dust_remaining(position.qty_exact, target_qty_exact);
    let closing = projected_remaining <= EXECUTION_CANARY_POSITION_CLOSE_EPS || dust_close;
    let closed_qty = if closing { position.qty } else { capped_qty };
    let remaining_qty = if closing {
        0.0
    } else {
        (position.qty - closed_qty).max(0.0)
    };
    let closed_qty_exact = closed_qty_exact(position.qty_exact, target_qty_exact, closing)?;
    let remaining_qty_exact =
        merge_position_qty_exact_on_sell(position.qty_exact, closed_qty_exact, closing)?;
    let remaining_qty_exact = if closing {
        remaining_qty_exact
    } else {
        reject_zero_raw_exact_qty(remaining_qty_exact, "execution canary close remaining qty")?
    };
    let total_cost_lamports = position_cost_lamports(position)?;
    let entry_cost_sol = if closing {
        position.cost_sol
    } else {
        position.cost_sol * (closed_qty / position.qty)
    };
    let entry_cost_lamports = if closing {
        total_cost_lamports
    } else {
        sol_to_lamports_ceil(entry_cost_sol, "execution canary close entry_cost_sol")
            .map(|estimated| estimated.min(total_cost_lamports))?
    };
    let remaining_cost_lamports = total_cost_lamports
        .checked_sub(entry_cost_lamports)
        .ok_or_else(|| anyhow!("execution canary position cost underflow"))?;
    let remaining_cost_sol = if closing {
        0.0
    } else {
        (position.cost_sol - entry_cost_sol).max(0.0)
    };
    let exit_value_sol = closed_qty * exit_price_sol;
    let exit_value_lamports =
        sol_to_lamports_floor(exit_value_sol, "execution canary close exit_value_sol")?;
    let pnl_lamports = SignedLamports::new(
        i128::from(exit_value_lamports.as_u64()) - i128::from(entry_cost_lamports.as_u64()),
    );
    let close_status = if dust_close || exact_raw_position_dust(position.qty_exact) {
        EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED
    } else if closing {
        EXECUTION_CANARY_POSITION_CLOSE_CLOSED
    } else {
        EXECUTION_CANARY_POSITION_CLOSE_PARTIAL
    };
    Ok(PlannedPositionClose {
        close_status,
        closed_qty,
        closed_qty_exact,
        remaining_qty,
        remaining_qty_exact,
        remaining_cost_sol,
        remaining_cost_lamports,
        entry_cost_sol,
        exit_value_sol,
        pnl_sol: signed_lamports_to_sol(pnl_lamports),
        entry_cost_lamports,
        exit_value_lamports,
        pnl_lamports,
    })
}

fn exact_raw_dust_remaining(current: Option<TokenQuantity>, target: Option<TokenQuantity>) -> bool {
    let (Some(current), Some(target)) = (current, target) else {
        return false;
    };
    if current.decimals() != target.decimals() || current.decimals() == 0 {
        return false;
    }
    let closed_raw = target.raw().min(current.raw());
    let remaining_raw = current.raw().saturating_sub(closed_raw);
    remaining_raw > 0 && remaining_raw <= EXECUTION_CANARY_POSITION_RAW_DUST_UNITS
}

fn exact_raw_position_dust(qty: Option<TokenQuantity>) -> bool {
    qty.is_some_and(|qty| {
        qty.decimals() > 0 && qty.raw() > 0 && qty.raw() <= EXECUTION_CANARY_POSITION_RAW_DUST_UNITS
    })
}

fn closed_qty_exact(
    current: Option<TokenQuantity>,
    target: Option<TokenQuantity>,
    closing: bool,
) -> Result<Option<TokenQuantity>> {
    match (current, target) {
        (Some(current), _) if closing => Ok(Some(current)),
        (Some(current), Some(target)) if current.decimals() == target.decimals() => {
            let raw = current.raw().min(target.raw());
            Ok((raw > 0).then(|| TokenQuantity::new(raw, current.decimals())))
        }
        (Some(_), Some(_)) => Err(anyhow!(
            "execution canary close exact qty decimals mismatch"
        )),
        _ => Ok(None),
    }
}

fn position_cost_lamports(position: &ExecutionCanaryOwnedPosition) -> Result<Lamports> {
    match position.cost_lamports {
        Some(value) => Ok(value),
        None => sol_to_lamports_ceil(position.cost_sol, "execution canary position cost_sol"),
    }
}

fn apply_position_close(
    conn: &Connection,
    position: &ExecutionCanaryOwnedPosition,
    plan: &PlannedPositionClose,
    closed_ts: DateTime<Utc>,
) -> Result<()> {
    let closing = plan.close_status == EXECUTION_CANARY_POSITION_CLOSE_CLOSED
        || plan.close_status == EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED;
    let next_state = if closing {
        EXECUTION_CANARY_POSITION_STATE_CLOSED
    } else {
        EXECUTION_CANARY_POSITION_STATE_OPEN
    };
    let closed_ts = closing.then(|| closed_ts.to_rfc3339());
    let rows = conn
        .execute(
            "UPDATE positions
         SET qty = ?2,
             qty_raw = ?3,
             qty_decimals = ?4,
             cost_sol = ?5,
             cost_lamports = ?6,
             state = ?7,
             closed_ts = COALESCE(?8, closed_ts),
             pnl_sol = COALESCE(pnl_sol, 0.0) + ?9,
             pnl_lamports = COALESCE(pnl_lamports, 0) + ?10
         WHERE position_id = ?1
           AND accounting_bucket = ?11
           AND state = ?12",
            params![
                position.position_id.as_str(),
                plan.remaining_qty,
                plan.remaining_qty_exact
                    .as_ref()
                    .map(|value| value.raw().to_string()),
                plan.remaining_qty_exact
                    .as_ref()
                    .map(|value| i64::from(value.decimals())),
                plan.remaining_cost_sol,
                u64_to_sql_i64(
                    "positions.cost_lamports",
                    plan.remaining_cost_lamports.as_u64()
                )?,
                next_state,
                closed_ts,
                plan.pnl_sol,
                signed_lamports_to_sql_i64("positions.pnl_lamports", plan.pnl_lamports)?,
                EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                EXECUTION_CANARY_POSITION_STATE_OPEN,
            ],
        )
        .context("failed updating execution canary position close")?;
    if rows != 1 {
        return Err(anyhow!(
            "execution canary position close updated {rows} rows for {}",
            position.position_id
        ));
    }
    Ok(())
}
