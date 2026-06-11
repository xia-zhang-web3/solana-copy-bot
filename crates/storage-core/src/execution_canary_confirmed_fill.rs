use crate::{
    execution_canary_position_close::close_execution_canary_open_position_on_conn,
    execution_canary_position_open::{
        execution_canary_position_id, record_execution_canary_open_position_on_conn,
    },
    execution_canary_rows::execution_canary_order_from_row,
    money::sol_to_lamports_ceil,
    ExecutionCanaryOrder, ExecutionCanaryOwnedPositionRecordResult,
    ExecutionCanaryPositionCloseResult, SqliteDiscoveryStore, EXECUTION_STATUS_CANARY_CONFIRMED,
    EXECUTION_STATUS_CANARY_SUBMITTED,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::TokenQuantity;
use rusqlite::{params, Connection, OptionalExtension};

impl SqliteDiscoveryStore {
    pub fn record_execution_canary_confirmed_buy_fill(
        &self,
        order_id: &str,
        token: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        cost_sol: f64,
        fill_ts: DateTime<Utc>,
    ) -> Result<ExecutionCanaryOwnedPositionRecordResult> {
        let order = self.load_confirmed_canary_fill_order(order_id, "buy")?;
        self.record_execution_canary_open_position(
            &order.order_id,
            token,
            qty,
            qty_exact,
            cost_sol,
            fill_ts,
        )
    }

    pub fn confirm_execution_canary_buy_fill(
        &self,
        order_id: &str,
        token: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        cost_sol: f64,
        fill_ts: DateTime<Utc>,
        confirmed_at: DateTime<Utc>,
    ) -> Result<(
        ExecutionCanaryOrder,
        ExecutionCanaryOwnedPositionRecordResult,
    )> {
        self.with_immediate_transaction_retry("execution canary buy fill confirmation", |conn| {
            let order = load_canary_fill_order(
                conn,
                order_id,
                "buy",
                &[
                    EXECUTION_STATUS_CANARY_SUBMITTED,
                    EXECUTION_STATUS_CANARY_CONFIRMED,
                ],
            )?;
            let position_id = execution_canary_position_id(&order.order_id);
            let cost_lamports =
                sol_to_lamports_ceil(cost_sol, "execution canary position cost_sol")?;
            let result = record_execution_canary_open_position_on_conn(
                conn,
                &order.order_id,
                token,
                qty,
                qty_exact,
                cost_sol,
                cost_lamports,
                &position_id,
                &fill_ts.to_rfc3339(),
            )?;
            confirm_order_if_submitted(conn, &order, confirmed_at)?;
            let order = load_canary_fill_order(
                conn,
                order_id,
                "buy",
                &[EXECUTION_STATUS_CANARY_CONFIRMED],
            )?;
            Ok((order, result))
        })
    }

    pub fn close_execution_canary_confirmed_sell_fill(
        &self,
        order_id: &str,
        token: &str,
        target_qty: f64,
        target_qty_exact: Option<TokenQuantity>,
        exit_price_sol: f64,
        dust_qty_epsilon: f64,
        fill_ts: DateTime<Utc>,
    ) -> Result<ExecutionCanaryPositionCloseResult> {
        self.load_confirmed_canary_fill_order(order_id, "sell")?;
        self.with_immediate_transaction_retry("execution canary confirmed sell fill", |conn| {
            close_execution_canary_open_position_on_conn(
                conn,
                Some(order_id),
                token,
                target_qty,
                target_qty_exact,
                exit_price_sol,
                dust_qty_epsilon,
                fill_ts,
            )
        })
    }

    pub fn confirm_execution_canary_sell_fill(
        &self,
        order_id: &str,
        token: &str,
        target_qty: f64,
        target_qty_exact: Option<TokenQuantity>,
        exit_price_sol: f64,
        dust_qty_epsilon: f64,
        fill_ts: DateTime<Utc>,
        confirmed_at: DateTime<Utc>,
    ) -> Result<(ExecutionCanaryOrder, ExecutionCanaryPositionCloseResult)> {
        self.with_immediate_transaction_retry("execution canary sell fill confirmation", |conn| {
            let order = load_canary_fill_order(
                conn,
                order_id,
                "sell",
                &[
                    EXECUTION_STATUS_CANARY_SUBMITTED,
                    EXECUTION_STATUS_CANARY_CONFIRMED,
                ],
            )?;
            let result = close_execution_canary_open_position_on_conn(
                conn,
                Some(&order.order_id),
                token,
                target_qty,
                target_qty_exact,
                exit_price_sol,
                dust_qty_epsilon,
                fill_ts,
            )?;
            confirm_order_if_submitted(conn, &order, confirmed_at)?;
            let order = load_canary_fill_order(
                conn,
                order_id,
                "sell",
                &[EXECUTION_STATUS_CANARY_CONFIRMED],
            )?;
            Ok((order, result))
        })
    }

    fn load_confirmed_canary_fill_order(
        &self,
        order_id: &str,
        expected_side: &str,
    ) -> Result<ExecutionCanaryOrder> {
        let order = self
            .load_execution_canary_order(order_id)?
            .ok_or_else(|| anyhow!("missing execution canary fill order {order_id}"))?;
        if order.status != EXECUTION_STATUS_CANARY_CONFIRMED {
            return Err(anyhow!(
                "execution canary fill order {order_id} requires confirmed status, got {}",
                order.status
            ));
        }
        if order
            .tx_signature
            .as_deref()
            .map_or(true, |signature| signature.trim().is_empty())
        {
            return Err(anyhow!(
                "execution canary fill order {order_id} requires confirmed tx_signature"
            ));
        }
        let signal = self
            .load_copy_signal_by_signal_id(&order.signal_id)?
            .ok_or_else(|| anyhow!("missing copy signal for execution canary order {order_id}"))?;
        if !signal.side.eq_ignore_ascii_case(expected_side) {
            return Err(anyhow!(
                "execution canary fill order {order_id} expected {expected_side} signal, got {}",
                signal.side
            ));
        }
        Ok(order)
    }
}

fn load_canary_fill_order(
    conn: &Connection,
    order_id: &str,
    expected_side: &str,
    allowed_statuses: &[&str],
) -> Result<ExecutionCanaryOrder> {
    let order = conn
        .query_row(
            "SELECT
                order_id,
                signal_id,
                route,
                submit_ts,
                confirm_ts,
                status,
                err_code,
                client_order_id,
                tx_signature,
                simulation_status,
                simulation_error,
                attempt
             FROM orders
             WHERE order_id = ?1
             LIMIT 1",
            params![order_id],
            execution_canary_order_from_row,
        )
        .optional()
        .context("failed loading execution canary fill order")?
        .ok_or_else(|| anyhow!("missing execution canary fill order {order_id}"))?;
    if !allowed_statuses
        .iter()
        .any(|status| *status == order.status)
    {
        return Err(anyhow!(
            "execution canary fill order {order_id} requires status in {:?}, got {}",
            allowed_statuses,
            order.status
        ));
    }
    validate_canary_fill_order(conn, &order, expected_side)?;
    Ok(order)
}

fn validate_canary_fill_order(
    conn: &Connection,
    order: &ExecutionCanaryOrder,
    expected_side: &str,
) -> Result<()> {
    if order
        .tx_signature
        .as_deref()
        .map_or(true, |signature| signature.trim().is_empty())
    {
        return Err(anyhow!(
            "execution canary fill order {} requires confirmed tx_signature",
            order.order_id
        ));
    }
    let signal_side: String = conn
        .query_row(
            "SELECT side FROM copy_signals WHERE signal_id = ?1 LIMIT 1",
            params![order.signal_id],
            |row| row.get(0),
        )
        .optional()
        .context("failed loading copy signal for execution canary fill order")?
        .ok_or_else(|| {
            anyhow!(
                "missing copy signal for execution canary order {}",
                order.order_id
            )
        })?;
    if !signal_side.eq_ignore_ascii_case(expected_side) {
        return Err(anyhow!(
            "execution canary fill order {} expected {expected_side} signal, got {signal_side}",
            order.order_id
        ));
    }
    Ok(())
}

fn confirm_order_if_submitted(
    conn: &Connection,
    order: &ExecutionCanaryOrder,
    confirmed_at: DateTime<Utc>,
) -> Result<()> {
    if order.status == EXECUTION_STATUS_CANARY_CONFIRMED {
        return Ok(());
    }
    if order.status != EXECUTION_STATUS_CANARY_SUBMITTED {
        return Err(anyhow!(
            "execution canary fill order {} cannot confirm from {}",
            order.order_id,
            order.status
        ));
    }
    let rows = conn
        .execute(
            "UPDATE orders
             SET status = ?2,
                 confirm_ts = COALESCE(confirm_ts, ?3)
             WHERE order_id = ?1
               AND status = ?4",
            params![
                order.order_id,
                EXECUTION_STATUS_CANARY_CONFIRMED,
                confirmed_at.to_rfc3339(),
                EXECUTION_STATUS_CANARY_SUBMITTED,
            ],
        )
        .context("failed confirming execution canary fill order")?;
    if rows != 1 {
        return Err(anyhow!(
            "execution canary fill confirmation updated {rows} rows for {}",
            order.order_id
        ));
    }
    Ok(())
}
