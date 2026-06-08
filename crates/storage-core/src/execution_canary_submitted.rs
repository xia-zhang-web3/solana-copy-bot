use crate::{
    execution_canary_rows::execution_canary_order_from_row, ExecutionCanaryOrder,
    SqliteDiscoveryStore, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
    EXECUTION_CANARY_POSITION_STATE_OPEN, EXECUTION_ERROR_SIMULATION_FAILED,
    EXECUTION_STATUS_CANARY_FAILED, EXECUTION_STATUS_CANARY_SIMULATED,
    EXECUTION_STATUS_CANARY_SUBMITTED,
};
use anyhow::{Context, Result};
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub fn list_reconcilable_execution_canary_orders_for_route(
        &self,
        route: &str,
        retry_reason: &str,
        limit: u32,
    ) -> Result<Vec<ExecutionCanaryOrder>> {
        let mut stmt = self
            .conn
            .prepare(
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
                 WHERE order_id LIKE 'exec-canary:%'
                   AND route = ?1
                   AND (
                       status = ?2
                       OR (
                           status = ?3
                           AND (tx_signature IS NULL OR TRIM(tx_signature) = '')
                           AND simulation_error = ?4
                       )
                   )
                 ORDER BY submit_ts ASC, order_id ASC
                 LIMIT ?5",
            )
            .context("failed to prepare submitted execution canary order query")?;
        let rows = stmt
            .query_map(
                params![
                    route,
                    EXECUTION_STATUS_CANARY_SUBMITTED,
                    EXECUTION_STATUS_CANARY_SIMULATED,
                    retry_reason,
                    i64::from(limit),
                ],
                execution_canary_order_from_row,
            )
            .context("failed querying submitted execution canary orders")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading submitted execution canary orders")
    }

    pub fn list_failed_simulation_sell_execution_canary_orders_for_route(
        &self,
        route: &str,
        limit: u32,
    ) -> Result<Vec<ExecutionCanaryOrder>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    orders.order_id,
                    orders.signal_id,
                    orders.route,
                    orders.submit_ts,
                    orders.confirm_ts,
                    orders.status,
                    orders.err_code,
                    orders.client_order_id,
                    orders.tx_signature,
                    orders.simulation_status,
                    orders.simulation_error,
                    orders.attempt
                 FROM orders
                 JOIN copy_signals ON copy_signals.signal_id = orders.signal_id
                 WHERE orders.order_id LIKE 'exec-canary:%'
                   AND orders.route = ?1
                   AND orders.status = ?2
                   AND orders.err_code = ?3
                   AND (orders.tx_signature IS NULL OR TRIM(orders.tx_signature) = '')
                   AND lower(copy_signals.side) = 'sell'
                   AND EXISTS (
                        SELECT 1
                        FROM positions AS pos
                        WHERE pos.token = copy_signals.token
                          AND pos.accounting_bucket = ?4
                          AND pos.state = ?5
                   )
                 ORDER BY orders.submit_ts ASC, orders.order_id ASC
                 LIMIT ?6",
            )
            .context("failed to prepare failed simulation sell order query")?;
        let rows = stmt
            .query_map(
                params![
                    route,
                    EXECUTION_STATUS_CANARY_FAILED,
                    EXECUTION_ERROR_SIMULATION_FAILED,
                    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                    EXECUTION_CANARY_POSITION_STATE_OPEN,
                    i64::from(limit.max(1)),
                ],
                execution_canary_order_from_row,
            )
            .context("failed querying failed simulation sell orders")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading failed simulation sell orders")
    }
}
