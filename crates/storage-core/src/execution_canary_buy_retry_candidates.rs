use crate::{
    execution_canary_rows::execution_canary_order_from_row, ExecutionCanaryOrder,
    SqliteDiscoveryStore, EXECUTION_SIMULATION_STATUS_NOT_RUN, EXECUTION_STATUS_CANARY_CANDIDATE,
};
use anyhow::{Context, Result};
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub fn list_retry_candidate_buy_execution_canary_orders_for_route(
        &self,
        route: &str,
        retry_reason: &str,
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
                   AND orders.simulation_status = ?3
                   AND orders.simulation_error = ?4
                   AND (orders.tx_signature IS NULL OR TRIM(orders.tx_signature) = '')
                   AND lower(copy_signals.side) = 'buy'
                 ORDER BY orders.submit_ts DESC, orders.order_id DESC
                 LIMIT ?5",
            )
            .context("failed to prepare retry candidate buy order query")?;
        let rows = stmt
            .query_map(
                params![
                    route,
                    EXECUTION_STATUS_CANARY_CANDIDATE,
                    EXECUTION_SIMULATION_STATUS_NOT_RUN,
                    retry_reason,
                    i64::from(limit.max(1)),
                ],
                execution_canary_order_from_row,
            )
            .context("failed querying retry candidate buy orders")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading retry candidate buy orders")
    }
}
