use crate::{
    execution_canary_rows::execution_canary_order_from_row, ExecutionCanaryOrder,
    SqliteDiscoveryStore, EXECUTION_STATUS_CANARY_SIMULATED, EXECUTION_STATUS_CANARY_SUBMITTED,
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
}
