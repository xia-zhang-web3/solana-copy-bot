use crate::{
    execution_canary_rows::execution_canary_order_from_row, ExecutionCanaryOrder,
    SqliteDiscoveryStore, EXECUTION_SIMULATION_STATUS_NOT_RUN, EXECUTION_STATUS_CANARY_CANDIDATE,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub fn list_stale_not_run_execution_canary_candidates_for_route(
        &self,
        route: &str,
        submitted_before: DateTime<Utc>,
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
                 LEFT JOIN execution_canary_build_plan_metadata AS metadata
                   ON metadata.order_id = orders.order_id
                 WHERE orders.order_id LIKE 'exec-canary:%'
                   AND orders.route = ?1
                   AND orders.status = ?2
                   AND orders.simulation_status = ?3
                   AND (orders.simulation_error IS NULL OR TRIM(orders.simulation_error) = '')
                   AND (orders.tx_signature IS NULL OR TRIM(orders.tx_signature) = '')
                   AND metadata.order_id IS NULL
                   AND orders.submit_ts <= ?4
                 ORDER BY orders.submit_ts ASC, orders.order_id ASC
                 LIMIT ?5",
            )
            .context("failed to prepare stale not-run canary candidate query")?;
        let rows = stmt
            .query_map(
                params![
                    route,
                    EXECUTION_STATUS_CANARY_CANDIDATE,
                    EXECUTION_SIMULATION_STATUS_NOT_RUN,
                    submitted_before.to_rfc3339(),
                    i64::from(limit.max(1)),
                ],
                execution_canary_order_from_row,
            )
            .context("failed querying stale not-run canary candidates")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading stale not-run canary candidates")
    }
}
