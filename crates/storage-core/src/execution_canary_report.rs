use crate::{
    execution_canary_rows::execution_canary_order_from_row, ExecutionCanaryStatusReport,
    SqliteDiscoveryStore, EXECUTION_STATUS_CANARY_BUILT, EXECUTION_STATUS_CANARY_CANDIDATE,
    EXECUTION_STATUS_CANARY_CONFIRMED, EXECUTION_STATUS_CANARY_EXPIRED,
    EXECUTION_STATUS_CANARY_FAILED, EXECUTION_STATUS_CANARY_SIMULATED,
    EXECUTION_STATUS_CANARY_SUBMITTED, EXECUTION_STATUS_CANARY_SUBMIT_DISABLED,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
    pub fn execution_canary_status_report(
        &self,
        as_of: DateTime<Utc>,
    ) -> Result<ExecutionCanaryStatusReport> {
        let mut report = ExecutionCanaryStatusReport {
            as_of,
            total: 0,
            candidate: 0,
            built: 0,
            simulated: 0,
            submitted: 0,
            confirmed: 0,
            failed: 0,
            expired: 0,
            submit_disabled: 0,
            other: 0,
            latest_order: None,
            latest_error_order: None,
            latest_build_plan_metadata: None,
        };

        let mut stmt = self
            .conn
            .prepare(
                "SELECT status, COUNT(*)
                 FROM orders
                 WHERE order_id LIKE 'exec-canary:%'
                 GROUP BY status",
            )
            .context("failed to prepare execution canary status count query")?;
        let mut rows = stmt
            .query([])
            .context("failed querying execution canary status counts")?;
        while let Some(row) = rows
            .next()
            .context("failed iterating execution canary status counts")?
        {
            let status: String = row.get(0).context("failed reading canary status")?;
            let count_raw: i64 = row.get(1).context("failed reading canary status count")?;
            let count = u64::try_from(count_raw)
                .context("invalid negative execution canary status count")?;
            report.total += count;
            match status.as_str() {
                EXECUTION_STATUS_CANARY_CANDIDATE => report.candidate += count,
                EXECUTION_STATUS_CANARY_BUILT => report.built += count,
                EXECUTION_STATUS_CANARY_SIMULATED => report.simulated += count,
                EXECUTION_STATUS_CANARY_SUBMITTED => report.submitted += count,
                EXECUTION_STATUS_CANARY_CONFIRMED => report.confirmed += count,
                EXECUTION_STATUS_CANARY_FAILED => report.failed += count,
                EXECUTION_STATUS_CANARY_EXPIRED => report.expired += count,
                EXECUTION_STATUS_CANARY_SUBMIT_DISABLED => report.submit_disabled += count,
                _ => report.other += count,
            }
        }

        report.latest_order = self.latest_execution_canary_order()?;
        report.latest_error_order = self.latest_execution_canary_error_order()?;
        report.latest_build_plan_metadata = self.latest_execution_canary_build_plan_metadata()?;
        Ok(report)
    }

    fn latest_execution_canary_order(&self) -> Result<Option<crate::ExecutionCanaryOrder>> {
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
                 ORDER BY submit_ts DESC, order_id DESC
                 LIMIT 1",
            )
            .context("failed to prepare latest execution canary order query")?;
        stmt.query_row([], execution_canary_order_from_row)
            .optional()
            .context("failed querying latest execution canary order")
    }

    fn latest_execution_canary_error_order(&self) -> Result<Option<crate::ExecutionCanaryOrder>> {
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
                   AND (err_code IS NOT NULL OR simulation_error IS NOT NULL)
                 ORDER BY COALESCE(confirm_ts, submit_ts) DESC, order_id DESC
                 LIMIT 1",
            )
            .context("failed to prepare latest execution canary error order query")?;
        stmt.query_row(params![], execution_canary_order_from_row)
            .optional()
            .context("failed querying latest execution canary error order")
    }
}
