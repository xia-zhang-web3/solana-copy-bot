use crate::{
    execution_canary_rows::execution_canary_order_from_row, ExecutionCanaryOrder,
    ExecutionCanaryRecordOutcome, ExecutionCanaryReserveResult, SqliteDiscoveryStore,
    EXECUTION_ERROR_EXPIRED, EXECUTION_ERROR_SUBMIT_DISABLED,
    EXECUTION_SIMULATION_STATUS_SKIPPED_NO_SUBMIT, EXECUTION_STATUS_CANARY_BUILT,
    EXECUTION_STATUS_CANARY_CANDIDATE, EXECUTION_STATUS_CANARY_CONFIRMED,
    EXECUTION_STATUS_CANARY_EXPIRED, EXECUTION_STATUS_CANARY_FAILED,
    EXECUTION_STATUS_CANARY_SIMULATED, EXECUTION_STATUS_CANARY_SUBMITTED,
    EXECUTION_STATUS_CANARY_SUBMIT_DISABLED,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
    pub fn reserve_execution_canary_order(
        &self,
        signal_id: &str,
        route: &str,
        now: DateTime<Utc>,
    ) -> Result<ExecutionCanaryReserveResult> {
        let order_id = execution_canary_order_id(signal_id);
        let client_order_id = execution_canary_client_order_id(signal_id);
        let now_rfc3339 = now.to_rfc3339();
        let inserted =
            self.with_immediate_transaction_retry("execution canary order reserve", |conn| {
                let existing: Option<String> = conn
                    .query_row(
                        "SELECT order_id FROM orders WHERE order_id = ?1 LIMIT 1",
                        params![order_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .context("failed checking existing execution canary order")?;
                if existing.is_some() {
                    return Ok(false);
                }

                conn.execute(
                    "INSERT INTO orders(
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
                    ) VALUES (?1, ?2, ?3, ?4, NULL, ?5, NULL, ?6, NULL, ?7, NULL, 1)",
                    params![
                        order_id,
                        signal_id,
                        route,
                        now_rfc3339,
                        EXECUTION_STATUS_CANARY_CANDIDATE,
                        client_order_id,
                        crate::EXECUTION_SIMULATION_STATUS_NOT_RUN,
                    ],
                )
                .context("failed inserting execution canary order")?;
                Ok(true)
            })?;
        let order = self
            .load_execution_canary_order(&order_id)?
            .ok_or_else(|| anyhow!("missing reserved execution canary order for {signal_id}"))?;
        let outcome = if inserted {
            ExecutionCanaryRecordOutcome::Inserted
        } else {
            ExecutionCanaryRecordOutcome::Existing
        };
        Ok(ExecutionCanaryReserveResult { outcome, order })
    }

    pub fn load_execution_canary_order_by_signal(
        &self,
        signal_id: &str,
    ) -> Result<Option<ExecutionCanaryOrder>> {
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
                 WHERE order_id = ?1
                 LIMIT 1",
            )
            .context("failed to prepare execution canary order by signal query")?;
        stmt.query_row(
            params![execution_canary_order_id(signal_id)],
            execution_canary_order_from_row,
        )
        .optional()
        .context("failed querying execution canary order by signal")
    }

    pub fn load_execution_canary_order(
        &self,
        order_id: &str,
    ) -> Result<Option<ExecutionCanaryOrder>> {
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
                 WHERE order_id = ?1
                 LIMIT 1",
            )
            .context("failed to prepare execution canary order query")?;
        stmt.query_row(params![order_id], execution_canary_order_from_row)
            .optional()
            .context("failed querying execution canary order")
    }

    pub fn mark_execution_canary_built(
        &self,
        order_id: &str,
        now: DateTime<Utc>,
    ) -> Result<ExecutionCanaryOrder> {
        self.transition_execution_canary_order(
            order_id,
            &[EXECUTION_STATUS_CANARY_CANDIDATE],
            EXECUTION_STATUS_CANARY_BUILT,
            now,
            None,
            None,
            None,
            None,
        )
    }

    pub fn mark_execution_canary_simulated(
        &self,
        order_id: &str,
        now: DateTime<Utc>,
        simulation_status: &str,
        simulation_error: Option<&str>,
    ) -> Result<ExecutionCanaryOrder> {
        self.transition_execution_canary_order(
            order_id,
            &[EXECUTION_STATUS_CANARY_BUILT],
            EXECUTION_STATUS_CANARY_SIMULATED,
            now,
            Some(simulation_status),
            simulation_error,
            None,
            None,
        )
    }

    pub fn mark_execution_canary_submitted(
        &self,
        order_id: &str,
        now: DateTime<Utc>,
        tx_signature: &str,
    ) -> Result<ExecutionCanaryOrder> {
        if tx_signature.trim().is_empty() {
            return Err(anyhow!("execution canary tx_signature must be non-empty"));
        }
        self.transition_execution_canary_order(
            order_id,
            &[EXECUTION_STATUS_CANARY_SIMULATED],
            EXECUTION_STATUS_CANARY_SUBMITTED,
            now,
            None,
            None,
            None,
            Some(tx_signature),
        )
    }

    pub fn mark_execution_canary_confirmed(
        &self,
        order_id: &str,
        now: DateTime<Utc>,
    ) -> Result<ExecutionCanaryOrder> {
        self.transition_execution_canary_order(
            order_id,
            &[EXECUTION_STATUS_CANARY_SUBMITTED],
            EXECUTION_STATUS_CANARY_CONFIRMED,
            now,
            None,
            None,
            None,
            None,
        )
    }

    pub fn mark_execution_canary_failed(
        &self,
        order_id: &str,
        now: DateTime<Utc>,
        error_code: &str,
        error: &str,
    ) -> Result<ExecutionCanaryOrder> {
        if error_code.trim().is_empty() {
            return Err(anyhow!(
                "execution canary failure error_code must be non-empty"
            ));
        }
        self.transition_execution_canary_order(
            order_id,
            &[
                EXECUTION_STATUS_CANARY_CANDIDATE,
                EXECUTION_STATUS_CANARY_BUILT,
                EXECUTION_STATUS_CANARY_SIMULATED,
                EXECUTION_STATUS_CANARY_SUBMITTED,
            ],
            EXECUTION_STATUS_CANARY_FAILED,
            now,
            None,
            Some(error),
            Some(error_code),
            None,
        )
    }

    pub fn mark_execution_canary_expired(
        &self,
        order_id: &str,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<ExecutionCanaryOrder> {
        self.transition_execution_canary_order(
            order_id,
            &[
                EXECUTION_STATUS_CANARY_CANDIDATE,
                EXECUTION_STATUS_CANARY_BUILT,
                EXECUTION_STATUS_CANARY_SIMULATED,
                EXECUTION_STATUS_CANARY_SUBMITTED,
            ],
            EXECUTION_STATUS_CANARY_EXPIRED,
            now,
            None,
            Some(reason),
            Some(EXECUTION_ERROR_EXPIRED),
            None,
        )
    }

    pub fn mark_execution_canary_submit_disabled(
        &self,
        order_id: &str,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<ExecutionCanaryOrder> {
        self.transition_execution_canary_order(
            order_id,
            &[
                EXECUTION_STATUS_CANARY_CANDIDATE,
                EXECUTION_STATUS_CANARY_SIMULATED,
            ],
            EXECUTION_STATUS_CANARY_SUBMIT_DISABLED,
            now,
            Some(EXECUTION_SIMULATION_STATUS_SKIPPED_NO_SUBMIT),
            Some(reason),
            Some(EXECUTION_ERROR_SUBMIT_DISABLED),
            None,
        )
    }

    fn transition_execution_canary_order(
        &self,
        order_id: &str,
        allowed_from: &[&str],
        next_status: &str,
        now: DateTime<Utc>,
        simulation_status: Option<&str>,
        simulation_error: Option<&str>,
        err_code: Option<&str>,
        tx_signature: Option<&str>,
    ) -> Result<ExecutionCanaryOrder> {
        let now_rfc3339 = now.to_rfc3339();
        let changed =
            self.with_immediate_transaction_retry("execution canary order transition", |conn| {
                let current: Option<String> = conn
                    .query_row(
                        "SELECT status FROM orders WHERE order_id = ?1 LIMIT 1",
                        params![order_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .context("failed loading current execution canary status")?;
                let Some(current) = current else {
                    return Err(anyhow!("missing execution canary order {order_id}"));
                };
                if !allowed_from.iter().any(|status| *status == current) {
                    return Err(anyhow!(
                        "invalid execution canary transition for {order_id}: {current} -> {next_status}"
                    ));
                }
                let confirm_ts = match next_status {
                    EXECUTION_STATUS_CANARY_CONFIRMED
                    | EXECUTION_STATUS_CANARY_EXPIRED
                    | EXECUTION_STATUS_CANARY_FAILED
                    | EXECUTION_STATUS_CANARY_SUBMIT_DISABLED => Some(now_rfc3339.as_str()),
                    _ => None,
                };
                let submit_ts = if next_status == EXECUTION_STATUS_CANARY_SUBMITTED {
                    Some(now_rfc3339.as_str())
                } else {
                    None
                };
                let simulation_status = if next_status == EXECUTION_STATUS_CANARY_SUBMIT_DISABLED
                    && current != EXECUTION_STATUS_CANARY_CANDIDATE
                {
                    None
                } else {
                    simulation_status
                };
                let simulation_error = if next_status == EXECUTION_STATUS_CANARY_SUBMIT_DISABLED
                    && current != EXECUTION_STATUS_CANARY_CANDIDATE
                {
                    None
                } else {
                    simulation_error
                };
                conn.execute(
                    "UPDATE orders
                     SET status = ?2,
                         confirm_ts = COALESCE(?3, confirm_ts),
                         err_code = COALESCE(?4, err_code),
                         simulation_status = COALESCE(?5, simulation_status),
                         simulation_error = COALESCE(?6, simulation_error),
                         tx_signature = COALESCE(?7, tx_signature),
                         submit_ts = COALESCE(?8, submit_ts)
                     WHERE order_id = ?1",
                    params![
                        order_id,
                        next_status,
                        confirm_ts,
                        err_code,
                        simulation_status,
                        simulation_error,
                        tx_signature,
                        submit_ts,
                    ],
                )
                .context("failed updating execution canary order status")?;
                Ok(true)
            })?;
        if !changed {
            return Err(anyhow!(
                "execution canary transition did not update {order_id}"
            ));
        }
        self.load_execution_canary_order(order_id)?
            .ok_or_else(|| anyhow!("missing execution canary order after transition {order_id}"))
    }
}

pub fn execution_canary_order_id(signal_id: &str) -> String {
    format!("exec-canary:{signal_id}")
}

pub fn execution_canary_client_order_id(signal_id: &str) -> String {
    format!("copybot:canary:{signal_id}")
}
