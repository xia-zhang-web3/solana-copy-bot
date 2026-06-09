use crate::{
    ExecutionCanaryConfirmTimeoutDecision, ExecutionCanaryOrder, SqliteDiscoveryStore,
    EXECUTION_CANARY_CONFIRM_DECISION_EXPIRE_UNSAFE,
    EXECUTION_CANARY_CONFIRM_DECISION_NOT_SUBMITTED, EXECUTION_CANARY_CONFIRM_DECISION_RETRY,
    EXECUTION_CANARY_CONFIRM_DECISION_WAIT, EXECUTION_ERROR_BUILD_FAILED,
    EXECUTION_ERROR_SIMULATION_FAILED, EXECUTION_SIMULATION_STATUS_NOT_RUN,
    EXECUTION_STATUS_CANARY_CANDIDATE, EXECUTION_STATUS_CANARY_FAILED,
    EXECUTION_STATUS_CANARY_SIMULATED, EXECUTION_STATUS_CANARY_SUBMITTED,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
    pub fn mark_execution_canary_submitted_unknown(
        &self,
        order_id: &str,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<ExecutionCanaryOrder> {
        validate_reason(reason, "execution canary submitted-unknown reason")?;
        let now_rfc3339 = now.to_rfc3339();
        self.with_immediate_transaction_retry("execution canary submit-unknown mark", |conn| {
            let current: Option<String> = conn
                .query_row(
                    "SELECT status
                     FROM orders
                     WHERE order_id = ?1
                     LIMIT 1",
                    params![order_id],
                    |row| row.get(0),
                )
                .optional()
                .context("failed loading current execution canary status")?;
            let Some(current) = current else {
                return Err(anyhow!("missing execution canary order {order_id}"));
            };
            if current != EXECUTION_STATUS_CANARY_SIMULATED {
                return Err(anyhow!(
                    "invalid execution canary submitted-unknown transition for {order_id}: {current} -> {EXECUTION_STATUS_CANARY_SUBMITTED}"
                ));
            }
            conn.execute(
                "UPDATE orders
                 SET status = ?2,
                     submit_ts = ?3,
                     simulation_error = ?4,
                     tx_signature = NULL
                 WHERE order_id = ?1",
                params![
                    order_id,
                    EXECUTION_STATUS_CANARY_SUBMITTED,
                    now_rfc3339,
                    reason,
                ],
            )
            .context("failed marking execution canary submitted without signature")?;
            Ok(())
        })?;
        self.load_execution_canary_order(order_id)?
            .ok_or_else(|| anyhow!("missing execution canary order after submit-unknown mark"))
    }

    pub fn execution_canary_confirm_timeout_decision(
        &self,
        order_id: &str,
        now: DateTime<Utc>,
        timeout: Duration,
    ) -> Result<ExecutionCanaryConfirmTimeoutDecision> {
        validate_timeout(timeout)?;
        let order = self
            .load_execution_canary_order(order_id)?
            .ok_or_else(|| anyhow!("missing execution canary order {order_id}"))?;
        let elapsed = elapsed_since_submit(now, order.submit_ts);
        let timeout_seconds = timeout.num_seconds();
        if order.status != EXECUTION_STATUS_CANARY_SUBMITTED {
            return Ok(confirm_decision(
                EXECUTION_CANARY_CONFIRM_DECISION_NOT_SUBMITTED,
                "order_not_submitted",
                order,
                elapsed.num_seconds(),
                timeout_seconds,
            ));
        }
        if elapsed < timeout {
            return Ok(confirm_decision(
                EXECUTION_CANARY_CONFIRM_DECISION_WAIT,
                "confirm_timeout_not_reached",
                order,
                elapsed.num_seconds(),
                timeout_seconds,
            ));
        }
        if order
            .tx_signature
            .as_deref()
            .is_some_and(|signature| !signature.trim().is_empty())
        {
            return Ok(confirm_decision(
                EXECUTION_CANARY_CONFIRM_DECISION_EXPIRE_UNSAFE,
                "tx_signature_present_retry_unsafe",
                order,
                elapsed.num_seconds(),
                timeout_seconds,
            ));
        }
        Ok(confirm_decision(
            EXECUTION_CANARY_CONFIRM_DECISION_RETRY,
            "no_tx_signature_retry_safe",
            order,
            elapsed.num_seconds(),
            timeout_seconds,
        ))
    }

    pub fn mark_execution_canary_retry_after_submit_timeout(
        &self,
        order_id: &str,
        now: DateTime<Utc>,
        timeout: Duration,
        reason: &str,
    ) -> Result<ExecutionCanaryOrder> {
        validate_reason(reason, "execution canary retry reason")?;
        let decision = self.execution_canary_confirm_timeout_decision(order_id, now, timeout)?;
        if decision.decision_status != EXECUTION_CANARY_CONFIRM_DECISION_RETRY {
            return Err(anyhow!(
                "execution canary retry is not safe for {order_id}: {} ({})",
                decision.decision_status,
                decision.decision_reason
            ));
        }
        let now_rfc3339 = now.to_rfc3339();
        self.with_immediate_transaction_retry("execution canary submit retry mark", |conn| {
            let current: Option<(String, Option<String>)> = conn
                .query_row(
                    "SELECT status, tx_signature
                     FROM orders
                     WHERE order_id = ?1
                     LIMIT 1",
                    params![order_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .context("failed loading execution canary retry state")?;
            let Some((status, tx_signature)) = current else {
                return Err(anyhow!("missing execution canary order {order_id}"));
            };
            if status != EXECUTION_STATUS_CANARY_SUBMITTED {
                return Err(anyhow!(
                    "invalid execution canary retry transition for {order_id}: {status} -> {EXECUTION_STATUS_CANARY_SIMULATED}"
                ));
            }
            if tx_signature
                .as_deref()
                .is_some_and(|signature| !signature.trim().is_empty())
            {
                return Err(anyhow!(
                    "execution canary retry is unsafe for {order_id}: tx_signature is present"
                ));
            }
            conn.execute(
                "UPDATE orders
                 SET status = ?2,
                     submit_ts = ?3,
                     confirm_ts = NULL,
                     err_code = NULL,
                     simulation_error = ?4,
                     attempt = attempt + 1
                 WHERE order_id = ?1",
                params![order_id, EXECUTION_STATUS_CANARY_SIMULATED, now_rfc3339, reason],
            )
            .context("failed marking execution canary retry after submit timeout")?;
            Ok(())
        })?;
        self.load_execution_canary_order(order_id)?
            .ok_or_else(|| anyhow!("missing execution canary order after retry mark"))
    }

    pub fn mark_execution_canary_failed_simulation_retry_candidate(
        &self,
        order_id: &str,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<ExecutionCanaryOrder> {
        validate_reason(reason, "execution canary failed simulation retry reason")?;
        let now_rfc3339 = now.to_rfc3339();
        let has_metadata_table =
            self.sqlite_table_exists("execution_canary_build_plan_metadata")?;
        self.with_immediate_transaction_retry("execution canary failed simulation retry", |conn| {
            let current: Option<(String, Option<String>, Option<String>)> = conn
                .query_row(
                    "SELECT status, err_code, tx_signature
                     FROM orders
                     WHERE order_id = ?1
                     LIMIT 1",
                    params![order_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .context("failed loading failed simulation retry state")?;
            let Some((status, err_code, tx_signature)) = current else {
                return Err(anyhow!("missing execution canary order {order_id}"));
            };
            if status != EXECUTION_STATUS_CANARY_FAILED
                || err_code.as_deref() != Some(EXECUTION_ERROR_SIMULATION_FAILED)
            {
                return Err(anyhow!(
                    "invalid failed simulation retry transition for {order_id}: status={status} err_code={:?}",
                    err_code
                ));
            }
            if tx_signature
                .as_deref()
                .is_some_and(|signature| !signature.trim().is_empty())
            {
                return Err(anyhow!(
                    "execution canary failed simulation retry is unsafe for {order_id}: tx_signature is present"
                ));
            }
            conn.execute(
                "UPDATE orders
                 SET status = ?2,
                     submit_ts = ?3,
                     confirm_ts = NULL,
                     err_code = NULL,
                     simulation_status = ?4,
                     simulation_error = ?5,
                     attempt = attempt + 1
                 WHERE order_id = ?1",
                params![
                    order_id,
                    EXECUTION_STATUS_CANARY_CANDIDATE,
                    now_rfc3339,
                    EXECUTION_SIMULATION_STATUS_NOT_RUN,
                    reason,
                ],
            )
            .context("failed marking failed simulation retry candidate")?;
            if has_metadata_table {
                conn.execute(
                    "DELETE FROM execution_canary_build_plan_metadata WHERE order_id = ?1",
                    params![order_id],
                )
                .context("failed clearing stale failed simulation build metadata")?;
            }
            Ok(())
        })?;
        self.load_execution_canary_order(order_id)?
            .ok_or_else(|| anyhow!("missing execution canary order after failed simulation retry"))
    }

    pub fn mark_execution_canary_failed_build_retry_candidate(
        &self,
        order_id: &str,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<ExecutionCanaryOrder> {
        validate_reason(reason, "execution canary failed build retry reason")?;
        let now_rfc3339 = now.to_rfc3339();
        self.with_immediate_transaction_retry("execution canary failed build retry", |conn| {
            let current: Option<(String, Option<String>, Option<String>)> = conn
                .query_row(
                    "SELECT status, err_code, tx_signature
                     FROM orders
                     WHERE order_id = ?1
                     LIMIT 1",
                    params![order_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .context("failed loading failed build retry state")?;
            let Some((status, err_code, tx_signature)) = current else {
                return Err(anyhow!("missing execution canary order {order_id}"));
            };
            if status != EXECUTION_STATUS_CANARY_FAILED
                || err_code.as_deref() != Some(EXECUTION_ERROR_BUILD_FAILED)
            {
                return Err(anyhow!(
                    "invalid failed build retry transition for {order_id}: status={status} err_code={:?}",
                    err_code
                ));
            }
            if tx_signature
                .as_deref()
                .is_some_and(|signature| !signature.trim().is_empty())
            {
                return Err(anyhow!(
                    "execution canary failed build retry is unsafe for {order_id}: tx_signature is present"
                ));
            }
            conn.execute(
                "UPDATE orders
                 SET status = ?2,
                     submit_ts = ?3,
                     confirm_ts = NULL,
                     err_code = NULL,
                     simulation_status = ?4,
                     simulation_error = ?5
                 WHERE order_id = ?1",
                params![
                    order_id,
                    EXECUTION_STATUS_CANARY_CANDIDATE,
                    now_rfc3339,
                    EXECUTION_SIMULATION_STATUS_NOT_RUN,
                    reason,
                ],
            )
            .context("failed marking failed build retry candidate")?;
            Ok(())
        })?;
        self.load_execution_canary_order(order_id)?
            .ok_or_else(|| anyhow!("missing execution canary order after failed build retry"))
    }
}

fn confirm_decision(
    decision_status: &str,
    decision_reason: &str,
    order: ExecutionCanaryOrder,
    elapsed_seconds: i64,
    timeout_seconds: i64,
) -> ExecutionCanaryConfirmTimeoutDecision {
    ExecutionCanaryConfirmTimeoutDecision {
        decision_status: decision_status.to_string(),
        decision_reason: decision_reason.to_string(),
        order,
        elapsed_seconds,
        timeout_seconds,
    }
}

fn validate_timeout(timeout: Duration) -> Result<()> {
    if timeout <= Duration::zero() {
        return Err(anyhow!(
            "execution canary confirm timeout must be positive, got {} seconds",
            timeout.num_seconds()
        ));
    }
    Ok(())
}

fn validate_reason(reason: &str, label: &str) -> Result<()> {
    if reason.trim().is_empty() {
        return Err(anyhow!("{label} must be non-empty"));
    }
    Ok(())
}

fn elapsed_since_submit(now: DateTime<Utc>, submit_ts: DateTime<Utc>) -> Duration {
    let elapsed = now.signed_duration_since(submit_ts);
    if elapsed < Duration::zero() {
        Duration::zero()
    } else {
        elapsed
    }
}
