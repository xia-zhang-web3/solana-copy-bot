use crate::{
    execution_canary_rows::execution_canary_order_from_row, ExecutionCanaryOrder,
    ExecutionCanarySubmitRiskOrder, ExecutionCanarySubmitRiskSummary, SqliteDiscoveryStore,
    EXECUTION_STATUS_CANARY_BUILT, EXECUTION_STATUS_CANARY_CANDIDATE,
    EXECUTION_STATUS_CANARY_SIMULATED, EXECUTION_STATUS_CANARY_SUBMITTED,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

const RPC_NOT_SENT_RETRY_REASON: &str = "retry_after_rpc_submit_not_sent";

impl SqliteDiscoveryStore {
    pub fn execution_canary_submit_risk_summary(
        &self,
        as_of: DateTime<Utc>,
        retry_reason: &str,
        max_submit_attempts: u32,
    ) -> Result<ExecutionCanarySubmitRiskSummary> {
        let orders = self.list_active_execution_canary_orders()?;
        Ok(submit_risk_summary_from_orders(
            as_of,
            retry_reason,
            max_submit_attempts.max(1),
            orders,
        ))
    }

    fn list_active_execution_canary_orders(&self) -> Result<Vec<ExecutionCanaryOrder>> {
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
                   AND status IN (?1, ?2, ?3, ?4)
                 ORDER BY submit_ts DESC, order_id DESC",
            )
            .context("failed to prepare execution canary submit risk query")?;
        let rows = stmt
            .query_map(
                params![
                    EXECUTION_STATUS_CANARY_CANDIDATE,
                    EXECUTION_STATUS_CANARY_BUILT,
                    EXECUTION_STATUS_CANARY_SIMULATED,
                    EXECUTION_STATUS_CANARY_SUBMITTED,
                ],
                execution_canary_order_from_row,
            )
            .context("failed querying execution canary submit risk orders")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading execution canary submit risk orders")
    }
}

fn submit_risk_summary_from_orders(
    as_of: DateTime<Utc>,
    retry_reason: &str,
    max_submit_attempts: u32,
    orders: Vec<ExecutionCanaryOrder>,
) -> ExecutionCanarySubmitRiskSummary {
    let mut summary = ExecutionCanarySubmitRiskSummary {
        as_of,
        max_submit_attempts,
        active_orders: 0,
        submitted_orders: 0,
        submitted_with_signature_orders: 0,
        submitted_without_signature_orders: 0,
        retry_ready_orders: 0,
        retry_budget_blocked_orders: 0,
        max_active_attempt: 0,
        latest_active_order: None,
    };

    for order in orders {
        summary.active_orders += 1;
        summary.max_active_attempt = summary.max_active_attempt.max(order.attempt);
        if summary.latest_active_order.is_none() {
            summary.latest_active_order = Some(submit_risk_latest_order(&order));
        }
        record_submit_risk_order(&mut summary, &order, retry_reason, max_submit_attempts);
    }
    summary
}

fn record_submit_risk_order(
    summary: &mut ExecutionCanarySubmitRiskSummary,
    order: &ExecutionCanaryOrder,
    retry_reason: &str,
    max_submit_attempts: u32,
) {
    let has_signature = tx_signature_present(order);
    if order.status == EXECUTION_STATUS_CANARY_SUBMITTED {
        summary.submitted_orders += 1;
        if has_signature {
            summary.submitted_with_signature_orders += 1;
        } else {
            summary.submitted_without_signature_orders += 1;
            if order.attempt >= max_submit_attempts {
                summary.retry_budget_blocked_orders += 1;
            }
        }
    }
    if is_retry_ready_order(order, retry_reason) {
        summary.retry_ready_orders += 1;
        if order.attempt > max_submit_attempts {
            summary.retry_budget_blocked_orders += 1;
        }
    }
}

fn is_retry_ready_order(order: &ExecutionCanaryOrder, retry_reason: &str) -> bool {
    order.status == EXECUTION_STATUS_CANARY_SIMULATED
        && !tx_signature_present(order)
        && order.simulation_error.as_deref().is_some_and(|reason| {
            reason == retry_reason
                || reason == RPC_NOT_SENT_RETRY_REASON
                || reason
                    .strip_prefix(RPC_NOT_SENT_RETRY_REASON)
                    .is_some_and(|suffix| suffix.starts_with(':'))
        })
}

fn submit_risk_latest_order(order: &ExecutionCanaryOrder) -> ExecutionCanarySubmitRiskOrder {
    ExecutionCanarySubmitRiskOrder {
        order_id: order.order_id.clone(),
        status: order.status.clone(),
        attempt: order.attempt,
        tx_signature_present: tx_signature_present(order),
        simulation_error: order.simulation_error.clone(),
    }
}

fn tx_signature_present(order: &ExecutionCanaryOrder) -> bool {
    order
        .tx_signature
        .as_deref()
        .is_some_and(|signature| !signature.trim().is_empty())
}
