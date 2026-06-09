use crate::execution_swap_http_request::is_missing_account_error_text;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{
    ExecutionCanaryOrder, SqliteStore, EXECUTION_ERROR_SIMULATION_FAILED,
    EXECUTION_SIMULATION_STATUS_NOT_RUN, EXECUTION_STATUS_CANARY_CANDIDATE,
    EXECUTION_STATUS_CANARY_FAILED,
};

pub(super) const RETRY_FAILED_BUY_TRANSIENT_SIMULATION_REASON: &str =
    "retry_failed_buy_transient_simulation";

pub(super) enum BuyRetryDecision {
    None,
    Retry(ExecutionCanaryOrder),
    Reconcile(ExecutionCanaryOrder),
}

pub(super) fn buy_retry_decision_for_signal(
    config: &ExecutionConfig,
    store: &SqliteStore,
    signal_id: &str,
    now: DateTime<Utc>,
) -> Result<BuyRetryDecision> {
    let Some(existing) = store.load_execution_canary_order_by_signal(signal_id)? else {
        return Ok(BuyRetryDecision::None);
    };
    if retry_failed_buy_candidate_ready(&existing) {
        return Ok(BuyRetryDecision::Retry(existing));
    }
    if failed_buy_simulation_retry_ready(config, &existing) {
        let retry = store.mark_execution_canary_failed_simulation_retry_candidate(
            &existing.order_id,
            now,
            RETRY_FAILED_BUY_TRANSIENT_SIMULATION_REASON,
        )?;
        return Ok(BuyRetryDecision::Retry(retry));
    }
    Ok(BuyRetryDecision::Reconcile(existing))
}

pub(super) fn next_failed_buy_retry_signal(
    config: &ExecutionConfig,
    store: &SqliteStore,
    limit: u32,
) -> Result<Option<CopySignalRow>> {
    let failed_buy_orders = store.list_failed_simulation_buy_execution_canary_orders_for_route(
        &config.canary_route,
        limit,
    )?;
    for order in failed_buy_orders {
        if !failed_buy_simulation_retry_ready(config, &order) {
            continue;
        }
        if let Some(signal) = store.load_copy_signal_by_signal_id(&order.signal_id)? {
            return Ok(Some(signal));
        }
    }
    Ok(None)
}

pub(super) fn failed_buy_simulation_retry_ready(
    config: &ExecutionConfig,
    order: &ExecutionCanaryOrder,
) -> bool {
    order.status == EXECUTION_STATUS_CANARY_FAILED
        && order.err_code.as_deref() == Some(EXECUTION_ERROR_SIMULATION_FAILED)
        && order
            .tx_signature
            .as_deref()
            .is_none_or(|signature| signature.trim().is_empty())
        && order.attempt < config.max_submit_attempts.max(1)
        && transient_buy_simulation_error(order.simulation_error.as_deref())
}

pub(super) fn retry_failed_buy_candidate_ready(order: &ExecutionCanaryOrder) -> bool {
    order.status == EXECUTION_STATUS_CANARY_CANDIDATE
        && order.simulation_status.as_deref() == Some(EXECUTION_SIMULATION_STATUS_NOT_RUN)
        && order.simulation_error.as_deref() == Some(RETRY_FAILED_BUY_TRANSIENT_SIMULATION_REASON)
        && order
            .tx_signature
            .as_deref()
            .is_none_or(|signature| signature.trim().is_empty())
}

fn transient_buy_simulation_error(error: Option<&str>) -> bool {
    let Some(error) = error.map(str::trim).filter(|error| !error.is_empty()) else {
        return false;
    };
    let lower = error.to_ascii_lowercase();
    is_missing_account_error_text(&lower)
        || lower.contains("market not found")
        || (lower.contains("market ") && lower.contains(" not found"))
        || pump_fun_paid_simulation_retryable(&lower)
}

fn pump_fun_paid_simulation_retryable(lower_error: &str) -> bool {
    lower_error.contains("pump.fun swap")
        && lower_error.contains("failed to simulate transaction")
        && (lower_error.contains("custom\":6063")
            || lower_error.contains("\"custom\":6063")
            || lower_error.contains("custom program error: 0x1788")
            || lower_error.contains("too much sol")
            || lower_error.contains("too little sol"))
}
