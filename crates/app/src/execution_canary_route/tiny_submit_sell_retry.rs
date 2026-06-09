use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_quote_canary_helpers::SIDE_SELL;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ExecutionCanaryOrder, SqliteStore, EXECUTION_ERROR_BUILD_FAILED,
    EXECUTION_ERROR_SIMULATION_FAILED, EXECUTION_SIMULATION_STATUS_NOT_RUN,
    EXECUTION_STATUS_CANARY_CANDIDATE, EXECUTION_STATUS_CANARY_FAILED,
};

pub(super) const RETRY_FAILED_SELL_WITH_OWNED_POSITION_AMOUNT_REASON: &str =
    "retry_failed_sell_with_owned_position_amount";

pub(super) fn failed_sell_simulation_retry_ready(
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
}

pub(super) fn retry_failed_sell_candidate_ready(order: &ExecutionCanaryOrder) -> bool {
    order.status == EXECUTION_STATUS_CANARY_CANDIDATE
        && order.simulation_status.as_deref() == Some(EXECUTION_SIMULATION_STATUS_NOT_RUN)
        && order.simulation_error.as_deref()
            == Some(RETRY_FAILED_SELL_WITH_OWNED_POSITION_AMOUNT_REASON)
        && order
            .tx_signature
            .as_deref()
            .is_none_or(|signature| signature.trim().is_empty())
}

pub(super) fn failed_sell_build_retry_ready(order: &ExecutionCanaryOrder) -> bool {
    order.status == EXECUTION_STATUS_CANARY_FAILED
        && order.err_code.as_deref() == Some(EXECUTION_ERROR_BUILD_FAILED)
        && order
            .tx_signature
            .as_deref()
            .is_none_or(|signature| signature.trim().is_empty())
}

pub(super) fn terminal_failed_sell_simulation(
    config: &ExecutionConfig,
    order: &ExecutionCanaryOrder,
) -> bool {
    order.status == EXECUTION_STATUS_CANARY_FAILED
        && order.err_code.as_deref() == Some(EXECUTION_ERROR_SIMULATION_FAILED)
        && order
            .tx_signature
            .as_deref()
            .is_none_or(|signature| signature.trim().is_empty())
        && order.attempt >= config.max_submit_attempts.max(1)
}

pub(super) fn hold_terminal_failed_sell_simulation(
    store: &SqliteStore,
    order: &ExecutionCanaryOrder,
    _now: DateTime<Utc>,
) -> Result<ExecutionCanaryStateMachineSummary> {
    let mut summary = ExecutionCanaryStateMachineSummary {
        sell_candidates: 1,
        existing: 1,
        last_order_id: Some(order.order_id.clone()),
        ..ExecutionCanaryStateMachineSummary::default()
    };
    let Some(signal) = store.load_copy_signal_by_signal_id(&order.signal_id)? else {
        summary.skipped_reason = Some("missing_terminal_failed_sell_signal");
        return Ok(summary);
    };
    if !signal.side.eq_ignore_ascii_case(SIDE_SELL) {
        summary.skipped_reason = Some("terminal_failed_sell_side_mismatch");
        return Ok(summary);
    }
    if store
        .load_execution_canary_open_position(&signal.token)?
        .is_none()
    {
        summary.sell_no_position = 1;
        summary.skipped_reason = Some("no_owned_position");
        return Ok(summary);
    }
    store.mark_execution_canary_terminal_sell_simulation_blocked(
        &order.order_id,
        "terminal_failed_sell_simulation_kept_open",
    )?;
    summary.open_positions = store.execution_canary_open_position_count()?;
    summary.last_error = order.simulation_error.clone();
    summary.skipped_reason = Some("terminal_failed_sell_simulation_kept_open");
    Ok(summary)
}
