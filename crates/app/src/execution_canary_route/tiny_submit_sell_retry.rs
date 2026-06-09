use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_quote_canary_helpers::SIDE_SELL;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ExecutionCanaryOrder, SqliteStore, EXECUTION_CANARY_POSITION_CLOSE_CLOSED,
    EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED, EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION,
    EXECUTION_CANARY_POSITION_CLOSE_PARTIAL, EXECUTION_ERROR_SIMULATION_FAILED,
    EXECUTION_SIMULATION_STATUS_NOT_RUN, EXECUTION_STATUS_CANARY_CANDIDATE,
    EXECUTION_STATUS_CANARY_FAILED,
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

pub(super) fn write_off_terminal_failed_sell_simulation(
    store: &SqliteStore,
    order: &ExecutionCanaryOrder,
    now: DateTime<Utc>,
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
    let Some(position) = store.load_execution_canary_open_position(&signal.token)? else {
        summary.sell_no_position = 1;
        summary.skipped_reason = Some("no_owned_position");
        return Ok(summary);
    };

    let close = store.close_execution_canary_open_position(
        &signal.token,
        position.qty,
        position.qty_exact,
        0.0,
        0.0,
        now,
    )?;
    summary.last_close_status = Some(close.close_status.clone());
    summary.last_closed_qty = close.closed_qty;
    summary.last_pnl_sol = close.pnl_sol;
    summary.skipped_reason = Some("terminal_failed_sell_simulation_write_off");
    match close.close_status.as_str() {
        EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION => {
            summary.sell_no_position = 1;
            summary.skipped_reason = Some("no_owned_position");
        }
        EXECUTION_CANARY_POSITION_CLOSE_PARTIAL => {
            summary.sell_closed = 1;
            summary.sell_partial = 1;
        }
        EXECUTION_CANARY_POSITION_CLOSE_CLOSED => {
            summary.sell_closed = 1;
        }
        EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED => {
            summary.sell_closed = 1;
            summary.sell_dust_closed = 1;
        }
        _ => summary.skipped_reason = Some("unsupported_terminal_write_off_status"),
    }
    Ok(summary)
}
