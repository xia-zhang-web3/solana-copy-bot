use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_quote_canary_helpers::SIDE_SELL;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ExecutionCanaryOrder, ExecutionCanaryPositionCloseResult, SqliteStore,
    EXECUTION_CANARY_POSITION_CLOSE_CLOSED, EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED,
    EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION, EXECUTION_CANARY_POSITION_CLOSE_PARTIAL,
    EXECUTION_ERROR_BUILD_FAILED, EXECUTION_ERROR_SIMULATION_FAILED,
    EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE, EXECUTION_SIMULATION_STATUS_NOT_RUN,
    EXECUTION_STATUS_CANARY_CANDIDATE, EXECUTION_STATUS_CANARY_FAILED,
};

pub(super) const RETRY_FAILED_SELL_WITH_OWNED_POSITION_AMOUNT_REASON: &str =
    "retry_failed_sell_with_owned_position_amount";
pub(super) const RETRY_TERMINAL_SELL_NO_ROUTE_REASON: &str =
    "retry_terminal_sell_no_route_after_cooldown";
pub(super) const TERMINAL_SELL_NO_ROUTE_REPROBE_COOLDOWN_SECONDS: i64 = 600;
const TERMINAL_SELL_WRITE_OFF_EXIT_PRICE_SOL: f64 = 0.0;
const TERMINAL_SELL_WRITE_OFF_DUST_QTY_EPSILON: f64 = 1e-12;

pub(super) fn next_failed_sell_retry_event_id(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<Option<String>> {
    let mut retry_events = store.list_retry_candidate_sell_execution_quote_event_ids_for_route(
        &config.canary_route,
        RETRY_FAILED_SELL_WITH_OWNED_POSITION_AMOUNT_REASON,
        1,
    )?;
    if let Some(event_id) = retry_events.pop() {
        return Ok(Some(event_id));
    }

    let retry_after =
        now - chrono::Duration::seconds(TERMINAL_SELL_NO_ROUTE_REPROBE_COOLDOWN_SECONDS);
    let mut terminal_no_route_events = store
        .list_terminal_no_route_sell_execution_quote_event_ids_for_route(
            &config.canary_route,
            retry_after,
            1,
        )?;
    if let Some(event_id) = terminal_no_route_events.pop() {
        return Ok(Some(event_id));
    }

    let mut failed_build_events = store
        .list_failed_build_sell_execution_quote_event_ids_for_route(&config.canary_route, 1)?;
    Ok(failed_build_events.pop())
}

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

pub(super) fn failed_sell_build_retry_ready(
    config: &ExecutionConfig,
    order: &ExecutionCanaryOrder,
) -> bool {
    order.status == EXECUTION_STATUS_CANARY_FAILED
        && order.err_code.as_deref() == Some(EXECUTION_ERROR_BUILD_FAILED)
        && order
            .tx_signature
            .as_deref()
            .is_none_or(|signature| signature.trim().is_empty())
        && order.attempt < config.max_submit_attempts.max(1)
}

pub(super) fn terminal_failed_sell_no_route(
    config: &ExecutionConfig,
    order: &ExecutionCanaryOrder,
) -> bool {
    order.status == EXECUTION_STATUS_CANARY_FAILED
        && order.err_code.as_deref() == Some(EXECUTION_ERROR_BUILD_FAILED)
        && order
            .tx_signature
            .as_deref()
            .is_none_or(|signature| signature.trim().is_empty())
        && order.attempt >= config.max_submit_attempts.max(1)
        && order
            .simulation_error
            .as_deref()
            .is_some_and(|error| error.contains("NO_ROUTES_FOUND"))
}

pub(super) fn terminal_failed_sell_no_route_retry_ready(
    order: &ExecutionCanaryOrder,
    now: DateTime<Utc>,
) -> bool {
    let retry_after =
        now - chrono::Duration::seconds(TERMINAL_SELL_NO_ROUTE_REPROBE_COOLDOWN_SECONDS);
    order.status == EXECUTION_STATUS_CANARY_FAILED
        && order.err_code.as_deref() == Some(EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE)
        && order
            .tx_signature
            .as_deref()
            .is_none_or(|signature| signature.trim().is_empty())
        && order.confirm_ts.unwrap_or(order.submit_ts) <= retry_after
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
    let terminal_order = store.mark_execution_canary_terminal_sell_simulation_blocked(
        &order.order_id,
        "terminal_failed_sell_simulation_written_off",
    )?;
    let close_result = store.close_execution_canary_open_position(
        &signal.token,
        position.qty,
        position.qty_exact,
        TERMINAL_SELL_WRITE_OFF_EXIT_PRICE_SOL,
        TERMINAL_SELL_WRITE_OFF_DUST_QTY_EPSILON,
        now,
    )?;
    record_terminal_write_off_summary(
        store,
        &mut summary,
        &terminal_order,
        close_result,
        "terminal_failed_sell_simulation_written_off",
    )?;
    Ok(summary)
}

pub(super) fn hold_terminal_failed_sell_no_route(
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
    let terminal_order = store.mark_execution_canary_terminal_sell_no_route_blocked(
        &order.order_id,
        "terminal_failed_sell_no_route_written_off",
    )?;
    let close_result = store.close_execution_canary_open_position(
        &signal.token,
        position.qty,
        position.qty_exact,
        TERMINAL_SELL_WRITE_OFF_EXIT_PRICE_SOL,
        TERMINAL_SELL_WRITE_OFF_DUST_QTY_EPSILON,
        now,
    )?;
    record_terminal_write_off_summary(
        store,
        &mut summary,
        &terminal_order,
        close_result,
        "terminal_failed_sell_no_route_written_off",
    )?;
    Ok(summary)
}

fn record_terminal_write_off_summary(
    store: &SqliteStore,
    summary: &mut ExecutionCanaryStateMachineSummary,
    order: &ExecutionCanaryOrder,
    close_result: ExecutionCanaryPositionCloseResult,
    reason: &'static str,
) -> Result<()> {
    summary.last_close_status = Some(close_result.close_status.clone());
    summary.last_closed_qty = close_result.closed_qty;
    summary.last_pnl_sol = close_result.pnl_sol;
    match close_result.close_status.as_str() {
        EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION => {
            summary.sell_no_position = 1;
            summary.skipped_reason = Some("no_owned_position");
        }
        EXECUTION_CANARY_POSITION_CLOSE_PARTIAL => {
            summary.sell_closed = 1;
            summary.sell_partial = 1;
            summary.skipped_reason = Some(reason);
        }
        EXECUTION_CANARY_POSITION_CLOSE_CLOSED => {
            summary.sell_closed = 1;
            summary.skipped_reason = Some(reason);
        }
        EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED => {
            summary.sell_closed = 1;
            summary.sell_dust_closed = 1;
            summary.skipped_reason = Some(reason);
        }
        _ => {
            summary.skipped_reason = Some("unsupported_terminal_write_off_close_status");
        }
    }
    summary.open_positions = store.execution_canary_open_position_count()?;
    summary.last_error = order.simulation_error.clone();
    Ok(())
}
