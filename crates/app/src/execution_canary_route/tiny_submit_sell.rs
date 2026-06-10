use super::tiny_submit::{
    apply_tiny_submit_confirm_path_outcome, build_simulated_signed_envelope,
    reconcile_existing_tiny_submit_order, tiny_submit_runtime_block_reason,
};
use super::tiny_submit_request::build_submit_request;
use super::tiny_submit_sell_metadata::{owned_position_sell_metadata, validate_tiny_sell_metadata};
use super::tiny_submit_sell_retry::{
    failed_sell_build_retry_ready, failed_sell_simulation_retry_ready,
    hold_terminal_failed_sell_no_route, hold_terminal_failed_sell_simulation,
    next_failed_sell_retry_event_id, retry_failed_sell_candidate_ready,
    terminal_failed_sell_no_route, terminal_failed_sell_no_route_retry_ready,
    terminal_failed_sell_simulation, RETRY_FAILED_SELL_WITH_OWNED_POSITION_AMOUNT_REASON,
    RETRY_TERMINAL_SELL_NO_ROUTE_REASON,
};
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_canary_submit_contract::ExecutionTinySubmitGate;
use crate::execution_quote_canary_helpers::{quote_canary_slippage_limit_bps, SIDE_SELL};
use crate::execution_quote_provider_selection::selected_execution_build_plan_metadata;
use crate::execution_submit_adapter::{
    record_execution_tiny_submit_confirm_path, JupiterMetisDryRunExecutionAdapter,
    RpcExecutionSubmitTransport,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ExecutionCanaryOrder, ExecutionCanaryRecordOutcome, SqliteStore,
    EXECUTION_CANARY_POSITION_CLOSE_CLOSED, EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED,
    EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION, EXECUTION_CANARY_POSITION_CLOSE_PARTIAL,
    EXECUTION_CANARY_SELL_DECISION_EXECUTE, EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT,
    EXECUTION_CANARY_SELL_DECISION_NO_POSITION, EXECUTION_ERROR_BUILD_FAILED,
};
use std::path::Path;

pub(super) async fn process_tiny_submit_sell_quote_event(
    config: &ExecutionConfig,
    store: &SqliteStore,
    event_id: &str,
    now: DateTime<Utc>,
) -> Result<Option<ExecutionCanaryStateMachineSummary>> {
    let Some(event) = store.load_execution_quote_canary_event_by_id(event_id)? else {
        return Ok(None);
    };
    if !event.side.eq_ignore_ascii_case(SIDE_SELL) {
        return Ok(None);
    }

    let mut summary = ExecutionCanaryStateMachineSummary {
        sell_candidates: 1,
        ..ExecutionCanaryStateMachineSummary::default()
    };
    if let Some(reason) = tiny_submit_runtime_block_reason(config) {
        summary.safety_blocked = 1;
        summary.skipped_reason = Some(reason);
        return Ok(Some(summary));
    }
    if sell_safety_blocked(config, store, &mut summary)? {
        return Ok(Some(summary));
    }

    let Some(signal_id) = event.signal_id.as_deref() else {
        summary.skipped_reason = Some("missing_sell_signal_id");
        return Ok(Some(summary));
    };
    let Some(signal) = store.load_copy_signal_by_signal_id(signal_id)? else {
        summary.skipped_reason = Some("missing_sell_copy_signal");
        return Ok(Some(summary));
    };
    if !signal.side.eq_ignore_ascii_case(SIDE_SELL) {
        summary.skipped_reason = Some("sell_signal_side_mismatch");
        return Ok(Some(summary));
    }
    if let Some(latest_buy_ts) = store.latest_live_execution_canary_buy_signal_ts(&signal.token)? {
        if signal.ts < latest_buy_ts {
            summary.skipped_reason = Some("sell_before_latest_buy");
            return Ok(Some(summary));
        }
    }

    let retry_order =
        if let Some(existing) = store.load_execution_canary_order_by_signal(&signal.signal_id)? {
            summary.existing = 1;
            summary.last_order_id = Some(existing.order_id.clone());
            if retry_failed_sell_candidate_ready(&existing) {
                Some(existing)
            } else if terminal_failed_sell_no_route_retry_ready(&existing, now) {
                if store
                    .load_execution_canary_open_position(&signal.token)?
                    .is_none()
                {
                    summary.open_positions = store.execution_canary_open_position_count()?;
                    summary.sell_no_position = 1;
                    summary.skipped_reason = Some("no_owned_position");
                    return Ok(Some(summary));
                }
                Some(
                    store.mark_execution_canary_terminal_sell_no_route_retry_candidate(
                        &existing.order_id,
                        now,
                        RETRY_TERMINAL_SELL_NO_ROUTE_REASON,
                    )?,
                )
            } else if terminal_failed_sell_no_route(config, &existing) {
                return Ok(Some(hold_terminal_failed_sell_no_route(
                    store, &existing, now,
                )?));
            } else if failed_sell_build_retry_ready(config, &existing) {
                Some(store.mark_execution_canary_failed_build_retry_candidate(
                    &existing.order_id,
                    now,
                    RETRY_FAILED_SELL_WITH_OWNED_POSITION_AMOUNT_REASON,
                )?)
            } else if failed_sell_simulation_retry_ready(config, &existing) {
                Some(
                    store.mark_execution_canary_failed_simulation_retry_candidate(
                        &existing.order_id,
                        now,
                        RETRY_FAILED_SELL_WITH_OWNED_POSITION_AMOUNT_REASON,
                    )?,
                )
            } else {
                reconcile_existing_tiny_submit_order(config, store, &existing, now, &mut summary)
                    .await?;
                return Ok(Some(summary));
            }
        } else {
            None
        };

    let metadata = selected_execution_build_plan_metadata(store, event)?;
    let sell_limit_bps = quote_canary_slippage_limit_bps(config, SIDE_SELL) as f64;
    let decision = store.execution_canary_sell_decision(
        &signal.token,
        metadata.slippage_bps,
        sell_limit_bps,
    )?;
    summary.last_sell_decision = Some(decision.decision_status.clone());
    match decision.decision_status.as_str() {
        EXECUTION_CANARY_SELL_DECISION_NO_POSITION => {
            summary.sell_no_position = 1;
            summary.skipped_reason = Some("no_owned_position");
            return Ok(Some(summary));
        }
        EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT => summary.sell_force_exit = 1,
        EXECUTION_CANARY_SELL_DECISION_EXECUTE => summary.sell_execute = 1,
        _ => {
            summary.skipped_reason = Some("unknown_sell_decision");
            return Ok(Some(summary));
        }
    }
    let order = if let Some(order) = retry_order {
        summary.last_order_id = Some(order.order_id.clone());
        order
    } else {
        let reserve = store.reserve_execution_canary_sell_order_unless_token_in_flight(
            &signal.signal_id,
            &config.canary_route,
            now,
        )?;
        summary.last_order_id = Some(reserve.order.order_id.clone());
        if reserve.blocked_by_in_flight_sell {
            summary.existing = 1;
            summary.skipped_reason = Some("sell_token_in_flight");
            return Ok(Some(summary));
        }
        if reserve.outcome == ExecutionCanaryRecordOutcome::Existing {
            summary.existing = 1;
            return Ok(Some(summary));
        }
        summary.reserved = 1;
        reserve.order
    };

    let metadata = match owned_position_sell_metadata(config, store, &signal.token, metadata).await
    {
        Ok(metadata) => metadata,
        Err(error) => {
            let error = format!("owned_sell_quote_failed: {error}");
            record_owned_sell_quote_failure(store, &order, now, &error, &mut summary)?;
            if write_off_dust_no_route_position(
                store,
                &order,
                &signal.token,
                signal.ts,
                now,
                &mut summary,
                &error,
            )? {
                return Ok(Some(summary));
            }
            summary.skipped_reason = Some("owned_sell_quote_error");
            summary.last_error = Some(error);
            return Ok(Some(summary));
        }
    };

    if let Some(reason) = validate_tiny_sell_metadata(&metadata) {
        summary.entry_gate_blocked = 1;
        summary.skipped_reason = Some(reason);
        return Ok(Some(summary));
    }

    let request = build_submit_request(config, &signal, &order, metadata);
    let adapter = JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let Some(envelope) =
        build_simulated_signed_envelope(store, &adapter, &request, now, &mut summary).await?
    else {
        return Ok(Some(summary));
    };
    let submit_gate = ExecutionTinySubmitGate {
        allow_rpc_submit: config.canary_tiny_submit_enabled,
        submit_timeout_ms: config.submit_timeout_ms,
    };
    let submit_transport = RpcExecutionSubmitTransport::new(config.submit_adapter_http_url.clone());
    let confirmation_timeout_ms = config.max_confirm_seconds.saturating_mul(1_000).max(1);
    let outcome = record_execution_tiny_submit_confirm_path(
        store,
        &adapter,
        &request,
        &envelope,
        &submit_gate,
        &submit_transport,
        &reqwest::Client::new(),
        &config.submit_adapter_http_url,
        now,
        confirmation_timeout_ms,
    )
    .await?;
    apply_tiny_submit_confirm_path_outcome(&mut summary, outcome);
    Ok(Some(summary))
}

pub(super) async fn process_failed_sell_simulation_sweep_for_route(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<ExecutionCanaryStateMachineSummary> {
    if let Some(event_id) = next_failed_sell_retry_event_id(config, store, now)? {
        return Ok(
            process_tiny_submit_sell_quote_event(config, store, &event_id, now)
                .await?
                .unwrap_or_default(),
        );
    }

    let mut orders = store
        .list_failed_simulation_sell_execution_canary_orders_for_route(&config.canary_route, 1)?;
    let Some(order) = orders.pop() else {
        return Ok(ExecutionCanaryStateMachineSummary::default());
    };
    if terminal_failed_sell_simulation(config, &order) {
        return hold_terminal_failed_sell_simulation(store, &order, now);
    }
    let Some(metadata) = store.load_execution_canary_build_plan_metadata(&order.order_id)? else {
        return Ok(ExecutionCanaryStateMachineSummary::default());
    };
    let Some(event_id) = metadata
        .quote_event_id
        .as_deref()
        .filter(|event_id| !event_id.trim().is_empty())
    else {
        return Ok(ExecutionCanaryStateMachineSummary::default());
    };
    Ok(
        process_tiny_submit_sell_quote_event(config, store, event_id, now)
            .await?
            .unwrap_or_default(),
    )
}

fn sell_safety_blocked(
    config: &ExecutionConfig,
    store: &SqliteStore,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<bool> {
    summary.open_positions = store.execution_canary_open_position_count()?;
    if Path::new(&config.canary_kill_switch_path).exists() {
        summary.safety_blocked = 1;
        summary.skipped_reason = Some("kill_switch_active");
        return Ok(true);
    }
    Ok(false)
}

fn record_owned_sell_quote_failure(
    store: &SqliteStore,
    order: &ExecutionCanaryOrder,
    now: DateTime<Utc>,
    error: &str,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<()> {
    store.mark_execution_canary_failed(
        &order.order_id,
        now,
        EXECUTION_ERROR_BUILD_FAILED,
        error,
    )?;
    summary.failed = 1;
    summary.last_order_id = Some(order.order_id.clone());
    Ok(())
}

fn write_off_dust_no_route_position(
    store: &SqliteStore,
    order: &ExecutionCanaryOrder,
    token: &str,
    signal_ts: DateTime<Utc>,
    now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary,
    error: &str,
) -> Result<bool> {
    if !error.contains("NO_ROUTES_FOUND") {
        return Ok(false);
    }
    let Some(position) = store.load_execution_canary_open_position(token)? else {
        return Ok(false);
    };
    if !is_exact_raw_dust_position(&position) {
        return Ok(false);
    }
    if signal_ts < position.opened_ts {
        return Ok(false);
    }
    if let Some(latest_buy_ts) = store.latest_live_execution_canary_buy_signal_ts(token)? {
        if signal_ts < latest_buy_ts {
            return Ok(false);
        }
    }

    let terminal_order = store.mark_execution_canary_terminal_sell_no_route_blocked(
        &order.order_id,
        "terminal_failed_sell_no_route_written_off",
    )?;
    let close_result = store.close_execution_canary_open_position(
        token,
        position.qty,
        position.qty_exact,
        0.0,
        1e-12,
        now,
    )?;
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
            summary.skipped_reason = Some("terminal_failed_sell_no_route_written_off");
        }
        EXECUTION_CANARY_POSITION_CLOSE_CLOSED => {
            summary.sell_closed = 1;
            summary.skipped_reason = Some("terminal_failed_sell_no_route_written_off");
        }
        EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED => {
            summary.sell_closed = 1;
            summary.sell_dust_closed = 1;
            summary.skipped_reason = Some("terminal_failed_sell_no_route_written_off");
        }
        _ => summary.skipped_reason = Some("unsupported_terminal_write_off_close_status"),
    }
    summary.open_positions = store.execution_canary_open_position_count()?;
    summary.last_error = terminal_order.simulation_error;
    Ok(true)
}

fn is_exact_raw_dust_position(
    position: &copybot_storage_core::ExecutionCanaryOwnedPosition,
) -> bool {
    position
        .qty_exact
        .is_some_and(|qty| qty.decimals() > 0 && qty.raw() == 1)
}
