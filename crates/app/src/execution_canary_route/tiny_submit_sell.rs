use super::tiny_submit::{
    apply_tiny_submit_confirm_path_outcome, build_simulated_signed_envelope, build_submit_request,
    reconcile_existing_tiny_submit_order, tiny_submit_runtime_block_reason,
};
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_canary_submit_contract::ExecutionTinySubmitGate;
use crate::execution_quote_canary_helpers::{
    quote_canary_slippage_limit_bps, DECISION_WOULD_EXECUTE, DECISION_WOULD_FORCE_EXIT,
    QUOTE_STATUS_OK, SIDE_SELL,
};
use crate::execution_quote_provider_selection::selected_execution_build_plan_metadata;
use crate::execution_submit_adapter::{
    record_execution_tiny_submit_confirm_path, ExecutionBuildPlanMetadata,
    JupiterMetisDryRunExecutionAdapter, RpcExecutionSubmitTransport,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ExecutionCanaryRecordOutcome, SqliteStore, EXECUTION_CANARY_SELL_DECISION_EXECUTE,
    EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT, EXECUTION_CANARY_SELL_DECISION_NO_POSITION,
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

    if let Some(existing) = store.load_execution_canary_order_by_signal(&signal.signal_id)? {
        summary.existing = 1;
        summary.last_order_id = Some(existing.order_id.clone());
        reconcile_existing_tiny_submit_order(config, store, &existing, now, &mut summary).await?;
        return Ok(Some(summary));
    }

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

    if let Some(reason) = validate_tiny_sell_metadata(&metadata) {
        summary.entry_gate_blocked = 1;
        summary.skipped_reason = Some(reason);
        return Ok(Some(summary));
    }

    let reserve =
        store.reserve_execution_canary_order(&signal.signal_id, &config.canary_route, now)?;
    summary.last_order_id = Some(reserve.order.order_id.clone());
    if reserve.outcome == ExecutionCanaryRecordOutcome::Existing {
        summary.existing = 1;
        return Ok(Some(summary));
    }
    summary.reserved = 1;

    let request = build_submit_request(config, &signal, &reserve.order, metadata);
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

fn validate_tiny_sell_metadata(metadata: &ExecutionBuildPlanMetadata) -> Option<&'static str> {
    if metadata.quote_status.as_deref() != Some(QUOTE_STATUS_OK) {
        return Some("sell_quote_not_ok");
    }
    match metadata.decision_status.as_deref() {
        Some(DECISION_WOULD_EXECUTE) | Some(DECISION_WOULD_FORCE_EXIT) => {}
        _ => return Some("sell_quote_not_executable"),
    }
    if metadata.quote_event_id.as_deref().is_none_or(str::is_empty) {
        return Some("missing_quote_event_id");
    }
    if metadata
        .quote_in_amount_raw
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Some("missing_quote_in_amount_raw");
    }
    if metadata
        .quote_out_amount_raw
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Some("missing_quote_out_amount_raw");
    }
    if metadata
        .quote_price_sol
        .is_none_or(|price| !price.is_finite() || price <= 0.0)
    {
        return Some("missing_quote_price_sol");
    }
    if metadata
        .route_plan_json
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Some("missing_route_plan_json");
    }
    if metadata.priority_fee_status.as_deref() != Some(QUOTE_STATUS_OK) {
        return Some("priority_fee_not_ok");
    }
    if metadata.priority_fee_lamports.is_none() {
        return Some("missing_priority_fee_lamports");
    }
    None
}
