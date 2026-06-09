use super::tiny_submit_retry::{
    is_tiny_submit_retry_ready, retry_existing_simulated_tiny_submit_order,
    TINY_SUBMIT_RETRY_AFTER_UNKNOWN_SUBMIT_TIMEOUT_REASON,
};
use super::tiny_submit_timeout::process_tiny_submit_timeout;
use crate::execution_build_plan_metadata::{
    load_execution_build_plan_metadata, record_execution_build_plan_metadata,
};
use crate::execution_build_plan_refresh::{
    fresh_submit_gate_reason, refresh_tiny_buy_build_plan_metadata,
};
use crate::execution_canary_entry_gate::validate_execution_canary_entry_metadata;
use crate::execution_canary_safety::pre_submit_safety_snapshot;
use crate::execution_canary_signing_contract::record_execution_signing_envelope;
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_canary_submit_contract::ExecutionTinySubmitGate;
use crate::execution_quote_canary_helpers::quote_canary_slippage_limit_bps;
use crate::execution_submit_adapter::{
    cap_execution_priority_fee_lamports, reconcile_execution_tiny_submit_confirmation,
    record_execution_tiny_submit_confirm_path, ExecutionSubmitAdapter, ExecutionSubmitRequest,
    ExecutionTinySubmitConfirmPathOutcome, JupiterMetisDryRunExecutionAdapter,
    RpcExecutionSubmitTransport,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{
    ExecutionCanaryOrder, ExecutionCanaryRecordOutcome, SqliteStore, EXECUTION_ERROR_BUILD_FAILED,
    EXECUTION_ERROR_SIMULATION_FAILED, EXECUTION_SIMULATION_STATUS_FAILED,
    EXECUTION_STATUS_CANARY_CONFIRMED, EXECUTION_STATUS_CANARY_FAILED,
    EXECUTION_STATUS_CANARY_SUBMITTED,
};

pub(crate) async fn process_tiny_submit_state_machine_for_route(
    config: &ExecutionConfig,
    store: &SqliteStore,
    signal: &CopySignalRow,
    now: DateTime<Utc>,
) -> Result<ExecutionCanaryStateMachineSummary> {
    let mut summary = ExecutionCanaryStateMachineSummary::default();
    if let Some(reason) = pre_candidate_skip_reason(config, signal) {
        summary.skipped_reason = Some(reason);
        return Ok(summary);
    }
    summary.candidates = 1;
    if let Some(reason) = tiny_submit_runtime_block_reason(config) {
        summary.safety_blocked = 1;
        summary.skipped_reason = Some(reason);
        return Ok(summary);
    }
    if apply_safety(config, store, now, &mut summary)? {
        return Ok(summary);
    }
    if let Some(existing) = store.load_execution_canary_order_by_signal(&signal.signal_id)? {
        summary.existing = 1;
        summary.last_order_id = Some(existing.order_id.clone());
        reconcile_existing_tiny_submit_order(config, store, &existing, now, &mut summary).await?;
        return Ok(summary);
    }
    let metadata = load_execution_build_plan_metadata(store, &signal.signal_id)?;
    let http = reqwest::Client::new();
    let metadata = refresh_tiny_buy_build_plan_metadata(&http, config, signal, metadata).await?;
    if let Some(reason) = validate_execution_canary_entry_metadata(config, &metadata) {
        summary.entry_gate_blocked = 1;
        summary.skipped_reason = Some(fresh_submit_gate_reason(&metadata, reason));
        return Ok(summary);
    }

    let reserve =
        store.reserve_execution_canary_order(&signal.signal_id, &config.canary_route, now)?;
    summary.last_order_id = Some(reserve.order.order_id.clone());
    if reserve.outcome == ExecutionCanaryRecordOutcome::Existing {
        summary.existing = 1;
        return Ok(summary);
    }
    summary.reserved = 1;

    let request = build_submit_request(config, signal, &reserve.order, metadata);
    let adapter = JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let Some(envelope) =
        build_simulated_signed_envelope(store, &adapter, &request, now, &mut summary).await?
    else {
        return Ok(summary);
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
    Ok(summary)
}

pub(crate) async fn process_tiny_submit_reconciliation_sweep_for_route(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<ExecutionCanaryStateMachineSummary> {
    let mut summary = ExecutionCanaryStateMachineSummary::default();
    if let Some(reason) = tiny_submit_runtime_block_reason(config) {
        summary.safety_blocked = 1;
        summary.skipped_reason = Some(reason);
        return Ok(summary);
    }
    let limit = config.canary_batch_limit.max(1);
    let orders = store.list_reconcilable_execution_canary_orders_for_route(
        &config.canary_route,
        TINY_SUBMIT_RETRY_AFTER_UNKNOWN_SUBMIT_TIMEOUT_REASON,
        limit,
    )?;
    for order in orders {
        summary.existing += 1;
        summary.last_order_id = Some(order.order_id.clone());
        reconcile_existing_tiny_submit_order(config, store, &order, now, &mut summary).await?;
    }
    Ok(summary)
}

fn pre_candidate_skip_reason(
    config: &ExecutionConfig,
    signal: &CopySignalRow,
) -> Option<&'static str> {
    if !config.canary_enabled {
        return Some("disabled");
    }
    if !signal.side.eq_ignore_ascii_case("buy") {
        return Some("not_buy");
    }
    None
}

pub(super) fn tiny_submit_runtime_block_reason(config: &ExecutionConfig) -> Option<&'static str> {
    if !config.canary_dry_run {
        return Some("non_dry_run_canary_unsupported");
    }
    if config.submit_adapter_http_url.trim().is_empty() {
        return Some("missing_submit_rpc_url");
    }
    if config.execution_signer_pubkey.trim().is_empty() {
        return Some("missing_execution_signer_pubkey");
    }
    if config.execution_signer_keypair_path.trim().is_empty() {
        return Some("missing_execution_signer_keypair_path");
    }
    if !config.swap_transaction_dry_run_enabled {
        return Some("missing_swap_transaction_dry_run");
    }
    None
}

pub(super) fn apply_safety(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<bool> {
    let safety = pre_submit_safety_snapshot(config, store, now)?;
    summary.open_positions = safety.open_positions;
    summary.daily_loss_sol = safety.daily_loss_sol;
    if let Some(reason) = safety.blocked_reason {
        summary.safety_blocked = 1;
        summary.skipped_reason = Some(reason);
        return Ok(true);
    }
    Ok(false)
}

pub(super) async fn reconcile_existing_tiny_submit_order(
    config: &ExecutionConfig,
    store: &SqliteStore,
    order: &ExecutionCanaryOrder,
    now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<()> {
    if !matches!(
        order.status.as_str(),
        EXECUTION_STATUS_CANARY_SUBMITTED | EXECUTION_STATUS_CANARY_CONFIRMED
    ) {
        if is_tiny_submit_retry_ready(order) {
            retry_existing_simulated_tiny_submit_order(config, store, order, now, summary).await?;
        }
        return Ok(());
    }
    let confirmation_timeout_ms = config.max_confirm_seconds.saturating_mul(1_000).max(1);
    if order
        .tx_signature
        .as_deref()
        .is_none_or(|signature| signature.trim().is_empty())
    {
        process_tiny_submit_timeout(
            store,
            &order.order_id,
            confirmation_timeout_ms,
            config.max_submit_attempts,
            now,
            summary,
        )?;
        return Ok(());
    }
    let outcome = reconcile_execution_tiny_submit_confirmation(
        store,
        config,
        &order.order_id,
        &reqwest::Client::new(),
        &config.submit_adapter_http_url,
        now,
        confirmation_timeout_ms,
    )
    .await?;
    let confirmation_pending = outcome.confirmation_pending;
    apply_tiny_submit_confirm_path_outcome(summary, outcome);
    if confirmation_pending > 0 {
        process_tiny_submit_timeout(
            store,
            &order.order_id,
            confirmation_timeout_ms,
            config.max_submit_attempts,
            now,
            summary,
        )?;
    }
    Ok(())
}

pub(super) fn apply_tiny_submit_confirm_path_outcome(
    summary: &mut ExecutionCanaryStateMachineSummary,
    outcome: ExecutionTinySubmitConfirmPathOutcome,
) {
    summary.failed = outcome.submit_failed + outcome.confirmation_failed;
    summary.submit_disabled = outcome.submit_disabled;
    summary.submit_ready_rejected = outcome.submit_ready_rejected;
    summary.sell_closed = outcome.sell_closed;
    summary.sell_partial = outcome.sell_partial;
    summary.sell_dust_closed = outcome.sell_dust_closed;
    summary.sell_no_position = outcome.sell_no_position;
    summary.last_confirm_reason = outcome.reason.clone();
    summary.last_error = outcome.error.or(outcome.reason);
}

pub(super) fn build_submit_request(
    config: &ExecutionConfig,
    signal: &CopySignalRow,
    order: &copybot_storage_core::ExecutionCanaryOrder,
    metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata,
) -> ExecutionSubmitRequest {
    let metadata = cap_execution_priority_fee_lamports(config, metadata);
    ExecutionSubmitRequest {
        order_id: order.order_id.clone(),
        signal_id: signal.signal_id.clone(),
        client_order_id: order.client_order_id.clone(),
        attempt: order.attempt,
        route: config.canary_route.clone(),
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: signal.side.clone(),
        buy_size_sol: config.canary_buy_size_sol,
        slippage_tolerance_bps: quote_canary_slippage_limit_bps(config, signal.side.as_str()),
        wallet_pubkey: config.canary_wallet_pubkey.clone(),
        metadata,
    }
}

pub(super) async fn build_simulated_signed_envelope(
    store: &SqliteStore,
    adapter: &JupiterMetisDryRunExecutionAdapter,
    request: &ExecutionSubmitRequest,
    now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<Option<crate::execution_signing_envelope::ExecutionSigningEnvelope>> {
    let plan = match adapter.build_transaction_plan(request) {
        Ok(plan) => plan,
        Err(error) => {
            mark_canary_failed(
                store,
                request,
                now,
                EXECUTION_ERROR_BUILD_FAILED,
                error.to_string(),
                summary,
            )?;
            return Ok(None);
        }
    };
    record_execution_build_plan_metadata(store, &plan, now)?;
    store.mark_execution_canary_built(&request.order_id, now)?;
    summary.built = 1;

    let simulation = match adapter.simulate_transaction_plan(&plan).await {
        Ok(simulation) => simulation,
        Err(error) => {
            let error = error.to_string();
            store.mark_execution_canary_simulated(
                &request.order_id,
                now,
                EXECUTION_SIMULATION_STATUS_FAILED,
                Some(&error),
            )?;
            mark_canary_failed(
                store,
                request,
                now,
                EXECUTION_ERROR_SIMULATION_FAILED,
                error,
                summary,
            )?;
            summary.simulated = 1;
            return Ok(None);
        }
    };
    store.mark_execution_canary_simulated(
        &request.order_id,
        now,
        &simulation.status,
        simulation.error.as_deref(),
    )?;
    summary.simulated = 1;
    if simulation.status == EXECUTION_SIMULATION_STATUS_FAILED {
        let error = simulation
            .error
            .unwrap_or_else(|| "simulation_failed".to_string());
        mark_canary_failed(
            store,
            request,
            now,
            EXECUTION_ERROR_SIMULATION_FAILED,
            error,
            summary,
        )?;
        return Ok(None);
    }

    let signing = record_execution_signing_envelope(store, adapter, request, &plan, now)?;
    summary.signing_envelope_built = signing.built;
    summary.last_signing_envelope_id = signing.envelope_id;
    summary.last_signing_envelope_mode = signing.envelope_mode;
    summary.failed = signing.failed;
    summary.last_error = signing.error;
    Ok(signing.envelope)
}

fn mark_canary_failed(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    now: DateTime<Utc>,
    code: &str,
    error: String,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<()> {
    let order = store.mark_execution_canary_failed(&request.order_id, now, code, &error)?;
    if order.status == EXECUTION_STATUS_CANARY_FAILED {
        summary.failed = 1;
        summary.last_error = Some(error);
    }
    Ok(())
}
