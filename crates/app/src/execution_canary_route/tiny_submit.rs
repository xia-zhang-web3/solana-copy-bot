pub(super) use super::tiny_submit_reconcile::reconcile_existing_tiny_submit_order;
use super::NativeBuyGuard;
use super::tiny_submit_build::{build_simulated_signed_envelope, mark_canary_failed};
use super::tiny_submit_buy_retry::{
    buy_retry_decision_for_signal, next_failed_buy_retry_signal, BuyRetryDecision,
};
use super::tiny_submit_candidate_cleanup::expire_stale_tiny_submit_candidates;
use super::tiny_submit_request::build_submit_request;
use crate::execution_build_plan_metadata::{
    load_execution_build_plan_metadata,
};
use crate::execution_build_plan_refresh::{
    fresh_submit_gate_reason, refresh_tiny_buy_build_plan_metadata,
};
use crate::execution_canary_entry_gate::validate_execution_canary_entry_metadata;
use crate::execution_canary_safety::live_pre_submit_safety_snapshot;
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_canary_submit_contract::ExecutionTinySubmitGate;
use crate::execution_submit_adapter::{
    ExecutionSubmitAdapter,
    ExecutionBuildPlanMetadata, ExecutionTinySubmitConfirmPathOutcome,
    JupiterMetisDryRunExecutionAdapter, RpcExecutionSubmitTransport,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{
    ExecutionCanaryRecordOutcome, SqliteStore, EXECUTION_ERROR_BUILD_FAILED,
};

pub(crate) async fn process_tiny_submit_state_machine_for_route(
    config: &ExecutionConfig,
    store: &SqliteStore,
    signal: &CopySignalRow,
    now: DateTime<Utc>,
) -> Result<ExecutionCanaryStateMachineSummary> {
    let adapter = JupiterMetisDryRunExecutionAdapter::new(config.clone());
    process_buy(config, store, signal, now, None, &adapter, None).await
}

mod native_buy_runner;
pub(crate) use native_buy_runner::process_native_buy_state_machine_for_route;
#[cfg(test)]
pub(crate) use crate::app_tests::native_buy_submit_helpers::process_native_buy_with_mock_quote_and_adapter;

pub(crate) async fn process_buy<A: ExecutionSubmitAdapter>(
    config: &ExecutionConfig,
    store: &SqliteStore,
    signal: &CopySignalRow,
    now: DateTime<Utc>,
    native: Option<&NativeBuyGuard>,
    adapter: &A,
    refreshed_quote: Option<ExecutionBuildPlanMetadata>,
) -> Result<ExecutionCanaryStateMachineSummary> {
    let mut summary = ExecutionCanaryStateMachineSummary::default();
    // A committed send remains an obligation after the admission policy expires
    // or is disabled. Reconciliation never turns this decision into another send.
    if native.is_some() {
        if let Some(existing) = store.load_execution_canary_order_by_signal(&signal.signal_id)? {
            summary.existing = 1;
            summary.last_order_id = Some(existing.order_id.clone());
            reconcile_existing_tiny_submit_order(config, store, &existing, now, &mut summary)
                .await?;
            return Ok(summary);
        }
    }
    if native.is_some()
        && !config.native_fresh_buy.as_ref().is_some_and(|policy| {
            policy.policy == copybot_config::PROCESSED_SLOT_FENCE_AVAILABILITY_V1
        })
    {
        summary.skipped_reason = Some("native_buy_authority_disabled");
        return Ok(summary);
    }
    if native.is_some()
        && (!super::uses_swap_blueprint_state_machine(config)
            || !config.canary_tiny_submit_enabled)
    {
        summary.skipped_reason = Some("native_buy_tiny_route_disabled");
        return Ok(summary);
    }
    if native.is_some() && config.tiny_experiment.activate
        && !copybot_config::native_first_buy_activation(config) {
        summary.skipped_reason = Some("native_buy_activation_disabled");
        return Ok(summary);
    }
    if let Some(reason) = pre_candidate_skip_reason(config, signal) {
        summary.skipped_reason = Some(reason);
        return Ok(summary);
    }
    if native.is_some() && signal.status != "native_buy_fenced_v1" {
        summary.skipped_reason = Some("native_buy_signal_status");
        return Ok(summary);
    }
    if native.is_some() && !native_signal_matches(store, signal)? {
        summary.skipped_reason = Some("native_buy_signal_changed");
        return Ok(summary);
    }
    if !native_current(native, store)? {
        summary.skipped_reason = Some("native_buy_decision_changed");
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
    let retry_decision = if native.is_some() {
        BuyRetryDecision::None
    } else {
        buy_retry_decision_for_signal(config, store, &signal.signal_id, now)?
    };
    let retry_order = match retry_decision {
        BuyRetryDecision::Retry(order) => {
            summary.existing = 1;
            summary.last_order_id = Some(order.order_id.clone());
            Some(order)
        }
        BuyRetryDecision::Reconcile(existing) => {
            summary.existing = 1;
            summary.last_order_id = Some(existing.order_id.clone());
            reconcile_existing_tiny_submit_order(config, store, &existing, now, &mut summary)
                .await?;
            return Ok(summary);
        }
        BuyRetryDecision::None => None,
    };
    let metadata = load_execution_build_plan_metadata(store, &signal.signal_id)?;
    let metadata = if let Some(fresh) = refreshed_quote {
        #[cfg(test)]
        if let Some(mock) = native.and_then(|guard| guard.mock_io()) {
            mock.count(|counts| counts.quote += 1);
        }
        if native.is_none()
            || metadata.quote_event_id.is_none()
            || fresh.quote_event_id != metadata.quote_event_id
        {
            summary.skipped_reason = Some("native_buy_quote_binding");
            return Ok(summary);
        }
        fresh
    } else {
        let http = reqwest::Client::new();
        #[cfg(test)]
        if let Some(mock) = native.and_then(|guard| guard.mock_io()) {
            let runner = mock.runner.as_ref().ok_or_else(|| anyhow::anyhow!("native_runner_mock_missing"))?;
            mock.count(|c| c.fresh_quote += 1);
            crate::execution_build_plan_refresh::refresh_tiny_buy_build_plan_metadata_with_external_quote(
                &http, config, signal, metadata, runner.fresh_quote.clone(),
            ).await?
        } else {
            refresh_tiny_buy_build_plan_metadata(&http, config, signal, metadata).await?
        }
        #[cfg(not(test))]
        refresh_tiny_buy_build_plan_metadata(&http, config, signal, metadata).await?
    };
    if !native_current(native, store)? {
        summary.skipped_reason = Some("native_buy_decision_changed");
        return Ok(summary);
    }
    if let Some(reason) = validate_execution_canary_entry_metadata(config, &metadata) {
        let reason = fresh_submit_gate_reason(&metadata, reason);
        summary.entry_gate_blocked = 1;
        summary.skipped_reason = Some(reason);
        if let Some(order) = retry_order.as_ref() {
            let error = format!("retry_buy_fresh_metadata_blocked:{reason}");
            let code = EXECUTION_ERROR_BUILD_FAILED;
            store.mark_execution_canary_failed(&order.order_id, now, code, &error)?;
            summary.failed = 1;
            summary.last_error = Some(error);
        }
        return Ok(summary);
    }
    let order = if let Some(order) = retry_order {
        summary.last_order_id = Some(order.order_id.clone());
        order
    } else {
        let reserve =
            store.reserve_execution_canary_order(&signal.signal_id, &config.canary_route, now)?;
        summary.last_order_id = Some(reserve.order.order_id.clone());
        if reserve.outcome == ExecutionCanaryRecordOutcome::Existing {
            summary.existing = 1;
            return Ok(summary);
        }
        summary.reserved = 1;
        reserve.order
    };
    let mut request = build_submit_request(config, signal, &order, metadata, None);
    if let Err(error) = crate::execution_native_floor_policy::protected::prepare_request_guarded(
        store,
        config,
        &mut request,
        now,
        native,
    )
    .await
    {
        if matches!(error.to_string().as_str(), "tiny_capital_order_changed" | "native_buy_decision_changed") {
            summary.skipped_reason = Some(if error.to_string() == "tiny_capital_order_changed" {
                "tiny_capital_order_changed"
            } else { "native_buy_decision_changed" });
            summary.last_error = Some(error.to_string());
            return Ok(summary);
        }
        mark_canary_failed(
            store,
            &request,
            now,
            EXECUTION_ERROR_BUILD_FAILED,
            error.to_string(),
            &mut summary,
        )?;
        return Ok(summary);
    }
    if !native_current(native, store)? {
        summary.skipped_reason = Some("native_buy_decision_changed");
        return Ok(summary);
    }
    let Some(envelope) =
        build_simulated_signed_envelope(store, adapter, &request, now, &mut summary, native).await?
    else {
        return Ok(summary);
    };
    if !native_current(native, store)? {
        summary.skipped_reason = Some("native_buy_decision_changed");
        return Ok(summary);
    }
    let submit_gate = ExecutionTinySubmitGate::from_config(config);
    let submit_transport = RpcExecutionSubmitTransport::new(config.submit_adapter_http_url.clone());
    let confirmation_timeout_ms = config.max_confirm_seconds.saturating_mul(1_000).max(1);
    let outcome = crate::execution_submit_adapter::record_execution_tiny_submit_confirm_path_guarded(
        store,
        adapter,
        &request,
        &envelope,
        &submit_gate,
        &submit_transport,
        &reqwest::Client::new(),
        &config.submit_adapter_http_url,
        now,
        confirmation_timeout_ms,
        native,
    )
    .await?;
    apply_tiny_submit_confirm_path_outcome(&mut summary, outcome);
    Ok(summary)
}

pub(super) fn native_current(native: Option<&NativeBuyGuard>, store: &SqliteStore) -> Result<bool> {
    native.map_or(Ok(true), |guard| guard.check(store))
}

fn native_signal_matches(store: &SqliteStore, signal: &CopySignalRow) -> Result<bool> {
    let Some(saved) = store.load_copy_signal_by_signal_id(&signal.signal_id)? else {
        return Ok(false);
    };
    Ok(saved.signal_id == signal.signal_id
        && saved.wallet_id == signal.wallet_id
        && saved.side == signal.side
        && saved.token == signal.token
        && saved.notional_sol.to_bits() == signal.notional_sol.to_bits()
        && saved.notional_lamports == signal.notional_lamports
        && saved.notional_origin == signal.notional_origin
        && saved.ts == signal.ts
        && saved.status == signal.status)
}

pub(crate) async fn process_tiny_submit_reconciliation_sweep_for_route(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
    progress: &crate::execution_source_sell_continuation::Continuation,
) -> Result<ExecutionCanaryStateMachineSummary> {
    let mut summary = ExecutionCanaryStateMachineSummary::default();
    if let Some(reason) = tiny_submit_runtime_block_reason(config) {
        summary.safety_blocked = 1;
        summary.skipped_reason = Some(reason);
        return Ok(summary);
    }
    crate::execution_submit_adapter::recover_failed_expenses(config, store, now).await?;
    // Selection cannot let a deferred BUY occupy the whole recovery budget.
    // Each selected BUY is checked again against current state before refresh.
    let blocked_buy_retry_max_attempt = apply_safety(config, store, now, &mut summary)?
        .then_some(config.max_submit_attempts.max(1));
    let limit = config.canary_batch_limit.max(1);
    super::tiny_submit_recovery_selection::reconcile_selected(
        config,
        store,
        now,
        blocked_buy_retry_max_attempt,
        progress,
        &mut summary,
    )
    .await?;
    if summary.existing > 0 {
        return Ok(summary);
    }
    if expire_stale_tiny_submit_candidates(config, store, now, limit, &mut summary)? {
        return Ok(summary);
    }
    if let Some(signal) = next_failed_buy_retry_signal(config, store, limit)? {
        return process_tiny_submit_state_machine_for_route(config, store, &signal, now).await;
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
    let safety = live_pre_submit_safety_snapshot(config, store, now)?;
    summary.open_positions = safety.open_positions;
    summary.daily_loss_sol = safety.daily_loss_sol;
    summary.entry_cost = safety.entry_cost;
    if config.canary_tiny_submit_enabled
        && crate::execution_native_floor_policy::reserve_lamports(config.pretrade_min_sol_reserve)
            .is_err()
    {
        summary.safety_blocked = 1;
        summary.skipped_reason = Some("native_floor_invalid_policy");
        return Ok(true);
    }
    if let Some(reason) = safety.blocked_reason {
        summary.buy_blocker = safety.buy_blocker;
        summary.safety_blocked = 1;
        summary.skipped_reason = Some(reason);
        return Ok(true);
    }
    Ok(false)
}

pub(crate) fn apply_tiny_submit_confirm_path_outcome(
    summary: &mut ExecutionCanaryStateMachineSummary,
    outcome: ExecutionTinySubmitConfirmPathOutcome,
) {
    summary
        .pre_submit_refusals
        .record(outcome.pre_submit_refusal);
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
