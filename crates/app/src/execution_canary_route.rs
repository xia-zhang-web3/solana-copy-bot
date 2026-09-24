use crate::execution_build_plan_metadata::load_execution_build_plan_metadata;
use crate::execution_canary_entry_gate::validate_execution_canary_entry_metadata;
use crate::execution_canary_state_machine::{
    ExecutionCanaryStateMachine, ExecutionCanaryStateMachineSummary,
};
use crate::execution_quote_canary_helpers::DECISION_WOULD_EXECUTE;
use crate::execution_submit_adapter::{
    JupiterMetisDryRunExecutionAdapter, NoSubmitExecutionAdapter,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::SqliteStore;

/// One immutable native BUY decision carried through the existing tiny route.
/// The source signal timestamp is an availability clock, never source UTC.
#[derive(Debug, Clone)]
pub(crate) struct NativeBuyGuard {
    signal_id: String,
    decision_id: String,
    policy_identity: String,
    max_age_seconds: u64,
    tick_at: DateTime<Utc>,
    #[cfg(test)]
    pub(crate) mock_io: Option<std::sync::Arc<NativeBuyMockIo>>,
}

#[cfg(test)]
pub(crate) use crate::app_tests::native_buy_mock_helpers::{
    NativeBuyMockAdapter, NativeBuyMockCounts, NativeBuyMockIo, NativeBuyRunnerOperands,
};

impl NativeBuyGuard {
    pub(crate) fn new(
        config: &ExecutionConfig,
        signal_id: &str,
        decision_id: &str,
        tick_at: DateTime<Utc>,
    ) -> Result<Self> {
        Ok(Self {
            signal_id: signal_id.into(),
            decision_id: decision_id.into(),
            policy_identity: crate::execution_native_buy_rpc::policy_identity(config)?,
            max_age_seconds: config.canary_max_signal_age_seconds,
            tick_at,
            #[cfg(test)]
            mock_io: None,
        })
    }

    pub(crate) fn check(&self, store: &SqliteStore) -> Result<bool> {
        let Some(now) = crate::execution_canary_safety::risk_clock::decision_time(self.tick_at)
        else {
            return Ok(false);
        };
        self.check_at(store, now)
    }

    pub(crate) fn check_at(&self, store: &SqliteStore, now: DateTime<Utc>) -> Result<bool> {
        if store.native_buy_policy_identity(&self.signal_id)?.as_deref()
            != Some(self.policy_identity.as_str())
        {
            return Ok(false);
        }
        store.native_buy_recheck(
            &self.signal_id,
            &self.decision_id,
            now,
            self.max_age_seconds,
        )
    }

    pub(crate) fn activation_binding(
        &self, store: &SqliteStore, config: &ExecutionConfig,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
        now: DateTime<Utc>,
    ) -> Result<copybot_storage_core::native_buy::NativeBuyActivationBinding> {
        ensure!(
            copybot_config::native_first_buy_activation(config)
                && self.signal_id == request.signal_id
                && self.policy_identity == crate::execution_native_buy_rpc::policy_identity(config)?
                && self.check_at(store, now)?,
            "native_buy_decision_changed"
        );
        Ok(copybot_storage_core::native_buy::NativeBuyActivationBinding {
            signal_id: self.signal_id.clone(),
            decision_id: self.decision_id.clone(),
            policy_identity: self.policy_identity.clone(),
            max_age_seconds: self.max_age_seconds,
            order_id: request.order_id.clone(),
            client_order_id: request.client_order_id.clone(),
            attempt: request.attempt,
            route: request.route.clone(),
        })
    }
}

mod tiny_submit;
mod tiny_submit_build;
pub(crate) use tiny_submit::apply_tiny_submit_confirm_path_outcome;
pub(crate) use tiny_submit_build::build_simulated_signed_envelope;
mod tiny_submit_buy_retry;
mod tiny_submit_candidate_cleanup;
mod tiny_submit_expiry;
mod tiny_submit_orphan_recovery;
mod tiny_submit_recovery_selection;
mod tiny_submit_reconcile;
mod tiny_submit_request;
mod tiny_submit_retry;
mod tiny_submit_sell;
mod tiny_submit_sell_metadata;
mod tiny_submit_sell_retry;
mod tiny_submit_sell_sweep;
mod tiny_submit_source_write_off;
mod tiny_submit_timeout;
mod tiny_submit_wallet_balance;
#[cfg(test)]
pub(crate) use self::tiny_submit::process_buy as test_process_buy;
#[cfg(test)]
pub(crate) use self::tiny_submit_reconcile::reconcile_existing_tiny_submit_order_inner as test_reconcile_existing_tiny_submit_order_inner;

use self::tiny_submit::{
    process_native_buy_state_machine_for_route, process_tiny_submit_reconciliation_sweep_for_route,
    process_tiny_submit_state_machine_for_route,
};
pub(crate) use self::tiny_submit_reconcile::process_native_buy_receipt_recovery_for_route;
#[cfg(test)]
pub(crate) use crate::app_tests::native_buy_submit_helpers::process_native_buy_receipt_recovery_for_route_with_mock;
#[cfg(test)]
pub(crate) use crate::app_tests::native_buy_submit_helpers::process_native_buy_with_mock_quote_and_adapter;

pub(crate) async fn process_native_buy_candidate_for_route(
    config: &ExecutionConfig,
    store: &SqliteStore,
    signal: &CopySignalRow,
    now: DateTime<Utc>,
    guard: &NativeBuyGuard,
) -> Result<ExecutionCanaryStateMachineSummary> {
    process_native_buy_state_machine_for_route(config, store, signal, now, guard).await
}
use self::tiny_submit_orphan_recovery::process_tiny_submit_orphan_position_recovery_for_route;
use self::tiny_submit_sell::process_tiny_submit_sell_quote_event;
#[cfg(test)]
pub(crate) use self::tiny_submit_sell_metadata::{
    guarded_owned_position_sell_metadata, owned_position_sell_metadata,
};
use self::tiny_submit_sell_sweep::process_failed_sell_simulation_sweep_for_route;

pub(crate) const CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN: &str =
    "metis-swap-instructions-dry-run";

pub(crate) fn uses_swap_blueprint_state_machine(config: &ExecutionConfig) -> bool {
    uses_jupiter_metis_dry_run_adapter(&config.canary_route)
}

pub(crate) fn uses_jupiter_metis_dry_run_adapter(route: &str) -> bool {
    route
        .trim()
        .eq_ignore_ascii_case(CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN)
}

pub(crate) fn list_swap_blueprint_state_machine_candidates(
    store: &SqliteStore,
    config: &ExecutionConfig,
    copy_signal_status: &str,
    since: DateTime<Utc>,
) -> Result<Vec<CopySignalRow>> {
    let batch_limit = config.canary_batch_limit.max(1);
    let scan_limit = batch_limit.saturating_mul(10).min(500);
    let signals = store
        .list_execution_canary_candidates(copy_signal_status, since, scan_limit)
        .context("failed loading execution canary state-machine candidates")?;
    let mut executable = Vec::new();
    for signal in signals {
        let event = store.load_latest_execution_quote_canary_entry_event(&signal.signal_id)?;
        let would_execute = event.and_then(|event| event.decision_status).as_deref()
            == Some(DECISION_WOULD_EXECUTE);
        if would_execute
            && validate_execution_canary_entry_metadata(
                config,
                &load_execution_build_plan_metadata(store, &signal.signal_id)?,
            )
            .is_none()
        {
            executable.push(signal);
            if executable.len() >= batch_limit as usize {
                break;
            }
        }
    }
    Ok(executable)
}

pub(crate) async fn process_canary_state_machine_for_route(
    config: &ExecutionConfig,
    store: &SqliteStore,
    signal: &CopySignalRow,
    now: DateTime<Utc>,
) -> Result<ExecutionCanaryStateMachineSummary> {
    if uses_swap_blueprint_state_machine(config) {
        if config.canary_tiny_submit_enabled {
            return process_tiny_submit_state_machine_for_route(config, store, signal, now).await;
        }
        let adapter = JupiterMetisDryRunExecutionAdapter::new(config.clone());
        let state_machine = ExecutionCanaryStateMachine::new(config.clone(), adapter);
        return state_machine
            .process_buy_candidate(store, signal, now)
            .await;
    }
    let state_machine = ExecutionCanaryStateMachine::new(config.clone(), NoSubmitExecutionAdapter);
    state_machine
        .process_buy_candidate(store, signal, now)
        .await
}

pub(crate) async fn process_tiny_submit_sell_quote_event_for_route(
    config: &ExecutionConfig,
    store: &SqliteStore,
    event_id: &str,
    now: DateTime<Utc>,
) -> Result<Option<ExecutionCanaryStateMachineSummary>> {
    if !uses_swap_blueprint_state_machine(config) || !config.canary_tiny_submit_enabled {
        return Ok(None);
    }
    process_tiny_submit_sell_quote_event(config, store, event_id, now).await
}

pub(crate) async fn process_tiny_submit_reconciliation_sweep(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<Option<ExecutionCanaryStateMachineSummary>> {
    process_tiny_submit_reconciliation_sweep_with_continuation(
        config,
        store,
        now,
        &Default::default(),
    )
    .await
}

pub(crate) async fn process_tiny_submit_reconciliation_sweep_with_continuation(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
    progress: &crate::execution_source_sell_continuation::Continuation,
) -> Result<Option<ExecutionCanaryStateMachineSummary>> {
    if !uses_swap_blueprint_state_machine(config) || !config.canary_tiny_submit_enabled {
        return Ok(None);
    }
    process_tiny_submit_reconciliation_sweep_for_route(config, store, now, progress)
        .await
        .map(Some)
}

pub(crate) async fn process_tiny_submit_orphan_position_recovery_sweep(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<Option<ExecutionCanaryStateMachineSummary>> {
    if !uses_swap_blueprint_state_machine(config) || !config.canary_tiny_submit_enabled {
        return Ok(None);
    }
    process_tiny_submit_orphan_position_recovery_for_route(config, store, now)
        .await
        .map(Some)
}

pub(crate) async fn process_failed_sell_simulation_sweep(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<Option<ExecutionCanaryStateMachineSummary>> {
    process_failed_sell_simulation_sweep_with_continuation(config, store, now, &Default::default())
        .await
}

pub(crate) async fn process_failed_sell_simulation_sweep_with_continuation(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
    progress: &crate::execution_source_sell_continuation::Continuation,
) -> Result<Option<ExecutionCanaryStateMachineSummary>> {
    if !uses_swap_blueprint_state_machine(config) || !config.canary_tiny_submit_enabled {
        return Ok(None);
    }
    process_failed_sell_simulation_sweep_for_route(config, store, now, progress)
        .await
        .map(Some)
}
