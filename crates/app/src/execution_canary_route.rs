use crate::execution_canary_state_machine::{
    ExecutionCanaryStateMachine, ExecutionCanaryStateMachineSummary,
};
use crate::execution_quote_canary_helpers::DECISION_WOULD_EXECUTE;
use crate::execution_submit_adapter::{
    uses_jupiter_metis_dry_run_adapter, JupiterMetisDryRunExecutionAdapter,
    NoSubmitExecutionAdapter,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::SqliteStore;

pub(crate) fn uses_swap_blueprint_state_machine(config: &ExecutionConfig) -> bool {
    uses_jupiter_metis_dry_run_adapter(&config.canary_route)
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
        if would_execute {
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
