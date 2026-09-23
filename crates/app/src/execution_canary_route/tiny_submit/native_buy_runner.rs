//! Native guarded entry into the shared tiny BUY state machine.
use super::process_buy;
use crate::execution_canary_route::NativeBuyGuard;
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter;
#[cfg(test)]
use crate::execution_submit_adapter::{ExecutionBuildPlanMetadata, ExecutionSubmitAdapter};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::SqliteStore;

pub(crate) async fn process_native_buy_state_machine_for_route(
    config: &ExecutionConfig, store: &SqliteStore, signal: &CopySignalRow,
    now: DateTime<Utc>, guard: &NativeBuyGuard,
) -> Result<ExecutionCanaryStateMachineSummary> {
    #[cfg(test)]
    if let Some(runner) = guard.mock_io().and_then(|mock| mock.runner.as_ref()) {
        return process_buy(config, store, signal, now, Some(guard), &runner.adapter, None).await;
    }
    let adapter = JupiterMetisDryRunExecutionAdapter::new(config.clone());
    process_buy(config, store, signal, now, Some(guard), &adapter, None).await
}

#[cfg(test)]
pub(crate) async fn process_native_buy_with_mock_quote_and_adapter<A: ExecutionSubmitAdapter>(
    config: &ExecutionConfig, store: &SqliteStore, signal: &CopySignalRow,
    now: DateTime<Utc>, guard: &NativeBuyGuard, adapter: &A,
    refreshed_quote: ExecutionBuildPlanMetadata,
) -> Result<ExecutionCanaryStateMachineSummary> {
    process_buy(config, store, signal, now, Some(guard), adapter, Some(refreshed_quote)).await
}
