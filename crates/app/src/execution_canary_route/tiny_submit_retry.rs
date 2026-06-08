use super::tiny_submit::apply_tiny_submit_confirm_path_outcome;
use crate::execution_build_plan_metadata::record_execution_build_plan_metadata;
use crate::execution_canary_signing_contract::record_execution_signing_envelope;
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_canary_submit_contract::ExecutionTinySubmitGate;
use crate::execution_submit_adapter::{
    build_tiny_submit_reconciliation_request, record_execution_tiny_submit_confirm_path,
    ExecutionSubmitAdapter, ExecutionSubmitRequest, JupiterMetisDryRunExecutionAdapter,
    RpcExecutionSubmitTransport,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ExecutionCanaryOrder, SqliteStore, EXECUTION_ERROR_BUILD_FAILED,
    EXECUTION_ERROR_SIMULATION_FAILED, EXECUTION_SIMULATION_STATUS_FAILED,
    EXECUTION_STATUS_CANARY_EXPIRED, EXECUTION_STATUS_CANARY_FAILED,
    EXECUTION_STATUS_CANARY_SIMULATED,
};

pub(super) const TINY_SUBMIT_RETRY_AFTER_UNKNOWN_SUBMIT_TIMEOUT_REASON: &str =
    "retry_after_unknown_submit_timeout";

pub(super) fn is_tiny_submit_retry_ready(order: &ExecutionCanaryOrder) -> bool {
    order.status == EXECUTION_STATUS_CANARY_SIMULATED
        && order
            .tx_signature
            .as_deref()
            .is_none_or(|signature| signature.trim().is_empty())
        && order.simulation_error.as_deref()
            == Some(TINY_SUBMIT_RETRY_AFTER_UNKNOWN_SUBMIT_TIMEOUT_REASON)
}

pub(super) async fn retry_existing_simulated_tiny_submit_order(
    config: &ExecutionConfig,
    store: &SqliteStore,
    order: &ExecutionCanaryOrder,
    now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<()> {
    if !is_tiny_submit_retry_ready(order) {
        return Ok(());
    }
    if order.attempt > config.max_submit_attempts.max(1) {
        let order = store.mark_execution_canary_expired(
            &order.order_id,
            now,
            "submit_retry_budget_exhausted",
        )?;
        if order.status == EXECUTION_STATUS_CANARY_EXPIRED {
            summary.expired += 1;
            summary.skipped_reason = Some("submit_retry_budget_exhausted");
        }
        return Ok(());
    }
    let request = build_tiny_submit_reconciliation_request(store, config, order)?;
    let adapter = JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let Some(envelope) =
        build_retry_signed_envelope(store, &adapter, &request, now, summary).await?
    else {
        return Ok(());
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
    apply_tiny_submit_confirm_path_outcome(summary, outcome);
    Ok(())
}

async fn build_retry_signed_envelope(
    store: &SqliteStore,
    adapter: &JupiterMetisDryRunExecutionAdapter,
    request: &ExecutionSubmitRequest,
    now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<Option<crate::execution_signing_envelope::ExecutionSigningEnvelope>> {
    let plan = match adapter.build_transaction_plan(request) {
        Ok(plan) => plan,
        Err(error) => {
            mark_retry_failed(
                store,
                request,
                now,
                EXECUTION_ERROR_BUILD_FAILED,
                error.to_string(),
            )?;
            summary.failed = 1;
            summary.last_error = Some(error.to_string());
            return Ok(None);
        }
    };
    record_execution_build_plan_metadata(store, &plan, now)?;

    let simulation = match adapter.simulate_transaction_plan(&plan).await {
        Ok(simulation) => simulation,
        Err(error) => {
            mark_retry_failed(
                store,
                request,
                now,
                EXECUTION_ERROR_SIMULATION_FAILED,
                error.to_string(),
            )?;
            summary.simulated += 1;
            summary.failed = 1;
            summary.last_error = Some(error.to_string());
            return Ok(None);
        }
    };
    summary.simulated += 1;
    if simulation.status == EXECUTION_SIMULATION_STATUS_FAILED {
        let error = simulation
            .error
            .unwrap_or_else(|| "simulation_failed".to_string());
        mark_retry_failed(
            store,
            request,
            now,
            EXECUTION_ERROR_SIMULATION_FAILED,
            error.clone(),
        )?;
        summary.failed = 1;
        summary.last_error = Some(error);
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

fn mark_retry_failed(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    now: DateTime<Utc>,
    code: &str,
    error: String,
) -> Result<()> {
    let order = store.mark_execution_canary_failed(&request.order_id, now, code, &error)?;
    if order.status != EXECUTION_STATUS_CANARY_FAILED {
        anyhow::bail!("execution canary retry failure did not mark order failed");
    }
    Ok(())
}
