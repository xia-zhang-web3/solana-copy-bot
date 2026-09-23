//! Signed BUY construction after the durable source and safety gates.
use super::tiny_submit::native_current;
use super::NativeBuyGuard;
use crate::execution_build_plan_metadata::record_execution_build_plan_metadata;
use crate::execution_canary_signing_contract::record_execution_signing_envelope;
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_source_sell_guard as source_guard;
use crate::execution_submit_adapter::{ExecutionSubmitAdapter, ExecutionSubmitRequest};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    SqliteStore, EXECUTION_ERROR_BUILD_FAILED, EXECUTION_ERROR_SIMULATION_FAILED,
    EXECUTION_SIMULATION_STATUS_FAILED, EXECUTION_STATUS_CANARY_FAILED,
};

pub(super) async fn build_simulated_signed_envelope<A: ExecutionSubmitAdapter>(
    store: &SqliteStore,
    adapter: &A,
    request: &ExecutionSubmitRequest,
    now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary,
    native: Option<&NativeBuyGuard>,
) -> Result<Option<crate::execution_signing_envelope::ExecutionSigningEnvelope>> {
    if !native_current(native, store)? {
        summary.skipped_reason = Some("native_buy_decision_changed");
        return Ok(None);
    }
    if source_guard::retry::state_result(
        store,
        now,
        source_guard::request(
            store,
            request,
            &[copybot_storage_core::EXECUTION_STATUS_CANARY_CANDIDATE],
        ),
        summary,
    )?
    .is_none()
    {
        return Ok(None);
    }
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
    let Some(source) = source_guard::retry::state_result(
        store,
        now,
        source_guard::request(
            store,
            request,
            &[copybot_storage_core::EXECUTION_STATUS_CANARY_BUILT],
        ),
        summary,
    )?
    else {
        return Ok(None);
    };

    let simulation_result = adapter.simulate_transaction_plan(&plan).await;
    if !native_current(native, store)? {
        summary.skipped_reason = Some("native_buy_decision_changed");
        return Ok(None);
    }
    if source_guard::retry::state_result(
        store,
        now,
        source_guard::recheck(source.as_ref(), store),
        summary,
    )?
    .is_none()
    {
        return Ok(None);
    }
    let simulation = match simulation_result {
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

    if !native_current(native, store)? {
        summary.skipped_reason = Some("native_buy_decision_changed");
        return Ok(None);
    }
    let signing = record_execution_signing_envelope(store, adapter, request, &plan, now)?;
    if let Some(refusal) = signing.source_refusal.as_ref() {
        source_guard::record_state(refusal, summary);
        return Ok(None);
    }
    summary.signing_envelope_built = signing.built;
    summary.last_signing_envelope_id = signing.envelope_id;
    summary.last_signing_envelope_mode = signing.envelope_mode;
    summary.failed = signing.failed;
    summary.last_error = signing.error;
    Ok(signing.envelope)
}

pub(super) fn mark_canary_failed(
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
