use crate::execution_signing_envelope::{
    validate_execution_signing_envelope, ExecutionSigningEnvelope,
};
use crate::execution_submit_adapter::{
    ExecutionSubmitAdapter, ExecutionSubmitRequest, ExecutionTransactionPlan,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    SqliteStore, EXECUTION_ERROR_SIGNING_ENVELOPE_FAILED, EXECUTION_STATUS_CANARY_FAILED,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ExecutionSigningEnvelopeOutcome {
    pub(crate) source_refusal: Option<crate::execution_source_sell_guard::Refusal>,
    pub(crate) built: usize,
    pub(crate) failed: usize,
    pub(crate) envelope: Option<ExecutionSigningEnvelope>,
    pub(crate) envelope_id: Option<String>,
    pub(crate) envelope_mode: Option<String>,
    pub(crate) error: Option<String>,
}

pub(crate) fn record_execution_signing_envelope<A: ExecutionSubmitAdapter>(
    store: &SqliteStore,
    adapter: &A,
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
    now: DateTime<Utc>,
) -> Result<ExecutionSigningEnvelopeOutcome> {
    if let Err(error) = crate::execution_source_sell_guard::signing(store, request, plan) {
        return match error.downcast::<crate::execution_source_sell_guard::Refusal>() {
            Ok(refusal) => {
                crate::execution_source_sell_guard::retry::retire(store, now, &refusal)?;
                Ok(ExecutionSigningEnvelopeOutcome {
                    error: Some(refusal.to_string()),
                    source_refusal: Some(refusal),
                    ..Default::default()
                })
            }
            Err(error) => Err(error),
        };
    }
    let policy_check = if request.side.eq_ignore_ascii_case("buy")
        && (request.metadata.protected_capital.is_some()
            || adapter
                .native_floor_config()
                .is_ok_and(crate::execution_native_floor_policy::protected::enabled))
    {
        adapter
            .native_floor_config()
            .and_then(|config| {
                crate::execution_native_floor_policy::protected::current(
                    store, config, request, now,
                )
            })
            .map(|_| ())
    } else {
        Ok(())
    };
    match policy_check
        .and_then(|_| adapter.build_signing_envelope(request, plan))
        .and_then(|envelope| {
            validate_execution_signing_envelope(&envelope, request, plan)?;
            crate::execution_priority_fee_proof::persist(
                store,
                request,
                plan,
                &envelope,
                adapter.priority_fee_cap(),
                now,
            )?;
            Ok(envelope)
        }) {
        Ok(envelope) => {
            let envelope_id = envelope.envelope_id.clone();
            let envelope_mode = envelope.mode.clone();
            Ok(ExecutionSigningEnvelopeOutcome {
                built: 1,
                envelope: Some(envelope),
                envelope_id: Some(envelope_id),
                envelope_mode: Some(envelope_mode),
                ..ExecutionSigningEnvelopeOutcome::default()
            })
        }
        Err(error) => record_signing_envelope_failure(store, request, now, error.to_string()),
    }
}

fn record_signing_envelope_failure(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    now: DateTime<Utc>,
    error: String,
) -> Result<ExecutionSigningEnvelopeOutcome> {
    let order = store.mark_execution_canary_failed(
        &request.order_id,
        now,
        EXECUTION_ERROR_SIGNING_ENVELOPE_FAILED,
        &error,
    )?;
    Ok(ExecutionSigningEnvelopeOutcome {
        failed: usize::from(order.status == EXECUTION_STATUS_CANARY_FAILED),
        error: Some(error),
        ..ExecutionSigningEnvelopeOutcome::default()
    })
}
