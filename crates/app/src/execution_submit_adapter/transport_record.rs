use super::{
    execution_submit_idempotency_key, ExecutionSubmitRequest, ExecutionSubmitTransportOutcome,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    SqliteStore, EXECUTION_STATUS_CANARY_SUBMITTED, EXECUTION_STATUS_CANARY_SUBMIT_DISABLED,
};

pub(crate) const SUBMITTED_UNKNOWN_NO_SIGNATURE_REASON: &str =
    "submit_transport_submitted_unknown_no_signature";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ExecutionSubmitTransportRecordOutcome {
    pub(crate) submitted: usize,
    pub(crate) submit_disabled: usize,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) tx_signature: Option<String>,
    pub(crate) reason: Option<String>,
}

pub(crate) fn record_submit_transport_outcome(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    outcome: ExecutionSubmitTransportOutcome,
    now: DateTime<Utc>,
) -> Result<ExecutionSubmitTransportRecordOutcome> {
    validate_transport_outcome_idempotency(request, &outcome)?;
    match outcome {
        ExecutionSubmitTransportOutcome::NotSent {
            idempotency_key,
            reason,
        } => record_not_sent(store, request, now, idempotency_key, reason),
        ExecutionSubmitTransportOutcome::SubmittedUnknown {
            idempotency_key,
            tx_signature,
        } => record_submitted_unknown(store, request, now, idempotency_key, tx_signature),
    }
}

fn record_not_sent(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    now: DateTime<Utc>,
    idempotency_key: String,
    reason: String,
) -> Result<ExecutionSubmitTransportRecordOutcome> {
    let order = store.mark_execution_canary_submit_disabled(&request.order_id, now, &reason)?;
    Ok(ExecutionSubmitTransportRecordOutcome {
        submit_disabled: usize::from(order.status == EXECUTION_STATUS_CANARY_SUBMIT_DISABLED),
        idempotency_key: Some(idempotency_key),
        reason: Some(reason),
        ..ExecutionSubmitTransportRecordOutcome::default()
    })
}

fn record_submitted_unknown(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    now: DateTime<Utc>,
    idempotency_key: String,
    tx_signature: Option<String>,
) -> Result<ExecutionSubmitTransportRecordOutcome> {
    let tx_signature = non_empty_signature(tx_signature)?;
    let order = match tx_signature.as_deref() {
        Some(signature) => {
            store.mark_execution_canary_submitted(&request.order_id, now, signature)?
        }
        None => store.mark_execution_canary_submitted_unknown(
            &request.order_id,
            now,
            SUBMITTED_UNKNOWN_NO_SIGNATURE_REASON,
        )?,
    };
    Ok(ExecutionSubmitTransportRecordOutcome {
        submitted: usize::from(order.status == EXECUTION_STATUS_CANARY_SUBMITTED),
        idempotency_key: Some(idempotency_key),
        tx_signature,
        reason: order.simulation_error,
        ..ExecutionSubmitTransportRecordOutcome::default()
    })
}

fn validate_transport_outcome_idempotency(
    request: &ExecutionSubmitRequest,
    outcome: &ExecutionSubmitTransportOutcome,
) -> Result<()> {
    let expected = execution_submit_idempotency_key(request);
    let actual = outcome.idempotency_key();
    if actual != expected {
        anyhow::bail!(
            "submit transport idempotency key mismatch: actual={actual} expected={expected}"
        );
    }
    Ok(())
}

fn non_empty_signature(tx_signature: Option<String>) -> Result<Option<String>> {
    let Some(signature) = tx_signature else {
        return Ok(None);
    };
    if signature.trim().is_empty() {
        anyhow::bail!("submit transport tx_signature must be non-empty when present");
    }
    Ok(Some(signature))
}
