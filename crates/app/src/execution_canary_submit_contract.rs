use crate::execution_signing_envelope::ExecutionSigningEnvelope;
use crate::execution_submit_adapter::{
    build_submit_transport_attempt, dry_run_no_send_submit_intent, record_submit_transport_outcome,
    ExecutionSubmitAdapter, ExecutionSubmitPlan, ExecutionSubmitRequest,
    ExecutionSubmitTransportOutcome, RpcExecutionSubmitTransport,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    SqliteStore, EXECUTION_ERROR_SUBMIT_PLAN_FAILED, EXECUTION_STATUS_CANARY_FAILED,
    EXECUTION_STATUS_CANARY_SUBMIT_DISABLED,
};

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionSubmitPlanOutcome {
    pub(crate) failed: usize,
    pub(crate) submitted: usize,
    pub(crate) submit_disabled: usize,
    pub(crate) submit_ready_rejected: usize,
    pub(crate) skipped_reason: Option<&'static str>,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) tx_signature: Option<String>,
    pub(crate) reason: Option<String>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionTinySubmitGate {
    pub(crate) allow_rpc_submit: bool,
    pub(crate) submit_timeout_ms: u64,
}

impl Default for ExecutionTinySubmitGate {
    fn default() -> Self {
        Self {
            allow_rpc_submit: false,
            submit_timeout_ms: 3_000,
        }
    }
}

pub(crate) fn record_execution_submit_plan<A: ExecutionSubmitAdapter>(
    store: &SqliteStore,
    adapter: &A,
    request: &ExecutionSubmitRequest,
    envelope: &ExecutionSigningEnvelope,
    now: DateTime<Utc>,
) -> Result<ExecutionSubmitPlanOutcome> {
    let submit_plan = match validated_submit_plan(adapter, request, envelope) {
        Ok(plan) => plan,
        Err(error) => {
            return record_submit_plan_failure(store, request, now, error.to_string());
        }
    };

    let idempotency_key = Some(submit_plan.idempotency_key().to_string());
    match submit_plan {
        ExecutionSubmitPlan::SubmitDisabled { reason, .. } => {
            record_submit_disabled(store, request, now, reason, idempotency_key)
        }
        ExecutionSubmitPlan::SubmitReady(intent) => {
            let transport_outcome = match dry_run_no_send_submit_intent(&intent, now) {
                Ok(outcome) => outcome,
                Err(error) => {
                    return record_submit_plan_failure(store, request, now, error.to_string());
                }
            };
            let reason = format!(
                "submit_ready_rejected_in_canary_dry_run:{}:{}:{}",
                intent.submit_route,
                intent
                    .tx_signature_hint
                    .as_deref()
                    .unwrap_or("no_tx_signature_hint"),
                transport_outcome.reason_label()
            );
            let record_outcome = match record_submit_transport_outcome(
                store,
                request,
                ExecutionSubmitTransportOutcome::NotSent {
                    idempotency_key: transport_outcome.idempotency_key().to_string(),
                    reason,
                },
                now,
            ) {
                Ok(outcome) => outcome,
                Err(error) => {
                    return record_submit_plan_failure(store, request, now, error.to_string());
                }
            };
            Ok(ExecutionSubmitPlanOutcome {
                submit_disabled: record_outcome.submit_disabled,
                submit_ready_rejected: record_outcome.submit_disabled,
                skipped_reason: Some("submit_ready_rejected_in_dry_run"),
                idempotency_key: record_outcome.idempotency_key,
                ..ExecutionSubmitPlanOutcome::default()
            })
        }
    }
}

pub(crate) async fn record_execution_tiny_submit_plan<A: ExecutionSubmitAdapter>(
    store: &SqliteStore,
    adapter: &A,
    request: &ExecutionSubmitRequest,
    envelope: &ExecutionSigningEnvelope,
    gate: &ExecutionTinySubmitGate,
    transport: &RpcExecutionSubmitTransport,
    now: DateTime<Utc>,
) -> Result<ExecutionSubmitPlanOutcome> {
    let submit_plan = match validated_submit_plan(adapter, request, envelope) {
        Ok(plan) => plan,
        Err(error) => {
            return record_submit_plan_failure(store, request, now, error.to_string());
        }
    };
    let idempotency_key = Some(submit_plan.idempotency_key().to_string());
    match submit_plan {
        ExecutionSubmitPlan::SubmitDisabled { reason, .. } => {
            record_submit_disabled(store, request, now, reason, idempotency_key)
        }
        ExecutionSubmitPlan::SubmitReady(intent) => {
            if !gate.allow_rpc_submit {
                let reason = format!(
                    "tiny_submit_gate_disabled:{}:{}",
                    intent.submit_route,
                    intent
                        .tx_signature_hint
                        .as_deref()
                        .unwrap_or("no_tx_signature_hint")
                );
                let record_outcome = record_submit_transport_outcome(
                    store,
                    request,
                    ExecutionSubmitTransportOutcome::NotSent {
                        idempotency_key: intent.idempotency_key.clone(),
                        reason,
                    },
                    now,
                )?;
                return Ok(ExecutionSubmitPlanOutcome {
                    submit_disabled: record_outcome.submit_disabled,
                    submit_ready_rejected: record_outcome.submit_disabled,
                    skipped_reason: Some("tiny_submit_gate_disabled"),
                    idempotency_key: record_outcome.idempotency_key,
                    reason: record_outcome.reason,
                    ..ExecutionSubmitPlanOutcome::default()
                });
            }
            let attempt = build_submit_transport_attempt(&intent, gate.submit_timeout_ms, now)?;
            let transport_outcome = match transport.submit(&attempt).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    return record_submit_plan_failure(store, request, now, error.to_string());
                }
            };
            let record_outcome =
                record_submit_transport_outcome(store, request, transport_outcome, now)?;
            Ok(ExecutionSubmitPlanOutcome {
                submitted: record_outcome.submitted,
                submit_disabled: record_outcome.submit_disabled,
                idempotency_key: record_outcome.idempotency_key,
                tx_signature: record_outcome.tx_signature,
                reason: record_outcome.reason,
                ..ExecutionSubmitPlanOutcome::default()
            })
        }
    }
}

fn validated_submit_plan<A: ExecutionSubmitAdapter>(
    adapter: &A,
    request: &ExecutionSubmitRequest,
    envelope: &ExecutionSigningEnvelope,
) -> Result<ExecutionSubmitPlan> {
    let plan = adapter.plan_submit_with_envelope(request, envelope)?;
    plan.validate_for_request(request)?;
    Ok(plan)
}

fn record_submit_plan_failure(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    now: DateTime<Utc>,
    error: String,
) -> Result<ExecutionSubmitPlanOutcome> {
    let order = store.mark_execution_canary_failed(
        &request.order_id,
        now,
        EXECUTION_ERROR_SUBMIT_PLAN_FAILED,
        &error,
    )?;
    Ok(ExecutionSubmitPlanOutcome {
        failed: usize::from(order.status == EXECUTION_STATUS_CANARY_FAILED),
        error: Some(error),
        ..ExecutionSubmitPlanOutcome::default()
    })
}

fn record_submit_disabled(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    now: DateTime<Utc>,
    reason: String,
    idempotency_key: Option<String>,
) -> Result<ExecutionSubmitPlanOutcome> {
    let order = store.mark_execution_canary_submit_disabled(&request.order_id, now, &reason)?;
    Ok(ExecutionSubmitPlanOutcome {
        submit_disabled: usize::from(order.status == EXECUTION_STATUS_CANARY_SUBMIT_DISABLED),
        idempotency_key,
        ..ExecutionSubmitPlanOutcome::default()
    })
}
