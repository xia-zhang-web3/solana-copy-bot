use crate::execution_signing_envelope::ExecutionSigningEnvelope;
use crate::execution_submit_adapter::{
    dry_run_no_send_submit_intent, record_submit_transport_outcome, ExecutionSubmitAdapter,
    ExecutionSubmitPlan, ExecutionSubmitRequest, ExecutionSubmitTransportOutcome,
    RpcExecutionSubmitTransport,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    SqliteStore, EXECUTION_ERROR_SUBMIT_PLAN_FAILED, EXECUTION_STATUS_CANARY_FAILED,
    EXECUTION_STATUS_CANARY_SUBMIT_DISABLED,
};

pub(crate) const TINY_SUBMIT_RETRY_AFTER_RPC_NOT_SENT_REASON: &str =
    "retry_after_rpc_submit_not_sent";

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionSubmitPlanOutcome {
    pub(crate) pre_submit_refusal: Option<Box<crate::execution_submit_refusal::PreSubmitRefusal>>,
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

#[derive(Clone)]
pub(crate) struct ExecutionTinySubmitGate {
    pub(crate) buy_safety_config: Option<copybot_config::ExecutionConfig>,
    pub(crate) allow_rpc_submit: bool,
    pub(crate) pretrade_max_priority_fee_lamports: u64,
    pub(crate) pretrade_min_sol_reserve: f64,
    pub(crate) execution_wallet_pubkey: String,
    pub(crate) submit_timeout_ms: u64,
}

impl Default for ExecutionTinySubmitGate {
    fn default() -> Self {
        Self {
            buy_safety_config: None,
            allow_rpc_submit: false,
            pretrade_max_priority_fee_lamports: 0,
            pretrade_min_sol_reserve: 0.0, // invalid if a caller enables tiny BUY without policy
            execution_wallet_pubkey: String::new(),
            submit_timeout_ms: 3_000,
        }
    }
}

impl ExecutionTinySubmitGate {
    pub(crate) fn from_config(config: &copybot_config::ExecutionConfig) -> Self {
        Self {
            buy_safety_config: Some(config.clone()),
            allow_rpc_submit: config.canary_tiny_submit_enabled,
            pretrade_max_priority_fee_lamports: config.pretrade_max_priority_fee_lamports,
            pretrade_min_sol_reserve: config.pretrade_min_sol_reserve,
            execution_wallet_pubkey: config.execution_signer_pubkey.clone(),
            submit_timeout_ms: config.submit_timeout_ms,
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
    let state = match crate::execution_tiny_submit_state::eligible(store, request) {
        Ok(state) => state,
        Err(reason) => return Ok(crate::execution_tiny_submit_state::reject(reason)),
    };
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
            if let Some(config) = gate.buy_safety_config.as_ref() {
                if let Err(error) = crate::execution_native_floor_policy::protected::current(
                    store, config, request, now,
                ) {
                    return record_submit_plan_failure(store, request, now, error.to_string());
                }
            }
            if let Err(error) = crate::execution_priority_fee_proof::validate_submit(
                store,
                request,
                envelope,
                &intent,
                gate.pretrade_max_priority_fee_lamports,
            ) {
                return record_submit_plan_failure(store, request, now, error.to_string());
            }
            if let Err(error) = crate::execution_native_floor_policy::verify_submit_payload(
                request,
                &intent.signed_transaction_base64,
                gate.pretrade_min_sol_reserve,
                &gate.execution_wallet_pubkey,
            ) {
                return record_submit_plan_failure(store, request, now, error.to_string());
            }
            if let Some(rejection) = crate::execution_initial_sol_submit::before_send(
                store, request, envelope, &intent, gate, transport, &state, now,
            )
            .await
            {
                return Ok(rejection);
            }
            crate::execution_tiny_submit_state::dispatch::send(
                store, request, &intent, envelope, gate, transport, &state, now,
            )
            .await
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

pub(crate) fn record_submit_plan_failure(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    now: DateTime<Utc>,
    error: String,
) -> Result<ExecutionSubmitPlanOutcome> {
    if let Some(dispatch) = store.load_execution_canary_dispatch(&request.order_id)? {
        return Ok(ExecutionSubmitPlanOutcome {
            submitted: 1,
            tx_signature: Some(dispatch.tx_signature),
            reason: Some("dispatch_outcome_unknown".into()),
            ..Default::default()
        });
    }
    if request.metadata.rpc_owned_sell.is_some() {
        return Ok(ExecutionSubmitPlanOutcome {
            reason: Some(error),
            ..crate::execution_tiny_submit_state::reject("owned_sell_submit_refused")
        });
    }
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
