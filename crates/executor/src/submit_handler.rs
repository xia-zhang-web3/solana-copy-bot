use chrono::Utc;
use serde_json::Value;
use tracing::{debug, warn};

use crate::backend_mode::ExecutorBackendMode;
use crate::common_contract::{validate_common_contract_inputs, CommonContractInputs};
use crate::fee_hints::{parse_response_fee_hint_fields, resolve_fee_hints, FeeHintInputs};
use crate::idempotency::SubmitClaimGuardOutcome;
use crate::reject_mapping::{
    map_common_contract_validation_error_to_reject, map_compute_budget_validation_error_to_reject,
    map_fee_hint_error_to_reject, map_fee_hint_field_parse_error_to_reject,
    map_forward_payload_build_error_to_reject, map_idempotency_error_to_reject,
    map_parsed_upstream_reject, map_request_validation_error_to_reject,
    map_slippage_validation_error_to_reject, map_submit_response_validation_error_to_reject,
    map_submit_tip_policy_error_to_reject, map_submit_transport_artifact_error_to_reject,
};
use crate::request_types::SubmitRequest;
use crate::request_validation::validate_submit_request_identity;
use crate::route_backend::UpstreamAction;
use crate::route_executor::{
    execute_route_action, RouteActionPayloadExpectations, RouteSubmitExecutionContext,
};
use crate::route_normalization::normalize_route;
use crate::route_policy::{classify_normalized_route, RouteKind};
use crate::send_rpc::send_signed_transaction_via_rpc;
use crate::submit_claim_guard::SubmitClaimGuard;
use crate::submit_deadline::SubmitDeadline;
use crate::submit_payload::{build_submit_success_payload, SubmitSuccessPayloadInputs};
use crate::submit_response::{
    resolve_submit_response_submitted_at, validate_submit_response_extended_identity,
    validate_submit_response_request_identity, validate_submit_response_route_and_contract,
};
use crate::submit_transport::{extract_submit_transport_artifact, SubmitTransportArtifact};
use crate::submit_verify::{verify_submitted_signature_visibility, SubmitSignatureVerification};
use crate::submit_verify_payload::submit_signature_verification_to_json;
use crate::tx_build::{
    build_submit_plan, ComputeBudgetBounds, SubmitBuildPlanError, SubmitBuildPlanInputs,
};
use crate::upstream_outcome::{parse_upstream_outcome, UpstreamOutcome};
use crate::{AppState, Reject};

async fn reject_after_claimed_submit_error<T>(
    submit_claim_guard: &mut SubmitClaimGuard,
    route: &str,
    request: &SubmitRequest,
    stage: &str,
    reject: Reject,
) -> std::result::Result<T, Reject> {
    if let Err(error) = submit_claim_guard.release_now().await {
        warn!(
            route = %route,
            signal_id = %request.signal_id,
            client_order_id = %request.client_order_id,
            stage = %stage,
            error = %error,
            "failed to release submit claim before returning submit error"
        );
    }
    Err(reject)
}

pub(crate) async fn handle_submit(
    state: &AppState,
    request: &SubmitRequest,
    raw_body: &[u8],
) -> std::result::Result<Value, Reject> {
    validate_common_contract_inputs(CommonContractInputs {
        request_contract_version: request.contract_version.as_deref(),
        expected_contract_version: state.config.contract_version.as_str(),
        route: request.route.as_str(),
        route_allowlist: &state.config.route_allowlist,
        submit_fastlane_enabled: state.config.submit_fastlane_enabled,
        side: request.side.as_str(),
        token: request.token.as_str(),
        notional_sol: request.notional_sol,
        max_notional_sol: state.config.max_notional_sol,
    })
    .map_err(map_common_contract_validation_error_to_reject)?;
    validate_submit_request_identity(
        request.signal_ts.as_str(),
        request.client_order_id.as_str(),
        request.request_id.as_str(),
        request.signal_id.as_str(),
    )
    .map_err(map_request_validation_error_to_reject)?;
    let route = normalize_route(request.route.as_str());
    let route_kind = classify_normalized_route(route.as_str());
    let bypass_submit_verify = matches!(route_kind, RouteKind::Paper)
        || state.config.backend_mode == ExecutorBackendMode::Mock;
    let submit_plan = build_submit_plan(SubmitBuildPlanInputs {
        route: route.as_str(),
        raw_body,
        requested_tip_lamports: request.tip_lamports,
        tip_max_lamports: crate::TIP_MAX_LAMPORTS,
        allow_nonzero_tip: state.config.allow_nonzero_tip,
        cu_limit: request.compute_budget.cu_limit,
        cu_price_micro_lamports: request.compute_budget.cu_price_micro_lamports,
        compute_budget_bounds: ComputeBudgetBounds {
            cu_limit_min: crate::CU_LIMIT_MIN,
            cu_limit_max: crate::CU_LIMIT_MAX,
            cu_price_min: crate::CU_PRICE_MIN,
            cu_price_max: crate::CU_PRICE_MAX,
        },
        slippage_bps: request.slippage_bps,
        route_slippage_cap_bps: request.route_slippage_cap_bps,
        slippage_epsilon: crate::POLICY_FLOAT_EPSILON,
    })
    .map_err(|error| match error {
        SubmitBuildPlanError::SlippagePolicy(inner) => {
            map_slippage_validation_error_to_reject(inner)
        }
        SubmitBuildPlanError::TipPolicy(inner) => map_submit_tip_policy_error_to_reject(inner),
        SubmitBuildPlanError::ComputeBudget(inner) => {
            map_compute_budget_validation_error_to_reject(inner)
        }
        SubmitBuildPlanError::ForwardPayload(inner) => {
            map_forward_payload_build_error_to_reject(inner)
        }
    })?;
    let instruction_plan = submit_plan.instruction_plan;
    let effective_tip_lamports = submit_plan.effective_tip_lamports;
    let tip_policy_code = submit_plan.tip_policy_code;
    debug!(
        route = %route,
        cu_limit = instruction_plan.compute_budget_cu_limit,
        cu_price_micro_lamports = instruction_plan.compute_budget_cu_price_micro_lamports,
        tip_instruction_lamports = ?instruction_plan.tip_instruction_lamports,
        tip_instruction_present = instruction_plan.tip_instruction_lamports.is_some(),
        "prepared submit instruction plan"
    );
    let forward_body = submit_plan.forward_body;
    if let Some(policy_code) = tip_policy_code {
        debug!(
            route = %route,
            policy_code = %policy_code,
            requested_tip_lamports = request.tip_lamports,
            effective_tip_lamports = effective_tip_lamports,
            "applied submit tip policy"
        );
    }
    debug!(
        route = %route,
        signal_id = %request.signal_id,
        client_order_id = %request.client_order_id,
        "handling submit request"
    );
    let submit_deadline = SubmitDeadline::new(state.config.submit_total_budget_ms);
    let mut submit_claim_guard = match state
        .idempotency
        .load_cached_or_claim_submit_guard_async(
            request.client_order_id.as_str(),
            request.request_id.as_str(),
            state.config.idempotency_claim_ttl_sec,
        )
        .await
    {
        Ok(SubmitClaimGuardOutcome::Cached(cached_response)) => {
            debug!(
                route = %route,
                signal_id = %request.signal_id,
                client_order_id = %request.client_order_id,
                "serving cached idempotent submit response"
            );
            return Ok(cached_response);
        }
        Ok(SubmitClaimGuardOutcome::Claimed(submit_claim_guard)) => submit_claim_guard,
        Ok(SubmitClaimGuardOutcome::InFlight) => {
            return Err(Reject::retryable(
                "submit_in_flight",
                format!(
                    "client_order_id={} is already being processed",
                    request.client_order_id
                ),
            ));
        }
        Err(error) => return Err(map_idempotency_error_to_reject(error)),
    };
    let backend_response = match execute_route_action(
        state,
        route.as_str(),
        UpstreamAction::Submit,
        forward_body.as_slice(),
        Some(&submit_deadline),
        RouteActionPayloadExpectations {
            route_hint: Some(route.as_str()),
            request_id: Some(request.request_id.as_str()),
            signal_id: Some(request.signal_id.as_str()),
            client_order_id: Some(request.client_order_id.as_str()),
            side: Some(request.side.as_str()),
            token: Some(request.token.as_str()),
        },
        RouteSubmitExecutionContext {
            instruction_plan: Some(instruction_plan),
            expected_slippage_bps: Some(request.slippage_bps),
            expected_route_slippage_cap_bps: Some(request.route_slippage_cap_bps),
        },
    )
    .await
    {
        Ok(response) => response,
        Err(reject) => {
            return reject_after_claimed_submit_error(
                &mut submit_claim_guard,
                route.as_str(),
                request,
                "forward_submit",
                reject,
            )
            .await;
        }
    };
    match parse_upstream_outcome(&backend_response, "submit_adapter_rejected") {
        UpstreamOutcome::Reject(reject) => {
            return reject_after_claimed_submit_error(
                &mut submit_claim_guard,
                route.as_str(),
                request,
                "parse_upstream_outcome",
                map_parsed_upstream_reject(reject),
            )
            .await;
        }
        UpstreamOutcome::Success => {}
    }

    let submit_transport_artifact = match extract_submit_transport_artifact(&backend_response) {
        Ok(artifact) => artifact,
        Err(error) => {
            return reject_after_claimed_submit_error(
                &mut submit_claim_guard,
                route.as_str(),
                request,
                "extract_submit_transport_artifact",
                map_submit_transport_artifact_error_to_reject(error),
            )
            .await;
        }
    };

    if matches!(
        submit_transport_artifact,
        SubmitTransportArtifact::UpstreamSignature(_)
    ) {
        submit_claim_guard.retain_claim_on_drop();
    }

    if let Err(error) = validate_submit_response_route_and_contract(
        &backend_response,
        route.as_str(),
        state.config.contract_version.as_str(),
    )
    {
        return reject_after_claimed_submit_error(
            &mut submit_claim_guard,
            route.as_str(),
            request,
            "validate_submit_response_route_and_contract",
            map_submit_response_validation_error_to_reject(error),
        )
        .await;
    }

    if let Err(error) = validate_submit_response_request_identity(
        &backend_response,
        request.client_order_id.as_str(),
        request.request_id.as_str(),
    )
    {
        return reject_after_claimed_submit_error(
            &mut submit_claim_guard,
            route.as_str(),
            request,
            "validate_submit_response_request_identity",
            map_submit_response_validation_error_to_reject(error),
        )
        .await;
    }

    if let Err(error) = validate_submit_response_extended_identity(
        &backend_response,
        request.signal_id.as_str(),
        request.side.as_str(),
        request.token.as_str(),
    )
    {
        return reject_after_claimed_submit_error(
            &mut submit_claim_guard,
            route.as_str(),
            request,
            "validate_submit_response_extended_identity",
            map_submit_response_validation_error_to_reject(error),
        )
        .await;
    }

    let (tx_signature, submit_transport) = match submit_transport_artifact {
        SubmitTransportArtifact::UpstreamSignature(value) => {
            let submit_transport = if matches!(route_kind, RouteKind::Paper) {
                "executor_paper_internal"
            } else if state.config.backend_mode == ExecutorBackendMode::Mock {
                "executor_mock_internal"
            } else {
                "upstream_signature"
            };
            (value, submit_transport)
        }
        SubmitTransportArtifact::SignedTransactionBase64(value) => {
            let signature = match send_signed_transaction_via_rpc(
                state,
                route.as_str(),
                value.as_str(),
                Some(&submit_deadline),
            )
            .await
            {
                Ok(signature) => signature,
                Err(reject) => {
                    return reject_after_claimed_submit_error(
                        &mut submit_claim_guard,
                        route.as_str(),
                        request,
                        "send_signed_transaction_via_rpc",
                        reject,
                    )
                    .await;
                }
            };
            submit_claim_guard.retain_claim_on_drop();
            (signature, "adapter_send_rpc")
        }
    };

    let submit_signature_verify = if bypass_submit_verify {
        SubmitSignatureVerification::Skipped
    } else {
        match verify_submitted_signature_visibility(
            state,
            route.as_str(),
            tx_signature.as_str(),
            Some(&submit_deadline),
        )
        .await
        {
            Ok(verification) => verification,
            Err(reject) => {
                return reject_after_claimed_submit_error(
                    &mut submit_claim_guard,
                    route.as_str(),
                    request,
                    "verify_submitted_signature_visibility",
                    reject,
                )
                .await;
            }
        }
    };

    let submitted_at = match resolve_submit_response_submitted_at(&backend_response, Utc::now()) {
        Ok(submitted_at) => submitted_at,
        Err(error) => {
            return reject_after_claimed_submit_error(
                &mut submit_claim_guard,
                route.as_str(),
                request,
                "resolve_submit_response_submitted_at",
                map_submit_response_validation_error_to_reject(error),
            )
            .await;
        }
    };

    let parsed_response_fee_hints = match parse_response_fee_hint_fields(&backend_response) {
        Ok(parsed) => parsed,
        Err(error) => {
            return reject_after_claimed_submit_error(
                &mut submit_claim_guard,
                route.as_str(),
                request,
                "parse_response_fee_hint_fields",
                map_fee_hint_field_parse_error_to_reject(error),
            )
            .await;
        }
    };

    let resolved_fee_hints = match resolve_fee_hints(FeeHintInputs {
        response_network_fee_lamports: parsed_response_fee_hints.network_fee_lamports,
        response_base_fee_lamports: parsed_response_fee_hints.base_fee_lamports,
        response_priority_fee_lamports: parsed_response_fee_hints.priority_fee_lamports,
        response_ata_create_rent_lamports: parsed_response_fee_hints.ata_create_rent_lamports,
        request_cu_limit: request.compute_budget.cu_limit,
        request_cu_price_micro_lamports: request.compute_budget.cu_price_micro_lamports,
        default_base_fee_lamports: crate::DEFAULT_BASE_FEE_LAMPORTS,
    })
    {
        Ok(fee_hints) => fee_hints,
        Err(error) => {
            return reject_after_claimed_submit_error(
                &mut submit_claim_guard,
                route.as_str(),
                request,
                "resolve_fee_hints",
                map_fee_hint_error_to_reject(error),
            )
            .await;
        }
    };

    let submit_signature_verify_json =
        submit_signature_verification_to_json(&submit_signature_verify);
    let submitted_at_rfc3339 = submitted_at.to_rfc3339();
    let response = build_submit_success_payload(SubmitSuccessPayloadInputs {
        route: route.as_str(),
        contract_version: state.config.contract_version.as_str(),
        client_order_id: request.client_order_id.as_str(),
        request_id: request.request_id.as_str(),
        signal_id: request.signal_id.as_str(),
        side: request.side.as_str(),
        token: request.token.as_str(),
        tx_signature: tx_signature.as_str(),
        submit_transport,
        submitted_at_rfc3339: submitted_at_rfc3339.as_str(),
        slippage_bps: request.slippage_bps,
        requested_tip_lamports: request.tip_lamports,
        effective_tip_lamports,
        tip_policy_code,
        cu_limit: request.compute_budget.cu_limit,
        cu_price_micro_lamports: request.compute_budget.cu_price_micro_lamports,
        resolved_fee_hints,
        submit_signature_verify: submit_signature_verify_json,
    });
    let inserted = match state
        .idempotency
        .store_submit_response_async(
            request.client_order_id.as_str(),
            request.request_id.as_str(),
            &response,
        )
        .await
    {
        Ok(inserted) => inserted,
        Err(error) => {
            warn!(
                route = %route,
                signal_id = %request.signal_id,
                client_order_id = %request.client_order_id,
                error = %error,
                "failed to persist submit idempotency record"
            );
            return reject_after_claimed_submit_error(
                &mut submit_claim_guard,
                route.as_str(),
                request,
                "store_submit_response",
                map_idempotency_error_to_reject(error),
            )
            .await;
        }
    };
    if !inserted {
        warn!(
            route = %route,
            signal_id = %request.signal_id,
            client_order_id = %request.client_order_id,
            "idempotency row already exists; keeping first stored response"
        );
        let canonical = match state
            .idempotency
            .load_submit_response_async(request.client_order_id.as_str())
            .await
        {
            Ok(Some(canonical)) => canonical,
            Ok(None) => {
                return reject_after_claimed_submit_error(
                    &mut submit_claim_guard,
                    route.as_str(),
                    request,
                    "load_canonical_submit_response_missing",
                    Reject::retryable(
                        "idempotency_store_unavailable",
                        "idempotency conflict detected but canonical response missing",
                    ),
                )
                .await;
            }
            Err(error) => {
                return reject_after_claimed_submit_error(
                    &mut submit_claim_guard,
                    route.as_str(),
                    request,
                    "load_canonical_submit_response",
                    map_idempotency_error_to_reject(error),
                )
                .await;
            }
        };
        submit_claim_guard.release_claim_on_drop();
        if let Err(error) = submit_claim_guard.release_now().await {
            warn!(
                route = %route,
                signal_id = %request.signal_id,
                client_order_id = %request.client_order_id,
                error = %error,
                "failed to release submit claim after idempotency conflict"
            );
        }
        return Ok(canonical);
    }
    submit_claim_guard.release_claim_on_drop();
    if let Err(error) = submit_claim_guard.release_now().await {
        warn!(
            route = %route,
            signal_id = %request.signal_id,
            client_order_id = %request.client_order_id,
            error = %error,
            "failed to release submit claim after storing idempotency response"
        );
    }
    Ok(response)
}
