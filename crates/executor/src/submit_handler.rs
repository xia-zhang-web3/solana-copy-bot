use chrono::Utc;
use serde_json::Value;
use tracing::{debug, warn};

use crate::common_contract::{validate_common_contract_inputs, CommonContractInputs};
use crate::fee_hints::{parse_response_fee_hint_fields, resolve_fee_hints, FeeHintInputs};
use crate::idempotency::SubmitClaimOutcome;
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
use crate::submit_claim_guard::SubmitClaimGuard;
use crate::submit_deadline::SubmitDeadline;
use crate::submit_payload::{build_submit_success_payload, SubmitSuccessPayloadInputs};
use crate::submit_response::{
    resolve_submit_response_submitted_at, validate_submit_response_request_identity,
    validate_submit_response_route_and_contract,
};
use crate::send_rpc::send_signed_transaction_via_rpc;
use crate::submit_transport::{extract_submit_transport_artifact, SubmitTransportArtifact};
use crate::submit_verify::verify_submitted_signature_visibility;
use crate::submit_verify_payload::submit_signature_verification_to_json;
use crate::tx_build::{
    build_submit_plan, ComputeBudgetBounds, SubmitBuildPlanError, SubmitBuildPlanInputs,
};
use crate::upstream_outcome::{parse_upstream_outcome, UpstreamOutcome};
use crate::{AppState, Reject};

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
    let _submit_claim_guard = match state.idempotency.load_cached_or_claim_submit(
        request.client_order_id.as_str(),
        request.request_id.as_str(),
        state.config.idempotency_claim_ttl_sec,
    ) {
        Ok(SubmitClaimOutcome::Cached(cached_response)) => {
            debug!(
                route = %route,
                signal_id = %request.signal_id,
                client_order_id = %request.client_order_id,
                "serving cached idempotent submit response"
            );
            return Ok(cached_response);
        }
        Ok(SubmitClaimOutcome::Claimed) => SubmitClaimGuard::new(
            state.idempotency.clone(),
            request.client_order_id.as_str(),
            request.request_id.as_str(),
        ),
        Ok(SubmitClaimOutcome::InFlight) => {
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
    let backend_response = execute_route_action(
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
        },
    )
    .await?;
    match parse_upstream_outcome(&backend_response, "submit_adapter_rejected") {
        UpstreamOutcome::Reject(reject) => return Err(map_parsed_upstream_reject(reject)),
        UpstreamOutcome::Success => {}
    }

    validate_submit_response_route_and_contract(
        &backend_response,
        route.as_str(),
        state.config.contract_version.as_str(),
    )
    .map_err(map_submit_response_validation_error_to_reject)?;

    validate_submit_response_request_identity(
        &backend_response,
        request.client_order_id.as_str(),
        request.request_id.as_str(),
    )
    .map_err(map_submit_response_validation_error_to_reject)?;

    let (tx_signature, submit_transport) =
        match extract_submit_transport_artifact(&backend_response)
            .map_err(map_submit_transport_artifact_error_to_reject)?
        {
            SubmitTransportArtifact::UpstreamSignature(value) => (value, "upstream_signature"),
            SubmitTransportArtifact::SignedTransactionBase64(value) => {
                let signature = send_signed_transaction_via_rpc(
                    state,
                    route.as_str(),
                    value.as_str(),
                    Some(&submit_deadline),
                )
                .await?;
                (signature, "adapter_send_rpc")
            }
        };

    let submit_signature_verify = verify_submitted_signature_visibility(
        state,
        route.as_str(),
        tx_signature.as_str(),
        Some(&submit_deadline),
    )
    .await?;

    let submitted_at = resolve_submit_response_submitted_at(&backend_response, Utc::now())
        .map_err(map_submit_response_validation_error_to_reject)?;

    let parsed_response_fee_hints = parse_response_fee_hint_fields(&backend_response)
        .map_err(map_fee_hint_field_parse_error_to_reject)?;

    let resolved_fee_hints = resolve_fee_hints(FeeHintInputs {
        response_network_fee_lamports: parsed_response_fee_hints.network_fee_lamports,
        response_base_fee_lamports: parsed_response_fee_hints.base_fee_lamports,
        response_priority_fee_lamports: parsed_response_fee_hints.priority_fee_lamports,
        response_ata_create_rent_lamports: parsed_response_fee_hints.ata_create_rent_lamports,
        request_cu_limit: request.compute_budget.cu_limit,
        request_cu_price_micro_lamports: request.compute_budget.cu_price_micro_lamports,
        default_base_fee_lamports: crate::DEFAULT_BASE_FEE_LAMPORTS,
    })
    .map_err(map_fee_hint_error_to_reject)?;

    let submit_signature_verify_json =
        submit_signature_verification_to_json(&submit_signature_verify);
    let submitted_at_rfc3339 = submitted_at.to_rfc3339();
    let response = build_submit_success_payload(SubmitSuccessPayloadInputs {
        route: route.as_str(),
        contract_version: state.config.contract_version.as_str(),
        client_order_id: request.client_order_id.as_str(),
        request_id: request.request_id.as_str(),
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
    let inserted = state
        .idempotency
        .store_submit_response(
            request.client_order_id.as_str(),
            request.request_id.as_str(),
            &response,
        )
        .map_err(|error| {
            warn!(
                route = %route,
                signal_id = %request.signal_id,
                client_order_id = %request.client_order_id,
                error = %error,
                "failed to persist submit idempotency record"
            );
            map_idempotency_error_to_reject(error)
        })?;
    if !inserted {
        warn!(
            route = %route,
            signal_id = %request.signal_id,
            client_order_id = %request.client_order_id,
            "idempotency row already exists; keeping first stored response"
        );
        let canonical = state
            .idempotency
            .load_submit_response(request.client_order_id.as_str())
            .map_err(map_idempotency_error_to_reject)?
            .ok_or_else(|| {
                Reject::retryable(
                    "idempotency_store_unavailable",
                    "idempotency conflict detected but canonical response missing",
                )
            })?;
        return Ok(canonical);
    }
    Ok(response)
}
