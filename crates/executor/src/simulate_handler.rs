use serde_json::Value;
use tracing::debug;

use crate::common_contract::{validate_common_contract_inputs, CommonContractInputs};
use crate::reject_mapping::{
    map_common_contract_validation_error_to_reject, map_parsed_upstream_reject,
    map_request_validation_error_to_reject, map_simulate_response_validation_error_to_reject,
};
use crate::request_types::SimulateRequest;
use crate::request_validation::validate_simulate_request_basics;
use crate::route_backend::UpstreamAction;
use crate::route_executor::{
    execute_route_action, RouteActionPayloadExpectations, RouteSubmitExecutionContext,
};
use crate::route_normalization::normalize_route;
use crate::simulate_response::{
    build_simulate_success_payload, resolve_simulate_response_detail,
    validate_simulate_response_identity, validate_simulate_response_route_and_contract,
};
use crate::upstream_outcome::{parse_upstream_outcome, UpstreamOutcome};
use crate::{AppState, Reject};

pub(crate) async fn handle_simulate(
    state: &AppState,
    request: &SimulateRequest,
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
    validate_simulate_request_basics(
        request.action.as_deref(),
        request.dry_run,
        request.signal_ts.as_str(),
        request.request_id.as_str(),
        request.signal_id.as_str(),
    )
    .map_err(map_request_validation_error_to_reject)?;

    let route = normalize_route(request.route.as_str());
    debug!(
        route = %route,
        signal_id = %request.signal_id,
        "handling simulate request"
    );
    let backend_response =
        execute_route_action(
            state,
            route.as_str(),
            UpstreamAction::Simulate,
            raw_body,
            None,
            RouteActionPayloadExpectations {
                route_hint: Some(route.as_str()),
                request_id: Some(request.request_id.as_str()),
                signal_id: Some(request.signal_id.as_str()),
                client_order_id: None,
                side: Some(request.side.as_str()),
                token: Some(request.token.as_str()),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await?;
    match parse_upstream_outcome(&backend_response, "simulation_rejected") {
        UpstreamOutcome::Reject(reject) => return Err(map_parsed_upstream_reject(reject)),
        UpstreamOutcome::Success => {}
    }

    validate_simulate_response_route_and_contract(
        &backend_response,
        route.as_str(),
        state.config.contract_version.as_str(),
    )
    .map_err(map_simulate_response_validation_error_to_reject)?;

    validate_simulate_response_identity(
        &backend_response,
        request.request_id.as_str(),
        request.signal_id.as_str(),
        request.side.as_str(),
        request.token.as_str(),
    )
    .map_err(map_simulate_response_validation_error_to_reject)?;

    let detail = resolve_simulate_response_detail(&backend_response, "adapter_simulation_ok")
        .map_err(map_simulate_response_validation_error_to_reject)?;

    Ok(build_simulate_success_payload(
        route.as_str(),
        state.config.contract_version.as_str(),
        request.request_id.as_str(),
        request.signal_id.as_str(),
        request.side.as_str(),
        request.token.as_str(),
        detail.as_str(),
    ))
}
