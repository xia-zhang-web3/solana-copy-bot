use axum::{
    body::Bytes,
    extract::State,
    http::HeaderMap,
    response::IntoResponse,
};

use crate::request_ingress::{parse_json_or_reject, verify_auth_or_reject};
use crate::request_types::{SimulateRequest, SubmitRequest};
use crate::response_envelope::success_or_reject_to_http;
use crate::simulate_handler::handle_simulate;
use crate::submit_handler::handle_submit;
use crate::AppState;

pub(crate) async fn simulate(
    State(state): State<AppState>,
    headers: HeaderMap,
    raw_body: Bytes,
) -> impl IntoResponse {
    if let Some(response) = verify_auth_or_reject(
        state.auth.as_ref(),
        &headers,
        raw_body.as_ref(),
        &state.config.contract_version,
    )
    .await
    {
        return response;
    }

    let request: SimulateRequest =
        match parse_json_or_reject(raw_body.as_ref(), &state.config.contract_version) {
            Ok(value) => value,
            Err(response) => return response,
        };

    success_or_reject_to_http(
        handle_simulate(&state, &request, raw_body.as_ref()).await,
        None,
        &state.config.contract_version,
    )
}

pub(crate) async fn submit(
    State(state): State<AppState>,
    headers: HeaderMap,
    raw_body: Bytes,
) -> impl IntoResponse {
    if let Some(response) = verify_auth_or_reject(
        state.auth.as_ref(),
        &headers,
        raw_body.as_ref(),
        &state.config.contract_version,
    )
    .await
    {
        return response;
    }

    let request: SubmitRequest =
        match parse_json_or_reject(raw_body.as_ref(), &state.config.contract_version) {
            Ok(value) => value,
            Err(response) => return response,
        };

    let client_order_id = request.client_order_id.clone();
    success_or_reject_to_http(
        handle_submit(&state, &request, raw_body.as_ref()).await,
        Some(client_order_id.as_str()),
        &state.config.contract_version,
    )
}
