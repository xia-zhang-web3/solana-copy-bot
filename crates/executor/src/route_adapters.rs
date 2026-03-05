use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tracing::debug;
#[cfg(test)]
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use crate::backend_mode::ExecutorBackendMode;
use crate::route_backend::UpstreamAction;
use crate::route_executor::{
    RouteActionPayloadExpectations, RouteExecutorKind, RouteSubmitExecutionContext,
};
use crate::route_normalization::normalize_route;
use crate::submit_deadline::SubmitDeadline;
use crate::tx_build::SubmitInstructionPlan;
use crate::upstream_forward::forward_to_upstream;
use crate::{AppState, Reject};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteAdapter {
    Paper,
    Rpc,
    Jito,
    Fastlane,
}

#[cfg(test)]
fn submit_instruction_plan_presence_store() -> &'static Mutex<HashMap<String, bool>> {
    static STORE: OnceLock<Mutex<HashMap<String, bool>>> = OnceLock::new();
    STORE.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn record_submit_instruction_plan_presence_for_test(
    client_order_id: Option<&str>,
    has_instruction_plan: bool,
) {
    let Some(client_order_id) = client_order_id else {
        return;
    };
    if let Ok(mut store) = submit_instruction_plan_presence_store().lock() {
        store.insert(client_order_id.to_string(), has_instruction_plan);
    }
}

#[cfg(test)]
pub(crate) fn clear_submit_instruction_plan_presence_for_test(client_order_id: &str) {
    if let Ok(mut store) = submit_instruction_plan_presence_store().lock() {
        store.remove(client_order_id);
    }
}

#[cfg(test)]
pub(crate) fn take_submit_instruction_plan_presence_for_test(client_order_id: &str) -> Option<bool> {
    submit_instruction_plan_presence_store()
        .lock()
        .ok()
        .and_then(|mut store| store.remove(client_order_id))
}

impl RouteAdapter {
    pub(crate) fn from_kind(kind: RouteExecutorKind) -> Self {
        match kind {
            RouteExecutorKind::Paper => Self::Paper,
            RouteExecutorKind::Rpc => Self::Rpc,
            RouteExecutorKind::Jito => Self::Jito,
            RouteExecutorKind::Fastlane => Self::Fastlane,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Paper => "paper",
            Self::Rpc => "rpc",
            Self::Jito => "jito",
            Self::Fastlane => "fastlane",
        }
    }

    fn requires_rpc_submit_tip_guard(self) -> bool {
        matches!(self, Self::Rpc)
    }

    pub(crate) async fn execute(
        self,
        state: &AppState,
        route: &str,
        action: UpstreamAction,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
        payload_expectations: RouteActionPayloadExpectations<'_>,
        submit_context: RouteSubmitExecutionContext,
    ) -> std::result::Result<Value, Reject> {
        match action {
            UpstreamAction::Simulate => {
                self.execute_simulate(state, route, raw_body, submit_deadline, payload_expectations)
                    .await
            }
            UpstreamAction::Submit => {
                self.execute_submit(
                    state,
                    route,
                    raw_body,
                    submit_deadline,
                    payload_expectations,
                    submit_context,
                )
                .await
            }
        }
    }

    async fn execute_simulate(
        self,
        state: &AppState,
        route: &str,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
        payload_expectations: RouteActionPayloadExpectations<'_>,
    ) -> std::result::Result<Value, Reject> {
        validate_simulate_payload_for_route(
            raw_body,
            route,
            state.config.contract_version.as_str(),
            payload_expectations.request_id,
            payload_expectations.signal_id,
            payload_expectations.side,
            payload_expectations.token,
        )?;
        if is_internal_paper_route(route) {
            return Ok(build_paper_simulate_backend_response(
                route,
                state.config.contract_version.as_str(),
            ));
        }
        if state.config.backend_mode == ExecutorBackendMode::Mock {
            return Ok(build_mock_simulate_backend_response(
                route,
                state.config.contract_version.as_str(),
            ));
        }
        forward_to_upstream(
            state,
            route,
            UpstreamAction::Simulate,
            raw_body,
            submit_deadline,
        )
        .await
    }

    async fn execute_submit(
        self,
        state: &AppState,
        route: &str,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
        payload_expectations: RouteActionPayloadExpectations<'_>,
        submit_context: RouteSubmitExecutionContext,
    ) -> std::result::Result<Value, Reject> {
        #[cfg(test)]
        record_submit_instruction_plan_presence_for_test(
            payload_expectations.client_order_id,
            submit_context.instruction_plan.is_some(),
        );
        let Some(plan) = submit_context.instruction_plan else {
            return Err(Reject::terminal(
                "invalid_request_body",
                "submit payload missing instruction plan at route-adapter boundary",
            ));
        };
        let Some(expected_slippage_bps) = submit_context.expected_slippage_bps else {
            return Err(Reject::terminal(
                "invalid_request_body",
                "submit payload missing slippage_bps expectation at route-adapter boundary",
            ));
        };
        let Some(expected_route_slippage_cap_bps) = submit_context.expected_route_slippage_cap_bps
        else {
            return Err(Reject::terminal(
                "invalid_request_body",
                "submit payload missing route_slippage_cap_bps expectation at route-adapter boundary",
            ));
        };
        let payload = if self.requires_rpc_submit_tip_guard() {
            validate_rpc_submit_tip_payload(
                raw_body,
                route,
                state.config.contract_version.as_str(),
                payload_expectations.request_id,
                payload_expectations.signal_id,
                payload_expectations.client_order_id,
                payload_expectations.side,
                payload_expectations.token,
            )?
        } else {
            validate_submit_payload_for_route(
                raw_body,
                route,
                state.config.contract_version.as_str(),
                payload_expectations.request_id,
                payload_expectations.signal_id,
                payload_expectations.client_order_id,
                payload_expectations.side,
                payload_expectations.token,
            )?
        };
        validate_submit_instruction_plan_payload_consistency(&payload, plan)?;
        validate_submit_slippage_payload_consistency(
            &payload,
            expected_slippage_bps,
            expected_route_slippage_cap_bps,
        )?;
        debug!(
            route = %route,
            route_adapter = %self.as_str(),
            cu_limit = plan.compute_budget_cu_limit,
            cu_price_micro_lamports = plan.compute_budget_cu_price_micro_lamports,
            tip_instruction_lamports = ?plan.tip_instruction_lamports,
            "route adapter received submit instruction plan"
        );
        if is_internal_paper_route(route) {
            return Ok(build_paper_submit_backend_response(
                route,
                state.config.contract_version.as_str(),
                payload_expectations,
            ));
        }
        if state.config.backend_mode == ExecutorBackendMode::Mock {
            return Ok(build_mock_submit_backend_response(
                route,
                state.config.contract_version.as_str(),
                payload_expectations,
            ));
        }
        forward_to_upstream(
            state,
            route,
            UpstreamAction::Submit,
            raw_body,
            submit_deadline,
        )
        .await
    }
}

fn mock_submit_signature() -> String {
    bs58::encode([7u8; 64]).into_string()
}

fn paper_submit_signature(payload_expectations: RouteActionPayloadExpectations<'_>) -> String {
    let request_id = payload_expectations.request_id.unwrap_or("");
    let client_order_id = payload_expectations.client_order_id.unwrap_or("");

    let mut first = Sha256::new();
    first.update(b"executor-paper-submit-signature:v1:");
    first.update(request_id.as_bytes());
    first.update(b":");
    first.update(client_order_id.as_bytes());
    let first_digest = first.finalize();

    let mut second = Sha256::new();
    second.update(b"executor-paper-submit-signature:v2:");
    second.update(client_order_id.as_bytes());
    second.update(b":");
    second.update(request_id.as_bytes());
    let second_digest = second.finalize();

    let mut signature_bytes = [0u8; 64];
    signature_bytes[..32].copy_from_slice(first_digest.as_slice());
    signature_bytes[32..].copy_from_slice(second_digest.as_slice());
    bs58::encode(signature_bytes).into_string()
}

fn is_internal_paper_route(route: &str) -> bool {
    route == "paper"
}

fn build_paper_simulate_backend_response(route: &str, contract_version: &str) -> Value {
    json!({
        "status": "ok",
        "ok": true,
        "accepted": true,
        "route": route,
        "contract_version": contract_version,
        "detail": "executor_paper_simulation_ok"
    })
}

fn build_paper_submit_backend_response(
    route: &str,
    contract_version: &str,
    payload_expectations: RouteActionPayloadExpectations<'_>,
) -> Value {
    let mut payload = json!({
        "status": "ok",
        "ok": true,
        "accepted": true,
        "route": route,
        "contract_version": contract_version,
        "detail": "executor_paper_submit_ok",
        "tx_signature": paper_submit_signature(payload_expectations),
    });
    if let Some(request_id) = payload_expectations.request_id {
        payload["request_id"] = Value::String(request_id.to_string());
    }
    if let Some(client_order_id) = payload_expectations.client_order_id {
        payload["client_order_id"] = Value::String(client_order_id.to_string());
    }
    payload
}

fn build_mock_simulate_backend_response(route: &str, contract_version: &str) -> Value {
    json!({
        "status": "ok",
        "ok": true,
        "accepted": true,
        "route": route,
        "contract_version": contract_version,
        "detail": "executor_mock_simulation_ok"
    })
}

fn build_mock_submit_backend_response(
    route: &str,
    contract_version: &str,
    payload_expectations: RouteActionPayloadExpectations<'_>,
) -> Value {
    let mut payload = json!({
        "status": "ok",
        "ok": true,
        "accepted": true,
        "route": route,
        "contract_version": contract_version,
        "detail": "executor_mock_submit_ok",
        "tx_signature": mock_submit_signature(),
    });
    if let Some(request_id) = payload_expectations.request_id {
        payload["request_id"] = Value::String(request_id.to_string());
    }
    if let Some(client_order_id) = payload_expectations.client_order_id {
        payload["client_order_id"] = Value::String(client_order_id.to_string());
    }
    payload
}

fn parse_payload_object_for_action(
    raw_body: &[u8],
    action_label: &str,
) -> std::result::Result<serde_json::Map<String, Value>, Reject> {
    let payload: Value = serde_json::from_slice(raw_body).map_err(|error| {
        Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload must be valid JSON object: {}", error),
        )
    })?;
    payload.as_object().cloned().ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload must be JSON object"),
        )
    })
}

fn validate_payload_route_for_action(
    raw_body: &[u8],
    expected_route: &str,
    action_label: &str,
) -> std::result::Result<serde_json::Map<String, Value>, Reject> {
    let payload = parse_payload_object_for_action(raw_body, action_label)?;
    let expected_normalized_route = normalize_route(expected_route);
    let payload_route = payload.get("route").ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!(
                "{action_label} payload missing route at route-adapter boundary expected={expected_normalized_route}"
            ),
        )
    })?;
    let payload_route_raw = payload_route.as_str().ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload route must be string"),
        )
    })?;
    let payload_normalized_route = normalize_route(payload_route_raw);
    if payload_normalized_route != expected_normalized_route {
        return Err(Reject::terminal(
            "invalid_request_body",
            format!(
                "{action_label} payload route mismatch at route-adapter boundary expected={expected_normalized_route} got={payload_normalized_route}"
            ),
        ));
    }

    Ok(payload)
}

fn validate_required_payload_action_field(
    payload: &serde_json::Map<String, Value>,
    action_label: &str,
    expected_action: &str,
) -> std::result::Result<(), Reject> {
    let action_value = payload.get("action").ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!(
                "{action_label} payload missing action at route-adapter boundary expected={expected_action}"
            ),
        )
    })?;
    let action_raw = action_value.as_str().ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload action must be string"),
        )
    })?;
    let normalized_action = action_raw.trim().to_ascii_lowercase();
    if normalized_action != expected_action {
        return Err(Reject::terminal(
            "invalid_request_body",
            format!(
                "{action_label} payload action mismatch at route-adapter boundary expected={expected_action} got={normalized_action}"
            ),
        ));
    }

    Ok(())
}

fn validate_optional_payload_action_field(
    payload: &serde_json::Map<String, Value>,
    action_label: &str,
    expected_action: &str,
) -> std::result::Result<(), Reject> {
    let Some(action_value) = payload.get("action") else {
        return Ok(());
    };
    let action_raw = action_value.as_str().ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload action must be string when present"),
        )
    })?;
    let normalized_action = action_raw.trim().to_ascii_lowercase();
    if normalized_action != expected_action {
        return Err(Reject::terminal(
            "invalid_request_body",
            format!(
                "{action_label} payload action mismatch at route-adapter boundary expected={expected_action} got={normalized_action}"
            ),
        ));
    }

    Ok(())
}

fn validate_required_payload_contract_version_field(
    payload: &serde_json::Map<String, Value>,
    action_label: &str,
    expected_contract_version: &str,
) -> std::result::Result<(), Reject> {
    let contract_version_value = payload.get("contract_version").ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!(
                "{action_label} payload missing contract_version at route-adapter boundary expected={expected_contract_version}"
            ),
        )
    })?;
    let contract_version_raw = contract_version_value.as_str().ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload contract_version must be string"),
        )
    })?;
    let contract_version = contract_version_raw.trim();
    if contract_version.is_empty() {
        return Err(Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload contract_version must be non-empty"),
        ));
    }
    if contract_version != expected_contract_version {
        return Err(Reject::terminal(
            "invalid_request_body",
            format!(
                "{action_label} payload contract_version mismatch at route-adapter boundary expected={expected_contract_version} got={contract_version}"
            ),
        ));
    }

    Ok(())
}

fn validate_optional_payload_non_empty_string_field(
    payload: &serde_json::Map<String, Value>,
    action_label: &str,
    field_name: &'static str,
    expected_value: Option<&str>,
    normalize_for_compare: bool,
) -> std::result::Result<(), Reject> {
    let Some(field_value) = payload.get(field_name) else {
        if expected_value.is_some() {
            return Err(Reject::terminal(
                "invalid_request_body",
                format!("{action_label} payload missing {field_name} at route-adapter boundary"),
            ));
        }
        return Ok(());
    };
    let field_raw = field_value.as_str().ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload {field_name} must be string when present"),
        )
    })?;
    if field_raw.trim().is_empty() {
        return Err(Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload {field_name} must be non-empty when present"),
        ));
    }
    if let Some(expected) = expected_value {
        let payload_value = field_raw.trim();
        let expected = expected.trim();
        let (payload_cmp, expected_cmp) = if normalize_for_compare {
            (
                payload_value.to_ascii_lowercase(),
                expected.to_ascii_lowercase(),
            )
        } else {
            (payload_value.to_string(), expected.to_string())
        };
        if payload_cmp != expected_cmp {
            return Err(Reject::terminal(
                "invalid_request_body",
                format!(
                    "{action_label} payload {field_name} mismatch at route-adapter boundary expected={expected} got={}",
                    payload_value
                ),
            ));
        }
    }

    Ok(())
}

fn validate_optional_payload_bool_field(
    payload: &serde_json::Map<String, Value>,
    action_label: &str,
    field_name: &'static str,
    expected_value: Option<bool>,
) -> std::result::Result<(), Reject> {
    let Some(field_value) = payload.get(field_name) else {
        if expected_value.is_some() {
            return Err(Reject::terminal(
                "invalid_request_body",
                format!("{action_label} payload missing {field_name} at route-adapter boundary"),
            ));
        }
        return Ok(());
    };
    let field_bool = field_value.as_bool().ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload {field_name} must be boolean when present"),
        )
    })?;
    if let Some(expected) = expected_value {
        if field_bool != expected {
            return Err(Reject::terminal(
                "invalid_request_body",
                format!(
                    "{action_label} payload {field_name} mismatch at route-adapter boundary expected={expected} got={field_bool}"
                ),
            ));
        }
    }

    Ok(())
}

fn validate_submit_payload_for_route(
    raw_body: &[u8],
    expected_route: &str,
    expected_contract_version: &str,
    expected_request_id: Option<&str>,
    expected_signal_id: Option<&str>,
    expected_client_order_id: Option<&str>,
    expected_side: Option<&str>,
    expected_token: Option<&str>,
) -> std::result::Result<serde_json::Map<String, Value>, Reject> {
    let payload = validate_payload_route_for_action(raw_body, expected_route, "submit")?;
    validate_optional_payload_action_field(&payload, "submit", "submit")?;
    validate_required_payload_contract_version_field(
        &payload,
        "submit",
        expected_contract_version,
    )?;
    validate_optional_payload_non_empty_string_field(
        &payload,
        "submit",
        "request_id",
        expected_request_id,
        false,
    )?;
    validate_optional_payload_non_empty_string_field(
        &payload,
        "submit",
        "signal_id",
        expected_signal_id,
        false,
    )?;
    validate_optional_payload_non_empty_string_field(
        &payload,
        "submit",
        "client_order_id",
        expected_client_order_id,
        false,
    )?;
    validate_optional_payload_non_empty_string_field(
        &payload,
        "submit",
        "side",
        expected_side,
        true,
    )?;
    validate_optional_payload_non_empty_string_field(
        &payload,
        "submit",
        "token",
        expected_token,
        false,
    )?;
    Ok(payload)
}

fn validate_simulate_payload_for_route(
    raw_body: &[u8],
    expected_route: &str,
    expected_contract_version: &str,
    expected_request_id: Option<&str>,
    expected_signal_id: Option<&str>,
    expected_side: Option<&str>,
    expected_token: Option<&str>,
) -> std::result::Result<serde_json::Map<String, Value>, Reject> {
    let payload = validate_payload_route_for_action(raw_body, expected_route, "simulate")?;
    validate_required_payload_action_field(&payload, "simulate", "simulate")?;
    validate_required_payload_contract_version_field(
        &payload,
        "simulate",
        expected_contract_version,
    )?;
    validate_optional_payload_bool_field(&payload, "simulate", "dry_run", Some(true))?;
    validate_optional_payload_non_empty_string_field(
        &payload,
        "simulate",
        "request_id",
        expected_request_id,
        false,
    )?;
    validate_optional_payload_non_empty_string_field(
        &payload,
        "simulate",
        "signal_id",
        expected_signal_id,
        false,
    )?;
    validate_optional_payload_non_empty_string_field(
        &payload,
        "simulate",
        "side",
        expected_side,
        true,
    )?;
    validate_optional_payload_non_empty_string_field(
        &payload,
        "simulate",
        "token",
        expected_token,
        false,
    )?;
    Ok(payload)
}

fn validate_rpc_submit_tip_payload(
    raw_body: &[u8],
    expected_route: &str,
    expected_contract_version: &str,
    expected_request_id: Option<&str>,
    expected_signal_id: Option<&str>,
    expected_client_order_id: Option<&str>,
    expected_side: Option<&str>,
    expected_token: Option<&str>,
) -> std::result::Result<serde_json::Map<String, Value>, Reject> {
    let payload = validate_submit_payload_for_route(
        raw_body,
        expected_route,
        expected_contract_version,
        expected_request_id,
        expected_signal_id,
        expected_client_order_id,
        expected_side,
        expected_token,
    )?;
    let tip_lamports = match payload.get("tip_lamports") {
        None => 0u64,
        Some(value) if value.is_null() => 0u64,
        Some(value) => value.as_u64().ok_or_else(|| {
            Reject::terminal(
                "invalid_request_body",
                "submit payload tip_lamports must be non-negative integer when present",
            )
        })?,
    };
    if tip_lamports > 0 {
        return Err(Reject::terminal(
            "tip_not_supported",
            format!(
                "rpc route requires tip_lamports=0 at route-adapter boundary (got {})",
                tip_lamports
            ),
        ));
    }

    Ok(payload)
}

fn validate_required_payload_u64_field(
    payload: &serde_json::Map<String, Value>,
    action_label: &str,
    field_lookup: &'static str,
    field_label: &'static str,
) -> std::result::Result<u64, Reject> {
    let field_value = payload.get(field_lookup).ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload missing {field_label} at route-adapter boundary"),
        )
    })?;
    field_value.as_u64().ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload {field_label} must be non-negative integer"),
        )
    })
}

fn validate_required_payload_f64_field(
    payload: &serde_json::Map<String, Value>,
    action_label: &str,
    field_lookup: &'static str,
    field_label: &'static str,
) -> std::result::Result<f64, Reject> {
    let field_value = payload.get(field_lookup).ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload missing {field_label} at route-adapter boundary"),
        )
    })?;
    let parsed = field_value.as_f64().ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload {field_label} must be number"),
        )
    })?;
    if !parsed.is_finite() {
        return Err(Reject::terminal(
            "invalid_request_body",
            format!("{action_label} payload {field_label} must be finite"),
        ));
    }
    Ok(parsed)
}

fn validate_submit_instruction_plan_payload_consistency(
    payload: &serde_json::Map<String, Value>,
    instruction_plan: SubmitInstructionPlan,
) -> std::result::Result<(), Reject> {
    let expected_tip = instruction_plan.tip_instruction_lamports.unwrap_or(0);
    let actual_tip = validate_required_payload_u64_field(
        payload,
        "submit",
        "tip_lamports",
        "tip_lamports",
    )?;
    if actual_tip != expected_tip {
        return Err(Reject::terminal(
            "invalid_request_body",
            format!(
                "submit payload tip_lamports mismatch at route-adapter boundary expected={} got={}",
                expected_tip, actual_tip
            ),
        ));
    }

    let compute_budget_value = payload.get("compute_budget").ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            "submit payload missing compute_budget at route-adapter boundary",
        )
    })?;
    let compute_budget = compute_budget_value.as_object().ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            "submit payload compute_budget must be object",
        )
    })?;

    let expected_cu_limit = u64::from(instruction_plan.compute_budget_cu_limit);
    let actual_cu_limit = validate_required_payload_u64_field(
        compute_budget,
        "submit",
        "cu_limit",
        "compute_budget.cu_limit",
    )?;
    if actual_cu_limit != expected_cu_limit {
        return Err(Reject::terminal(
            "invalid_request_body",
            format!(
                "submit payload compute_budget.cu_limit mismatch at route-adapter boundary expected={} got={}",
                expected_cu_limit, actual_cu_limit
            ),
        ));
    }

    let expected_cu_price = instruction_plan.compute_budget_cu_price_micro_lamports;
    let actual_cu_price = validate_required_payload_u64_field(
        compute_budget,
        "submit",
        "cu_price_micro_lamports",
        "compute_budget.cu_price_micro_lamports",
    )?;
    if actual_cu_price != expected_cu_price {
        return Err(Reject::terminal(
            "invalid_request_body",
            format!(
                "submit payload compute_budget.cu_price_micro_lamports mismatch at route-adapter boundary expected={} got={}",
                expected_cu_price, actual_cu_price
            ),
        ));
    }

    Ok(())
}

fn validate_submit_slippage_payload_consistency(
    payload: &serde_json::Map<String, Value>,
    expected_slippage_bps: f64,
    expected_route_slippage_cap_bps: f64,
) -> std::result::Result<(), Reject> {
    const FLOAT_MATCH_EPSILON: f64 = 1e-9;
    let actual_slippage_bps = validate_required_payload_f64_field(
        payload,
        "submit",
        "slippage_bps",
        "slippage_bps",
    )?;
    if (actual_slippage_bps - expected_slippage_bps).abs() > FLOAT_MATCH_EPSILON {
        return Err(Reject::terminal(
            "invalid_request_body",
            format!(
                "submit payload slippage_bps mismatch at route-adapter boundary expected={} got={}",
                expected_slippage_bps, actual_slippage_bps
            ),
        ));
    }

    let actual_route_slippage_cap_bps = validate_required_payload_f64_field(
        payload,
        "submit",
        "route_slippage_cap_bps",
        "route_slippage_cap_bps",
    )?;
    if (actual_route_slippage_cap_bps - expected_route_slippage_cap_bps).abs()
        > FLOAT_MATCH_EPSILON
    {
        return Err(Reject::terminal(
            "invalid_request_body",
            format!(
                "submit payload route_slippage_cap_bps mismatch at route-adapter boundary expected={} got={}",
                expected_route_slippage_cap_bps, actual_route_slippage_cap_bps
            ),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        validate_submit_slippage_payload_consistency as validate_submit_slippage_payload_consistency_with_payload,
        validate_submit_instruction_plan_payload_consistency as validate_submit_instruction_plan_payload_consistency_with_payload,
        validate_rpc_submit_tip_payload as validate_rpc_submit_tip_payload_with_expectations,
        validate_simulate_payload_for_route as validate_simulate_payload_for_route_with_expectations,
        validate_submit_payload_for_route as validate_submit_payload_for_route_with_expectations,
        RouteAdapter,
    };
    use crate::Reject;
    use crate::route_executor::{RouteActionPayloadExpectations, RouteExecutorKind};
    use crate::tx_build::SubmitInstructionPlan;
    use serde_json::Map;
    use serde_json::Value;

    fn validate_submit_payload_for_route(
        raw_body: &[u8],
        expected_route: &str,
        expected_contract_version: &str,
        expected_request_id: Option<&str>,
        expected_signal_id: Option<&str>,
        expected_client_order_id: Option<&str>,
    ) -> std::result::Result<Map<String, Value>, Reject> {
        validate_submit_payload_for_route_with_expectations(
            raw_body,
            expected_route,
            expected_contract_version,
            expected_request_id,
            expected_signal_id,
            expected_client_order_id,
            None,
            None,
        )
    }

    fn validate_simulate_payload_for_route(
        raw_body: &[u8],
        expected_route: &str,
        expected_contract_version: &str,
        expected_request_id: Option<&str>,
        expected_signal_id: Option<&str>,
    ) -> std::result::Result<Map<String, Value>, Reject> {
        validate_simulate_payload_for_route_with_expectations(
            raw_body,
            expected_route,
            expected_contract_version,
            expected_request_id,
            expected_signal_id,
            None,
            None,
        )
    }

    fn validate_rpc_submit_tip_payload(
        raw_body: &[u8],
        expected_route: &str,
        expected_contract_version: &str,
        expected_request_id: Option<&str>,
        expected_signal_id: Option<&str>,
        expected_client_order_id: Option<&str>,
    ) -> std::result::Result<Map<String, Value>, Reject> {
        validate_rpc_submit_tip_payload_with_expectations(
            raw_body,
            expected_route,
            expected_contract_version,
            expected_request_id,
            expected_signal_id,
            expected_client_order_id,
            None,
            None,
        )
    }

    fn validate_submit_instruction_plan_payload_consistency(
        raw_body: &[u8],
        instruction_plan: SubmitInstructionPlan,
    ) -> std::result::Result<(), Reject> {
        let payload = validate_submit_payload_for_route(raw_body, "rpc", "v1", None, None, None)?;
        validate_submit_instruction_plan_payload_consistency_with_payload(&payload, instruction_plan)
    }

    fn validate_submit_slippage_payload_consistency(
        raw_body: &[u8],
        expected_slippage_bps: f64,
        expected_route_slippage_cap_bps: f64,
    ) -> std::result::Result<(), Reject> {
        let payload = validate_submit_payload_for_route(raw_body, "rpc", "v1", None, None, None)?;
        validate_submit_slippage_payload_consistency_with_payload(
            &payload,
            expected_slippage_bps,
            expected_route_slippage_cap_bps,
        )
    }

    #[test]
    fn route_adapter_from_kind_maps_expected_label() {
        assert_eq!(
            RouteAdapter::from_kind(RouteExecutorKind::Paper).as_str(),
            "paper"
        );
        assert_eq!(
            RouteAdapter::from_kind(RouteExecutorKind::Rpc).as_str(),
            "rpc"
        );
        assert_eq!(
            RouteAdapter::from_kind(RouteExecutorKind::Jito).as_str(),
            "jito"
        );
        assert_eq!(
            RouteAdapter::from_kind(RouteExecutorKind::Fastlane).as_str(),
            "fastlane"
        );
    }

    #[test]
    fn route_adapter_rpc_tip_guard_applies_only_to_rpc_submit() {
        assert!(RouteAdapter::Rpc.requires_rpc_submit_tip_guard());
        assert!(!RouteAdapter::Paper.requires_rpc_submit_tip_guard());
        assert!(!RouteAdapter::Jito.requires_rpc_submit_tip_guard());
        assert!(!RouteAdapter::Fastlane.requires_rpc_submit_tip_guard());
    }

    #[test]
    fn build_mock_simulate_backend_response_includes_contract_fields() {
        let payload = super::build_mock_simulate_backend_response("rpc", "v1");
        assert_eq!(payload.get("status").and_then(Value::as_str), Some("ok"));
        assert_eq!(payload.get("route").and_then(Value::as_str), Some("rpc"));
        assert_eq!(
            payload.get("contract_version").and_then(Value::as_str),
            Some("v1")
        );
        assert_eq!(
            payload.get("detail").and_then(Value::as_str),
            Some("executor_mock_simulation_ok")
        );
    }

    #[test]
    fn build_mock_submit_backend_response_includes_signature_and_identity() {
        let payload = super::build_mock_submit_backend_response(
            "rpc",
            "v1",
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-1"),
                signal_id: Some("signal-1"),
                client_order_id: Some("client-order-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
        );
        assert_eq!(payload.get("status").and_then(Value::as_str), Some("ok"));
        assert_eq!(
            payload.get("request_id").and_then(Value::as_str),
            Some("request-1")
        );
        assert_eq!(
            payload.get("client_order_id").and_then(Value::as_str),
            Some("client-order-1")
        );
        let signature = payload
            .get("tx_signature")
            .and_then(Value::as_str)
            .expect("tx_signature should be present");
        assert!(signature.len() > 40, "signature should look like base58");
    }

    #[test]
    fn build_paper_simulate_backend_response_includes_contract_fields() {
        let payload = super::build_paper_simulate_backend_response("paper", "v1");
        assert_eq!(payload.get("status").and_then(Value::as_str), Some("ok"));
        assert_eq!(payload.get("route").and_then(Value::as_str), Some("paper"));
        assert_eq!(
            payload.get("contract_version").and_then(Value::as_str),
            Some("v1")
        );
        assert_eq!(
            payload.get("detail").and_then(Value::as_str),
            Some("executor_paper_simulation_ok")
        );
    }

    #[test]
    fn build_paper_submit_backend_response_includes_deterministic_signature_and_identity() {
        let payload = super::build_paper_submit_backend_response(
            "paper",
            "v1",
            RouteActionPayloadExpectations {
                route_hint: Some("paper"),
                request_id: Some("request-paper-1"),
                signal_id: Some("signal-paper-1"),
                client_order_id: Some("client-order-paper-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
        );
        assert_eq!(payload.get("status").and_then(Value::as_str), Some("ok"));
        assert_eq!(
            payload.get("request_id").and_then(Value::as_str),
            Some("request-paper-1")
        );
        assert_eq!(
            payload.get("client_order_id").and_then(Value::as_str),
            Some("client-order-paper-1")
        );
        assert_eq!(
            payload.get("detail").and_then(Value::as_str),
            Some("executor_paper_submit_ok")
        );
        let signature = payload
            .get("tx_signature")
            .and_then(Value::as_str)
            .expect("tx_signature should be present");
        assert!(signature.len() > 40, "signature should look like base58");

        let second_payload = super::build_paper_submit_backend_response(
            "paper",
            "v1",
            RouteActionPayloadExpectations {
                route_hint: Some("paper"),
                request_id: Some("request-paper-1"),
                signal_id: Some("signal-paper-1"),
                client_order_id: Some("client-order-paper-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
        );
        let second_signature = second_payload
            .get("tx_signature")
            .and_then(Value::as_str)
            .expect("second tx_signature should be present");
        assert_eq!(
            signature, second_signature,
            "paper signature must be deterministic for the same request identity"
        );
    }

    #[test]
    fn validate_rpc_submit_tip_payload_accepts_zero_tip() {
        let body = br#"{"tip_lamports":0,"route":"rpc","contract_version":"v1"}"#;
        assert!(validate_rpc_submit_tip_payload(body, "rpc", "v1", None, None, None).is_ok());
    }

    #[test]
    fn validate_rpc_submit_tip_payload_rejects_nonzero_tip() {
        let body = br#"{"tip_lamports":42,"route":"rpc","contract_version":"v1"}"#;
        let reject = validate_rpc_submit_tip_payload(body, "rpc", "v1", None, None, None)
            .expect_err("non-zero rpc tip must be rejected");
        assert_eq!(reject.code, "tip_not_supported");
        assert!(!reject.retryable);
    }

    #[test]
    fn validate_rpc_submit_tip_payload_rejects_invalid_json() {
        let body = br#"{"tip_lamports":"oops""#;
        let reject = validate_rpc_submit_tip_payload(body, "rpc", "v1", None, None, None)
            .expect_err("invalid json must reject");
        assert_eq!(reject.code, "invalid_request_body");
    }

    #[test]
    fn validate_rpc_submit_tip_payload_rejects_non_object_payload() {
        let body = br#"["rpc"]"#;
        let reject = validate_rpc_submit_tip_payload(body, "rpc", "v1", None, None, None)
            .expect_err("non-object payload must reject");
        assert_eq!(reject.code, "invalid_request_body");
    }

    #[test]
    fn validate_submit_instruction_plan_payload_consistency_accepts_matching_payload() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","compute_budget":{"cu_limit":300000,"cu_price_micro_lamports":1000}}"#;
        let plan = SubmitInstructionPlan {
            compute_budget_cu_limit: 300_000,
            compute_budget_cu_price_micro_lamports: 1_000,
            tip_instruction_lamports: None,
        };
        assert!(validate_submit_instruction_plan_payload_consistency(body, plan).is_ok());
    }

    #[test]
    fn validate_submit_instruction_plan_payload_consistency_rejects_tip_mismatch() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","compute_budget":{"cu_limit":300000,"cu_price_micro_lamports":1000}}"#;
        let plan = SubmitInstructionPlan {
            compute_budget_cu_limit: 300_000,
            compute_budget_cu_price_micro_lamports: 1_000,
            tip_instruction_lamports: Some(42),
        };
        let reject = validate_submit_instruction_plan_payload_consistency(body, plan)
            .expect_err("tip mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("tip_lamports mismatch"));
    }

    #[test]
    fn validate_submit_instruction_plan_payload_consistency_rejects_cu_limit_mismatch() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","compute_budget":{"cu_limit":300000,"cu_price_micro_lamports":1000}}"#;
        let plan = SubmitInstructionPlan {
            compute_budget_cu_limit: 350_000,
            compute_budget_cu_price_micro_lamports: 1_000,
            tip_instruction_lamports: None,
        };
        let reject = validate_submit_instruction_plan_payload_consistency(body, plan)
            .expect_err("compute-budget mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("compute_budget.cu_limit mismatch"));
    }

    #[test]
    fn validate_submit_instruction_plan_payload_consistency_rejects_cu_price_mismatch() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","compute_budget":{"cu_limit":300000,"cu_price_micro_lamports":1000}}"#;
        let plan = SubmitInstructionPlan {
            compute_budget_cu_limit: 300_000,
            compute_budget_cu_price_micro_lamports: 1_500,
            tip_instruction_lamports: None,
        };
        let reject = validate_submit_instruction_plan_payload_consistency(body, plan)
            .expect_err("compute-budget cu_price mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("compute_budget.cu_price_micro_lamports mismatch"));
    }

    #[test]
    fn validate_submit_slippage_payload_consistency_accepts_matching_values() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","slippage_bps":10.0,"route_slippage_cap_bps":20.0}"#;
        assert!(validate_submit_slippage_payload_consistency(body, 10.0, 20.0).is_ok());
    }

    #[test]
    fn validate_submit_slippage_payload_consistency_rejects_missing_slippage_bps() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","route_slippage_cap_bps":20.0}"#;
        let reject = validate_submit_slippage_payload_consistency(body, 10.0, 20.0)
            .expect_err("missing slippage_bps must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing slippage_bps"));
    }

    #[test]
    fn validate_submit_slippage_payload_consistency_rejects_non_numeric_slippage_bps() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","slippage_bps":"10.0","route_slippage_cap_bps":20.0}"#;
        let reject = validate_submit_slippage_payload_consistency(body, 10.0, 20.0)
            .expect_err("non-numeric slippage_bps must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("slippage_bps must be number"));
    }

    #[test]
    fn validate_submit_slippage_payload_consistency_rejects_slippage_mismatch() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","slippage_bps":12.0,"route_slippage_cap_bps":20.0}"#;
        let reject = validate_submit_slippage_payload_consistency(body, 10.0, 20.0)
            .expect_err("slippage mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("slippage_bps mismatch"));
    }

    #[test]
    fn validate_submit_slippage_payload_consistency_rejects_missing_route_slippage_cap_bps() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","slippage_bps":10.0}"#;
        let reject = validate_submit_slippage_payload_consistency(body, 10.0, 20.0)
            .expect_err("missing route_slippage_cap_bps must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing route_slippage_cap_bps"));
    }

    #[test]
    fn validate_submit_slippage_payload_consistency_rejects_non_numeric_route_slippage_cap_bps() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","slippage_bps":10.0,"route_slippage_cap_bps":"20.0"}"#;
        let reject = validate_submit_slippage_payload_consistency(body, 10.0, 20.0)
            .expect_err("non-numeric route_slippage_cap_bps must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("route_slippage_cap_bps must be number"));
    }

    #[test]
    fn validate_submit_slippage_payload_consistency_rejects_route_slippage_cap_mismatch() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","slippage_bps":10.0,"route_slippage_cap_bps":25.0}"#;
        let reject = validate_submit_slippage_payload_consistency(body, 10.0, 20.0)
            .expect_err("route_slippage_cap mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("route_slippage_cap_bps mismatch"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_mismatched_route() {
        let body = br#"{"route":"jito","tip_lamports":0,"contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, None, None)
            .expect_err("route mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route mismatch"));
    }

    #[test]
    fn validate_submit_payload_for_route_accepts_matching_route_case_insensitive() {
        let body = br#"{"route":" RPC ","tip_lamports":0,"contract_version":"v1"}"#;
        assert!(validate_submit_payload_for_route(body, "rpc", "v1", None, None, None).is_ok());
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_missing_route() {
        let body = br#"{"tip_lamports":0,"contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, None, None)
            .expect_err("missing route must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing route"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_non_string_route() {
        let body = br#"{"route":123,"tip_lamports":0,"contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, None, None)
            .expect_err("non-string route must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route must be string"));
    }

    #[test]
    fn validate_submit_payload_for_route_accepts_missing_action() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1"}"#;
        assert!(validate_submit_payload_for_route(body, "rpc", "v1", None, None, None).is_ok());
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_mismatched_action_when_present() {
        let body = br#"{"route":"rpc","tip_lamports":0,"action":"simulate","contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, None, None)
            .expect_err("mismatched submit action must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("action mismatch"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_non_string_action_when_present() {
        let body = br#"{"route":"rpc","tip_lamports":0,"action":123,"contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, None, None)
            .expect_err("non-string submit action must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("action must be string"));
    }

    #[test]
    fn validate_submit_payload_for_route_accepts_matching_action_case_insensitive_when_present() {
        let body = br#"{"route":"rpc","tip_lamports":0,"action":" SUBMIT ","contract_version":"v1"}"#;
        assert!(validate_submit_payload_for_route(body, "rpc", "v1", None, None, None).is_ok());
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_missing_contract_version() {
        let body = br#"{"route":"rpc","tip_lamports":0}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, None, None)
            .expect_err("submit missing contract_version must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing contract_version"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_non_string_contract_version() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":123}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, None, None)
            .expect_err("submit non-string contract_version must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version must be string"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_empty_contract_version() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":" "}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, None, None)
            .expect_err("submit empty contract_version must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version must be non-empty"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_contract_version_mismatch() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v2"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, None, None)
            .expect_err("submit contract_version mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version mismatch"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_empty_client_order_id_when_present() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","client_order_id":" "}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, None, None)
            .expect_err("submit empty client_order_id must reject when present");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("client_order_id must be non-empty"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_non_string_request_id_when_present() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","request_id":123}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, None, None)
            .expect_err("submit non-string request_id must reject when present");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("request_id must be string"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_request_id_mismatch_when_expected() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","request_id":"request-other"}"#;
        let reject = validate_submit_payload_for_route(
            body,
            "rpc",
            "v1",
            Some("request-expected"),
            None,
            None,
        )
        .expect_err("submit request_id mismatch must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("request_id mismatch"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_missing_request_id_when_expected() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", Some("request-expected"), None, None)
            .expect_err("submit missing request_id must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing request_id"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_signal_id_mismatch_when_expected() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","signal_id":"signal-other"}"#;
        let reject = validate_submit_payload_for_route(
            body,
            "rpc",
            "v1",
            None,
            Some("signal-expected"),
            None,
        )
        .expect_err("submit signal_id mismatch must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("signal_id mismatch"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_missing_signal_id_when_expected() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, Some("signal-expected"), None)
            .expect_err("submit missing signal_id must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing signal_id"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_client_order_id_mismatch_when_expected() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","client_order_id":"client-other"}"#;
        let reject = validate_submit_payload_for_route(
            body,
            "rpc",
            "v1",
            None,
            None,
            Some("client-expected"),
        )
        .expect_err("submit client_order_id mismatch must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("client_order_id mismatch"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_missing_client_order_id_when_expected() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, None, Some("client-expected"))
            .expect_err("submit missing client_order_id must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing client_order_id"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_side_mismatch_when_expected() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","side":"sell"}"#;
        let reject = validate_submit_payload_for_route_with_expectations(
            body,
            "rpc",
            "v1",
            None,
            None,
            None,
            Some("buy"),
            None,
        )
        .expect_err("submit side mismatch must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("side mismatch"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_missing_side_when_expected() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route_with_expectations(
            body,
            "rpc",
            "v1",
            None,
            None,
            None,
            Some("buy"),
            None,
        )
        .expect_err("submit missing side must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing side"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_token_mismatch_when_expected() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","token":"22222222222222222222222222222222"}"#;
        let reject = validate_submit_payload_for_route_with_expectations(
            body,
            "rpc",
            "v1",
            None,
            None,
            None,
            None,
            Some("11111111111111111111111111111111"),
        )
        .expect_err("submit token mismatch must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("token mismatch"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_missing_token_when_expected() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route_with_expectations(
            body,
            "rpc",
            "v1",
            None,
            None,
            None,
            None,
            Some("11111111111111111111111111111111"),
        )
        .expect_err("submit missing token must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing token"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_empty_signal_id_when_present() {
        let body =
            br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1","signal_id":" "}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1", None, None, None)
            .expect_err("submit empty signal_id must reject when present");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("signal_id must be non-empty"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_mismatched_route() {
        let body = br#"{"route":"jito","action":"simulate","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate route mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route mismatch"));
    }

    #[test]
    fn validate_simulate_payload_for_route_accepts_matching_route_case_insensitive() {
        let body = br#"{"route":" RPC ","action":"simulate","dry_run":true,"contract_version":"v1"}"#;
        assert!(validate_simulate_payload_for_route(body, "rpc", "v1", None, None).is_ok());
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_missing_route() {
        let body = br#"{"action":"simulate","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate missing route must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing route"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_non_string_route() {
        let body = br#"{"route":123,"action":"simulate","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate non-string route must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route must be string"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_missing_action() {
        let body = br#"{"route":"rpc","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate missing action must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing action"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_missing_dry_run() {
        let body = br#"{"route":"rpc","action":"simulate","contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate missing dry_run must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing dry_run"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_non_bool_dry_run() {
        let body =
            br#"{"route":"rpc","action":"simulate","dry_run":"true","contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate non-bool dry_run must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("dry_run must be boolean"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_dry_run_false() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":false,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate dry_run=false must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("dry_run mismatch"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_non_string_action() {
        let body = br#"{"route":"rpc","action":123,"dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate non-string action must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("action must be string"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_mismatched_action() {
        let body = br#"{"route":"rpc","action":"submit","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate mismatched action must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("action mismatch"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_missing_contract_version() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate missing contract_version must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing contract_version"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_non_string_contract_version() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":123}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate non-string contract_version must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version must be string"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_empty_contract_version() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":" "}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate empty contract_version must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version must be non-empty"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_contract_version_mismatch() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":"v2"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate contract_version mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version mismatch"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_empty_signal_id_when_present() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":"v1","signal_id":" "}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate empty signal_id must reject when present");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("signal_id must be non-empty"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_non_string_request_id_when_present() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":"v1","request_id":123}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate non-string request_id must reject when present");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("request_id must be string"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_signal_id_mismatch_when_expected() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":"v1","signal_id":"signal-other"}"#;
        let reject = validate_simulate_payload_for_route(
            body,
            "rpc",
            "v1",
            None,
            Some("signal-expected"),
        )
        .expect_err("simulate signal_id mismatch must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("signal_id mismatch"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_request_id_mismatch_when_expected() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":"v1","request_id":"request-other"}"#;
        let reject = validate_simulate_payload_for_route(
            body,
            "rpc",
            "v1",
            Some("request-expected"),
            None,
        )
        .expect_err("simulate request_id mismatch must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("request_id mismatch"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_missing_request_id_when_expected() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(
            body,
            "rpc",
            "v1",
            Some("request-expected"),
            None,
        )
        .expect_err("simulate missing request_id must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing request_id"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_token_mismatch_when_expected() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":"v1","token":"22222222222222222222222222222222"}"#;
        let reject = validate_simulate_payload_for_route_with_expectations(
            body,
            "rpc",
            "v1",
            None,
            None,
            None,
            Some("11111111111111111111111111111111"),
        )
        .expect_err("simulate token mismatch must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("token mismatch"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_missing_token_when_expected() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route_with_expectations(
            body,
            "rpc",
            "v1",
            None,
            None,
            None,
            Some("11111111111111111111111111111111"),
        )
        .expect_err("simulate missing token must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing token"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_side_mismatch_when_expected() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":"v1","side":"sell"}"#;
        let reject = validate_simulate_payload_for_route_with_expectations(
            body,
            "rpc",
            "v1",
            None,
            None,
            Some("buy"),
            None,
        )
        .expect_err("simulate side mismatch must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("side mismatch"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_missing_signal_id_when_expected() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(
            body,
            "rpc",
            "v1",
            None,
            Some("signal-expected"),
        )
        .expect_err("simulate missing signal_id must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing signal_id"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_missing_side_when_expected() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route_with_expectations(
            body,
            "rpc",
            "v1",
            None,
            None,
            Some("buy"),
            None,
        )
        .expect_err("simulate missing side must reject when expected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing side"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_empty_request_id_when_present() {
        let body =
            br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":"v1","request_id":" "}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1", None, None)
            .expect_err("simulate empty request_id must reject when present");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("request_id must be non-empty"));
    }
}
