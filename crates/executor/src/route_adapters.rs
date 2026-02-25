use serde_json::Value;
use tracing::debug;
#[cfg(test)]
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use crate::route_backend::UpstreamAction;
use crate::route_executor::{
    RouteActionPayloadExpectations, RouteExecutorKind, RouteSubmitExecutionContext,
};
use crate::route_normalization::normalize_route;
use crate::submit_deadline::SubmitDeadline;
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
        if let Some(plan) = submit_context.instruction_plan {
            debug!(
                route = %route,
                route_adapter = %self.as_str(),
                cu_limit = plan.compute_budget_cu_limit,
                cu_price_micro_lamports = plan.compute_budget_cu_price_micro_lamports,
                tip_instruction_lamports = ?plan.tip_instruction_lamports,
                "route adapter received submit instruction plan"
            );
        }
        if self.requires_rpc_submit_tip_guard() {
            validate_rpc_submit_tip_payload(
                raw_body,
                route,
                state.config.contract_version.as_str(),
                payload_expectations.request_id,
                payload_expectations.signal_id,
                payload_expectations.client_order_id,
                payload_expectations.side,
                payload_expectations.token,
            )?;
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
            )?;
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
) -> std::result::Result<(), Reject> {
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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        validate_rpc_submit_tip_payload as validate_rpc_submit_tip_payload_with_expectations,
        validate_simulate_payload_for_route as validate_simulate_payload_for_route_with_expectations,
        validate_submit_payload_for_route as validate_submit_payload_for_route_with_expectations,
        RouteAdapter,
    };
    use crate::Reject;
    use crate::route_executor::RouteExecutorKind;
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
    ) -> std::result::Result<(), Reject> {
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
