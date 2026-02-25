use serde_json::Value;

use crate::route_backend::UpstreamAction;
use crate::route_executor::RouteExecutorKind;
use crate::route_normalization::normalize_route;
use crate::submit_deadline::SubmitDeadline;
use crate::upstream_forward::forward_to_upstream;
use crate::{AppState, Reject};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PaperRouteExecutor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RpcRouteExecutor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JitoRouteExecutor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FastlaneRouteExecutor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteAdapter {
    Paper(PaperRouteExecutor),
    Rpc(RpcRouteExecutor),
    Jito(JitoRouteExecutor),
    Fastlane(FastlaneRouteExecutor),
}

impl RouteAdapter {
    pub(crate) fn from_kind(kind: RouteExecutorKind) -> Self {
        match kind {
            RouteExecutorKind::Paper => Self::Paper(PaperRouteExecutor),
            RouteExecutorKind::Rpc => Self::Rpc(RpcRouteExecutor),
            RouteExecutorKind::Jito => Self::Jito(JitoRouteExecutor),
            RouteExecutorKind::Fastlane => Self::Fastlane(FastlaneRouteExecutor),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Paper(_) => "paper",
            Self::Rpc(_) => "rpc",
            Self::Jito(_) => "jito",
            Self::Fastlane(_) => "fastlane",
        }
    }

    pub(crate) async fn execute(
        self,
        state: &AppState,
        route: &str,
        action: UpstreamAction,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        match self {
            Self::Paper(adapter) => {
                adapter
                    .execute_action(state, route, action, raw_body, submit_deadline)
                    .await
            }
            Self::Rpc(adapter) => {
                adapter
                    .execute_action(state, route, action, raw_body, submit_deadline)
                    .await
            }
            Self::Jito(adapter) => {
                adapter
                    .execute_action(state, route, action, raw_body, submit_deadline)
                    .await
            }
            Self::Fastlane(adapter) => {
                adapter
                    .execute_action(state, route, action, raw_body, submit_deadline)
                    .await
            }
        }
    }
}

impl PaperRouteExecutor {
    async fn execute_action(
        self,
        state: &AppState,
        route: &str,
        action: UpstreamAction,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        match action {
            UpstreamAction::Simulate => self.simulate(state, route, raw_body, submit_deadline).await,
            UpstreamAction::Submit => self.submit(state, route, raw_body, submit_deadline).await,
        }
    }

    async fn simulate(
        self,
        state: &AppState,
        route: &str,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        validate_simulate_payload_for_route(raw_body, route, state.config.contract_version.as_str())?;
        forward_to_upstream(
            state,
            route,
            UpstreamAction::Simulate,
            raw_body,
            submit_deadline,
        )
        .await
    }

    async fn submit(
        self,
        state: &AppState,
        route: &str,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        validate_submit_payload_for_route(raw_body, route, state.config.contract_version.as_str())?;
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

impl RpcRouteExecutor {
    async fn execute_action(
        self,
        state: &AppState,
        route: &str,
        action: UpstreamAction,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        match action {
            UpstreamAction::Simulate => self.simulate(state, route, raw_body, submit_deadline).await,
            UpstreamAction::Submit => self.submit(state, route, raw_body, submit_deadline).await,
        }
    }

    async fn simulate(
        self,
        state: &AppState,
        route: &str,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        validate_simulate_payload_for_route(raw_body, route, state.config.contract_version.as_str())?;
        forward_to_upstream(
            state,
            route,
            UpstreamAction::Simulate,
            raw_body,
            submit_deadline,
        )
        .await
    }

    async fn submit(
        self,
        state: &AppState,
        route: &str,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        validate_rpc_submit_tip_payload(raw_body, route, state.config.contract_version.as_str())?;
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

impl JitoRouteExecutor {
    async fn execute_action(
        self,
        state: &AppState,
        route: &str,
        action: UpstreamAction,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        match action {
            UpstreamAction::Simulate => self.simulate(state, route, raw_body, submit_deadline).await,
            UpstreamAction::Submit => self.submit(state, route, raw_body, submit_deadline).await,
        }
    }

    async fn simulate(
        self,
        state: &AppState,
        route: &str,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        validate_simulate_payload_for_route(raw_body, route, state.config.contract_version.as_str())?;
        forward_to_upstream(
            state,
            route,
            UpstreamAction::Simulate,
            raw_body,
            submit_deadline,
        )
        .await
    }

    async fn submit(
        self,
        state: &AppState,
        route: &str,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        validate_submit_payload_for_route(raw_body, route, state.config.contract_version.as_str())?;
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

impl FastlaneRouteExecutor {
    async fn execute_action(
        self,
        state: &AppState,
        route: &str,
        action: UpstreamAction,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        match action {
            UpstreamAction::Simulate => self.simulate(state, route, raw_body, submit_deadline).await,
            UpstreamAction::Submit => self.submit(state, route, raw_body, submit_deadline).await,
        }
    }

    async fn simulate(
        self,
        state: &AppState,
        route: &str,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        validate_simulate_payload_for_route(raw_body, route, state.config.contract_version.as_str())?;
        forward_to_upstream(
            state,
            route,
            UpstreamAction::Simulate,
            raw_body,
            submit_deadline,
        )
        .await
    }

    async fn submit(
        self,
        state: &AppState,
        route: &str,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        validate_submit_payload_for_route(raw_body, route, state.config.contract_version.as_str())?;
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

fn validate_submit_payload_for_route(
    raw_body: &[u8],
    expected_route: &str,
    expected_contract_version: &str,
) -> std::result::Result<serde_json::Map<String, Value>, Reject> {
    let payload = validate_payload_route_for_action(raw_body, expected_route, "submit")?;
    validate_optional_payload_action_field(&payload, "submit", "submit")?;
    validate_required_payload_contract_version_field(
        &payload,
        "submit",
        expected_contract_version,
    )?;
    Ok(payload)
}

fn validate_simulate_payload_for_route(
    raw_body: &[u8],
    expected_route: &str,
    expected_contract_version: &str,
) -> std::result::Result<serde_json::Map<String, Value>, Reject> {
    let payload = validate_payload_route_for_action(raw_body, expected_route, "simulate")?;
    validate_required_payload_action_field(&payload, "simulate", "simulate")?;
    validate_required_payload_contract_version_field(
        &payload,
        "simulate",
        expected_contract_version,
    )?;
    Ok(payload)
}

fn validate_rpc_submit_tip_payload(
    raw_body: &[u8],
    expected_route: &str,
    expected_contract_version: &str,
) -> std::result::Result<(), Reject> {
    let payload =
        validate_submit_payload_for_route(raw_body, expected_route, expected_contract_version)?;
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
        validate_rpc_submit_tip_payload, validate_simulate_payload_for_route,
        validate_submit_payload_for_route, RouteAdapter,
    };
    use crate::route_executor::RouteExecutorKind;

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
    fn validate_rpc_submit_tip_payload_accepts_zero_tip() {
        let body = br#"{"tip_lamports":0,"route":"rpc","contract_version":"v1"}"#;
        assert!(validate_rpc_submit_tip_payload(body, "rpc", "v1").is_ok());
    }

    #[test]
    fn validate_rpc_submit_tip_payload_rejects_nonzero_tip() {
        let body = br#"{"tip_lamports":42,"route":"rpc","contract_version":"v1"}"#;
        let reject = validate_rpc_submit_tip_payload(body, "rpc", "v1")
            .expect_err("non-zero rpc tip must be rejected");
        assert_eq!(reject.code, "tip_not_supported");
        assert!(!reject.retryable);
    }

    #[test]
    fn validate_rpc_submit_tip_payload_rejects_invalid_json() {
        let body = br#"{"tip_lamports":"oops""#;
        let reject = validate_rpc_submit_tip_payload(body, "rpc", "v1")
            .expect_err("invalid json must reject");
        assert_eq!(reject.code, "invalid_request_body");
    }

    #[test]
    fn validate_rpc_submit_tip_payload_rejects_non_object_payload() {
        let body = br#"["rpc"]"#;
        let reject = validate_rpc_submit_tip_payload(body, "rpc", "v1")
            .expect_err("non-object payload must reject");
        assert_eq!(reject.code, "invalid_request_body");
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_mismatched_route() {
        let body = br#"{"route":"jito","tip_lamports":0,"contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1")
            .expect_err("route mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route mismatch"));
    }

    #[test]
    fn validate_submit_payload_for_route_accepts_matching_route_case_insensitive() {
        let body = br#"{"route":" RPC ","tip_lamports":0,"contract_version":"v1"}"#;
        assert!(validate_submit_payload_for_route(body, "rpc", "v1").is_ok());
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_missing_route() {
        let body = br#"{"tip_lamports":0,"contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1")
            .expect_err("missing route must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing route"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_non_string_route() {
        let body = br#"{"route":123,"tip_lamports":0,"contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1")
            .expect_err("non-string route must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route must be string"));
    }

    #[test]
    fn validate_submit_payload_for_route_accepts_missing_action() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v1"}"#;
        assert!(validate_submit_payload_for_route(body, "rpc", "v1").is_ok());
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_mismatched_action_when_present() {
        let body = br#"{"route":"rpc","tip_lamports":0,"action":"simulate","contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1")
            .expect_err("mismatched submit action must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("action mismatch"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_non_string_action_when_present() {
        let body = br#"{"route":"rpc","tip_lamports":0,"action":123,"contract_version":"v1"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1")
            .expect_err("non-string submit action must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("action must be string"));
    }

    #[test]
    fn validate_submit_payload_for_route_accepts_matching_action_case_insensitive_when_present() {
        let body = br#"{"route":"rpc","tip_lamports":0,"action":" SUBMIT ","contract_version":"v1"}"#;
        assert!(validate_submit_payload_for_route(body, "rpc", "v1").is_ok());
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_missing_contract_version() {
        let body = br#"{"route":"rpc","tip_lamports":0}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1")
            .expect_err("submit missing contract_version must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing contract_version"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_non_string_contract_version() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":123}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1")
            .expect_err("submit non-string contract_version must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version must be string"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_empty_contract_version() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":" "}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1")
            .expect_err("submit empty contract_version must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version must be non-empty"));
    }

    #[test]
    fn validate_submit_payload_for_route_rejects_contract_version_mismatch() {
        let body = br#"{"route":"rpc","tip_lamports":0,"contract_version":"v2"}"#;
        let reject = validate_submit_payload_for_route(body, "rpc", "v1")
            .expect_err("submit contract_version mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version mismatch"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_mismatched_route() {
        let body = br#"{"route":"jito","action":"simulate","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1")
            .expect_err("simulate route mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route mismatch"));
    }

    #[test]
    fn validate_simulate_payload_for_route_accepts_matching_route_case_insensitive() {
        let body = br#"{"route":" RPC ","action":"simulate","dry_run":true,"contract_version":"v1"}"#;
        assert!(validate_simulate_payload_for_route(body, "rpc", "v1").is_ok());
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_missing_route() {
        let body = br#"{"action":"simulate","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1")
            .expect_err("simulate missing route must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing route"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_non_string_route() {
        let body = br#"{"route":123,"action":"simulate","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1")
            .expect_err("simulate non-string route must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route must be string"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_missing_action() {
        let body = br#"{"route":"rpc","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1")
            .expect_err("simulate missing action must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing action"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_non_string_action() {
        let body = br#"{"route":"rpc","action":123,"dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1")
            .expect_err("simulate non-string action must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("action must be string"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_mismatched_action() {
        let body = br#"{"route":"rpc","action":"submit","dry_run":true,"contract_version":"v1"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1")
            .expect_err("simulate mismatched action must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("action mismatch"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_missing_contract_version() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1")
            .expect_err("simulate missing contract_version must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing contract_version"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_non_string_contract_version() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":123}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1")
            .expect_err("simulate non-string contract_version must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version must be string"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_empty_contract_version() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":" "}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1")
            .expect_err("simulate empty contract_version must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version must be non-empty"));
    }

    #[test]
    fn validate_simulate_payload_for_route_rejects_contract_version_mismatch() {
        let body = br#"{"route":"rpc","action":"simulate","dry_run":true,"contract_version":"v2"}"#;
        let reject = validate_simulate_payload_for_route(body, "rpc", "v1")
            .expect_err("simulate contract_version mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version mismatch"));
    }
}
