use serde_json::Value;
use std::collections::{HashMap, HashSet};
use tracing::debug;

use crate::route_adapters::RouteAdapter;
use crate::route_backend::{RouteBackend, UpstreamAction};
use crate::route_normalization::normalize_route;
use crate::route_policy::{classify_normalized_route, requires_submit_fastlane_enabled, RouteKind};
use crate::submit_deadline::SubmitDeadline;
use crate::tx_build::SubmitInstructionPlan;
use crate::{AppState, Reject};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteExecutorKind {
    Paper,
    Rpc,
    Jito,
    Fastlane,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RouteActionPayloadExpectations<'a> {
    pub(crate) request_id: Option<&'a str>,
    pub(crate) signal_id: Option<&'a str>,
    pub(crate) client_order_id: Option<&'a str>,
    pub(crate) side: Option<&'a str>,
    pub(crate) token: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RouteSubmitExecutionContext {
    pub(crate) instruction_plan: Option<SubmitInstructionPlan>,
}

impl RouteExecutorKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Paper => "paper",
            Self::Rpc => "rpc",
            Self::Jito => "jito",
            Self::Fastlane => "fastlane",
        }
    }
}

fn resolve_route_executor_kind_normalized(route: &str) -> Option<RouteExecutorKind> {
    match classify_normalized_route(route) {
        RouteKind::Paper => Some(RouteExecutorKind::Paper),
        RouteKind::Rpc => Some(RouteExecutorKind::Rpc),
        RouteKind::Jito => Some(RouteExecutorKind::Jito),
        RouteKind::Fastlane => Some(RouteExecutorKind::Fastlane),
        RouteKind::Other => None,
    }
}

fn validate_route_executor_feature_gate(
    normalized_route: &str,
    submit_fastlane_enabled: bool,
) -> std::result::Result<(), Reject> {
    if requires_submit_fastlane_enabled(normalized_route) && !submit_fastlane_enabled {
        return Err(Reject::terminal(
            "fastlane_not_enabled",
            "route=fastlane requires COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED=true",
        ));
    }
    Ok(())
}

fn validate_route_executor_allowlist(
    normalized_route: &str,
    route_allowlist: &HashSet<String>,
) -> std::result::Result<(), Reject> {
    if !route_allowlist.contains(normalized_route) {
        return Err(Reject::terminal(
            "route_not_allowed",
            format!("route={} is not allowed", normalized_route),
        ));
    }
    Ok(())
}

fn validate_route_executor_backend_configured(
    normalized_route: &str,
    route_backends: &HashMap<String, RouteBackend>,
) -> std::result::Result<(), Reject> {
    if !route_backends.contains_key(normalized_route) {
        return Err(Reject::terminal(
            "route_not_allowed",
            format!("route={} not configured", normalized_route),
        ));
    }
    Ok(())
}

fn validate_route_executor_action_context(
    action: UpstreamAction,
    submit_context: RouteSubmitExecutionContext,
) -> std::result::Result<(), Reject> {
    match action {
        UpstreamAction::Submit if submit_context.instruction_plan.is_none() => Err(
            Reject::terminal(
                "invalid_request_body",
                "submit route action missing instruction plan at route-executor boundary",
            ),
        ),
        UpstreamAction::Simulate if submit_context.instruction_plan.is_some() => Err(
            Reject::terminal(
                "invalid_request_body",
                "simulate route action must not include submit instruction plan at route-executor boundary",
            ),
        ),
        _ => Ok(()),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn resolve_route_executor_kind(route: &str) -> Option<RouteExecutorKind> {
    let normalized_route = normalize_route(route);
    resolve_route_executor_kind_normalized(normalized_route.as_str())
}

pub(crate) async fn execute_route_action(
    state: &AppState,
    route: &str,
    action: UpstreamAction,
    raw_body: &[u8],
    submit_deadline: Option<&SubmitDeadline>,
    payload_expectations: RouteActionPayloadExpectations<'_>,
    submit_context: RouteSubmitExecutionContext,
) -> std::result::Result<Value, Reject> {
    let normalized_route = normalize_route(route);
    let route_executor_kind =
        resolve_route_executor_kind_normalized(normalized_route.as_str()).ok_or_else(|| {
            Reject::terminal(
                "route_not_allowed",
                format!("route={} is not supported by route executor", route),
            )
        })?;
    validate_route_executor_allowlist(normalized_route.as_str(), &state.config.route_allowlist)?;
    validate_route_executor_backend_configured(normalized_route.as_str(), &state.config.route_backends)?;
    validate_route_executor_feature_gate(
        normalized_route.as_str(),
        state.config.submit_fastlane_enabled,
    )?;
    validate_route_executor_action_context(action, submit_context)?;
    let route_adapter = RouteAdapter::from_kind(route_executor_kind);
    debug!(
        route = %normalized_route,
        action = %action.as_str(),
        route_executor = %route_executor_kind.as_str(),
        route_adapter = %route_adapter.as_str(),
        submit_instruction_plan_present = submit_context.instruction_plan.is_some(),
        "executing route action"
    );
    route_adapter
        .execute(
            state,
            normalized_route.as_str(),
            action,
            raw_body,
            submit_deadline,
            payload_expectations,
            submit_context,
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::{
        validate_route_executor_action_context, validate_route_executor_allowlist,
        validate_route_executor_backend_configured,
        validate_route_executor_feature_gate, resolve_route_executor_kind,
        resolve_route_executor_kind_normalized,
        RouteSubmitExecutionContext, RouteExecutorKind,
    };
    use crate::route_backend::{RouteBackend, UpstreamAction};
    use crate::tx_build::SubmitInstructionPlan;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn route_executor_resolve_maps_known_routes_case_insensitive() {
        assert_eq!(
            resolve_route_executor_kind("paper"),
            Some(RouteExecutorKind::Paper)
        );
        assert_eq!(resolve_route_executor_kind("RPC"), Some(RouteExecutorKind::Rpc));
        assert_eq!(
            resolve_route_executor_kind(" jito "),
            Some(RouteExecutorKind::Jito)
        );
        assert_eq!(
            resolve_route_executor_kind("FastLane"),
            Some(RouteExecutorKind::Fastlane)
        );
    }

    #[test]
    fn route_executor_resolve_rejects_unknown_route() {
        assert_eq!(resolve_route_executor_kind("unknown"), None);
    }

    #[test]
    fn route_executor_kind_as_str_matches_contract_route_tokens() {
        assert_eq!(RouteExecutorKind::Paper.as_str(), "paper");
        assert_eq!(RouteExecutorKind::Rpc.as_str(), "rpc");
        assert_eq!(RouteExecutorKind::Jito.as_str(), "jito");
        assert_eq!(RouteExecutorKind::Fastlane.as_str(), "fastlane");
    }

    #[test]
    fn route_executor_resolve_normalized_route_for_backend_lookup() {
        assert_eq!(
            resolve_route_executor_kind_normalized("rpc"),
            Some(RouteExecutorKind::Rpc)
        );
        assert_eq!(
            resolve_route_executor_kind(" RPC "),
            Some(RouteExecutorKind::Rpc)
        );
    }

    #[test]
    fn route_executor_submit_execution_context_defaults_none() {
        let context = RouteSubmitExecutionContext::default();
        assert!(context.instruction_plan.is_none());
    }

    #[test]
    fn route_executor_feature_gate_rejects_fastlane_when_disabled() {
        let reject = validate_route_executor_feature_gate("fastlane", false)
            .expect_err("fastlane must be blocked when feature disabled");
        assert_eq!(reject.code, "fastlane_not_enabled");
        assert!(!reject.retryable);
    }

    #[test]
    fn route_executor_feature_gate_allows_fastlane_when_enabled() {
        validate_route_executor_feature_gate("fastlane", true)
            .expect("fastlane should pass when feature enabled");
    }

    #[test]
    fn route_executor_allowlist_rejects_route_not_in_allowlist() {
        let allowlist = HashSet::from([String::from("rpc")]);
        let reject = validate_route_executor_allowlist("jito", &allowlist)
            .expect_err("route not in allowlist must be rejected");
        assert_eq!(reject.code, "route_not_allowed");
        assert!(!reject.retryable);
    }

    #[test]
    fn route_executor_allowlist_accepts_route_in_allowlist() {
        let allowlist = HashSet::from([String::from("rpc"), String::from("jito")]);
        validate_route_executor_allowlist("jito", &allowlist)
            .expect("allowlisted route should pass");
    }

    #[test]
    fn route_executor_backend_configured_rejects_missing_backend() {
        let backends = HashMap::from([(
            String::from("rpc"),
            RouteBackend {
                submit_url: "https://submit.primary".to_string(),
                submit_fallback_url: None,
                simulate_url: "https://simulate.primary".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        )]);
        let reject = validate_route_executor_backend_configured("jito", &backends)
            .expect_err("missing route backend must be rejected");
        assert_eq!(reject.code, "route_not_allowed");
        assert!(!reject.retryable);
    }

    #[test]
    fn route_executor_backend_configured_accepts_present_backend() {
        let backends = HashMap::from([(
            String::from("rpc"),
            RouteBackend {
                submit_url: "https://submit.primary".to_string(),
                submit_fallback_url: None,
                simulate_url: "https://simulate.primary".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        )]);
        validate_route_executor_backend_configured("rpc", &backends)
            .expect("present route backend should pass");
    }

    #[test]
    fn route_executor_action_context_rejects_submit_without_plan() {
        let reject =
            validate_route_executor_action_context(UpstreamAction::Submit, RouteSubmitExecutionContext::default())
                .expect_err("submit without plan must be rejected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(!reject.retryable);
    }

    #[test]
    fn route_executor_action_context_rejects_simulate_with_plan() {
        let reject = validate_route_executor_action_context(
            UpstreamAction::Simulate,
            RouteSubmitExecutionContext {
                instruction_plan: Some(SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
            },
        )
        .expect_err("simulate with submit plan must be rejected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(!reject.retryable);
    }
}
