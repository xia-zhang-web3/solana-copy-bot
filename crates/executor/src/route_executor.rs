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
    pub(crate) route_hint: Option<&'a str>,
    pub(crate) request_id: Option<&'a str>,
    pub(crate) signal_id: Option<&'a str>,
    pub(crate) client_order_id: Option<&'a str>,
    pub(crate) side: Option<&'a str>,
    pub(crate) token: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RouteSubmitExecutionContext {
    pub(crate) instruction_plan: Option<SubmitInstructionPlan>,
    pub(crate) expected_slippage_bps: Option<f64>,
    pub(crate) expected_route_slippage_cap_bps: Option<f64>,
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
    let validate_finite = |field_name: &str, value: f64| -> std::result::Result<(), Reject> {
        if !value.is_finite() {
            return Err(Reject::terminal(
                "invalid_request_body",
                format!(
                    "submit route action {} expectation must be finite at route-executor boundary",
                    field_name
                ),
            ));
        }
        Ok(())
    };
    match action {
        UpstreamAction::Submit => {
            if submit_context.instruction_plan.is_none() {
                return Err(Reject::terminal(
                    "invalid_request_body",
                    "submit route action missing instruction plan at route-executor boundary",
                ));
            }
            let expected_slippage_bps = submit_context.expected_slippage_bps.ok_or_else(|| {
                Reject::terminal(
                    "invalid_request_body",
                    "submit route action missing slippage_bps expectation at route-executor boundary",
                )
            })?;
            let expected_route_slippage_cap_bps = submit_context
                .expected_route_slippage_cap_bps
                .ok_or_else(|| {
                    Reject::terminal(
                        "invalid_request_body",
                        "submit route action missing route_slippage_cap_bps expectation at route-executor boundary",
                    )
                })?;
            validate_finite("slippage_bps", expected_slippage_bps)?;
            validate_finite("route_slippage_cap_bps", expected_route_slippage_cap_bps)?;
            Ok(())
        }
        UpstreamAction::Simulate => {
            if submit_context.instruction_plan.is_some() {
                return Err(Reject::terminal(
                    "invalid_request_body",
                    "simulate route action must not include submit instruction plan at route-executor boundary",
                ));
            }
            if submit_context.expected_slippage_bps.is_some() {
                return Err(Reject::terminal(
                    "invalid_request_body",
                    "simulate route action must not include slippage_bps expectation at route-executor boundary",
                ));
            }
            if submit_context.expected_route_slippage_cap_bps.is_some() {
                return Err(Reject::terminal(
                    "invalid_request_body",
                    "simulate route action must not include route_slippage_cap_bps expectation at route-executor boundary",
                ));
            }
            Ok(())
        }
    }
}

fn validate_route_executor_deadline_context(
    action: UpstreamAction,
    submit_context: RouteSubmitExecutionContext,
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<(), Reject> {
    match action {
        UpstreamAction::Submit
            if submit_context.instruction_plan.is_some() && submit_deadline.is_none() =>
        {
            Err(Reject::terminal(
                "invalid_request_body",
                "submit route action missing deadline at route-executor boundary",
            ))
        }
        UpstreamAction::Simulate if submit_deadline.is_some() => Err(Reject::terminal(
            "invalid_request_body",
            "simulate route action must not include submit deadline at route-executor boundary",
        )),
        _ => Ok(()),
    }
}

fn validate_route_executor_payload_expectations_shape(
    action: UpstreamAction,
    payload_expectations: RouteActionPayloadExpectations<'_>,
) -> std::result::Result<(), Reject> {
    let require = |field_name: &str, value: Option<&str>| {
        let value = value.ok_or_else(|| {
            Reject::terminal(
                "invalid_request_body",
                format!(
                    "route action missing {} expectation at route-executor boundary",
                    field_name
                ),
            )
        })?;
        if value.trim().is_empty() {
            return Err(Reject::terminal(
                "invalid_request_body",
                format!(
                    "route action has empty {} expectation at route-executor boundary",
                    field_name
                ),
            ));
        }
        Ok(())
    };
    require("request_id", payload_expectations.request_id)?;
    require("signal_id", payload_expectations.signal_id)?;
    require("side", payload_expectations.side)?;
    require("token", payload_expectations.token)?;
    match action {
        UpstreamAction::Submit => require("client_order_id", payload_expectations.client_order_id),
        UpstreamAction::Simulate if payload_expectations.client_order_id.is_some() => {
            Err(Reject::terminal(
                "invalid_request_body",
                "simulate route action must not include client_order_id expectation at route-executor boundary",
            ))
        }
        UpstreamAction::Simulate => Ok(()),
    }
}

fn validate_route_executor_payload_route_expectation(
    normalized_route: &str,
    payload_expectations: RouteActionPayloadExpectations<'_>,
) -> std::result::Result<(), Reject> {
    let payload_route = payload_expectations
        .route_hint
        .ok_or_else(|| {
            Reject::terminal(
                "invalid_request_body",
                "route payload hint missing at route-executor boundary",
            )
        })?;
    let normalized_payload_route = normalize_route(payload_route);
    if normalized_payload_route != normalized_route {
        return Err(Reject::terminal(
            "invalid_request_body",
            format!(
                "route payload mismatch at route-executor boundary expected={} got={}",
                normalized_route, normalized_payload_route
            ),
        ));
    }
    Ok(())
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
    validate_route_executor_payload_route_expectation(normalized_route.as_str(), payload_expectations)?;
    validate_route_executor_payload_expectations_shape(action, payload_expectations)?;
    validate_route_executor_deadline_context(action, submit_context, submit_deadline)?;
    validate_route_executor_action_context(action, submit_context)?;
    validate_route_executor_allowlist(normalized_route.as_str(), &state.config.route_allowlist)?;
    validate_route_executor_backend_configured(normalized_route.as_str(), &state.config.route_backends)?;
    validate_route_executor_feature_gate(
        normalized_route.as_str(),
        state.config.submit_fastlane_enabled,
    )?;
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
        validate_route_executor_backend_configured, validate_route_executor_deadline_context,
        validate_route_executor_feature_gate,
        validate_route_executor_payload_expectations_shape,
        validate_route_executor_payload_route_expectation,
        resolve_route_executor_kind,
        resolve_route_executor_kind_normalized,
        RouteActionPayloadExpectations, RouteSubmitExecutionContext, RouteExecutorKind,
    };
    use crate::route_backend::{RouteBackend, UpstreamAction};
    use crate::submit_deadline::SubmitDeadline;
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
                ..RouteSubmitExecutionContext::default()
            },
        )
        .expect_err("simulate with submit plan must be rejected");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(!reject.retryable);
    }

    #[test]
    fn route_executor_action_context_rejects_submit_missing_slippage_expectation() {
        let reject = validate_route_executor_action_context(
            UpstreamAction::Submit,
            RouteSubmitExecutionContext {
                instruction_plan: Some(SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: None,
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .expect_err("submit must require slippage expectation");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(!reject.retryable);
        assert!(reject.detail.contains("missing slippage_bps expectation"));
    }

    #[test]
    fn route_executor_action_context_rejects_submit_missing_route_slippage_cap_expectation() {
        let reject = validate_route_executor_action_context(
            UpstreamAction::Submit,
            RouteSubmitExecutionContext {
                instruction_plan: Some(SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: None,
            },
        )
        .expect_err("submit must require route slippage cap expectation");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(!reject.retryable);
        assert!(reject
            .detail
            .contains("missing route_slippage_cap_bps expectation"));
    }

    #[test]
    fn route_executor_action_context_rejects_simulate_with_slippage_expectation() {
        let reject = validate_route_executor_action_context(
            UpstreamAction::Simulate,
            RouteSubmitExecutionContext {
                expected_slippage_bps: Some(10.0),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .expect_err("simulate must not include slippage expectation");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(!reject.retryable);
        assert!(reject
            .detail
            .contains("must not include slippage_bps expectation"));
    }

    #[test]
    fn route_executor_action_context_rejects_simulate_with_route_slippage_cap_expectation() {
        let reject = validate_route_executor_action_context(
            UpstreamAction::Simulate,
            RouteSubmitExecutionContext {
                expected_route_slippage_cap_bps: Some(20.0),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .expect_err("simulate must not include route slippage cap expectation");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(!reject.retryable);
        assert!(reject
            .detail
            .contains("must not include route_slippage_cap_bps expectation"));
    }

    #[test]
    fn route_executor_action_context_accepts_submit_with_plan_and_slippage_expectations() {
        validate_route_executor_action_context(
            UpstreamAction::Submit,
            RouteSubmitExecutionContext {
                instruction_plan: Some(SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .expect("submit with complete expectations should pass");
    }

    #[test]
    fn route_executor_action_context_rejects_submit_with_non_finite_slippage_expectation() {
        let reject = validate_route_executor_action_context(
            UpstreamAction::Submit,
            RouteSubmitExecutionContext {
                instruction_plan: Some(SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(f64::NAN),
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .expect_err("submit must reject non-finite slippage expectation");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(!reject.retryable);
        assert!(reject
            .detail
            .contains("slippage_bps expectation must be finite"));
    }

    #[test]
    fn route_executor_action_context_rejects_submit_with_non_finite_route_slippage_cap_expectation() {
        let reject = validate_route_executor_action_context(
            UpstreamAction::Submit,
            RouteSubmitExecutionContext {
                instruction_plan: Some(SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: Some(f64::INFINITY),
            },
        )
        .expect_err("submit must reject non-finite route slippage cap expectation");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(!reject.retryable);
        assert!(reject
            .detail
            .contains("route_slippage_cap_bps expectation must be finite"));
    }

    #[test]
    fn route_executor_deadline_context_rejects_submit_with_plan_without_deadline() {
        let reject = validate_route_executor_deadline_context(
            UpstreamAction::Submit,
            RouteSubmitExecutionContext {
                instruction_plan: Some(SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
            None,
        )
        .expect_err("submit with plan must require deadline");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[test]
    fn route_executor_deadline_context_rejects_simulate_with_deadline() {
        let deadline = SubmitDeadline::new(1_000);
        let reject = validate_route_executor_deadline_context(
            UpstreamAction::Simulate,
            RouteSubmitExecutionContext::default(),
            Some(&deadline),
        )
        .expect_err("simulate must not include submit deadline");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("must not include submit deadline"));
    }

    #[test]
    fn route_executor_deadline_context_accepts_submit_with_plan_and_deadline() {
        let deadline = SubmitDeadline::new(1_000);
        validate_route_executor_deadline_context(
            UpstreamAction::Submit,
            RouteSubmitExecutionContext {
                instruction_plan: Some(SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
            Some(&deadline),
        )
        .expect("submit with deadline should pass");
    }

    #[test]
    fn route_executor_deadline_context_accepts_simulate_without_deadline() {
        validate_route_executor_deadline_context(
            UpstreamAction::Simulate,
            RouteSubmitExecutionContext::default(),
            None,
        )
        .expect("simulate without deadline should pass");
    }

    #[test]
    fn route_executor_payload_route_expectation_rejects_missing_hint() {
        let reject = validate_route_executor_payload_route_expectation(
            "rpc",
            RouteActionPayloadExpectations::default(),
        )
        .expect_err("missing route hint must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(!reject.retryable);
    }

    #[test]
    fn route_executor_payload_route_expectation_rejects_mismatch() {
        let reject = validate_route_executor_payload_route_expectation(
            "rpc",
            RouteActionPayloadExpectations {
                route_hint: Some("jito"),
                ..RouteActionPayloadExpectations::default()
            },
        )
        .expect_err("route hint mismatch must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route payload mismatch"));
    }

    #[test]
    fn route_executor_payload_route_expectation_accepts_match_case_insensitive() {
        validate_route_executor_payload_route_expectation(
            "rpc",
            RouteActionPayloadExpectations {
                route_hint: Some(" RPC "),
                ..RouteActionPayloadExpectations::default()
            },
        )
        .expect("matching route hint should pass");
    }

    #[test]
    fn route_executor_payload_expectations_shape_rejects_submit_missing_client_order_id() {
        let reject = validate_route_executor_payload_expectations_shape(
            UpstreamAction::Submit,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-id-1"),
                signal_id: Some("signal-id-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
        )
        .expect_err("submit must require client_order_id expectation");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing client_order_id expectation"));
    }

    #[test]
    fn route_executor_payload_expectations_shape_rejects_simulate_with_client_order_id() {
        let reject = validate_route_executor_payload_expectations_shape(
            UpstreamAction::Simulate,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-id-1"),
                signal_id: Some("signal-id-1"),
                client_order_id: Some("client-order-id-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
        )
        .expect_err("simulate must reject client_order_id expectation");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("must not include client_order_id expectation"));
    }

    #[test]
    fn route_executor_payload_expectations_shape_accepts_submit_required_fields() {
        validate_route_executor_payload_expectations_shape(
            UpstreamAction::Submit,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-id-1"),
                signal_id: Some("signal-id-1"),
                client_order_id: Some("client-order-id-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
        )
        .expect("submit with complete expectations should pass");
    }

    #[test]
    fn route_executor_payload_expectations_shape_accepts_simulate_required_fields() {
        validate_route_executor_payload_expectations_shape(
            UpstreamAction::Simulate,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-id-1"),
                signal_id: Some("signal-id-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
        )
        .expect("simulate with complete expectations should pass");
    }

    #[test]
    fn route_executor_payload_expectations_shape_rejects_missing_shared_fields() {
        let cases = [
            (
                "request_id",
                RouteActionPayloadExpectations {
                    route_hint: Some("rpc"),
                    request_id: None,
                    signal_id: Some("signal-id-1"),
                    client_order_id: Some("client-order-id-1"),
                    side: Some("buy"),
                    token: Some("11111111111111111111111111111111"),
                },
            ),
            (
                "signal_id",
                RouteActionPayloadExpectations {
                    route_hint: Some("rpc"),
                    request_id: Some("request-id-1"),
                    signal_id: None,
                    client_order_id: Some("client-order-id-1"),
                    side: Some("buy"),
                    token: Some("11111111111111111111111111111111"),
                },
            ),
            (
                "side",
                RouteActionPayloadExpectations {
                    route_hint: Some("rpc"),
                    request_id: Some("request-id-1"),
                    signal_id: Some("signal-id-1"),
                    client_order_id: Some("client-order-id-1"),
                    side: None,
                    token: Some("11111111111111111111111111111111"),
                },
            ),
            (
                "token",
                RouteActionPayloadExpectations {
                    route_hint: Some("rpc"),
                    request_id: Some("request-id-1"),
                    signal_id: Some("signal-id-1"),
                    client_order_id: Some("client-order-id-1"),
                    side: Some("buy"),
                    token: None,
                },
            ),
        ];

        for (missing_field, expectations) in cases {
            let reject =
                validate_route_executor_payload_expectations_shape(UpstreamAction::Submit, expectations)
                    .expect_err("missing shared expectation must reject");
            assert_eq!(reject.code, "invalid_request_body");
            assert!(reject
                .detail
                .contains(format!("missing {} expectation", missing_field).as_str()));
        }
    }

    #[test]
    fn route_executor_payload_expectations_shape_rejects_empty_shared_fields() {
        let cases = [
            (
                "request_id",
                RouteActionPayloadExpectations {
                    route_hint: Some("rpc"),
                    request_id: Some(""),
                    signal_id: Some("signal-id-1"),
                    client_order_id: Some("client-order-id-1"),
                    side: Some("buy"),
                    token: Some("11111111111111111111111111111111"),
                },
            ),
            (
                "signal_id",
                RouteActionPayloadExpectations {
                    route_hint: Some("rpc"),
                    request_id: Some("request-id-1"),
                    signal_id: Some("  "),
                    client_order_id: Some("client-order-id-1"),
                    side: Some("buy"),
                    token: Some("11111111111111111111111111111111"),
                },
            ),
            (
                "side",
                RouteActionPayloadExpectations {
                    route_hint: Some("rpc"),
                    request_id: Some("request-id-1"),
                    signal_id: Some("signal-id-1"),
                    client_order_id: Some("client-order-id-1"),
                    side: Some(""),
                    token: Some("11111111111111111111111111111111"),
                },
            ),
            (
                "token",
                RouteActionPayloadExpectations {
                    route_hint: Some("rpc"),
                    request_id: Some("request-id-1"),
                    signal_id: Some("signal-id-1"),
                    client_order_id: Some("client-order-id-1"),
                    side: Some("buy"),
                    token: Some(""),
                },
            ),
            (
                "client_order_id",
                RouteActionPayloadExpectations {
                    route_hint: Some("rpc"),
                    request_id: Some("request-id-1"),
                    signal_id: Some("signal-id-1"),
                    client_order_id: Some(" "),
                    side: Some("buy"),
                    token: Some("11111111111111111111111111111111"),
                },
            ),
        ];

        for (empty_field, expectations) in cases {
            let reject =
                validate_route_executor_payload_expectations_shape(UpstreamAction::Submit, expectations)
                    .expect_err("empty shared expectation must reject");
            assert_eq!(reject.code, "invalid_request_body");
            assert!(reject
                .detail
                .contains(format!("empty {} expectation", empty_field).as_str()));
        }
    }
}
