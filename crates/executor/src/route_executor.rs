use serde_json::Value;
use tracing::debug;

use crate::route_backend::UpstreamAction;
use crate::route_normalization::normalize_route;
use crate::route_policy::{classify_normalized_route, RouteKind};
use crate::submit_deadline::SubmitDeadline;
use crate::upstream_forward::forward_to_upstream;
use crate::{AppState, Reject};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteExecutorKind {
    Paper,
    Rpc,
    Jito,
    Fastlane,
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
) -> std::result::Result<Value, Reject> {
    let normalized_route = normalize_route(route);
    let route_executor_kind =
        resolve_route_executor_kind_normalized(normalized_route.as_str()).ok_or_else(|| {
            Reject::terminal(
                "route_not_allowed",
                format!("route={} is not supported by route executor", route),
            )
        })?;
    debug!(
        route = %normalized_route,
        action = %action.as_str(),
        route_executor = %route_executor_kind.as_str(),
        "executing route action"
    );

    match route_executor_kind {
        RouteExecutorKind::Paper => {
            execute_paper_route_action(
                state,
                normalized_route.as_str(),
                action,
                raw_body,
                submit_deadline,
            )
            .await
        }
        RouteExecutorKind::Rpc => {
            execute_rpc_route_action(
                state,
                normalized_route.as_str(),
                action,
                raw_body,
                submit_deadline,
            )
            .await
        }
        RouteExecutorKind::Jito => {
            execute_jito_route_action(
                state,
                normalized_route.as_str(),
                action,
                raw_body,
                submit_deadline,
            )
            .await
        }
        RouteExecutorKind::Fastlane => {
            execute_fastlane_route_action(
                state,
                normalized_route.as_str(),
                action,
                raw_body,
                submit_deadline,
            )
            .await
        }
    }
}

async fn execute_paper_route_action(
    state: &AppState,
    route: &str,
    action: UpstreamAction,
    raw_body: &[u8],
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<Value, Reject> {
    forward_to_upstream(state, route, action, raw_body, submit_deadline).await
}

async fn execute_rpc_route_action(
    state: &AppState,
    route: &str,
    action: UpstreamAction,
    raw_body: &[u8],
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<Value, Reject> {
    forward_to_upstream(state, route, action, raw_body, submit_deadline).await
}

async fn execute_jito_route_action(
    state: &AppState,
    route: &str,
    action: UpstreamAction,
    raw_body: &[u8],
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<Value, Reject> {
    forward_to_upstream(state, route, action, raw_body, submit_deadline).await
}

async fn execute_fastlane_route_action(
    state: &AppState,
    route: &str,
    action: UpstreamAction,
    raw_body: &[u8],
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<Value, Reject> {
    forward_to_upstream(state, route, action, raw_body, submit_deadline).await
}

#[cfg(test)]
mod tests {
    use super::{
        resolve_route_executor_kind, resolve_route_executor_kind_normalized, RouteExecutorKind,
    };

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
}
