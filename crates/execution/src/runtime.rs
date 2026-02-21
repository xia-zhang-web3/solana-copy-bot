use crate::ExecutionRuntime;
use std::collections::BTreeMap;

impl ExecutionRuntime {
    pub(crate) fn submit_route_for_attempt(&self, attempt: u32) -> &str {
        if self.submit_route_order.is_empty() {
            return self.default_route.as_str();
        }
        let index = attempt.saturating_sub(1) as usize;
        let index = index.min(self.submit_route_order.len().saturating_sub(1));
        self.submit_route_order[index].as_str()
    }

    pub(crate) fn route_tip_lamports(&self, route: &str) -> u64 {
        normalize_route(route)
            .and_then(|value| self.route_tip_lamports.get(value.as_str()).copied())
            .unwrap_or(0)
    }

    pub(crate) fn submit_fallback_route_allowed(
        &self,
        current_route: &str,
        next_route: &str,
        retryable_error_code: &str,
    ) -> bool {
        let Some(current_route) = normalize_route(current_route) else {
            return true;
        };
        let Some(next_route) = normalize_route(next_route) else {
            return true;
        };
        if current_route == next_route {
            return true;
        }
        if current_route == "jito" && next_route == "rpc" {
            return is_allowed_jito_rpc_fallback_error(retryable_error_code);
        }
        true
    }
}

pub(crate) fn build_submit_route_order(
    default_route: &str,
    allowed_routes: &[String],
    configured_order: &[String],
) -> Vec<String> {
    let mut routes = Vec::new();
    let normalized_default = default_route.trim().to_ascii_lowercase();
    if !normalized_default.is_empty() {
        routes.push(normalized_default);
    }
    let mut allowed = Vec::new();
    for route in allowed_routes {
        let normalized = route.trim().to_ascii_lowercase();
        if normalized.is_empty() || allowed.iter().any(|value| value == &normalized) {
            continue;
        }
        allowed.push(normalized);
    }
    for route in configured_order {
        let normalized = route.trim().to_ascii_lowercase();
        if normalized.is_empty() || routes.iter().any(|value| value == &normalized) {
            continue;
        }
        if allowed.iter().any(|value| value == &normalized) {
            routes.push(normalized);
        }
    }
    for route in allowed {
        if !routes.iter().any(|value| value == &route) {
            routes.push(route);
        }
    }
    if routes.is_empty() {
        vec!["paper".to_string()]
    } else {
        routes
    }
}

fn normalize_route(value: &str) -> Option<String> {
    let route = value.trim().to_ascii_lowercase();
    if route.is_empty() {
        None
    } else {
        Some(route)
    }
}

fn is_allowed_jito_rpc_fallback_error(code: &str) -> bool {
    let normalized = code.trim().to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "submit_adapter_unavailable"
            | "submit_adapter_http_unavailable"
            | "submit_adapter_invalid_json"
            | "upstream_unavailable"
            | "upstream_request_failed"
            | "upstream_http_unavailable"
            | "upstream_submit_signature_unseen"
            | "send_rpc_unavailable"
            | "send_rpc_request_failed"
            | "send_rpc_http_unavailable"
            | "send_rpc_error_payload_retryable"
    )
}

pub(crate) fn normalize_route_tip_lamports(
    route_tip_lamports: &BTreeMap<String, u64>,
) -> BTreeMap<String, u64> {
    route_tip_lamports
        .iter()
        .filter_map(|(route, tip)| normalize_route(route).map(|key| (key, *tip)))
        .collect()
}
