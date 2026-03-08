use std::collections::HashMap;

use crate::{
    route_backend::{RouteBackend, UpstreamAction},
    submit_verify_config::SubmitSignatureVerifyConfig,
};

pub(crate) const DEFAULT_SUBMIT_TOTAL_BUDGET_MS: u64 = 7_000;
const CLAIM_TTL_SAFETY_PADDING_MS: u64 = 1_000;

pub(crate) fn min_claim_ttl_sec_for_submit_path(
    request_timeout_ms: u64,
    submit_total_budget_ms: u64,
    route_backends: &HashMap<String, RouteBackend>,
    submit_signature_verify: Option<&SubmitSignatureVerifyConfig>,
) -> u64 {
    let effective_request_timeout_ms = request_timeout_ms.max(500);
    let submit_hops = route_backends
        .values()
        .map(|backend| backend.endpoint_chain(UpstreamAction::Submit).len() as u64)
        .max()
        .unwrap_or(1)
        .max(1);
    let send_rpc_hops = route_backends
        .values()
        .map(|backend| backend.send_rpc_endpoint_chain().len() as u64)
        .max()
        .unwrap_or(0);
    let verify_hops = submit_signature_verify
        .map(|config| {
            config
                .attempts
                .saturating_mul(config.endpoints.len() as u64)
                .max(1)
        })
        .unwrap_or(0);
    let verify_wait_ms = submit_signature_verify
        .map(|config| {
            config
                .interval_ms
                .saturating_mul(config.attempts.saturating_sub(1))
        })
        .unwrap_or(0);
    let total_hops = submit_hops
        .saturating_add(send_rpc_hops)
        .saturating_add(verify_hops)
        .max(1);
    let hop_budget_ms = effective_request_timeout_ms
        .saturating_mul(total_hops)
        .saturating_add(verify_wait_ms)
        .saturating_add(CLAIM_TTL_SAFETY_PADDING_MS);
    let submit_budget_floor_ms = submit_total_budget_ms
        .max(effective_request_timeout_ms)
        .saturating_add(CLAIM_TTL_SAFETY_PADDING_MS);
    let budget_ms = hop_budget_ms.max(submit_budget_floor_ms);
    (budget_ms.saturating_add(999) / 1000).max(1)
}

pub(crate) fn default_submit_total_budget_ms(request_timeout_ms: u64) -> u64 {
    request_timeout_ms
        .max(500)
        .saturating_mul(3)
        .saturating_add(1_000)
        .max(DEFAULT_SUBMIT_TOTAL_BUDGET_MS)
}

#[cfg(test)]
mod tests {
    use super::min_claim_ttl_sec_for_submit_path;
    use crate::{route_backend::RouteBackend, submit_verify_config::SubmitSignatureVerifyConfig};
    use std::collections::HashMap;

    #[test]
    fn min_claim_ttl_sec_for_submit_path_accounts_for_verify_and_fallback_hops() {
        let mut route_backends = HashMap::new();
        route_backends.insert(
            "rpc".to_string(),
            RouteBackend {
                submit_url: "https://submit.primary".to_string(),
                submit_fallback_url: Some("https://submit.fallback".to_string()),
                simulate_url: "https://simulate.primary".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: Some("https://send-rpc.primary".to_string()),
                send_rpc_fallback_url: Some("https://send-rpc.fallback".to_string()),
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let verify = SubmitSignatureVerifyConfig {
            endpoints: vec![
                "https://verify.primary".to_string(),
                "https://verify.fallback".to_string(),
            ],
            attempts: 3,
            interval_ms: 250,
            strict: false,
        };
        let ttl = min_claim_ttl_sec_for_submit_path(2_000, 7_000, &route_backends, Some(&verify));
        assert_eq!(ttl, 22);
    }

    #[test]
    fn min_claim_ttl_sec_for_submit_path_applies_500ms_runtime_floor() {
        let mut route_backends = HashMap::new();
        route_backends.insert(
            "rpc".to_string(),
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
        );
        let ttl = min_claim_ttl_sec_for_submit_path(100, 1_000, &route_backends, None);
        assert_eq!(ttl, 2);
    }

    #[test]
    fn min_claim_ttl_sec_for_submit_path_respects_submit_total_budget_floor() {
        let mut route_backends = HashMap::new();
        route_backends.insert(
            "rpc".to_string(),
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
        );
        let ttl = min_claim_ttl_sec_for_submit_path(1_000, 60_000, &route_backends, None);
        assert_eq!(ttl, 61);
    }
}
