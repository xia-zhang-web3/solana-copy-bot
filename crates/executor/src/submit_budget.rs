use std::collections::HashMap;

use crate::{
    route_backend::{RouteBackend, UpstreamAction},
    submit_verify_config::SubmitSignatureVerifyConfig,
};

pub(crate) const DEFAULT_SUBMIT_TOTAL_BUDGET_MS: u64 = 7_000;
const CLAIM_TTL_SAFETY_PADDING_MS: u64 = 1_000;

pub(crate) fn min_claim_ttl_sec_for_submit_path(
    request_timeout_ms: u64,
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
    let budget_ms = effective_request_timeout_ms
        .saturating_mul(total_hops)
        .saturating_add(verify_wait_ms)
        .saturating_add(CLAIM_TTL_SAFETY_PADDING_MS);
    (budget_ms.saturating_add(999) / 1000).max(1)
}

pub(crate) fn default_submit_total_budget_ms(request_timeout_ms: u64) -> u64 {
    request_timeout_ms
        .max(500)
        .saturating_mul(3)
        .saturating_add(1_000)
        .max(DEFAULT_SUBMIT_TOTAL_BUDGET_MS)
}
