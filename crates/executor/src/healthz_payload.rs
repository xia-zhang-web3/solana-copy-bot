use std::collections::{HashMap, HashSet};

use crate::route_allowlist::sorted_routes;
use crate::route_backend::RouteBackend;
use serde_json::{json, Value};

#[derive(Clone, Debug)]
pub(crate) struct HealthzPayloadInputs<'a> {
    pub(crate) contract_version: &'a str,
    pub(crate) backend_mode: &'a str,
    pub(crate) enabled_routes: &'a HashSet<String>,
    pub(crate) route_backends: &'a HashMap<String, RouteBackend>,
    pub(crate) signer_source: &'a str,
    pub(crate) signer_kms_key_id_configured: bool,
    pub(crate) signer_keypair_file_configured: bool,
    pub(crate) submit_fastlane_enabled: bool,
    pub(crate) idempotency_store_status: &'a str,
    pub(crate) signer_pubkey: &'a str,
}

pub(crate) fn top_level_healthz_status(idempotency_store_status: &str) -> &'static str {
    if idempotency_store_status == "ok" {
        "ok"
    } else {
        "degraded"
    }
}

fn sorted_send_rpc_routes(
    route_backends: &HashMap<String, RouteBackend>,
    has_route: impl Fn(&RouteBackend) -> bool,
) -> Vec<String> {
    let routes: HashSet<String> = route_backends
        .iter()
        .filter_map(|(route, backend)| has_route(backend).then_some(route.clone()))
        .collect();
    sorted_routes(&routes)
}

pub(crate) fn build_healthz_payload(inputs: HealthzPayloadInputs<'_>) -> Value {
    let enabled_routes_sorted = sorted_routes(inputs.enabled_routes);
    let send_rpc_enabled_routes = sorted_send_rpc_routes(inputs.route_backends, |backend| {
        backend.send_rpc_url.is_some()
    });
    let send_rpc_fallback_routes = sorted_send_rpc_routes(inputs.route_backends, |backend| {
        backend.send_rpc_fallback_url.is_some()
    });
    json!({
        "status": top_level_healthz_status(inputs.idempotency_store_status),
        "contract_version": inputs.contract_version,
        "backend_mode": inputs.backend_mode,
        "enabled_routes": enabled_routes_sorted,
        "signer_source": inputs.signer_source,
        "signer_kms_key_id_configured": inputs.signer_kms_key_id_configured,
        "signer_keypair_file_configured": inputs.signer_keypair_file_configured,
        "submit_fastlane_enabled": inputs.submit_fastlane_enabled,
        "idempotency_store_status": inputs.idempotency_store_status,
        "signer_pubkey": inputs.signer_pubkey,
        "send_rpc_enabled_routes": send_rpc_enabled_routes,
        "send_rpc_fallback_routes": send_rpc_fallback_routes,
        // Backward-compat alias for existing preflight/reporting consumers.
        "send_rpc_routes": sorted_send_rpc_routes(
            inputs.route_backends,
            |backend| backend.send_rpc_url.is_some(),
        ),
        // Backward-compat alias for existing preflight/reporting consumers.
        "routes": sorted_routes(inputs.enabled_routes),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use serde_json::Value;

    use super::{build_healthz_payload, top_level_healthz_status, HealthzPayloadInputs};
    use crate::route_backend::RouteBackend;

    fn routes(values: &[&str]) -> HashSet<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn backend(send_rpc_url: Option<&str>, send_rpc_fallback_url: Option<&str>) -> RouteBackend {
        RouteBackend {
            submit_url: "https://submit.example.com".to_string(),
            submit_fallback_url: None,
            simulate_url: "https://simulate.example.com".to_string(),
            simulate_fallback_url: None,
            primary_auth_token: None,
            fallback_auth_token: None,
            send_rpc_url: send_rpc_url.map(ToString::to_string),
            send_rpc_fallback_url: send_rpc_fallback_url.map(ToString::to_string),
            send_rpc_primary_auth_token: None,
            send_rpc_fallback_auth_token: None,
        }
    }

    fn route_backends_fixture() -> HashMap<String, RouteBackend> {
        HashMap::from([
            (
                "rpc".to_string(),
                backend(Some("https://send-rpc.example.com"), None),
            ),
            (
                "jito".to_string(),
                backend(
                    Some("https://send-rpc-jito.example.com"),
                    Some("https://send-rpc-jito-fallback.example.com"),
                ),
            ),
            ("paper".to_string(), backend(None, None)),
        ])
    }

    #[test]
    fn healthz_payload_top_level_status_is_ok_when_store_ok() {
        assert_eq!(top_level_healthz_status("ok"), "ok");
    }

    #[test]
    fn healthz_payload_top_level_status_is_degraded_when_store_not_ok() {
        assert_eq!(top_level_healthz_status("degraded"), "degraded");
        assert_eq!(top_level_healthz_status("unknown"), "degraded");
    }

    #[test]
    fn healthz_payload_build_includes_expected_fields() {
        let route_allowlist = routes(&["rpc", "jito"]);
        let route_backends = route_backends_fixture();
        let payload = build_healthz_payload(HealthzPayloadInputs {
            contract_version: "v1",
            backend_mode: "upstream",
            enabled_routes: &route_allowlist,
            route_backends: &route_backends,
            signer_source: "file",
            signer_kms_key_id_configured: false,
            signer_keypair_file_configured: true,
            submit_fastlane_enabled: false,
            idempotency_store_status: "ok",
            signer_pubkey: "11111111111111111111111111111111",
        });
        assert_eq!(payload.get("status").and_then(Value::as_str), Some("ok"));
        assert_eq!(
            payload.get("contract_version").and_then(Value::as_str),
            Some("v1")
        );
        assert_eq!(
            payload.get("backend_mode").and_then(Value::as_str),
            Some("upstream")
        );
        assert_eq!(
            payload
                .get("idempotency_store_status")
                .and_then(Value::as_str),
            Some("ok")
        );
    }

    #[test]
    fn healthz_payload_routes_alias_matches_enabled_routes() {
        let route_allowlist = routes(&["rpc", "jito"]);
        let route_backends = route_backends_fixture();
        let payload = build_healthz_payload(HealthzPayloadInputs {
            contract_version: "v1",
            backend_mode: "upstream",
            enabled_routes: &route_allowlist,
            route_backends: &route_backends,
            signer_source: "file",
            signer_kms_key_id_configured: false,
            signer_keypair_file_configured: true,
            submit_fastlane_enabled: false,
            idempotency_store_status: "ok",
            signer_pubkey: "11111111111111111111111111111111",
        });
        let enabled_routes: std::collections::HashSet<String> = payload
            .get("enabled_routes")
            .and_then(Value::as_array)
            .expect("enabled_routes must be array")
            .iter()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect();
        let routes_alias: std::collections::HashSet<String> = payload
            .get("routes")
            .and_then(Value::as_array)
            .expect("routes alias must be array")
            .iter()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect();
        assert_eq!(enabled_routes, routes_alias);
    }

    #[test]
    fn healthz_payload_routes_are_sorted_deterministically() {
        let route_allowlist = routes(&["rpc", "jito", "paper"]);
        let route_backends = route_backends_fixture();
        let payload = build_healthz_payload(HealthzPayloadInputs {
            contract_version: "v1",
            backend_mode: "upstream",
            enabled_routes: &route_allowlist,
            route_backends: &route_backends,
            signer_source: "file",
            signer_kms_key_id_configured: false,
            signer_keypair_file_configured: true,
            submit_fastlane_enabled: false,
            idempotency_store_status: "ok",
            signer_pubkey: "11111111111111111111111111111111",
        });

        let enabled_routes: Vec<&str> = payload
            .get("enabled_routes")
            .and_then(Value::as_array)
            .expect("enabled_routes must be array")
            .iter()
            .filter_map(Value::as_str)
            .collect();
        let routes_alias: Vec<&str> = payload
            .get("routes")
            .and_then(Value::as_array)
            .expect("routes alias must be array")
            .iter()
            .filter_map(Value::as_str)
            .collect();

        assert_eq!(enabled_routes, vec!["jito", "paper", "rpc"]);
        assert_eq!(routes_alias, vec!["jito", "paper", "rpc"]);
    }

    #[test]
    fn healthz_payload_includes_send_rpc_route_topology() {
        let route_allowlist = routes(&["rpc", "jito", "paper"]);
        let route_backends = route_backends_fixture();
        let payload = build_healthz_payload(HealthzPayloadInputs {
            contract_version: "v1",
            backend_mode: "upstream",
            enabled_routes: &route_allowlist,
            route_backends: &route_backends,
            signer_source: "file",
            signer_kms_key_id_configured: false,
            signer_keypair_file_configured: true,
            submit_fastlane_enabled: false,
            idempotency_store_status: "ok",
            signer_pubkey: "11111111111111111111111111111111",
        });

        let send_rpc_enabled_routes: Vec<&str> = payload
            .get("send_rpc_enabled_routes")
            .and_then(Value::as_array)
            .expect("send_rpc_enabled_routes must be array")
            .iter()
            .filter_map(Value::as_str)
            .collect();
        let send_rpc_fallback_routes: Vec<&str> = payload
            .get("send_rpc_fallback_routes")
            .and_then(Value::as_array)
            .expect("send_rpc_fallback_routes must be array")
            .iter()
            .filter_map(Value::as_str)
            .collect();
        let send_rpc_alias: Vec<&str> = payload
            .get("send_rpc_routes")
            .and_then(Value::as_array)
            .expect("send_rpc_routes alias must be array")
            .iter()
            .filter_map(Value::as_str)
            .collect();

        assert_eq!(send_rpc_enabled_routes, vec!["jito", "rpc"]);
        assert_eq!(send_rpc_fallback_routes, vec!["jito"]);
        assert_eq!(send_rpc_alias, vec!["jito", "rpc"]);
    }
}
