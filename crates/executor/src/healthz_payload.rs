use std::collections::HashSet;

use serde_json::{json, Value};
use crate::route_allowlist::sorted_routes;

#[derive(Clone, Debug)]
pub(crate) struct HealthzPayloadInputs<'a> {
    pub(crate) contract_version: &'a str,
    pub(crate) enabled_routes: &'a HashSet<String>,
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

pub(crate) fn build_healthz_payload(inputs: HealthzPayloadInputs<'_>) -> Value {
    let enabled_routes_sorted = sorted_routes(inputs.enabled_routes);
    json!({
        "status": top_level_healthz_status(inputs.idempotency_store_status),
        "contract_version": inputs.contract_version,
        "enabled_routes": enabled_routes_sorted,
        "signer_source": inputs.signer_source,
        "signer_kms_key_id_configured": inputs.signer_kms_key_id_configured,
        "signer_keypair_file_configured": inputs.signer_keypair_file_configured,
        "submit_fastlane_enabled": inputs.submit_fastlane_enabled,
        "idempotency_store_status": inputs.idempotency_store_status,
        "signer_pubkey": inputs.signer_pubkey,
        // Backward-compat alias for existing preflight/reporting consumers.
        "routes": sorted_routes(inputs.enabled_routes),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::Value;

    use super::{build_healthz_payload, top_level_healthz_status, HealthzPayloadInputs};

    fn routes(values: &[&str]) -> HashSet<String> {
        values.iter().map(|value| value.to_string()).collect()
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
        let payload = build_healthz_payload(HealthzPayloadInputs {
            contract_version: "v1",
            enabled_routes: &route_allowlist,
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
            payload.get("idempotency_store_status").and_then(Value::as_str),
            Some("ok")
        );
    }

    #[test]
    fn healthz_payload_routes_alias_matches_enabled_routes() {
        let route_allowlist = routes(&["rpc", "jito"]);
        let payload = build_healthz_payload(HealthzPayloadInputs {
            contract_version: "v1",
            enabled_routes: &route_allowlist,
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
        let payload = build_healthz_payload(HealthzPayloadInputs {
            contract_version: "v1",
            enabled_routes: &route_allowlist,
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
}
