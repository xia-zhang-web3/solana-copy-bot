use serde_json::{json, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SimulateResponseValidationError {
    RouteMismatch {
        response_route: String,
        expected_route: String,
    },
    ContractVersionMismatch {
        response_contract_version: String,
        expected_contract_version: String,
    },
}

pub(crate) fn validate_simulate_response_route_and_contract(
    backend_response: &Value,
    expected_route: &str,
    expected_contract_version: &str,
) -> Result<(), SimulateResponseValidationError> {
    if let Some(response_route) = backend_response
        .get("route")
        .and_then(Value::as_str)
        .map(normalize_route)
    {
        if response_route != expected_route {
            return Err(SimulateResponseValidationError::RouteMismatch {
                response_route,
                expected_route: expected_route.to_string(),
            });
        }
    }

    if let Some(response_contract_version) = backend_response
        .get("contract_version")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if response_contract_version != expected_contract_version {
            return Err(SimulateResponseValidationError::ContractVersionMismatch {
                response_contract_version: response_contract_version.to_string(),
                expected_contract_version: expected_contract_version.to_string(),
            });
        }
    }

    Ok(())
}

pub(crate) fn resolve_simulate_response_detail(
    backend_response: &Value,
    default_detail: &str,
) -> String {
    backend_response
        .get("detail")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default_detail)
        .to_string()
}

pub(crate) fn build_simulate_success_payload(
    route: &str,
    contract_version: &str,
    request_id: &str,
    detail: &str,
) -> Value {
    json!({
        "status": "ok",
        "ok": true,
        "accepted": true,
        "route": route,
        "contract_version": contract_version,
        "request_id": request_id,
        "detail": detail
    })
}

fn normalize_route(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::{
        build_simulate_success_payload, resolve_simulate_response_detail,
        validate_simulate_response_route_and_contract, SimulateResponseValidationError,
    };
    use serde_json::{json, Value};

    #[test]
    fn simulate_response_validation_rejects_route_mismatch() {
        let backend = json!({
            "route": "jito",
            "contract_version": "v1",
        });
        let error = validate_simulate_response_route_and_contract(&backend, "rpc", "v1")
            .expect_err("route mismatch must reject");
        assert!(matches!(
            error,
            SimulateResponseValidationError::RouteMismatch { .. }
        ));
    }

    #[test]
    fn simulate_response_detail_defaults_when_missing() {
        let backend = json!({});
        let detail = resolve_simulate_response_detail(&backend, "adapter_simulation_ok");
        assert_eq!(detail, "adapter_simulation_ok");
    }

    #[test]
    fn simulate_response_payload_contains_expected_fields() {
        let payload = build_simulate_success_payload("rpc", "v1", "request-1", "ok-detail");
        assert_eq!(payload.get("route").and_then(Value::as_str), Some("rpc"));
        assert_eq!(
            payload.get("request_id").and_then(Value::as_str),
            Some("request-1")
        );
        assert_eq!(
            payload.get("detail").and_then(Value::as_str),
            Some("ok-detail")
        );
    }
}
