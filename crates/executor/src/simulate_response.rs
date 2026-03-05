use serde_json::{json, Value};
use crate::route_normalization::normalize_route;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SimulateResponseValidationError {
    FieldMustBeNonEmptyStringWhenPresent {
        field_name: String,
    },
    RouteMismatch {
        response_route: String,
        expected_route: String,
    },
    ContractVersionMismatch {
        response_contract_version: String,
        expected_contract_version: String,
    },
    RequestIdMismatch {
        response_request_id: String,
        expected_request_id: String,
    },
    SignalIdMismatch {
        response_signal_id: String,
        expected_signal_id: String,
    },
    SideMismatch {
        response_side: String,
        expected_side: String,
    },
    TokenMismatch {
        response_token: String,
        expected_token: String,
    },
}

pub(crate) fn validate_simulate_response_route_and_contract(
    backend_response: &Value,
    expected_route: &str,
    expected_contract_version: &str,
) -> Result<(), SimulateResponseValidationError> {
    if let Some(response_route_raw) =
        parse_optional_non_empty_string_field(backend_response, "route")?
    {
        let response_route = normalize_route(response_route_raw.as_str());
        if response_route != expected_route {
            return Err(SimulateResponseValidationError::RouteMismatch {
                response_route,
                expected_route: expected_route.to_string(),
            });
        }
    }

    if let Some(response_contract_version) =
        parse_optional_non_empty_string_field(backend_response, "contract_version")?
    {
        if response_contract_version.as_str() != expected_contract_version {
            return Err(SimulateResponseValidationError::ContractVersionMismatch {
                response_contract_version,
                expected_contract_version: expected_contract_version.to_string(),
            });
        }
    }

    Ok(())
}

pub(crate) fn validate_simulate_response_identity(
    backend_response: &Value,
    expected_request_id: &str,
    expected_signal_id: &str,
    expected_side: &str,
    expected_token: &str,
) -> Result<(), SimulateResponseValidationError> {
    let normalized_expected_request_id = expected_request_id.trim();
    let normalized_expected_signal_id = expected_signal_id.trim();
    let normalized_expected_side = expected_side.trim().to_ascii_lowercase();
    let normalized_expected_token = expected_token.trim();

    if let Some(response_request_id) =
        parse_optional_non_empty_string_field(backend_response, "request_id")?
    {
        if response_request_id.as_str() != normalized_expected_request_id {
            return Err(SimulateResponseValidationError::RequestIdMismatch {
                response_request_id,
                expected_request_id: normalized_expected_request_id.to_string(),
            });
        }
    }

    if let Some(response_signal_id) =
        parse_optional_non_empty_string_field(backend_response, "signal_id")?
    {
        if response_signal_id.as_str() != normalized_expected_signal_id {
            return Err(SimulateResponseValidationError::SignalIdMismatch {
                response_signal_id,
                expected_signal_id: normalized_expected_signal_id.to_string(),
            });
        }
    }

    if let Some(response_side) = parse_optional_non_empty_string_field(backend_response, "side")? {
        let normalized_response_side = response_side.to_ascii_lowercase();
        if normalized_response_side != normalized_expected_side {
            return Err(SimulateResponseValidationError::SideMismatch {
                response_side,
                expected_side: normalized_expected_side,
            });
        }
    }

    if let Some(response_token) = parse_optional_non_empty_string_field(backend_response, "token")?
    {
        if response_token.as_str() != normalized_expected_token {
            return Err(SimulateResponseValidationError::TokenMismatch {
                response_token,
                expected_token: normalized_expected_token.to_string(),
            });
        }
    }

    Ok(())
}

fn parse_optional_non_empty_string_field(
    backend_response: &Value,
    field_name: &str,
) -> Result<Option<String>, SimulateResponseValidationError> {
    let Some(field_value) = backend_response.get(field_name) else {
        return Ok(None);
    };
    let Some(raw_value) = field_value.as_str() else {
        return Err(SimulateResponseValidationError::FieldMustBeNonEmptyStringWhenPresent {
            field_name: field_name.to_string(),
        });
    };
    let normalized = raw_value.trim();
    if normalized.is_empty() {
        return Err(SimulateResponseValidationError::FieldMustBeNonEmptyStringWhenPresent {
            field_name: field_name.to_string(),
        });
    }
    Ok(Some(normalized.to_string()))
}

pub(crate) fn resolve_simulate_response_detail(
    backend_response: &Value,
    default_detail: &str,
) -> Result<String, SimulateResponseValidationError> {
    Ok(
        parse_optional_non_empty_string_field(backend_response, "detail")?
            .unwrap_or_else(|| default_detail.to_string()),
    )
}

pub(crate) fn build_simulate_success_payload(
    route: &str,
    contract_version: &str,
    request_id: &str,
    signal_id: &str,
    side: &str,
    token: &str,
    detail: &str,
) -> Value {
    let normalized_request_id = request_id.trim();
    let normalized_signal_id = signal_id.trim();
    let normalized_side = side.trim().to_ascii_lowercase();
    let normalized_token = token.trim();
    json!({
        "status": "ok",
        "ok": true,
        "accepted": true,
        "route": route,
        "contract_version": contract_version,
        "request_id": normalized_request_id,
        "signal_id": normalized_signal_id,
        "side": normalized_side,
        "token": normalized_token,
        "detail": detail
    })
}

#[cfg(test)]
mod tests {
    use super::{
        build_simulate_success_payload, resolve_simulate_response_detail,
        validate_simulate_response_identity, validate_simulate_response_route_and_contract,
        SimulateResponseValidationError,
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
    fn simulate_response_validation_rejects_non_string_route() {
        let backend = json!({
            "route": 123,
            "contract_version": "v1",
        });
        let error = validate_simulate_response_route_and_contract(&backend, "rpc", "v1")
            .expect_err("non-string route must reject");
        assert!(matches!(
            error,
            SimulateResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name }
            if field_name == "route"
        ));
    }

    #[test]
    fn simulate_response_validation_rejects_null_route() {
        let backend = json!({
            "route": null,
            "contract_version": "v1",
        });
        let error = validate_simulate_response_route_and_contract(&backend, "rpc", "v1")
            .expect_err("null route must reject");
        assert!(matches!(
            error,
            SimulateResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name }
            if field_name == "route"
        ));
    }

    #[test]
    fn simulate_response_validation_rejects_non_string_contract_version() {
        let backend = json!({
            "route": "rpc",
            "contract_version": 123,
        });
        let error = validate_simulate_response_route_and_contract(&backend, "rpc", "v1")
            .expect_err("non-string contract_version must reject");
        assert!(matches!(
            error,
            SimulateResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name }
            if field_name == "contract_version"
        ));
    }

    #[test]
    fn simulate_response_validation_rejects_empty_contract_version() {
        let backend = json!({
            "route": "rpc",
            "contract_version": " ",
        });
        let error = validate_simulate_response_route_and_contract(&backend, "rpc", "v1")
            .expect_err("empty contract_version must reject");
        assert!(matches!(
            error,
            SimulateResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name }
            if field_name == "contract_version"
        ));
    }

    #[test]
    fn simulate_response_validation_identity_rejects_signal_id_mismatch() {
        let backend = json!({
            "signal_id": "signal-2",
        });
        let error = validate_simulate_response_identity(
            &backend,
            "request-1",
            "signal-1",
            "buy",
            "11111111111111111111111111111111",
        )
        .expect_err("signal_id mismatch must reject");
        assert!(matches!(
            error,
            SimulateResponseValidationError::SignalIdMismatch { .. }
        ));
    }

    #[test]
    fn simulate_response_validation_identity_accepts_side_case_insensitive() {
        let backend = json!({
            "side": "BUY",
        });
        validate_simulate_response_identity(
            &backend,
            "request-1",
            "signal-1",
            "buy",
            "11111111111111111111111111111111",
        )
        .expect("side case mismatch should be normalized");
    }

    #[test]
    fn simulate_response_validation_identity_rejects_token_mismatch() {
        let backend = json!({
            "token": "22222222222222222222222222222222",
        });
        let error = validate_simulate_response_identity(
            &backend,
            "request-1",
            "signal-1",
            "buy",
            "11111111111111111111111111111111",
        )
        .expect_err("token mismatch must reject");
        assert!(matches!(
            error,
            SimulateResponseValidationError::TokenMismatch { .. }
        ));
    }

    #[test]
    fn simulate_response_detail_defaults_when_missing() {
        let backend = json!({});
        let detail = resolve_simulate_response_detail(&backend, "adapter_simulation_ok")
            .expect("missing detail should resolve to default");
        assert_eq!(detail, "adapter_simulation_ok");
    }

    #[test]
    fn simulate_response_detail_rejects_non_string_when_present() {
        let backend = json!({ "detail": 123 });
        let error = resolve_simulate_response_detail(&backend, "adapter_simulation_ok")
            .expect_err("non-string detail should reject");
        assert!(matches!(
            error,
            SimulateResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name }
            if field_name == "detail"
        ));
    }

    #[test]
    fn simulate_response_detail_rejects_null_when_present() {
        let backend = json!({ "detail": null });
        let error = resolve_simulate_response_detail(&backend, "adapter_simulation_ok")
            .expect_err("null detail should reject");
        assert!(matches!(
            error,
            SimulateResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name }
            if field_name == "detail"
        ));
    }

    #[test]
    fn simulate_response_detail_rejects_empty_when_present() {
        let backend = json!({ "detail": "   " });
        let error = resolve_simulate_response_detail(&backend, "adapter_simulation_ok")
            .expect_err("empty detail should reject");
        assert!(matches!(
            error,
            SimulateResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name }
            if field_name == "detail"
        ));
    }

    #[test]
    fn simulate_response_payload_contains_expected_fields() {
        let payload = build_simulate_success_payload(
            "rpc",
            "v1",
            "request-1",
            "signal-1",
            "buy",
            "11111111111111111111111111111111",
            "ok-detail",
        );
        assert_eq!(payload.get("route").and_then(Value::as_str), Some("rpc"));
        assert_eq!(
            payload.get("request_id").and_then(Value::as_str),
            Some("request-1")
        );
        assert_eq!(
            payload.get("signal_id").and_then(Value::as_str),
            Some("signal-1")
        );
        assert_eq!(payload.get("side").and_then(Value::as_str), Some("buy"));
        assert_eq!(
            payload.get("token").and_then(Value::as_str),
            Some("11111111111111111111111111111111")
        );
        assert_eq!(
            payload.get("detail").and_then(Value::as_str),
            Some("ok-detail")
        );
    }

    #[test]
    fn simulate_response_payload_canonicalizes_identity_fields() {
        let payload = build_simulate_success_payload(
            "rpc",
            "v1",
            " request-1 ",
            "\tsignal-1\t",
            " BUY ",
            " 11111111111111111111111111111111 ",
            "ok-detail",
        );
        assert_eq!(
            payload.get("request_id").and_then(Value::as_str),
            Some("request-1")
        );
        assert_eq!(
            payload.get("signal_id").and_then(Value::as_str),
            Some("signal-1")
        );
        assert_eq!(payload.get("side").and_then(Value::as_str), Some("buy"));
        assert_eq!(
            payload.get("token").and_then(Value::as_str),
            Some("11111111111111111111111111111111")
        );
    }
}
