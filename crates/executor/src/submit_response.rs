use chrono::{DateTime, Utc};
use serde_json::Value;
use crate::route_normalization::normalize_route;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SubmitResponseValidationError {
    RouteMismatch {
        response_route: String,
        expected_route: String,
    },
    ContractVersionMismatch {
        response_contract_version: String,
        expected_contract_version: String,
    },
    ClientOrderIdMismatch {
        response_client_order_id: String,
        expected_client_order_id: String,
    },
    RequestIdMismatch {
        response_request_id: String,
        expected_request_id: String,
    },
    SubmittedAtMustBeNonEmptyRfc3339,
    SubmittedAtInvalidRfc3339 {
        raw: String,
    },
}

pub(crate) fn validate_submit_response_route_and_contract(
    backend_response: &Value,
    expected_route: &str,
    expected_contract_version: &str,
) -> Result<(), SubmitResponseValidationError> {
    if let Some(response_route) = backend_response
        .get("route")
        .and_then(Value::as_str)
        .map(normalize_route)
    {
        if response_route != expected_route {
            return Err(SubmitResponseValidationError::RouteMismatch {
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
            return Err(SubmitResponseValidationError::ContractVersionMismatch {
                response_contract_version: response_contract_version.to_string(),
                expected_contract_version: expected_contract_version.to_string(),
            });
        }
    }

    Ok(())
}

pub(crate) fn validate_submit_response_request_identity(
    backend_response: &Value,
    expected_client_order_id: &str,
    expected_request_id: &str,
) -> Result<(), SubmitResponseValidationError> {
    if let Some(response_client_order_id) = backend_response
        .get("client_order_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if response_client_order_id != expected_client_order_id {
            return Err(SubmitResponseValidationError::ClientOrderIdMismatch {
                response_client_order_id: response_client_order_id.to_string(),
                expected_client_order_id: expected_client_order_id.to_string(),
            });
        }
    }

    if let Some(response_request_id) = backend_response
        .get("request_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if response_request_id != expected_request_id {
            return Err(SubmitResponseValidationError::RequestIdMismatch {
                response_request_id: response_request_id.to_string(),
                expected_request_id: expected_request_id.to_string(),
            });
        }
    }

    Ok(())
}

pub(crate) fn resolve_submit_response_submitted_at(
    backend_response: &Value,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>, SubmitResponseValidationError> {
    let Some(value) = backend_response.get("submitted_at") else {
        return Ok(now);
    };
    let raw = value
        .as_str()
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty())
        .ok_or(SubmitResponseValidationError::SubmittedAtMustBeNonEmptyRfc3339)?;
    parse_rfc3339_utc(raw).ok_or_else(|| SubmitResponseValidationError::SubmittedAtInvalidRfc3339 {
        raw: raw.to_string(),
    })
}

fn parse_rfc3339_utc(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value.trim())
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::{
        resolve_submit_response_submitted_at, validate_submit_response_request_identity,
        validate_submit_response_route_and_contract, SubmitResponseValidationError,
    };
    use chrono::TimeZone;
    use serde_json::json;

    #[test]
    fn submit_response_validate_route_and_contract_rejects_route_mismatch() {
        let backend = json!({
            "route": "jito",
            "contract_version": "v1"
        });
        let error = validate_submit_response_route_and_contract(&backend, "rpc", "v1")
            .expect_err("route mismatch must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::RouteMismatch { .. }
        ));
    }

    #[test]
    fn submit_response_validate_route_and_contract_rejects_contract_version_mismatch() {
        let backend = json!({
            "route": "rpc",
            "contract_version": "v2"
        });
        let error = validate_submit_response_route_and_contract(&backend, "rpc", "v1")
            .expect_err("contract mismatch must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::ContractVersionMismatch { .. }
        ));
    }

    #[test]
    fn submit_response_validate_request_identity_rejects_request_id_mismatch() {
        let backend = json!({
            "client_order_id": "client-1",
            "request_id": "request-2"
        });
        let error = validate_submit_response_request_identity(&backend, "client-1", "request-1")
            .expect_err("request mismatch must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::RequestIdMismatch { .. }
        ));
    }

    #[test]
    fn submit_response_resolve_submitted_at_defaults_to_now_when_missing() {
        let backend = json!({});
        let now = chrono::Utc
            .with_ymd_and_hms(2026, 2, 24, 0, 0, 0)
            .single()
            .expect("valid fixed time");
        let resolved =
            resolve_submit_response_submitted_at(&backend, now).expect("must default to now");
        assert_eq!(resolved, now);
    }

    #[test]
    fn submit_response_resolve_submitted_at_rejects_non_string() {
        let backend = json!({
            "submitted_at": 12345
        });
        let now = chrono::Utc
            .with_ymd_and_hms(2026, 2, 24, 0, 0, 0)
            .single()
            .expect("valid fixed time");
        let error = resolve_submit_response_submitted_at(&backend, now)
            .expect_err("non-string submitted_at must reject");
        assert_eq!(
            error,
            SubmitResponseValidationError::SubmittedAtMustBeNonEmptyRfc3339
        );
    }

    #[test]
    fn submit_response_resolve_submitted_at_rejects_invalid_rfc3339() {
        let backend = json!({
            "submitted_at": "not-a-timestamp"
        });
        let now = chrono::Utc
            .with_ymd_and_hms(2026, 2, 24, 0, 0, 0)
            .single()
            .expect("valid fixed time");
        let error = resolve_submit_response_submitted_at(&backend, now)
            .expect_err("invalid submitted_at must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::SubmittedAtInvalidRfc3339 { .. }
        ));
    }
}
