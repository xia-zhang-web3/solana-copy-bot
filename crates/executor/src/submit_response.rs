use crate::rfc3339_time::parse_rfc3339_utc;
use crate::route_normalization::normalize_route;
use chrono::{DateTime, Utc};
use serde_json::Value;

const RESPONSE_FLOAT_MATCH_EPSILON: f64 = 1e-9;

pub(crate) struct SubmitResponsePolicyEchoInputs<'a> {
    pub(crate) expected_slippage_bps: f64,
    pub(crate) expected_cu_limit: u32,
    pub(crate) expected_cu_price_micro_lamports: u64,
    pub(crate) expected_effective_tip_lamports: u64,
    pub(crate) expected_requested_tip_lamports: u64,
    pub(crate) expected_tip_policy_code: Option<&'a str>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SubmitResponseValidationError {
    FieldMustBeNonEmptyStringWhenPresent {
        field_name: String,
    },
    FieldMustBeFiniteNumberWhenPresent {
        field_name: String,
    },
    FieldMustBeNonNegativeIntegerWhenPresent {
        field_name: String,
    },
    FieldMustBeObjectWhenPresent {
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
    ClientOrderIdMismatch {
        response_client_order_id: String,
        expected_client_order_id: String,
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
    SubmittedAtMustBeNonEmptyRfc3339,
    SubmittedAtInvalidRfc3339 {
        raw: String,
    },
    SlippageBpsMismatch {
        response_slippage_bps: f64,
        expected_slippage_bps: f64,
    },
    ComputeBudgetCuLimitMismatch {
        response_cu_limit: u64,
        expected_cu_limit: u64,
    },
    ComputeBudgetCuPriceMicroLamportsMismatch {
        response_cu_price_micro_lamports: u64,
        expected_cu_price_micro_lamports: u64,
    },
    TipLamportsMismatch {
        response_tip_lamports: u64,
        expected_tip_lamports: u64,
    },
    TipPolicyUnexpected {
        response_policy_code: String,
    },
    TipPolicyCodeMismatch {
        response_policy_code: String,
        expected_policy_code: String,
    },
    TipPolicyRequestedTipMismatch {
        response_requested_tip_lamports: u64,
        expected_requested_tip_lamports: u64,
    },
    TipPolicyEffectiveTipMismatch {
        response_effective_tip_lamports: u64,
        expected_effective_tip_lamports: u64,
    },
}

pub(crate) fn validate_submit_response_route_and_contract(
    backend_response: &Value,
    expected_route: &str,
    expected_contract_version: &str,
) -> Result<(), SubmitResponseValidationError> {
    if let Some(response_route_raw) =
        parse_optional_non_empty_string_field(backend_response, "route")?
    {
        let response_route = normalize_route(response_route_raw.as_str());
        if response_route != expected_route {
            return Err(SubmitResponseValidationError::RouteMismatch {
                response_route,
                expected_route: expected_route.to_string(),
            });
        }
    }

    if let Some(response_contract_version) =
        parse_optional_non_empty_string_field(backend_response, "contract_version")?
    {
        if response_contract_version.as_str() != expected_contract_version {
            return Err(SubmitResponseValidationError::ContractVersionMismatch {
                response_contract_version,
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
    let normalized_expected_client_order_id = expected_client_order_id.trim();
    let normalized_expected_request_id = expected_request_id.trim();

    if let Some(response_client_order_id) =
        parse_optional_non_empty_string_field(backend_response, "client_order_id")?
    {
        if response_client_order_id.as_str() != normalized_expected_client_order_id {
            return Err(SubmitResponseValidationError::ClientOrderIdMismatch {
                response_client_order_id,
                expected_client_order_id: normalized_expected_client_order_id.to_string(),
            });
        }
    }

    if let Some(response_request_id) =
        parse_optional_non_empty_string_field(backend_response, "request_id")?
    {
        if response_request_id.as_str() != normalized_expected_request_id {
            return Err(SubmitResponseValidationError::RequestIdMismatch {
                response_request_id,
                expected_request_id: normalized_expected_request_id.to_string(),
            });
        }
    }

    Ok(())
}

pub(crate) fn validate_submit_response_extended_identity(
    backend_response: &Value,
    expected_signal_id: &str,
    expected_side: &str,
    expected_token: &str,
) -> Result<(), SubmitResponseValidationError> {
    let normalized_expected_signal_id = expected_signal_id.trim();
    let normalized_expected_side = expected_side.trim().to_ascii_lowercase();
    let normalized_expected_token = expected_token.trim();

    if let Some(response_signal_id) =
        parse_optional_non_empty_string_field(backend_response, "signal_id")?
    {
        if response_signal_id.as_str() != normalized_expected_signal_id {
            return Err(SubmitResponseValidationError::SignalIdMismatch {
                response_signal_id,
                expected_signal_id: normalized_expected_signal_id.to_string(),
            });
        }
    }

    if let Some(response_side) = parse_optional_non_empty_string_field(backend_response, "side")? {
        let normalized_response_side = response_side.to_ascii_lowercase();
        if normalized_response_side != normalized_expected_side {
            return Err(SubmitResponseValidationError::SideMismatch {
                response_side,
                expected_side: normalized_expected_side,
            });
        }
    }

    if let Some(response_token) = parse_optional_non_empty_string_field(backend_response, "token")?
    {
        if response_token.as_str() != normalized_expected_token {
            return Err(SubmitResponseValidationError::TokenMismatch {
                response_token,
                expected_token: normalized_expected_token.to_string(),
            });
        }
    }

    Ok(())
}

pub(crate) fn validate_submit_response_policy_echoes(
    backend_response: &Value,
    inputs: SubmitResponsePolicyEchoInputs<'_>,
) -> Result<(), SubmitResponseValidationError> {
    if let Some(response_slippage_bps) =
        parse_optional_finite_f64_field(backend_response, "slippage_bps")?
    {
        if (response_slippage_bps - inputs.expected_slippage_bps).abs()
            > RESPONSE_FLOAT_MATCH_EPSILON
        {
            return Err(SubmitResponseValidationError::SlippageBpsMismatch {
                response_slippage_bps,
                expected_slippage_bps: inputs.expected_slippage_bps,
            });
        }
    }

    if let Some(response_tip_lamports) =
        parse_optional_non_negative_u64_field(backend_response, "tip_lamports")?
    {
        if response_tip_lamports != inputs.expected_effective_tip_lamports {
            return Err(SubmitResponseValidationError::TipLamportsMismatch {
                response_tip_lamports,
                expected_tip_lamports: inputs.expected_effective_tip_lamports,
            });
        }
    }

    if let Some(compute_budget) = backend_response.get("compute_budget") {
        let Some(compute_budget) = compute_budget.as_object() else {
            return Err(SubmitResponseValidationError::FieldMustBeObjectWhenPresent {
                field_name: "compute_budget".to_string(),
            });
        };
        let response_cu_limit = parse_required_non_negative_u64_field(
            compute_budget.get("cu_limit"),
            "compute_budget.cu_limit",
        )?;
        let expected_cu_limit = u64::from(inputs.expected_cu_limit);
        if response_cu_limit != expected_cu_limit {
            return Err(SubmitResponseValidationError::ComputeBudgetCuLimitMismatch {
                response_cu_limit,
                expected_cu_limit,
            });
        }

        let response_cu_price_micro_lamports = parse_required_non_negative_u64_field(
            compute_budget.get("cu_price_micro_lamports"),
            "compute_budget.cu_price_micro_lamports",
        )?;
        if response_cu_price_micro_lamports != inputs.expected_cu_price_micro_lamports {
            return Err(
                SubmitResponseValidationError::ComputeBudgetCuPriceMicroLamportsMismatch {
                    response_cu_price_micro_lamports,
                    expected_cu_price_micro_lamports: inputs.expected_cu_price_micro_lamports,
                },
            );
        }
    }

    if let Some(tip_policy) = backend_response.get("tip_policy") {
        let Some(tip_policy) = tip_policy.as_object() else {
            return Err(SubmitResponseValidationError::FieldMustBeObjectWhenPresent {
                field_name: "tip_policy".to_string(),
            });
        };
        let response_policy_code =
            parse_required_non_empty_string_field(tip_policy.get("policy_code"), "tip_policy.policy_code")?;
        let Some(expected_policy_code) = inputs.expected_tip_policy_code else {
            return Err(SubmitResponseValidationError::TipPolicyUnexpected {
                response_policy_code,
            });
        };
        if response_policy_code != expected_policy_code {
            return Err(SubmitResponseValidationError::TipPolicyCodeMismatch {
                response_policy_code,
                expected_policy_code: expected_policy_code.to_string(),
            });
        }
        let response_requested_tip_lamports = parse_required_non_negative_u64_field(
            tip_policy.get("requested_tip_lamports"),
            "tip_policy.requested_tip_lamports",
        )?;
        if response_requested_tip_lamports != inputs.expected_requested_tip_lamports {
            return Err(SubmitResponseValidationError::TipPolicyRequestedTipMismatch {
                response_requested_tip_lamports,
                expected_requested_tip_lamports: inputs.expected_requested_tip_lamports,
            });
        }
        let response_effective_tip_lamports = parse_required_non_negative_u64_field(
            tip_policy.get("effective_tip_lamports"),
            "tip_policy.effective_tip_lamports",
        )?;
        if response_effective_tip_lamports != inputs.expected_effective_tip_lamports {
            return Err(SubmitResponseValidationError::TipPolicyEffectiveTipMismatch {
                response_effective_tip_lamports,
                expected_effective_tip_lamports: inputs.expected_effective_tip_lamports,
            });
        }
    }

    Ok(())
}

fn parse_optional_non_empty_string_field(
    backend_response: &Value,
    field_name: &str,
) -> Result<Option<String>, SubmitResponseValidationError> {
    let Some(field_value) = backend_response.get(field_name) else {
        return Ok(None);
    };
    let Some(raw_value) = field_value.as_str() else {
        return Err(
            SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent {
                field_name: field_name.to_string(),
            },
        );
    };
    let normalized = raw_value.trim();
    if normalized.is_empty() {
        return Err(
            SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent {
                field_name: field_name.to_string(),
            },
        );
    }
    Ok(Some(normalized.to_string()))
}

fn parse_required_non_empty_string_field(
    field_value: Option<&Value>,
    field_name: &str,
) -> Result<String, SubmitResponseValidationError> {
    let Some(field_value) = field_value else {
        return Err(SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent {
            field_name: field_name.to_string(),
        });
    };
    let Some(raw_value) = field_value.as_str() else {
        return Err(SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent {
            field_name: field_name.to_string(),
        });
    };
    let normalized = raw_value.trim();
    if normalized.is_empty() {
        return Err(SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent {
            field_name: field_name.to_string(),
        });
    }
    Ok(normalized.to_string())
}

fn parse_optional_finite_f64_field(
    backend_response: &Value,
    field_name: &str,
) -> Result<Option<f64>, SubmitResponseValidationError> {
    let Some(field_value) = backend_response.get(field_name) else {
        return Ok(None);
    };
    let Some(value) = field_value.as_f64() else {
        return Err(SubmitResponseValidationError::FieldMustBeFiniteNumberWhenPresent {
            field_name: field_name.to_string(),
        });
    };
    if !value.is_finite() {
        return Err(SubmitResponseValidationError::FieldMustBeFiniteNumberWhenPresent {
            field_name: field_name.to_string(),
        });
    }
    Ok(Some(value))
}

fn parse_optional_non_negative_u64_field(
    backend_response: &Value,
    field_name: &str,
) -> Result<Option<u64>, SubmitResponseValidationError> {
    let Some(field_value) = backend_response.get(field_name) else {
        return Ok(None);
    };
    parse_required_non_negative_u64_field(Some(field_value), field_name).map(Some)
}

fn parse_required_non_negative_u64_field(
    field_value: Option<&Value>,
    field_name: &str,
) -> Result<u64, SubmitResponseValidationError> {
    let Some(field_value) = field_value else {
        return Err(
            SubmitResponseValidationError::FieldMustBeNonNegativeIntegerWhenPresent {
                field_name: field_name.to_string(),
            },
        );
    };
    let Some(value) = field_value.as_u64() else {
        return Err(
            SubmitResponseValidationError::FieldMustBeNonNegativeIntegerWhenPresent {
                field_name: field_name.to_string(),
            },
        );
    };
    Ok(value)
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
    parse_rfc3339_utc(raw).ok_or_else(
        || SubmitResponseValidationError::SubmittedAtInvalidRfc3339 {
            raw: raw.to_string(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::{
        resolve_submit_response_submitted_at, validate_submit_response_extended_identity,
        validate_submit_response_policy_echoes, validate_submit_response_request_identity,
        validate_submit_response_route_and_contract, SubmitResponsePolicyEchoInputs,
        SubmitResponseValidationError,
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
    fn submit_response_validate_request_identity_accepts_trimmed_expected_values() {
        let backend = json!({
            "client_order_id": "client-1",
            "request_id": "request-1"
        });
        validate_submit_response_request_identity(&backend, " client-1 ", " request-1 ")
            .expect("trimmed expected identity should be accepted");
    }

    #[test]
    fn submit_response_validate_request_identity_accepts_trimmed_response_values() {
        let backend = json!({
            "client_order_id": " client-1 ",
            "request_id": "\trequest-1\t"
        });
        validate_submit_response_request_identity(&backend, "client-1", "request-1")
            .expect("trimmed response identity should be accepted");
    }

    #[test]
    fn submit_response_policy_echoes_reject_slippage_bps_mismatch() {
        let backend = json!({
            "slippage_bps": 12.0
        });
        let error = validate_submit_response_policy_echoes(
            &backend,
            SubmitResponsePolicyEchoInputs {
                expected_slippage_bps: 10.0,
                expected_cu_limit: 300_000,
                expected_cu_price_micro_lamports: 1_000,
                expected_effective_tip_lamports: 0,
                expected_requested_tip_lamports: 0,
                expected_tip_policy_code: None,
            },
        )
        .expect_err("slippage_bps mismatch must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::SlippageBpsMismatch { .. }
        ));
    }

    #[test]
    fn submit_response_policy_echoes_accept_matching_tip_policy() {
        let backend = json!({
            "slippage_bps": 10.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            },
            "tip_policy": {
                "policy_code": "rpc_tip_forced_zero",
                "requested_tip_lamports": 2500,
                "effective_tip_lamports": 0
            }
        });
        validate_submit_response_policy_echoes(
            &backend,
            SubmitResponsePolicyEchoInputs {
                expected_slippage_bps: 10.0,
                expected_cu_limit: 300_000,
                expected_cu_price_micro_lamports: 1_000,
                expected_effective_tip_lamports: 0,
                expected_requested_tip_lamports: 2_500,
                expected_tip_policy_code: Some("rpc_tip_forced_zero"),
            },
        )
        .expect("matching policy echoes should pass");
    }

    #[test]
    fn submit_response_validate_extended_identity_rejects_signal_id_mismatch() {
        let backend = json!({
            "signal_id": "signal-2"
        });
        let error = validate_submit_response_extended_identity(
            &backend,
            "signal-1",
            "buy",
            "11111111111111111111111111111111",
        )
        .expect_err("signal_id mismatch must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::SignalIdMismatch { .. }
        ));
    }

    #[test]
    fn submit_response_validate_extended_identity_accepts_side_case_insensitive() {
        let backend = json!({
            "side": "BUY"
        });
        validate_submit_response_extended_identity(
            &backend,
            "signal-1",
            "buy",
            "11111111111111111111111111111111",
        )
        .expect("side case mismatch should be normalized");
    }

    #[test]
    fn submit_response_validate_extended_identity_rejects_token_mismatch() {
        let backend = json!({
            "token": "22222222222222222222222222222222"
        });
        let error = validate_submit_response_extended_identity(
            &backend,
            "signal-1",
            "buy",
            "11111111111111111111111111111111",
        )
        .expect_err("token mismatch must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::TokenMismatch { .. }
        ));
    }

    #[test]
    fn submit_response_validate_route_and_contract_rejects_non_string_contract_version() {
        let backend = json!({
            "route": "rpc",
            "contract_version": 123
        });
        let error = validate_submit_response_route_and_contract(&backend, "rpc", "v1")
            .expect_err("non-string contract_version must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { .. }
        ));
    }

    #[test]
    fn submit_response_validate_route_and_contract_rejects_null_route() {
        let backend = json!({
            "route": null,
            "contract_version": "v1"
        });
        let error = validate_submit_response_route_and_contract(&backend, "rpc", "v1")
            .expect_err("null route must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name }
            if field_name == "route"
        ));
    }

    #[test]
    fn submit_response_validate_route_and_contract_rejects_null_contract_version() {
        let backend = json!({
            "route": "rpc",
            "contract_version": null
        });
        let error = validate_submit_response_route_and_contract(&backend, "rpc", "v1")
            .expect_err("null contract_version must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name }
            if field_name == "contract_version"
        ));
    }

    #[test]
    fn submit_response_validate_request_identity_rejects_non_string_request_id() {
        let backend = json!({
            "client_order_id": "client-1",
            "request_id": 123
        });
        let error = validate_submit_response_request_identity(&backend, "client-1", "request-1")
            .expect_err("non-string request_id must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { .. }
        ));
    }

    #[test]
    fn submit_response_validate_request_identity_rejects_null_request_id() {
        let backend = json!({
            "client_order_id": "client-1",
            "request_id": null
        });
        let error = validate_submit_response_request_identity(&backend, "client-1", "request-1")
            .expect_err("null request_id must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name }
            if field_name == "request_id"
        ));
    }

    #[test]
    fn submit_response_validate_request_identity_rejects_empty_client_order_id() {
        let backend = json!({
            "client_order_id": " ",
            "request_id": "request-1"
        });
        let error = validate_submit_response_request_identity(&backend, "client-1", "request-1")
            .expect_err("empty client_order_id must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { .. }
        ));
    }

    #[test]
    fn submit_response_validate_request_identity_rejects_null_client_order_id() {
        let backend = json!({
            "client_order_id": null,
            "request_id": "request-1"
        });
        let error = validate_submit_response_request_identity(&backend, "client-1", "request-1")
            .expect_err("null client_order_id must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name }
            if field_name == "client_order_id"
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
