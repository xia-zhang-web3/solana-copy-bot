use crate::submitter::{SubmitError, SubmitResult};
use chrono::{DateTime, Utc};
use serde_json::Value;

const POLICY_FLOAT_EPSILON: f64 = 1e-6;

pub(crate) fn parse_adapter_submit_response(
    body: &Value,
    expected_route: &str,
    expected_client_order_id: &str,
    expected_contract_version: &str,
    require_policy_echo: bool,
    expected_slippage_bps: f64,
    expected_tip_lamports: u64,
    expected_cu_limit: u32,
    expected_cu_price_micro_lamports: u64,
) -> std::result::Result<SubmitResult, SubmitError> {
    let status = body
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let ok_flag = body.get("ok").and_then(Value::as_bool);
    let accepted_flag = body.get("accepted").and_then(Value::as_bool);
    let is_known_success_status = matches!(status.as_str(), "ok" | "accepted" | "success");
    let is_known_reject_status = matches!(
        status.as_str(),
        "reject" | "rejected" | "error" | "failed" | "failure"
    );
    if !status.is_empty() && !is_known_success_status && !is_known_reject_status {
        return Err(SubmitError::terminal(
            "submit_adapter_invalid_status",
            format!("adapter response status={} is not recognized", status),
        ));
    }
    let is_reject =
        is_known_reject_status || ok_flag == Some(false) || accepted_flag == Some(false);
    if is_reject {
        let retryable = body
            .get("retryable")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let code = body
            .get("code")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("submit_adapter_rejected");
        let detail = body
            .get("detail")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("submit adapter rejected order");
        return Err(if retryable {
            SubmitError::retryable(code, detail)
        } else {
            SubmitError::terminal(code, detail)
        });
    }
    let is_success = accepted_flag.or(ok_flag).unwrap_or(is_known_success_status);
    if !is_success {
        return Err(SubmitError::terminal(
            "submit_adapter_invalid_response",
            "adapter response did not explicitly confirm submit success".to_string(),
        ));
    }

    let tx_signature = body
        .get("tx_signature")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            SubmitError::retryable(
                "submit_adapter_invalid_response",
                "missing tx_signature in adapter response".to_string(),
            )
        })?;
    let route = body
        .get("route")
        .and_then(Value::as_str)
        .and_then(normalize_route)
        .ok_or_else(|| {
            SubmitError::terminal(
                "submit_adapter_policy_echo_missing",
                "adapter response missing required field route".to_string(),
            )
        })?;
    if route != expected_route {
        return Err(SubmitError::terminal(
            "submit_adapter_route_mismatch",
            format!(
                "adapter response route={} does not match requested route={}",
                route, expected_route
            ),
        ));
    }
    if let Some(client_order_id) = body
        .get("client_order_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if client_order_id != expected_client_order_id {
            return Err(SubmitError::terminal(
                "submit_adapter_client_order_id_mismatch",
                format!(
                    "adapter response client_order_id={} does not match requested client_order_id={}",
                    client_order_id, expected_client_order_id
                ),
            ));
        }
    }
    if let Some(request_id) = body
        .get("request_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if request_id != expected_client_order_id {
            return Err(SubmitError::terminal(
                "submit_adapter_request_id_mismatch",
                format!(
                    "adapter response request_id={} does not match requested client_order_id={}",
                    request_id, expected_client_order_id
                ),
            ));
        }
    }
    let response_contract_version = body
        .get("contract_version")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(version) = response_contract_version {
        if version != expected_contract_version {
            return Err(SubmitError::terminal(
                "submit_adapter_contract_version_mismatch",
                format!(
                    "adapter response contract_version={} does not match expected contract_version={}",
                    version, expected_contract_version
                ),
            ));
        }
    } else if require_policy_echo {
        return Err(SubmitError::terminal(
            "submit_adapter_policy_echo_missing",
            "adapter response missing required field contract_version".to_string(),
        ));
    }

    let response_slippage_bps = body.get("slippage_bps").and_then(Value::as_f64);
    if let Some(value) = response_slippage_bps {
        if !approx_f64_eq(value, expected_slippage_bps) {
            return Err(SubmitError::terminal(
                "submit_adapter_policy_mismatch",
                format!(
                    "adapter response slippage_bps={} does not match expected slippage_bps={}",
                    value, expected_slippage_bps
                ),
            ));
        }
    } else if require_policy_echo {
        return Err(SubmitError::terminal(
            "submit_adapter_policy_echo_missing",
            "adapter response missing required field slippage_bps".to_string(),
        ));
    }

    let response_tip_lamports = body.get("tip_lamports").and_then(Value::as_u64);
    if let Some(value) = response_tip_lamports {
        if value != expected_tip_lamports {
            return Err(SubmitError::terminal(
                "submit_adapter_policy_mismatch",
                format!(
                    "adapter response tip_lamports={} does not match expected tip_lamports={}",
                    value, expected_tip_lamports
                ),
            ));
        }
    } else if require_policy_echo {
        return Err(SubmitError::terminal(
            "submit_adapter_policy_echo_missing",
            "adapter response missing required field tip_lamports".to_string(),
        ));
    }
    let applied_tip_lamports = response_tip_lamports.unwrap_or(expected_tip_lamports);
    let ata_create_rent_lamports =
        parse_optional_non_negative_u64_field(body, "ata_create_rent_lamports")?;
    let network_fee_lamports_hint =
        parse_optional_non_negative_u64_field(body, "network_fee_lamports")?;
    let base_fee_lamports_hint = parse_optional_non_negative_u64_field(body, "base_fee_lamports")?;
    let priority_fee_lamports_hint =
        parse_optional_non_negative_u64_field(body, "priority_fee_lamports")?;
    let derived_network_fee_lamports_hint = if let (Some(base), Some(priority)) =
        (base_fee_lamports_hint, priority_fee_lamports_hint)
    {
        Some(base.saturating_add(priority))
    } else {
        None
    };
    if let (Some(network_fee), Some(derived_network_fee)) =
        (network_fee_lamports_hint, derived_network_fee_lamports_hint)
    {
        if network_fee != derived_network_fee {
            return Err(SubmitError::terminal(
                "submit_adapter_invalid_response",
                format!(
                    "adapter response network_fee_lamports={} does not match base+priority={}",
                    network_fee, derived_network_fee
                ),
            ));
        }
    }

    if let Some(value) = ata_create_rent_lamports {
        if value > i64::MAX as u64 {
            return Err(SubmitError::terminal(
                "submit_adapter_invalid_response",
                format!(
                    "adapter response ata_create_rent_lamports={} exceeds i64::MAX",
                    value
                ),
            ));
        }
    }
    for (field, value) in [
        ("network_fee_lamports", network_fee_lamports_hint),
        ("base_fee_lamports", base_fee_lamports_hint),
        ("priority_fee_lamports", priority_fee_lamports_hint),
        (
            "derived_network_fee_lamports",
            derived_network_fee_lamports_hint,
        ),
    ] {
        if let Some(value) = value {
            if value > i64::MAX as u64 {
                return Err(SubmitError::terminal(
                    "submit_adapter_invalid_response",
                    format!("adapter response {}={} exceeds i64::MAX", field, value),
                ));
            }
        }
    }

    let response_cu_limit = body
        .get("compute_budget")
        .and_then(|value| value.get("cu_limit"))
        .and_then(Value::as_u64);
    if let Some(value) = response_cu_limit {
        if value != expected_cu_limit as u64 {
            return Err(SubmitError::terminal(
                "submit_adapter_policy_mismatch",
                format!(
                    "adapter response compute_budget.cu_limit={} does not match expected cu_limit={}",
                    value, expected_cu_limit
                ),
            ));
        }
    } else if require_policy_echo {
        return Err(SubmitError::terminal(
            "submit_adapter_policy_echo_missing",
            "adapter response missing required field compute_budget.cu_limit".to_string(),
        ));
    }

    let response_cu_price = body
        .get("compute_budget")
        .and_then(|value| value.get("cu_price_micro_lamports"))
        .and_then(Value::as_u64);
    if let Some(value) = response_cu_price {
        if value != expected_cu_price_micro_lamports {
            return Err(SubmitError::terminal(
                "submit_adapter_policy_mismatch",
                format!(
                    "adapter response compute_budget.cu_price_micro_lamports={} does not match expected cu_price_micro_lamports={}",
                    value, expected_cu_price_micro_lamports
                ),
            ));
        }
    } else if require_policy_echo {
        return Err(SubmitError::terminal(
            "submit_adapter_policy_echo_missing",
            "adapter response missing required field compute_budget.cu_price_micro_lamports"
                .to_string(),
        ));
    }
    if require_policy_echo {
        if base_fee_lamports_hint.is_none() {
            return Err(SubmitError::terminal(
                "submit_adapter_policy_echo_missing",
                "adapter response missing required field base_fee_lamports".to_string(),
            ));
        }
        if priority_fee_lamports_hint.is_none() {
            return Err(SubmitError::terminal(
                "submit_adapter_policy_echo_missing",
                "adapter response missing required field priority_fee_lamports".to_string(),
            ));
        }
        if network_fee_lamports_hint.is_none() {
            return Err(SubmitError::terminal(
                "submit_adapter_policy_echo_missing",
                "adapter response missing required field network_fee_lamports".to_string(),
            ));
        }
    }

    let submitted_at = match body.get("submitted_at") {
        Some(value) => {
            let raw = value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    SubmitError::terminal(
                        "submit_adapter_invalid_response",
                        "adapter response field submitted_at must be a non-empty RFC3339 string"
                            .to_string(),
                    )
                })?;
            parse_rfc3339_utc(raw).ok_or_else(|| {
                SubmitError::terminal(
                    "submit_adapter_invalid_response",
                    format!(
                        "adapter response submitted_at is not valid RFC3339 timestamp: {}",
                        raw
                    ),
                )
            })?
        }
        None => Utc::now(),
    };

    Ok(SubmitResult {
        route,
        tx_signature: tx_signature.to_string(),
        submitted_at,
        applied_tip_lamports,
        ata_create_rent_lamports,
        network_fee_lamports_hint: network_fee_lamports_hint.or(derived_network_fee_lamports_hint),
        base_fee_lamports_hint,
        priority_fee_lamports_hint,
    })
}

fn parse_rfc3339_utc(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value.trim())
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn parse_optional_non_negative_u64_field(
    body: &Value,
    field: &str,
) -> std::result::Result<Option<u64>, SubmitError> {
    let Some(value) = body.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    if let Some(parsed) = value.as_u64() {
        return Ok(Some(parsed));
    }
    Err(SubmitError::terminal(
        "submit_adapter_invalid_response",
        format!(
            "adapter response {} must be a non-negative integer when present",
            field
        ),
    ))
}

fn approx_f64_eq(left: f64, right: f64) -> bool {
    (left - right).abs() <= POLICY_FLOAT_EPSILON
}

pub(crate) fn normalize_route(route: &str) -> Option<String> {
    let route = route.trim().to_ascii_lowercase();
    if route.is_empty() {
        None
    } else {
        Some(route)
    }
}
