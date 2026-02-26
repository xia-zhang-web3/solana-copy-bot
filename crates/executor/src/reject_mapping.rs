#[cfg(test)]
use axum::http::StatusCode;
use serde_json::{json, Value};

use crate::common_contract::CommonContractValidationError;
use crate::fee_hints::{FeeHintError, FeeHintFieldParseError};
use crate::request_validation::RequestValidationError;
use crate::simulate_response::SimulateResponseValidationError;
use crate::submit_response::SubmitResponseValidationError;
use crate::submit_transport::SubmitTransportArtifactError;
use crate::tx_build::{
    ComputeBudgetValidationError, ForwardPayloadBuildError, SlippageValidationError,
    SubmitTipPolicyError,
};
use crate::upstream_outcome::ParsedUpstreamReject;
use crate::Reject;

pub(crate) fn map_fee_hint_error_to_reject(error: FeeHintError) -> Reject {
    match error {
        FeeHintError::DerivedPriorityFeeExceedsU64 { .. } => {
            Reject::terminal("fee_overflow", "derived priority fee exceeds u64 range")
        }
        FeeHintError::OverflowBasePlusPriority => {
            Reject::terminal("fee_overflow", "base+priority fee overflow")
        }
        FeeHintError::NetworkFeeMismatch {
            network_fee_lamports,
            derived_network_fee_lamports,
        } => Reject::terminal(
            "submit_adapter_invalid_response",
            format!(
                "network_fee_lamports={} does not match base+priority={}",
                network_fee_lamports, derived_network_fee_lamports
            ),
        ),
        FeeHintError::FieldExceedsI64 { field, value } => Reject::terminal(
            "submit_adapter_invalid_response",
            format!("{}={} exceeds i64::MAX", field, value),
        ),
    }
}

pub(crate) fn map_common_contract_validation_error_to_reject(
    error: CommonContractValidationError,
) -> Reject {
    match error {
        CommonContractValidationError::ContractVersionMissing => Reject::terminal(
            "contract_version_missing",
            "contract_version must be provided",
        ),
        CommonContractValidationError::ContractVersionMismatch {
            contract_version,
            expected_contract_version,
        } => Reject::terminal(
            "contract_version_mismatch",
            format!(
                "contract_version={} does not match expected={}",
                contract_version, expected_contract_version
            ),
        ),
        CommonContractValidationError::RouteNotAllowed { route } => Reject::terminal(
            "route_not_allowed",
            format!("route={} is not allowed", route),
        ),
        CommonContractValidationError::FastlaneNotEnabled => Reject::terminal(
            "fastlane_not_enabled",
            "route=fastlane requires COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED=true",
        ),
        CommonContractValidationError::InvalidSide => {
            Reject::terminal("invalid_side", "side must be buy|sell")
        }
        CommonContractValidationError::InvalidTokenEmpty => {
            Reject::terminal("invalid_token", "token must be non-empty")
        }
        CommonContractValidationError::InvalidTokenShape { error } => {
            Reject::terminal("invalid_token", error)
        }
        CommonContractValidationError::InvalidNotional => Reject::terminal(
            "invalid_notional_sol",
            "notional_sol must be finite and > 0",
        ),
        CommonContractValidationError::NotionalTooHigh {
            notional_sol,
            max_notional_sol,
        } => Reject::terminal(
            "notional_too_high",
            format!(
                "notional_sol={} exceeds executor max_notional_sol={}",
                notional_sol, max_notional_sol
            ),
        ),
    }
}

pub(crate) fn map_request_validation_error_to_reject(error: RequestValidationError) -> Reject {
    match error {
        RequestValidationError::InvalidAction => Reject::terminal(
            "invalid_action",
            "simulate endpoint requires action=simulate",
        ),
        RequestValidationError::InvalidDryRun => {
            Reject::terminal("invalid_dry_run", "simulate endpoint requires dry_run=true")
        }
        RequestValidationError::InvalidSignalTs => {
            Reject::terminal("invalid_signal_ts", "signal_ts must be RFC3339")
        }
        RequestValidationError::InvalidRequestId => {
            Reject::terminal("invalid_request_id", "request_id must be non-empty")
        }
        RequestValidationError::InvalidSignalId => {
            Reject::terminal("invalid_signal_id", "signal_id must be non-empty")
        }
        RequestValidationError::InvalidClientOrderId => Reject::terminal(
            "invalid_client_order_id",
            "client_order_id must be non-empty",
        ),
    }
}

pub(crate) fn map_simulate_response_validation_error_to_reject(
    error: SimulateResponseValidationError,
) -> Reject {
    match error {
        SimulateResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name } => {
            Reject::terminal(
                "simulation_invalid_response",
                format!("upstream {} must be non-empty string when present", field_name),
            )
        }
        SimulateResponseValidationError::RouteMismatch {
            response_route,
            expected_route,
        } => Reject::terminal(
            "simulation_route_mismatch",
            format!(
                "upstream route={} does not match requested route={}",
                response_route, expected_route
            ),
        ),
        SimulateResponseValidationError::ContractVersionMismatch {
            response_contract_version,
            expected_contract_version,
        } => Reject::terminal(
            "simulation_contract_version_mismatch",
            format!(
                "upstream contract_version={} does not match expected={}",
                response_contract_version, expected_contract_version
            ),
        ),
    }
}

pub(crate) fn map_parsed_upstream_reject(reject: ParsedUpstreamReject) -> Reject {
    if reject.retryable {
        Reject::retryable(reject.code, reject.detail)
    } else {
        Reject::terminal(reject.code, reject.detail)
    }
}

pub(crate) fn map_fee_hint_field_parse_error_to_reject(error: FeeHintFieldParseError) -> Reject {
    match error {
        FeeHintFieldParseError::FieldMustBeNonNegativeIntegerWhenPresent { field } => {
            Reject::terminal(
                "submit_adapter_invalid_response",
                format!("{} must be non-negative integer when present", field),
            )
        }
    }
}

pub(crate) fn map_idempotency_error_to_reject(error: anyhow::Error) -> Reject {
    Reject::retryable(
        "idempotency_store_unavailable",
        format!("idempotency store unavailable: {}", error),
    )
}

pub(crate) fn map_submit_tip_policy_error_to_reject(error: SubmitTipPolicyError) -> Reject {
    match error {
        SubmitTipPolicyError::TipExceedsMax {
            tip_lamports: _,
            max_lamports,
        } => Reject::terminal(
            "invalid_tip_lamports",
            format!("tip_lamports exceeds max {}", max_lamports),
        ),
        SubmitTipPolicyError::TipNotAllowed { .. } => Reject::terminal(
            "tip_not_supported",
            "non-zero tip_lamports is disabled in executor config",
        ),
    }
}

pub(crate) fn map_slippage_validation_error_to_reject(error: SlippageValidationError) -> Reject {
    match error {
        SlippageValidationError::SlippageOutOfRange { .. } => Reject::terminal(
            "invalid_slippage_bps",
            "slippage_bps must be finite and > 0",
        ),
        SlippageValidationError::RouteCapOutOfRange { .. } => Reject::terminal(
            "invalid_route_slippage_cap_bps",
            "route_slippage_cap_bps must be finite and > 0",
        ),
        SlippageValidationError::ExceedsRouteCap {
            slippage_bps,
            route_slippage_cap_bps,
        } => Reject::terminal(
            "slippage_exceeds_route_cap",
            format!(
                "slippage_bps={} exceeds route_slippage_cap_bps={}",
                slippage_bps, route_slippage_cap_bps
            ),
        ),
    }
}

pub(crate) fn map_compute_budget_validation_error_to_reject(
    error: ComputeBudgetValidationError,
) -> Reject {
    match error {
        ComputeBudgetValidationError::CuLimitOutOfRange { min, max, .. } => Reject::terminal(
            "invalid_compute_budget",
            format!("compute_budget.cu_limit must be in {}..={}", min, max),
        ),
        ComputeBudgetValidationError::CuPriceOutOfRange { min, max, .. } => Reject::terminal(
            "invalid_compute_budget",
            format!(
                "compute_budget.cu_price_micro_lamports must be in {}..={}",
                min, max
            ),
        ),
    }
}

pub(crate) fn map_submit_transport_artifact_error_to_reject(
    error: SubmitTransportArtifactError,
) -> Reject {
    match error {
        SubmitTransportArtifactError::InvalidSubmitArtifactType { field_name } => {
            Reject::retryable(
                "submit_adapter_invalid_response",
                format!("upstream {} must be non-empty string when present", field_name),
            )
        }
        SubmitTransportArtifactError::InvalidUpstreamSignature { error } => Reject::retryable(
            "submit_adapter_invalid_response",
            format!(
                "upstream tx_signature is not valid base58 signature: {}",
                error
            ),
        ),
        SubmitTransportArtifactError::ConflictingSubmitArtifacts => Reject::retryable(
            "submit_adapter_invalid_response",
            "upstream response must include exactly one of tx_signature or signed_tx_base64",
        ),
        SubmitTransportArtifactError::MissingSubmitArtifact => Reject::retryable(
            "submit_adapter_invalid_response",
            "upstream response missing tx_signature and signed_tx_base64",
        ),
    }
}

pub(crate) fn map_submit_response_validation_error_to_reject(
    error: SubmitResponseValidationError,
) -> Reject {
    match error {
        SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name } => {
            Reject::terminal(
                "submit_adapter_invalid_response",
                format!("upstream {} must be non-empty string when present", field_name),
            )
        }
        SubmitResponseValidationError::RouteMismatch {
            response_route,
            expected_route,
        } => Reject::terminal(
            "submit_adapter_route_mismatch",
            format!(
                "upstream route={} does not match requested route={}",
                response_route, expected_route
            ),
        ),
        SubmitResponseValidationError::ContractVersionMismatch {
            response_contract_version,
            expected_contract_version,
        } => Reject::terminal(
            "submit_adapter_contract_version_mismatch",
            format!(
                "upstream contract_version={} does not match expected={}",
                response_contract_version, expected_contract_version
            ),
        ),
        SubmitResponseValidationError::ClientOrderIdMismatch {
            response_client_order_id,
            expected_client_order_id,
        } => Reject::terminal(
            "submit_adapter_client_order_id_mismatch",
            format!(
                "upstream client_order_id={} does not match expected client_order_id={}",
                response_client_order_id, expected_client_order_id
            ),
        ),
        SubmitResponseValidationError::RequestIdMismatch {
            response_request_id,
            expected_request_id,
        } => Reject::terminal(
            "submit_adapter_request_id_mismatch",
            format!(
                "upstream request_id={} does not match expected request_id={}",
                response_request_id, expected_request_id
            ),
        ),
        SubmitResponseValidationError::SubmittedAtMustBeNonEmptyRfc3339 => Reject::terminal(
            "submit_adapter_invalid_response",
            "submitted_at must be non-empty RFC3339 string",
        ),
        SubmitResponseValidationError::SubmittedAtInvalidRfc3339 { raw } => Reject::terminal(
            "submit_adapter_invalid_response",
            format!("submitted_at is not valid RFC3339: {}", raw),
        ),
    }
}

pub(crate) fn map_forward_payload_build_error_to_reject(error: ForwardPayloadBuildError) -> Reject {
    match error {
        ForwardPayloadBuildError::InvalidJson(detail) => {
            Reject::terminal("invalid_request_body", detail)
        }
        ForwardPayloadBuildError::RootNotObject => Reject::terminal(
            "invalid_request_body",
            "submit request body must be JSON object",
        ),
        ForwardPayloadBuildError::Encode(detail) => {
            Reject::terminal("invalid_request_body", detail)
        }
    }
}

pub(crate) fn reject_to_json(
    reject: &Reject,
    client_order_id: Option<&str>,
    contract_version: &str,
) -> Value {
    let mut payload = json!({
        "status": "reject",
        "ok": false,
        "accepted": false,
        "retryable": reject.retryable,
        "code": reject.code,
        "detail": reject.detail,
        "contract_version": contract_version,
    });
    if let Some(client_order_id) = client_order_id {
        payload["client_order_id"] = Value::String(client_order_id.to_string());
    }
    payload
}

#[cfg(test)]
pub(crate) fn simulate_http_status_for_reject(_reject: &Reject) -> StatusCode {
    StatusCode::OK
}
