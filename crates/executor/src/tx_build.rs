use serde_json::Value;

use crate::route_policy::apply_submit_tip_policy;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubmitTipPolicyError {
    TipExceedsMax { tip_lamports: u64, max_lamports: u64 },
    TipNotAllowed { tip_lamports: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ForwardPayloadBuildError {
    InvalidJson(String),
    RootNotObject,
    Encode(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ComputeBudgetBounds {
    pub(crate) cu_limit_min: u32,
    pub(crate) cu_limit_max: u32,
    pub(crate) cu_price_min: u64,
    pub(crate) cu_price_max: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ComputeBudgetValidationError {
    CuLimitOutOfRange { min: u32, max: u32, value: u32 },
    CuPriceOutOfRange { min: u64, max: u64, value: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum SlippageValidationError {
    SlippageOutOfRange { value: f64 },
    RouteCapOutOfRange { value: f64 },
    ExceedsRouteCap {
        slippage_bps: f64,
        route_slippage_cap_bps: f64,
    },
}

pub(crate) fn resolve_submit_tip_lamports(
    route: &str,
    requested_tip_lamports: u64,
    tip_max_lamports: u64,
    allow_nonzero_tip: bool,
) -> Result<(u64, Option<&'static str>), SubmitTipPolicyError> {
    let (effective_tip_lamports, tip_policy_code) =
        apply_submit_tip_policy(route, requested_tip_lamports);
    if effective_tip_lamports > tip_max_lamports {
        return Err(SubmitTipPolicyError::TipExceedsMax {
            tip_lamports: effective_tip_lamports,
            max_lamports: tip_max_lamports,
        });
    }
    if effective_tip_lamports > 0 && !allow_nonzero_tip {
        return Err(SubmitTipPolicyError::TipNotAllowed {
            tip_lamports: effective_tip_lamports,
        });
    }

    Ok((effective_tip_lamports, tip_policy_code))
}

pub(crate) fn build_submit_forward_payload(
    raw_body: &[u8],
    requested_tip_lamports: u64,
    effective_tip_lamports: u64,
) -> Result<Vec<u8>, ForwardPayloadBuildError> {
    if requested_tip_lamports == effective_tip_lamports {
        return Ok(raw_body.to_vec());
    }

    let mut payload: Value = serde_json::from_slice(raw_body).map_err(|error| {
        ForwardPayloadBuildError::InvalidJson(format!(
            "submit request body is not valid JSON object: {}",
            error
        ))
    })?;
    let object = payload
        .as_object_mut()
        .ok_or(ForwardPayloadBuildError::RootNotObject)?;
    object.insert("tip_lamports".to_string(), Value::from(effective_tip_lamports));

    serde_json::to_vec(&payload).map_err(|error| {
        ForwardPayloadBuildError::Encode(format!("failed to encode submit request body: {}", error))
    })
}

pub(crate) fn validate_submit_compute_budget(
    cu_limit: u32,
    cu_price_micro_lamports: u64,
    bounds: ComputeBudgetBounds,
) -> Result<(), ComputeBudgetValidationError> {
    if !(bounds.cu_limit_min..=bounds.cu_limit_max).contains(&cu_limit) {
        return Err(ComputeBudgetValidationError::CuLimitOutOfRange {
            min: bounds.cu_limit_min,
            max: bounds.cu_limit_max,
            value: cu_limit,
        });
    }
    if !(bounds.cu_price_min..=bounds.cu_price_max).contains(&cu_price_micro_lamports) {
        return Err(ComputeBudgetValidationError::CuPriceOutOfRange {
            min: bounds.cu_price_min,
            max: bounds.cu_price_max,
            value: cu_price_micro_lamports,
        });
    }

    Ok(())
}

pub(crate) fn validate_submit_slippage_policy(
    slippage_bps: f64,
    route_slippage_cap_bps: f64,
    epsilon: f64,
) -> Result<(), SlippageValidationError> {
    if !slippage_bps.is_finite() || slippage_bps <= 0.0 {
        return Err(SlippageValidationError::SlippageOutOfRange {
            value: slippage_bps,
        });
    }
    if !route_slippage_cap_bps.is_finite() || route_slippage_cap_bps <= 0.0 {
        return Err(SlippageValidationError::RouteCapOutOfRange {
            value: route_slippage_cap_bps,
        });
    }
    if slippage_bps - route_slippage_cap_bps > epsilon {
        return Err(SlippageValidationError::ExceedsRouteCap {
            slippage_bps,
            route_slippage_cap_bps,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        build_submit_forward_payload, resolve_submit_tip_lamports, validate_submit_compute_budget,
        validate_submit_slippage_policy, ComputeBudgetBounds, ComputeBudgetValidationError,
        ForwardPayloadBuildError, SlippageValidationError, SubmitTipPolicyError,
    };

    #[test]
    fn tx_build_tip_policy_forces_rpc_tip_to_zero() {
        let (effective, code) =
            resolve_submit_tip_lamports("rpc", 12_345, 100_000_000, true).expect("must resolve");
        assert_eq!(effective, 0);
        assert_eq!(code, Some("rpc_tip_forced_zero"));
    }

    #[test]
    fn tx_build_tip_policy_rejects_nonzero_tip_when_disabled() {
        let error = resolve_submit_tip_lamports("jito", 1_000, 100_000_000, false)
            .expect_err("must reject non-zero tip when disabled");
        assert_eq!(
            error,
            SubmitTipPolicyError::TipNotAllowed { tip_lamports: 1_000 }
        );
    }

    #[test]
    fn tx_build_tip_policy_rejects_tip_above_max() {
        let error = resolve_submit_tip_lamports("jito", 100_000_001, 100_000_000, true)
            .expect_err("must reject above max");
        assert_eq!(
            error,
            SubmitTipPolicyError::TipExceedsMax {
                tip_lamports: 100_000_001,
                max_lamports: 100_000_000,
            }
        );
    }

    #[test]
    fn tx_build_forward_payload_rewrites_tip_when_policy_changes_value() {
        let input = br#"{"route":"rpc","tip_lamports":12345,"compute_budget":{"cu_limit":300000}}"#;
        let output = build_submit_forward_payload(input, 12_345, 0).expect("must build payload");
        let json: serde_json::Value = serde_json::from_slice(&output).expect("valid json");
        assert_eq!(json.get("tip_lamports").and_then(|v| v.as_u64()), Some(0));
    }

    #[test]
    fn tx_build_forward_payload_rejects_non_object_root() {
        let error = build_submit_forward_payload(br#"[]"#, 1, 0).expect_err("must reject");
        assert!(matches!(error, ForwardPayloadBuildError::RootNotObject));
    }

    #[test]
    fn tx_build_compute_budget_accepts_in_range_values() {
        let result = validate_submit_compute_budget(
            300_000,
            1_500,
            ComputeBudgetBounds {
                cu_limit_min: 1,
                cu_limit_max: 1_400_000,
                cu_price_min: 1,
                cu_price_max: 10_000_000,
            },
        );
        assert!(result.is_ok());
    }

    #[test]
    fn tx_build_compute_budget_rejects_limit_out_of_range() {
        let error = validate_submit_compute_budget(
            0,
            1_500,
            ComputeBudgetBounds {
                cu_limit_min: 1,
                cu_limit_max: 1_400_000,
                cu_price_min: 1,
                cu_price_max: 10_000_000,
            },
        )
        .expect_err("must reject low cu_limit");
        assert_eq!(
            error,
            ComputeBudgetValidationError::CuLimitOutOfRange {
                min: 1,
                max: 1_400_000,
                value: 0,
            }
        );
    }

    #[test]
    fn tx_build_compute_budget_rejects_price_out_of_range() {
        let error = validate_submit_compute_budget(
            300_000,
            0,
            ComputeBudgetBounds {
                cu_limit_min: 1,
                cu_limit_max: 1_400_000,
                cu_price_min: 1,
                cu_price_max: 10_000_000,
            },
        )
        .expect_err("must reject low cu_price");
        assert_eq!(
            error,
            ComputeBudgetValidationError::CuPriceOutOfRange {
                min: 1,
                max: 10_000_000,
                value: 0,
            }
        );
    }

    #[test]
    fn tx_build_slippage_accepts_valid_values() {
        let result = validate_submit_slippage_policy(10.0, 10.0, 1e-6);
        assert!(result.is_ok());
    }

    #[test]
    fn tx_build_slippage_rejects_invalid_slippage_input() {
        let error = validate_submit_slippage_policy(0.0, 10.0, 1e-6)
            .expect_err("must reject non-positive slippage");
        assert_eq!(
            error,
            SlippageValidationError::SlippageOutOfRange { value: 0.0 }
        );
    }

    #[test]
    fn tx_build_slippage_rejects_invalid_route_cap_input() {
        let error = validate_submit_slippage_policy(10.0, 0.0, 1e-6)
            .expect_err("must reject non-positive route cap");
        assert_eq!(
            error,
            SlippageValidationError::RouteCapOutOfRange { value: 0.0 }
        );
    }

    #[test]
    fn tx_build_slippage_rejects_slippage_above_route_cap() {
        let error = validate_submit_slippage_policy(25.0, 20.0, 1e-6)
            .expect_err("must reject slippage above cap");
        assert_eq!(
            error,
            SlippageValidationError::ExceedsRouteCap {
                slippage_bps: 25.0,
                route_slippage_cap_bps: 20.0,
            }
        );
    }
}
