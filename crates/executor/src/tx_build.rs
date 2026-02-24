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

    if requested_tip_lamports != effective_tip_lamports {
        object.insert("tip_lamports".to_string(), Value::from(effective_tip_lamports));
    }

    serde_json::to_vec(&payload)
        .map_err(|error| ForwardPayloadBuildError::Encode(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{
        build_submit_forward_payload, resolve_submit_tip_lamports, ForwardPayloadBuildError,
        SubmitTipPolicyError,
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
}
