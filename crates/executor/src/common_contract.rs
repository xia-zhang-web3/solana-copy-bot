use std::collections::HashSet;

use crate::{
    key_validation::validate_pubkey_like, route_normalization::normalize_route,
    route_policy::requires_submit_fastlane_enabled,
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CommonContractValidationError {
    ContractVersionMissing,
    ContractVersionMismatch {
        contract_version: String,
        expected_contract_version: String,
    },
    RouteNotAllowed {
        route: String,
    },
    FastlaneNotEnabled,
    InvalidSide,
    InvalidTokenEmpty,
    InvalidTokenShape {
        error: String,
    },
    InvalidNotional,
    NotionalTooHigh {
        notional_sol: f64,
        max_notional_sol: f64,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct CommonContractInputs<'a> {
    pub(crate) request_contract_version: Option<&'a str>,
    pub(crate) expected_contract_version: &'a str,
    pub(crate) route: &'a str,
    pub(crate) route_allowlist: &'a HashSet<String>,
    pub(crate) submit_fastlane_enabled: bool,
    pub(crate) side: &'a str,
    pub(crate) token: &'a str,
    pub(crate) notional_sol: f64,
    pub(crate) max_notional_sol: f64,
}

pub(crate) fn validate_common_contract_inputs(
    inputs: CommonContractInputs<'_>,
) -> Result<(), CommonContractValidationError> {
    let contract_version = inputs
        .request_contract_version
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(CommonContractValidationError::ContractVersionMissing)?;
    if contract_version != inputs.expected_contract_version {
        return Err(CommonContractValidationError::ContractVersionMismatch {
            contract_version: contract_version.to_string(),
            expected_contract_version: inputs.expected_contract_version.to_string(),
        });
    }

    let route = normalize_route(inputs.route);
    if !inputs.route_allowlist.contains(route.as_str()) {
        return Err(CommonContractValidationError::RouteNotAllowed { route });
    }
    if requires_submit_fastlane_enabled(route.as_str()) && !inputs.submit_fastlane_enabled {
        return Err(CommonContractValidationError::FastlaneNotEnabled);
    }

    let side = inputs.side.trim().to_ascii_lowercase();
    if side != "buy" && side != "sell" {
        return Err(CommonContractValidationError::InvalidSide);
    }

    if inputs.token.trim().is_empty() {
        return Err(CommonContractValidationError::InvalidTokenEmpty);
    }
    validate_pubkey_like(inputs.token.trim()).map_err(|error| {
        CommonContractValidationError::InvalidTokenShape {
            error: error.to_string(),
        }
    })?;

    if !inputs.notional_sol.is_finite() || inputs.notional_sol <= 0.0 {
        return Err(CommonContractValidationError::InvalidNotional);
    }
    if inputs.notional_sol > inputs.max_notional_sol {
        return Err(CommonContractValidationError::NotionalTooHigh {
            notional_sol: inputs.notional_sol,
            max_notional_sol: inputs.max_notional_sol,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        validate_common_contract_inputs, CommonContractInputs, CommonContractValidationError,
    };
    use std::collections::HashSet;

    fn allowlist(values: &[&str]) -> HashSet<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn common_contract_validation_accepts_valid_inputs() {
        let inputs = CommonContractInputs {
            request_contract_version: Some("v1"),
            expected_contract_version: "v1",
            route: "rpc",
            route_allowlist: &allowlist(&["rpc"]),
            submit_fastlane_enabled: false,
            side: "buy",
            token: "11111111111111111111111111111111",
            notional_sol: 0.1,
            max_notional_sol: 1.0,
        };
        validate_common_contract_inputs(inputs).expect("valid inputs must pass");
    }

    #[test]
    fn common_contract_validation_rejects_invalid_side() {
        let inputs = CommonContractInputs {
            request_contract_version: Some("v1"),
            expected_contract_version: "v1",
            route: "rpc",
            route_allowlist: &allowlist(&["rpc"]),
            submit_fastlane_enabled: false,
            side: "hold",
            token: "11111111111111111111111111111111",
            notional_sol: 0.1,
            max_notional_sol: 1.0,
        };
        let error = validate_common_contract_inputs(inputs).expect_err("invalid side must fail");
        assert_eq!(error, CommonContractValidationError::InvalidSide);
    }

    #[test]
    fn common_contract_validation_rejects_notional_too_high() {
        let inputs = CommonContractInputs {
            request_contract_version: Some("v1"),
            expected_contract_version: "v1",
            route: "rpc",
            route_allowlist: &allowlist(&["rpc"]),
            submit_fastlane_enabled: false,
            side: "buy",
            token: "11111111111111111111111111111111",
            notional_sol: 2.0,
            max_notional_sol: 1.0,
        };
        let error =
            validate_common_contract_inputs(inputs).expect_err("too high notional must fail");
        assert!(matches!(
            error,
            CommonContractValidationError::NotionalTooHigh { .. }
        ));
    }
}
