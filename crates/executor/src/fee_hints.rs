#[derive(Clone, Copy, Debug)]
pub(crate) struct FeeHintInputs {
    pub(crate) response_network_fee_lamports: Option<u64>,
    pub(crate) response_base_fee_lamports: Option<u64>,
    pub(crate) response_priority_fee_lamports: Option<u64>,
    pub(crate) response_ata_create_rent_lamports: Option<u64>,
    pub(crate) request_cu_limit: u32,
    pub(crate) request_cu_price_micro_lamports: u64,
    pub(crate) default_base_fee_lamports: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedFeeHints {
    pub(crate) network_fee_lamports: u64,
    pub(crate) base_fee_lamports: u64,
    pub(crate) priority_fee_lamports: u64,
    pub(crate) ata_create_rent_lamports: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FeeHintError {
    OverflowBasePlusPriority,
    NetworkFeeMismatch {
        network_fee_lamports: u64,
        derived_network_fee_lamports: u64,
    },
    FieldExceedsI64 {
        field: &'static str,
        value: u64,
    },
}

pub(crate) fn resolve_fee_hints(inputs: FeeHintInputs) -> Result<ResolvedFeeHints, FeeHintError> {
    let default_priority_fee = ((inputs.request_cu_limit as u128)
        .saturating_mul(inputs.request_cu_price_micro_lamports as u128)
        / 1_000_000u128) as u64;
    let base_fee_lamports = inputs
        .response_base_fee_lamports
        .unwrap_or(inputs.default_base_fee_lamports);
    let priority_fee_lamports = inputs
        .response_priority_fee_lamports
        .unwrap_or(default_priority_fee);
    let derived_network_fee_lamports = base_fee_lamports
        .checked_add(priority_fee_lamports)
        .ok_or(FeeHintError::OverflowBasePlusPriority)?;

    let network_fee_lamports = inputs
        .response_network_fee_lamports
        .unwrap_or(derived_network_fee_lamports);
    if network_fee_lamports != derived_network_fee_lamports {
        return Err(FeeHintError::NetworkFeeMismatch {
            network_fee_lamports,
            derived_network_fee_lamports,
        });
    }

    for (field, value) in [
        ("network_fee_lamports", network_fee_lamports),
        ("base_fee_lamports", base_fee_lamports),
        ("priority_fee_lamports", priority_fee_lamports),
    ] {
        if value > i64::MAX as u64 {
            return Err(FeeHintError::FieldExceedsI64 { field, value });
        }
    }

    if let Some(value) = inputs.response_ata_create_rent_lamports {
        if value > i64::MAX as u64 {
            return Err(FeeHintError::FieldExceedsI64 {
                field: "ata_create_rent_lamports",
                value,
            });
        }
    }

    Ok(ResolvedFeeHints {
        network_fee_lamports,
        base_fee_lamports,
        priority_fee_lamports,
        ata_create_rent_lamports: inputs.response_ata_create_rent_lamports,
    })
}

#[cfg(test)]
mod tests {
    use super::{resolve_fee_hints, FeeHintError, FeeHintInputs, ResolvedFeeHints};

    #[test]
    fn resolve_fee_hints_uses_defaults_when_response_hints_missing() {
        let resolved = resolve_fee_hints(FeeHintInputs {
            response_network_fee_lamports: None,
            response_base_fee_lamports: None,
            response_priority_fee_lamports: None,
            response_ata_create_rent_lamports: None,
            request_cu_limit: 300_000,
            request_cu_price_micro_lamports: 1_500,
            default_base_fee_lamports: 5_000,
        })
        .expect("defaults must resolve");
        assert_eq!(
            resolved,
            ResolvedFeeHints {
                network_fee_lamports: 5_450,
                base_fee_lamports: 5_000,
                priority_fee_lamports: 450,
                ata_create_rent_lamports: None,
            }
        );
    }

    #[test]
    fn resolve_fee_hints_rejects_network_fee_mismatch() {
        let error = resolve_fee_hints(FeeHintInputs {
            response_network_fee_lamports: Some(8_000),
            response_base_fee_lamports: Some(5_000),
            response_priority_fee_lamports: Some(400),
            response_ata_create_rent_lamports: None,
            request_cu_limit: 300_000,
            request_cu_price_micro_lamports: 1_500,
            default_base_fee_lamports: 5_000,
        })
        .expect_err("mismatch must fail");
        assert_eq!(
            error,
            FeeHintError::NetworkFeeMismatch {
                network_fee_lamports: 8_000,
                derived_network_fee_lamports: 5_400,
            }
        );
    }

    #[test]
    fn resolve_fee_hints_rejects_values_exceeding_i64() {
        let error = resolve_fee_hints(FeeHintInputs {
            response_network_fee_lamports: Some(i64::MAX as u64 + 1),
            response_base_fee_lamports: Some(5_000),
            response_priority_fee_lamports: Some(i64::MAX as u64 - 4_999),
            response_ata_create_rent_lamports: None,
            request_cu_limit: 1,
            request_cu_price_micro_lamports: 1,
            default_base_fee_lamports: 5_000,
        })
        .expect_err("i64 overflow must fail");
        assert_eq!(
            error,
            FeeHintError::FieldExceedsI64 {
                field: "network_fee_lamports",
                value: i64::MAX as u64 + 1,
            }
        );
    }
}
