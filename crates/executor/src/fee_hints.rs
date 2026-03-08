use serde_json::Value;

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
pub(crate) struct ParsedResponseFeeHints {
    pub(crate) network_fee_lamports: Option<u64>,
    pub(crate) base_fee_lamports: Option<u64>,
    pub(crate) priority_fee_lamports: Option<u64>,
    pub(crate) ata_create_rent_lamports: Option<u64>,
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
    DerivedPriorityFeeExceedsU64 {
        value: u128,
    },
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FeeHintFieldParseError {
    FieldMustBeNonNegativeIntegerWhenPresent { field: &'static str },
}

pub(crate) fn parse_response_fee_hint_fields(
    body: &Value,
) -> Result<ParsedResponseFeeHints, FeeHintFieldParseError> {
    let ata_create_rent_lamports = normalize_ata_create_rent_hint(
        parse_optional_non_negative_u64_field(body, "ata_create_rent_lamports")?,
    );
    Ok(ParsedResponseFeeHints {
        network_fee_lamports: parse_optional_non_negative_u64_field(body, "network_fee_lamports")?,
        base_fee_lamports: parse_optional_non_negative_u64_field(body, "base_fee_lamports")?,
        priority_fee_lamports: parse_optional_non_negative_u64_field(
            body,
            "priority_fee_lamports",
        )?,
        ata_create_rent_lamports,
    })
}

pub(crate) fn resolve_fee_hints(inputs: FeeHintInputs) -> Result<ResolvedFeeHints, FeeHintError> {
    let default_priority_fee = derive_priority_fee_lamports(
        inputs.request_cu_limit,
        inputs.request_cu_price_micro_lamports,
    )?;
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

    let ata_create_rent_lamports =
        normalize_ata_create_rent_hint(inputs.response_ata_create_rent_lamports);
    if let Some(value) = ata_create_rent_lamports {
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
        ata_create_rent_lamports,
    })
}

fn parse_optional_non_negative_u64_field(
    body: &Value,
    field: &'static str,
) -> Result<Option<u64>, FeeHintFieldParseError> {
    let Some(value) = body.get(field) else {
        return Ok(None);
    };
    if let Some(parsed) = value.as_u64() {
        return Ok(Some(parsed));
    }
    Err(FeeHintFieldParseError::FieldMustBeNonNegativeIntegerWhenPresent { field })
}

fn normalize_ata_create_rent_hint(value: Option<u64>) -> Option<u64> {
    match value {
        Some(0) => None,
        other => other,
    }
}

fn derive_priority_fee_lamports(
    cu_limit: u32,
    cu_price_micro_lamports: u64,
) -> Result<u64, FeeHintError> {
    let lamports_u128 =
        (cu_limit as u128).saturating_mul(cu_price_micro_lamports as u128) / 1_000_000u128;
    u64::try_from(lamports_u128).map_err(|_| FeeHintError::DerivedPriorityFeeExceedsU64 {
        value: lamports_u128,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        parse_response_fee_hint_fields, resolve_fee_hints, FeeHintError, FeeHintFieldParseError,
        FeeHintInputs, ParsedResponseFeeHints, ResolvedFeeHints,
    };
    use serde_json::json;

    #[test]
    fn parse_response_fee_hint_fields_accepts_missing_values() {
        let body = json!({});
        let parsed = parse_response_fee_hint_fields(&body).expect("missing values are optional");
        assert_eq!(
            parsed,
            ParsedResponseFeeHints {
                network_fee_lamports: None,
                base_fee_lamports: None,
                priority_fee_lamports: None,
                ata_create_rent_lamports: None,
            }
        );
    }

    #[test]
    fn parse_response_fee_hint_fields_rejects_invalid_field_type() {
        let body = json!({
            "network_fee_lamports": "5000"
        });
        let error =
            parse_response_fee_hint_fields(&body).expect_err("string value must be rejected");
        assert_eq!(
            error,
            FeeHintFieldParseError::FieldMustBeNonNegativeIntegerWhenPresent {
                field: "network_fee_lamports",
            }
        );
    }

    #[test]
    fn parse_response_fee_hint_fields_rejects_null_network_fee() {
        let body = json!({
            "network_fee_lamports": null
        });
        let error = parse_response_fee_hint_fields(&body).expect_err("null value must be rejected");
        assert_eq!(
            error,
            FeeHintFieldParseError::FieldMustBeNonNegativeIntegerWhenPresent {
                field: "network_fee_lamports",
            }
        );
    }

    #[test]
    fn parse_response_fee_hint_fields_rejects_null_ata_create_rent() {
        let body = json!({
            "ata_create_rent_lamports": null
        });
        let error = parse_response_fee_hint_fields(&body).expect_err("null value must be rejected");
        assert_eq!(
            error,
            FeeHintFieldParseError::FieldMustBeNonNegativeIntegerWhenPresent {
                field: "ata_create_rent_lamports",
            }
        );
    }

    #[test]
    fn ata_parse_response_fee_hint_fields_normalizes_zero_to_absent() {
        let body = json!({
            "ata_create_rent_lamports": 0
        });
        let parsed = parse_response_fee_hint_fields(&body).expect("must parse");
        assert_eq!(parsed.ata_create_rent_lamports, None);
    }

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

    #[test]
    fn resolve_fee_hints_rejects_derived_priority_fee_overflow() {
        let error = resolve_fee_hints(FeeHintInputs {
            response_network_fee_lamports: None,
            response_base_fee_lamports: None,
            response_priority_fee_lamports: None,
            response_ata_create_rent_lamports: None,
            request_cu_limit: u32::MAX,
            request_cu_price_micro_lamports: u64::MAX,
            default_base_fee_lamports: 5_000,
        })
        .expect_err("derived priority fee must not truncate to u64");
        match error {
            FeeHintError::DerivedPriorityFeeExceedsU64 { value } => {
                assert!(value > u64::MAX as u128);
            }
            _ => panic!("unexpected error variant: {:?}", error),
        }
    }

    #[test]
    fn ata_fee_hint_zero_is_treated_as_absent() {
        let resolved = resolve_fee_hints(FeeHintInputs {
            response_network_fee_lamports: None,
            response_base_fee_lamports: None,
            response_priority_fee_lamports: None,
            response_ata_create_rent_lamports: Some(0),
            request_cu_limit: 300_000,
            request_cu_price_micro_lamports: 1_500,
            default_base_fee_lamports: 5_000,
        })
        .expect("zero ATA hint should normalize to absent");
        assert_eq!(resolved.ata_create_rent_lamports, None);
    }

    #[test]
    fn ata_fee_hint_positive_value_is_preserved() {
        let resolved = resolve_fee_hints(FeeHintInputs {
            response_network_fee_lamports: None,
            response_base_fee_lamports: None,
            response_priority_fee_lamports: None,
            response_ata_create_rent_lamports: Some(2_039_280),
            request_cu_limit: 300_000,
            request_cu_price_micro_lamports: 1_500,
            default_base_fee_lamports: 5_000,
        })
        .expect("positive ATA hint must be preserved");
        assert_eq!(resolved.ata_create_rent_lamports, Some(2_039_280));
    }

    #[test]
    fn ata_fee_hint_rejects_values_exceeding_i64() {
        let error = resolve_fee_hints(FeeHintInputs {
            response_network_fee_lamports: None,
            response_base_fee_lamports: None,
            response_priority_fee_lamports: None,
            response_ata_create_rent_lamports: Some(i64::MAX as u64 + 1),
            request_cu_limit: 300_000,
            request_cu_price_micro_lamports: 1_500,
            default_base_fee_lamports: 5_000,
        })
        .expect_err("ATA hint exceeding i64 must fail");
        assert_eq!(
            error,
            FeeHintError::FieldExceedsI64 {
                field: "ata_create_rent_lamports",
                value: i64::MAX as u64 + 1,
            }
        );
    }
}
