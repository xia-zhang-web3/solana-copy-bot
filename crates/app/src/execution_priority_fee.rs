//! Fee units at the sample/build boundary. Old untagged JSON is deliberately unknown.
use crate::execution_submit_adapter::ExecutionBuildPlanMetadata;
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "unit", content = "value", rename_all = "snake_case")]
pub(crate) enum PriorityFee {
    MicroLamportsPerComputeUnit(u64),
    TotalPriorityFeeLamports(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RequestedComputeUnitLimit(u32);

impl RequestedComputeUnitLimit {
    pub(crate) fn get(self) -> u32 {
        self.0
    }
    pub(crate) fn checked(value: u32) -> Result<Self> {
        ensure!(
            (1..=1_400_000).contains(&value),
            "priority_fee_invalid_cu_limit"
        );
        Ok(Self(value))
    }
}

impl PriorityFee {
    pub(crate) fn total(self, limit: RequestedComputeUnitLimit) -> Result<u64> {
        match self {
            Self::TotalPriorityFeeLamports(value) => Ok(value),
            Self::MicroLamportsPerComputeUnit(value) => {
                let numerator = u128::from(value)
                    .checked_mul(u128::from(limit.0))
                    .context("priority_fee_arithmetic_overflow")?;
                u64::try_from(numerator.div_ceil(1_000_000)).context("priority_fee_total_overflow")
            }
        }
    }

    pub(crate) fn price_for_limit(self, limit: RequestedComputeUnitLimit) -> Result<u64> {
        match self {
            Self::MicroLamportsPerComputeUnit(value) => Ok(value),
            // Floor intentionally preserves an integer total budget: ceil(floor(T*M/L)*L/M) <= T.
            Self::TotalPriorityFeeLamports(value) => u64::try_from(
                u128::from(value)
                    .checked_mul(1_000_000)
                    .context("priority_fee_arithmetic_overflow")?
                    / u128::from(limit.0),
            )
            .context("priority_fee_price_overflow"),
        }
    }
}

pub(crate) fn tagged_fee(json_text: Option<&str>) -> Result<PriorityFee> {
    let value: Value = serde_json::from_str(json_text.context("priority_fee_units_unknown")?)
        .context("priority_fee_invalid_sample_json")?;
    ensure!(
        value["version"].as_u64() == Some(1),
        "priority_fee_units_unknown"
    );
    ensure!(
        value["source"].as_str().is_some_and(|s| !s.is_empty()),
        "priority_fee_source_unknown"
    );
    serde_json::from_value(value).context("priority_fee_units_unknown")
}

pub(crate) fn metadata_fee(metadata: &ExecutionBuildPlanMetadata) -> Result<PriorityFee> {
    ensure!(
        metadata.priority_fee_status.as_deref() == Some("ok"),
        "priority_fee_not_ok"
    );
    let fee = tagged_fee(metadata.priority_fee_json.as_deref())?;
    match fee {
        PriorityFee::MicroLamportsPerComputeUnit(_) => {
            ensure!(
                metadata.priority_fee_lamports.is_none(),
                "priority_fee_unit_conflict"
            );
            Ok(fee)
        }
        PriorityFee::TotalPriorityFeeLamports(sample) => {
            let json: Value = serde_json::from_str(metadata.priority_fee_json.as_deref().unwrap())?;
            let requested = json
                .get("requested_total_priority_fee_lamports")
                .map(|v| v.as_u64().context("priority_fee_invalid_requested_total"))
                .transpose()?
                .unwrap_or(sample);
            ensure!(
                requested <= sample && metadata.priority_fee_lamports == Some(requested),
                "priority_fee_total_conflict"
            );
            Ok(PriorityFee::TotalPriorityFeeLamports(requested))
        }
    }
}

pub(crate) fn sample_quicknode_fee(result: &Value) -> Result<(PriorityFee, String)> {
    // QuickNode Transactions documents recommended and per_compute_unit as micro-lamports/CU.
    // Preserve the existing preference among those fields; never infer per_transaction units.
    for (field, value) in [
        ("recommended", result.get("recommended")),
        (
            "per_compute_unit.high",
            result.pointer("/per_compute_unit/high"),
        ),
        (
            "per_compute_unit.medium",
            result.pointer("/per_compute_unit/medium"),
        ),
    ] {
        if let Some(value) = value.filter(|v| !v.is_null()) {
            let number = value
                .as_u64()
                .or_else(|| value.as_str()?.parse().ok())
                .context("priority_fee_invalid_integer")?;
            let fee = PriorityFee::MicroLamportsPerComputeUnit(number);
            let mut tagged = serde_json::to_value(fee)?;
            tagged["version"] = json!(1);
            tagged["source"] = json!("qn_estimatePriorityFees");
            tagged["api_version"] = json!(2);
            tagged["field"] = json!(field);
            tagged["raw"] = result.clone();
            return Ok((fee, tagged.to_string()));
        }
    }
    bail!("priority_fee_units_unknown: no documented CU-price field; per_transaction not inferred")
}

pub(crate) fn cap_metadata_total(
    cap: u64,
    mut metadata: ExecutionBuildPlanMetadata,
) -> ExecutionBuildPlanMetadata {
    if let Ok(PriorityFee::TotalPriorityFeeLamports(total)) = metadata_fee(&metadata) {
        if cap != 0 && total > cap {
            let mut json: Value =
                serde_json::from_str(metadata.priority_fee_json.as_deref().unwrap())
                    .expect("metadata_fee validated JSON");
            json["requested_total_priority_fee_lamports"] = json!(cap);
            metadata.priority_fee_json = Some(json.to_string());
            metadata.priority_fee_lamports = Some(cap);
        }
    }
    metadata
}
