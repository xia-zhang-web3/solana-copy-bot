//! Explicit native BUY policy selection. Absence grants no BUY authority.
use serde::Deserialize;

pub const PROCESSED_SLOT_FENCE_AVAILABILITY_V1: &str = "processed_slot_fence_availability_v1";

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeFreshBuyConfig {
    pub policy: String,
}
