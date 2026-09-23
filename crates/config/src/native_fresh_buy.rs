//! Explicit native BUY policy selection. Absence grants no BUY authority.
use serde::Deserialize;

pub const PROCESSED_SLOT_FENCE_AVAILABILITY_V1: &str = "processed_slot_fence_availability_v1";

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeFreshBuyConfig {
    pub policy: String,
}

/// Explicit one-time authority for the first protected native BUY only.
/// An experiment ID alone never enables activation.
pub fn native_first_buy_activation(e: &crate::ExecutionConfig) -> bool {
    !e.enabled
        && e.canary_tiny_submit_enabled
        && e.tiny_experiment.activate
        && e.tiny_experiment.policy_mode == crate::TinyPolicyMode::ProtectedNativeCapital
        && e.native_fresh_buy.as_ref().is_some_and(|p| p.policy == PROCESSED_SLOT_FENCE_AVAILABILITY_V1)
        && crate::owned_sell_dispatch(e)
}
