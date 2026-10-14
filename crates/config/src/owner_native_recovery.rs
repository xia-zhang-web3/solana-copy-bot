//! A new EXIT lease references, but never renews, the original native BUY.
use anyhow::{ensure, Result};
use serde::Deserialize;

pub const FRESH_NATIVE_RECOVERY_V1: &str = "receipt_bound_fresh_exit_preserve_usdc_100_v1";
pub const RECOVERY_USDC_100_V1: &str = "receipt_owned_lot_preserve_usdc_100_v1";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerNativeRecoveryConfig {
    pub policy: String,
    pub original_activated_at: String,
    pub original_expires_at: String,
    pub original_policy_identity: String,
    pub protected_bot_usdc_baseline: RecoveryUsdcBaselineConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryUsdcBaselineConfig {
    pub policy: String,
    pub amount_raw: u64,
    pub token_account: String,
}

pub(crate) fn validate(p: &OwnerNativeRecoveryConfig) -> Result<()> {
    let b = &p.protected_bot_usdc_baseline;
    ensure!(p.policy == FRESH_NATIVE_RECOVERY_V1
        && !p.original_activated_at.is_empty() && !p.original_expires_at.is_empty()
        && p.original_policy_identity.len() == 64
        && p.original_policy_identity.bytes().all(|b| b.is_ascii_hexdigit())
        && b.policy == RECOVERY_USDC_100_V1 && b.amount_raw == 100
        && (32..=44).contains(&b.token_account.len())
        && b.token_account.bytes().all(|b| b.is_ascii_alphanumeric()),
        "owner_native_recovery_contract");
    Ok(())
}
