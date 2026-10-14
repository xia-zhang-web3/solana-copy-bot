//! Durable EXIT identity commits to original BUY history and a separate lease.
use crate::OwnerExitIntent;
use anyhow::{ensure, Result};
use chrono::{DateTime, Duration, Utc};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct OwnerNativeRecoveryOriginal {
    pub activated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub policy_identity: String,
}

pub fn owner_native_recovery_authority_sha256(
    intent: &OwnerExitIntent, original: &OwnerNativeRecoveryOriginal,
) -> Result<String> {
    ensure!(original.activated_at < original.expires_at
        && original.policy_identity.len() == 64
        && original.policy_identity.bytes().all(|b| b.is_ascii_hexdigit())
        && intent.activated_at >= original.expires_at
        && intent.activated_at < intent.expires_at
        && intent.expires_at - intent.activated_at <= Duration::hours(4),
        "native_recovery_fresh_window");
    let mut bound = intent.clone();
    bound.authority_sha256.clear();
    // The ATA is uniquely derived from this intent's wallet/USDC/classic SPL.
    // Runtime validates that canonical address before quote, key load and send.
    let payload = serde_json::json!({
        "policy": "receipt_bound_fresh_exit_preserve_usdc_100_v1",
        "original": original, "exit": bound,
        "protected_baseline_policy": "receipt_owned_lot_preserve_usdc_100_v1",
        "protected_baseline_raw": 100,
    });
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&payload)?)))
}
