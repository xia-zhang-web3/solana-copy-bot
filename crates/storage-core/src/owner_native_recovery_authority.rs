//! Durable EXIT identity commits to original BUY history without renewing it.
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

impl OwnerExitIntent {
    /// Exact legacy native EXIT fingerprint; the zero-baseline path is unchanged.
    pub fn native_recovery_legacy_authority_sha256(&self) -> Result<String> {
        let payload = serde_json::json!({
            "policy": "owner_native_recovery_exit_v1", "run_id": self.run_id,
            "intent_id": self.intent_id, "buy_order_id": self.buy_order_id,
            "buy_receipt_signature": self.buy_receipt_signature, "position_id": self.position_id,
            "wallet": self.wallet, "signer": self.signer, "genesis_hash": self.genesis_hash,
            "mint": self.mint, "amount_raw": self.amount_raw, "decimals": self.decimals,
            "route": self.route, "activated_at": self.activated_at, "expires_at": self.expires_at,
            "max_priority_fee_lamports": self.max_priority_fee_lamports,
            "min_reserve_lamports": self.min_reserve_lamports,
            "max_slippage_bps": self.max_slippage_bps,
            "max_daily_loss_lamports": self.max_daily_loss_lamports,
        });
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&payload)?)))
    }
}

pub fn owner_native_recovery_authority_sha256(
    intent: &OwnerExitIntent, original: &OwnerNativeRecoveryOriginal,
) -> Result<String> {
    let original_window = intent.activated_at == original.activated_at
        && intent.expires_at == original.expires_at;
    ensure!(original.activated_at < original.expires_at
        && original.policy_identity.len() == 64
        && original.policy_identity.bytes().all(|b| b.is_ascii_hexdigit())
        && (original_window || intent.activated_at >= original.expires_at)
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
