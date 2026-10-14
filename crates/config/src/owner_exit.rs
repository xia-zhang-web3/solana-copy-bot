//! Single-position owner exit; inactive unless explicitly activated.
use anyhow::{ensure, Result};
use serde::Deserialize;

pub const OWNER_ROUNDTRIP_EXIT_V1: &str = "owner_execution_roundtrip_exit_v1";
pub const OWNER_EXIT_V1: &str = "owner_exit_v1";
pub const OWNER_SOURCE_RECOVERY_EXIT_V1: &str = "owner_source_recovery_exit_v1";
pub const OWNER_NATIVE_RECOVERY_EXIT_V1: &str = "owner_native_recovery_exit_v1";

pub fn owner_native_recovery_exit(e: &crate::ExecutionConfig) -> bool {
    e.owner_exit.as_ref().is_some_and(|p| p.policy == OWNER_NATIVE_RECOVERY_EXIT_V1)
}

pub fn owner_source_recovery_exit(e: &crate::ExecutionConfig) -> bool {
    e.owner_exit.as_ref().is_some_and(|p| p.policy == OWNER_SOURCE_RECOVERY_EXIT_V1)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerExitConfig {
    pub policy: String,
    #[serde(default)]
    pub native_recovery: Option<crate::OwnerNativeRecoveryConfig>,
    #[serde(default)]
    pub activate: bool,
    pub run_id: String,
    pub intent_id: String,
    pub buy_order_id: String,
    pub buy_receipt_signature: String,
    pub position_id: String,
    pub wallet_pubkey: String,
    pub signer_pubkey: String,
    pub genesis_hash: String,
    pub mint: String,
    pub amount_raw: u64,
    pub decimals: u8,
    pub route: String,
    pub activated_at: String,
    pub expires_at: String,
    pub max_priority_fee_lamports: u64,
    pub min_reserve_lamports: u64,
    pub max_slippage_bps: u64,
    pub max_daily_loss_lamports: u64,
}

pub fn validate_owner_exit(e: &crate::ExecutionConfig) -> Result<()> {
    let Some(p) = &e.owner_exit else { return Ok(()); };
    ensure!(matches!(p.policy.as_str(),OWNER_EXIT_V1 | OWNER_ROUNDTRIP_EXIT_V1
        | OWNER_SOURCE_RECOVERY_EXIT_V1 | OWNER_NATIVE_RECOVERY_EXIT_V1), "owner_exit_policy");
    ensure!(owner_native_recovery_exit(e) || (!p.intent_id.starts_with("native-recovery:")
        && !p.buy_order_id.starts_with("exec-canary:native-buy-v1:")), "owner_exit_native_policy_required");
    ensure!(owner_native_recovery_exit(e) || p.native_recovery.is_none(), "owner_native_recovery_policy_required");
    if let Some(recovery) = &p.native_recovery { crate::owner_native_recovery::validate(recovery)?; }
    if owner_native_recovery_exit(e) {
        let source = p.buy_order_id.strip_prefix("exec-canary:native-buy-v1:")
            .ok_or_else(|| anyhow::anyhow!("owner_native_recovery_buy_identity"))?;
        ensure!(!source.is_empty() && p.intent_id == format!("native-recovery:{source}")
            && p.position_id == format!("exec-canary-pos:{}",p.buy_order_id)
            && e.owner_technical_buy.is_none() && e.native_fresh_buy.is_none()
            && e.technical_cohort.is_none() && e.owned_sell_preparation.is_none()
            && !e.enabled && !e.canary_entry_submit_enabled && !e.tiny_experiment.activate
            && e.max_submit_attempts == 1 && p.max_priority_fee_lamports <= 22_000
            && e.canary_max_open_positions == 1,
            "owner_native_recovery_sell_only");
    } else if p.policy != OWNER_EXIT_V1 {
        let b=e.owner_technical_buy.as_ref().ok_or_else(||anyhow::anyhow!("owner_roundtrip_exit_parent"))?;
        ensure!(b.policy == crate::OWNER_EXECUTION_ROUNDTRIP_V1
            && p.intent_id == format!("execution-only:{}",b.intent_id)
            && p.buy_order_id == format!("exec-canary:owner-buy:{}",b.intent_id)
            && p.run_id == b.run_id && p.wallet_pubkey == b.wallet_pubkey && p.signer_pubkey == b.signer_pubkey
            && p.genesis_hash == b.genesis_hash && p.mint == b.mint && p.route == b.route
            && p.activated_at == b.activated_at && p.expires_at == b.expires_at
            && p.max_priority_fee_lamports <= b.max_priority_fee_lamports.min(22_000)
            && p.min_reserve_lamports >= b.min_reserve_lamports
            && p.max_slippage_bps <= b.max_slippage_bps
            && p.max_daily_loss_lamports <= b.max_daily_loss_lamports,
            "owner_roundtrip_exit_parent");
        if owner_source_recovery_exit(e) {
            crate::validate_owner_source_copy_gate(b)?;
            ensure!(!b.activate && !e.enabled && !e.canary_entry_submit_enabled
                && e.native_fresh_buy.is_none() && e.technical_cohort.is_none()
                && e.max_submit_attempts == 1 && p.min_reserve_lamports == b.min_reserve_lamports
                && b.source_copy_exit_gate.as_ref().is_some_and(|g|
                    g.bot_policy_identity != "0".repeat(64)),
                "owner_source_recovery_sell_only");
        }
    }
    for id in [&p.run_id, &p.intent_id] {
        ensure!((1..=128).contains(&id.len())
            && id.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)),
            "owner_exit_id");
    }
    ensure!(p.wallet_pubkey == p.signer_pubkey
        && p.wallet_pubkey == e.canary_wallet_pubkey
        && p.signer_pubkey == e.execution_signer_pubkey,
        "owner_exit_wallet_signer");
    ensure!(p.mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        && p.decimals == 6 && (if p.policy == OWNER_EXIT_V1 {p.amount_raw == 1_167_085}
            else {p.amount_raw > 0 && (p.intent_id.starts_with("execution-only:")
                || (owner_native_recovery_exit(e) && p.intent_id.starts_with("native-recovery:")))}),
        "owner_exit_exact_position_amount");
    ensure!(p.route == "jupiter_swap_instructions" && p.route == e.canary_route,
        "owner_exit_route");
    ensure!(p.max_priority_fee_lamports > 0
        && p.max_priority_fee_lamports <= e.pretrade_max_priority_fee_lamports,
        "owner_exit_priority_cap");
    ensure!(p.min_reserve_lamports >= 160_200_031
        && (e.pretrade_min_sol_reserve * 1_000_000_000.0).ceil()
            >= p.min_reserve_lamports as f64,
        "owner_exit_reserve_floor");
    ensure!(p.max_slippage_bps > 0 && p.max_slippage_bps <= 50
        && e.quote_canary_sell_slippage_bps >= p.max_slippage_bps,
        "owner_exit_slippage_cap");
    ensure!(p.max_daily_loss_lamports > 0 && p.max_daily_loss_lamports <= 20_000_000
        && e.canary_max_daily_loss_sol * 1_000_000_000.0 <= p.max_daily_loss_lamports as f64,
        "owner_exit_loss_cap");
    ensure!(!p.buy_order_id.is_empty() && !p.buy_receipt_signature.is_empty()
        && !p.position_id.is_empty() && !p.genesis_hash.is_empty(),
        "owner_exit_buy_binding_missing");
    if p.activate {
        ensure!(e.owner_technical_buy.as_ref().is_none_or(|b| !b.activate)
            && e.native_fresh_buy.is_none() && !e.canary_entry_submit_enabled,
            "owner_exit_buy_disabled");
        ensure!(!e.enabled && e.canary_enabled && e.canary_tiny_submit_enabled
            && e.canary_dry_run && e.quote_canary_enabled
            && e.swap_instructions_dry_run_enabled && e.swap_transaction_dry_run_enabled
            && e.simulate_before_submit && e.max_submit_attempts == 1,
            "owner_exit_execution_flags");
        ensure!(e.tiny_experiment.id.as_deref() == Some(p.run_id.as_str())
            && !e.tiny_experiment.activate,
            "owner_exit_tiny_experiment");
        ensure!(!e.execution_signer_keypair_path.trim().is_empty()
            && !e.submit_adapter_http_url.trim().is_empty(),
            "owner_exit_signer_or_rpc");
    }
    Ok(())
}
