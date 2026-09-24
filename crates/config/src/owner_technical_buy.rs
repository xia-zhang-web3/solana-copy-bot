//! Explicit, one-use owner BUY authority. Presence alone never activates it.
use anyhow::{ensure, Result};
use serde::Deserialize;

pub const OWNER_TECHNICAL_BUY_V1: &str = "owner_technical_buy_v1";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerTechnicalBuyConfig {
    pub policy: String,
    #[serde(default)]
    pub activate: bool,
    pub run_id: String,
    pub intent_id: String,
    pub wallet_pubkey: String,
    pub signer_pubkey: String,
    pub genesis_hash: String,
    pub mint: String,
    pub amount_lamports: u64,
    pub route: String,
    pub activated_at: String,
    pub expires_at: String,
    pub max_priority_fee_lamports: u64,
    pub min_reserve_lamports: u64,
    pub max_slippage_bps: u64,
    pub max_daily_loss_lamports: u64,
    pub max_open_positions: u32,
    pub max_buy_count: u32,
}

pub fn validate_owner_technical_buy(e: &crate::ExecutionConfig) -> Result<()> {
    let Some(p) = &e.owner_technical_buy else { return Ok(()); };
    ensure!(p.policy == OWNER_TECHNICAL_BUY_V1, "owner_buy_policy");
    for id in [&p.run_id, &p.intent_id] {
        ensure!((1..=128).contains(&id.len())
            && id.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)),
            "owner_buy_id");
    }
    ensure!(!p.activated_at.is_empty() && !p.expires_at.is_empty(), "owner_buy_window");
    ensure!(p.wallet_pubkey == p.signer_pubkey
        && p.wallet_pubkey == e.canary_wallet_pubkey
        && p.signer_pubkey == e.execution_signer_pubkey,
        "owner_buy_wallet_signer");
    ensure!(p.route == e.canary_route && p.route == "jupiter_swap_instructions",
        "owner_buy_route");
    ensure!(p.amount_lamports > 0
        && e.canary_buy_size_sol.is_finite()
        && e.canary_buy_size_sol > 0.0
        && (e.canary_buy_size_sol * 1_000_000_000.0).round() == p.amount_lamports as f64,
        "owner_buy_amount");
    ensure!(p.max_buy_count == 1 && p.max_open_positions == 1,
        "owner_buy_one_buy_limit");
    ensure!(p.max_priority_fee_lamports > 0
        && p.max_priority_fee_lamports <= e.pretrade_max_priority_fee_lamports,
        "owner_buy_priority_cap");
    ensure!(p.min_reserve_lamports > 0
        && (e.pretrade_min_sol_reserve * 1_000_000_000.0).ceil() >= p.min_reserve_lamports as f64,
        "owner_buy_reserve_floor");
    ensure!(p.max_slippage_bps > 0
        && p.max_slippage_bps <= 5_000
        && (if e.quote_canary_buy_slippage_bps > 0 {
            e.quote_canary_buy_slippage_bps
        } else { e.quote_canary_slippage_bps }) >= p.max_slippage_bps,
        "owner_buy_slippage_cap");
    ensure!(p.max_daily_loss_lamports > 0
        && e.canary_max_daily_loss_sol.is_finite()
        && e.canary_max_daily_loss_sol * 1_000_000_000.0 <= p.max_daily_loss_lamports as f64,
        "owner_buy_loss_cap");
    ensure!(e.canary_max_open_positions <= p.max_open_positions,
        "owner_buy_position_cap");
    if p.activate {
        ensure!(e.native_fresh_buy.is_none(), "owner_buy_native_copy_conflict");
        ensure!(!e.enabled && e.canary_enabled && e.canary_tiny_submit_enabled
            && e.canary_dry_run && e.canary_entry_submit_enabled
            && e.quote_canary_enabled && e.swap_instructions_dry_run_enabled
            && e.swap_transaction_dry_run_enabled && e.simulate_before_submit,
            "owner_buy_execution_flags");
        ensure!(e.tiny_experiment.id.as_deref() == Some(p.run_id.as_str())
            && !e.tiny_experiment.activate
            && matches!(e.tiny_experiment.policy_mode, crate::TinyPolicyMode::DecodedAmount
                | crate::TinyPolicyMode::ProtectedNativeCapital),
            "owner_buy_tiny_experiment");
        ensure!(!e.execution_signer_keypair_path.trim().is_empty()
            && !e.submit_adapter_http_url.trim().is_empty(), "owner_buy_signer_or_rpc");
    }
    ensure!([&p.wallet_pubkey, &p.signer_pubkey, &p.genesis_hash, &p.mint]
        .iter().all(|v| !v.trim().is_empty()), "owner_buy_pubkey");
    Ok(())
}
