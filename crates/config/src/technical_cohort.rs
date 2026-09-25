//! Bounded source-driven copy authority, separate from Discovery publication.
use anyhow::{ensure, Result};
use serde::Deserialize;

pub const TECHNICAL_COHORT_V1: &str = "technical_cohort_v1";
pub const CLASSIC_SPL_MINT_V1: &str = "classic_spl_mint_v1";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TechnicalCohortConfig {
    pub policy: String,
    #[serde(default)]
    pub activate: bool,
    pub run_id: String,
    pub wallet_ids: Vec<String>,
    pub mint_policy: String,
    pub route: String,
    pub activated_at: String,
    pub deadline: String,
    pub max_wait_seconds: u64,
    pub max_buy_count: u32,
    pub max_source_sell_count: u32,
}

pub fn validate_technical_cohort(e: &crate::ExecutionConfig) -> Result<()> {
    let Some(p) = &e.technical_cohort else { return Ok(()); };
    ensure!(p.policy == TECHNICAL_COHORT_V1, "technical_cohort_policy");
    ensure!(p.mint_policy == CLASSIC_SPL_MINT_V1,
        "technical_cohort_mint_policy");
    ensure!((1..=3).contains(&p.wallet_ids.len())
        && p.wallet_ids.iter().all(|w| (32..=44).contains(&w.len())
            && w.bytes().all(|b| b.is_ascii_alphanumeric())
            && w != &e.canary_wallet_pubkey)
        && { let mut ids = p.wallet_ids.clone(); ids.sort(); ids.dedup();
            ids.len() == p.wallet_ids.len() },
        "technical_cohort_wallets");
    ensure!((1..=128).contains(&p.run_id.len())
        && p.run_id.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)),
        "technical_cohort_run_id");
    ensure!(p.route == "jupiter_swap_instructions" && e.canary_route == p.route,
        "technical_cohort_route");
    ensure!((121..=14_400).contains(&p.max_wait_seconds)
        && p.max_buy_count == 1 && p.max_source_sell_count == 1,
        "technical_cohort_window_or_count");
    ensure!(e.native_fresh_buy.as_ref().is_some_and(|native|
            native.policy == crate::PROCESSED_SLOT_FENCE_AVAILABILITY_V1)
        && e.owned_sell_preparation.as_ref().is_some_and(|sell|
            sell.tiny_dispatch && sell.fractional_inventory.as_deref()
                == Some("whole_wallet_parent_program_fraction_v1")),
        "technical_cohort_native_source_sell_required");
    ensure!(e.canary_max_signal_age_seconds == 120
        && e.canary_max_open_positions == 1
        && e.canary_buy_size_sol == 0.01
        && e.quote_canary_buy_size_sol == 0.01
        && e.canary_max_daily_loss_sol <= 0.02
        && e.pretrade_max_priority_fee_lamports <= 50_000
        && e.pretrade_min_sol_reserve >= 0.160_200_031
        && e.quote_canary_slippage_bps <= 50
        && e.quote_canary_buy_slippage_bps <= 50
        && e.quote_canary_sell_slippage_bps <= 50
        && e.max_submit_attempts == 1,
        "technical_cohort_financial_caps");
    ensure!(!e.enabled && e.canary_entry_submit_enabled == p.activate
        && e.owner_technical_buy.is_none() && e.owner_exit.is_none(),
        "technical_cohort_exclusive");
    if p.activate {
        ensure!(!p.activated_at.is_empty() && !p.deadline.is_empty()
            && e.canary_enabled && e.canary_tiny_submit_enabled && e.canary_dry_run
            && e.quote_canary_enabled && e.swap_instructions_dry_run_enabled
            && e.swap_transaction_dry_run_enabled && e.simulate_before_submit
            && e.tiny_experiment.id.as_deref() == Some(p.run_id.as_str())
            && crate::native_first_buy_activation(e),
            "technical_cohort_activation");
    } else {
        ensure!(!e.tiny_experiment.activate,
            "technical_cohort_inactive_tiny_activation");
    }
    Ok(())
}
