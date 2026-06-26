use crate::{DiscoveryConfig, ShadowConfig};

pub const DISCOVERY_V2_SCORING_SOURCE: &str = "discovery_v2_operational_window";
pub const DISCOVERY_V2_TOKEN_QUALITY_TTL_SECONDS: i64 = 2 * 60 * 60;
pub const DISCOVERY_V2_TOKEN_ROLLING_MARKET_WINDOW_SECONDS: i64 = 5 * 60;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_WINDOW_HOURS: i64 = 24;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_MIN_CLOSED_TRADES: u64 = 3;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_MIN_ENTRY_SOL: f64 = 0.30;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_MAX_PNL_SOL: f64 = -0.05;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_MAX_ROI: f64 = -0.10;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MIN_CLOSED_TRADES: u64 = 1;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MIN_ENTRY_SOL: f64 = 0.20;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MAX_ROI: f64 = -0.50;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_SINGLE_LOSS_MIN_ENTRY_SOL: f64 = 0.20;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_SINGLE_LOSS_MAX_ROI: f64 = -0.20;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_FAST_LOSS_MIN_ENTRY_SOL: f64 = 0.15;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_FAST_LOSS_MAX_ROI: f64 = -0.08;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_FAST_LOSS_MAX_HOLD_SECONDS: i64 = 5 * 60;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_STALE_COPY_MIN_ENTRY_SOL: f64 = 0.15;
pub const DISCOVERY_V2_SHADOW_FEEDBACK_STALE_COPY_MAX_ROI: f64 = -0.10;

#[derive(Debug, Clone, Copy)]
pub struct DiscoveryV2PolicyFingerprintInput {
    pub window_minutes: u64,
    pub max_tail_lag_seconds: u64,
    pub max_rows: usize,
    pub time_budget_ms: u64,
    pub execution_enabled: bool,
}

pub fn discovery_v2_policy_fingerprint(
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    input: DiscoveryV2PolicyFingerprintInput,
) -> String {
    format!(
        concat!(
            "scoring_source={};window_minutes={};max_tail_lag_seconds={};",
            "max_rows={};time_budget_ms={};follow_top_n={};",
            "publish_min_candidate_wallets={};",
            "decay_window_days={};rug_lookahead_seconds={};",
            "metric_snapshot_interval_seconds={};",
            "min_leader_notional_sol_bits={:016x};min_trades={};min_active_days={};",
            "maturity_window_days={};maturity_min_active_days={};",
            "maturity_score_bonus_bits={:016x};maturity_selection_version=2;",
            "min_score_bits={:016x};max_tx_per_minute={};min_buy_count={};",
            "min_tradable_ratio_bits={:016x};require_open_positions_for_publication={};",
            "slow_hold_wallets_enabled={};slow_hold_top_m={};",
            "slow_hold_min_hold_median_seconds={};slow_hold_min_trades={};",
            "slow_hold_min_buy_count={};slow_hold_min_active_days={};",
            "slow_hold_max_stale_days={};slow_hold_min_score_bits={:016x};",
            "live_portfolio_gate_enabled={};min_live_sol_balance_bits={:016x};",
            "min_live_portfolio_value_sol_bits={:016x};live_portfolio_max_wallets={};",
            "live_portfolio_request_timeout_ms={};live_portfolio_max_token_accounts={};",
            "max_rug_ratio_bits={:016x};thin_market_min_volume_sol_bits={:016x};",
            "thin_market_min_unique_traders={};quality_gates_enabled={};",
            "min_token_age_seconds={};min_holders={};min_liquidity_sol_bits={:016x};",
            "min_volume_5m_sol_bits={:016x};min_unique_traders_5m={};",
            "execution_enabled={};token_quality_ttl_seconds={};",
            "token_rolling_market_window_seconds={};",
            "shadow_feedback_version=5;shadow_feedback_window_hours={};",
            "shadow_feedback_min_closed_trades={};",
            "shadow_feedback_min_entry_sol_bits={:016x};",
            "shadow_feedback_max_pnl_sol_bits={:016x};",
            "shadow_feedback_max_roi_bits={:016x};",
            "shadow_feedback_catastrophe_min_closed_trades={};",
            "shadow_feedback_catastrophe_min_entry_sol_bits={:016x};",
            "shadow_feedback_catastrophe_max_roi_bits={:016x};",
            "shadow_feedback_single_loss_min_entry_sol_bits={:016x};",
            "shadow_feedback_single_loss_max_roi_bits={:016x};",
            "shadow_feedback_fast_loss_min_entry_sol_bits={:016x};",
            "shadow_feedback_fast_loss_max_roi_bits={:016x};",
            "shadow_feedback_fast_loss_max_hold_seconds={};",
            "shadow_feedback_stale_copy_min_entry_sol_bits={:016x};",
            "shadow_feedback_stale_copy_max_roi_bits={:016x};",
            "executable_wallet_filter_version=1;",
            "executable_wallet_filter_enabled={};",
            "executable_wallet_filter_window_hours={};",
            "executable_wallet_filter_min_samples={};",
            "executable_wallet_filter_max_pnl_sol_bits={:016x};",
            "executable_wallet_filter_max_flip_rate_bits={:016x};",
            "rug_wallet_filter_version=1;",
            "rug_wallet_filter_enabled={};",
            "rug_wallet_filter_window_hours={};",
            "rug_wallet_filter_min_closed_trades={};",
            "rug_wallet_filter_max_stale_terminal_rate_bits={:016x};",
            "rug_wallet_filter_max_stale_terminal_pnl_sol_bits={:016x};",
            "rug_wallet_filter_quarantine_hours={}"
        ),
        DISCOVERY_V2_SCORING_SOURCE,
        input.window_minutes,
        input.max_tail_lag_seconds,
        input.max_rows,
        input.time_budget_ms,
        discovery.follow_top_n,
        discovery.effective_publish_min_candidate_wallets(),
        discovery.decay_window_days,
        discovery.rug_lookahead_seconds,
        discovery.metric_snapshot_interval_seconds,
        discovery.min_leader_notional_sol.to_bits(),
        discovery.min_trades,
        discovery.min_active_days,
        discovery.maturity_window_days,
        discovery.maturity_min_active_days,
        discovery.maturity_score_bonus.to_bits(),
        discovery.min_score.to_bits(),
        discovery.max_tx_per_minute,
        discovery.min_buy_count,
        discovery.min_tradable_ratio.to_bits(),
        discovery.require_open_positions_for_publication,
        discovery.slow_hold_wallets_enabled,
        discovery.slow_hold_top_m,
        discovery.slow_hold_min_hold_median_seconds,
        discovery.slow_hold_min_trades,
        discovery.slow_hold_min_buy_count,
        discovery.slow_hold_min_active_days,
        discovery.slow_hold_max_stale_days,
        discovery.slow_hold_min_score.to_bits(),
        discovery.live_portfolio_gate_enabled,
        discovery.min_live_sol_balance.to_bits(),
        discovery.min_live_portfolio_value_sol.to_bits(),
        discovery.live_portfolio_max_wallets,
        discovery.live_portfolio_request_timeout_ms,
        discovery.live_portfolio_max_token_accounts,
        discovery.max_rug_ratio.to_bits(),
        discovery.thin_market_min_volume_sol.to_bits(),
        discovery.thin_market_min_unique_traders,
        shadow.quality_gates_enabled,
        shadow.min_token_age_seconds,
        shadow.min_holders,
        shadow.min_liquidity_sol.to_bits(),
        shadow.min_volume_5m_sol.to_bits(),
        shadow.min_unique_traders_5m,
        input.execution_enabled,
        DISCOVERY_V2_TOKEN_QUALITY_TTL_SECONDS,
        DISCOVERY_V2_TOKEN_ROLLING_MARKET_WINDOW_SECONDS,
        DISCOVERY_V2_SHADOW_FEEDBACK_WINDOW_HOURS,
        DISCOVERY_V2_SHADOW_FEEDBACK_MIN_CLOSED_TRADES,
        DISCOVERY_V2_SHADOW_FEEDBACK_MIN_ENTRY_SOL.to_bits(),
        DISCOVERY_V2_SHADOW_FEEDBACK_MAX_PNL_SOL.to_bits(),
        DISCOVERY_V2_SHADOW_FEEDBACK_MAX_ROI.to_bits(),
        DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MIN_CLOSED_TRADES,
        DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MIN_ENTRY_SOL.to_bits(),
        DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MAX_ROI.to_bits(),
        DISCOVERY_V2_SHADOW_FEEDBACK_SINGLE_LOSS_MIN_ENTRY_SOL.to_bits(),
        DISCOVERY_V2_SHADOW_FEEDBACK_SINGLE_LOSS_MAX_ROI.to_bits(),
        DISCOVERY_V2_SHADOW_FEEDBACK_FAST_LOSS_MIN_ENTRY_SOL.to_bits(),
        DISCOVERY_V2_SHADOW_FEEDBACK_FAST_LOSS_MAX_ROI.to_bits(),
        DISCOVERY_V2_SHADOW_FEEDBACK_FAST_LOSS_MAX_HOLD_SECONDS,
        DISCOVERY_V2_SHADOW_FEEDBACK_STALE_COPY_MIN_ENTRY_SOL.to_bits(),
        DISCOVERY_V2_SHADOW_FEEDBACK_STALE_COPY_MAX_ROI.to_bits(),
        discovery.executable_wallet_filter_enabled,
        discovery.executable_wallet_filter_window_hours,
        discovery.executable_wallet_filter_min_samples,
        discovery.executable_wallet_filter_max_pnl_sol.to_bits(),
        discovery.executable_wallet_filter_max_flip_rate.to_bits(),
        discovery.rug_wallet_filter_enabled,
        discovery.rug_wallet_filter_window_hours,
        discovery.rug_wallet_filter_min_closed_trades,
        discovery
            .rug_wallet_filter_max_stale_terminal_rate
            .to_bits(),
        discovery
            .rug_wallet_filter_max_stale_terminal_pnl_sol
            .to_bits(),
        discovery.rug_wallet_filter_quarantine_hours,
    )
}
