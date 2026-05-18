use super::redacted_url_debug_value;
use serde::Deserialize;
use std::fmt;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RiskConfig {
    pub max_position_sol: f64,
    pub max_total_exposure_sol: f64,
    pub max_exposure_per_token_sol: f64,
    pub max_concurrent_positions: u32,
    pub execution_buy_cooldown_seconds: u64,
    pub daily_loss_limit_pct: f64,
    pub max_drawdown_pct: f64,
    pub max_hold_hours: u32,
    pub shadow_stale_close_terminal_zero_price_hours: u32,
    pub shadow_stale_close_recovery_zero_price_enabled: bool,
    pub max_copy_delay_sec: u64,
    pub shadow_killswitch_enabled: bool,
    pub shadow_soft_exposure_cap_sol: f64,
    pub shadow_soft_exposure_resume_below_sol: f64,
    pub shadow_soft_pause_minutes: u64,
    pub shadow_hard_exposure_cap_sol: f64,
    pub shadow_max_open_notional_per_token_sol: f64,
    pub shadow_drawdown_1h_stop_sol: f64,
    pub shadow_drawdown_1h_pause_minutes: u64,
    pub shadow_drawdown_6h_stop_sol: f64,
    pub shadow_drawdown_6h_pause_minutes: u64,
    pub shadow_drawdown_24h_stop_sol: f64,
    pub shadow_rug_loss_return_threshold: f64,
    pub shadow_rug_loss_window_minutes: u64,
    pub shadow_rug_loss_count_threshold: u64,
    pub shadow_rug_loss_rate_sample_size: u64,
    pub shadow_rug_loss_rate_threshold: f64,
    pub shadow_token_loss_cooldown_enabled: bool,
    pub shadow_token_loss_cooldown_window_minutes: u64,
    pub shadow_token_loss_cooldown_count_threshold: u64,
    pub shadow_token_loss_cooldown_return_threshold: f64,
    pub shadow_token_loss_cooldown_catastrophe_min_entry_sol: f64,
    pub shadow_token_loss_cooldown_catastrophe_max_roi: f64,
    pub shadow_wallet_loss_cooldown_enabled: bool,
    pub shadow_wallet_loss_cooldown_window_minutes: u64,
    pub shadow_wallet_loss_cooldown_min_closed_trades: u64,
    pub shadow_wallet_loss_cooldown_min_entry_sol: f64,
    pub shadow_wallet_loss_cooldown_max_pnl_sol: f64,
    pub shadow_wallet_loss_cooldown_max_roi: f64,
    pub shadow_wallet_loss_cooldown_catastrophe_min_closed_trades: u64,
    pub shadow_wallet_loss_cooldown_catastrophe_min_entry_sol: f64,
    pub shadow_wallet_loss_cooldown_catastrophe_max_roi: f64,
    pub shadow_infra_window_minutes: u64,
    pub shadow_infra_lag_p95_threshold_ms: u64,
    pub shadow_infra_lag_breach_minutes: u64,
    pub shadow_infra_replaced_ratio_threshold: f64,
    pub shadow_infra_rpc429_delta_threshold: u64,
    pub shadow_infra_rpc5xx_delta_threshold: u64,
    pub shadow_universe_min_active_follow_wallets: u64,
    pub shadow_universe_min_eligible_wallets: u64,
    pub shadow_universe_breach_cycles: u64,
}

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct ShadowConfig {
    pub enabled: bool,
    pub helius_http_url: String,
    pub causal_holdback_enabled: bool,
    pub causal_holdback_ms: u64,
    pub refresh_seconds: u64,
    pub copy_notional_sol: f64,
    pub min_leader_notional_sol: f64,
    pub max_signal_lag_seconds: u64,
    pub quality_gates_enabled: bool,
    pub min_token_age_seconds: u64,
    pub min_holders: u64,
    pub min_liquidity_sol: f64,
    pub min_volume_5m_sol: f64,
    pub min_unique_traders_5m: u64,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            helius_http_url: String::new(),
            causal_holdback_enabled: true,
            causal_holdback_ms: 2_500,
            refresh_seconds: 60,
            copy_notional_sol: 0.25,
            min_leader_notional_sol: 0.5,
            max_signal_lag_seconds: 45,
            quality_gates_enabled: true,
            min_token_age_seconds: 30,
            min_holders: 5,
            min_liquidity_sol: 1.0,
            min_volume_5m_sol: 0.5,
            min_unique_traders_5m: 1,
        }
    }
}

impl fmt::Debug for ShadowConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShadowConfig")
            .field("enabled", &self.enabled)
            .field(
                "helius_http_url",
                &redacted_url_debug_value(&self.helius_http_url),
            )
            .field("causal_holdback_enabled", &self.causal_holdback_enabled)
            .field("causal_holdback_ms", &self.causal_holdback_ms)
            .field("refresh_seconds", &self.refresh_seconds)
            .field("copy_notional_sol", &self.copy_notional_sol)
            .field("min_leader_notional_sol", &self.min_leader_notional_sol)
            .field("max_signal_lag_seconds", &self.max_signal_lag_seconds)
            .field("quality_gates_enabled", &self.quality_gates_enabled)
            .field("min_token_age_seconds", &self.min_token_age_seconds)
            .field("min_holders", &self.min_holders)
            .field("min_liquidity_sol", &self.min_liquidity_sol)
            .field("min_volume_5m_sol", &self.min_volume_5m_sol)
            .field("min_unique_traders_5m", &self.min_unique_traders_5m)
            .finish()
    }
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_position_sol: 0.75,
            max_total_exposure_sol: 4.0,
            max_exposure_per_token_sol: 1.0,
            max_concurrent_positions: 5,
            execution_buy_cooldown_seconds: 0,
            daily_loss_limit_pct: 2.0,
            max_drawdown_pct: 8.0,
            max_hold_hours: 8,
            shadow_stale_close_terminal_zero_price_hours: 0,
            shadow_stale_close_recovery_zero_price_enabled: false,
            max_copy_delay_sec: 5,
            shadow_killswitch_enabled: true,
            shadow_soft_exposure_cap_sol: 10.0,
            shadow_soft_exposure_resume_below_sol: 9.5,
            shadow_soft_pause_minutes: 15,
            shadow_hard_exposure_cap_sol: 12.0,
            shadow_max_open_notional_per_token_sol: 1.0,
            shadow_drawdown_1h_stop_sol: -0.7,
            shadow_drawdown_1h_pause_minutes: 30,
            shadow_drawdown_6h_stop_sol: -2.5,
            shadow_drawdown_6h_pause_minutes: 240,
            shadow_drawdown_24h_stop_sol: -5.0,
            shadow_rug_loss_return_threshold: -0.70,
            shadow_rug_loss_window_minutes: 120,
            shadow_rug_loss_count_threshold: 3,
            shadow_rug_loss_rate_sample_size: 200,
            shadow_rug_loss_rate_threshold: 0.04,
            shadow_token_loss_cooldown_enabled: true,
            shadow_token_loss_cooldown_window_minutes: 1440,
            shadow_token_loss_cooldown_count_threshold: 2,
            shadow_token_loss_cooldown_return_threshold: -0.60,
            shadow_token_loss_cooldown_catastrophe_min_entry_sol: 0.15,
            shadow_token_loss_cooldown_catastrophe_max_roi: -0.60,
            shadow_wallet_loss_cooldown_enabled: true,
            shadow_wallet_loss_cooldown_window_minutes: 1440,
            shadow_wallet_loss_cooldown_min_closed_trades: 3,
            shadow_wallet_loss_cooldown_min_entry_sol: 0.30,
            shadow_wallet_loss_cooldown_max_pnl_sol: -0.05,
            shadow_wallet_loss_cooldown_max_roi: -0.10,
            shadow_wallet_loss_cooldown_catastrophe_min_closed_trades: 1,
            shadow_wallet_loss_cooldown_catastrophe_min_entry_sol: 0.20,
            shadow_wallet_loss_cooldown_catastrophe_max_roi: -0.60,
            shadow_infra_window_minutes: 20,
            shadow_infra_lag_p95_threshold_ms: 10_000,
            shadow_infra_lag_breach_minutes: 10,
            shadow_infra_replaced_ratio_threshold: 0.85,
            shadow_infra_rpc429_delta_threshold: 5,
            shadow_infra_rpc5xx_delta_threshold: 20,
            shadow_universe_min_active_follow_wallets: 15,
            shadow_universe_min_eligible_wallets: 80,
            shadow_universe_breach_cycles: 3,
        }
    }
}
