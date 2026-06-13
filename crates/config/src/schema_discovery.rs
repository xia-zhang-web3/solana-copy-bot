use super::redacted_url_debug_value;
use serde::Deserialize;
use std::fmt;

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct DiscoveryConfig {
    pub scoring_window_days: u32,
    pub status_scan_window_minutes: u64,
    pub decay_window_days: u32,
    pub follow_top_n: u32,
    pub publish_min_candidate_wallets: u32,
    pub min_leader_notional_sol: f64,
    pub helius_http_url: String,
    pub fetch_refresh_seconds: u64,
    pub refresh_seconds: u64,
    pub min_trades: u32,
    pub min_active_days: u32,
    pub maturity_window_days: u32,
    pub maturity_min_active_days: u32,
    pub maturity_score_bonus: f64,
    pub min_score: f64,
    pub max_tx_per_minute: u32,
    pub min_buy_count: u32,
    pub min_tradable_ratio: f64,
    pub require_open_positions_for_publication: bool,
    pub live_portfolio_gate_enabled: bool,
    pub min_live_sol_balance: f64,
    pub min_live_portfolio_value_sol: f64,
    pub live_portfolio_max_wallets: usize,
    pub live_portfolio_request_timeout_ms: u64,
    pub live_portfolio_max_token_accounts: usize,
    pub max_rug_ratio: f64,
    pub rug_lookahead_seconds: u64,
    pub metric_snapshot_interval_seconds: u64,
    pub max_bootstrap_snapshot_age_seconds: u64,
    pub thin_market_min_volume_sol: f64,
    pub thin_market_min_unique_traders: u32,
    pub executable_wallet_filter_enabled: bool,
    pub executable_wallet_filter_window_hours: u64,
    pub executable_wallet_filter_min_samples: u32,
    pub executable_wallet_filter_max_pnl_sol: f64,
    pub executable_wallet_filter_max_flip_rate: f64,
    pub max_window_swaps_in_memory: usize,
    pub max_fetch_swaps_per_cycle: usize,
    pub max_fetch_pages_per_cycle: usize,
    pub fetch_time_budget_ms: u64,
    pub observed_swaps_retention_days: u32,
    pub scoring_aggregates_write_enabled: bool,
    pub scoring_aggregates_enabled: bool,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            scoring_window_days: 30,
            status_scan_window_minutes: 0,
            decay_window_days: 7,
            follow_top_n: 20,
            publish_min_candidate_wallets: 0,
            min_leader_notional_sol: 0.5,
            helius_http_url: String::new(),
            fetch_refresh_seconds: 60,
            refresh_seconds: 600,
            min_trades: 8,
            min_active_days: 3,
            maturity_window_days: 0,
            maturity_min_active_days: 0,
            maturity_score_bonus: 0.0,
            min_score: 0.55,
            max_tx_per_minute: 50,
            min_buy_count: 10,
            min_tradable_ratio: 0.25,
            require_open_positions_for_publication: false,
            live_portfolio_gate_enabled: false,
            min_live_sol_balance: 0.25,
            min_live_portfolio_value_sol: 0.25,
            live_portfolio_max_wallets: 60,
            live_portfolio_request_timeout_ms: 8_000,
            live_portfolio_max_token_accounts: 128,
            max_rug_ratio: 0.60,
            rug_lookahead_seconds: 30 * 60,
            metric_snapshot_interval_seconds: 30 * 60,
            max_bootstrap_snapshot_age_seconds: 12 * 60 * 60,
            thin_market_min_volume_sol: 3.0,
            thin_market_min_unique_traders: 10,
            executable_wallet_filter_enabled: false,
            executable_wallet_filter_window_hours: 48,
            executable_wallet_filter_min_samples: 10,
            executable_wallet_filter_max_pnl_sol: 0.0,
            executable_wallet_filter_max_flip_rate: 0.40,
            max_window_swaps_in_memory: 60_000,
            max_fetch_swaps_per_cycle: 20_000,
            max_fetch_pages_per_cycle: 5,
            fetch_time_budget_ms: 15_000,
            observed_swaps_retention_days: 7,
            scoring_aggregates_write_enabled: false,
            scoring_aggregates_enabled: false,
        }
    }
}

impl DiscoveryConfig {
    pub fn effective_status_scan_window_minutes(&self) -> u64 {
        let scoring_window_minutes =
            u64::from(self.scoring_window_days.max(1)).saturating_mul(24 * 60);
        if self.status_scan_window_minutes == 0 {
            scoring_window_minutes
        } else {
            self.status_scan_window_minutes
                .max(1)
                .min(scoring_window_minutes)
        }
    }

    pub fn effective_publish_min_candidate_wallets(&self) -> usize {
        let floor = if self.publish_min_candidate_wallets == 0 {
            self.follow_top_n
        } else {
            self.publish_min_candidate_wallets
        };
        floor.max(1) as usize
    }
}

impl fmt::Debug for DiscoveryConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DiscoveryConfig")
            .field("scoring_window_days", &self.scoring_window_days)
            .field(
                "status_scan_window_minutes",
                &self.status_scan_window_minutes,
            )
            .field("decay_window_days", &self.decay_window_days)
            .field("follow_top_n", &self.follow_top_n)
            .field(
                "publish_min_candidate_wallets",
                &self.publish_min_candidate_wallets,
            )
            .field("min_leader_notional_sol", &self.min_leader_notional_sol)
            .field(
                "helius_http_url",
                &redacted_url_debug_value(&self.helius_http_url),
            )
            .field("fetch_refresh_seconds", &self.fetch_refresh_seconds)
            .field("refresh_seconds", &self.refresh_seconds)
            .field("min_trades", &self.min_trades)
            .field("min_active_days", &self.min_active_days)
            .field("maturity_window_days", &self.maturity_window_days)
            .field("maturity_min_active_days", &self.maturity_min_active_days)
            .field("maturity_score_bonus", &self.maturity_score_bonus)
            .field("min_score", &self.min_score)
            .field("max_tx_per_minute", &self.max_tx_per_minute)
            .field("min_buy_count", &self.min_buy_count)
            .field("min_tradable_ratio", &self.min_tradable_ratio)
            .field(
                "require_open_positions_for_publication",
                &self.require_open_positions_for_publication,
            )
            .field(
                "live_portfolio_gate_enabled",
                &self.live_portfolio_gate_enabled,
            )
            .field("min_live_sol_balance", &self.min_live_sol_balance)
            .field(
                "min_live_portfolio_value_sol",
                &self.min_live_portfolio_value_sol,
            )
            .field(
                "live_portfolio_max_wallets",
                &self.live_portfolio_max_wallets,
            )
            .field(
                "live_portfolio_request_timeout_ms",
                &self.live_portfolio_request_timeout_ms,
            )
            .field(
                "live_portfolio_max_token_accounts",
                &self.live_portfolio_max_token_accounts,
            )
            .field("max_rug_ratio", &self.max_rug_ratio)
            .field("rug_lookahead_seconds", &self.rug_lookahead_seconds)
            .field(
                "metric_snapshot_interval_seconds",
                &self.metric_snapshot_interval_seconds,
            )
            .field(
                "max_bootstrap_snapshot_age_seconds",
                &self.max_bootstrap_snapshot_age_seconds,
            )
            .field(
                "thin_market_min_volume_sol",
                &self.thin_market_min_volume_sol,
            )
            .field(
                "thin_market_min_unique_traders",
                &self.thin_market_min_unique_traders,
            )
            .field(
                "executable_wallet_filter_enabled",
                &self.executable_wallet_filter_enabled,
            )
            .field(
                "executable_wallet_filter_window_hours",
                &self.executable_wallet_filter_window_hours,
            )
            .field(
                "executable_wallet_filter_min_samples",
                &self.executable_wallet_filter_min_samples,
            )
            .field(
                "executable_wallet_filter_max_pnl_sol",
                &self.executable_wallet_filter_max_pnl_sol,
            )
            .field(
                "executable_wallet_filter_max_flip_rate",
                &self.executable_wallet_filter_max_flip_rate,
            )
            .field(
                "max_window_swaps_in_memory",
                &self.max_window_swaps_in_memory,
            )
            .field("max_fetch_swaps_per_cycle", &self.max_fetch_swaps_per_cycle)
            .field("max_fetch_pages_per_cycle", &self.max_fetch_pages_per_cycle)
            .field("fetch_time_budget_ms", &self.fetch_time_budget_ms)
            .field(
                "observed_swaps_retention_days",
                &self.observed_swaps_retention_days,
            )
            .field(
                "scoring_aggregates_write_enabled",
                &self.scoring_aggregates_write_enabled,
            )
            .field(
                "scoring_aggregates_enabled",
                &self.scoring_aggregates_enabled,
            )
            .finish()
    }
}
