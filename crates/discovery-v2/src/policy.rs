use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};

use crate::tradability::TOKEN_ROLLING_MARKET_WINDOW_SECONDS;

pub const TOKEN_QUALITY_TTL_SECONDS: i64 = 10 * 60;

#[derive(Debug, Clone)]
pub struct DiscoveryV2BuildOptions {
    pub now: DateTime<Utc>,
    pub window_minutes: u64,
    pub max_tail_lag_seconds: u64,
    pub max_rows: usize,
    pub time_budget_ms: u64,
    pub execution_enabled: bool,
}

impl DiscoveryV2BuildOptions {
    pub fn from_config(
        discovery: &DiscoveryConfig,
        execution_enabled: bool,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            now,
            window_minutes: u64::from(discovery.scoring_window_days.max(1)).saturating_mul(24 * 60),
            max_tail_lag_seconds: discovery.refresh_seconds.max(1),
            max_rows: discovery
                .max_window_swaps_in_memory
                .max(discovery.max_fetch_swaps_per_cycle)
                .max(1),
            time_budget_ms: discovery.fetch_time_budget_ms.max(1),
            execution_enabled,
        }
    }

    pub fn window_start(&self) -> DateTime<Utc> {
        self.now - Duration::minutes(self.window_minutes.min(i64::MAX as u64) as i64)
    }
}

pub fn discovery_v2_policy_fingerprint(
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    options: &DiscoveryV2BuildOptions,
) -> String {
    format!(
        concat!(
            "scoring_source={};window_minutes={};max_tail_lag_seconds={};",
            "max_rows={};time_budget_ms={};follow_top_n={};",
            "decay_window_days={};rug_lookahead_seconds={};",
            "metric_snapshot_interval_seconds={};",
            "min_leader_notional_sol={:.6};min_trades={};min_active_days={};",
            "min_score={:.6};max_tx_per_minute={};min_buy_count={};",
            "min_tradable_ratio={:.6};require_open_positions_for_publication={};",
            "max_rug_ratio={:.6};thin_market_min_volume_sol={:.6};",
            "thin_market_min_unique_traders={};quality_gates_enabled={};",
            "min_token_age_seconds={};min_holders={};min_liquidity_sol={:.6};",
            "min_volume_5m_sol={:.6};min_unique_traders_5m={};",
            "execution_enabled={};token_quality_ttl_seconds={};",
            "token_rolling_market_window_seconds={}"
        ),
        crate::status::DISCOVERY_V2_SCORING_SOURCE,
        options.window_minutes,
        options.max_tail_lag_seconds,
        options.max_rows,
        options.time_budget_ms,
        discovery.follow_top_n,
        discovery.decay_window_days,
        discovery.rug_lookahead_seconds,
        discovery.metric_snapshot_interval_seconds,
        discovery.min_leader_notional_sol,
        discovery.min_trades,
        discovery.min_active_days,
        discovery.min_score,
        discovery.max_tx_per_minute,
        discovery.min_buy_count,
        discovery.min_tradable_ratio,
        discovery.require_open_positions_for_publication,
        discovery.max_rug_ratio,
        discovery.thin_market_min_volume_sol,
        discovery.thin_market_min_unique_traders,
        shadow.quality_gates_enabled,
        shadow.min_token_age_seconds,
        shadow.min_holders,
        shadow.min_liquidity_sol,
        shadow.min_volume_5m_sol,
        shadow.min_unique_traders_5m,
        options.execution_enabled,
        TOKEN_QUALITY_TTL_SECONDS,
        TOKEN_ROLLING_MARKET_WINDOW_SECONDS,
    )
}
