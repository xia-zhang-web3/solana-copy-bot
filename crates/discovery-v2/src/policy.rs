use chrono::{DateTime, Duration, Utc};
use copybot_config::{
    discovery_v2_policy_fingerprint as shared_discovery_v2_policy_fingerprint, DiscoveryConfig,
    DiscoveryV2PolicyFingerprintInput, ShadowConfig, DISCOVERY_V2_TOKEN_QUALITY_TTL_SECONDS,
};

pub const TOKEN_QUALITY_TTL_SECONDS: i64 = DISCOVERY_V2_TOKEN_QUALITY_TTL_SECONDS;

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
    shared_discovery_v2_policy_fingerprint(
        discovery,
        shadow,
        DiscoveryV2PolicyFingerprintInput {
            window_minutes: options.window_minutes,
            max_tail_lag_seconds: options.max_tail_lag_seconds,
            max_rows: options.max_rows,
            time_budget_ms: options.time_budget_ms,
            execution_enabled: options.execution_enabled,
        },
    )
}
