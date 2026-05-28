use chrono::DateTime;
use chrono::Utc;
use copybot_config::{
    discovery_v2_policy_fingerprint, DiscoveryConfig, DiscoveryV2PolicyFingerprintInput,
    ShadowConfig, DISCOVERY_V2_SCORING_SOURCE,
};
use copybot_storage_core::{
    DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateRow, DiscoveryRuntimeMode,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub(crate) struct DiscoveryService {
    config: DiscoveryConfig,
    shadow_quality: ShadowConfig,
}

impl DiscoveryService {
    pub(crate) fn new(config: DiscoveryConfig, shadow_quality: ShadowConfig) -> Self {
        Self {
            config,
            shadow_quality,
        }
    }

    pub(crate) fn new_with_helius(
        config: DiscoveryConfig,
        shadow_quality: ShadowConfig,
        _helius_http_url: Option<String>,
    ) -> Self {
        Self::new(config, shadow_quality)
    }

    pub(crate) fn publication_freshness_gate(&self) -> DiscoveryPublicationFreshnessGate {
        DiscoveryPublicationFreshnessGate {
            scoring_window_days: self.config.scoring_window_days as i64,
            window_minutes: Some(self.config.effective_status_scan_window_minutes()),
            metric_snapshot_interval_seconds: self.config.metric_snapshot_interval_seconds,
            refresh_seconds: self.config.refresh_seconds,
            expected_scoring_source: Some(DISCOVERY_V2_SCORING_SOURCE.to_string()),
            expected_policy_fingerprint: Some(
                self.discovery_v2_publication_policy_fingerprint(false),
            ),
        }
    }

    pub(crate) fn discovery_v2_publication_policy_fingerprint(
        &self,
        execution_enabled: bool,
    ) -> String {
        let options = DiscoveryV2PolicyFingerprintInput {
            window_minutes: self.config.effective_status_scan_window_minutes(),
            max_tail_lag_seconds: self.config.refresh_seconds.max(1),
            max_rows: self
                .config
                .max_window_swaps_in_memory
                .max(self.config.max_fetch_swaps_per_cycle)
                .max(1),
            time_budget_ms: self.config.fetch_time_budget_ms.max(1),
            execution_enabled,
        };
        discovery_v2_policy_fingerprint(&self.config, &self.shadow_quality, options)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RuntimePublishedUniverseTruth {
    pub runtime_mode: DiscoveryRuntimeMode,
    pub reason: String,
    pub last_published_at: DateTime<Utc>,
    pub last_published_window_start: DateTime<Utc>,
    pub published_scoring_source: Option<String>,
    pub published_wallet_ids: Vec<String>,
}

impl RuntimePublishedUniverseTruth {
    pub(crate) fn from_state(publication_state: DiscoveryPublicationStateRow) -> Option<Self> {
        Some(Self {
            runtime_mode: publication_state.runtime_mode,
            reason: publication_state.reason,
            last_published_at: publication_state.last_published_at?,
            last_published_window_start: publication_state.last_published_window_start?,
            published_scoring_source: publication_state.published_scoring_source,
            published_wallet_ids: publication_state
                .published_wallet_ids
                .filter(|wallet_ids| !wallet_ids.is_empty())?,
        })
    }

    pub(crate) fn active_wallets(&self) -> HashSet<String> {
        self.published_wallet_ids.iter().cloned().collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RuntimePublicationTruthResolution {
    Recent(RuntimePublishedUniverseTruth),
    BootstrapDegraded(RuntimePublishedUniverseTruth),
}
