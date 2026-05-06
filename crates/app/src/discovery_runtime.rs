use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_discovery_v2::{
    discovery_v2_policy_fingerprint, DiscoveryV2BuildOptions, DISCOVERY_V2_SCORING_SOURCE,
};
use copybot_storage_core::{
    DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateRow, DiscoveryRuntimeMode,
    SqliteDiscoveryStore,
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
        let options =
            DiscoveryV2BuildOptions::from_config(&self.config, execution_enabled, Utc::now());
        discovery_v2_policy_fingerprint(&self.config, &self.shadow_quality, &options)
    }

    pub(crate) fn runtime_publication_truth_resolution(
        &self,
        store: &SqliteDiscoveryStore,
        now: DateTime<Utc>,
    ) -> Result<Option<RuntimePublicationTruthResolution>> {
        let Some(publication_state) = store.discovery_publication_state_read_only()? else {
            return Ok(None);
        };
        if publication_state.runtime_mode != DiscoveryRuntimeMode::Healthy {
            return Ok(None);
        }
        let Some(runtime_truth) =
            RuntimePublishedUniverseTruth::from_state(publication_state.clone())
        else {
            return Ok(None);
        };
        if publication_state.is_fresh_under_gate(&self.publication_freshness_gate(), now) {
            return Ok(Some(RuntimePublicationTruthResolution::Recent(
                runtime_truth,
            )));
        }
        Ok(None)
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
    fn from_state(publication_state: DiscoveryPublicationStateRow) -> Option<Self> {
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
