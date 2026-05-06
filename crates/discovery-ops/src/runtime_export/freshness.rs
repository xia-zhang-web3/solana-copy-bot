use chrono::{DateTime, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_discovery_v2::{
    discovery_v2_policy_fingerprint, DiscoveryV2BuildOptions, DISCOVERY_V2_SCORING_SOURCE,
};
use copybot_storage_core::{DiscoveryPublicationFreshnessGate, DiscoveryRuntimeArtifact};

pub(super) fn publication_freshness_gate(
    config: &DiscoveryConfig,
    shadow: &ShadowConfig,
    execution_enabled: bool,
    now: DateTime<Utc>,
) -> DiscoveryPublicationFreshnessGate {
    let options = DiscoveryV2BuildOptions::from_config(config, execution_enabled, now);
    DiscoveryPublicationFreshnessGate {
        scoring_window_days: config.scoring_window_days as i64,
        metric_snapshot_interval_seconds: config.metric_snapshot_interval_seconds,
        refresh_seconds: config.refresh_seconds,
        expected_scoring_source: Some(DISCOVERY_V2_SCORING_SOURCE.to_string()),
        expected_policy_fingerprint: Some(discovery_v2_policy_fingerprint(
            config, shadow, &options,
        )),
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RuntimeArtifactFreshness {
    pub(super) fresh_under_export_gate: bool,
    pub(super) fresh_under_current_gate: bool,
}

pub(super) fn assess_runtime_artifact_freshness(
    artifact: &DiscoveryRuntimeArtifact,
    current_gate: &DiscoveryPublicationFreshnessGate,
    now: DateTime<Utc>,
) -> RuntimeArtifactFreshness {
    RuntimeArtifactFreshness {
        fresh_under_export_gate: artifact
            .publication_state
            .is_fresh_under_gate(&artifact.export_gate, now)
            && artifact
                .publication_state
                .matches_expected_publication_identity(&artifact.export_gate),
        fresh_under_current_gate: artifact
            .publication_state
            .is_fresh_under_gate(current_gate, now)
            && artifact
                .publication_state
                .matches_expected_publication_identity(current_gate),
    }
}
