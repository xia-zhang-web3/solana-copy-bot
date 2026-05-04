use chrono::{DateTime, Utc};
use copybot_config::DiscoveryConfig;
use copybot_storage_core::{DiscoveryPublicationFreshnessGate, DiscoveryRuntimeArtifact};

pub(super) fn publication_freshness_gate(
    config: &DiscoveryConfig,
) -> DiscoveryPublicationFreshnessGate {
    DiscoveryPublicationFreshnessGate {
        scoring_window_days: config.scoring_window_days as i64,
        metric_snapshot_interval_seconds: config.metric_snapshot_interval_seconds,
        refresh_seconds: config.refresh_seconds,
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RuntimeArtifactFreshness {
    pub(super) fresh_under_export_gate: bool,
    pub(super) fresh_under_current_gate: bool,
}

pub(super) fn assess_runtime_artifact_freshness(
    artifact: &DiscoveryRuntimeArtifact,
    current_gate: DiscoveryPublicationFreshnessGate,
    now: DateTime<Utc>,
) -> RuntimeArtifactFreshness {
    RuntimeArtifactFreshness {
        fresh_under_export_gate: artifact
            .publication_state
            .is_fresh_under_gate(artifact.export_gate, now),
        fresh_under_current_gate: artifact
            .publication_state
            .is_fresh_under_gate(current_gate, now),
    }
}
