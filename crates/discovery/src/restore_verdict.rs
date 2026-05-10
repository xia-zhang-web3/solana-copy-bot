use crate::DiscoveryService;
use chrono::{DateTime, Utc};
use copybot_storage::{
    DiscoveryPublicationFreshnessGate, DiscoveryRuntimeArtifact, DiscoveryRuntimeMode,
    DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
};
use serde::Serialize;
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryRuntimeArtifactFreshnessAssessment {
    pub exported_at: DateTime<Utc>,
    pub last_published_at: Option<DateTime<Utc>>,
    pub last_published_window_start: Option<DateTime<Utc>>,
    pub export_gate: DiscoveryPublicationFreshnessGate,
    pub current_gate: DiscoveryPublicationFreshnessGate,
    pub publication_truth_complete: bool,
    pub fresh_under_export_gate: bool,
    pub fresh_under_current_gate: bool,
    pub fresh_runtime_cursor_under_export_gate: bool,
    pub fresh_runtime_cursor_under_current_gate: bool,
}

impl DiscoveryService {
    pub fn assess_runtime_artifact_freshness(
        &self,
        artifact: &DiscoveryRuntimeArtifact,
        now: DateTime<Utc>,
    ) -> DiscoveryRuntimeArtifactFreshnessAssessment {
        let current_gate = self.publication_freshness_gate();
        let fresh_under_export_gate = artifact
            .publication_state
            .is_fresh_under_gate(&artifact.export_gate, now);
        let fresh_under_current_gate = artifact
            .publication_state
            .is_fresh_under_gate(&current_gate, now);
        let fresh_runtime_cursor_under_export_gate = artifact
            .publication_state
            .has_fresh_publication_runtime_cursor_under_gate(&artifact.export_gate, now);
        let fresh_runtime_cursor_under_current_gate = artifact
            .publication_state
            .has_fresh_publication_runtime_cursor_under_gate(&current_gate, now);
        DiscoveryRuntimeArtifactFreshnessAssessment {
            exported_at: artifact.exported_at,
            last_published_at: artifact.publication_state.last_published_at,
            last_published_window_start: artifact.publication_state.last_published_window_start,
            export_gate: artifact.export_gate.clone(),
            current_gate,
            publication_truth_complete: artifact.publication_state.has_complete_publication_truth(),
            fresh_under_export_gate,
            fresh_under_current_gate,
            fresh_runtime_cursor_under_export_gate,
            fresh_runtime_cursor_under_current_gate,
        }
    }

    /// Validation contract for a future non-quarantined runtime restore lane.
    ///
    /// The legacy `copybot-storage` restore entrypoint is intentionally still
    /// quarantined before mutation, so this validator is exercised by tests as
    /// the restore contract until a dedicated V2 restore operator is wired.
    pub fn validate_normal_runtime_restore_artifact(
        &self,
        artifact: &DiscoveryRuntimeArtifact,
        now: DateTime<Utc>,
    ) -> anyhow::Result<DiscoveryRuntimeArtifactFreshnessAssessment> {
        let assessment = self.assess_runtime_artifact_freshness(artifact, now);
        if artifact.exported_at > now {
            anyhow::bail!("artifact exported_at is future-dated");
        }
        if !assessment.publication_truth_complete {
            anyhow::bail!("artifact publication truth is incomplete");
        }
        if !assessment.fresh_under_export_gate {
            anyhow::bail!("artifact is stale under exported runtime gate");
        }
        if !assessment.fresh_under_current_gate {
            anyhow::bail!("artifact is stale under current runtime gate");
        }
        if !assessment.fresh_runtime_cursor_under_export_gate {
            anyhow::bail!("artifact runtime cursor is stale under exported runtime gate");
        }
        if !assessment.fresh_runtime_cursor_under_current_gate {
            anyhow::bail!("artifact runtime cursor is stale under current runtime gate");
        }
        validate_normal_runtime_restore_identity(artifact, &assessment)?;
        validate_normal_runtime_restore_snapshot_shape(artifact)?;
        Ok(assessment)
    }
}

fn validate_normal_runtime_restore_identity(
    artifact: &DiscoveryRuntimeArtifact,
    assessment: &DiscoveryRuntimeArtifactFreshnessAssessment,
) -> anyhow::Result<()> {
    if artifact.publication_state.runtime_mode != DiscoveryRuntimeMode::Healthy {
        anyhow::bail!("artifact restore requires healthy discovery runtime publication");
    }
    if artifact.export_gate.expected_scoring_source.is_none()
        || artifact.export_gate.expected_policy_fingerprint.is_none()
        || assessment.current_gate.expected_scoring_source.is_none()
        || assessment.current_gate.expected_policy_fingerprint.is_none()
    {
        anyhow::bail!("artifact restore requires explicit expected publication identity");
    }
    if artifact
        .publication_state
        .published_scoring_source
        .is_none()
        || artifact
            .publication_state
            .publication_policy_fingerprint
            .is_none()
    {
        anyhow::bail!("artifact restore requires complete stored publication identity");
    }
    if !artifact
        .publication_state
        .matches_expected_publication_identity(&artifact.export_gate)
    {
        anyhow::bail!("artifact publication identity does not match export gate");
    }
    if !artifact
        .publication_state
        .matches_expected_publication_identity(&assessment.current_gate)
    {
        anyhow::bail!("artifact publication identity does not match current runtime gate");
    }
    if !artifact
        .publication_state
        .matches_publication_runtime_cursor(&artifact.runtime_cursor)
    {
        anyhow::bail!("artifact publication cursor is not bound to runtime cursor");
    }
    Ok(())
}

fn validate_normal_runtime_restore_snapshot_shape(
    artifact: &DiscoveryRuntimeArtifact,
) -> anyhow::Result<()> {
    if artifact.format_version != DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION {
        anyhow::bail!(
            "unsupported discovery runtime artifact format_version={} expected={}",
            artifact.format_version,
            DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION
        );
    }
    let published_window_start = artifact
        .publication_state
        .last_published_window_start
        .expect("validated complete publication truth before snapshot shape");
    if artifact.published_wallet_metrics_snapshot.is_empty() {
        anyhow::bail!("artifact is missing published wallet_metrics snapshot rows");
    }
    if artifact
        .published_wallet_metrics_snapshot
        .iter()
        .any(|row| row.window_start != published_window_start)
    {
        anyhow::bail!(
            "artifact wallet_metrics snapshot rows do not match publication window_start={}",
            published_window_start.to_rfc3339()
        );
    }
    let snapshot_wallet_ids: Vec<String> = artifact
        .published_wallet_metrics_snapshot
        .iter()
        .map(|row| row.wallet_id.clone())
        .collect();
    let snapshot_unique_wallet_ids: HashSet<String> = snapshot_wallet_ids.iter().cloned().collect();
    if snapshot_unique_wallet_ids.len() != snapshot_wallet_ids.len() {
        anyhow::bail!("artifact wallet_metrics snapshot contains duplicate wallet rows");
    }
    let published_wallet_ids = artifact
        .publication_state
        .published_wallet_ids
        .as_ref()
        .expect("validated complete publication truth before snapshot shape");
    let published_unique_wallet_ids: HashSet<String> =
        published_wallet_ids.iter().cloned().collect();
    if published_unique_wallet_ids.len() != published_wallet_ids.len() {
        anyhow::bail!("artifact publication truth contains duplicate published wallet ids");
    }
    if snapshot_wallet_ids.len() != published_wallet_ids.len()
        || snapshot_unique_wallet_ids != published_unique_wallet_ids
    {
        anyhow::bail!("artifact wallet_metrics snapshot must exactly match published wallet ids");
    }
    Ok(())
}
