use crate::DiscoveryService;
use chrono::{DateTime, Utc};
use copybot_storage::{DiscoveryPublicationFreshnessGate, DiscoveryRuntimeArtifact};
use serde::Serialize;

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
}

impl DiscoveryRuntimeArtifactFreshnessAssessment {
    pub fn fresh_for_normal_restore(&self) -> bool {
        self.publication_truth_complete
            && self.fresh_under_export_gate
            && self.fresh_under_current_gate
    }
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
        DiscoveryRuntimeArtifactFreshnessAssessment {
            exported_at: artifact.exported_at,
            last_published_at: artifact.publication_state.last_published_at,
            last_published_window_start: artifact.publication_state.last_published_window_start,
            export_gate: artifact.export_gate.clone(),
            current_gate,
            publication_truth_complete: artifact.publication_state.has_complete_publication_truth(),
            fresh_under_export_gate,
            fresh_under_current_gate,
        }
    }

    pub fn validate_normal_runtime_restore_artifact(
        &self,
        artifact: &DiscoveryRuntimeArtifact,
        now: DateTime<Utc>,
    ) -> anyhow::Result<DiscoveryRuntimeArtifactFreshnessAssessment> {
        let assessment = self.assess_runtime_artifact_freshness(artifact, now);
        if !assessment.publication_truth_complete {
            anyhow::bail!("artifact publication truth is incomplete");
        }
        if !assessment.fresh_under_export_gate {
            anyhow::bail!("artifact is stale under exported runtime gate");
        }
        if !assessment.fresh_under_current_gate {
            anyhow::bail!("artifact is stale under current runtime gate");
        }
        Ok(assessment)
    }
}
