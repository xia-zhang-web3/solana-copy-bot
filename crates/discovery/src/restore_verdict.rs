use crate::operator_status::DiscoveryOperatorStatus;
use crate::DiscoveryService;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage::{DiscoveryPublicationFreshnessGate, DiscoveryRuntimeArtifact, SqliteStore};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DiscoveryRuntimeRestoreVerdictKind {
    TradingReady,
    BootstrapDegraded,
    FailClosed,
}

impl DiscoveryRuntimeRestoreVerdictKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TradingReady => "trading_ready",
            Self::BootstrapDegraded => "bootstrap_degraded",
            Self::FailClosed => "fail_closed",
        }
    }
}

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

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryRuntimeRestoreVerdict {
    pub verdict: String,
    pub runtime_state: String,
    pub runtime_mode: String,
    pub scoring_source: String,
    pub runtime_cursor_restored: bool,
    pub recent_publication_truth_available: bool,
    pub bootstrap_degraded_active: bool,
    pub bootstrap_degraded_publication_truth_available: bool,
    pub journal_available: bool,
    pub journal_replayed: bool,
    pub journal_covers_artifact_cursor: bool,
    pub raw_coverage_satisfied: bool,
    pub journal_replayed_rows: usize,
    pub active_follow_wallets: usize,
}

impl DiscoveryService {
    pub fn assess_runtime_artifact_freshness(
        &self,
        artifact: &DiscoveryRuntimeArtifact,
        now: DateTime<Utc>,
    ) -> DiscoveryRuntimeArtifactFreshnessAssessment {
        let current_gate = self.publication_freshness_gate();
        DiscoveryRuntimeArtifactFreshnessAssessment {
            exported_at: artifact.exported_at,
            last_published_at: artifact.publication_state.last_published_at,
            last_published_window_start: artifact.publication_state.last_published_window_start,
            export_gate: artifact.export_gate,
            current_gate,
            publication_truth_complete: artifact.publication_state.has_complete_publication_truth(),
            fresh_under_export_gate: artifact
                .publication_state
                .is_fresh_under_gate(artifact.export_gate, now),
            fresh_under_current_gate: artifact
                .publication_state
                .is_fresh_under_gate(current_gate, now),
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

    pub fn runtime_restore_verdict(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<DiscoveryRuntimeRestoreVerdict> {
        let status = self.operator_status(store, now)?;
        let runtime_cursor_restored = store.load_discovery_runtime_cursor()?.is_some();
        Ok(runtime_restore_verdict_from_status(
            &status,
            runtime_cursor_restored,
        ))
    }
}

pub fn runtime_restore_verdict_from_status(
    status: &DiscoveryOperatorStatus,
    runtime_cursor_restored: bool,
) -> DiscoveryRuntimeRestoreVerdict {
    let verdict = if status.runtime_state == "healthy_runtime_truth"
        && status.runtime_mode == "healthy"
        && status.scoring_source == "raw_window"
        && status.publication.recent_publication_truth_available
        && runtime_cursor_restored
        && status.recent_raw_restore.journal_available
        && status.recent_raw_restore.journal_replayed
        && status.recent_raw_restore.journal_covers_artifact_cursor
        && status.recent_raw_restore.raw_coverage_satisfied
        && status.active_follow_wallets > 0
    {
        DiscoveryRuntimeRestoreVerdictKind::TradingReady
    } else if status.runtime_mode == "bootstrap_degraded"
        && status.publication.bootstrap_degraded_active
        && status
            .publication
            .bootstrap_degraded_publication_truth_available
        && runtime_cursor_restored
    {
        DiscoveryRuntimeRestoreVerdictKind::BootstrapDegraded
    } else {
        DiscoveryRuntimeRestoreVerdictKind::FailClosed
    };
    DiscoveryRuntimeRestoreVerdict {
        verdict: verdict.as_str().to_string(),
        runtime_state: status.runtime_state.clone(),
        runtime_mode: status.runtime_mode.clone(),
        scoring_source: status.scoring_source.clone(),
        runtime_cursor_restored,
        recent_publication_truth_available: status.publication.recent_publication_truth_available,
        bootstrap_degraded_active: status.publication.bootstrap_degraded_active,
        bootstrap_degraded_publication_truth_available: status
            .publication
            .bootstrap_degraded_publication_truth_available,
        journal_available: status.recent_raw_restore.journal_available,
        journal_replayed: status.recent_raw_restore.journal_replayed,
        journal_covers_artifact_cursor: status.recent_raw_restore.journal_covers_artifact_cursor,
        raw_coverage_satisfied: status.recent_raw_restore.raw_coverage_satisfied,
        journal_replayed_rows: status.recent_raw_restore.replayed_rows,
        active_follow_wallets: status.active_follow_wallets,
    }
}
