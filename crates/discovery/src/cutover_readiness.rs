use crate::{
    operator_status::{DiscoveryOperatorCursor, DiscoveryOperatorStatus},
    AggregateReadinessStatus, DiscoveryService,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage::SqliteStore;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryCutoverRuntimeTruthFacts {
    pub runtime_state: String,
    pub runtime_mode: String,
    pub scoring_source: String,
    pub active_follow_wallets: usize,
    pub raw_window_state: String,
    pub false_healthy_detected: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryCutoverPublicationTruthFacts {
    pub runtime_mode: Option<String>,
    pub latest_publication_ts: Option<DateTime<Utc>>,
    pub publication_age_seconds: Option<u64>,
    pub recent_publication_truth_available: bool,
    pub published_wallet_count: usize,
    pub published_scoring_source: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryCutoverOfflineRecoveryFacts {
    pub state: String,
    pub cursor: Option<DiscoveryOperatorCursor>,
    pub covered_through_cursor: Option<DiscoveryOperatorCursor>,
    pub effective_reads_ready: bool,
    pub read_blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryCutoverReadiness {
    pub now: DateTime<Utc>,
    pub verdict: String,
    pub blockers: Vec<String>,
    pub runtime_truth: DiscoveryCutoverRuntimeTruthFacts,
    pub publication_truth: DiscoveryCutoverPublicationTruthFacts,
    pub offline_recovery: DiscoveryCutoverOfflineRecoveryFacts,
}

impl DiscoveryService {
    pub fn cutover_readiness(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<DiscoveryCutoverReadiness> {
        let operator_status = self.operator_status(store, now)?;
        let aggregate_status = self.aggregate_readiness_status(store, now)?;
        Ok(build_cutover_readiness(
            now,
            &operator_status,
            &aggregate_status,
        ))
    }
}

fn build_cutover_readiness(
    now: DateTime<Utc>,
    operator_status: &DiscoveryOperatorStatus,
    aggregate_status: &AggregateReadinessStatus,
) -> DiscoveryCutoverReadiness {
    let false_healthy_detected = detect_false_healthy(operator_status);
    let mut blockers = Vec::new();

    if operator_status.runtime_state != "healthy_runtime_truth" {
        push_unique(&mut blockers, "runtime_truth_not_healthy".to_string());
    }
    if operator_status.scoring_source != "raw_window" {
        push_unique(
            &mut blockers,
            "runtime_scoring_source_not_raw_window".to_string(),
        );
    }
    if operator_status.active_follow_wallets == 0 {
        push_unique(&mut blockers, "active_follow_wallets_empty".to_string());
    }
    if operator_status.publication.runtime_mode.as_deref() != Some("healthy") {
        push_unique(&mut blockers, "publication_truth_not_healthy".to_string());
    }
    if !operator_status
        .publication
        .recent_publication_truth_available
    {
        push_unique(
            &mut blockers,
            "recent_publication_truth_unavailable".to_string(),
        );
    }
    if operator_status.publication.published_wallet_count == 0 {
        push_unique(&mut blockers, "published_wallets_empty".to_string());
    }
    if operator_status
        .publication
        .published_scoring_source
        .as_deref()
        != Some("raw_window")
    {
        push_unique(
            &mut blockers,
            "publication_scoring_source_not_raw_window".to_string(),
        );
    }
    if false_healthy_detected {
        push_unique(&mut blockers, "false_healthy_detected".to_string());
    }
    for blocker in &aggregate_status.read_blockers {
        push_unique(
            &mut blockers,
            format!("offline_recovery_{}", blocker.as_str()),
        );
    }

    DiscoveryCutoverReadiness {
        now,
        verdict: if blockers.is_empty() {
            "ready".to_string()
        } else {
            "not_ready".to_string()
        },
        blockers,
        runtime_truth: DiscoveryCutoverRuntimeTruthFacts {
            runtime_state: operator_status.runtime_state.clone(),
            runtime_mode: operator_status.runtime_mode.clone(),
            scoring_source: operator_status.scoring_source.clone(),
            active_follow_wallets: operator_status.active_follow_wallets,
            raw_window_state: operator_status.raw_window_state.clone(),
            false_healthy_detected,
        },
        publication_truth: DiscoveryCutoverPublicationTruthFacts {
            runtime_mode: operator_status.publication.runtime_mode.clone(),
            latest_publication_ts: operator_status.publication.latest_publication_ts,
            publication_age_seconds: operator_status.publication.publication_age_seconds,
            recent_publication_truth_available: operator_status
                .publication
                .recent_publication_truth_available,
            published_wallet_count: operator_status.publication.published_wallet_count,
            published_scoring_source: operator_status.publication.published_scoring_source.clone(),
        },
        offline_recovery: DiscoveryCutoverOfflineRecoveryFacts {
            state: operator_status.offline_recovery.state.clone(),
            cursor: operator_status.offline_recovery.cursor.clone(),
            covered_through_cursor: operator_status
                .offline_recovery
                .covered_through_cursor
                .clone(),
            effective_reads_ready: aggregate_status.effective_reads_ready,
            read_blockers: aggregate_status
                .read_blockers
                .iter()
                .map(|blocker| blocker.as_str().to_string())
                .collect(),
        },
    }
}

fn detect_false_healthy(operator_status: &DiscoveryOperatorStatus) -> bool {
    operator_status.runtime_mode == "healthy"
        && (operator_status.runtime_state != "healthy_runtime_truth"
            || operator_status.scoring_source != "raw_window"
            || operator_status.active_follow_wallets == 0
            || operator_status.publication.runtime_mode.as_deref() != Some("healthy")
            || !operator_status
                .publication
                .recent_publication_truth_available
            || operator_status.publication.published_wallet_count == 0
            || operator_status
                .publication
                .published_scoring_source
                .as_deref()
                != Some("raw_window"))
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}
