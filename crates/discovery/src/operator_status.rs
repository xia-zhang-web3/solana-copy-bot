use crate::{
    AggregateReadinessStatus, DiscoveryService, RuntimePublicationTruthResolution,
    RuntimePublishedUniverseTruth,
};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage::{
    DiscoveryBootstrapDegradedStateRow, DiscoveryPersistedRebuildStateRow,
    DiscoveryRecentRawRestoreStateRow, DiscoveryRuntimeCursor, DiscoveryRuntimeMode, SqliteStore,
};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DiscoveryOperatorRuntimeState {
    HealthyRuntimeTruth,
    DegradedRecentPublicationTruth,
    BootstrapDegradedPublicationTruth,
    FailClosedRebuildInProgress,
    FailClosedNoRecentPublishedUniverse,
}

impl DiscoveryOperatorRuntimeState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HealthyRuntimeTruth => "healthy_runtime_truth",
            Self::DegradedRecentPublicationTruth => "degraded_recent_publication_truth",
            Self::BootstrapDegradedPublicationTruth => "bootstrap_degraded_publication_truth",
            Self::FailClosedRebuildInProgress => "fail_closed_rebuild_in_progress",
            Self::FailClosedNoRecentPublishedUniverse => "fail_closed_no_recent_published_universe",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryOperatorCursor {
    pub ts_utc: DateTime<Utc>,
    pub slot: u64,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryOperatorPublicationStatus {
    pub runtime_mode: Option<String>,
    pub reason: Option<String>,
    pub latest_publication_ts: Option<DateTime<Utc>>,
    pub publication_age_seconds: Option<u64>,
    pub latest_publication_window_start: Option<DateTime<Utc>>,
    pub published_scoring_source: Option<String>,
    pub recent_publication_truth_available: bool,
    pub bootstrap_degraded_publication_truth_available: bool,
    pub bootstrap_degraded_active: bool,
    pub bootstrap_degraded_reason: Option<String>,
    pub bootstrap_degraded_armed_at: Option<DateTime<Utc>>,
    pub published_wallet_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryOperatorPersistedRebuildStatus {
    pub phase: String,
    pub cursor: Option<String>,
    pub phase_cursor: Option<DiscoveryOperatorCursor>,
    pub chunks_completed: usize,
    pub prepass_rows_processed: usize,
    pub prepass_pages_processed: usize,
    pub replay_rows_processed: usize,
    pub replay_pages_processed: usize,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryOperatorOfflineRecoveryStatus {
    pub state: String,
    pub cursor: Option<DiscoveryOperatorCursor>,
    pub covered_through_cursor: Option<DiscoveryOperatorCursor>,
    pub covered_since: Option<DateTime<Utc>>,
    pub protected_since: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryOperatorRecentRawRestoreStatus {
    pub journal_available: bool,
    pub journal_replayed: bool,
    pub required_window_start: Option<DateTime<Utc>>,
    pub journal_covered_since: Option<DateTime<Utc>>,
    pub journal_covered_through_cursor: Option<DiscoveryOperatorCursor>,
    pub gap_fill_replayed: bool,
    pub gap_fill_covered_since: Option<DateTime<Utc>>,
    pub gap_fill_covered_through_cursor: Option<DiscoveryOperatorCursor>,
    pub effective_covered_since: Option<DateTime<Utc>>,
    pub effective_covered_through_cursor: Option<DiscoveryOperatorCursor>,
    pub artifact_runtime_cursor: Option<DiscoveryOperatorCursor>,
    pub journal_covers_artifact_cursor: bool,
    pub raw_coverage_satisfied: bool,
    pub gap_fill_replayed_rows: usize,
    pub replayed_rows: usize,
    pub reason: Option<String>,
    pub replay_started_at: Option<DateTime<Utc>>,
    pub replay_completed_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryOperatorStatus {
    pub now: DateTime<Utc>,
    pub window_start: DateTime<Utc>,
    pub runtime_state: String,
    pub runtime_mode: String,
    pub scoring_source: String,
    pub active_follow_wallets: usize,
    pub recent_swaps_window: usize,
    pub raw_window_state: String,
    pub publication: DiscoveryOperatorPublicationStatus,
    pub persisted_rebuild: Option<DiscoveryOperatorPersistedRebuildStatus>,
    pub recent_raw_restore: DiscoveryOperatorRecentRawRestoreStatus,
    pub offline_recovery: DiscoveryOperatorOfflineRecoveryStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawWindowState {
    Healthy,
    Unavailable,
    Incomplete,
    ShortRetention,
}

impl RawWindowState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Unavailable => "unavailable",
            Self::Incomplete => "incomplete",
            Self::ShortRetention => "short_retention",
        }
    }
}

impl DiscoveryService {
    pub fn operator_status(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<DiscoveryOperatorStatus> {
        let window_start = now - Duration::days(self.config.scoring_window_days.max(1) as i64);
        let short_retention_window = self.config.observed_swaps_retention_days.max(1)
            < self.config.scoring_window_days.max(1);
        let (recent_swaps, truncated_by_limit) = store.load_recent_observed_swaps_since(
            window_start,
            self.config.max_window_swaps_in_memory.max(1),
        )?;
        let raw_window_state = if recent_swaps.is_empty() {
            RawWindowState::Unavailable
        } else if short_retention_window {
            RawWindowState::ShortRetention
        } else if truncated_by_limit {
            RawWindowState::Incomplete
        } else {
            RawWindowState::Healthy
        };

        let publication_state = store.discovery_publication_state_read_only()?;
        let bootstrap_degraded_state = store.discovery_bootstrap_degraded_state_read_only()?;
        let publication_truth_resolution = self.runtime_publication_truth_resolution(store, now)?;
        let recent_publication_truth = match publication_truth_resolution.as_ref() {
            Some(RuntimePublicationTruthResolution::Recent(truth)) => Some(truth),
            Some(RuntimePublicationTruthResolution::BootstrapDegraded(_)) | None => None,
        };
        let bootstrap_degraded_publication_truth = match publication_truth_resolution.as_ref() {
            Some(RuntimePublicationTruthResolution::BootstrapDegraded(truth)) => Some(truth),
            Some(RuntimePublicationTruthResolution::Recent(_)) | None => None,
        };
        let persisted_rebuild = store
            .load_discovery_persisted_rebuild_state_read_only()?
            .map(|row| self.operator_persisted_rebuild_status(row))
            .transpose()?;
        let recent_raw_restore = store.discovery_recent_raw_restore_state_read_only()?;
        let aggregate_status = self.aggregate_readiness_status(store, now)?;
        let active_follow_wallets = store.list_active_follow_wallets()?.len();

        let runtime_state = classify_runtime_state(
            raw_window_state,
            recent_publication_truth,
            bootstrap_degraded_publication_truth,
            persisted_rebuild.as_ref(),
        );
        let runtime_mode = match runtime_state {
            DiscoveryOperatorRuntimeState::HealthyRuntimeTruth => DiscoveryRuntimeMode::Healthy,
            DiscoveryOperatorRuntimeState::DegradedRecentPublicationTruth => {
                DiscoveryRuntimeMode::Degraded
            }
            DiscoveryOperatorRuntimeState::BootstrapDegradedPublicationTruth => {
                DiscoveryRuntimeMode::BootstrapDegraded
            }
            DiscoveryOperatorRuntimeState::FailClosedRebuildInProgress
            | DiscoveryOperatorRuntimeState::FailClosedNoRecentPublishedUniverse => {
                DiscoveryRuntimeMode::FailClosed
            }
        };

        Ok(DiscoveryOperatorStatus {
            now,
            window_start,
            runtime_state: runtime_state.as_str().to_string(),
            runtime_mode: runtime_mode.as_str().to_string(),
            scoring_source: inferred_scoring_source(
                raw_window_state,
                recent_publication_truth.is_some(),
                bootstrap_degraded_publication_truth.is_some(),
            )
            .to_string(),
            active_follow_wallets,
            recent_swaps_window: recent_swaps.len(),
            raw_window_state: raw_window_state.as_str().to_string(),
            publication: operator_publication_status(
                publication_state.as_ref(),
                recent_publication_truth,
                bootstrap_degraded_publication_truth,
                &bootstrap_degraded_state,
                now,
            ),
            persisted_rebuild,
            recent_raw_restore: operator_recent_raw_restore_status(&recent_raw_restore),
            offline_recovery: operator_offline_recovery_status(&aggregate_status),
        })
    }

    fn operator_persisted_rebuild_status(
        &self,
        row: DiscoveryPersistedRebuildStateRow,
    ) -> Result<DiscoveryOperatorPersistedRebuildStatus> {
        let state = Self::persisted_stream_rebuild_state_from_row(row)?;
        Ok(DiscoveryOperatorPersistedRebuildStatus {
            phase: state.phase.as_str().to_string(),
            cursor: bounded_rebuild_cursor(&state),
            phase_cursor: state.phase_cursor.as_ref().map(operator_cursor),
            chunks_completed: state.chunks_completed,
            prepass_rows_processed: state.prepass_rows_processed,
            prepass_pages_processed: state.prepass_pages_processed,
            replay_rows_processed: state.replay_rows_processed,
            replay_pages_processed: state.replay_pages_processed,
            updated_at: state.updated_at,
        })
    }
}

fn classify_runtime_state(
    raw_window_state: RawWindowState,
    recent_publication_truth: Option<&RuntimePublishedUniverseTruth>,
    bootstrap_degraded_publication_truth: Option<&RuntimePublishedUniverseTruth>,
    persisted_rebuild: Option<&DiscoveryOperatorPersistedRebuildStatus>,
) -> DiscoveryOperatorRuntimeState {
    if bootstrap_degraded_publication_truth.is_some() {
        return DiscoveryOperatorRuntimeState::BootstrapDegradedPublicationTruth;
    }
    if persisted_rebuild.is_some() {
        return if recent_publication_truth.is_some() {
            DiscoveryOperatorRuntimeState::DegradedRecentPublicationTruth
        } else {
            DiscoveryOperatorRuntimeState::FailClosedRebuildInProgress
        };
    }
    if raw_window_state == RawWindowState::Healthy {
        return DiscoveryOperatorRuntimeState::HealthyRuntimeTruth;
    }
    if recent_publication_truth.is_some() {
        DiscoveryOperatorRuntimeState::DegradedRecentPublicationTruth
    } else {
        DiscoveryOperatorRuntimeState::FailClosedNoRecentPublishedUniverse
    }
}

fn inferred_scoring_source(
    raw_window_state: RawWindowState,
    recent_publication_truth_available: bool,
    bootstrap_degraded_publication_truth_available: bool,
) -> &'static str {
    match (
        raw_window_state,
        recent_publication_truth_available,
        bootstrap_degraded_publication_truth_available,
    ) {
        (RawWindowState::Healthy, _, false) => "raw_window",
        (RawWindowState::Healthy, false, true) => {
            "bootstrap_degraded_publication_truth_raw_window_recovered_pending_healthy_refresh"
        }
        (RawWindowState::Unavailable, true, false) => "published_universe_raw_window_unavailable",
        (RawWindowState::Unavailable, false, true) => {
            "bootstrap_degraded_publication_truth_raw_window_unavailable"
        }
        (RawWindowState::Unavailable, false, false) => {
            "raw_window_unusable_no_recent_published_universe"
        }
        (RawWindowState::Incomplete, true, false) => "published_universe_raw_window_degraded",
        (RawWindowState::Incomplete, false, true) => {
            "bootstrap_degraded_publication_truth_raw_window_degraded"
        }
        (RawWindowState::Incomplete, false, false) => {
            "raw_window_incomplete_no_recent_published_universe"
        }
        (RawWindowState::ShortRetention, true, false) => {
            "published_universe_short_retention_degraded"
        }
        (RawWindowState::ShortRetention, false, true) => {
            "bootstrap_degraded_publication_truth_short_retention_degraded"
        }
        (RawWindowState::ShortRetention, false, false) => {
            "raw_window_short_retention_no_recent_published_universe"
        }
        (_, true, true) => "raw_window",
    }
}

fn operator_publication_status(
    publication_state: Option<&copybot_storage::DiscoveryPublicationStateRow>,
    recent_publication_truth: Option<&RuntimePublishedUniverseTruth>,
    bootstrap_degraded_publication_truth: Option<&RuntimePublishedUniverseTruth>,
    bootstrap_degraded_state: &DiscoveryBootstrapDegradedStateRow,
    now: DateTime<Utc>,
) -> DiscoveryOperatorPublicationStatus {
    let latest_publication_ts = publication_state.and_then(|state| state.last_published_at);
    let publication_age_seconds = latest_publication_ts
        .map(|published_at| now.signed_duration_since(published_at).num_seconds().max(0) as u64);
    DiscoveryOperatorPublicationStatus {
        runtime_mode: publication_state.map(|state| state.runtime_mode.as_str().to_string()),
        reason: publication_state.map(|state| state.reason.clone()),
        latest_publication_ts,
        publication_age_seconds,
        latest_publication_window_start: publication_state
            .and_then(|state| state.last_published_window_start),
        published_scoring_source: publication_state
            .and_then(|state| state.published_scoring_source.clone()),
        recent_publication_truth_available: recent_publication_truth.is_some(),
        bootstrap_degraded_publication_truth_available: bootstrap_degraded_publication_truth
            .is_some(),
        bootstrap_degraded_active: bootstrap_degraded_state.active,
        bootstrap_degraded_reason: bootstrap_degraded_state.reason.clone(),
        bootstrap_degraded_armed_at: bootstrap_degraded_state.armed_at,
        published_wallet_count: publication_state
            .and_then(|state| state.published_wallet_ids.as_ref().map(Vec::len))
            .unwrap_or(0),
    }
}

fn operator_offline_recovery_status(
    aggregate_status: &AggregateReadinessStatus,
) -> DiscoveryOperatorOfflineRecoveryStatus {
    let state = if aggregate_status.backfill_progress.is_some() && aggregate_status.backfill_active
    {
        "backfill_in_progress"
    } else if aggregate_status.backfill_resume_required {
        "backfill_resume_required"
    } else if aggregate_status.covered_through_cursor.is_some() {
        "idle_with_covered_through"
    } else {
        "idle"
    };
    DiscoveryOperatorOfflineRecoveryStatus {
        state: state.to_string(),
        cursor: aggregate_status
            .backfill_progress
            .as_ref()
            .map(|progress| operator_cursor(&progress.cursor)),
        covered_through_cursor: aggregate_status
            .covered_through_cursor
            .as_ref()
            .map(operator_cursor),
        covered_since: aggregate_status.covered_since,
        protected_since: aggregate_status.backfill_protected_since,
    }
}

fn operator_recent_raw_restore_status(
    row: &DiscoveryRecentRawRestoreStateRow,
) -> DiscoveryOperatorRecentRawRestoreStatus {
    DiscoveryOperatorRecentRawRestoreStatus {
        journal_available: row.journal_available,
        journal_replayed: row.journal_replayed,
        required_window_start: row.required_window_start,
        journal_covered_since: row.journal_covered_since,
        journal_covered_through_cursor: row
            .journal_covered_through_cursor
            .as_ref()
            .map(operator_cursor),
        gap_fill_replayed: row.gap_fill_replayed,
        gap_fill_covered_since: row.gap_fill_covered_since,
        gap_fill_covered_through_cursor: row
            .gap_fill_covered_through_cursor
            .as_ref()
            .map(operator_cursor),
        effective_covered_since: row.effective_covered_since,
        effective_covered_through_cursor: row
            .effective_covered_through_cursor
            .as_ref()
            .map(operator_cursor),
        artifact_runtime_cursor: row.artifact_runtime_cursor.as_ref().map(operator_cursor),
        journal_covers_artifact_cursor: row.journal_covers_artifact_cursor,
        raw_coverage_satisfied: row.raw_coverage_satisfied,
        gap_fill_replayed_rows: row.gap_fill_replayed_rows,
        replayed_rows: row.replayed_rows,
        reason: row.reason.clone(),
        replay_started_at: row.replay_started_at,
        replay_completed_at: row.replay_completed_at,
        updated_at: row.updated_at,
    }
}

fn operator_cursor(cursor: &DiscoveryRuntimeCursor) -> DiscoveryOperatorCursor {
    DiscoveryOperatorCursor {
        ts_utc: cursor.ts_utc,
        slot: cursor.slot,
        signature: cursor.signature.clone(),
    }
}

fn bounded_rebuild_cursor(state: &crate::PersistedStreamRebuildState) -> Option<String> {
    if let Some(cursor) = state.phase_cursor.as_ref() {
        return Some(format!(
            "phase_cursor ts_utc={} slot={} signature={}",
            cursor.ts_utc.to_rfc3339(),
            cursor.slot,
            cursor.signature
        ));
    }
    if let Some(wallet_cursor) = state.payload.replay_wallet_stats_wallet_cursor.as_ref() {
        return Some(format!("wallet_stats_wallet_cursor {}", wallet_cursor));
    }
    if let Some(cursor_token) = state
        .payload
        .collect_buy_mints_reconcile_new_tail_cursor_token
        .as_ref()
    {
        return Some(format!("collect_buy_mints_new_tail_token {}", cursor_token));
    }
    if let Some(cursor_token) = state
        .payload
        .collect_buy_mints_reconcile_expired_head_cursor_token
        .as_ref()
    {
        return Some(format!(
            "collect_buy_mints_expired_head_token {}",
            cursor_token
        ));
    }
    state
        .payload
        .collect_buy_mints_cursor_token
        .as_ref()
        .map(|cursor_token| format!("collect_buy_mints_token {}", cursor_token))
}
