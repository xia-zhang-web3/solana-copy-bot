use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RecentRawPromotionSnapshotManifest {
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) source_db_path: String,
    pub(crate) snapshot_path: String,
    pub(crate) row_count: usize,
    pub(crate) covered_since: Option<DateTime<Utc>>,
    pub(crate) covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub(crate) last_batch_completed_at: Option<DateTime<Utc>>,
    pub(crate) updated_at: Option<DateTime<Utc>>,
    pub(crate) snapshot_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RecentRawSurfaceRead {
    pub(crate) snapshot_present: bool,
    pub(crate) metadata_present: bool,
    pub(crate) manifest: Option<RecentRawPromotionSnapshotManifest>,
    pub(crate) manifest_error: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RecentRawStagedCandidateRead {
    pub(crate) snapshot_path: PathBuf,
    pub(crate) metadata_path: PathBuf,
    pub(crate) surface: RecentRawSurfaceRead,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RecentRawSnapshotSqliteContentRead {
    pub(crate) state: Option<RecentRawJournalStateRow>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RecentRawObservedSwapsBoundedProbeRead {
    pub(crate) covered_since: Option<DateTime<Utc>>,
    pub(crate) covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RecentRawSnapshotAttemptTelemetryArtifact {
    pub(crate) event: Option<String>,
    pub(crate) state: Option<String>,
    pub(crate) staged_progress_resumed: Option<bool>,
    pub(crate) staged_seeded_from_latest_surface: Option<bool>,
    pub(crate) staged_progress_preserved_for_retry: Option<bool>,
    pub(crate) staged_progress_advanced: Option<bool>,
    pub(crate) staged_row_count_before_attempt: Option<usize>,
    pub(crate) staged_row_count_after_attempt: Option<usize>,
    pub(crate) staged_covered_through_cursor_before_attempt: Option<DiscoveryRuntimeCursor>,
    pub(crate) staged_covered_through_cursor_after_attempt: Option<DiscoveryRuntimeCursor>,
    pub(crate) created_at: Option<DateTime<Utc>>,
    pub(crate) last_batch_completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub(crate) struct RecentRawSnapshotAttemptTelemetryRead {
    pub(crate) path: PathBuf,
    pub(crate) modified_at: Option<DateTime<Utc>>,
    pub(crate) telemetry: Option<RecentRawSnapshotAttemptTelemetryArtifact>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecentRawManifestProgressRelation {
    CandidateAhead,
    ReferenceAhead,
    Equivalent,
}

#[derive(Debug, Clone)]
pub(crate) struct RecentRawDiagnosticState {
    pub(crate) snapshot_dir: PathBuf,
    pub(crate) promoted_snapshot_path: PathBuf,
    pub(crate) promoted_metadata_path: PathBuf,
    pub(crate) staged_snapshot_path: PathBuf,
    pub(crate) staged_metadata_path: PathBuf,
    pub(crate) staged_candidates: Vec<RecentRawStagedCandidateRead>,
    pub(crate) promoted: RecentRawSurfaceRead,
    pub(crate) staged: RecentRawSurfaceRead,
    pub(crate) promoted_exists: bool,
    pub(crate) staged_exists: bool,
    pub(crate) runtime_db_path: Option<String>,
    pub(crate) runtime_db_size_bytes: Option<u64>,
    pub(crate) runtime_db_mtime: Option<String>,
    pub(crate) runtime_db_wal_present: bool,
    pub(crate) runtime_db_wal_size_bytes: Option<u64>,
    pub(crate) source_state: Option<RecentRawJournalStateRow>,
    pub(crate) source_state_available: bool,
    pub(crate) source_outruns_promoted: Option<bool>,
    pub(crate) source_outruns_staged: Option<bool>,
    pub(crate) staged_vs_promoted_relation: Option<RecentRawManifestProgressRelation>,
}
