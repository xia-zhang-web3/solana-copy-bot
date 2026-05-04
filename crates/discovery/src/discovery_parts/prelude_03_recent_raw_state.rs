#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RecentRawPromotionSnapshotManifest {
    created_at: DateTime<Utc>,
    source_db_path: String,
    snapshot_path: String,
    row_count: usize,
    covered_since: Option<DateTime<Utc>>,
    covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    last_batch_completed_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    snapshot_bytes: u64,
}

#[derive(Debug, Clone, Default)]
struct RecentRawSurfaceRead {
    snapshot_present: bool,
    metadata_present: bool,
    manifest: Option<RecentRawPromotionSnapshotManifest>,
    manifest_error: Option<String>,
}

#[derive(Debug, Clone)]
struct RecentRawStagedCandidateRead {
    snapshot_path: PathBuf,
    metadata_path: PathBuf,
    surface: RecentRawSurfaceRead,
}

#[derive(Debug, Clone, Default)]
struct RecentRawSnapshotSqliteContentRead {
    state: Option<RecentRawJournalStateRow>,
    error: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct RecentRawObservedSwapsBoundedProbeRead {
    covered_since: Option<DateTime<Utc>>,
    covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RecentRawSnapshotAttemptTelemetryArtifact {
    event: Option<String>,
    state: Option<String>,
    staged_progress_resumed: Option<bool>,
    staged_seeded_from_latest_surface: Option<bool>,
    staged_progress_preserved_for_retry: Option<bool>,
    staged_progress_advanced: Option<bool>,
    staged_row_count_before_attempt: Option<usize>,
    staged_row_count_after_attempt: Option<usize>,
    staged_covered_through_cursor_before_attempt: Option<DiscoveryRuntimeCursor>,
    staged_covered_through_cursor_after_attempt: Option<DiscoveryRuntimeCursor>,
    created_at: Option<DateTime<Utc>>,
    last_batch_completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct RecentRawSnapshotAttemptTelemetryRead {
    path: PathBuf,
    modified_at: Option<DateTime<Utc>>,
    telemetry: Option<RecentRawSnapshotAttemptTelemetryArtifact>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecentRawManifestProgressRelation {
    CandidateAhead,
    ReferenceAhead,
    Equivalent,
}

#[derive(Debug, Clone)]
struct RecentRawDiagnosticState {
    snapshot_dir: PathBuf,
    promoted_snapshot_path: PathBuf,
    promoted_metadata_path: PathBuf,
    staged_snapshot_path: PathBuf,
    staged_metadata_path: PathBuf,
    staged_candidates: Vec<RecentRawStagedCandidateRead>,
    promoted: RecentRawSurfaceRead,
    staged: RecentRawSurfaceRead,
    promoted_exists: bool,
    staged_exists: bool,
    runtime_db_path: Option<String>,
    runtime_db_size_bytes: Option<u64>,
    runtime_db_mtime: Option<String>,
    runtime_db_wal_present: bool,
    runtime_db_wal_size_bytes: Option<u64>,
    source_state: Option<RecentRawJournalStateRow>,
    source_state_available: bool,
    source_outruns_promoted: Option<bool>,
    source_outruns_staged: Option<bool>,
    staged_vs_promoted_relation: Option<RecentRawManifestProgressRelation>,
}
