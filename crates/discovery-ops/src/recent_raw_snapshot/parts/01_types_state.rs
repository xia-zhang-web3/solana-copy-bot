#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LatestSurfaceStatus {
    NotApplicable,
    Healthy,
    MissingLatestSnapshot,
    MissingLatestMetadata,
    MissingBoth,
    InvalidLatestMetadata,
}

impl LatestSurfaceStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::NotApplicable => "not_applicable",
            Self::Healthy => "healthy",
            Self::MissingLatestSnapshot => "missing_latest_snapshot",
            Self::MissingLatestMetadata => "missing_latest_metadata",
            Self::MissingBoth => "missing_both",
            Self::InvalidLatestMetadata => "invalid_latest_metadata",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LatestSurfaceAction {
    ExplicitOutput,
    ExplicitOutputDeferred,
    HealthySkip,
    RefreshedFromSource,
    RecreatedLatestSnapshotFromArchive,
    RewroteLatestMetadataFromArchive,
    RewroteLatestMetadataFromLatestSqlite,
    RecreatedLatestSurfaceFromSource,
    DeferredDueToAttemptBudget,
    UnchangedDueToRetryableBusy,
    UnchangedDueToAttemptBudget,
    UnchangedDueToHardFailure,
}

impl LatestSurfaceAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::ExplicitOutput => "explicit_output",
            Self::ExplicitOutputDeferred => "explicit_output_deferred",
            Self::HealthySkip => "healthy_skip",
            Self::RefreshedFromSource => "refreshed_from_source",
            Self::RecreatedLatestSnapshotFromArchive => "recreated_latest_snapshot_from_archive",
            Self::RewroteLatestMetadataFromArchive => "rewrote_latest_metadata_from_archive",
            Self::RewroteLatestMetadataFromLatestSqlite => {
                "rewrote_latest_metadata_from_latest_sqlite"
            }
            Self::RecreatedLatestSurfaceFromSource => "recreated_latest_surface_from_source",
            Self::DeferredDueToAttemptBudget => "deferred_due_to_attempt_budget",
            Self::UnchangedDueToRetryableBusy => "unchanged_due_to_retryable_busy",
            Self::UnchangedDueToAttemptBudget => "unchanged_due_to_attempt_budget",
            Self::UnchangedDueToHardFailure => "unchanged_due_to_hard_failure",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotState {
    Written,
    SkippedNotDue,
    SelfHealedLatestSurface,
    RetryableBusy,
    Deferred,
    HardFailure,
}

impl SnapshotState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Written => "written",
            Self::SkippedNotDue => "skipped_not_due",
            Self::SelfHealedLatestSurface => "self_healed_latest_surface",
            Self::RetryableBusy => "retryable_busy",
            Self::Deferred => "deferred",
            Self::HardFailure => "hard_failure",
        }
    }

    fn exit_code(self) -> i32 {
        match self {
            Self::Written | Self::SkippedNotDue | Self::SelfHealedLatestSurface => 0,
            Self::RetryableBusy | Self::Deferred => 75,
            Self::HardFailure => 1,
        }
    }
}

#[derive(Debug, Clone)]
struct LatestSurfaceAssessment {
    status: LatestSurfaceStatus,
    manifest: Option<RecentRawJournalSnapshotManifest>,
}

#[derive(Debug, Clone)]
struct SnapshotSourceStats {
    source_db_bytes: u64,
    source_wal_bytes: u64,
    source_page_size_bytes: usize,
    source_page_count: usize,
}

impl SnapshotSourceStats {
    fn source_total_bytes(&self) -> u64 {
        self.source_db_bytes.saturating_add(self.source_wal_bytes)
    }
}

#[derive(Debug, Clone)]
struct SnapshotContext {
    source_stats: SnapshotSourceStats,
    policy: SqliteSnapshotPolicy,
}

#[derive(Debug, Clone)]
struct SnapshotExecution {
    rendered_output: String,
    exit_code: i32,
}
