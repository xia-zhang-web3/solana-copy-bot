use super::*;

#[derive(Debug, Clone, Default)]
pub(super) struct SnapshotArchiveMaintenance {
    pub(super) cleanup_removed_paths: Vec<PathBuf>,
    pub(super) pruned_snapshot_paths: Vec<PathBuf>,
    pub(super) archive_set_count_before: Option<usize>,
    pub(super) archive_set_count_after: Option<usize>,
}

impl SnapshotArchiveMaintenance {
    pub(super) fn record_pass(
        &mut self,
        archive_set_count_before: usize,
        archive_set_count_after: usize,
        cleanup_removed_paths: Vec<PathBuf>,
        pruned_snapshot_paths: Vec<PathBuf>,
    ) {
        if self.archive_set_count_before.is_none() {
            self.archive_set_count_before = Some(archive_set_count_before);
        }
        self.archive_set_count_after = Some(archive_set_count_after);
        self.cleanup_removed_paths.extend(cleanup_removed_paths);
        self.pruned_snapshot_paths.extend(pruned_snapshot_paths);
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct SnapshotOutputContext {
    pub(super) archive_promoted: bool,
    pub(super) archive_maintenance: SnapshotArchiveMaintenance,
    pub(super) staged_progress: StagedSnapshotProgress,
    pub(super) attempt_duration_ms_override: Option<u64>,
    pub(super) terminal_reason_override: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct StagedSnapshotProgress {
    pub(super) staged_snapshot_path: Option<PathBuf>,
    pub(super) staged_metadata_path: Option<PathBuf>,
    pub(super) resumed_from_existing_stage: bool,
    pub(super) seeded_from_latest_surface: bool,
    pub(super) preserved_for_retry: bool,
    pub(super) advanced_during_attempt: bool,
    pub(super) completed_batches: usize,
    pub(super) source_rows_loaded: usize,
    pub(super) rows_processed_during_attempt: usize,
    pub(super) rows_inserted_during_attempt: usize,
    pub(super) row_count_before_attempt: Option<usize>,
    pub(super) row_count_after_attempt: Option<usize>,
    pub(super) covered_through_cursor_before_attempt: Option<DiscoveryRuntimeCursor>,
    pub(super) covered_through_cursor_after_attempt: Option<DiscoveryRuntimeCursor>,
    pub(super) terminal_phase: Option<StagedSnapshotTerminalPhase>,
    pub(super) source_read_duration_ms: u64,
    pub(super) staged_write_duration_ms: u64,
    pub(super) write_batch_rows: usize,
    pub(super) staged_write_sqlite_variable_limit: usize,
    pub(super) staged_write_statement_params_per_row: usize,
    pub(super) staged_write_statement_chunk_row_cap: usize,
    pub(super) staged_write_effective_statement_chunk_rows: usize,
    pub(super) staged_write_statement_count: usize,
    pub(super) staged_write_rows_processed: usize,
    pub(super) staged_write_rows_inserted: usize,
    pub(super) staged_write_value_build_duration_ms: u64,
    pub(super) staged_write_prepare_duration_ms: u64,
    pub(super) staged_write_execute_duration_ms: u64,
    pub(super) staged_write_state_refresh_duration_ms: u64,
    pub(super) staged_write_state_upsert_duration_ms: u64,
    pub(super) staged_write_transaction_duration_ms: u64,
    pub(super) staged_write_deadline_exhausted_before_statement: bool,
    pub(super) staged_write_deadline_exhausted_during_execute: bool,
}

#[derive(Debug, Clone)]
pub(super) struct StagedSnapshotAttempt {
    pub(super) manifest: RecentRawJournalSnapshotManifest,
    pub(super) progress: StagedSnapshotProgress,
    pub(super) attempt_duration_ms: u64,
}

#[derive(Debug, Clone)]
pub(super) enum StagedSnapshotAttemptResult {
    Completed(StagedSnapshotAttempt),
    Deferred(StagedSnapshotAttempt),
    HardFailure {
        manifest: Option<RecentRawJournalSnapshotManifest>,
        progress: StagedSnapshotProgress,
        attempt_duration_ms: u64,
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub(super) enum SnapshotWriteError {
    RetryableBusy {
        summary: SqliteSnapshotSummary,
    },
    Deferred {
        summary: SqliteSnapshotSummary,
    },
    HardFailure {
        summary: Option<SqliteSnapshotSummary>,
        reason: String,
    },
}

#[derive(Debug, Clone, Copy)]
pub(super) enum StagedSnapshotTerminalPhase {
    SourceRead,
    StagedWrite,
}

impl StagedSnapshotTerminalPhase {
    pub(super) fn budget_reason(self) -> &'static str {
        match self {
            Self::SourceRead => "source_read_attempt_duration_budget_exhausted",
            Self::StagedWrite => "staged_write_attempt_duration_budget_exhausted",
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::SourceRead => "source_read",
            Self::StagedWrite => "staged_write",
        }
    }
}
