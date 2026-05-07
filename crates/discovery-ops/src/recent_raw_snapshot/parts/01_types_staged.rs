use super::*;

#[derive(Debug, Clone, Default)]
pub(crate) struct SnapshotArchiveMaintenance {
    pub(crate) cleanup_removed_paths: Vec<PathBuf>,
    pub(crate) pruned_snapshot_paths: Vec<PathBuf>,
    pub(crate) archive_set_count_before: Option<usize>,
    pub(crate) archive_set_count_after: Option<usize>,
}

impl SnapshotArchiveMaintenance {
    pub(crate) fn record_pass(
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
pub(crate) struct SnapshotOutputContext {
    pub(crate) archive_promoted: bool,
    pub(crate) archive_maintenance: SnapshotArchiveMaintenance,
    pub(crate) staged_progress: StagedSnapshotProgress,
    pub(crate) attempt_duration_ms_override: Option<u64>,
    pub(crate) terminal_reason_override: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct StagedSnapshotProgress {
    pub(crate) staged_snapshot_path: Option<PathBuf>,
    pub(crate) staged_metadata_path: Option<PathBuf>,
    pub(crate) resumed_from_existing_stage: bool,
    pub(crate) seeded_from_latest_surface: bool,
    pub(crate) preserved_for_retry: bool,
    pub(crate) advanced_during_attempt: bool,
    pub(crate) completed_batches: usize,
    pub(crate) source_rows_loaded: usize,
    pub(crate) rows_processed_during_attempt: usize,
    pub(crate) rows_inserted_during_attempt: usize,
    pub(crate) row_count_before_attempt: Option<usize>,
    pub(crate) row_count_after_attempt: Option<usize>,
    pub(crate) covered_through_cursor_before_attempt: Option<DiscoveryRuntimeCursor>,
    pub(crate) covered_through_cursor_after_attempt: Option<DiscoveryRuntimeCursor>,
    pub(crate) terminal_phase: Option<StagedSnapshotTerminalPhase>,
    pub(crate) source_read_duration_ms: u64,
    pub(crate) staged_write_duration_ms: u64,
    pub(crate) write_batch_rows: usize,
    pub(crate) staged_write_sqlite_variable_limit: usize,
    pub(crate) staged_write_statement_params_per_row: usize,
    pub(crate) staged_write_statement_chunk_row_cap: usize,
    pub(crate) staged_write_effective_statement_chunk_rows: usize,
    pub(crate) staged_write_statement_count: usize,
    pub(crate) staged_write_rows_processed: usize,
    pub(crate) staged_write_rows_inserted: usize,
    pub(crate) staged_write_value_build_duration_ms: u64,
    pub(crate) staged_write_prepare_duration_ms: u64,
    pub(crate) staged_write_execute_duration_ms: u64,
    pub(crate) staged_write_state_refresh_duration_ms: u64,
    pub(crate) staged_write_state_upsert_duration_ms: u64,
    pub(crate) staged_write_transaction_duration_ms: u64,
    pub(crate) staged_write_deadline_exhausted_before_statement: bool,
    pub(crate) staged_write_deadline_exhausted_during_execute: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct StagedSnapshotAttempt {
    pub(crate) manifest: RecentRawJournalSnapshotManifest,
    pub(crate) progress: StagedSnapshotProgress,
    pub(crate) attempt_duration_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) enum StagedSnapshotAttemptResult {
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
pub(crate) enum SnapshotWriteError {
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
pub(crate) enum StagedSnapshotTerminalPhase {
    SourceRead,
    StagedWrite,
}

impl StagedSnapshotTerminalPhase {
    pub(crate) fn budget_reason(self) -> &'static str {
        match self {
            Self::SourceRead => "source_read_attempt_duration_budget_exhausted",
            Self::StagedWrite => "staged_write_attempt_duration_budget_exhausted",
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::SourceRead => "source_read",
            Self::StagedWrite => "staged_write",
        }
    }
}
