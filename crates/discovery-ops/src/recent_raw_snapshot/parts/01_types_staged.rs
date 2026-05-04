#[derive(Debug, Clone, Default)]
struct SnapshotArchiveMaintenance {
    cleanup_removed_paths: Vec<PathBuf>,
    pruned_snapshot_paths: Vec<PathBuf>,
    archive_set_count_before: Option<usize>,
    archive_set_count_after: Option<usize>,
}

impl SnapshotArchiveMaintenance {
    fn record_pass(
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
struct SnapshotOutputContext {
    archive_promoted: bool,
    archive_maintenance: SnapshotArchiveMaintenance,
    staged_progress: StagedSnapshotProgress,
    attempt_duration_ms_override: Option<u64>,
    terminal_reason_override: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct StagedSnapshotProgress {
    staged_snapshot_path: Option<PathBuf>,
    staged_metadata_path: Option<PathBuf>,
    resumed_from_existing_stage: bool,
    seeded_from_latest_surface: bool,
    preserved_for_retry: bool,
    advanced_during_attempt: bool,
    completed_batches: usize,
    source_rows_loaded: usize,
    rows_processed_during_attempt: usize,
    rows_inserted_during_attempt: usize,
    row_count_before_attempt: Option<usize>,
    row_count_after_attempt: Option<usize>,
    covered_through_cursor_before_attempt: Option<DiscoveryRuntimeCursor>,
    covered_through_cursor_after_attempt: Option<DiscoveryRuntimeCursor>,
    terminal_phase: Option<StagedSnapshotTerminalPhase>,
    source_read_duration_ms: u64,
    staged_write_duration_ms: u64,
    write_batch_rows: usize,
    staged_write_sqlite_variable_limit: usize,
    staged_write_statement_params_per_row: usize,
    staged_write_statement_chunk_row_cap: usize,
    staged_write_effective_statement_chunk_rows: usize,
    staged_write_statement_count: usize,
    staged_write_rows_processed: usize,
    staged_write_rows_inserted: usize,
    staged_write_value_build_duration_ms: u64,
    staged_write_prepare_duration_ms: u64,
    staged_write_execute_duration_ms: u64,
    staged_write_state_refresh_duration_ms: u64,
    staged_write_state_upsert_duration_ms: u64,
    staged_write_transaction_duration_ms: u64,
    staged_write_deadline_exhausted_before_statement: bool,
    staged_write_deadline_exhausted_during_execute: bool,
}

#[derive(Debug, Clone)]
struct StagedSnapshotAttempt {
    manifest: RecentRawJournalSnapshotManifest,
    progress: StagedSnapshotProgress,
    attempt_duration_ms: u64,
}

#[derive(Debug, Clone)]
enum StagedSnapshotAttemptResult {
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
enum SnapshotWriteError {
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
enum StagedSnapshotTerminalPhase {
    SourceRead,
    StagedWrite,
}

impl StagedSnapshotTerminalPhase {
    fn budget_reason(self) -> &'static str {
        match self {
            Self::SourceRead => "source_read_attempt_duration_budget_exhausted",
            Self::StagedWrite => "staged_write_attempt_duration_budget_exhausted",
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::SourceRead => "source_read",
            Self::StagedWrite => "staged_write",
        }
    }
}
