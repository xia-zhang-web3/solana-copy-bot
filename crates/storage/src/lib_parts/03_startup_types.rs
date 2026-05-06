#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqliteStartupLargeWalCheckpointSummary {
    pub threshold_bytes: u64,
    pub before_wal_bytes: u64,
    pub after_wal_bytes: u64,
    pub busy: i64,
    pub log_frames: i64,
    pub checkpointed_frames: i64,
}

fn sqlite_startup_large_wal_checkpoint_detail(
    summary: SqliteStartupLargeWalCheckpointSummary,
) -> String {
    format!(
        "threshold_bytes={} before_wal_bytes={} after_wal_bytes={} busy={} log_frames={} checkpointed_frames={}",
        summary.threshold_bytes,
        summary.before_wal_bytes,
        summary.after_wal_bytes,
        summary.busy,
        summary.log_frames,
        summary.checkpointed_frames
    )
}

fn sqlite_startup_large_wal_checkpoint_skip_detail(
    reason: &str,
    threshold_bytes: u64,
    before_wal_bytes: Option<u64>,
) -> String {
    format!(
        "reason={} threshold_bytes={} before_wal_bytes={}",
        reason,
        threshold_bytes,
        before_wal_bytes.unwrap_or(0)
    )
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SqliteContentionSnapshot {
    pub write_retry_total: u64,
    pub busy_error_total: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqliteBatchedDeleteSummary {
    pub deleted_rows: usize,
    pub batches: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqliteBatchedDeleteSummaryWithCompletion {
    pub deleted_rows: usize,
    pub batches: usize,
    pub completed_full_sweep: bool,
}

pub struct SqliteStartupBootstrapResult {
    pub store: SqliteStore,
    pub applied_migrations: usize,
    pub deferred_migrations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryScoringBoundarySeedLot {
    pub buy_signature: String,
    pub wallet_id: String,
    pub token: String,
    pub qty: f64,
    pub cost_sol: f64,
    pub opened_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveryScoringBoundarySeedSnapshot {
    pub boundary_start_ts: DateTime<Utc>,
    pub boundary_cursor: DiscoveryRuntimeCursor,
    pub open_lots: Vec<DiscoveryScoringBoundarySeedLot>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveryScoringSeedBoundaryInstallMarker {
    pub boundary_start_ts: DateTime<Utc>,
    pub boundary_cursor: DiscoveryRuntimeCursor,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiscoveryScoringBatchStageTimings {
    pub prepare_ms: u64,
    pub apply_ms: u64,
    pub rug_finalize_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiscoveryScoringCheckpointedBatchTimings {
    pub prepare_ms: u64,
    pub apply_ms: u64,
    pub progress_update_ms: u64,
}
