use super::*;

pub use copybot_storage_core::{
    SqliteBatchedDeleteSummary, SqliteBatchedDeleteSummaryWithCompletion, SqliteContentionSnapshot,
    SqliteStartupLargeWalCheckpointSummary,
};

pub(super) fn sqlite_startup_large_wal_checkpoint_detail(
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

pub(super) fn sqlite_startup_large_wal_checkpoint_skip_detail(
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
