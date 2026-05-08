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
