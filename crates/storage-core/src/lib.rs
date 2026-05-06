mod db;
mod history_retention;
mod observed;
mod observed_writer;
mod publication;
mod quality;
mod recent_raw;
mod schema;
mod snapshot;
mod sqlite_retry;
mod startup_progress;
mod system_events;
mod types;

pub use crate::db::SqliteDiscoveryStore;
pub use crate::db::SqliteDiscoveryStore as SqliteStore;
pub use crate::publication::{
    validate_discovery_runtime_artifact_export_readiness,
    validate_discovery_runtime_artifact_snapshot_shape,
};
pub use crate::schema::{ensure_discovery_v2_schema, validate_discovery_v2_schema_read_only};
pub use crate::sqlite_retry::sqlite_contention_snapshot;
pub use crate::sqlite_retry::{is_fatal_sqlite_anyhow_error, is_retryable_sqlite_anyhow_error};
pub use crate::startup_progress::{
    report_startup_step_progress, run_observed_startup_step,
    run_observed_startup_step_with_completion_detail,
};
pub use types::{
    DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateRow,
    DiscoveryPublicationStateUpdate, DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor,
    DiscoveryRuntimeMode, FollowlistUpdateResult, HistoryRetentionCutoffs, HistoryRetentionSummary,
    ObservedSwapBatchWriteMetrics, ObservedSwapCursorPage, PersistedWalletMetricSnapshotRow,
    RecentRawJournalStateRow, RecentRawJournalWriteSummary, RiskEventRow,
    SqliteBatchedDeleteSummary, SqliteContentionSnapshot, SqliteSnapshotDeferredReason,
    SqliteSnapshotOutcome, SqliteSnapshotPolicy, SqliteSnapshotRetryReason,
    SqliteSnapshotSourceMetrics, SqliteSnapshotSummary, SqliteStartupPolicy, StartupStepOutcome,
    StartupStepProgress, StartupStepProgressReporter, StartupStepRuntimePolicy, StartupStepTimeout,
    StartupStepTimeoutBehavior, DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
    SHADOW_CLOSE_CONTEXT_MARKET, SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
    SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE, SHADOW_RISK_CONTEXT_MARKET,
    SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY, SQLITE_DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
    SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_THRESHOLD_BYTES,
    SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE, STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES,
    STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES, STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
    STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
};
