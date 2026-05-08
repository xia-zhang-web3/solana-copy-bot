mod connection_pragmas;
mod copy_signals;
mod db;
mod discovery_rebuild_state;
mod history_retention;
mod market_context;
mod migrations;
mod money;
mod observed;
mod observed_writer;
mod publication;
mod publication_compat;
mod quality;
mod recent_raw;
mod schema;
mod shadow_close;
mod shadow_lots;
mod shadow_metrics;
mod shadow_mutation;
mod snapshot;
mod sqlite_retry;
mod startup_progress;
mod system_events;
mod trusted_selection;
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
pub use migrations::SqliteStartupBootstrapResult;
pub use money::{lamports_to_sol, signed_lamports_to_sol};
pub use types::{
    DiscoveryBootstrapDegradedStateRow, DiscoveryPersistedRebuildPhase,
    DiscoveryPersistedRebuildStateRow, DiscoveryPublicationFreshnessGate,
    DiscoveryPublicationStateRow, DiscoveryPublicationStateUpdate,
    DiscoveryRecentRawRestoreStateRow, DiscoveryRecentRawRestoreStateUpdate,
    DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor, DiscoveryRuntimeMode,
    DiscoveryTrustedSelectionStateRow, DiscoveryTrustedSelectionStateUpdate,
    FollowlistUpdateResult, HistoryRetentionCutoffs, HistoryRetentionSummary,
    ObservedSwapBatchWriteMetrics, ObservedSwapCursorPage, PersistedWalletMetricSnapshotRow,
    RecentRawJournalStateRow, RecentRawJournalWriteSummary, RiskEventRow, ShadowCloseOutcome,
    ShadowLotRow, SqliteBatchedDeleteSummary, SqliteBatchedDeleteSummaryWithCompletion,
    SqliteContentionSnapshot, SqliteSnapshotDeferredReason, SqliteSnapshotOutcome,
    SqliteSnapshotPolicy, SqliteSnapshotRetryReason, SqliteSnapshotSourceMetrics,
    SqliteSnapshotSummary, SqliteStartupLargeWalCheckpointSummary, SqliteStartupPolicy,
    StartupStepOutcome, StartupStepProgress, StartupStepProgressReporter, StartupStepRuntimePolicy,
    StartupStepTimeout, StartupStepTimeoutBehavior, StartupTrustedSelectionGateStatus,
    TokenMarketStats, TrustedSelectionState, TrustedSnapshotSourceKind,
    DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION, POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER,
    POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER, SHADOW_CLOSE_CONTEXT_MARKET,
    SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY, SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE, SHADOW_LOT_OPEN_EPS,
    SHADOW_RISK_CONTEXT_MARKET, SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY,
    SQLITE_DEFAULT_WAL_AUTOCHECKPOINT_PAGES, SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_THRESHOLD_BYTES,
    SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE, STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES,
    STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES, STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
    STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
};
