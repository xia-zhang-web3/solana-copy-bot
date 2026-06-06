mod connection_pragmas;
mod copy_signals;
mod db;
mod discovery_rebuild_state;
mod execution_canary_build_plan_metadata;
mod execution_canary_position_close;
mod execution_canary_positions;
mod execution_canary_quote_pnl;
mod execution_canary_quote_pnl_accumulator;
mod execution_canary_quote_pnl_breakdown;
mod execution_canary_quote_pnl_buckets;
mod execution_canary_quote_pnl_compute;
mod execution_canary_quote_pnl_diagnostics;
mod execution_canary_quote_pnl_gate;
mod execution_canary_quote_pnl_rows;
mod execution_canary_readiness;
mod execution_canary_report;
mod execution_canary_retry;
mod execution_canary_rows;
mod execution_canary_state;
mod execution_orders;
mod execution_quote_canary;
mod execution_quote_canary_lookup;
mod execution_quote_canary_route_samples;
mod history_retention;
mod market_context;
mod migrations;
mod migrations_index_guard;
mod money;
mod observed;
mod observed_activity;
mod observed_budget;
mod observed_row;
mod observed_sol_leg_projection;
mod observed_sol_leg_scan;
mod observed_timestamp;
mod observed_writer;
mod publication;
mod publication_compat;
mod quality;
mod quality_evidence_write;
mod recent_raw;
mod schema;
mod schema_indexes;
mod shadow_close;
mod shadow_feedback_runtime;
mod shadow_lots;
mod shadow_metrics;
mod shadow_mutation;
mod shadow_recent_close;
mod shadow_token_exposure;
mod snapshot;
mod sqlite_retry;
mod startup_progress;
mod status_snapshot;
mod system_events;
mod trusted_selection;
mod types;

pub use crate::db::SqliteDiscoveryStore;
pub use crate::db::SqliteDiscoveryStore as SqliteStore;
pub use crate::publication::{
    validate_discovery_runtime_artifact_export_readiness,
    validate_discovery_runtime_artifact_snapshot_shape,
};
pub use crate::schema::{
    ensure_discovery_v2_schema, validate_discovery_v2_schema_read_only,
    validate_discovery_v2_status_schema_read_only,
};
pub use crate::sqlite_retry::sqlite_contention_snapshot;
pub use crate::sqlite_retry::{is_fatal_sqlite_anyhow_error, is_retryable_sqlite_anyhow_error};
pub use crate::startup_progress::{
    report_startup_step_progress, run_observed_startup_step,
    run_observed_startup_step_with_completion_detail,
};
pub use crate::status_snapshot::DiscoveryV2StatusSnapshotRow;
pub use execution_quote_canary_route_samples::{
    PROVIDER_GENERIC_METIS, PROVIDER_GENERIC_PUBLIC, PROVIDER_PUMP_FUN_PAID,
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
    DiscoveryV2QualityEvidenceAggregate, DiscoveryV2QualityPrepareState,
    DiscoveryV2QualityPrepareUpsert, ExecutionCanaryBuildPlanMetadata,
    ExecutionCanaryBuildPlanMetadataRecordOutcome, ExecutionCanaryCloseCandidate,
    ExecutionCanaryConfirmTimeoutDecision, ExecutionCanaryObservedLeg, ExecutionCanaryOrder,
    ExecutionCanaryOwnedPosition, ExecutionCanaryOwnedPositionRecordResult,
    ExecutionCanaryPositionCloseResult, ExecutionCanaryPositionRecordOutcome,
    ExecutionCanaryQuoteBucketSummary, ExecutionCanaryQuoteDiagnosticsSummary,
    ExecutionCanaryQuotePnlSummary, ExecutionCanaryQuotePnlThresholdSummary,
    ExecutionCanaryQuotePnlTrade, ExecutionCanaryQuoteReadinessCheck,
    ExecutionCanaryQuoteReadinessGate, ExecutionCanaryQuoteRouteCount,
    ExecutionCanaryQuoteSideDiagnostics, ExecutionCanaryQuoteStatusCount,
    ExecutionCanaryQuoteThresholdCandidate, ExecutionCanaryReadinessCount,
    ExecutionCanaryReadinessLatestOrder, ExecutionCanaryReadinessSummary,
    ExecutionCanaryReadinessWindowSummary, ExecutionCanaryRecordOutcome,
    ExecutionCanaryReserveResult, ExecutionCanarySellDecision, ExecutionCanaryShadowCloseBreakdown,
    ExecutionCanaryShadowCloseContextSummary, ExecutionCanaryStatusReport, ExecutionDryRunOrder,
    ExecutionDryRunRecordOutcome, ExecutionHistoryRetentionSummary,
    ExecutionQuoteCanaryEventInsert, ExecutionQuoteCanaryProviderComparisonEvent,
    ExecutionQuoteCanaryProviderComparisonSummary, ExecutionQuoteCanaryProviderSampleInsert,
    ExecutionQuoteCanaryPublicPaidComparisonEvent, ExecutionQuoteCanaryPublicPaidComparisonSummary,
    ExecutionQuoteCanaryRecordOutcome, FollowlistUpdateResult, HistoryRetentionCutoffs,
    HistoryRetentionSummary, ObservedSolLegSwap, ObservedSwapBatchWriteMetrics,
    ObservedSwapCursorPage, PersistedWalletMetricSnapshotRow, RecentRawJournalStateRow,
    RecentRawJournalWriteSummary, RiskEventRow, ShadowCloseOutcome, ShadowLotRow,
    ShadowSignalSummary, ShadowTokenLossCooldown, ShadowTokenRecentClose, ShadowWalletFeedback,
    ShadowWalletTokenFastLossCooldown, SqliteBatchedDeleteSummary,
    SqliteBatchedDeleteSummaryWithCompletion, SqliteContentionSnapshot,
    SqliteSnapshotDeferredReason, SqliteSnapshotOutcome, SqliteSnapshotPolicy,
    SqliteSnapshotRetryReason, SqliteSnapshotSourceMetrics, SqliteSnapshotSummary,
    SqliteStartupLargeWalCheckpointSummary, SqliteStartupPolicy, StartupStepOutcome,
    StartupStepProgress, StartupStepProgressReporter, StartupStepRuntimePolicy, StartupStepTimeout,
    StartupStepTimeoutBehavior, StartupTrustedSelectionGateStatus, TokenMarketStats,
    TrustedSelectionState, TrustedSnapshotSourceKind, WalletActivityDayRow,
    WalletSolLegActivityWindow, DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
    EXECUTION_CANARY_CONFIRM_DECISION_EXPIRE_UNSAFE,
    EXECUTION_CANARY_CONFIRM_DECISION_NOT_SUBMITTED, EXECUTION_CANARY_CONFIRM_DECISION_RETRY,
    EXECUTION_CANARY_CONFIRM_DECISION_WAIT, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
    EXECUTION_CANARY_POSITION_CLOSE_CLOSED, EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED,
    EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION, EXECUTION_CANARY_POSITION_CLOSE_PARTIAL,
    EXECUTION_CANARY_POSITION_STATE_CLOSED, EXECUTION_CANARY_POSITION_STATE_OPEN,
    EXECUTION_CANARY_QUOTE_PNL_STATUS_COUNTED, EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED,
    EXECUTION_CANARY_QUOTE_PNL_STATUS_UNKNOWN, EXECUTION_CANARY_SELL_DECISION_EXECUTE,
    EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT, EXECUTION_CANARY_SELL_DECISION_NO_POSITION,
    EXECUTION_ERROR_BUILD_FAILED, EXECUTION_ERROR_EXPIRED, EXECUTION_ERROR_SIMULATION_FAILED,
    EXECUTION_ERROR_SUBMIT_DISABLED, EXECUTION_SIMULATION_STATUS_DRY_RUN_SKIPPED,
    EXECUTION_SIMULATION_STATUS_FAILED, EXECUTION_SIMULATION_STATUS_NOT_RUN,
    EXECUTION_SIMULATION_STATUS_PASSED, EXECUTION_SIMULATION_STATUS_SKIPPED_NO_SUBMIT,
    EXECUTION_STATUS_CANARY_BUILT, EXECUTION_STATUS_CANARY_CANDIDATE,
    EXECUTION_STATUS_CANARY_CONFIRMED, EXECUTION_STATUS_CANARY_EXPIRED,
    EXECUTION_STATUS_CANARY_FAILED, EXECUTION_STATUS_CANARY_SIMULATED,
    EXECUTION_STATUS_CANARY_SUBMITTED, EXECUTION_STATUS_CANARY_SUBMIT_DISABLED,
    EXECUTION_STATUS_DRY_RUN_CONFIRMED, POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER,
    POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER, SHADOW_CLOSE_CONTEXT_MARKET,
    SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY, SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_MARKET_PRICE, SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE, SHADOW_LOT_OPEN_EPS,
    SHADOW_RISK_CONTEXT_MARKET, SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY,
    SQLITE_DEFAULT_WAL_AUTOCHECKPOINT_PAGES, SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_THRESHOLD_BYTES,
    SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE, STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES,
    STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES, STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
    STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
};
