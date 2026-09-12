mod association_candidate;
pub mod association_inbox;
pub mod association_sell_preparation;
mod association_sell_types;
pub mod ordered_sell_quote;
pub mod ordered_source_sell;
mod quote_http_timing;
pub mod quote_response_availability;
pub use quote_http_timing::{
    quote_http_started_expr, quote_http_timing_available, QUOTE_HTTP_TIMING_MIGRATION,
};
mod source_sell_handoff;
mod source_sell_handoff_consumer;
mod source_sell_handoff_queue;
mod source_sell_handoff_rows;
pub mod source_sell_handoff_schema;
pub use source_sell_handoff::{SourceSellCandidate, SourceSellHandoff};
mod execution_canary_dispatch;
pub use execution_canary_dispatch::{
    ExecutionCanaryDispatch, ExecutionDispatchClaim, EXECUTION_UNRESOLVED_BUY_REASON,
};
mod execution_source_sell_guard;
mod execution_source_sell_staging_cursor;
pub use execution_source_sell_staging_cursor::ExecutionSourceSellStagingVisit;
mod execution_source_sell_intent;
mod execution_source_sell_promotion;
pub use execution_source_sell_guard::ExecutionSourceSellGuard;
mod execution_canary_recovery_selection;
mod execution_retry_selection;
pub use execution_retry_selection::{
    ExecutionRetryCursor, ExecutionRetryOrder, ExecutionRetryQuote,
};
mod source_sell_promotion_guard;
mod source_sell_promotion_rows;
mod source_sell_validation;
mod types_source_sell_promotion;
pub use types_source_sell_promotion::*;
mod source_sell_event;
mod source_sell_intent_rows;
mod types_source_sell_intent;
pub use types_source_sell_intent::*;
mod buy_fill_identity;
mod buy_fill_replay;
mod buy_receipt_collisions;
mod buy_receipt_ownership;
mod execution_canary_buy_attribution;
mod types_buy_attribution;
pub use types_buy_attribution::*;
mod connection_pragmas;
mod copy_signals;
mod db;
mod discovery_rebuild_state;
mod entry_cost;
mod sell_cash_day;
pub use sell_cash_day::{
    ExecutionCanarySellCashDay, RecognizedSellCashEvents, UndatedSellCashObligations,
};
mod executable_wallet_feedback;
mod execution_canary_build_plan_metadata;
mod execution_canary_buy_retry_candidates;
mod execution_canary_confirmed_fill;
mod execution_canary_fill_marker;
mod execution_canary_manual_writeoff;
mod execution_canary_position_close;
mod execution_canary_position_open;
mod execution_canary_positions;
mod execution_canary_quote_pnl;
mod execution_canary_quote_pnl_accumulator;
mod execution_canary_quote_pnl_breakdown;
mod execution_canary_quote_pnl_buckets;
mod execution_canary_quote_pnl_compute;
mod execution_canary_quote_pnl_diagnostics;
mod execution_canary_quote_pnl_gate;
mod execution_canary_quote_pnl_gate_shadow;
mod execution_canary_quote_pnl_rows;
mod execution_canary_readiness;
mod execution_canary_receipt;
mod execution_canary_receipt_facts;
mod execution_canary_receipt_position;
mod execution_canary_report;
mod execution_canary_retry;
mod execution_canary_retry_terminal;
mod execution_canary_rows;
mod execution_canary_sell_boundary;
mod execution_canary_sell_reserve;
mod execution_canary_sell_settlement;
mod execution_canary_stale_candidates;
mod execution_canary_state;
mod execution_canary_submit_risk;
mod execution_canary_submitted;
mod failed_expenses;
pub use entry_cost::{CashEntryLoss, ClosedEntryLoss, EntryCostCap, ExecutionCanaryEntryCost};
mod quote_fee_allocation;
mod quote_fee_cohort;
mod quote_fee_identity;
pub use failed_expenses::{
    proven_transaction_failure, FailedExpenseCoverage, FailedExpenseReport, FailedExpenseReportRow,
    FailedExpenseTask, FailedTransactionFacts,
};
mod execution_cash_settlement;
mod execution_cash_settlement_report;
mod execution_cash_settlement_rows;
mod types_cash_settlement_report;
pub use types_cash_settlement_report::{
    ExecutionCashSettlementReport, ExecutionCashSettlementReportRow,
};
mod execution_entry_quote_shadow_diagnostic;
mod execution_exit_policy_sim;
mod execution_exit_policy_types;
mod execution_market_exit_shadow_quote;
mod execution_quote_entry_pending;
pub use execution_quote_entry_pending::{
    execution_quote_entry_is_closed, pend_execution_quote_entry, EXECUTION_QUOTE_OWNER_PENDING,
};
mod execution_quote_entry_refusal;
pub use execution_quote_entry_refusal::execution_quote_entry_is_refused;
mod execution_orders;
mod execution_quote_canary;
mod execution_quote_canary_lookup;
mod execution_quote_canary_rank_stamp;
mod execution_quote_canary_route_samples;
mod execution_quote_canary_shadow_gate;
mod execution_quote_canary_source_cohort;
mod execution_quote_provider_lookup;
mod execution_quote_provider_selection_report;
mod execution_stale_decay;
mod execution_tiny_entry_funnel;
mod execution_tiny_order_failures;
mod execution_tiny_proof;
mod execution_tiny_proof_aggregate;
mod execution_tiny_proof_rows;
// Shared only with the legacy storage migration runner; no runtime dependency growth.
#[doc(hidden)]
pub mod fill_cash_migration;
mod history_retention;
mod legacy_sell_receipt_ownership;
mod market_context;
mod market_price;
pub use market_price::ObservedTokenSolPrice;
pub mod association_sell_shadow_types;
mod migrations;
mod migrations_index_guard;
mod money;
mod observed;
mod observed_activity;
mod observed_budget;
pub mod observed_retention;
mod observed_row;
mod observed_sol_leg_projection;
mod observed_sol_leg_scan;
mod observed_timestamp;
mod observed_writer;
mod publication;
mod publication_compat;
mod quality;
mod quality_evidence_write;
mod receipt_buy_initial_result;
mod receipt_facts_identity;
mod receipt_facts_rows;
mod recent_raw;
mod rug_wallet_feedback;
mod rug_wallet_quarantine;
mod schema;
mod schema_indexes;
mod sell_receipt_ownership;
mod sell_settlement_position;
mod shadow_close;
mod shadow_feedback_runtime;
pub mod shadow_lot_origin;
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
mod types_cash_settlement;
mod types_receipt_facts;
mod types_sell_settlement;
pub use types_cash_settlement::{
    ExecutionCanaryCashSettlement, ExecutionCanaryCashSettlementResult,
};

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
pub use execution_exit_policy_types::{
    ExecutionExitPolicyBaseline, ExecutionExitPolicyContextSummary, ExecutionExitPolicySimConfig,
    ExecutionExitPolicySimParams, ExecutionExitPolicySimReport, ExecutionExitPolicySummary,
};
pub use execution_quote_canary_rank_stamp::ExecutionQuoteCanaryDiscoveryRankStamp;
pub use execution_quote_canary_route_samples::{
    PROVIDER_GENERIC_METIS, PROVIDER_GENERIC_PUBLIC, PROVIDER_PUMP_FUN_PAID,
};
pub use execution_quote_canary_shadow_gate::{
    EXECUTION_QUOTE_CANARY_SHADOW_GATE_DROPPED, EXECUTION_QUOTE_CANARY_SHADOW_GATE_RECORDED,
};
pub use execution_stale_decay::{
    ExecutionStaleDecayCheckpointSummary, ExecutionStaleDecayPoint, ExecutionStaleDecayReport,
    ExecutionStaleDecayTrade,
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
    DiscoveryV2QualityPrepareUpsert, ExecutableWalletFeedback, ExecutionCanaryBuildPlanMetadata,
    ExecutionCanaryBuildPlanMetadataRecordOutcome, ExecutionCanaryCloseCandidate,
    ExecutionCanaryConfirmTimeoutDecision, ExecutionCanaryManualWriteOffResult,
    ExecutionCanaryObservedLeg, ExecutionCanaryOrder, ExecutionCanaryOwnedPosition,
    ExecutionCanaryOwnedPositionRecordResult, ExecutionCanaryPositionCloseResult,
    ExecutionCanaryPositionRecordOutcome, ExecutionCanaryQuoteBucketSummary,
    ExecutionCanaryQuoteDiagnosticsSummary, ExecutionCanaryQuotePnlSummary,
    ExecutionCanaryQuotePnlThresholdSummary, ExecutionCanaryQuotePnlTrade,
    ExecutionCanaryQuoteReadinessCheck, ExecutionCanaryQuoteReadinessGate,
    ExecutionCanaryQuoteRouteCount, ExecutionCanaryQuoteShadowGateReasonCount,
    ExecutionCanaryQuoteShadowGateSummary, ExecutionCanaryQuoteSideDiagnostics,
    ExecutionCanaryQuoteStatusCount, ExecutionCanaryQuoteThresholdCandidate,
    ExecutionCanaryReadinessCount, ExecutionCanaryReadinessLatestOrder,
    ExecutionCanaryReadinessSummary, ExecutionCanaryReadinessWindowSummary,
    ExecutionCanaryRecordOutcome, ExecutionCanaryReserveResult, ExecutionCanarySellDecision,
    ExecutionCanarySellReserveResult, ExecutionCanaryShadowCloseBreakdown,
    ExecutionCanaryShadowCloseContextSummary, ExecutionCanaryStatusReport,
    ExecutionCanarySubmitRiskOrder, ExecutionCanarySubmitRiskSummary, ExecutionDryRunOrder,
    ExecutionDryRunRecordOutcome, ExecutionHistoryRetentionSummary,
    ExecutionQuoteCanaryEventInsert, ExecutionQuoteCanaryProviderComparisonEvent,
    ExecutionQuoteCanaryProviderComparisonSummary, ExecutionQuoteCanaryProviderSampleInsert,
    ExecutionQuoteCanaryProviderSelectionEvent, ExecutionQuoteCanaryProviderSelectionSummary,
    ExecutionQuoteCanaryPublicPaidComparisonEvent, ExecutionQuoteCanaryPublicPaidComparisonSummary,
    ExecutionQuoteCanaryRecordOutcome, ExecutionQuoteFeeAllocation, ExecutionTinyEntryFunnel,
    ExecutionTinyEntryFunnelBucket, ExecutionTinyEntryFunnelDropReasonCount,
    ExecutionTinyOrderFailureCount, ExecutionTinyProofLatencyStats,
    ExecutionTinyProofLatencySummary, ExecutionTinyProofOpenPosition, ExecutionTinyProofOrder,
    ExecutionTinyProofPositionMatch, ExecutionTinyProofReasonCount, ExecutionTinyProofReport,
    ExecutionTinyProofSummary, ExecutionTinyProofTrade, FollowlistUpdateResult,
    HistoryRetentionCutoffs, HistoryRetentionSummary, ObservedSolLegSwap,
    ObservedSwapBatchWriteMetrics, ObservedSwapCursorPage, PersistedWalletMetricSnapshotRow,
    RecentRawJournalStateRow, RecentRawJournalWriteSummary, RiskEventRow, RugWalletFeedback,
    RugWalletQuarantineRow, RugWalletQuarantineUpsert, ShadowCloseOutcome, ShadowLotRow,
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
    EXECUTION_ERROR_BUILD_FAILED, EXECUTION_ERROR_CONFIRMATION_FAILED, EXECUTION_ERROR_EXPIRED,
    EXECUTION_ERROR_SIGNING_ENVELOPE_FAILED, EXECUTION_ERROR_SIMULATION_FAILED,
    EXECUTION_ERROR_SUBMIT_DISABLED, EXECUTION_ERROR_SUBMIT_PLAN_FAILED,
    EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE, EXECUTION_ERROR_TERMINAL_SELL_SIMULATION_FAILED,
    EXECUTION_SIMULATION_STATUS_DRY_RUN_SKIPPED, EXECUTION_SIMULATION_STATUS_FAILED,
    EXECUTION_SIMULATION_STATUS_NOT_RUN, EXECUTION_SIMULATION_STATUS_PASSED,
    EXECUTION_SIMULATION_STATUS_SKIPPED_NO_SUBMIT, EXECUTION_STATUS_CANARY_BUILT,
    EXECUTION_STATUS_CANARY_CANDIDATE, EXECUTION_STATUS_CANARY_CONFIRMED,
    EXECUTION_STATUS_CANARY_EXPIRED, EXECUTION_STATUS_CANARY_FAILED,
    EXECUTION_STATUS_CANARY_SIMULATED, EXECUTION_STATUS_CANARY_SUBMITTED,
    EXECUTION_STATUS_CANARY_SUBMIT_DISABLED, EXECUTION_STATUS_DRY_RUN_CONFIRMED,
    POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER, POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER,
    SHADOW_CLOSE_CONTEXT_MARKET, SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
    SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE, SHADOW_CLOSE_CONTEXT_STALE_MARKET_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE, SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
    SHADOW_LOT_OPEN_EPS, SHADOW_RISK_CONTEXT_MARKET, SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY,
    SQLITE_DEFAULT_WAL_AUTOCHECKPOINT_PAGES, SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_THRESHOLD_BYTES,
    SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE, STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES,
    STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES, STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
    STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
};

pub use execution_canary_receipt::{
    ExecutionCanaryReceiptProof, EXECUTION_ACCOUNTING_PENDING_REASON,
    EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
};

pub use execution_canary_receipt_position::ReceiptAccountingUnsupported;

mod execution_owned_sell_queue;
mod execution_sell_intent;
pub use execution_sell_intent::{
    ExecutionSellIntentOutcome, ExecutionSellIntentReject, EXECUTION_SELL_INTENT_STATUS,
};

pub use receipt_facts_identity::ReceiptFactsIdentityRejection;
pub use types_receipt_facts::{
    ExecutionCanaryReceiptFacts, ReceiptDecomposition, ReceiptFactsRecordOutcome,
    ReceiptFeeCoverage, ReceiptTokenCoverage, ReceiptTokenDelta, ReceiptWsolCoverage,
};
pub use types_sell_settlement::{
    ExecutionCanarySellSettlement, ExecutionCanarySellSettlementPlan,
    SellSettlementExpectedPosition, SellSettlementUnsupported, SettlementSqliteSignedValue,
    SettlementSwapPrice,
};

mod native_observations;
pub use native_observations::*;
mod execution_source_sell_write_off;
mod source_sell_write_off_eligibility;
mod types_source_sell_write_off;
pub use source_sell_write_off_eligibility::execution_terminal_sell_no_route_proof;
pub use types_source_sell_write_off::{
    ExecutionSourceSellWriteOffKind, ExecutionSourceSellWriteOffOutcome,
};
mod execution_failed_sell_sweep_cursor;
mod execution_failed_sell_sweep_selection;
pub use execution_failed_sell_sweep_cursor::ExecutionFailedSellSweepVisit;
pub use execution_failed_sell_sweep_selection::ExecutionFailedSellSweepOrder;

mod association_fault_tests;

mod tiny_experiment;
pub use tiny_experiment::{
    ProtectedCapitalClaim, ProtectedNativePolicy, TinyBudgetClaim, TinyExperiment,
    TINY_BUY_LAMPORTS, TINY_EXIT_RESERVE, TINY_HORIZON_SECONDS, TINY_NATIVE_ALLOWANCE,
    TINY_PRIORITY_FEE, TINY_TOTAL_FEE, TINY_TRANSACTION_FEE,
};
