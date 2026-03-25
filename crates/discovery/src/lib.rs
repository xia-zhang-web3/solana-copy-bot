use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_storage::{
    is_fatal_sqlite_anyhow_error, DiscoveryPersistedRebuildPhase,
    DiscoveryPersistedRebuildStateRow, DiscoveryPublicationFreshnessGate,
    DiscoveryPublicationStateRow, DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor,
    DiscoveryRuntimeMode, DiscoveryTrustedSelectionStateUpdate, ObservedSolLegCursorAccessPath,
    ObservedWalletActivityDayCountSource, ObservedWalletActivityRow,
    PersistedWalletMetricSnapshotRow, SqliteStore, StartupTrustedSelectionGateStatus,
    TrustedSelectionState, TrustedSnapshotSourceKind, TrustedWalletMetricsSnapshotRow,
    TrustedWalletMetricsSnapshotWrite, WalletMetricRow, WalletUpsertRow,
};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration as StdDuration, Instant};
use tracing::{info, warn};

pub mod cutover_readiness;
mod followlist;
pub mod operator_status;
pub mod perf_harness;
mod quality_cache;
pub mod raw_gap_fill_support;
pub mod restore_verdict;
pub mod runtime_restore_ops;
mod scoring;
pub mod wallet_freshness_audit;
mod windows;
use self::followlist::{desired_wallets, rank_follow_candidates, top_wallet_labels};
use self::scoring::{hold_time_quality_score, median_i64, tanh01};
use self::wallet_freshness_audit::PrecomputedWalletFreshnessCurrentRawTruth;
use self::windows::{
    cmp_swap_order, CachedCurrentRawTruthSample, CapTruncationDeactivationGuardReason,
    DiscoveryCursor, DiscoveryWindowState,
};
use quality_cache::BuyTradability;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const QUALITY_CACHE_TTL_SECONDS: i64 = 10 * 60;
const QUALITY_RPC_TIMEOUT_MS: u64 = 700;
const QUALITY_MAX_SIGNATURE_PAGES: u32 = 1;
const QUALITY_MAX_FETCH_PER_CYCLE: usize = 20;
const QUALITY_RPC_BUDGET_MS: u64 = 1_500;
const AGGREGATE_READINESS_MAX_LAG_BUCKETS: u64 = 2;
const CAP_TRUNCATION_FOLLOWLIST_DEACTIVATION_GUARD_CYCLES: u32 = 2;
const STREAMING_RUG_TRADE_SWEEP_INTERVAL_SWAPS: usize = 2_048;
const STALE_RECONCILE_TOKEN_BATCH_CAP: usize = 256;
const STALE_RECONCILE_EXACT_COUNT_BATCH_CAP: usize = 32;
const REPLAY_WALLET_STATS_WALLET_BATCH_CAP: usize = 900;
const REPLAY_WALLET_STATS_CATCH_UP_PAGE_LIMIT_MULTIPLIER: usize = 2;
#[cfg(test)]
const POST_BOOTSTRAP_ROTATION_BLOCKED_REASON: &str =
    "post_bootstrap_rotation_blocked_cap_truncated";

#[cfg(test)]
thread_local! {
    static TEST_FORCE_RECONCILE_NEW_TAIL_ZERO_ROW_TIMEOUT: Cell<bool> = const { Cell::new(false) };
    static TEST_FORCE_RECONCILE_EXPIRED_HEAD_EXACT_BATCH_ROW_LIMIT: Cell<usize> = const { Cell::new(0) };
    static TEST_FORCE_RECONCILE_NEW_TAIL_EXACT_BATCH_ROW_LIMIT: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn arm_test_force_reconcile_new_tail_zero_row_timeout() {
    TEST_FORCE_RECONCILE_NEW_TAIL_ZERO_ROW_TIMEOUT.with(|flag| flag.set(true));
}

#[cfg(test)]
fn take_test_force_reconcile_new_tail_zero_row_timeout() -> bool {
    TEST_FORCE_RECONCILE_NEW_TAIL_ZERO_ROW_TIMEOUT.with(|flag| flag.replace(false))
}

#[cfg(test)]
fn arm_test_force_reconcile_expired_head_exact_batch_row_limit(limit: usize) {
    TEST_FORCE_RECONCILE_EXPIRED_HEAD_EXACT_BATCH_ROW_LIMIT.with(|value| value.set(limit));
}

#[cfg(test)]
fn take_test_force_reconcile_expired_head_exact_batch_row_limit() -> Option<usize> {
    TEST_FORCE_RECONCILE_EXPIRED_HEAD_EXACT_BATCH_ROW_LIMIT.with(|value| {
        let limit = value.replace(0);
        (limit > 0).then_some(limit)
    })
}

#[cfg(test)]
fn arm_test_force_reconcile_new_tail_exact_batch_row_limit(limit: usize) {
    TEST_FORCE_RECONCILE_NEW_TAIL_EXACT_BATCH_ROW_LIMIT.with(|value| value.set(limit));
}

#[cfg(test)]
fn take_test_force_reconcile_new_tail_exact_batch_row_limit() -> Option<usize> {
    TEST_FORCE_RECONCILE_NEW_TAIL_EXACT_BATCH_ROW_LIMIT.with(|value| {
        let limit = value.replace(0);
        (limit > 0).then_some(limit)
    })
}

fn discovery_runtime_cursor_error_requires_abort(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn discovery_runtime_cursor_load_error_requires_abort(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn discovery_recent_window_load_error_requires_abort(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn discovery_wallet_activity_day_count_error_requires_abort(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn maybe_arm_cap_truncation_deactivation_guard(
    state: &mut DiscoveryWindowState,
    now: DateTime<Utc>,
    reason: CapTruncationDeactivationGuardReason,
) {
    if !state.arm_cap_truncation_deactivation_guard(
        now,
        reason,
        CAP_TRUNCATION_FOLLOWLIST_DEACTIVATION_GUARD_CYCLES,
    ) {
        return;
    }
    let Some(floor) = state.cap_truncation_floor.as_ref() else {
        return;
    };
    warn!(
        followlist_deactivation_suppression_reason = reason.as_str(),
        followlist_deactivation_suppression_started_at = %now,
        cap_truncation_floor_ts = %floor.ts_utc,
        cap_truncation_floor_signature = floor.signature.as_str(),
        cap_truncation_deactivation_guard_cycles =
            state.cap_truncation_deactivation_guard_cycles_remaining,
        "discovery followlist deactivations temporarily suppressed while raw window is cap-truncated"
    );
}

fn maybe_warn_on_cap_truncation_deactivation_guard_expiry(
    state: &DiscoveryWindowState,
    followlist_deactivations_suppressed: bool,
) {
    let Some(floor) = state.cap_truncation_floor.as_ref() else {
        return;
    };
    let reason = state
        .cap_truncation_deactivation_guard_reason
        .map(CapTruncationDeactivationGuardReason::as_str)
        .unwrap_or("cap_truncation");
    if followlist_deactivations_suppressed {
        warn!(
            followlist_deactivation_suppression_reason = reason,
            followlist_deactivation_suppression_started_at = ?state
                .cap_truncation_deactivation_guard_started_at,
            cap_truncation_floor_ts = %floor.ts_utc,
            cap_truncation_floor_signature = floor.signature.as_str(),
            "discovery cap-truncation guard countdown expired, but raw-window followlist mutations remain suppressed until truncation state clears"
        );
    }
}

#[derive(Debug, Clone, Default)]
struct CapTruncationTelemetrySnapshot {
    raw_window_cap_truncated: bool,
    cap_truncation_deactivation_guard_active: bool,
    cap_truncation_deactivation_guard_reason: Option<&'static str>,
    cap_truncation_deactivation_guard_started_at: Option<DateTime<Utc>>,
    cap_truncation_floor_ts_utc: Option<DateTime<Utc>>,
    cap_truncation_floor_signature: Option<String>,
}

fn snapshot_cap_truncation_telemetry(
    state: &DiscoveryWindowState,
    followlist_deactivations_suppressed: bool,
) -> CapTruncationTelemetrySnapshot {
    CapTruncationTelemetrySnapshot {
        raw_window_cap_truncated: state.cap_truncation_floor.is_some(),
        cap_truncation_deactivation_guard_active: followlist_deactivations_suppressed,
        cap_truncation_deactivation_guard_reason: state
            .cap_truncation_deactivation_guard_reason
            .map(CapTruncationDeactivationGuardReason::as_str),
        cap_truncation_deactivation_guard_started_at: state
            .cap_truncation_deactivation_guard_started_at,
        cap_truncation_floor_ts_utc: state
            .cap_truncation_floor
            .as_ref()
            .map(|floor| floor.ts_utc),
        cap_truncation_floor_signature: state
            .cap_truncation_floor
            .as_ref()
            .map(|floor| floor.signature.clone()),
    }
}

fn raw_window_history_incomplete_for_followlist_or_metrics(state: &DiscoveryWindowState) -> bool {
    state.cap_truncation_floor.is_some()
}

#[derive(Debug, Clone)]
pub struct DiscoveryService {
    config: DiscoveryConfig,
    shadow_quality: ShadowConfig,
    helius_http_url: Option<String>,
    window_state: Arc<Mutex<DiscoveryWindowState>>,
}

#[derive(Debug, Clone, Default)]
pub struct DiscoverySummary {
    pub window_start: DateTime<Utc>,
    pub wallets_seen: usize,
    pub eligible_wallets: usize,
    pub metrics_written: usize,
    pub follow_promoted: usize,
    pub follow_demoted: usize,
    pub active_follow_wallets: usize,
    pub top_wallets: Vec<String>,
    pub published: bool,
    pub runtime_mode: DiscoveryRuntimeMode,
    pub scoring_source: &'static str,
    pub trusted_selection_fail_closed: bool,
    pub raw_window_cap_truncated: bool,
    pub cap_truncation_deactivation_guard_active: bool,
    pub cap_truncation_deactivation_guard_reason: Option<&'static str>,
    pub cap_truncation_deactivation_guard_started_at: Option<DateTime<Utc>>,
    pub cap_truncation_floor_ts_utc: Option<DateTime<Utc>>,
    pub cap_truncation_floor_signature: Option<String>,
    pub persisted_stream_catch_up_requested: bool,
    pub wallet_freshness_capture_state: Option<&'static str>,
    pub wallet_freshness_capture_reason: Option<String>,
    pub wallet_freshness_capture_id: Option<i64>,
    pub wallet_freshness_capture_captured_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct TrustedBootstrapWalletMetricsMaterializationSummary {
    pub metrics_window_start: DateTime<Utc>,
    pub observed_swaps_loaded: usize,
    pub wallets_seen: usize,
    pub eligible_wallets: usize,
    pub metrics_written: usize,
    pub bucket_already_exists: bool,
    pub top_wallets: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct CloneLatestTrustedBootstrapSummary {
    pub source_metrics_window_start: DateTime<Utc>,
    pub target_metrics_window_start: DateTime<Utc>,
    pub source_snapshot_age_seconds: u64,
    pub source_rows: usize,
    pub inserted_rows: usize,
    pub stale_source: bool,
    pub forced_stale: bool,
    pub dry_run: bool,
    pub top_wallets: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateReadinessBlocker {
    WritesDisabledByConfig,
    ReadsDisabledByConfig,
    MissingCoveredSince,
    MissingCoveredThroughCursor,
    CoveredSincePendingBackfillCompletion,
    CoveredThroughCursorPendingBackfillCompletion,
    MaterializationGapLatched,
    CoveredSinceAfterWindowStart,
    CoveredThroughTooStaleForRuntimeGate,
    CoveredThroughTooStaleForAuditLag,
    BackfillInProgress,
    BackfillResumeRequired,
    BackfillProtectionActive,
}

impl AggregateReadinessBlocker {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WritesDisabledByConfig => "writes_disabled_by_config",
            Self::ReadsDisabledByConfig => "reads_disabled_by_config",
            Self::MissingCoveredSince => "missing_covered_since",
            Self::MissingCoveredThroughCursor => "missing_covered_through_cursor",
            Self::CoveredSincePendingBackfillCompletion => {
                "covered_since_pending_backfill_completion"
            }
            Self::CoveredThroughCursorPendingBackfillCompletion => {
                "covered_through_cursor_pending_backfill_completion"
            }
            Self::MaterializationGapLatched => "materialization_gap_latched",
            Self::CoveredSinceAfterWindowStart => "covered_since_after_window_start",
            Self::CoveredThroughTooStaleForRuntimeGate => {
                "covered_through_too_stale_for_runtime_gate"
            }
            Self::CoveredThroughTooStaleForAuditLag => "covered_through_too_stale_for_audit_lag",
            Self::BackfillInProgress => "backfill_in_progress",
            Self::BackfillResumeRequired => "backfill_resume_required",
            Self::BackfillProtectionActive => "backfill_protection_active",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateBackfillProgressStatus {
    pub start_ts: DateTime<Utc>,
    pub cursor: DiscoveryRuntimeCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateReadinessStatus {
    pub window_start: DateTime<Utc>,
    pub writes_enabled: bool,
    pub reads_enabled: bool,
    pub runtime_gate_max_lag_seconds: u64,
    pub audit_max_lag_buckets: u64,
    pub audit_max_lag_seconds: u64,
    pub covered_since: Option<DateTime<Utc>>,
    pub covered_through_ts: Option<DateTime<Utc>>,
    pub covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub covered_through_lag_seconds: Option<u64>,
    pub materialization_gap_cursor: Option<DiscoveryRuntimeCursor>,
    pub backfill_progress: Option<AggregateBackfillProgressStatus>,
    pub backfill_protected_since: Option<DateTime<Utc>>,
    pub backfill_active: bool,
    pub backfill_resume_required: bool,
    pub coverage_markers_pending_backfill_completion: bool,
    pub scoring_horizon_covered: bool,
    pub covered_through_within_runtime_lag: bool,
    pub covered_through_within_audit_lag: bool,
    pub storage_ready_for_runtime_gate: bool,
    pub effective_writes_ready: bool,
    pub effective_reads_ready: bool,
    pub write_blockers: Vec<AggregateReadinessBlocker>,
    pub read_blockers: Vec<AggregateReadinessBlocker>,
}

fn trusted_snapshot_id(
    source_kind: TrustedSnapshotSourceKind,
    effective_window_start: DateTime<Utc>,
) -> String {
    format!(
        "wallet_metrics:{}:{}",
        source_kind.as_str(),
        effective_window_start.to_rfc3339()
    )
}

fn trusted_snapshot_write(
    source_kind: TrustedSnapshotSourceKind,
    trust_state: TrustedSelectionState,
    effective_window_start: DateTime<Utc>,
    created_at: DateTime<Utc>,
    row_count: usize,
    source_snapshot_id: Option<String>,
    source_window_start: Option<DateTime<Utc>>,
) -> TrustedWalletMetricsSnapshotWrite {
    TrustedWalletMetricsSnapshotWrite {
        snapshot_id: trusted_snapshot_id(source_kind, effective_window_start),
        source_snapshot_id,
        source_window_start,
        effective_window_start,
        created_at,
        source_kind,
        row_count,
        trust_state,
    }
}

fn snapshot_publish_time(
    source_window_start: DateTime<Utc>,
    scoring_window_days: i64,
) -> DateTime<Utc> {
    source_window_start + Duration::days(scoring_window_days.max(1))
}

fn snapshot_age_seconds_since_publish(
    now: DateTime<Utc>,
    source_window_start: DateTime<Utc>,
    scoring_window_days: i64,
) -> u64 {
    now.signed_duration_since(snapshot_publish_time(
        source_window_start,
        scoring_window_days,
    ))
    .num_seconds()
    .max(0) as u64
}

impl DiscoverySummary {
    fn with_runtime_mode(mut self, runtime_mode: DiscoveryRuntimeMode) -> Self {
        self.runtime_mode = runtime_mode;
        self.trusted_selection_fail_closed = matches!(
            runtime_mode,
            DiscoveryRuntimeMode::FailClosed | DiscoveryRuntimeMode::BootstrapDegraded
        );
        self
    }

    fn with_scoring_source(mut self, scoring_source: &'static str) -> Self {
        self.scoring_source = scoring_source;
        self
    }

    fn with_cap_truncation_telemetry(mut self, telemetry: &CapTruncationTelemetrySnapshot) -> Self {
        self.raw_window_cap_truncated = telemetry.raw_window_cap_truncated;
        self.cap_truncation_deactivation_guard_active =
            telemetry.cap_truncation_deactivation_guard_active;
        self.cap_truncation_deactivation_guard_reason =
            telemetry.cap_truncation_deactivation_guard_reason;
        self.cap_truncation_deactivation_guard_started_at =
            telemetry.cap_truncation_deactivation_guard_started_at;
        self.cap_truncation_floor_ts_utc = telemetry.cap_truncation_floor_ts_utc;
        self.cap_truncation_floor_signature = telemetry.cap_truncation_floor_signature.clone();
        self
    }

    fn with_persisted_stream_catch_up_requested(mut self, requested: bool) -> Self {
        self.persisted_stream_catch_up_requested = requested;
        self
    }

    fn with_wallet_freshness_capture(
        mut self,
        telemetry: &InBandWalletFreshnessCaptureTelemetry,
    ) -> Self {
        self.wallet_freshness_capture_state = Some(telemetry.state);
        self.wallet_freshness_capture_reason = telemetry.reason.clone();
        self.wallet_freshness_capture_id = telemetry.capture_id;
        self.wallet_freshness_capture_captured_at = telemetry.captured_at;
        self
    }
}

#[derive(Debug, Clone)]
pub struct RuntimePublishedUniverseTruth {
    pub runtime_mode: DiscoveryRuntimeMode,
    pub reason: String,
    pub last_published_at: DateTime<Utc>,
    pub last_published_window_start: DateTime<Utc>,
    pub published_scoring_source: Option<String>,
    pub published_wallet_ids: Vec<String>,
}

impl RuntimePublishedUniverseTruth {
    pub fn active_wallets(&self) -> HashSet<String> {
        self.published_wallet_ids.iter().cloned().collect()
    }
}

#[derive(Debug, Clone)]
pub enum RuntimePublicationTruthResolution {
    Recent(RuntimePublishedUniverseTruth),
    BootstrapDegraded(RuntimePublishedUniverseTruth),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalletSnapshot {
    wallet_id: String,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
    pnl_sol: f64,
    win_rate: f64,
    trades: u32,
    closed_trades: u32,
    hold_median_seconds: i64,
    score: f64,
    buy_total: u32,
    tradable_ratio: f64,
    rug_ratio: f64,
    eligible: bool,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct RugMetrics {
    evaluated: u32,
    rugged: u32,
    unevaluated: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuyFactRugStatus {
    Healthy,
    Rugged,
    Unevaluated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Lot {
    qty: f64,
    cost_sol: f64,
    opened_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
struct BuyObservation {
    token: String,
    ts: DateTime<Utc>,
    tradable: bool,
    quality_resolved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingBuyRugCheck {
    token: String,
    wallet_id: String,
    buy_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Default)]
struct FetchProgress {
    query_rows: usize,
    query_rows_last_page: usize,
    pages: usize,
    saturated: bool,
    page_budget_exhausted: bool,
    time_budget_exhausted: bool,
}

#[derive(Debug, Clone)]
enum PreparedCycleState {
    Cached {
        publish_due: bool,
        followlist_activations_suppressed: bool,
        followlist_deactivations_suppressed: bool,
        summary: DiscoverySummary,
        current_raw: Option<CachedCurrentRawTruthSample>,
    },
    Degraded {
        publish_due: bool,
        active_wallets: HashSet<String>,
        scoring_source: &'static str,
    },
    BootstrapDegraded {
        active_wallets: HashSet<String>,
        scoring_source: &'static str,
    },
    Unusable {
        publish_due: bool,
        scoring_source: &'static str,
    },
    PersistedRecompute {
        publish_due: bool,
        scoring_source: &'static str,
        empty_window_degraded_scoring_source: &'static str,
        empty_window_bootstrap_degraded_scoring_source: &'static str,
        empty_window_unusable_scoring_source: &'static str,
    },
    Recompute {
        publish_due: bool,
        followlist_activations_suppressed: bool,
        followlist_deactivations_suppressed: bool,
        metrics_persistence_suppressed: bool,
        swaps: VecDeque<SwapEvent>,
    },
}

#[derive(Debug, Clone, Default)]
struct InBandWalletFreshnessCaptureTelemetry {
    state: &'static str,
    reason: Option<String>,
    capture_id: Option<i64>,
    captured_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SolLegTrade {
    ts: DateTime<Utc>,
    wallet_id: String,
    sol_notional: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct TokenRollingState {
    first_seen: Option<DateTime<Utc>>,
    wallets_seen: HashSet<String>,
    sol_trades_5m: VecDeque<SolLegTrade>,
    sol_volume_5m: f64,
    sol_traders_5m: HashMap<String, u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct WalletAccumulator {
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    trades: u32,
    #[serde(default)]
    exact_active_day_count: Option<u32>,
    spent_sol: f64,
    realized_pnl_sol: f64,
    max_buy_notional_sol: f64,
    wins: u32,
    closed_trades: u32,
    hold_samples_sec: Vec<i64>,
    active_days: HashSet<NaiveDate>,
    realized_pnl_by_day: HashMap<NaiveDate, f64>,
    tx_per_minute: HashMap<i64, u32>,
    suspicious: bool,
    positions: HashMap<String, VecDeque<Lot>>,
    buy_total: u32,
    quality_resolved_buys: u32,
    tradable_buys: u32,
    rug_metrics: RugMetrics,
    buy_observations: Vec<BuyObservation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
enum CollectBuyMintsMode {
    #[default]
    FreshScan,
    ReconcileExpiredHead,
    ReconcileNewTail,
}

impl CollectBuyMintsMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::FreshScan => "fresh_scan",
            Self::ReconcileExpiredHead => "reconcile_expired_head",
            Self::ReconcileNewTail => "reconcile_new_tail",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
enum ReplayMode {
    #[default]
    LegacyFullWindow,
    WalletStatsThenSolLeg,
}

impl ReplayMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::LegacyFullWindow => "legacy_full_window",
            Self::WalletStatsThenSolLeg => "wallet_stats_then_sol_leg",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
struct ReplayWalletStatsDayCountSourceProgress {
    fast_path_pages_processed: usize,
    fallback_pages_processed: usize,
    fast_path_wallets_processed: usize,
    fallback_wallets_processed: usize,
}

impl ReplayWalletStatsDayCountSourceProgress {
    fn observe_page(
        &mut self,
        source: Option<ObservedWalletActivityDayCountSource>,
        wallet_count: usize,
    ) {
        match source {
            Some(ObservedWalletActivityDayCountSource::WalletActivityDays) => {
                self.fast_path_pages_processed = self.fast_path_pages_processed.saturating_add(1);
                self.fast_path_wallets_processed = self
                    .fast_path_wallets_processed
                    .saturating_add(wallet_count);
            }
            Some(ObservedWalletActivityDayCountSource::ObservedSwapsFallback) => {
                self.fallback_pages_processed = self.fallback_pages_processed.saturating_add(1);
                self.fallback_wallets_processed =
                    self.fallback_wallets_processed.saturating_add(wallet_count);
            }
            None => {}
        }
    }

    fn merge(&mut self, other: Self) {
        self.fast_path_pages_processed = self
            .fast_path_pages_processed
            .saturating_add(other.fast_path_pages_processed);
        self.fallback_pages_processed = self
            .fallback_pages_processed
            .saturating_add(other.fallback_pages_processed);
        self.fast_path_wallets_processed = self
            .fast_path_wallets_processed
            .saturating_add(other.fast_path_wallets_processed);
        self.fallback_wallets_processed = self
            .fallback_wallets_processed
            .saturating_add(other.fallback_wallets_processed);
    }

    fn merged_with(mut self, other: Self) -> Self {
        self.merge(other);
        self
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedStreamRebuildPayload {
    unique_buy_mints: Vec<String>,
    #[serde(default)]
    buy_mint_counts: BTreeMap<String, u32>,
    #[serde(default)]
    collect_buy_mints_cursor_token: Option<String>,
    #[serde(default)]
    collect_buy_mints_prepass_complete: bool,
    #[serde(default)]
    collect_buy_mints_mode: CollectBuyMintsMode,
    #[serde(default)]
    collect_buy_mints_reconcile_source_window_start: Option<DateTime<Utc>>,
    #[serde(default)]
    collect_buy_mints_reconcile_source_horizon_end: Option<DateTime<Utc>>,
    #[serde(default)]
    collect_buy_mints_reconcile_expired_head_cursor: Option<DiscoveryRuntimeCursor>,
    #[serde(default)]
    collect_buy_mints_reconcile_new_tail_cursor: Option<DiscoveryRuntimeCursor>,
    #[serde(default)]
    collect_buy_mints_reconcile_expired_head_cursor_token: Option<String>,
    #[serde(default)]
    collect_buy_mints_reconcile_new_tail_cursor_token: Option<String>,
    #[serde(default)]
    collect_buy_mints_reconcile_expired_head_pending_mints: Vec<String>,
    #[serde(default)]
    collect_buy_mints_reconcile_new_tail_slice_end_token: Option<String>,
    #[serde(default)]
    collect_buy_mints_reconcile_new_tail_pending_mints: Vec<String>,
    #[serde(default)]
    replay_mode: ReplayMode,
    #[serde(default)]
    replay_wallet_stats_complete: bool,
    #[serde(default)]
    replay_wallet_stats_rows_processed: usize,
    #[serde(default)]
    replay_wallet_stats_pages_processed: usize,
    #[serde(default)]
    replay_wallet_stats_wallet_cursor: Option<String>,
    #[serde(default)]
    replay_wallet_stats_day_count_source_progress: ReplayWalletStatsDayCountSourceProgress,
    token_quality_cache: HashMap<String, quality_cache::TokenQualityResolution>,
    token_quality_progress: quality_cache::TokenQualityResolutionProgress,
    by_wallet: HashMap<String, WalletAccumulator>,
    token_states: HashMap<String, TokenRollingState>,
    token_recent_sol_trades: HashMap<String, VecDeque<SolLegTrade>>,
    pending_rug_checks: VecDeque<PendingBuyRugCheck>,
    token_pending_buy_starts: HashMap<String, VecDeque<DateTime<Utc>>>,
    completed_snapshots: Vec<WalletSnapshot>,
}

#[derive(Debug, Clone)]
struct PersistedStreamRebuildState {
    phase: DiscoveryPersistedRebuildPhase,
    window_start: DateTime<Utc>,
    horizon_end: DateTime<Utc>,
    metrics_window_start: DateTime<Utc>,
    phase_cursor: Option<DiscoveryRuntimeCursor>,
    prepass_rows_processed: usize,
    prepass_pages_processed: usize,
    replay_rows_processed: usize,
    replay_pages_processed: usize,
    chunks_completed: usize,
    started_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    payload: PersistedStreamRebuildPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistedStreamBudgetExhaustedReason {
    TimeBudget,
    PageBudget,
}

impl PersistedStreamBudgetExhaustedReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::TimeBudget => "time_budget",
            Self::PageBudget => "page_budget",
        }
    }
}

#[derive(Debug, Clone)]
struct PersistedStreamProgressTelemetry {
    phase: DiscoveryPersistedRebuildPhase,
    collect_buy_mints_mode: CollectBuyMintsMode,
    replay_mode: ReplayMode,
    window_start: DateTime<Utc>,
    horizon_end: DateTime<Utc>,
    metrics_window_start: DateTime<Utc>,
    phase_cursor: Option<DiscoveryRuntimeCursor>,
    collect_buy_mints_cursor_token: Option<String>,
    collect_buy_mints_reconcile_source_window_start: Option<DateTime<Utc>>,
    collect_buy_mints_reconcile_source_horizon_end: Option<DateTime<Utc>>,
    collect_buy_mints_reconcile_expired_head_cursor: Option<DiscoveryRuntimeCursor>,
    collect_buy_mints_reconcile_new_tail_cursor: Option<DiscoveryRuntimeCursor>,
    collect_buy_mints_reconcile_expired_head_cursor_token: Option<String>,
    collect_buy_mints_reconcile_new_tail_cursor_token: Option<String>,
    collect_buy_mints_reconcile_expired_head_pending_mints: usize,
    collect_buy_mints_reconcile_new_tail_slice_end_token: Option<String>,
    collect_buy_mints_reconcile_new_tail_pending_mints: usize,
    prepass_rows_processed: usize,
    prepass_pages_processed: usize,
    replay_wallet_stats_complete: bool,
    replay_wallet_stats_rows_processed: usize,
    replay_wallet_stats_pages_processed: usize,
    replay_wallet_stats_day_count_source_progress: ReplayWalletStatsDayCountSourceProgress,
    replay_sol_leg_access_path: Option<ObservedSolLegCursorAccessPath>,
    replay_rows_processed: usize,
    replay_pages_processed: usize,
    chunks_completed: usize,
    cycle_rows_processed: usize,
    cycle_pages_processed: usize,
    cycle_replay_wallet_stats_day_count_source_progress: ReplayWalletStatsDayCountSourceProgress,
    cycle_unique_buy_mints_discovered: usize,
    observed_swaps_loaded: usize,
    unique_buy_mints: usize,
    quality_next_mint_index: usize,
    quality_rpc_attempted: usize,
    quality_rpc_spent_ms: u64,
    wallets_buffered: usize,
    started_at: DateTime<Utc>,
    cycle_elapsed_ms: u64,
    total_elapsed_ms: u64,
    partial: bool,
    completed: bool,
    budget_exhausted_reason: Option<PersistedStreamBudgetExhaustedReason>,
}

fn should_request_persisted_stream_catch_up(telemetry: &PersistedStreamProgressTelemetry) -> bool {
    if telemetry.phase == DiscoveryPersistedRebuildPhase::Replay {
        return !telemetry.replay_wallet_stats_complete
            && telemetry.budget_exhausted_reason.is_some();
    }
    if telemetry.phase != DiscoveryPersistedRebuildPhase::CollectBuyMints {
        return false;
    }
    matches!(
        (
            telemetry.collect_buy_mints_mode,
            telemetry.budget_exhausted_reason,
        ),
        (_, Some(PersistedStreamBudgetExhaustedReason::PageBudget))
            | (
                CollectBuyMintsMode::FreshScan,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget)
            )
    )
}

#[derive(Debug)]
enum PersistedStreamRebuildAdvanceOutcome {
    Completed {
        snapshots: Vec<WalletSnapshot>,
        telemetry: PersistedStreamProgressTelemetry,
    },
    InProgress {
        telemetry: PersistedStreamProgressTelemetry,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistedStreamRebuildRestoreOutcome {
    StartedFresh,
    ResumedExisting,
    CarriedForwardMetricsWindow,
    ResumedStaleMetricsWindow,
}

#[derive(Debug, Clone)]
struct PersistedStreamPhaseAdvance {
    rows_processed: usize,
    pages_processed: usize,
    replay_wallet_stats_rows_processed: usize,
    replay_wallet_stats_pages_processed: usize,
    replay_wallet_stats_day_count_source_progress: ReplayWalletStatsDayCountSourceProgress,
    replay_sol_leg_access_path: Option<ObservedSolLegCursorAccessPath>,
    source_exhausted: bool,
    phase_cursor: Option<DiscoveryRuntimeCursor>,
    collect_buy_mints_cursor_token: Option<String>,
    unique_buy_mints_discovered: usize,
    budget_exhausted_reason: Option<PersistedStreamBudgetExhaustedReason>,
}

impl DiscoveryService {
    pub fn new(config: DiscoveryConfig, shadow_quality: ShadowConfig) -> Self {
        Self::new_with_helius(config, shadow_quality, None)
    }

    pub fn new_with_helius(
        config: DiscoveryConfig,
        shadow_quality: ShadowConfig,
        helius_http_url: Option<String>,
    ) -> Self {
        let helius_http_url = helius_http_url
            .map(|url| url.trim().to_string())
            .filter(|url| !url.is_empty() && !url.contains("REPLACE_ME"));
        Self {
            config,
            shadow_quality,
            helius_http_url,
            window_state: Arc::new(Mutex::new(DiscoveryWindowState::default())),
        }
    }

    pub fn aggregate_readiness_status(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<AggregateReadinessStatus> {
        let window_start = now - Duration::days(self.config.scoring_window_days.max(1) as i64);
        let runtime_gate_max_lag_seconds = self.config.refresh_seconds.max(1);
        let audit_max_lag_seconds = self
            .config
            .metric_snapshot_interval_seconds
            .max(1)
            .saturating_mul(AGGREGATE_READINESS_MAX_LAG_BUCKETS);
        let covered_since = store.load_discovery_scoring_covered_since()?;
        let covered_through_ts = store.load_discovery_scoring_covered_through()?;
        let covered_through_cursor = store.load_discovery_scoring_covered_through_cursor()?;
        let materialization_gap_cursor =
            store.load_discovery_scoring_materialization_gap_cursor()?;
        let backfill_progress = store
            .load_discovery_scoring_backfill_progress()?
            .map(|(start_ts, cursor)| AggregateBackfillProgressStatus { start_ts, cursor });
        let backfill_protected_since =
            store.load_discovery_scoring_backfill_protected_since(now)?;
        let backfill_active = backfill_protected_since.is_some();
        let backfill_resume_required = backfill_progress.is_some() && !backfill_active;

        let scoring_horizon_covered =
            covered_since.is_some_and(|covered_since| covered_since <= window_start);
        let covered_through_lag_seconds = covered_through_ts.map(|covered_through_ts| {
            now.signed_duration_since(covered_through_ts)
                .num_seconds()
                .max(0) as u64
        });
        let covered_through_within_runtime_lag =
            covered_through_cursor
                .as_ref()
                .is_some_and(|covered_through_cursor| {
                    covered_through_cursor.ts_utc
                        + Duration::seconds(runtime_gate_max_lag_seconds as i64)
                        >= now
                });
        let covered_through_within_audit_lag =
            covered_through_lag_seconds.is_some_and(|covered_through_lag_seconds| {
                covered_through_lag_seconds <= audit_max_lag_seconds
            });
        let storage_ready_for_runtime_gate = scoring_horizon_covered
            && materialization_gap_cursor.is_none()
            && covered_through_within_runtime_lag;
        let coverage_markers_pending_backfill_completion = backfill_progress.is_some()
            && (covered_since.is_none() || covered_through_cursor.is_none());

        let mut write_blockers = Vec::new();
        if !self.config.scoring_aggregates_write_enabled {
            write_blockers.push(AggregateReadinessBlocker::WritesDisabledByConfig);
        }
        if covered_through_cursor.is_none() {
            write_blockers.push(if backfill_progress.is_some() {
                AggregateReadinessBlocker::CoveredThroughCursorPendingBackfillCompletion
            } else {
                AggregateReadinessBlocker::MissingCoveredThroughCursor
            });
        }
        if materialization_gap_cursor.is_some() {
            write_blockers.push(AggregateReadinessBlocker::MaterializationGapLatched);
        }
        if backfill_active {
            write_blockers.push(AggregateReadinessBlocker::BackfillInProgress);
        }
        if backfill_resume_required {
            write_blockers.push(AggregateReadinessBlocker::BackfillResumeRequired);
        }

        let mut read_blockers = Vec::new();
        if !self.config.scoring_aggregates_enabled {
            read_blockers.push(AggregateReadinessBlocker::ReadsDisabledByConfig);
        }
        match covered_since {
            None => read_blockers.push(if backfill_progress.is_some() {
                AggregateReadinessBlocker::CoveredSincePendingBackfillCompletion
            } else {
                AggregateReadinessBlocker::MissingCoveredSince
            }),
            Some(covered_since) if covered_since > window_start => {
                read_blockers.push(AggregateReadinessBlocker::CoveredSinceAfterWindowStart);
            }
            Some(_) => {}
        }
        if covered_through_cursor.is_none() {
            read_blockers.push(if backfill_progress.is_some() {
                AggregateReadinessBlocker::CoveredThroughCursorPendingBackfillCompletion
            } else {
                AggregateReadinessBlocker::MissingCoveredThroughCursor
            });
        } else if !covered_through_within_runtime_lag {
            read_blockers.push(AggregateReadinessBlocker::CoveredThroughTooStaleForRuntimeGate);
        }
        if materialization_gap_cursor.is_some() {
            read_blockers.push(AggregateReadinessBlocker::MaterializationGapLatched);
        }
        if covered_through_ts.is_some() && !covered_through_within_audit_lag {
            read_blockers.push(AggregateReadinessBlocker::CoveredThroughTooStaleForAuditLag);
        }
        if backfill_active {
            read_blockers.push(AggregateReadinessBlocker::BackfillInProgress);
        }
        if backfill_resume_required {
            read_blockers.push(AggregateReadinessBlocker::BackfillResumeRequired);
        }

        Ok(AggregateReadinessStatus {
            window_start,
            writes_enabled: self.config.scoring_aggregates_write_enabled,
            reads_enabled: self.config.scoring_aggregates_enabled,
            runtime_gate_max_lag_seconds,
            audit_max_lag_buckets: AGGREGATE_READINESS_MAX_LAG_BUCKETS,
            audit_max_lag_seconds,
            covered_since,
            covered_through_ts,
            covered_through_cursor,
            covered_through_lag_seconds,
            materialization_gap_cursor,
            backfill_progress,
            backfill_protected_since,
            backfill_active,
            backfill_resume_required,
            coverage_markers_pending_backfill_completion,
            scoring_horizon_covered,
            covered_through_within_runtime_lag,
            covered_through_within_audit_lag,
            storage_ready_for_runtime_gate,
            effective_writes_ready: write_blockers.is_empty(),
            effective_reads_ready: self.config.scoring_aggregates_enabled
                && storage_ready_for_runtime_gate
                && read_blockers.is_empty(),
            write_blockers,
            read_blockers,
        })
    }

    pub fn published_universe_max_age(&self) -> Duration {
        self.publication_freshness_gate()
            .published_universe_max_age()
    }

    pub fn runtime_published_universe_max_age(&self) -> Duration {
        self.published_universe_max_age()
    }

    pub fn publication_freshness_gate(&self) -> DiscoveryPublicationFreshnessGate {
        DiscoveryPublicationFreshnessGate {
            scoring_window_days: self.runtime_scoring_window_days(),
            metric_snapshot_interval_seconds: self.runtime_metric_snapshot_interval_seconds(),
            refresh_seconds: self.config.refresh_seconds,
        }
    }

    pub fn runtime_scoring_window_days(&self) -> i64 {
        self.config.scoring_window_days as i64
    }

    pub fn runtime_metric_snapshot_interval_seconds(&self) -> u64 {
        self.config.metric_snapshot_interval_seconds
    }

    fn runtime_publication_truth_from_state(
        publication_state: DiscoveryPublicationStateRow,
    ) -> Option<RuntimePublishedUniverseTruth> {
        Some(RuntimePublishedUniverseTruth {
            runtime_mode: publication_state.runtime_mode,
            reason: publication_state.reason,
            last_published_at: publication_state.last_published_at?,
            last_published_window_start: publication_state.last_published_window_start?,
            published_scoring_source: publication_state.published_scoring_source,
            published_wallet_ids: publication_state
                .published_wallet_ids
                .filter(|wallet_ids| !wallet_ids.is_empty())?,
        })
    }

    pub fn runtime_publication_truth_resolution(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<Option<RuntimePublicationTruthResolution>> {
        let Some(publication_state) = store.discovery_publication_state_read_only()? else {
            return Ok(None);
        };
        if publication_state.runtime_mode == DiscoveryRuntimeMode::FailClosed {
            return Ok(None);
        }
        let Some(runtime_truth) =
            Self::runtime_publication_truth_from_state(publication_state.clone())
        else {
            return Ok(None);
        };
        if publication_state.is_fresh_under_gate(self.publication_freshness_gate(), now) {
            return Ok(Some(RuntimePublicationTruthResolution::Recent(
                runtime_truth,
            )));
        }
        if store.discovery_bootstrap_degraded_state_read_only()?.active {
            let mut bootstrap_runtime_truth = runtime_truth;
            bootstrap_runtime_truth.runtime_mode = DiscoveryRuntimeMode::BootstrapDegraded;
            return Ok(Some(RuntimePublicationTruthResolution::BootstrapDegraded(
                bootstrap_runtime_truth,
            )));
        }
        Ok(None)
    }

    pub fn recent_runtime_publication_truth(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<Option<RuntimePublishedUniverseTruth>> {
        Ok(
            match self.runtime_publication_truth_resolution(store, now)? {
                Some(RuntimePublicationTruthResolution::Recent(truth)) => Some(truth),
                Some(RuntimePublicationTruthResolution::BootstrapDegraded(_)) | None => None,
            },
        )
    }

    pub fn bootstrap_degraded_runtime_publication_truth(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<Option<RuntimePublishedUniverseTruth>> {
        Ok(
            match self.runtime_publication_truth_resolution(store, now)? {
                Some(RuntimePublicationTruthResolution::BootstrapDegraded(truth)) => Some(truth),
                Some(RuntimePublicationTruthResolution::Recent(_)) | None => None,
            },
        )
    }

    pub fn recent_published_follow_universe_wallets(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<Option<HashSet<String>>> {
        Ok(self
            .recent_runtime_publication_truth(store, now)?
            .map(|truth| truth.active_wallets()))
    }

    fn persist_publication_state(
        &self,
        store: &SqliteStore,
        runtime_mode: DiscoveryRuntimeMode,
        publish_due: bool,
        published_window_start: DateTime<Utc>,
        published_wallet_ids: Option<&[String]>,
        scoring_source: &'static str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        if !publish_due {
            return Ok(());
        }
        let published_universe = runtime_mode == DiscoveryRuntimeMode::Healthy;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode,
            reason: reason.to_string(),
            last_published_at: published_universe.then_some(now),
            last_published_window_start: published_universe.then_some(published_window_start),
            published_scoring_source: Some(scoring_source.to_string()),
            published_wallet_ids: if published_universe {
                published_wallet_ids.map(|wallet_ids| wallet_ids.to_vec())
            } else {
                None
            },
        })
    }

    fn in_band_wallet_freshness_shadow_evidence_lookback_seconds(&self) -> u64 {
        self.config.refresh_seconds.max(1).saturating_mul(2)
    }

    fn maybe_persist_in_band_wallet_freshness_capture(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        capture_due: bool,
        runtime_mode: DiscoveryRuntimeMode,
        current_raw: Option<PrecomputedWalletFreshnessCurrentRawTruth>,
    ) -> InBandWalletFreshnessCaptureTelemetry {
        if !capture_due {
            return InBandWalletFreshnessCaptureTelemetry {
                state: "skipped_due_cadence",
                reason: Some("capture_cadence_not_due".to_string()),
                ..InBandWalletFreshnessCaptureTelemetry::default()
            };
        }
        if runtime_mode != DiscoveryRuntimeMode::Healthy {
            return InBandWalletFreshnessCaptureTelemetry {
                state: "skipped_runtime_mode",
                reason: Some(format!(
                    "runtime_mode_{}_does_not_publish_exact_stage3_raw_truth",
                    runtime_mode.as_str()
                )),
                ..InBandWalletFreshnessCaptureTelemetry::default()
            };
        }
        let Some(current_raw) = current_raw else {
            return InBandWalletFreshnessCaptureTelemetry {
                state: "skipped_missing_current_raw_truth",
                reason: Some("exact_current_raw_truth_not_cached_for_capture".to_string()),
                ..InBandWalletFreshnessCaptureTelemetry::default()
            };
        };

        let computed = match self.wallet_freshness_capture_snapshot_from_precomputed_current_raw(
            store,
            now,
            current_raw,
            Some(self.in_band_wallet_freshness_shadow_evidence_lookback_seconds()),
        ) {
            Ok(computed) => computed,
            Err(error) => {
                let error_text = format!("{error:#}");
                warn!(
                    error = %error,
                    "in-band Stage 3 wallet freshness capture build failed; continuing discovery refresh without persisted capture"
                );
                return InBandWalletFreshnessCaptureTelemetry {
                    state: "build_failed",
                    reason: Some(error_text),
                    ..InBandWalletFreshnessCaptureTelemetry::default()
                };
            }
        };

        let write = match computed.snapshot.to_storage_write() {
            Ok(write) => write,
            Err(error) => {
                let error_text = format!("{error:#}");
                warn!(
                    error = %error,
                    "in-band Stage 3 wallet freshness capture serialization failed; continuing discovery refresh without persisted capture"
                );
                return InBandWalletFreshnessCaptureTelemetry {
                    state: "build_failed",
                    reason: Some(error_text),
                    ..InBandWalletFreshnessCaptureTelemetry::default()
                };
            }
        };

        match store.append_discovery_wallet_freshness_capture(&write) {
            Ok(row) => InBandWalletFreshnessCaptureTelemetry {
                state: "persisted",
                reason: None,
                capture_id: Some(row.capture_id),
                captured_at: Some(row.captured_at),
            },
            Err(error) => {
                let error_text = format!("{error:#}");
                warn!(
                    error = %error,
                    "in-band Stage 3 wallet freshness capture persistence failed; continuing discovery refresh without persisted capture"
                );
                InBandWalletFreshnessCaptureTelemetry {
                    state: "persistence_failed",
                    reason: Some(error_text),
                    ..InBandWalletFreshnessCaptureTelemetry::default()
                }
            }
        }
    }

    fn degraded_summary_from_published_universe(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        publish_due: bool,
        active_wallets: HashSet<String>,
        cap_truncation_telemetry: &CapTruncationTelemetrySnapshot,
        scoring_source: &'static str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<DiscoverySummary> {
        let mut desired_wallets: Vec<String> = active_wallets.iter().cloned().collect();
        desired_wallets.sort();
        let follow_delta =
            store.persist_discovery_cycle(&[], &[], &desired_wallets, true, true, now, reason)?;
        let (wallets_seen, eligible_wallets) =
            self.published_universe_telemetry(store, now, &active_wallets)?;
        let mut top_wallets = desired_wallets.clone();
        top_wallets.truncate(5);
        let active_follow_wallets = store.list_active_follow_wallets()?.len();
        let summary = DiscoverySummary {
            window_start,
            wallets_seen,
            eligible_wallets,
            metrics_written: 0,
            follow_promoted: follow_delta.activated,
            follow_demoted: follow_delta.deactivated,
            active_follow_wallets,
            top_wallets,
            published: publish_due,
            ..DiscoverySummary::default()
        }
        .with_runtime_mode(DiscoveryRuntimeMode::Degraded)
        .with_scoring_source(scoring_source)
        .with_cap_truncation_telemetry(cap_truncation_telemetry);
        if publish_due {
            self.record_live_publish(now);
        }
        self.persist_publication_state(
            store,
            DiscoveryRuntimeMode::Degraded,
            publish_due,
            metrics_window_start,
            None,
            scoring_source,
            reason,
            now,
        )?;
        Ok(summary)
    }

    fn bootstrap_degraded_summary_from_published_universe(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        active_wallets: HashSet<String>,
        cap_truncation_telemetry: &CapTruncationTelemetrySnapshot,
        scoring_source: &'static str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<DiscoverySummary> {
        let bootstrap_state = store.discovery_bootstrap_degraded_state_read_only()?;
        let mut desired_wallets: Vec<String> = active_wallets.iter().cloned().collect();
        desired_wallets.sort();
        let follow_delta =
            store.persist_discovery_cycle(&[], &[], &desired_wallets, true, true, now, reason)?;
        store.set_discovery_bootstrap_degraded_state(
            true,
            Some(reason),
            bootstrap_state.armed_at.or(Some(now)),
        )?;
        let (wallets_seen, eligible_wallets) =
            self.published_universe_telemetry(store, now, &active_wallets)?;
        let mut top_wallets = desired_wallets.clone();
        top_wallets.truncate(5);
        let active_follow_wallets = store.list_active_follow_wallets()?.len();
        Ok(DiscoverySummary {
            window_start,
            wallets_seen,
            eligible_wallets,
            metrics_written: 0,
            follow_promoted: follow_delta.activated,
            follow_demoted: follow_delta.deactivated,
            active_follow_wallets,
            top_wallets,
            published: false,
            ..DiscoverySummary::default()
        }
        .with_runtime_mode(DiscoveryRuntimeMode::BootstrapDegraded)
        .with_scoring_source(scoring_source)
        .with_cap_truncation_telemetry(cap_truncation_telemetry))
    }

    fn published_universe_telemetry(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        active_wallets: &HashSet<String>,
    ) -> Result<(usize, usize)> {
        let Some(publication_state) = store.discovery_publication_state()? else {
            return Ok((active_wallets.len(), active_wallets.len()));
        };
        let Some(last_published_window_start) = publication_state.last_published_window_start
        else {
            return Ok((active_wallets.len(), active_wallets.len()));
        };
        let persisted_rows =
            store.load_wallet_metric_snapshots_for_window(last_published_window_start)?;
        if persisted_rows.is_empty() {
            return Ok((active_wallets.len(), active_wallets.len()));
        }
        let snapshots = self.wallet_snapshots_from_persisted_metric_rows(now, persisted_rows);
        let eligible_wallets = rank_follow_candidates(&snapshots, self.config.min_score).len();
        Ok((snapshots.len(), eligible_wallets))
    }

    fn fail_close_without_recent_universe(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        publish_due: bool,
        force_followlist_deactivation: bool,
        cap_truncation_telemetry: &CapTruncationTelemetrySnapshot,
        scoring_source: &'static str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<DiscoverySummary> {
        let follow_delta = store.persist_discovery_cycle(
            &[],
            &[],
            &[],
            false,
            publish_due || force_followlist_deactivation,
            now,
            reason,
        )?;
        self.persist_trusted_selection_state(
            store,
            TrustedSelectionState::Invalid,
            None,
            None,
            None,
            true,
            reason,
            now,
        )?;
        self.persist_publication_state(
            store,
            DiscoveryRuntimeMode::FailClosed,
            publish_due,
            metrics_window_start,
            None,
            scoring_source,
            reason,
            now,
        )?;
        if publish_due {
            self.record_live_publish(now);
        }
        let active_follow_wallets = store.list_active_follow_wallets()?.len();
        Ok(DiscoverySummary {
            window_start,
            wallets_seen: 0,
            eligible_wallets: 0,
            metrics_written: 0,
            follow_promoted: 0,
            follow_demoted: follow_delta.deactivated,
            active_follow_wallets,
            top_wallets: Vec::new(),
            published: publish_due,
            ..DiscoverySummary::default()
        }
        .with_runtime_mode(DiscoveryRuntimeMode::FailClosed)
        .with_scoring_source(scoring_source)
        .with_cap_truncation_telemetry(cap_truncation_telemetry))
    }

    fn persisted_observed_swaps_cover_window(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
    ) -> Result<bool> {
        let Some(oldest_persisted_observed_swap_ts) = store.oldest_observed_swap_timestamp()?
        else {
            return Ok(false);
        };
        if oldest_persisted_observed_swap_ts > window_start {
            return Ok(false);
        }
        let (recent_window_swaps, _) = store.load_recent_observed_swaps_since(window_start, 1)?;
        Ok(!recent_window_swaps.is_empty())
    }

    fn persisted_stream_rebuild_state_from_row(
        row: DiscoveryPersistedRebuildStateRow,
    ) -> Result<PersistedStreamRebuildState> {
        let payload: PersistedStreamRebuildPayload = serde_json::from_str(&row.state_json)
            .context("failed deserializing discovery persisted rebuild state payload")?;
        Ok(PersistedStreamRebuildState {
            phase: row.phase,
            window_start: row.window_start,
            horizon_end: row.horizon_end,
            metrics_window_start: row.metrics_window_start,
            phase_cursor: row.phase_cursor,
            prepass_rows_processed: row.prepass_rows_processed,
            prepass_pages_processed: row.prepass_pages_processed,
            replay_rows_processed: row.replay_rows_processed,
            replay_pages_processed: row.replay_pages_processed,
            chunks_completed: row.chunks_completed,
            started_at: row.started_at,
            updated_at: row.updated_at,
            payload,
        })
    }

    fn persisted_stream_rebuild_row(
        state: &PersistedStreamRebuildState,
        updated_at: DateTime<Utc>,
    ) -> Result<DiscoveryPersistedRebuildStateRow> {
        Ok(DiscoveryPersistedRebuildStateRow {
            phase: state.phase,
            window_start: state.window_start,
            horizon_end: state.horizon_end,
            metrics_window_start: state.metrics_window_start,
            phase_cursor: state.phase_cursor.clone(),
            prepass_rows_processed: state.prepass_rows_processed,
            prepass_pages_processed: state.prepass_pages_processed,
            replay_rows_processed: state.replay_rows_processed,
            replay_pages_processed: state.replay_pages_processed,
            chunks_completed: state.chunks_completed,
            state_json: serde_json::to_string(&state.payload)
                .context("failed serializing discovery persisted rebuild state payload")?,
            started_at: state.started_at,
            updated_at,
        })
    }

    fn start_persisted_stream_rebuild_state(
        &self,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> PersistedStreamRebuildState {
        PersistedStreamRebuildState {
            phase: DiscoveryPersistedRebuildPhase::CollectBuyMints,
            window_start,
            horizon_end: now,
            metrics_window_start,
            phase_cursor: None,
            prepass_rows_processed: 0,
            prepass_pages_processed: 0,
            replay_rows_processed: 0,
            replay_pages_processed: 0,
            chunks_completed: 0,
            started_at: now,
            updated_at: now,
            payload: PersistedStreamRebuildPayload {
                replay_mode: ReplayMode::WalletStatsThenSolLeg,
                ..PersistedStreamRebuildPayload::default()
            },
        }
    }

    fn sync_unique_buy_mints_from_counts(payload: &mut PersistedStreamRebuildPayload) {
        payload.unique_buy_mints = payload
            .buy_mint_counts
            .iter()
            .filter_map(|(mint, count)| (*count > 0).then_some(mint.clone()))
            .collect();
    }

    fn payload_has_exact_buy_mint_membership(payload: &PersistedStreamRebuildPayload) -> bool {
        payload.unique_buy_mints.is_empty()
            || payload.buy_mint_counts.len() == payload.unique_buy_mints.len()
    }

    fn state_can_carry_forward_metrics_rollover(state: &PersistedStreamRebuildState) -> bool {
        state.payload.collect_buy_mints_mode == CollectBuyMintsMode::FreshScan
            && Self::payload_has_exact_buy_mint_membership(&state.payload)
    }

    fn state_can_resume_stale_metrics_window_until_exact_checkpoint(
        state: &PersistedStreamRebuildState,
    ) -> bool {
        state.phase == DiscoveryPersistedRebuildPhase::CollectBuyMints
            && matches!(
                state.payload.collect_buy_mints_mode,
                CollectBuyMintsMode::ReconcileExpiredHead | CollectBuyMintsMode::ReconcileNewTail
            )
            && Self::payload_has_exact_buy_mint_membership(&state.payload)
    }

    fn stale_reconcile_token_batch_size(fetch_limit: usize) -> usize {
        fetch_limit.max(1).min(STALE_RECONCILE_TOKEN_BATCH_CAP)
    }

    fn narrowed_stale_reconcile_slice_end(sorted_candidate_mints: &[String]) -> Option<String> {
        if sorted_candidate_mints.len() <= 1 {
            return sorted_candidate_mints.last().cloned();
        }
        let narrowed_end_index = sorted_candidate_mints.len().saturating_sub(1) / 2;
        sorted_candidate_mints.get(narrowed_end_index).cloned()
    }

    fn clear_reconcile_new_tail_pending_batch(payload: &mut PersistedStreamRebuildPayload) {
        payload
            .collect_buy_mints_reconcile_new_tail_pending_mints
            .clear();
    }

    fn clear_reconcile_expired_head_pending_batch(payload: &mut PersistedStreamRebuildPayload) {
        payload
            .collect_buy_mints_reconcile_expired_head_pending_mints
            .clear();
    }

    fn stale_reconcile_exact_count_batch_size(fetch_limit: usize) -> usize {
        fetch_limit
            .max(1)
            .min(STALE_RECONCILE_TOKEN_BATCH_CAP)
            .min(STALE_RECONCILE_EXACT_COUNT_BATCH_CAP)
    }

    fn replay_wallet_stats_catch_up_page_limit(fetch_page_limit: usize) -> usize {
        fetch_page_limit
            .max(1)
            .saturating_mul(REPLAY_WALLET_STATS_CATCH_UP_PAGE_LIMIT_MULTIPLIER)
    }

    fn replay_wallet_stats_wallet_batch_size(fetch_limit: usize) -> usize {
        fetch_limit.max(1).min(REPLAY_WALLET_STATS_WALLET_BATCH_CAP)
    }

    fn observe_replay_wallet_activity_summary(
        payload: &mut PersistedStreamRebuildPayload,
        row: ObservedWalletActivityRow,
    ) {
        let entry = payload.by_wallet.entry(row.wallet_id).or_default();
        entry.first_seen = Some(
            entry
                .first_seen
                .map(|current| current.min(row.first_seen))
                .unwrap_or(row.first_seen),
        );
        entry.last_seen = Some(
            entry
                .last_seen
                .map(|current| current.max(row.last_seen))
                .unwrap_or(row.last_seen),
        );
        entry.trades = entry
            .trades
            .saturating_add(row.trades.min(u32::MAX as usize) as u32);
        entry.exact_active_day_count = Some(
            entry
                .exact_active_day_count
                .unwrap_or(0)
                .saturating_add(row.active_day_count),
        );
        entry.suspicious |= row.suspicious;
    }

    fn add_buy_mint_occurrences(
        payload: &mut PersistedStreamRebuildPayload,
        mint: &str,
        count: usize,
    ) -> bool {
        if count == 0 {
            return false;
        }
        let entry = payload.buy_mint_counts.entry(mint.to_string()).or_insert(0);
        let was_zero = *entry == 0;
        *entry = entry.saturating_add(count.min(u32::MAX as usize) as u32);
        if was_zero {
            Self::insert_unique_buy_mint(payload, mint);
        }
        was_zero
    }

    fn insert_unique_buy_mint(payload: &mut PersistedStreamRebuildPayload, mint: &str) {
        match payload
            .unique_buy_mints
            .binary_search_by(|existing| existing.as_str().cmp(mint))
        {
            Ok(_) => {}
            Err(index) => payload.unique_buy_mints.insert(index, mint.to_string()),
        }
    }

    fn remove_unique_buy_mint(payload: &mut PersistedStreamRebuildPayload, mint: &str) {
        if let Ok(index) = payload
            .unique_buy_mints
            .binary_search_by(|existing| existing.as_str().cmp(mint))
        {
            payload.unique_buy_mints.remove(index);
        }
    }

    fn set_buy_mint_occurrences(
        payload: &mut PersistedStreamRebuildPayload,
        mint: &str,
        count: usize,
    ) -> bool {
        let was_missing = payload.buy_mint_counts.get(mint).copied().unwrap_or(0) == 0;
        if count == 0 {
            payload.buy_mint_counts.remove(mint);
            if !was_missing {
                Self::remove_unique_buy_mint(payload, mint);
            }
            return was_missing;
        }
        payload
            .buy_mint_counts
            .insert(mint.to_string(), count.min(u32::MAX as usize) as u32);
        if was_missing {
            Self::insert_unique_buy_mint(payload, mint);
        }
        was_missing
    }

    fn subtract_buy_mint_occurrences(
        payload: &mut PersistedStreamRebuildPayload,
        mint: &str,
        count: usize,
    ) {
        if count == 0 {
            return;
        }
        let count = count.min(u32::MAX as usize) as u32;
        let remove = match payload.buy_mint_counts.get_mut(mint) {
            Some(existing) if *existing <= count => {
                *existing = 0;
                true
            }
            Some(existing) => {
                *existing -= count;
                false
            }
            None => false,
        };
        if remove {
            payload.buy_mint_counts.remove(mint);
            Self::remove_unique_buy_mint(payload, mint);
        }
    }

    fn reset_replay_wallet_stats_progress(payload: &mut PersistedStreamRebuildPayload) {
        payload.replay_wallet_stats_complete = false;
        payload.replay_wallet_stats_rows_processed = 0;
        payload.replay_wallet_stats_pages_processed = 0;
        payload.replay_wallet_stats_wallet_cursor = None;
        payload.replay_wallet_stats_day_count_source_progress =
            ReplayWalletStatsDayCountSourceProgress::default();
    }

    fn reset_bucket_sensitive_rebuild_state_for_rollover(state: &mut PersistedStreamRebuildState) {
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.phase_cursor = None;
        state.replay_rows_processed = 0;
        state.replay_pages_processed = 0;
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        Self::reset_replay_wallet_stats_progress(&mut state.payload);
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileExpiredHead;
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor = None;
        state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor_token = None;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token = None;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
        Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
        Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
        state.payload.token_quality_cache.clear();
        state.payload.token_quality_progress =
            quality_cache::TokenQualityResolutionProgress::default();
        state.payload.by_wallet.clear();
        state.payload.token_states.clear();
        state.payload.token_recent_sol_trades.clear();
        state.payload.pending_rug_checks.clear();
        state.payload.token_pending_buy_starts.clear();
        state.payload.completed_snapshots.clear();
    }

    fn reset_replay_progress_for_optimized_resume(state: &mut PersistedStreamRebuildState) {
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.phase_cursor = None;
        state.replay_rows_processed = 0;
        state.replay_pages_processed = 0;
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        Self::reset_replay_wallet_stats_progress(&mut state.payload);
        state.payload.by_wallet.clear();
        state.payload.token_states.clear();
        state.payload.token_recent_sol_trades.clear();
        state.payload.pending_rug_checks.clear();
        state.payload.token_pending_buy_starts.clear();
        state.payload.completed_snapshots.clear();
    }

    fn replay_checkpoint_has_local_progress(state: &PersistedStreamRebuildState) -> bool {
        state.replay_rows_processed > 0
            || state.replay_pages_processed > 0
            || state.phase_cursor.is_some()
            || !state.payload.by_wallet.is_empty()
            || !state.payload.token_states.is_empty()
            || !state.payload.token_recent_sol_trades.is_empty()
            || !state.payload.pending_rug_checks.is_empty()
            || !state.payload.token_pending_buy_starts.is_empty()
    }

    fn prepare_persisted_stream_rebuild_for_metrics_window_rollover(
        &self,
        state: &mut PersistedStreamRebuildState,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        if state.metrics_window_start == metrics_window_start {
            return Ok(false);
        }
        if !Self::state_can_carry_forward_metrics_rollover(state) {
            return Ok(false);
        }

        let source_window_start = state.window_start;
        let source_horizon_end = state.horizon_end;
        let source_metrics_window_start = state.metrics_window_start;
        let old_phase = state.phase;
        let old_collect_cursor = state.payload.collect_buy_mints_cursor_token.clone();
        let prepass_complete = state.payload.collect_buy_mints_prepass_complete
            || state.phase != DiscoveryPersistedRebuildPhase::CollectBuyMints;

        state.window_start = window_start;
        state.horizon_end = now;
        state.metrics_window_start = metrics_window_start;
        state.payload.collect_buy_mints_prepass_complete = prepass_complete;
        state.payload.collect_buy_mints_cursor_token = if prepass_complete {
            None
        } else {
            old_collect_cursor
        };
        state
            .payload
            .collect_buy_mints_reconcile_source_window_start = Some(source_window_start);
        state.payload.collect_buy_mints_reconcile_source_horizon_end = Some(source_horizon_end);
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor = None;
        state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor_token = None;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token = None;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
        Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
        Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
        Self::sync_unique_buy_mints_from_counts(&mut state.payload);
        Self::reset_bucket_sensitive_rebuild_state_for_rollover(state);
        info!(
            rebuild_previous_phase = old_phase.as_str(),
            rebuild_previous_window_start = %source_window_start,
            rebuild_previous_horizon_end = %source_horizon_end,
            rebuild_previous_metrics_window_start = %source_metrics_window_start,
            rebuild_target_window_start = %window_start,
            rebuild_target_horizon_end = %now,
            rebuild_target_metrics_window_start = %metrics_window_start,
            rebuild_collect_buy_mints_cursor_token =
                state.payload.collect_buy_mints_cursor_token.as_deref(),
            rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
            "carrying forward exact canonical buy-mint membership progress across metrics bucket rollover; bucket-sensitive quality/replay state reset for fresh target-window rebuild"
        );
        Ok(true)
    }

    fn persisted_stream_rebuild_restart_reason(
        &self,
        state: &PersistedStreamRebuildState,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Option<&'static str> {
        if state.window_start > window_start {
            return Some("window_start_in_future");
        }
        if state.horizon_end > now {
            return Some("horizon_in_future");
        }
        None
    }

    fn publish_pending_snapshots(state: &PersistedStreamRebuildState) -> Vec<WalletSnapshot> {
        state.payload.completed_snapshots.clone()
    }

    fn persisted_stream_observed_swaps_loaded(state: &PersistedStreamRebuildState) -> usize {
        match state.payload.replay_mode {
            ReplayMode::LegacyFullWindow => state.replay_rows_processed,
            ReplayMode::WalletStatsThenSolLeg => state.payload.replay_wallet_stats_rows_processed,
        }
    }

    fn canonicalize_unique_buy_mints(mints: &mut Vec<String>) -> bool {
        let original = mints.clone();
        mints.sort();
        mints.dedup();
        *mints != original
    }

    fn repair_collect_buy_mints_payload_for_cursor(
        &self,
        state: &mut PersistedStreamRebuildState,
    ) -> bool {
        let original_len = state.payload.unique_buy_mints.len();
        let changed = Self::canonicalize_unique_buy_mints(&mut state.payload.unique_buy_mints);
        if let Some(cursor_token) = state.payload.collect_buy_mints_cursor_token.as_deref() {
            state
                .payload
                .unique_buy_mints
                .retain(|mint| mint.as_str() <= cursor_token);
            state
                .payload
                .buy_mint_counts
                .retain(|mint, count| *count > 0 && mint.as_str() <= cursor_token);
        }
        let truncated = state.payload.unique_buy_mints.len() != original_len;
        if Self::payload_has_exact_buy_mint_membership(&state.payload) {
            Self::sync_unique_buy_mints_from_counts(&mut state.payload);
        }
        if changed || truncated {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_collect_buy_mints_cursor_token =
                    state.payload.collect_buy_mints_cursor_token.as_deref(),
                rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                rebuild_unique_buy_mints_dropped =
                    original_len.saturating_sub(state.payload.unique_buy_mints.len()),
                "repaired persisted collect_buy_mints checkpoint to canonical sorted prefix for the stored token cursor"
            );
        }
        changed || truncated
    }

    fn repair_restored_persisted_stream_state_for_resume(
        &self,
        state: &mut PersistedStreamRebuildState,
    ) {
        match state.phase {
            DiscoveryPersistedRebuildPhase::CollectBuyMints => {
                if Self::payload_has_exact_buy_mint_membership(&state.payload) {
                    Self::sync_unique_buy_mints_from_counts(&mut state.payload);
                }
                if state.payload.collect_buy_mints_cursor_token.is_some() {
                    self.repair_collect_buy_mints_payload_for_cursor(state);
                }
                if state.payload.collect_buy_mints_mode != CollectBuyMintsMode::FreshScan
                    && (state
                        .payload
                        .collect_buy_mints_reconcile_expired_head_cursor
                        .is_some()
                        || state
                            .payload
                            .collect_buy_mints_reconcile_new_tail_cursor
                            .is_some())
                {
                    warn!(
                        rebuild_window_start = %state.window_start,
                        rebuild_horizon_end = %state.horizon_end,
                        rebuild_phase = state.phase.as_str(),
                        rebuild_collect_buy_mints_mode = state.payload.collect_buy_mints_mode.as_str(),
                        "rewinding legacy raw-swap collect_buy_mints reconciliation cursor onto grouped buy-mint delta pagination before resume"
                    );
                    state
                        .payload
                        .collect_buy_mints_reconcile_expired_head_cursor = None;
                    state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
                    state
                        .payload
                        .collect_buy_mints_reconcile_expired_head_cursor_token = None;
                    state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_cursor_token = None;
                    state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
                    Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
                    Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
                }
            }
            DiscoveryPersistedRebuildPhase::ResolveTokenQuality => {
                if Self::payload_has_exact_buy_mint_membership(&state.payload) {
                    Self::sync_unique_buy_mints_from_counts(&mut state.payload);
                }
                if Self::canonicalize_unique_buy_mints(&mut state.payload.unique_buy_mints) {
                    warn!(
                        rebuild_window_start = %state.window_start,
                        rebuild_horizon_end = %state.horizon_end,
                        rebuild_phase = state.phase.as_str(),
                        rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                        "rewinding persisted token-quality progress onto canonical sorted buy-mint order before resume"
                    );
                    state.payload.token_quality_cache.clear();
                    state.payload.token_quality_progress =
                        quality_cache::TokenQualityResolutionProgress::default();
                }
            }
            DiscoveryPersistedRebuildPhase::Replay
            | DiscoveryPersistedRebuildPhase::PublishPending => {
                if Self::payload_has_exact_buy_mint_membership(&state.payload) {
                    Self::sync_unique_buy_mints_from_counts(&mut state.payload);
                }
                if state.phase == DiscoveryPersistedRebuildPhase::Replay
                    && state.payload.replay_mode == ReplayMode::LegacyFullWindow
                {
                    if Self::replay_checkpoint_has_local_progress(state) {
                        warn!(
                            rebuild_window_start = %state.window_start,
                            rebuild_horizon_end = %state.horizon_end,
                            rebuild_phase_cursor_ts = ?state.phase_cursor.as_ref().map(|cursor| cursor.ts_utc),
                            rebuild_phase_cursor_slot = state.phase_cursor.as_ref().map(|cursor| cursor.slot),
                            rebuild_phase_cursor_signature =
                                state.phase_cursor.as_ref().map(|cursor| cursor.signature.as_str()),
                            rebuild_replay_rows_processed = state.replay_rows_processed,
                            rebuild_replay_pages_processed = state.replay_pages_processed,
                            "rewinding legacy replay checkpoint onto optimized replay pipeline before resume"
                        );
                    } else {
                        info!(
                            rebuild_window_start = %state.window_start,
                            rebuild_horizon_end = %state.horizon_end,
                            rebuild_phase = state.phase.as_str(),
                            "upgrading zero-progress legacy replay checkpoint onto optimized replay pipeline before resume"
                        );
                    }
                    Self::reset_replay_progress_for_optimized_resume(state);
                }
                if Self::canonicalize_unique_buy_mints(&mut state.payload.unique_buy_mints) {
                    warn!(
                        rebuild_window_start = %state.window_start,
                        rebuild_horizon_end = %state.horizon_end,
                        rebuild_phase = state.phase.as_str(),
                        rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                        "rewinding persisted replay/publish checkpoint onto canonical sorted buy-mint order before resume"
                    );
                    state.phase = DiscoveryPersistedRebuildPhase::ResolveTokenQuality;
                    state.phase_cursor = None;
                    state.replay_rows_processed = 0;
                    state.replay_pages_processed = 0;
                    state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
                    Self::reset_replay_wallet_stats_progress(&mut state.payload);
                    state.payload.token_quality_cache.clear();
                    state.payload.token_quality_progress =
                        quality_cache::TokenQualityResolutionProgress::default();
                    state.payload.by_wallet.clear();
                    state.payload.token_states.clear();
                    state.payload.token_recent_sol_trades.clear();
                    state.payload.pending_rug_checks.clear();
                    state.payload.token_pending_buy_starts.clear();
                    state.payload.completed_snapshots.clear();
                }
            }
        }
    }

    fn derive_legacy_collect_buy_mints_safe_prefix_len(
        &self,
        store: &SqliteStore,
        state: &PersistedStreamRebuildState,
        canonical_mints: &[String],
        deadline: Instant,
    ) -> Result<(usize, bool)> {
        if canonical_mints.is_empty() {
            return Ok((0, false));
        }

        let mut lo = 0usize;
        let mut hi = canonical_mints.len();
        let mut safe_prefix_len = 0usize;
        while lo < hi {
            if Instant::now() >= deadline {
                return Ok((safe_prefix_len, true));
            }
            let mid = lo + (hi - lo) / 2;
            let observed_prefix = store
                .count_observed_buy_mints_in_window_up_to_token_with_budget(
                    state.window_start,
                    state.horizon_end,
                    &canonical_mints[mid],
                    deadline,
                )?;
            if observed_prefix.time_budget_exhausted {
                return Ok((safe_prefix_len, true));
            }
            if observed_prefix.count == mid.saturating_add(1) {
                safe_prefix_len = mid.saturating_add(1);
                lo = mid.saturating_add(1);
            } else {
                hi = mid;
            }
        }
        Ok((safe_prefix_len, false))
    }

    fn load_or_start_persisted_stream_rebuild_state(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(
        PersistedStreamRebuildState,
        PersistedStreamRebuildRestoreOutcome,
    )> {
        let Some(row) = store.load_discovery_persisted_rebuild_state()? else {
            return Ok((
                self.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now),
                PersistedStreamRebuildRestoreOutcome::StartedFresh,
            ));
        };
        match Self::persisted_stream_rebuild_state_from_row(row) {
            Ok(mut state) => {
                if let Some(reason) =
                    self.persisted_stream_rebuild_restart_reason(&state, window_start, now)
                {
                    warn!(
                        persisted_window_start = %state.window_start,
                        persisted_metrics_window_start = %state.metrics_window_start,
                        persisted_horizon_end = %state.horizon_end,
                        current_window_start = %window_start,
                        current_metrics_window_start = %metrics_window_start,
                        current_now = %now,
                        restart_reason = reason,
                        "discarding stale persisted discovery rebuild progress and restarting from a fresh frozen horizon"
                    );
                    store.clear_discovery_persisted_rebuild_state()?;
                    return Ok((
                        self.start_persisted_stream_rebuild_state(
                            window_start,
                            metrics_window_start,
                            now,
                        ),
                        PersistedStreamRebuildRestoreOutcome::StartedFresh,
                    ));
                }
                self.repair_restored_persisted_stream_state_for_resume(&mut state);
                if state.metrics_window_start != metrics_window_start {
                    if self.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                        &mut state,
                        window_start,
                        metrics_window_start,
                        now,
                    )? {
                        return Ok((
                            state,
                            PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow,
                        ));
                    }
                    if Self::state_can_resume_stale_metrics_window_until_exact_checkpoint(&state) {
                        warn!(
                            persisted_window_start = %state.window_start,
                            persisted_metrics_window_start = %state.metrics_window_start,
                            persisted_horizon_end = %state.horizon_end,
                            current_window_start = %window_start,
                            current_metrics_window_start = %metrics_window_start,
                            current_now = %now,
                            rebuild_phase = state.phase.as_str(),
                            rebuild_collect_buy_mints_mode =
                                state.payload.collect_buy_mints_mode.as_str(),
                            restart_reason =
                                "metrics_window_start_changed_awaiting_exact_carry_forward_checkpoint",
                            "resuming stale in-progress collect_buy_mints reconciliation on its frozen target window until the next exact carry-forward checkpoint is available"
                        );
                        return Ok((
                            state,
                            PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow,
                        ));
                    }
                    warn!(
                        persisted_window_start = %state.window_start,
                        persisted_metrics_window_start = %state.metrics_window_start,
                        persisted_horizon_end = %state.horizon_end,
                        current_window_start = %window_start,
                        current_metrics_window_start = %metrics_window_start,
                        current_now = %now,
                        restart_reason = "metrics_window_start_changed_without_exact_buy_mint_membership",
                        "discarding persisted discovery rebuild progress because the metrics bucket moved before exact canonical buy-mint membership state was available"
                    );
                    store.clear_discovery_persisted_rebuild_state()?;
                    return Ok((
                        self.start_persisted_stream_rebuild_state(
                            window_start,
                            metrics_window_start,
                            now,
                        ),
                        PersistedStreamRebuildRestoreOutcome::StartedFresh,
                    ));
                }
                Ok((state, PersistedStreamRebuildRestoreOutcome::ResumedExisting))
            }
            Err(error) => {
                warn!(
                    error = %error,
                    "failed restoring persisted discovery rebuild progress; restarting from a fresh frozen horizon"
                );
                store.clear_discovery_persisted_rebuild_state()?;
                Ok((
                    self.start_persisted_stream_rebuild_state(
                        window_start,
                        metrics_window_start,
                        now,
                    ),
                    PersistedStreamRebuildRestoreOutcome::StartedFresh,
                ))
            }
        }
    }

    fn persist_persisted_stream_rebuild_state(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        state.updated_at = updated_at;
        let row = Self::persisted_stream_rebuild_row(state, updated_at)?;
        store.upsert_discovery_persisted_rebuild_state(&row)
    }

    fn log_persisted_stream_progress(
        &self,
        telemetry: &PersistedStreamProgressTelemetry,
        message: &'static str,
    ) {
        info!(
            rebuild_phase = telemetry.phase.as_str(),
            rebuild_collect_buy_mints_mode = telemetry.collect_buy_mints_mode.as_str(),
            rebuild_replay_mode = telemetry.replay_mode.as_str(),
            rebuild_replay_sol_leg_access_path = telemetry
                .replay_sol_leg_access_path
                .map(ObservedSolLegCursorAccessPath::as_str),
            rebuild_window_start = %telemetry.window_start,
            rebuild_horizon_end = %telemetry.horizon_end,
            rebuild_metrics_window_start = %telemetry.metrics_window_start,
            rebuild_partial = telemetry.partial,
            rebuild_completed = telemetry.completed,
            rebuild_budget_exhausted_reason = telemetry
                .budget_exhausted_reason
                .map(PersistedStreamBudgetExhaustedReason::as_str),
            rebuild_phase_cursor_ts = ?telemetry
                .phase_cursor
                .as_ref()
                .map(|cursor| cursor.ts_utc),
            rebuild_phase_cursor_slot = telemetry
                .phase_cursor
                .as_ref()
                .map(|cursor| cursor.slot),
            rebuild_phase_cursor_signature = telemetry
                .phase_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            rebuild_collect_buy_mints_cursor_token =
                telemetry.collect_buy_mints_cursor_token.as_deref(),
            rebuild_collect_buy_mints_reconcile_source_window_start =
                ?telemetry.collect_buy_mints_reconcile_source_window_start,
            rebuild_collect_buy_mints_reconcile_source_horizon_end =
                ?telemetry.collect_buy_mints_reconcile_source_horizon_end,
            rebuild_collect_buy_mints_reconcile_expired_head_cursor_ts = ?telemetry
                .collect_buy_mints_reconcile_expired_head_cursor
                .as_ref()
                .map(|cursor| cursor.ts_utc),
            rebuild_collect_buy_mints_reconcile_expired_head_cursor_slot = ?telemetry
                .collect_buy_mints_reconcile_expired_head_cursor
                .as_ref()
                .map(|cursor| cursor.slot),
            rebuild_collect_buy_mints_reconcile_expired_head_cursor_signature = ?telemetry
                .collect_buy_mints_reconcile_expired_head_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            rebuild_collect_buy_mints_reconcile_expired_head_cursor_token = telemetry
                .collect_buy_mints_reconcile_expired_head_cursor_token
                .as_deref(),
            rebuild_collect_buy_mints_reconcile_expired_head_pending_mints = telemetry
                .collect_buy_mints_reconcile_expired_head_pending_mints,
            rebuild_collect_buy_mints_reconcile_new_tail_cursor_ts = ?telemetry
                .collect_buy_mints_reconcile_new_tail_cursor
                .as_ref()
                .map(|cursor| cursor.ts_utc),
            rebuild_collect_buy_mints_reconcile_new_tail_cursor_slot = ?telemetry
                .collect_buy_mints_reconcile_new_tail_cursor
                .as_ref()
                .map(|cursor| cursor.slot),
            rebuild_collect_buy_mints_reconcile_new_tail_cursor_signature = ?telemetry
                .collect_buy_mints_reconcile_new_tail_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            rebuild_collect_buy_mints_reconcile_new_tail_cursor_token = telemetry
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .as_deref(),
            rebuild_collect_buy_mints_reconcile_new_tail_slice_end_token = telemetry
                .collect_buy_mints_reconcile_new_tail_slice_end_token
                .as_deref(),
            rebuild_collect_buy_mints_reconcile_new_tail_pending_mints = telemetry
                .collect_buy_mints_reconcile_new_tail_pending_mints,
            rebuild_cycle_rows_processed = telemetry.cycle_rows_processed,
            rebuild_cycle_pages_processed = telemetry.cycle_pages_processed,
            rebuild_cycle_replay_wallet_stats_fast_path_pages_processed = telemetry
                .cycle_replay_wallet_stats_day_count_source_progress
                .fast_path_pages_processed,
            rebuild_cycle_replay_wallet_stats_fallback_pages_processed = telemetry
                .cycle_replay_wallet_stats_day_count_source_progress
                .fallback_pages_processed,
            rebuild_cycle_replay_wallet_stats_fast_path_wallets_processed = telemetry
                .cycle_replay_wallet_stats_day_count_source_progress
                .fast_path_wallets_processed,
            rebuild_cycle_replay_wallet_stats_fallback_wallets_processed = telemetry
                .cycle_replay_wallet_stats_day_count_source_progress
                .fallback_wallets_processed,
            rebuild_cycle_unique_buy_mints_discovered =
                telemetry.cycle_unique_buy_mints_discovered,
            rebuild_cycle_rows_per_second = if telemetry.cycle_elapsed_ms == 0 {
                telemetry.cycle_rows_processed as u64
            } else {
                (telemetry.cycle_rows_processed as u64).saturating_mul(1_000)
                    / telemetry.cycle_elapsed_ms.max(1)
            },
            rebuild_prepass_rows_processed = telemetry.prepass_rows_processed,
            rebuild_prepass_pages_processed = telemetry.prepass_pages_processed,
            rebuild_replay_wallet_stats_complete = telemetry.replay_wallet_stats_complete,
            rebuild_replay_wallet_stats_rows_processed =
                telemetry.replay_wallet_stats_rows_processed,
            rebuild_replay_wallet_stats_pages_processed =
                telemetry.replay_wallet_stats_pages_processed,
            rebuild_replay_wallet_stats_fast_path_pages_processed = telemetry
                .replay_wallet_stats_day_count_source_progress
                .fast_path_pages_processed,
            rebuild_replay_wallet_stats_fallback_pages_processed = telemetry
                .replay_wallet_stats_day_count_source_progress
                .fallback_pages_processed,
            rebuild_replay_wallet_stats_fast_path_wallets_processed = telemetry
                .replay_wallet_stats_day_count_source_progress
                .fast_path_wallets_processed,
            rebuild_replay_wallet_stats_fallback_wallets_processed = telemetry
                .replay_wallet_stats_day_count_source_progress
                .fallback_wallets_processed,
            rebuild_replay_rows_processed = telemetry.replay_rows_processed,
            rebuild_replay_pages_processed = telemetry.replay_pages_processed,
            rebuild_observed_swaps_loaded = telemetry.observed_swaps_loaded,
            rebuild_unique_buy_mints = telemetry.unique_buy_mints,
            rebuild_quality_next_mint_index = telemetry.quality_next_mint_index,
            rebuild_quality_rpc_attempted = telemetry.quality_rpc_attempted,
            rebuild_quality_rpc_spent_ms = telemetry.quality_rpc_spent_ms,
            rebuild_wallets_buffered = telemetry.wallets_buffered,
            rebuild_chunks_completed = telemetry.chunks_completed,
            rebuild_started_at = %telemetry.started_at,
            rebuild_cycle_elapsed_ms = telemetry.cycle_elapsed_ms,
            rebuild_total_elapsed_ms = telemetry.total_elapsed_ms,
            "{message}"
        );
    }

    fn advance_persisted_stream_prepass(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        let mut rows_processed = 0usize;
        let mut pages_processed = 0usize;
        let mut unique_buy_mints_discovered = 0usize;
        let mut cursor_token = state.payload.collect_buy_mints_cursor_token.clone();
        if state.payload.collect_buy_mints_mode == CollectBuyMintsMode::FreshScan
            && cursor_token.is_none()
            && state.phase_cursor.is_some()
        {
            let mut canonical_mints = state.payload.unique_buy_mints.clone();
            Self::canonicalize_unique_buy_mints(&mut canonical_mints);
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_legacy_phase_cursor_ts =
                    ?state.phase_cursor.as_ref().map(|cursor| cursor.ts_utc),
                rebuild_legacy_phase_cursor_slot =
                    state.phase_cursor.as_ref().map(|cursor| cursor.slot),
                rebuild_legacy_phase_cursor_signature = state
                    .phase_cursor
                    .as_ref()
                    .map(|cursor| cursor.signature.as_str()),
                rebuild_legacy_unique_buy_mints = state.payload.unique_buy_mints.len(),
                rebuild_legacy_canonical_unique_buy_mints = canonical_mints.len(),
                "migrating collect_buy_mints checkpoint from raw-swap cursor replay to direct distinct SOL-buy mint pagination with canonical safe-prefix recovery"
            );
            let (safe_prefix_len, time_budget_exhausted) = self
                .derive_legacy_collect_buy_mints_safe_prefix_len(
                    store,
                    state,
                    &canonical_mints,
                    deadline,
                )?;
            if time_budget_exhausted {
                return Ok(PersistedStreamPhaseAdvance {
                    rows_processed: 0,
                    pages_processed: 0,
                    replay_wallet_stats_rows_processed: 0,
                    replay_wallet_stats_pages_processed: 0,
                    replay_wallet_stats_day_count_source_progress:
                        ReplayWalletStatsDayCountSourceProgress::default(),
                    replay_sol_leg_access_path: None,
                    source_exhausted: false,
                    phase_cursor: state.phase_cursor.clone(),
                    collect_buy_mints_cursor_token: None,
                    unique_buy_mints_discovered: 0,
                    budget_exhausted_reason: Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
                });
            }
            let dropped_legacy_tail = canonical_mints.len().saturating_sub(safe_prefix_len);
            state.payload.unique_buy_mints =
                canonical_mints.into_iter().take(safe_prefix_len).collect();
            cursor_token = state.payload.unique_buy_mints.last().cloned();
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_collect_buy_mints_cursor_token = cursor_token.as_deref(),
                rebuild_legacy_safe_prefix_mints = safe_prefix_len,
                rebuild_legacy_tail_mints_dropped = dropped_legacy_tail,
                "recovered canonical safe-prefix progress for legacy collect_buy_mints checkpoint before resuming direct distinct mint pagination"
            );
        }
        let budget_exhausted_reason = loop {
            if pages_processed >= fetch_page_limit {
                break Some(PersistedStreamBudgetExhaustedReason::PageBudget);
            }
            if Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            match state.payload.collect_buy_mints_mode {
                CollectBuyMintsMode::FreshScan => {
                    let page = store
                        .load_observed_buy_mint_counts_in_window_after_token_with_budget(
                            state.window_start,
                            state.horizon_end,
                            cursor_token.as_deref(),
                            None,
                            fetch_limit,
                            deadline,
                        )?;
                    pages_processed = pages_processed.saturating_add(1);
                    rows_processed = rows_processed.saturating_add(page.rows.len());
                    for row in &page.rows {
                        if Self::set_buy_mint_occurrences(
                            &mut state.payload,
                            &row.mint,
                            row.buy_count,
                        ) {
                            unique_buy_mints_discovered =
                                unique_buy_mints_discovered.saturating_add(1);
                        }
                    }
                    cursor_token = page.rows.last().map(|row| row.mint.clone());
                    if page.time_budget_exhausted {
                        break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
                    }
                    if page.rows.len() < fetch_limit {
                        state.payload.collect_buy_mints_prepass_complete = true;
                        state.payload.collect_buy_mints_cursor_token = None;
                        if Self::payload_has_exact_buy_mint_membership(&state.payload) {
                            Self::sync_unique_buy_mints_from_counts(&mut state.payload);
                        } else {
                            Self::canonicalize_unique_buy_mints(
                                &mut state.payload.unique_buy_mints,
                            );
                        }
                        return Ok(PersistedStreamPhaseAdvance {
                            rows_processed,
                            pages_processed,
                            replay_wallet_stats_rows_processed: 0,
                            replay_wallet_stats_pages_processed: 0,
                            replay_wallet_stats_day_count_source_progress:
                                ReplayWalletStatsDayCountSourceProgress::default(),
                            replay_sol_leg_access_path: None,
                            source_exhausted: true,
                            phase_cursor: None,
                            collect_buy_mints_cursor_token: None,
                            unique_buy_mints_discovered,
                            budget_exhausted_reason: None,
                        });
                    }
                }
                CollectBuyMintsMode::ReconcileExpiredHead => {
                    let Some(source_window_start) = state
                        .payload
                        .collect_buy_mints_reconcile_source_window_start
                    else {
                        state.payload.collect_buy_mints_mode =
                            CollectBuyMintsMode::ReconcileNewTail;
                        Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
                        continue;
                    };
                    if source_window_start >= state.window_start {
                        state.payload.collect_buy_mints_mode =
                            CollectBuyMintsMode::ReconcileNewTail;
                        state
                            .payload
                            .collect_buy_mints_reconcile_expired_head_cursor = None;
                        state
                            .payload
                            .collect_buy_mints_reconcile_expired_head_cursor_token = None;
                        Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
                        continue;
                    }

                    let reconcile_cursor_token = state
                        .payload
                        .collect_buy_mints_reconcile_expired_head_cursor_token
                        .clone();
                    let reconcile_batch_size = Self::stale_reconcile_token_batch_size(fetch_limit);
                    let exact_count_batch_size =
                        Self::stale_reconcile_exact_count_batch_size(fetch_limit);
                    if state
                        .payload
                        .collect_buy_mints_reconcile_expired_head_pending_mints
                        .is_empty()
                    {
                        let candidate_page = store
                            .load_observed_buy_mints_in_time_bounds_after_token_with_budget(
                                source_window_start,
                                true,
                                state.window_start,
                                false,
                                reconcile_cursor_token.as_deref(),
                                None,
                                reconcile_batch_size,
                                deadline,
                            )?;
                        if candidate_page.mints.is_empty() {
                            if candidate_page.time_budget_exhausted {
                                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
                            }
                            state.payload.collect_buy_mints_mode =
                                CollectBuyMintsMode::ReconcileNewTail;
                            state
                                .payload
                                .collect_buy_mints_reconcile_expired_head_cursor = None;
                            state
                                .payload
                                .collect_buy_mints_reconcile_expired_head_cursor_token = None;
                            Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
                            info!(
                                rebuild_window_start = %state.window_start,
                                rebuild_horizon_end = %state.horizon_end,
                                rebuild_reconcile_source_window_start = ?state
                                    .payload
                                    .collect_buy_mints_reconcile_source_window_start,
                                rebuild_reconcile_source_horizon_end = ?state
                                    .payload
                                    .collect_buy_mints_reconcile_source_horizon_end,
                                rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                                "completed expired-head reconciliation for carried-forward collect_buy_mints state; switching to new-tail reconciliation"
                            );
                            continue;
                        }
                        state
                            .payload
                            .collect_buy_mints_reconcile_expired_head_pending_mints =
                            candidate_page.mints;
                        if candidate_page.time_budget_exhausted {
                            break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
                        }
                    }

                    let active_pending_mints = state
                        .payload
                        .collect_buy_mints_reconcile_expired_head_pending_mints
                        .iter()
                        .take(exact_count_batch_size)
                        .cloned()
                        .collect::<Vec<_>>();
                    let Some(last_active_pending_mint) = active_pending_mints.last().cloned()
                    else {
                        state.payload.collect_buy_mints_mode =
                            CollectBuyMintsMode::ReconcileNewTail;
                        state
                            .payload
                            .collect_buy_mints_reconcile_expired_head_cursor = None;
                        state
                            .payload
                            .collect_buy_mints_reconcile_expired_head_cursor_token = None;
                        Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
                        info!(
                            rebuild_window_start = %state.window_start,
                            rebuild_horizon_end = %state.horizon_end,
                            rebuild_reconcile_source_window_start = ?state
                                .payload
                                .collect_buy_mints_reconcile_source_window_start,
                            rebuild_reconcile_source_horizon_end = ?state
                                .payload
                                .collect_buy_mints_reconcile_source_horizon_end,
                            rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                            "completed expired-head reconciliation for carried-forward collect_buy_mints state; switching to new-tail reconciliation"
                        );
                        continue;
                    };

                    if active_pending_mints.len() == 1 {
                        let exact_mint = last_active_pending_mint;
                        let exact_count = store
                            .count_observed_buy_mint_occurrences_in_time_bounds_with_budget(
                                source_window_start,
                                true,
                                state.window_start,
                                false,
                                &exact_mint,
                                deadline,
                            )?;
                        pages_processed = pages_processed.saturating_add(1);
                        state
                            .payload
                            .collect_buy_mints_reconcile_expired_head_cursor = None;
                        if exact_count.time_budget_exhausted {
                            break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
                        }
                        rows_processed = rows_processed.saturating_add(1);
                        Self::subtract_buy_mint_occurrences(
                            &mut state.payload,
                            &exact_mint,
                            exact_count.buy_count,
                        );
                        state
                            .payload
                            .collect_buy_mints_reconcile_expired_head_cursor_token =
                            Some(exact_mint);
                        Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
                    } else {
                        #[allow(unused_mut)]
                        let mut page = store
                            .load_observed_buy_mint_counts_for_exact_tokens_in_time_bounds_with_budget(
                                source_window_start,
                                true,
                                state.window_start,
                                false,
                                &active_pending_mints,
                                deadline,
                            )?;
                        #[cfg(test)]
                        if let Some(limit) =
                            take_test_force_reconcile_expired_head_exact_batch_row_limit()
                        {
                            if page.rows.len() > limit {
                                page.rows.truncate(limit);
                                page.time_budget_exhausted = true;
                            }
                        }
                        pages_processed = pages_processed.saturating_add(1);
                        rows_processed = rows_processed.saturating_add(page.rows.len());
                        for row in &page.rows {
                            Self::subtract_buy_mint_occurrences(
                                &mut state.payload,
                                &row.mint,
                                row.buy_count,
                            );
                        }
                        state
                            .payload
                            .collect_buy_mints_reconcile_expired_head_cursor = None;
                        let last_accounted_cursor = if page.time_budget_exhausted {
                            page.rows.last().map(|row| row.mint.clone())
                        } else {
                            Some(last_active_pending_mint)
                        };
                        if let Some(last_accounted_cursor) = last_accounted_cursor {
                            let processed_prefix_len = if page.time_budget_exhausted {
                                active_pending_mints.partition_point(|mint| {
                                    mint.as_str() <= last_accounted_cursor.as_str()
                                })
                            } else {
                                active_pending_mints.len()
                            };
                            if processed_prefix_len > 0 {
                                state
                                    .payload
                                    .collect_buy_mints_reconcile_expired_head_pending_mints
                                    .drain(..processed_prefix_len);
                            }
                            state
                                .payload
                                .collect_buy_mints_reconcile_expired_head_cursor_token =
                                Some(last_accounted_cursor);
                        }
                        if page.time_budget_exhausted {
                            break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
                        }
                    }

                    if !state
                        .payload
                        .collect_buy_mints_reconcile_expired_head_pending_mints
                        .is_empty()
                    {
                        continue;
                    }
                    continue;
                }
                CollectBuyMintsMode::ReconcileNewTail => {
                    let Some(source_horizon_end) =
                        state.payload.collect_buy_mints_reconcile_source_horizon_end
                    else {
                        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
                        continue;
                    };
                    if source_horizon_end >= state.horizon_end {
                        state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
                        state
                            .payload
                            .collect_buy_mints_reconcile_new_tail_cursor_token = None;
                        state
                            .payload
                            .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
                        Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
                        Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
                        state
                            .payload
                            .collect_buy_mints_reconcile_source_window_start = None;
                        state.payload.collect_buy_mints_reconcile_source_horizon_end = None;
                        if state.payload.collect_buy_mints_prepass_complete {
                            return Ok(PersistedStreamPhaseAdvance {
                                rows_processed,
                                pages_processed,
                                replay_wallet_stats_rows_processed: 0,
                                replay_wallet_stats_pages_processed: 0,
                                replay_wallet_stats_day_count_source_progress:
                                    ReplayWalletStatsDayCountSourceProgress::default(),
                                replay_sol_leg_access_path: None,
                                source_exhausted: true,
                                phase_cursor: None,
                                collect_buy_mints_cursor_token: None,
                                unique_buy_mints_discovered,
                                budget_exhausted_reason: None,
                            });
                        }
                        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
                        continue;
                    }

                    let reconcile_cursor_token = state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_cursor_token
                        .clone();
                    let reconcile_batch_size = Self::stale_reconcile_token_batch_size(fetch_limit);
                    let exact_count_batch_size =
                        Self::stale_reconcile_exact_count_batch_size(fetch_limit);
                    if state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_pending_mints
                        .is_empty()
                    {
                        let pending_slice_end_token = state
                            .payload
                            .collect_buy_mints_reconcile_new_tail_slice_end_token
                            .clone();
                        let candidate_page = store
                            .load_observed_buy_mints_in_time_bounds_after_token_with_budget(
                                source_horizon_end,
                                false,
                                state.horizon_end,
                                true,
                                reconcile_cursor_token.as_deref(),
                                pending_slice_end_token.as_deref(),
                                reconcile_batch_size,
                                deadline,
                            )?;
                        let Some(_) = candidate_page.mints.last() else {
                            if candidate_page.time_budget_exhausted {
                                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
                            }
                            if pending_slice_end_token.is_some() {
                                state
                                    .payload
                                    .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
                                continue;
                            }
                            state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
                            state
                                .payload
                                .collect_buy_mints_reconcile_new_tail_cursor_token = None;
                            state
                                .payload
                                .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
                            Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
                            state
                                .payload
                                .collect_buy_mints_reconcile_source_window_start = None;
                            state.payload.collect_buy_mints_reconcile_source_horizon_end = None;
                            if state.payload.collect_buy_mints_prepass_complete {
                                return Ok(PersistedStreamPhaseAdvance {
                                    rows_processed,
                                    pages_processed,
                                    replay_wallet_stats_rows_processed: 0,
                                    replay_wallet_stats_pages_processed: 0,
                                    replay_wallet_stats_day_count_source_progress:
                                        ReplayWalletStatsDayCountSourceProgress::default(),
                                    replay_sol_leg_access_path: None,
                                    source_exhausted: true,
                                    phase_cursor: None,
                                    collect_buy_mints_cursor_token: None,
                                    unique_buy_mints_discovered,
                                    budget_exhausted_reason: None,
                                });
                            }
                            state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
                            info!(
                                rebuild_window_start = %state.window_start,
                                rebuild_horizon_end = %state.horizon_end,
                                rebuild_collect_buy_mints_cursor_token =
                                    state.payload.collect_buy_mints_cursor_token.as_deref(),
                                rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                                "completed new-tail reconciliation for carried-forward collect_buy_mints state; resuming canonical distinct mint scan from persisted cursor"
                            );
                            continue;
                        };
                        state
                            .payload
                            .collect_buy_mints_reconcile_new_tail_pending_mints =
                            candidate_page.mints;
                        if candidate_page.time_budget_exhausted {
                            break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
                        }
                    }

                    let active_pending_mints = state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_pending_mints
                        .iter()
                        .take(exact_count_batch_size)
                        .cloned()
                        .collect::<Vec<_>>();

                    if active_pending_mints.len() == 1 {
                        let exact_mint = state
                            .payload
                            .collect_buy_mints_reconcile_new_tail_pending_mints
                            .first()
                            .cloned()
                            .expect("single pending mint");
                        let exact_count = store
                            .count_observed_buy_mint_occurrences_in_time_bounds_with_budget(
                                source_horizon_end,
                                false,
                                state.horizon_end,
                                true,
                                &exact_mint,
                                deadline,
                            )?;
                        pages_processed = pages_processed.saturating_add(1);
                        state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
                        if exact_count.time_budget_exhausted {
                            state
                                .payload
                                .collect_buy_mints_reconcile_new_tail_slice_end_token =
                                Some(exact_mint);
                            break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
                        }
                        rows_processed = rows_processed.saturating_add(1);
                        if Self::add_buy_mint_occurrences(
                            &mut state.payload,
                            &exact_mint,
                            exact_count.buy_count,
                        ) {
                            unique_buy_mints_discovered =
                                unique_buy_mints_discovered.saturating_add(1);
                        }
                        state
                            .payload
                            .collect_buy_mints_reconcile_new_tail_cursor_token = Some(exact_mint);
                        if state
                            .payload
                            .collect_buy_mints_reconcile_new_tail_slice_end_token
                            .as_deref()
                            == state
                                .payload
                                .collect_buy_mints_reconcile_new_tail_cursor_token
                                .as_deref()
                        {
                            state
                                .payload
                                .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
                        }
                        Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
                        if state
                            .payload
                            .collect_buy_mints_reconcile_new_tail_slice_end_token
                            .is_some()
                        {
                            continue;
                        }
                        continue;
                    }

                    #[allow(unused_mut)]
                    let mut page = store
                        .load_observed_buy_mint_counts_for_exact_tokens_in_time_bounds_with_budget(
                            source_horizon_end,
                            false,
                            state.horizon_end,
                            true,
                            &active_pending_mints,
                            deadline,
                        )?;
                    #[cfg(test)]
                    if let Some(limit) = take_test_force_reconcile_new_tail_exact_batch_row_limit()
                    {
                        if page.rows.len() > limit {
                            page.rows.truncate(limit);
                            page.time_budget_exhausted = true;
                        }
                    }
                    #[cfg(test)]
                    if take_test_force_reconcile_new_tail_zero_row_timeout() {
                        page.rows.clear();
                        page.time_budget_exhausted = true;
                    }
                    pages_processed = pages_processed.saturating_add(1);
                    if page.rows.is_empty() {
                        let candidate_mints = active_pending_mints.len();
                        let narrowed_slice_end_token =
                            Self::narrowed_stale_reconcile_slice_end(&active_pending_mints)
                                .unwrap_or_else(|| {
                                    active_pending_mints
                                        .last()
                                        .cloned()
                                        .expect("pending stale new-tail batch end token")
                                });
                        state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
                        Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
                        state
                            .payload
                            .collect_buy_mints_reconcile_new_tail_slice_end_token =
                            Some(narrowed_slice_end_token.clone());
                        info!(
                            rebuild_window_start = %state.window_start,
                            rebuild_horizon_end = %state.horizon_end,
                            rebuild_collect_buy_mints_reconcile_new_tail_cursor_token =
                                reconcile_cursor_token.as_deref(),
                            rebuild_collect_buy_mints_reconcile_new_tail_slice_end_token =
                                Some(narrowed_slice_end_token.as_str()),
                            rebuild_collect_buy_mints_reconcile_new_tail_candidate_mints =
                                candidate_mints,
                            "stale new-tail exact candidate batch returned no rows before completion; narrowing exact token slice before retry"
                        );
                        break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
                    }
                    rows_processed = rows_processed.saturating_add(page.rows.len());
                    for row in &page.rows {
                        if Self::add_buy_mint_occurrences(
                            &mut state.payload,
                            &row.mint,
                            row.buy_count,
                        ) {
                            unique_buy_mints_discovered =
                                unique_buy_mints_discovered.saturating_add(1);
                        }
                    }
                    state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
                    let last_processed_cursor = page.rows.last().map(|row| row.mint.clone());
                    if let Some(last_processed_cursor) = last_processed_cursor.clone() {
                        let processed_prefix_len = active_pending_mints.partition_point(|mint| {
                            mint.as_str() <= last_processed_cursor.as_str()
                        });
                        if processed_prefix_len > 0 {
                            state
                                .payload
                                .collect_buy_mints_reconcile_new_tail_pending_mints
                                .drain(..processed_prefix_len);
                        }
                        state
                            .payload
                            .collect_buy_mints_reconcile_new_tail_cursor_token =
                            Some(last_processed_cursor.clone());
                        if state
                            .payload
                            .collect_buy_mints_reconcile_new_tail_slice_end_token
                            .as_deref()
                            == Some(last_processed_cursor.as_str())
                        {
                            state
                                .payload
                                .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
                        }
                    }
                    if page.time_budget_exhausted {
                        break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
                    }
                    if !state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_pending_mints
                        .is_empty()
                    {
                        continue;
                    }
                }
            }
        };

        Ok(PersistedStreamPhaseAdvance {
            rows_processed,
            pages_processed,
            replay_wallet_stats_rows_processed: 0,
            replay_wallet_stats_pages_processed: 0,
            replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress::default(),
            replay_sol_leg_access_path: None,
            source_exhausted: false,
            phase_cursor: None,
            collect_buy_mints_cursor_token: cursor_token,
            unique_buy_mints_discovered,
            budget_exhausted_reason,
        })
    }

    fn advance_persisted_stream_token_quality(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        let mut rows_processed = 0usize;
        let mut pages_processed = 0usize;
        let budget_exhausted_reason = loop {
            if state.payload.token_quality_progress.next_mint_index
                >= state.payload.unique_buy_mints.len()
            {
                return Ok(PersistedStreamPhaseAdvance {
                    rows_processed,
                    pages_processed,
                    replay_wallet_stats_rows_processed: 0,
                    replay_wallet_stats_pages_processed: 0,
                    replay_wallet_stats_day_count_source_progress:
                        ReplayWalletStatsDayCountSourceProgress::default(),
                    replay_sol_leg_access_path: None,
                    source_exhausted: true,
                    phase_cursor: None,
                    collect_buy_mints_cursor_token: None,
                    unique_buy_mints_discovered: 0,
                    budget_exhausted_reason: None,
                });
            }
            if pages_processed >= fetch_page_limit {
                break Some(PersistedStreamBudgetExhaustedReason::PageBudget);
            }
            if Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            let outcome = self.resolve_token_quality_for_mints_chunk(
                store,
                &state.payload.unique_buy_mints,
                state.horizon_end,
                &mut state.payload.token_quality_cache,
                &mut state.payload.token_quality_progress,
                fetch_limit,
                deadline,
            )?;
            rows_processed = rows_processed.saturating_add(outcome.processed_mints);
            if outcome.processed_mints > 0 {
                pages_processed = pages_processed.saturating_add(1);
            }
            if outcome.source_exhausted {
                return Ok(PersistedStreamPhaseAdvance {
                    rows_processed,
                    pages_processed,
                    replay_wallet_stats_rows_processed: 0,
                    replay_wallet_stats_pages_processed: 0,
                    replay_wallet_stats_day_count_source_progress:
                        ReplayWalletStatsDayCountSourceProgress::default(),
                    replay_sol_leg_access_path: None,
                    source_exhausted: true,
                    phase_cursor: None,
                    collect_buy_mints_cursor_token: None,
                    unique_buy_mints_discovered: 0,
                    budget_exhausted_reason: None,
                });
            }
            if outcome.processed_mints == 0 {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }
        };

        Ok(PersistedStreamPhaseAdvance {
            rows_processed,
            pages_processed,
            replay_wallet_stats_rows_processed: 0,
            replay_wallet_stats_pages_processed: 0,
            replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress::default(),
            replay_sol_leg_access_path: None,
            source_exhausted: false,
            phase_cursor: None,
            collect_buy_mints_cursor_token: None,
            unique_buy_mints_discovered: 0,
            budget_exhausted_reason,
        })
    }

    fn advance_persisted_stream_replay_wallet_stats(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        let mut rows_processed = 0usize;
        let mut pages_processed = 0usize;
        let wallet_batch_size = Self::replay_wallet_stats_wallet_batch_size(fetch_limit);
        let mut wallet_cursor = state.payload.replay_wallet_stats_wallet_cursor.clone();
        let mut replay_wallet_stats_day_count_source_progress =
            ReplayWalletStatsDayCountSourceProgress::default();

        let budget_exhausted_reason = loop {
            if pages_processed >= fetch_page_limit {
                break Some(PersistedStreamBudgetExhaustedReason::PageBudget);
            }
            if Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            let page = store.observed_wallet_activity_page_in_window_with_budget(
                state.window_start,
                state.horizon_end,
                wallet_cursor.as_deref(),
                wallet_batch_size,
                self.config.max_tx_per_minute,
                deadline,
            )?;
            pages_processed = pages_processed.saturating_add(1);

            if page.time_budget_exhausted || Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }
            rows_processed = rows_processed.saturating_add(page.rows_seen);
            let page_wallet_count = page.rows.len();
            replay_wallet_stats_day_count_source_progress
                .observe_page(page.active_day_count_source, page_wallet_count);
            let last_wallet_id = page.rows.last().map(|row| row.wallet_id.clone());
            for row in page.rows {
                Self::observe_replay_wallet_activity_summary(&mut state.payload, row);
            }
            wallet_cursor = last_wallet_id;

            if page_wallet_count < wallet_batch_size {
                state.payload.replay_wallet_stats_wallet_cursor = None;
                return Ok(PersistedStreamPhaseAdvance {
                    rows_processed: 0,
                    pages_processed: 0,
                    replay_wallet_stats_rows_processed: rows_processed,
                    replay_wallet_stats_pages_processed: pages_processed,
                    replay_wallet_stats_day_count_source_progress,
                    replay_sol_leg_access_path: None,
                    source_exhausted: true,
                    phase_cursor: None,
                    collect_buy_mints_cursor_token: None,
                    unique_buy_mints_discovered: 0,
                    budget_exhausted_reason: None,
                });
            }
        };

        state.payload.replay_wallet_stats_wallet_cursor = wallet_cursor;

        Ok(PersistedStreamPhaseAdvance {
            rows_processed: 0,
            pages_processed: 0,
            replay_wallet_stats_rows_processed: rows_processed,
            replay_wallet_stats_pages_processed: pages_processed,
            replay_wallet_stats_day_count_source_progress,
            replay_sol_leg_access_path: None,
            source_exhausted: false,
            phase_cursor: None,
            collect_buy_mints_cursor_token: None,
            unique_buy_mints_discovered: 0,
            budget_exhausted_reason,
        })
    }

    fn advance_persisted_stream_replay_legacy(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        let mut rows_processed = 0usize;
        let mut pages_processed = 0usize;
        let mut cursor = state.phase_cursor.clone();
        let lookahead = Duration::seconds(self.config.rug_lookahead_seconds.max(1) as i64);

        let budget_exhausted_reason = loop {
            if pages_processed >= fetch_page_limit {
                break Some(PersistedStreamBudgetExhaustedReason::PageBudget);
            }
            if Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            let mut page_last_cursor = cursor.clone();
            let base_replay_rows_processed = state.replay_rows_processed;
            let page = store.for_each_observed_swap_in_window_after_cursor_with_budget(
                state.window_start,
                state.horizon_end,
                cursor.as_ref(),
                fetch_limit,
                deadline,
                |swap| {
                    page_last_cursor = Some(DiscoveryRuntimeCursor {
                        ts_utc: swap.ts_utc,
                        slot: swap.slot,
                        signature: swap.signature.clone(),
                    });
                    let buy_quality = self.update_token_quality_state_streaming(
                        &mut state.payload.token_states,
                        &mut state.payload.token_recent_sol_trades,
                        &state.payload.token_quality_cache,
                        &swap,
                    );
                    let entry = state
                        .payload
                        .by_wallet
                        .entry(swap.wallet.clone())
                        .or_default();
                    entry.observe_swap_streaming(&swap, self.config.max_tx_per_minute, buy_quality);

                    let Some(token) = sol_leg_token(&swap) else {
                        rows_processed = rows_processed.saturating_add(1);
                        return Ok(());
                    };
                    self.finalize_streaming_rug_metrics_up_to(
                        &mut state.payload.by_wallet,
                        token,
                        &mut state.payload.token_recent_sol_trades,
                        &mut state.payload.pending_rug_checks,
                        &mut state.payload.token_pending_buy_starts,
                        swap.ts_utc,
                        lookahead,
                        state.horizon_end,
                    );
                    if is_sol_buy(&swap) {
                        state
                            .payload
                            .pending_rug_checks
                            .push_back(PendingBuyRugCheck {
                                token: token.to_string(),
                                wallet_id: swap.wallet.clone(),
                                buy_ts: swap.ts_utc,
                            });
                        state
                            .payload
                            .token_pending_buy_starts
                            .entry(token.to_string())
                            .or_default()
                            .push_back(swap.ts_utc);
                    }
                    let processed_total = base_replay_rows_processed
                        .saturating_add(rows_processed)
                        .saturating_add(1);
                    if processed_total % STREAMING_RUG_TRADE_SWEEP_INTERVAL_SWAPS == 0 {
                        self.evict_idle_streaming_rug_trade_history(
                            &mut state.payload.token_recent_sol_trades,
                            &state.payload.token_pending_buy_starts,
                            swap.ts_utc - lookahead,
                        );
                    }
                    rows_processed = rows_processed.saturating_add(1);
                    Ok(())
                },
            )?;
            pages_processed = pages_processed.saturating_add(1);
            cursor = page_last_cursor;

            if page.time_budget_exhausted {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }
            if page.rows_seen < fetch_limit {
                return Ok(PersistedStreamPhaseAdvance {
                    rows_processed,
                    pages_processed,
                    replay_wallet_stats_rows_processed: 0,
                    replay_wallet_stats_pages_processed: 0,
                    replay_wallet_stats_day_count_source_progress:
                        ReplayWalletStatsDayCountSourceProgress::default(),
                    replay_sol_leg_access_path: None,
                    source_exhausted: true,
                    phase_cursor: None,
                    collect_buy_mints_cursor_token: None,
                    unique_buy_mints_discovered: 0,
                    budget_exhausted_reason: None,
                });
            }
        };

        Ok(PersistedStreamPhaseAdvance {
            rows_processed,
            pages_processed,
            replay_wallet_stats_rows_processed: 0,
            replay_wallet_stats_pages_processed: 0,
            replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress::default(),
            replay_sol_leg_access_path: None,
            source_exhausted: false,
            phase_cursor: cursor,
            collect_buy_mints_cursor_token: None,
            unique_buy_mints_discovered: 0,
            budget_exhausted_reason,
        })
    }

    fn advance_persisted_stream_replay_optimized(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        let mut replay_rows_processed = 0usize;
        let mut replay_pages_processed = 0usize;
        let mut replay_wallet_stats_rows_processed = 0usize;
        let mut replay_wallet_stats_pages_processed = 0usize;
        let mut replay_wallet_stats_day_count_source_progress =
            ReplayWalletStatsDayCountSourceProgress::default();
        let mut replay_sol_leg_access_path = None;
        let mut cursor = state.phase_cursor.clone();
        let lookahead = Duration::seconds(self.config.rug_lookahead_seconds.max(1) as i64);

        let budget_exhausted_reason = loop {
            let phase_page_limit = if state.payload.replay_wallet_stats_complete {
                fetch_page_limit
            } else {
                Self::replay_wallet_stats_catch_up_page_limit(fetch_page_limit)
            };
            let total_pages_processed =
                replay_pages_processed.saturating_add(replay_wallet_stats_pages_processed);
            if total_pages_processed >= phase_page_limit {
                break Some(PersistedStreamBudgetExhaustedReason::PageBudget);
            }
            if Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            if !state.payload.replay_wallet_stats_complete {
                state.phase_cursor = cursor.clone();
                let advance = self.advance_persisted_stream_replay_wallet_stats(
                    store,
                    state,
                    fetch_limit,
                    phase_page_limit.saturating_sub(total_pages_processed),
                    deadline,
                )?;
                replay_wallet_stats_rows_processed = replay_wallet_stats_rows_processed
                    .saturating_add(advance.replay_wallet_stats_rows_processed);
                replay_wallet_stats_pages_processed = replay_wallet_stats_pages_processed
                    .saturating_add(advance.replay_wallet_stats_pages_processed);
                replay_wallet_stats_day_count_source_progress
                    .merge(advance.replay_wallet_stats_day_count_source_progress);
                cursor = advance.phase_cursor.clone();
                if advance.source_exhausted {
                    state.payload.replay_wallet_stats_complete = true;
                    state.payload.replay_wallet_stats_wallet_cursor = None;
                    state.phase_cursor = None;
                    cursor = None;
                    let replay_wallet_stats_day_count_source_progress_total = state
                        .payload
                        .replay_wallet_stats_day_count_source_progress
                        .merged_with(replay_wallet_stats_day_count_source_progress);
                    for acc in state.payload.by_wallet.values_mut() {
                        acc.tx_per_minute.clear();
                    }
                    info!(
                        rebuild_window_start = %state.window_start,
                        rebuild_horizon_end = %state.horizon_end,
                        rebuild_wallets_buffered = state.payload.by_wallet.len(),
                        rebuild_replay_wallet_stats_rows_processed = state
                            .payload
                            .replay_wallet_stats_rows_processed
                            .saturating_add(replay_wallet_stats_rows_processed),
                        rebuild_replay_wallet_stats_pages_processed = state
                            .payload
                            .replay_wallet_stats_pages_processed
                            .saturating_add(replay_wallet_stats_pages_processed),
                        rebuild_replay_wallet_stats_fast_path_pages_processed =
                            replay_wallet_stats_day_count_source_progress_total
                                .fast_path_pages_processed,
                        rebuild_replay_wallet_stats_fallback_pages_processed =
                            replay_wallet_stats_day_count_source_progress_total
                                .fallback_pages_processed,
                        rebuild_replay_wallet_stats_fast_path_wallets_processed =
                            replay_wallet_stats_day_count_source_progress_total
                                .fast_path_wallets_processed,
                        rebuild_replay_wallet_stats_fallback_wallets_processed =
                            replay_wallet_stats_day_count_source_progress_total
                                .fallback_wallets_processed,
                        "completed bounded replay wallet-stats prepass; switching to SOL-leg replay"
                    );
                    continue;
                }
                return Ok(PersistedStreamPhaseAdvance {
                    rows_processed: replay_rows_processed,
                    pages_processed: replay_pages_processed,
                    replay_wallet_stats_rows_processed,
                    replay_wallet_stats_pages_processed,
                    replay_wallet_stats_day_count_source_progress,
                    replay_sol_leg_access_path: None,
                    source_exhausted: false,
                    phase_cursor: cursor,
                    collect_buy_mints_cursor_token: None,
                    unique_buy_mints_discovered: 0,
                    budget_exhausted_reason: advance.budget_exhausted_reason,
                });
            }

            let mut page_last_cursor = cursor.clone();
            let base_replay_rows_processed = state.replay_rows_processed;
            let page = store.for_each_observed_sol_leg_swap_in_window_after_cursor_with_budget(
                state.window_start,
                state.horizon_end,
                cursor.as_ref(),
                fetch_limit,
                deadline,
                |swap| {
                    page_last_cursor = Some(DiscoveryRuntimeCursor {
                        ts_utc: swap.ts_utc,
                        slot: swap.slot,
                        signature: swap.signature.clone(),
                    });
                    let buy_quality = self.update_token_quality_state_streaming(
                        &mut state.payload.token_states,
                        &mut state.payload.token_recent_sol_trades,
                        &state.payload.token_quality_cache,
                        &swap,
                    );
                    let entry = state
                        .payload
                        .by_wallet
                        .entry(swap.wallet.clone())
                        .or_default();
                    if is_sol_buy(&swap) {
                        entry.observe_buy_streaming(
                            swap.token_out.as_str(),
                            swap.amount_out,
                            swap.amount_in,
                            swap.ts_utc,
                            buy_quality.unwrap_or(BuyTradability::Rejected),
                        );
                    } else if is_sol_sell(&swap) {
                        entry.observe_sell(
                            swap.token_in.as_str(),
                            swap.amount_in,
                            swap.amount_out,
                            swap.ts_utc,
                        );
                    }

                    let Some(token) = sol_leg_token(&swap) else {
                        return Ok(());
                    };
                    self.finalize_streaming_rug_metrics_up_to(
                        &mut state.payload.by_wallet,
                        token,
                        &mut state.payload.token_recent_sol_trades,
                        &mut state.payload.pending_rug_checks,
                        &mut state.payload.token_pending_buy_starts,
                        swap.ts_utc,
                        lookahead,
                        state.horizon_end,
                    );
                    if is_sol_buy(&swap) {
                        state
                            .payload
                            .pending_rug_checks
                            .push_back(PendingBuyRugCheck {
                                token: token.to_string(),
                                wallet_id: swap.wallet.clone(),
                                buy_ts: swap.ts_utc,
                            });
                        state
                            .payload
                            .token_pending_buy_starts
                            .entry(token.to_string())
                            .or_default()
                            .push_back(swap.ts_utc);
                    }
                    let processed_total = base_replay_rows_processed
                        .saturating_add(replay_rows_processed)
                        .saturating_add(1);
                    if processed_total % STREAMING_RUG_TRADE_SWEEP_INTERVAL_SWAPS == 0 {
                        self.evict_idle_streaming_rug_trade_history(
                            &mut state.payload.token_recent_sol_trades,
                            &state.payload.token_pending_buy_starts,
                            swap.ts_utc - lookahead,
                        );
                    }
                    replay_rows_processed = replay_rows_processed.saturating_add(1);
                    Ok(())
                },
            )?;
            replay_sol_leg_access_path = Some(page.access_path);
            replay_pages_processed = replay_pages_processed.saturating_add(1);
            cursor = page_last_cursor;

            if page.time_budget_exhausted {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }
            if page.rows_seen < fetch_limit {
                return Ok(PersistedStreamPhaseAdvance {
                    rows_processed: replay_rows_processed,
                    pages_processed: replay_pages_processed,
                    replay_wallet_stats_rows_processed,
                    replay_wallet_stats_pages_processed,
                    replay_wallet_stats_day_count_source_progress,
                    replay_sol_leg_access_path,
                    source_exhausted: true,
                    phase_cursor: None,
                    collect_buy_mints_cursor_token: None,
                    unique_buy_mints_discovered: 0,
                    budget_exhausted_reason: None,
                });
            }
        };

        Ok(PersistedStreamPhaseAdvance {
            rows_processed: replay_rows_processed,
            pages_processed: replay_pages_processed,
            replay_wallet_stats_rows_processed,
            replay_wallet_stats_pages_processed,
            replay_wallet_stats_day_count_source_progress,
            replay_sol_leg_access_path,
            source_exhausted: false,
            phase_cursor: cursor,
            collect_buy_mints_cursor_token: None,
            unique_buy_mints_discovered: 0,
            budget_exhausted_reason,
        })
    }

    fn advance_persisted_stream_replay(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        match state.payload.replay_mode {
            ReplayMode::LegacyFullWindow => self.advance_persisted_stream_replay_legacy(
                store,
                state,
                fetch_limit,
                fetch_page_limit,
                deadline,
            ),
            ReplayMode::WalletStatsThenSolLeg => self.advance_persisted_stream_replay_optimized(
                store,
                state,
                fetch_limit,
                fetch_page_limit,
                deadline,
            ),
        }
    }

    fn advance_persisted_stream_rebuild(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        fetch_limit: usize,
        fetch_page_limit: usize,
        rebuild_time_budget: StdDuration,
    ) -> Result<PersistedStreamRebuildAdvanceOutcome> {
        let (mut state, restore_outcome) = self.load_or_start_persisted_stream_rebuild_state(
            store,
            window_start,
            metrics_window_start,
            now,
        )?;
        match restore_outcome {
            PersistedStreamRebuildRestoreOutcome::StartedFresh => {
                info!(
                    rebuild_window_start = %state.window_start,
                    rebuild_horizon_end = %state.horizon_end,
                    rebuild_metrics_window_start = %state.metrics_window_start,
                    "starting bounded discovery persisted observed_swaps rebuild"
                );
            }
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
            | PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow
            | PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow => {
                let message = if restore_outcome
                    == PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow
                {
                    "resuming carried-forward bounded discovery persisted observed_swaps rebuild after metrics bucket rollover"
                } else if restore_outcome
                    == PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
                {
                    "resuming stale metrics-window collect_buy_mints reconciliation until the next exact carry-forward checkpoint becomes available"
                } else {
                    "resuming bounded discovery persisted observed_swaps rebuild"
                };
                info!(
                    rebuild_phase = state.phase.as_str(),
                    rebuild_collect_buy_mints_mode = state.payload.collect_buy_mints_mode.as_str(),
                    rebuild_replay_mode = state.payload.replay_mode.as_str(),
                    rebuild_window_start = %state.window_start,
                    rebuild_horizon_end = %state.horizon_end,
                    rebuild_metrics_window_start = %state.metrics_window_start,
                    rebuild_phase_cursor_ts = ?state.phase_cursor.as_ref().map(|cursor| cursor.ts_utc),
                    rebuild_phase_cursor_slot = state.phase_cursor.as_ref().map(|cursor| cursor.slot),
                    rebuild_phase_cursor_signature =
                        state.phase_cursor.as_ref().map(|cursor| cursor.signature.as_str()),
                    rebuild_collect_buy_mints_cursor_token =
                        state.payload.collect_buy_mints_cursor_token.as_deref(),
                    rebuild_collect_buy_mints_reconcile_source_window_start = ?state
                        .payload
                        .collect_buy_mints_reconcile_source_window_start,
                    rebuild_collect_buy_mints_reconcile_source_horizon_end = ?state
                        .payload
                        .collect_buy_mints_reconcile_source_horizon_end,
                    rebuild_collect_buy_mints_reconcile_new_tail_slice_end_token = state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_slice_end_token
                        .as_deref(),
                    rebuild_prepass_rows_processed = state.prepass_rows_processed,
                    rebuild_replay_rows_processed = state.replay_rows_processed,
                    rebuild_replay_wallet_stats_complete =
                        state.payload.replay_wallet_stats_complete,
                    rebuild_replay_wallet_stats_rows_processed =
                        state.payload.replay_wallet_stats_rows_processed,
                    rebuild_replay_wallet_stats_pages_processed =
                        state.payload.replay_wallet_stats_pages_processed,
                    rebuild_replay_wallet_stats_fast_path_pages_processed = state
                        .payload
                        .replay_wallet_stats_day_count_source_progress
                        .fast_path_pages_processed,
                    rebuild_replay_wallet_stats_fallback_pages_processed = state
                        .payload
                        .replay_wallet_stats_day_count_source_progress
                        .fallback_pages_processed,
                    rebuild_replay_wallet_stats_fast_path_wallets_processed = state
                        .payload
                        .replay_wallet_stats_day_count_source_progress
                        .fast_path_wallets_processed,
                    rebuild_replay_wallet_stats_fallback_wallets_processed = state
                        .payload
                        .replay_wallet_stats_day_count_source_progress
                        .fallback_wallets_processed,
                    rebuild_quality_next_mint_index = state.payload.token_quality_progress.next_mint_index,
                    rebuild_quality_rpc_attempted = state.payload.token_quality_progress.rpc_attempted,
                    rebuild_quality_rpc_spent_ms = state.payload.token_quality_progress.rpc_spent_ms,
                    rebuild_chunks_completed = state.chunks_completed,
                    "{message}"
                );
            }
        }

        let cycle_started = Instant::now();
        let deadline = cycle_started + rebuild_time_budget;
        let mut cycle_rows_processed = 0usize;
        let mut cycle_pages_processed = 0usize;
        let mut cycle_replay_wallet_stats_day_count_source_progress =
            ReplayWalletStatsDayCountSourceProgress::default();
        let mut cycle_unique_buy_mints_discovered = 0usize;
        let mut cycle_replay_sol_leg_access_path = None;
        if state.phase == DiscoveryPersistedRebuildPhase::PublishPending {
            let snapshots = Self::publish_pending_snapshots(&state);
            let cycle_elapsed_ms = cycle_started.elapsed().as_millis() as u64;
            let telemetry = PersistedStreamProgressTelemetry {
                phase: DiscoveryPersistedRebuildPhase::PublishPending,
                collect_buy_mints_mode: state.payload.collect_buy_mints_mode,
                replay_mode: state.payload.replay_mode,
                window_start: state.window_start,
                horizon_end: state.horizon_end,
                metrics_window_start: state.metrics_window_start,
                phase_cursor: None,
                collect_buy_mints_cursor_token: None,
                collect_buy_mints_reconcile_source_window_start: state
                    .payload
                    .collect_buy_mints_reconcile_source_window_start,
                collect_buy_mints_reconcile_source_horizon_end: state
                    .payload
                    .collect_buy_mints_reconcile_source_horizon_end,
                collect_buy_mints_reconcile_expired_head_cursor: state
                    .payload
                    .collect_buy_mints_reconcile_expired_head_cursor
                    .clone(),
                collect_buy_mints_reconcile_new_tail_cursor: state
                    .payload
                    .collect_buy_mints_reconcile_new_tail_cursor
                    .clone(),
                collect_buy_mints_reconcile_expired_head_cursor_token: state
                    .payload
                    .collect_buy_mints_reconcile_expired_head_cursor_token
                    .clone(),
                collect_buy_mints_reconcile_new_tail_cursor_token: state
                    .payload
                    .collect_buy_mints_reconcile_new_tail_cursor_token
                    .clone(),
                collect_buy_mints_reconcile_expired_head_pending_mints: state
                    .payload
                    .collect_buy_mints_reconcile_expired_head_pending_mints
                    .len(),
                collect_buy_mints_reconcile_new_tail_slice_end_token: state
                    .payload
                    .collect_buy_mints_reconcile_new_tail_slice_end_token
                    .clone(),
                collect_buy_mints_reconcile_new_tail_pending_mints: state
                    .payload
                    .collect_buy_mints_reconcile_new_tail_pending_mints
                    .len(),
                prepass_rows_processed: state.prepass_rows_processed,
                prepass_pages_processed: state.prepass_pages_processed,
                replay_wallet_stats_complete: state.payload.replay_wallet_stats_complete,
                replay_wallet_stats_rows_processed: state
                    .payload
                    .replay_wallet_stats_rows_processed,
                replay_wallet_stats_pages_processed: state
                    .payload
                    .replay_wallet_stats_pages_processed,
                replay_wallet_stats_day_count_source_progress: state
                    .payload
                    .replay_wallet_stats_day_count_source_progress,
                replay_sol_leg_access_path: None,
                replay_rows_processed: state.replay_rows_processed,
                replay_pages_processed: state.replay_pages_processed,
                chunks_completed: state.chunks_completed,
                cycle_rows_processed: 0,
                cycle_pages_processed: 0,
                cycle_replay_wallet_stats_day_count_source_progress:
                    ReplayWalletStatsDayCountSourceProgress::default(),
                cycle_unique_buy_mints_discovered: 0,
                observed_swaps_loaded: Self::persisted_stream_observed_swaps_loaded(&state),
                unique_buy_mints: state.payload.unique_buy_mints.len(),
                quality_next_mint_index: state.payload.token_quality_progress.next_mint_index,
                quality_rpc_attempted: state.payload.token_quality_progress.rpc_attempted,
                quality_rpc_spent_ms: state.payload.token_quality_progress.rpc_spent_ms,
                wallets_buffered: snapshots.len(),
                started_at: state.started_at,
                cycle_elapsed_ms,
                total_elapsed_ms: (now
                    .signed_duration_since(state.started_at)
                    .num_milliseconds()
                    .max(0) as u64)
                    .saturating_add(cycle_elapsed_ms),
                partial: false,
                completed: true,
                budget_exhausted_reason: None,
            };
            self.log_persisted_stream_progress(
                &telemetry,
                "resuming bounded discovery persisted observed_swaps rebuild from publish-pending checkpoint",
            );
            return Ok(PersistedStreamRebuildAdvanceOutcome::Completed {
                snapshots,
                telemetry,
            });
        }
        let budget_exhausted_reason = loop {
            if state.metrics_window_start != metrics_window_start
                && self.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                    &mut state,
                    window_start,
                    metrics_window_start,
                    now,
                )?
            {
                continue;
            }
            if Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            let active_phase = state.phase;
            let phase_advance = match active_phase {
                DiscoveryPersistedRebuildPhase::CollectBuyMints => self
                    .advance_persisted_stream_prepass(
                        store,
                        &mut state,
                        fetch_limit,
                        fetch_page_limit,
                        deadline,
                    )?,
                DiscoveryPersistedRebuildPhase::ResolveTokenQuality => self
                    .advance_persisted_stream_token_quality(
                        store,
                        &mut state,
                        fetch_limit,
                        fetch_page_limit,
                        deadline,
                    )?,
                DiscoveryPersistedRebuildPhase::Replay => self.advance_persisted_stream_replay(
                    store,
                    &mut state,
                    fetch_limit,
                    fetch_page_limit,
                    deadline,
                )?,
                DiscoveryPersistedRebuildPhase::PublishPending => {
                    unreachable!(
                        "publish-pending checkpoints are returned before phase advancement"
                    )
                }
            };
            cycle_rows_processed = cycle_rows_processed
                .saturating_add(phase_advance.rows_processed)
                .saturating_add(phase_advance.replay_wallet_stats_rows_processed);
            cycle_pages_processed = cycle_pages_processed
                .saturating_add(phase_advance.pages_processed)
                .saturating_add(phase_advance.replay_wallet_stats_pages_processed);
            cycle_replay_wallet_stats_day_count_source_progress
                .merge(phase_advance.replay_wallet_stats_day_count_source_progress);
            cycle_unique_buy_mints_discovered = cycle_unique_buy_mints_discovered
                .saturating_add(phase_advance.unique_buy_mints_discovered);
            if phase_advance.replay_sol_leg_access_path.is_some() {
                cycle_replay_sol_leg_access_path = phase_advance.replay_sol_leg_access_path;
            }

            match state.phase {
                DiscoveryPersistedRebuildPhase::CollectBuyMints => {
                    state.prepass_rows_processed = state
                        .prepass_rows_processed
                        .saturating_add(phase_advance.rows_processed);
                    state.prepass_pages_processed = state
                        .prepass_pages_processed
                        .saturating_add(phase_advance.pages_processed);
                }
                DiscoveryPersistedRebuildPhase::ResolveTokenQuality => {}
                DiscoveryPersistedRebuildPhase::Replay => {
                    state.payload.replay_wallet_stats_rows_processed = state
                        .payload
                        .replay_wallet_stats_rows_processed
                        .saturating_add(phase_advance.replay_wallet_stats_rows_processed);
                    state.payload.replay_wallet_stats_pages_processed = state
                        .payload
                        .replay_wallet_stats_pages_processed
                        .saturating_add(phase_advance.replay_wallet_stats_pages_processed);
                    state
                        .payload
                        .replay_wallet_stats_day_count_source_progress
                        .merge(phase_advance.replay_wallet_stats_day_count_source_progress);
                    state.replay_rows_processed = state
                        .replay_rows_processed
                        .saturating_add(phase_advance.rows_processed);
                    state.replay_pages_processed = state
                        .replay_pages_processed
                        .saturating_add(phase_advance.pages_processed);
                }
                DiscoveryPersistedRebuildPhase::PublishPending => {}
            }
            state.phase_cursor = phase_advance.phase_cursor;
            if state.phase == DiscoveryPersistedRebuildPhase::CollectBuyMints {
                state.payload.collect_buy_mints_cursor_token =
                    phase_advance.collect_buy_mints_cursor_token.clone();
            }

            if phase_advance.source_exhausted {
                if active_phase == DiscoveryPersistedRebuildPhase::CollectBuyMints {
                    if Self::payload_has_exact_buy_mint_membership(&state.payload) {
                        Self::sync_unique_buy_mints_from_counts(&mut state.payload);
                    }
                    if Self::canonicalize_unique_buy_mints(&mut state.payload.unique_buy_mints) {
                        warn!(
                            rebuild_window_start = %state.window_start,
                            rebuild_horizon_end = %state.horizon_end,
                            rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                            "normalized collect_buy_mints output to canonical sorted distinct order before token-quality resolution"
                        );
                    }
                    state.phase = DiscoveryPersistedRebuildPhase::ResolveTokenQuality;
                    state.phase_cursor = None;
                    state.payload.collect_buy_mints_cursor_token = None;
                    state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
                    state.payload.collect_buy_mints_prepass_complete = true;
                    state
                        .payload
                        .collect_buy_mints_reconcile_source_window_start = None;
                    state.payload.collect_buy_mints_reconcile_source_horizon_end = None;
                    state
                        .payload
                        .collect_buy_mints_reconcile_expired_head_cursor = None;
                    state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
                    state
                        .payload
                        .collect_buy_mints_reconcile_expired_head_cursor_token = None;
                    state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_cursor_token = None;
                    state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
                    Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
                    Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
                    info!(
                        rebuild_window_start = %state.window_start,
                        rebuild_horizon_end = %state.horizon_end,
                        rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                        rebuild_cycle_unique_buy_mints_discovered =
                            cycle_unique_buy_mints_discovered,
                        rebuild_prepass_rows_processed = state.prepass_rows_processed,
                        rebuild_prepass_pages_processed = state.prepass_pages_processed,
                        "completed bounded discovery persisted observed_swaps prepass; switching to bounded token-quality resolution"
                    );
                    continue;
                }
                if active_phase == DiscoveryPersistedRebuildPhase::ResolveTokenQuality {
                    state.phase = DiscoveryPersistedRebuildPhase::Replay;
                    state.phase_cursor = None;
                    state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
                    Self::reset_replay_wallet_stats_progress(&mut state.payload);
                    info!(
                        rebuild_window_start = %state.window_start,
                        rebuild_horizon_end = %state.horizon_end,
                        rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                        rebuild_quality_next_mint_index = state.payload.token_quality_progress.next_mint_index,
                        rebuild_quality_rpc_attempted = state.payload.token_quality_progress.rpc_attempted,
                        rebuild_quality_rpc_spent_ms = state.payload.token_quality_progress.rpc_spent_ms,
                        "completed bounded discovery token-quality resolution; switching to replay"
                    );
                    continue;
                }

                self.finalize_all_streaming_rug_metrics(
                    &mut state.payload.by_wallet,
                    &mut state.payload.token_recent_sol_trades,
                    &mut state.payload.pending_rug_checks,
                    &mut state.payload.token_pending_buy_starts,
                    state.horizon_end,
                    Duration::seconds(self.config.rug_lookahead_seconds.max(1) as i64),
                );
                let empty_token_sol_history = HashMap::new();
                let unique_buy_mints = state.payload.unique_buy_mints.len();
                let by_wallet = std::mem::take(&mut state.payload.by_wallet);
                let snapshots = self.wallet_snapshots_from_accumulators(
                    store,
                    by_wallet,
                    state.horizon_end,
                    &empty_token_sol_history,
                )?;
                state.phase = DiscoveryPersistedRebuildPhase::PublishPending;
                state.phase_cursor = None;
                state.payload.completed_snapshots = snapshots.clone();
                state.payload.token_quality_cache.clear();
                state.payload.token_states.clear();
                state.payload.token_recent_sol_trades.clear();
                state.payload.pending_rug_checks.clear();
                state.payload.token_pending_buy_starts.clear();
                self.persist_persisted_stream_rebuild_state(store, &mut state, now)?;
                let cycle_elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                let telemetry = PersistedStreamProgressTelemetry {
                    phase: DiscoveryPersistedRebuildPhase::PublishPending,
                    collect_buy_mints_mode: state.payload.collect_buy_mints_mode,
                    replay_mode: state.payload.replay_mode,
                    window_start: state.window_start,
                    horizon_end: state.horizon_end,
                    metrics_window_start: state.metrics_window_start,
                    phase_cursor: None,
                    collect_buy_mints_cursor_token: None,
                    collect_buy_mints_reconcile_source_window_start: state
                        .payload
                        .collect_buy_mints_reconcile_source_window_start,
                    collect_buy_mints_reconcile_source_horizon_end: state
                        .payload
                        .collect_buy_mints_reconcile_source_horizon_end,
                    collect_buy_mints_reconcile_expired_head_cursor: state
                        .payload
                        .collect_buy_mints_reconcile_expired_head_cursor
                        .clone(),
                    collect_buy_mints_reconcile_new_tail_cursor: state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_cursor
                        .clone(),
                    collect_buy_mints_reconcile_expired_head_cursor_token: state
                        .payload
                        .collect_buy_mints_reconcile_expired_head_cursor_token
                        .clone(),
                    collect_buy_mints_reconcile_new_tail_cursor_token: state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_cursor_token
                        .clone(),
                    collect_buy_mints_reconcile_expired_head_pending_mints: state
                        .payload
                        .collect_buy_mints_reconcile_expired_head_pending_mints
                        .len(),
                    collect_buy_mints_reconcile_new_tail_slice_end_token: state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_slice_end_token
                        .clone(),
                    collect_buy_mints_reconcile_new_tail_pending_mints: state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_pending_mints
                        .len(),
                    prepass_rows_processed: state.prepass_rows_processed,
                    prepass_pages_processed: state.prepass_pages_processed,
                    replay_wallet_stats_complete: state.payload.replay_wallet_stats_complete,
                    replay_wallet_stats_rows_processed: state
                        .payload
                        .replay_wallet_stats_rows_processed,
                    replay_wallet_stats_pages_processed: state
                        .payload
                        .replay_wallet_stats_pages_processed,
                    replay_wallet_stats_day_count_source_progress: state
                        .payload
                        .replay_wallet_stats_day_count_source_progress,
                    replay_sol_leg_access_path: cycle_replay_sol_leg_access_path,
                    replay_rows_processed: state.replay_rows_processed,
                    replay_pages_processed: state.replay_pages_processed,
                    chunks_completed: state.chunks_completed,
                    cycle_rows_processed,
                    cycle_pages_processed,
                    cycle_replay_wallet_stats_day_count_source_progress,
                    cycle_unique_buy_mints_discovered,
                    observed_swaps_loaded: Self::persisted_stream_observed_swaps_loaded(&state),
                    unique_buy_mints,
                    quality_next_mint_index: state.payload.token_quality_progress.next_mint_index,
                    quality_rpc_attempted: state.payload.token_quality_progress.rpc_attempted,
                    quality_rpc_spent_ms: state.payload.token_quality_progress.rpc_spent_ms,
                    wallets_buffered: snapshots.len(),
                    started_at: state.started_at,
                    cycle_elapsed_ms,
                    total_elapsed_ms: (now
                        .signed_duration_since(state.started_at)
                        .num_milliseconds()
                        .max(0) as u64)
                        .saturating_add(cycle_elapsed_ms),
                    partial: false,
                    completed: true,
                    budget_exhausted_reason: None,
                };
                self.log_persisted_stream_progress(
                    &telemetry,
                    "completed bounded discovery persisted observed_swaps rebuild",
                );
                return Ok(PersistedStreamRebuildAdvanceOutcome::Completed {
                    snapshots,
                    telemetry,
                });
            }

            if phase_advance.budget_exhausted_reason.is_some() {
                break phase_advance.budget_exhausted_reason;
            }
        };

        state.chunks_completed = state.chunks_completed.saturating_add(1);
        self.persist_persisted_stream_rebuild_state(store, &mut state, now)?;
        let cycle_elapsed_ms = cycle_started.elapsed().as_millis() as u64;
        let telemetry = PersistedStreamProgressTelemetry {
            phase: state.phase,
            collect_buy_mints_mode: state.payload.collect_buy_mints_mode,
            replay_mode: state.payload.replay_mode,
            window_start: state.window_start,
            horizon_end: state.horizon_end,
            metrics_window_start: state.metrics_window_start,
            phase_cursor: state.phase_cursor.clone(),
            collect_buy_mints_cursor_token: state.payload.collect_buy_mints_cursor_token.clone(),
            collect_buy_mints_reconcile_source_window_start: state
                .payload
                .collect_buy_mints_reconcile_source_window_start,
            collect_buy_mints_reconcile_source_horizon_end: state
                .payload
                .collect_buy_mints_reconcile_source_horizon_end,
            collect_buy_mints_reconcile_expired_head_cursor: state
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor
                .clone(),
            collect_buy_mints_reconcile_new_tail_cursor: state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor
                .clone(),
            collect_buy_mints_reconcile_expired_head_cursor_token: state
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor_token
                .clone(),
            collect_buy_mints_reconcile_new_tail_cursor_token: state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .clone(),
            collect_buy_mints_reconcile_expired_head_pending_mints: state
                .payload
                .collect_buy_mints_reconcile_expired_head_pending_mints
                .len(),
            collect_buy_mints_reconcile_new_tail_slice_end_token: state
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token
                .clone(),
            collect_buy_mints_reconcile_new_tail_pending_mints: state
                .payload
                .collect_buy_mints_reconcile_new_tail_pending_mints
                .len(),
            prepass_rows_processed: state.prepass_rows_processed,
            prepass_pages_processed: state.prepass_pages_processed,
            replay_wallet_stats_complete: state.payload.replay_wallet_stats_complete,
            replay_wallet_stats_rows_processed: state.payload.replay_wallet_stats_rows_processed,
            replay_wallet_stats_pages_processed: state.payload.replay_wallet_stats_pages_processed,
            replay_wallet_stats_day_count_source_progress: state
                .payload
                .replay_wallet_stats_day_count_source_progress,
            replay_sol_leg_access_path: cycle_replay_sol_leg_access_path,
            replay_rows_processed: state.replay_rows_processed,
            replay_pages_processed: state.replay_pages_processed,
            chunks_completed: state.chunks_completed,
            cycle_rows_processed,
            cycle_pages_processed,
            cycle_replay_wallet_stats_day_count_source_progress,
            cycle_unique_buy_mints_discovered,
            observed_swaps_loaded: Self::persisted_stream_observed_swaps_loaded(&state),
            unique_buy_mints: state.payload.unique_buy_mints.len(),
            quality_next_mint_index: state.payload.token_quality_progress.next_mint_index,
            quality_rpc_attempted: state.payload.token_quality_progress.rpc_attempted,
            quality_rpc_spent_ms: state.payload.token_quality_progress.rpc_spent_ms,
            wallets_buffered: if state.phase == DiscoveryPersistedRebuildPhase::PublishPending {
                state.payload.completed_snapshots.len()
            } else {
                state.payload.by_wallet.len()
            },
            started_at: state.started_at,
            cycle_elapsed_ms,
            total_elapsed_ms: (now
                .signed_duration_since(state.started_at)
                .num_milliseconds()
                .max(0) as u64)
                .saturating_add(cycle_elapsed_ms),
            partial: true,
            completed: false,
            budget_exhausted_reason,
        };
        self.log_persisted_stream_progress(
            &telemetry,
            "yielding bounded discovery persisted observed_swaps rebuild back to scheduler",
        );
        Ok(PersistedStreamRebuildAdvanceOutcome::InProgress { telemetry })
    }

    pub fn run_cycle(&self, store: &SqliteStore, now: DateTime<Utc>) -> Result<DiscoverySummary> {
        let cycle_started = Instant::now();
        let publish_interval_seconds = self.config.refresh_seconds.max(1) as i64;
        let window_days = self.config.scoring_window_days.max(1);
        let window_start = now - Duration::days(window_days as i64);
        let metrics_window_start = self.metrics_window_start(now);
        let mut delta_fetched = 0usize;
        let mut swaps_evicted_due_cap = 0usize;
        let mut swaps_warm_loaded = 0usize;
        let max_window_swaps_in_memory = self.config.max_window_swaps_in_memory.max(1);
        let fetch_limit = self.config.max_fetch_swaps_per_cycle.max(1);
        let fetch_page_limit = self.config.max_fetch_pages_per_cycle.max(1);
        let fetch_time_budget = StdDuration::from_millis(self.config.fetch_time_budget_ms.max(1));
        let retention_days = self.config.observed_swaps_retention_days.max(1);
        let short_retention_window = retention_days < window_days;
        let runtime_publication_truth_resolution =
            self.runtime_publication_truth_resolution(store, now)?;
        let recent_published_follow_wallets = match runtime_publication_truth_resolution.as_ref() {
            Some(RuntimePublicationTruthResolution::Recent(truth)) => Some(truth.active_wallets()),
            Some(RuntimePublicationTruthResolution::BootstrapDegraded(_)) | None => None,
        };
        let bootstrap_degraded_follow_wallets = match runtime_publication_truth_resolution.as_ref()
        {
            Some(RuntimePublicationTruthResolution::BootstrapDegraded(truth)) => {
                Some(truth.active_wallets())
            }
            Some(RuntimePublicationTruthResolution::Recent(_)) | None => None,
        };
        let (swaps_window, fetch_progress, cap_truncation_telemetry, prepared_cycle) = {
            let mut state = match self.window_state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    warn!("discovery window mutex poisoned; continuing with recovered state");
                    poisoned.into_inner()
                }
            };
            state.evict_before(window_start);
            state.clear_cap_truncation_if_window_caught_up(window_start);
            if !short_retention_window {
                state.bootstrap_from_persisted_metrics = false;
            }
            let publish_due = state.last_publish_at.map_or(true, |last_publish_at| {
                now.signed_duration_since(last_publish_at).num_seconds() >= publish_interval_seconds
            });
            let mut out_of_order = false;
            let mut cursor_restored_from_store = false;
            if state.cursor.is_none() {
                let restored = match store.load_discovery_runtime_cursor() {
                    Ok(cursor) => cursor,
                    Err(error) => {
                        if discovery_runtime_cursor_load_error_requires_abort(&error) {
                            return Err(error).context(
                                "failed loading discovery runtime cursor with fatal sqlite I/O",
                            );
                        }
                        warn!(
                            error = %error,
                            "failed loading discovery runtime cursor; falling back to window_start bootstrap"
                        );
                        None
                    }
                };
                cursor_restored_from_store = restored.is_some();
                let restored = restored.map(|cursor| DiscoveryCursor {
                    ts_utc: cursor.ts_utc,
                    slot: cursor.slot,
                    signature: cursor.signature,
                });
                state.cursor =
                    Some(restored.unwrap_or_else(|| DiscoveryCursor::bootstrap(window_start)));
            }

            let mut cursor = state
                .cursor
                .clone()
                .unwrap_or_else(|| DiscoveryCursor::bootstrap(window_start));
            if cursor.ts_utc < window_start {
                cursor = DiscoveryCursor::bootstrap(window_start);
            }
            if state.swaps.is_empty() && cursor_restored_from_store {
                if short_retention_window {
                    state.bootstrap_from_persisted_metrics = true;
                }
                match store
                    .load_recent_observed_swaps_since(window_start, max_window_swaps_in_memory)
                {
                    Ok((swaps, truncated_by_limit)) => {
                        for swap in swaps {
                            if let Some(back) = state.swaps.back() {
                                if cmp_swap_order(&swap, back) == Ordering::Less {
                                    out_of_order = true;
                                }
                            }
                            if state.signatures.insert(swap.signature.clone()) {
                                swaps_evicted_due_cap = swaps_evicted_due_cap.saturating_add(
                                    state.push_swap_capped(swap, max_window_swaps_in_memory),
                                );
                                swaps_warm_loaded = swaps_warm_loaded.saturating_add(1);
                            }
                        }
                        if truncated_by_limit {
                            state.mark_warm_load_truncated();
                            maybe_arm_cap_truncation_deactivation_guard(
                                &mut state,
                                now,
                                CapTruncationDeactivationGuardReason::WarmLoadTruncated,
                            );
                        }
                    }
                    Err(error) => {
                        if discovery_recent_window_load_error_requires_abort(&error) {
                            return Err(error).context(
                            "failed warm-loading discovery window from sqlite recent slice with fatal sqlite I/O",
                        );
                        }
                        warn!(
                            error = %error,
                            "failed warm-loading discovery window from sqlite recent slice"
                        );
                    }
                }
            }
            if !state.swaps.is_empty() {
                state.bootstrap_from_persisted_metrics = false;
            }

            let mut fetch_progress = FetchProgress::default();
            let fetch_deadline = Instant::now() + fetch_time_budget;
            loop {
                if fetch_progress.pages >= fetch_page_limit {
                    fetch_progress.page_budget_exhausted =
                        fetch_progress.query_rows_last_page >= fetch_limit;
                    break;
                }
                if Instant::now() >= fetch_deadline {
                    fetch_progress.time_budget_exhausted = true;
                    break;
                }

                let cursor_signature = cursor.signature.clone();
                let page_result = store.for_each_observed_swap_after_cursor_with_budget(
                    cursor.ts_utc,
                    cursor.slot,
                    cursor_signature.as_str(),
                    fetch_limit,
                    fetch_deadline,
                    |swap| {
                        cursor = DiscoveryCursor::from_swap(&swap);
                        if swap.ts_utc < window_start {
                            return Ok(());
                        }
                        if state.signatures.contains(&swap.signature) {
                            return Ok(());
                        }
                        if let Some(back) = state.swaps.back() {
                            if cmp_swap_order(&swap, back) == Ordering::Less {
                                out_of_order = true;
                            }
                        }
                        state.signatures.insert(swap.signature.clone());
                        let evicted = state.push_swap_capped(swap, max_window_swaps_in_memory);
                        if evicted > 0 {
                            maybe_arm_cap_truncation_deactivation_guard(
                                &mut state,
                                now,
                                CapTruncationDeactivationGuardReason::LiveCapEviction,
                            );
                        }
                        swaps_evicted_due_cap = swaps_evicted_due_cap.saturating_add(evicted);
                        delta_fetched = delta_fetched.saturating_add(1);
                        Ok(())
                    },
                )?;
                let page_rows = page_result.rows_seen;
                fetch_progress.pages = fetch_progress.pages.saturating_add(1);
                fetch_progress.query_rows = fetch_progress.query_rows.saturating_add(page_rows);
                fetch_progress.query_rows_last_page = page_rows;
                fetch_progress.time_budget_exhausted |= page_result.time_budget_exhausted;

                if page_result.time_budget_exhausted {
                    break;
                }

                if page_rows < fetch_limit {
                    break;
                }
            }
            fetch_progress.saturated =
                fetch_progress.page_budget_exhausted || fetch_progress.time_budget_exhausted;

            if fetch_progress.query_rows > 0 {
                state.cursor = Some(cursor.clone());
                let persisted = DiscoveryRuntimeCursor {
                    ts_utc: cursor.ts_utc,
                    slot: cursor.slot,
                    signature: cursor.signature,
                };
                if let Err(error) = store.upsert_discovery_runtime_cursor(&persisted) {
                    if discovery_runtime_cursor_error_requires_abort(&error) {
                        return Err(error).context(
                            "failed persisting discovery runtime cursor with fatal sqlite I/O",
                        );
                    }
                    warn!(
                        error = %error,
                        "failed persisting discovery runtime cursor"
                    );
                }
            }

            if out_of_order {
                let mut sorted: Vec<SwapEvent> = state.swaps.drain(..).collect();
                sorted.sort_by(cmp_swap_order);
                state.swaps = sorted.into();
            }
            let raw_window_history_incomplete =
                raw_window_history_incomplete_for_followlist_or_metrics(&state);
            let followlist_deactivations_suppressed =
                state.cap_truncation_deactivations_suppressed(raw_window_history_incomplete);
            let cap_truncation_telemetry =
                snapshot_cap_truncation_telemetry(&state, followlist_deactivations_suppressed);
            if state.consume_cap_truncation_deactivation_guard_cycle() {
                maybe_warn_on_cap_truncation_deactivation_guard_expiry(
                    &state,
                    followlist_deactivations_suppressed,
                );
            }
            state.bootstrap_from_persisted_metrics = false;
            state.truncated_warm_restore_bootstrap = false;
            state.trusted_selection_bootstrap_pending = false;
            let swaps_window = state.swaps.len();
            let persisted_raw_window_complete =
                if swaps_window == 0 || raw_window_history_incomplete || short_retention_window {
                    self.persisted_observed_swaps_cover_window(store, window_start)?
                } else {
                    false
                };
            if swaps_window == 0 {
                state.clear_cap_truncation();
                state.last_snapshot_bucket = None;
                state.last_summary = None;
                state.clear_exact_current_raw_truth();
                if persisted_raw_window_complete {
                    (
                        swaps_window,
                        fetch_progress,
                        cap_truncation_telemetry,
                        PreparedCycleState::PersistedRecompute {
                            publish_due,
                            scoring_source: "raw_window_persisted_stream",
                            empty_window_degraded_scoring_source:
                                "published_universe_raw_window_unavailable",
                            empty_window_bootstrap_degraded_scoring_source:
                                "bootstrap_degraded_publication_truth_raw_window_unavailable",
                            empty_window_unusable_scoring_source:
                                "raw_window_unusable_no_recent_published_universe",
                        },
                    )
                } else if let Some(active_wallets) = recent_published_follow_wallets.clone() {
                    (
                        swaps_window,
                        fetch_progress,
                        cap_truncation_telemetry,
                        PreparedCycleState::Degraded {
                            publish_due,
                            active_wallets,
                            scoring_source: "published_universe_raw_window_unavailable",
                        },
                    )
                } else if let Some(active_wallets) = bootstrap_degraded_follow_wallets.clone() {
                    (
                        swaps_window,
                        fetch_progress,
                        cap_truncation_telemetry,
                        PreparedCycleState::BootstrapDegraded {
                            active_wallets,
                            scoring_source:
                                "bootstrap_degraded_publication_truth_raw_window_unavailable",
                        },
                    )
                } else {
                    (
                        swaps_window,
                        fetch_progress,
                        cap_truncation_telemetry,
                        PreparedCycleState::Unusable {
                            publish_due,
                            scoring_source: "raw_window_unusable_no_recent_published_universe",
                        },
                    )
                }
            } else if state.last_snapshot_bucket == Some(metrics_window_start)
                && state.last_summary.is_some()
            {
                (
                    swaps_window,
                    fetch_progress,
                    cap_truncation_telemetry,
                    PreparedCycleState::Cached {
                        publish_due,
                        followlist_activations_suppressed: false,
                        followlist_deactivations_suppressed,
                        summary: state
                            .last_summary
                            .clone()
                            .expect("checked last_summary exists above"),
                        current_raw: state.last_exact_current_raw_truth.clone(),
                    },
                )
            } else if raw_window_history_incomplete || short_retention_window {
                state.bootstrap_from_persisted_metrics = false;
                state.truncated_warm_restore_bootstrap = false;
                if persisted_raw_window_complete {
                    (
                        swaps_window,
                        fetch_progress,
                        cap_truncation_telemetry,
                        PreparedCycleState::PersistedRecompute {
                            publish_due,
                            scoring_source: "raw_window_persisted_stream",
                            empty_window_degraded_scoring_source: if raw_window_history_incomplete {
                                "published_universe_raw_window_degraded"
                            } else {
                                "published_universe_short_retention_degraded"
                            },
                            empty_window_bootstrap_degraded_scoring_source:
                                if raw_window_history_incomplete {
                                    "bootstrap_degraded_publication_truth_raw_window_degraded"
                                } else {
                                    "bootstrap_degraded_publication_truth_short_retention_degraded"
                                },
                            empty_window_unusable_scoring_source: if raw_window_history_incomplete {
                                "raw_window_incomplete_no_recent_published_universe"
                            } else {
                                "raw_window_short_retention_no_recent_published_universe"
                            },
                        },
                    )
                } else if let Some(active_wallets) = recent_published_follow_wallets.clone() {
                    (
                        swaps_window,
                        fetch_progress,
                        cap_truncation_telemetry,
                        PreparedCycleState::Degraded {
                            publish_due,
                            active_wallets,
                            scoring_source: if raw_window_history_incomplete {
                                "published_universe_raw_window_degraded"
                            } else {
                                "published_universe_short_retention_degraded"
                            },
                        },
                    )
                } else if let Some(active_wallets) = bootstrap_degraded_follow_wallets.clone() {
                    (
                        swaps_window,
                        fetch_progress,
                        cap_truncation_telemetry,
                        PreparedCycleState::BootstrapDegraded {
                            active_wallets,
                            scoring_source: if raw_window_history_incomplete {
                                "bootstrap_degraded_publication_truth_raw_window_degraded"
                            } else {
                                "bootstrap_degraded_publication_truth_short_retention_degraded"
                            },
                        },
                    )
                } else {
                    (
                        swaps_window,
                        fetch_progress,
                        cap_truncation_telemetry,
                        PreparedCycleState::Unusable {
                            publish_due,
                            scoring_source: if raw_window_history_incomplete {
                                "raw_window_incomplete_no_recent_published_universe"
                            } else {
                                "raw_window_short_retention_no_recent_published_universe"
                            },
                        },
                    )
                }
            } else {
                (
                    swaps_window,
                    fetch_progress,
                    cap_truncation_telemetry,
                    PreparedCycleState::Recompute {
                        publish_due,
                        followlist_activations_suppressed: false,
                        followlist_deactivations_suppressed,
                        metrics_persistence_suppressed: false,
                        swaps: state.swaps.clone(),
                    },
                )
            }
        };

        let (
            publish_due,
            followlist_activations_suppressed,
            followlist_deactivations_suppressed,
            metrics_persistence_suppressed,
            snapshots,
            observed_swaps_loaded_for_capture,
            scoring_source,
            effective_window_start,
            effective_metrics_window_start,
        ) = match prepared_cycle {
            PreparedCycleState::Degraded {
                publish_due,
                active_wallets,
                scoring_source,
            } => {
                let summary = self.degraded_summary_from_published_universe(
                    store,
                    window_start,
                    metrics_window_start,
                    publish_due,
                    active_wallets,
                    &cap_truncation_telemetry,
                    scoring_source,
                    scoring_source,
                    now,
                )?;
                store.clear_discovery_persisted_rebuild_state()?;
                let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                info!(
                    window_start = %summary.window_start,
                    wallets_seen = summary.wallets_seen,
                    eligible_wallets = summary.eligible_wallets,
                    metrics_written = summary.metrics_written,
                    follow_promoted = summary.follow_promoted,
                    follow_demoted = summary.follow_demoted,
                    active_follow_wallets = summary.active_follow_wallets,
                    swaps_window,
                    swaps_query_rows = fetch_progress.query_rows,
                    swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                    swaps_delta_fetched = delta_fetched,
                    swaps_warm_loaded,
                    swaps_evicted_due_cap,
                    swaps_fetch_limit = fetch_limit,
                    swaps_fetch_pages = fetch_progress.pages,
                    swaps_fetch_page_limit = fetch_page_limit,
                    swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                    swaps_fetch_limit_reached = fetch_progress.saturated,
                    swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                    swaps_fetch_time_budget_exhausted = fetch_progress.time_budget_exhausted,
                    metrics_window_start = %metrics_window_start,
                    scoring_source = summary.scoring_source,
                    discovery_runtime_mode = summary.runtime_mode.as_str(),
                    metrics_persisted = false,
                    snapshot_recomputed = false,
                    discovery_published = summary.published,
                    discovery_cycle_duration_ms = elapsed_ms,
                    top_wallets = ?summary.top_wallets,
                    "discovery cycle completed"
                );
                return Ok(summary);
            }
            PreparedCycleState::BootstrapDegraded {
                active_wallets,
                scoring_source,
            } => {
                let summary = self.bootstrap_degraded_summary_from_published_universe(
                    store,
                    window_start,
                    active_wallets,
                    &cap_truncation_telemetry,
                    scoring_source,
                    scoring_source,
                    now,
                )?;
                store.clear_discovery_persisted_rebuild_state()?;
                let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                warn!(
                    metrics_window_start = %metrics_window_start,
                    scoring_source = summary.scoring_source,
                    bootstrap_degraded_active = true,
                    "discovery runtime remains in explicit bootstrap-degraded mode while fresh raw truth is unavailable"
                );
                info!(
                    window_start = %summary.window_start,
                    wallets_seen = summary.wallets_seen,
                    eligible_wallets = summary.eligible_wallets,
                    metrics_written = summary.metrics_written,
                    follow_promoted = summary.follow_promoted,
                    follow_demoted = summary.follow_demoted,
                    active_follow_wallets = summary.active_follow_wallets,
                    swaps_window,
                    swaps_query_rows = fetch_progress.query_rows,
                    swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                    swaps_delta_fetched = delta_fetched,
                    swaps_warm_loaded,
                    swaps_evicted_due_cap,
                    swaps_fetch_limit = fetch_limit,
                    swaps_fetch_pages = fetch_progress.pages,
                    swaps_fetch_page_limit = fetch_page_limit,
                    swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                    swaps_fetch_limit_reached = fetch_progress.saturated,
                    swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                    swaps_fetch_time_budget_exhausted = fetch_progress.time_budget_exhausted,
                    metrics_window_start = %metrics_window_start,
                    scoring_source = summary.scoring_source,
                    discovery_runtime_mode = summary.runtime_mode.as_str(),
                    metrics_persisted = false,
                    snapshot_recomputed = false,
                    discovery_published = summary.published,
                    discovery_cycle_duration_ms = elapsed_ms,
                    top_wallets = ?summary.top_wallets,
                    "discovery cycle completed"
                );
                return Ok(summary);
            }
            PreparedCycleState::Unusable {
                publish_due,
                scoring_source,
                ..
            } => {
                let summary = self.fail_close_without_recent_universe(
                    store,
                    window_start,
                    metrics_window_start,
                    publish_due,
                    false,
                    &cap_truncation_telemetry,
                    scoring_source,
                    scoring_source,
                    now,
                )?;
                store.clear_discovery_persisted_rebuild_state()?;
                let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                warn!(
                    metrics_window_start = %metrics_window_start,
                    scoring_source = summary.scoring_source,
                    cleared_follow_wallets = summary.follow_demoted,
                    "discovery fail-closed because raw runtime truth is unavailable and no recent published universe exists"
                );
                info!(
                    window_start = %summary.window_start,
                    wallets_seen = summary.wallets_seen,
                    eligible_wallets = summary.eligible_wallets,
                    metrics_written = summary.metrics_written,
                    follow_promoted = summary.follow_promoted,
                    follow_demoted = summary.follow_demoted,
                    active_follow_wallets = summary.active_follow_wallets,
                    swaps_window,
                    swaps_query_rows = fetch_progress.query_rows,
                    swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                    swaps_delta_fetched = delta_fetched,
                    swaps_warm_loaded,
                    swaps_evicted_due_cap,
                    swaps_fetch_limit = fetch_limit,
                    swaps_fetch_pages = fetch_progress.pages,
                    swaps_fetch_page_limit = fetch_page_limit,
                    swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                    swaps_fetch_limit_reached = fetch_progress.saturated,
                    swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                    swaps_fetch_time_budget_exhausted = fetch_progress.time_budget_exhausted,
                    metrics_window_start = %metrics_window_start,
                    scoring_source = summary.scoring_source,
                    discovery_runtime_mode = summary.runtime_mode.as_str(),
                    metrics_persisted = false,
                    snapshot_recomputed = false,
                    discovery_published = summary.published,
                    discovery_cycle_duration_ms = elapsed_ms,
                    "discovery cycle completed"
                );
                return Ok(summary);
            }
            PreparedCycleState::Cached {
                publish_due,
                followlist_activations_suppressed,
                followlist_deactivations_suppressed,
                summary: previous_summary,
                current_raw,
            } => {
                let active_follow_wallets = store.list_active_follow_wallets()?.len();
                let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                let summary = DiscoverySummary {
                    window_start,
                    wallets_seen: previous_summary.wallets_seen,
                    eligible_wallets: previous_summary.eligible_wallets,
                    metrics_written: 0,
                    follow_promoted: 0,
                    follow_demoted: 0,
                    active_follow_wallets,
                    top_wallets: previous_summary.top_wallets.clone(),
                    published: publish_due,
                    ..DiscoverySummary::default()
                }
                .with_runtime_mode(previous_summary.runtime_mode)
                .with_scoring_source(previous_summary.scoring_source)
                .with_cap_truncation_telemetry(&cap_truncation_telemetry);
                if publish_due && summary.runtime_mode != DiscoveryRuntimeMode::BootstrapDegraded {
                    self.record_live_publish(now);
                }
                if summary.runtime_mode != DiscoveryRuntimeMode::BootstrapDegraded {
                    self.persist_publication_state(
                        store,
                        summary.runtime_mode,
                        publish_due,
                        metrics_window_start,
                        None,
                        summary.scoring_source,
                        summary.scoring_source,
                        now,
                    )?;
                }
                let capture_telemetry = self.maybe_persist_in_band_wallet_freshness_capture(
                    store,
                    now,
                    publish_due,
                    previous_summary.runtime_mode,
                    current_raw.map(|current_raw| PrecomputedWalletFreshnessCurrentRawTruth {
                        window_start: current_raw.window_start,
                        observed_swaps_loaded: current_raw.observed_swaps_loaded,
                        eligible_wallet_count: current_raw.eligible_wallet_count,
                        top_wallet_ids: current_raw.top_wallet_ids,
                    }),
                );
                let summary = summary.with_wallet_freshness_capture(&capture_telemetry);
                store.clear_discovery_persisted_rebuild_state()?;
                info!(
                    window_start = %summary.window_start,
                    wallets_seen = summary.wallets_seen,
                    eligible_wallets = summary.eligible_wallets,
                    metrics_written = summary.metrics_written,
                    follow_promoted = summary.follow_promoted,
                    follow_demoted = summary.follow_demoted,
                    active_follow_wallets = summary.active_follow_wallets,
                    swaps_window,
                    swaps_query_rows = fetch_progress.query_rows,
                    swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                    swaps_delta_fetched = delta_fetched,
                    swaps_warm_loaded,
                    swaps_evicted_due_cap,
                    swaps_fetch_limit = fetch_limit,
                    swaps_fetch_pages = fetch_progress.pages,
                    swaps_fetch_page_limit = fetch_page_limit,
                    swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                    swaps_fetch_limit_reached = fetch_progress.saturated,
                    swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                    swaps_fetch_time_budget_exhausted = fetch_progress.time_budget_exhausted,
                    metrics_window_start = %metrics_window_start,
                    scoring_source = summary.scoring_source,
                    metrics_persisted = false,
                    snapshot_recomputed = false,
                    discovery_published = summary.published,
                    followlist_activations_suppressed,
                    followlist_deactivations_suppressed,
                    discovery_cycle_duration_ms = elapsed_ms,
                    wallet_freshness_capture_state = summary.wallet_freshness_capture_state,
                    wallet_freshness_capture_reason = summary.wallet_freshness_capture_reason.as_deref(),
                    wallet_freshness_capture_id = summary.wallet_freshness_capture_id,
                    wallet_freshness_capture_captured_at = ?summary.wallet_freshness_capture_captured_at,
                    top_wallets = ?summary.top_wallets,
                    "discovery cycle completed"
                );
                if fetch_progress.saturated {
                    warn!(
                        swaps_query_rows = fetch_progress.query_rows,
                        swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                        swaps_fetch_limit = fetch_limit,
                        swaps_fetch_pages = fetch_progress.pages,
                        swaps_fetch_page_limit = fetch_page_limit,
                        swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                        swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                        swaps_fetch_time_budget_exhausted = fetch_progress.time_budget_exhausted,
                        "discovery swap fetch exhausted bounded per-cycle budget; backlog processing continues next cycle"
                    );
                }
                return Ok(summary);
            }
            PreparedCycleState::Recompute {
                publish_due,
                followlist_activations_suppressed,
                followlist_deactivations_suppressed,
                metrics_persistence_suppressed,
                swaps,
            } => (
                publish_due,
                followlist_activations_suppressed,
                followlist_deactivations_suppressed,
                metrics_persistence_suppressed,
                self.build_wallet_snapshots_from_cached(store, &swaps, now)?,
                swaps.len(),
                "raw_window",
                window_start,
                metrics_window_start,
            ),
            PreparedCycleState::PersistedRecompute {
                publish_due,
                scoring_source,
                empty_window_degraded_scoring_source,
                empty_window_bootstrap_degraded_scoring_source,
                empty_window_unusable_scoring_source,
            } => {
                match self.advance_persisted_stream_rebuild(
                    store,
                    window_start,
                    metrics_window_start,
                    now,
                    fetch_limit,
                    fetch_page_limit,
                    fetch_time_budget,
                )? {
                    PersistedStreamRebuildAdvanceOutcome::Completed {
                        snapshots,
                        telemetry,
                    } => {
                        if telemetry.observed_swaps_loaded == 0 {
                            warn!(
                                window_start = %telemetry.window_start,
                                metrics_window_start = %telemetry.metrics_window_start,
                                rebuild_horizon_end = %telemetry.horizon_end,
                                rebuild_total_elapsed_ms = telemetry.total_elapsed_ms,
                                "persisted observed_swaps runtime fallback completed with zero rows in the frozen scoring horizon; treating persisted raw truth as unavailable"
                            );
                            if let Some(active_wallets) = recent_published_follow_wallets.clone() {
                                store.clear_discovery_persisted_rebuild_state()?;
                                let summary = self.degraded_summary_from_published_universe(
                                    store,
                                    window_start,
                                    metrics_window_start,
                                    publish_due,
                                    active_wallets,
                                    &cap_truncation_telemetry,
                                    empty_window_degraded_scoring_source,
                                    empty_window_degraded_scoring_source,
                                    now,
                                )?;
                                let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                                info!(
                                    window_start = %summary.window_start,
                                    wallets_seen = summary.wallets_seen,
                                    eligible_wallets = summary.eligible_wallets,
                                    metrics_written = summary.metrics_written,
                                    follow_promoted = summary.follow_promoted,
                                    follow_demoted = summary.follow_demoted,
                                    active_follow_wallets = summary.active_follow_wallets,
                                    swaps_window,
                                    swaps_query_rows = fetch_progress.query_rows,
                                    swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                                    swaps_delta_fetched = delta_fetched,
                                    swaps_warm_loaded,
                                    swaps_evicted_due_cap,
                                    swaps_fetch_limit = fetch_limit,
                                    swaps_fetch_pages = fetch_progress.pages,
                                    swaps_fetch_page_limit = fetch_page_limit,
                                    swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                                    swaps_fetch_limit_reached = fetch_progress.saturated,
                                    swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                                    swaps_fetch_time_budget_exhausted =
                                        fetch_progress.time_budget_exhausted,
                                    metrics_window_start = %metrics_window_start,
                                    scoring_source = summary.scoring_source,
                                    discovery_runtime_mode = summary.runtime_mode.as_str(),
                                    metrics_persisted = false,
                                    snapshot_recomputed = false,
                                    discovery_published = summary.published,
                                    discovery_cycle_duration_ms = elapsed_ms,
                                    "discovery cycle completed"
                                );
                                return Ok(summary);
                            }
                            if let Some(active_wallets) = bootstrap_degraded_follow_wallets.clone()
                            {
                                store.clear_discovery_persisted_rebuild_state()?;
                                let summary = self
                                    .bootstrap_degraded_summary_from_published_universe(
                                        store,
                                        window_start,
                                        active_wallets,
                                        &cap_truncation_telemetry,
                                        empty_window_bootstrap_degraded_scoring_source,
                                        empty_window_bootstrap_degraded_scoring_source,
                                        now,
                                    )?;
                                let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                                warn!(
                                    metrics_window_start = %metrics_window_start,
                                    scoring_source = summary.scoring_source,
                                    bootstrap_degraded_active = true,
                                    "discovery runtime completed persisted fallback with zero rows and remains in explicit bootstrap-degraded mode"
                                );
                                info!(
                                    window_start = %summary.window_start,
                                    wallets_seen = summary.wallets_seen,
                                    eligible_wallets = summary.eligible_wallets,
                                    metrics_written = summary.metrics_written,
                                    follow_promoted = summary.follow_promoted,
                                    follow_demoted = summary.follow_demoted,
                                    active_follow_wallets = summary.active_follow_wallets,
                                    swaps_window,
                                    swaps_query_rows = fetch_progress.query_rows,
                                    swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                                    swaps_delta_fetched = delta_fetched,
                                    swaps_warm_loaded,
                                    swaps_evicted_due_cap,
                                    swaps_fetch_limit = fetch_limit,
                                    swaps_fetch_pages = fetch_progress.pages,
                                    swaps_fetch_page_limit = fetch_page_limit,
                                    swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                                    swaps_fetch_limit_reached = fetch_progress.saturated,
                                    swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                                    swaps_fetch_time_budget_exhausted =
                                        fetch_progress.time_budget_exhausted,
                                    metrics_window_start = %metrics_window_start,
                                    scoring_source = summary.scoring_source,
                                    discovery_runtime_mode = summary.runtime_mode.as_str(),
                                    metrics_persisted = false,
                                    snapshot_recomputed = false,
                                    discovery_published = summary.published,
                                    discovery_cycle_duration_ms = elapsed_ms,
                                    "discovery cycle completed"
                                );
                                return Ok(summary);
                            }
                            store.clear_discovery_persisted_rebuild_state()?;
                            let summary = self.fail_close_without_recent_universe(
                                store,
                                window_start,
                                metrics_window_start,
                                publish_due,
                                true,
                                &cap_truncation_telemetry,
                                empty_window_unusable_scoring_source,
                                empty_window_unusable_scoring_source,
                                now,
                            )?;
                            let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                            warn!(
                                metrics_window_start = %metrics_window_start,
                                scoring_source = summary.scoring_source,
                                cleared_follow_wallets = summary.follow_demoted,
                                "discovery fail-closed because persisted observed_swaps runtime fallback completed with zero rows and no recent published universe exists"
                            );
                            info!(
                                window_start = %summary.window_start,
                                wallets_seen = summary.wallets_seen,
                                eligible_wallets = summary.eligible_wallets,
                                metrics_written = summary.metrics_written,
                                follow_promoted = summary.follow_promoted,
                                follow_demoted = summary.follow_demoted,
                                active_follow_wallets = summary.active_follow_wallets,
                                swaps_window,
                                swaps_query_rows = fetch_progress.query_rows,
                                swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                                swaps_delta_fetched = delta_fetched,
                                swaps_warm_loaded,
                                swaps_evicted_due_cap,
                                swaps_fetch_limit = fetch_limit,
                                swaps_fetch_pages = fetch_progress.pages,
                                swaps_fetch_page_limit = fetch_page_limit,
                                swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                                swaps_fetch_limit_reached = fetch_progress.saturated,
                                swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                                swaps_fetch_time_budget_exhausted = fetch_progress.time_budget_exhausted,
                                metrics_window_start = %metrics_window_start,
                                scoring_source = summary.scoring_source,
                                discovery_runtime_mode = summary.runtime_mode.as_str(),
                                metrics_persisted = false,
                                snapshot_recomputed = false,
                                discovery_published = summary.published,
                                discovery_cycle_duration_ms = elapsed_ms,
                                "discovery cycle completed"
                            );
                            return Ok(summary);
                        }
                        (
                            true,
                            false,
                            false,
                            false,
                            snapshots,
                            telemetry.observed_swaps_loaded,
                            scoring_source,
                            telemetry.window_start,
                            telemetry.metrics_window_start,
                        )
                    }
                    PersistedStreamRebuildAdvanceOutcome::InProgress { telemetry } => {
                        let catch_up_requested =
                            should_request_persisted_stream_catch_up(&telemetry);
                        if let Some(active_wallets) = recent_published_follow_wallets.clone() {
                            let summary = self
                                .degraded_summary_from_published_universe(
                                    store,
                                    window_start,
                                    metrics_window_start,
                                    false,
                                    active_wallets,
                                    &cap_truncation_telemetry,
                                    empty_window_degraded_scoring_source,
                                    empty_window_degraded_scoring_source,
                                    now,
                                )?
                                .with_persisted_stream_catch_up_requested(false);
                            let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                            info!(
                                window_start = %summary.window_start,
                                wallets_seen = summary.wallets_seen,
                                eligible_wallets = summary.eligible_wallets,
                                metrics_written = summary.metrics_written,
                                follow_promoted = summary.follow_promoted,
                                follow_demoted = summary.follow_demoted,
                                active_follow_wallets = summary.active_follow_wallets,
                                swaps_window,
                                swaps_query_rows = fetch_progress.query_rows,
                                swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                                swaps_delta_fetched = delta_fetched,
                                swaps_warm_loaded,
                                swaps_evicted_due_cap,
                                swaps_fetch_limit = fetch_limit,
                                swaps_fetch_pages = fetch_progress.pages,
                                swaps_fetch_page_limit = fetch_page_limit,
                                swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                                swaps_fetch_limit_reached = fetch_progress.saturated,
                                swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                                swaps_fetch_time_budget_exhausted =
                                    fetch_progress.time_budget_exhausted,
                                metrics_window_start = %metrics_window_start,
                                scoring_source = summary.scoring_source,
                                discovery_runtime_mode = summary.runtime_mode.as_str(),
                                metrics_persisted = false,
                                snapshot_recomputed = false,
                                discovery_published = summary.published,
                                discovery_cycle_duration_ms = elapsed_ms,
                                rebuild_phase = telemetry.phase.as_str(),
                                rebuild_horizon_end = %telemetry.horizon_end,
                                discovery_persisted_stream_catch_up_requested =
                                    summary.persisted_stream_catch_up_requested,
                                rebuild_budget_exhausted_reason = telemetry
                                    .budget_exhausted_reason
                                    .map(PersistedStreamBudgetExhaustedReason::as_str),
                                "discovery cycle completed"
                            );
                            return Ok(summary);
                        }
                        if let Some(active_wallets) = bootstrap_degraded_follow_wallets.clone() {
                            let summary = self
                                .bootstrap_degraded_summary_from_published_universe(
                                    store,
                                    window_start,
                                    active_wallets,
                                    &cap_truncation_telemetry,
                                    empty_window_bootstrap_degraded_scoring_source,
                                    empty_window_bootstrap_degraded_scoring_source,
                                    now,
                                )?
                                .with_persisted_stream_catch_up_requested(catch_up_requested);
                            let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                            warn!(
                                metrics_window_start = %metrics_window_start,
                                scoring_source = summary.scoring_source,
                                bootstrap_degraded_active = true,
                                rebuild_phase = telemetry.phase.as_str(),
                                rebuild_horizon_end = %telemetry.horizon_end,
                                rebuild_budget_exhausted_reason = telemetry
                                    .budget_exhausted_reason
                                    .map(PersistedStreamBudgetExhaustedReason::as_str),
                                "discovery runtime remains in explicit bootstrap-degraded mode while bounded persisted rebuild continues"
                            );
                            info!(
                                window_start = %summary.window_start,
                                wallets_seen = summary.wallets_seen,
                                eligible_wallets = summary.eligible_wallets,
                                metrics_written = summary.metrics_written,
                                follow_promoted = summary.follow_promoted,
                                follow_demoted = summary.follow_demoted,
                                active_follow_wallets = summary.active_follow_wallets,
                                swaps_window,
                                swaps_query_rows = fetch_progress.query_rows,
                                swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                                swaps_delta_fetched = delta_fetched,
                                swaps_warm_loaded,
                                swaps_evicted_due_cap,
                                swaps_fetch_limit = fetch_limit,
                                swaps_fetch_pages = fetch_progress.pages,
                                swaps_fetch_page_limit = fetch_page_limit,
                                swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                                swaps_fetch_limit_reached = fetch_progress.saturated,
                                swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                                swaps_fetch_time_budget_exhausted =
                                    fetch_progress.time_budget_exhausted,
                                metrics_window_start = %metrics_window_start,
                                scoring_source = summary.scoring_source,
                                discovery_runtime_mode = summary.runtime_mode.as_str(),
                                metrics_persisted = false,
                                snapshot_recomputed = false,
                                discovery_published = summary.published,
                                discovery_cycle_duration_ms = elapsed_ms,
                                rebuild_phase = telemetry.phase.as_str(),
                                rebuild_horizon_end = %telemetry.horizon_end,
                                discovery_persisted_stream_catch_up_requested =
                                    summary.persisted_stream_catch_up_requested,
                                rebuild_budget_exhausted_reason = telemetry
                                    .budget_exhausted_reason
                                    .map(PersistedStreamBudgetExhaustedReason::as_str),
                                "discovery cycle completed"
                            );
                            return Ok(summary);
                        }
                        let summary = self
                            .fail_close_without_recent_universe(
                                store,
                                window_start,
                                metrics_window_start,
                                false,
                                true,
                                &cap_truncation_telemetry,
                                empty_window_unusable_scoring_source,
                                empty_window_unusable_scoring_source,
                                now,
                            )?
                            .with_persisted_stream_catch_up_requested(catch_up_requested);
                        let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                        warn!(
                            metrics_window_start = %metrics_window_start,
                            scoring_source = summary.scoring_source,
                            cleared_follow_wallets = summary.follow_demoted,
                            rebuild_phase = telemetry.phase.as_str(),
                            rebuild_horizon_end = %telemetry.horizon_end,
                            rebuild_budget_exhausted_reason = telemetry
                                .budget_exhausted_reason
                                .map(PersistedStreamBudgetExhaustedReason::as_str),
                            "discovery fail-closed while bounded persisted observed_swaps rebuild continues without a recent published universe"
                        );
                        info!(
                            window_start = %summary.window_start,
                            wallets_seen = summary.wallets_seen,
                            eligible_wallets = summary.eligible_wallets,
                            metrics_written = summary.metrics_written,
                            follow_promoted = summary.follow_promoted,
                            follow_demoted = summary.follow_demoted,
                            active_follow_wallets = summary.active_follow_wallets,
                            swaps_window,
                            swaps_query_rows = fetch_progress.query_rows,
                            swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                            swaps_delta_fetched = delta_fetched,
                            swaps_warm_loaded,
                            swaps_evicted_due_cap,
                            swaps_fetch_limit = fetch_limit,
                            swaps_fetch_pages = fetch_progress.pages,
                            swaps_fetch_page_limit = fetch_page_limit,
                            swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                            swaps_fetch_limit_reached = fetch_progress.saturated,
                            swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                            swaps_fetch_time_budget_exhausted = fetch_progress.time_budget_exhausted,
                            metrics_window_start = %metrics_window_start,
                            scoring_source = summary.scoring_source,
                            discovery_runtime_mode = summary.runtime_mode.as_str(),
                            metrics_persisted = false,
                            snapshot_recomputed = false,
                            discovery_published = summary.published,
                            discovery_cycle_duration_ms = elapsed_ms,
                            rebuild_phase = telemetry.phase.as_str(),
                            rebuild_horizon_end = %telemetry.horizon_end,
                            discovery_persisted_stream_catch_up_requested =
                                summary.persisted_stream_catch_up_requested,
                            rebuild_budget_exhausted_reason = telemetry
                                .budget_exhausted_reason
                                .map(PersistedStreamBudgetExhaustedReason::as_str),
                            "discovery cycle completed"
                        );
                        return Ok(summary);
                    }
                }
            }
        };
        if scoring_source != "raw_window_persisted_stream" {
            store.clear_discovery_persisted_rebuild_state()?;
        }
        let mut wallet_rows: Vec<WalletUpsertRow> = Vec::with_capacity(snapshots.len());
        let mut metric_rows: Vec<WalletMetricRow> = Vec::with_capacity(snapshots.len());
        for snapshot in snapshots.iter() {
            let status = if snapshot.eligible {
                "candidate"
            } else {
                "observed"
            };
            wallet_rows.push(WalletUpsertRow {
                wallet_id: snapshot.wallet_id.clone(),
                first_seen: snapshot.first_seen,
                last_seen: snapshot.last_seen,
                status: status.to_string(),
            });
            metric_rows.push(WalletMetricRow {
                wallet_id: snapshot.wallet_id.clone(),
                window_start: effective_metrics_window_start,
                pnl: snapshot.pnl_sol,
                win_rate: snapshot.win_rate,
                trades: snapshot.trades,
                closed_trades: snapshot.closed_trades,
                hold_median_seconds: snapshot.hold_median_seconds,
                score: snapshot.score,
                buy_total: snapshot.buy_total,
                tradable_ratio: snapshot.tradable_ratio,
                rug_ratio: snapshot.rug_ratio,
            });
        }

        let should_persist_metrics =
            !store.wallet_metrics_window_exists(effective_metrics_window_start)?;
        let metrics_to_persist = if should_persist_metrics && !metrics_persistence_suppressed {
            metric_rows.as_slice()
        } else {
            &[]
        };
        let snapshot_write = (!metrics_to_persist.is_empty()).then(|| {
            trusted_snapshot_write(
                TrustedSnapshotSourceKind::DiscoveryRefresh,
                TrustedSelectionState::TrustedCurrent,
                effective_metrics_window_start,
                now,
                metrics_to_persist.len(),
                None,
                Some(effective_metrics_window_start),
            )
        });

        let ranked = rank_follow_candidates(&snapshots, self.config.min_score);
        let desired_wallets = desired_wallets(&ranked, self.config.follow_top_n);
        let current_raw_for_capture = PrecomputedWalletFreshnessCurrentRawTruth {
            window_start: effective_window_start,
            observed_swaps_loaded: observed_swaps_loaded_for_capture,
            eligible_wallet_count: ranked.len(),
            top_wallet_ids: desired_wallets.clone(),
        };
        let follow_delta = store.persist_discovery_cycle_with_snapshot_metadata(
            &wallet_rows,
            metrics_to_persist,
            &desired_wallets,
            publish_due && !followlist_activations_suppressed,
            publish_due && !followlist_deactivations_suppressed,
            now,
            "discovery_score_refresh",
            snapshot_write.as_ref(),
        )?;
        let active_follow_wallets = store.list_active_follow_wallets()?.len();
        let top_wallets = top_wallet_labels(&ranked, 5);

        let summary = DiscoverySummary {
            window_start: effective_window_start,
            wallets_seen: snapshots.len(),
            eligible_wallets: ranked.len(),
            metrics_written: metrics_to_persist.len(),
            follow_promoted: follow_delta.activated,
            follow_demoted: follow_delta.deactivated,
            active_follow_wallets,
            top_wallets,
            published: publish_due,
            ..DiscoverySummary::default()
        }
        .with_runtime_mode(DiscoveryRuntimeMode::Healthy)
        .with_scoring_source(scoring_source)
        .with_cap_truncation_telemetry(&cap_truncation_telemetry);
        if publish_due {
            self.record_live_publish(now);
        }
        self.persist_publication_state(
            store,
            DiscoveryRuntimeMode::Healthy,
            publish_due,
            effective_metrics_window_start,
            Some(&desired_wallets),
            scoring_source,
            "discovery_score_refresh",
            now,
        )?;
        let capture_telemetry = self.maybe_persist_in_band_wallet_freshness_capture(
            store,
            now,
            publish_due,
            DiscoveryRuntimeMode::Healthy,
            Some(current_raw_for_capture.clone()),
        );
        let summary = summary.with_wallet_freshness_capture(&capture_telemetry);
        {
            let mut state = match self.window_state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    warn!("discovery window mutex poisoned while caching summary; continuing");
                    poisoned.into_inner()
                }
            };
            state.last_snapshot_bucket = Some(effective_metrics_window_start);
            state.last_summary = Some(summary.clone());
            state.last_exact_current_raw_truth = Some(CachedCurrentRawTruthSample {
                window_start: current_raw_for_capture.window_start,
                observed_swaps_loaded: current_raw_for_capture.observed_swaps_loaded,
                eligible_wallet_count: current_raw_for_capture.eligible_wallet_count,
                top_wallet_ids: current_raw_for_capture.top_wallet_ids.clone(),
            });
        }
        if let Some(snapshot_write) = snapshot_write.as_ref() {
            self.persist_trusted_selection_state_from_snapshot(
                store,
                snapshot_write,
                false,
                "discovery_score_refresh",
                now,
            )?;
        } else if let Some(metadata) = store
            .trusted_wallet_metrics_snapshot_metadata_for_window(effective_metrics_window_start)?
        {
            self.persist_trusted_selection_state(
                store,
                self.effective_trusted_snapshot_state(&metadata, now),
                Some(metadata.snapshot_id),
                Some(metadata.effective_window_start),
                Some(metadata.source_kind),
                false,
                "discovery_score_refresh",
                now,
            )?;
        } else {
            self.persist_trusted_selection_state(
                store,
                TrustedSelectionState::TrustedCurrent,
                None,
                Some(effective_metrics_window_start),
                Some(TrustedSnapshotSourceKind::Legacy),
                false,
                "discovery_score_refresh_legacy",
                now,
            )?;
        }
        store.set_discovery_bootstrap_degraded_state(false, None, None)?;
        if scoring_source == "raw_window_persisted_stream" {
            store.clear_discovery_persisted_rebuild_state()?;
        }
        let elapsed_ms = cycle_started.elapsed().as_millis() as u64;

        info!(
            window_start = %summary.window_start,
            wallets_seen = summary.wallets_seen,
            eligible_wallets = summary.eligible_wallets,
            metrics_written = summary.metrics_written,
            follow_promoted = summary.follow_promoted,
            follow_demoted = summary.follow_demoted,
            active_follow_wallets = summary.active_follow_wallets,
            swaps_window,
            swaps_query_rows = fetch_progress.query_rows,
            swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
            swaps_delta_fetched = delta_fetched,
            swaps_warm_loaded,
            swaps_evicted_due_cap,
            swaps_fetch_limit = fetch_limit,
            swaps_fetch_pages = fetch_progress.pages,
            swaps_fetch_page_limit = fetch_page_limit,
            swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
            swaps_fetch_limit_reached = fetch_progress.saturated,
            swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
            swaps_fetch_time_budget_exhausted = fetch_progress.time_budget_exhausted,
            metrics_window_start = %effective_metrics_window_start,
            scoring_source,
            metrics_persisted = should_persist_metrics,
            snapshot_recomputed = true,
            discovery_published = summary.published,
            followlist_activations_suppressed,
            followlist_deactivations_suppressed,
            discovery_cycle_duration_ms = elapsed_ms,
            wallet_freshness_capture_state = summary.wallet_freshness_capture_state,
            wallet_freshness_capture_reason = summary.wallet_freshness_capture_reason.as_deref(),
            wallet_freshness_capture_id = summary.wallet_freshness_capture_id,
            wallet_freshness_capture_captured_at = ?summary.wallet_freshness_capture_captured_at,
            top_wallets = ?summary.top_wallets,
            "discovery cycle completed"
        );

        if fetch_progress.saturated {
            warn!(
                swaps_query_rows = fetch_progress.query_rows,
                swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                swaps_fetch_limit = fetch_limit,
                swaps_fetch_pages = fetch_progress.pages,
                swaps_fetch_page_limit = fetch_page_limit,
                swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                swaps_fetch_time_budget_exhausted = fetch_progress.time_budget_exhausted,
                "discovery swap fetch exhausted bounded per-cycle budget; backlog processing continues next cycle"
            );
        }

        Ok(summary)
    }

    fn metrics_window_start(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        let interval_seconds = self.config.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        bucketed_now - Duration::days(self.config.scoring_window_days.max(1) as i64)
    }

    fn record_live_publish(&self, now: DateTime<Utc>) {
        let mut state = match self.window_state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("discovery window mutex poisoned while recording publish time; continuing");
                poisoned.into_inner()
            }
        };
        state.last_publish_at = Some(now);
    }

    fn effective_trusted_snapshot_state(
        &self,
        metadata: &TrustedWalletMetricsSnapshotRow,
        now: DateTime<Utc>,
    ) -> TrustedSelectionState {
        match metadata.trust_state {
            TrustedSelectionState::TrustedBridgedStale | TrustedSelectionState::Invalid => {
                metadata.trust_state
            }
            TrustedSelectionState::TrustedBridged
                if metadata.source_kind == TrustedSnapshotSourceKind::CloneLatestBridge =>
            {
                let Some(source_window_start) = metadata.source_window_start else {
                    return metadata.trust_state;
                };
                let source_snapshot_age_seconds = snapshot_age_seconds_since_publish(
                    now,
                    source_window_start,
                    self.config.scoring_window_days as i64,
                );
                if source_snapshot_age_seconds > self.config.max_bootstrap_snapshot_age_seconds {
                    TrustedSelectionState::TrustedBridgedStale
                } else {
                    TrustedSelectionState::TrustedBridged
                }
            }
            _ => metadata.trust_state,
        }
    }

    pub fn effective_startup_trusted_selection_state(
        &self,
        status: &StartupTrustedSelectionGateStatus,
        now: DateTime<Utc>,
    ) -> Option<TrustedSelectionState> {
        status.effective_selection_state(
            now,
            self.config.scoring_window_days as i64,
            self.config.metric_snapshot_interval_seconds,
            self.config.max_bootstrap_snapshot_age_seconds,
        )
    }

    pub fn effective_startup_trusted_selection_fail_closed(
        &self,
        status: &StartupTrustedSelectionGateStatus,
        now: DateTime<Utc>,
    ) -> bool {
        status.effective_startup_fail_closed(
            now,
            self.config.scoring_window_days as i64,
            self.config.metric_snapshot_interval_seconds,
            self.config.max_bootstrap_snapshot_age_seconds,
        )
    }

    fn wallet_snapshots_from_persisted_metric_rows(
        &self,
        now: DateTime<Utc>,
        rows: Vec<PersistedWalletMetricSnapshotRow>,
    ) -> Vec<WalletSnapshot> {
        let decay_cutoff = now - Duration::days(self.config.decay_window_days.max(1) as i64);
        rows.into_iter()
            .map(|row| self.snapshot_from_persisted_metrics(row, decay_cutoff))
            .collect()
    }

    fn persist_trusted_selection_state(
        &self,
        store: &SqliteStore,
        selection_state: TrustedSelectionState,
        active_snapshot_id: Option<String>,
        active_snapshot_window_start: Option<DateTime<Utc>>,
        source_kind: Option<TrustedSnapshotSourceKind>,
        bootstrap_required: bool,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        store.set_discovery_trusted_selection_state(&DiscoveryTrustedSelectionStateUpdate {
            bootstrap_required,
            reason: reason.to_string(),
            selection_state,
            active_snapshot_id,
            active_snapshot_window_start,
            last_bootstrap_source_kind: source_kind,
            last_bootstrap_at: Some(now),
        })
    }

    fn persist_trusted_selection_state_from_snapshot(
        &self,
        store: &SqliteStore,
        snapshot_write: &TrustedWalletMetricsSnapshotWrite,
        bootstrap_required: bool,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        self.persist_trusted_selection_state(
            store,
            snapshot_write.trust_state,
            Some(snapshot_write.snapshot_id.clone()),
            Some(snapshot_write.effective_window_start),
            Some(snapshot_write.source_kind),
            bootstrap_required,
            reason,
            now,
        )
    }

    pub fn materialize_trusted_bootstrap_wallet_metrics(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<TrustedBootstrapWalletMetricsMaterializationSummary> {
        let metrics_window_start = self.metrics_window_start(now);
        let Some(oldest_persisted_observed_swap_ts) = store.oldest_observed_swap_timestamp()?
        else {
            return Err(anyhow!(
                "no persisted observed_swaps available to materialize a trusted bootstrap bucket"
            ));
        };
        if oldest_persisted_observed_swap_ts > metrics_window_start {
            return Err(anyhow!(
                "persisted observed_swaps history is incomplete for trusted bootstrap: oldest_retained_ts={} metrics_window_start={}",
                oldest_persisted_observed_swap_ts.to_rfc3339(),
                metrics_window_start.to_rfc3339()
            ));
        }
        let (snapshots, observed_swaps_loaded) = self
            .build_wallet_snapshots_from_persisted_stream_one_shot(
                store,
                metrics_window_start,
                now,
            )?;
        if observed_swaps_loaded == 0 {
            return Err(anyhow!(
                "no persisted observed_swaps found in bootstrap window starting at {}",
                metrics_window_start.to_rfc3339()
            ));
        }
        let wallet_rows = self.wallet_rows_from_snapshots(&snapshots);
        let metric_rows: Vec<WalletMetricRow> = snapshots
            .iter()
            .map(|snapshot| WalletMetricRow {
                wallet_id: snapshot.wallet_id.clone(),
                window_start: metrics_window_start,
                pnl: snapshot.pnl_sol,
                win_rate: snapshot.win_rate,
                trades: snapshot.trades,
                closed_trades: snapshot.closed_trades,
                hold_median_seconds: snapshot.hold_median_seconds,
                score: snapshot.score,
                buy_total: snapshot.buy_total,
                tradable_ratio: snapshot.tradable_ratio,
                rug_ratio: snapshot.rug_ratio,
            })
            .collect();
        let ranked = rank_follow_candidates(&snapshots, self.config.min_score);
        let bucket_already_exists = store.wallet_metrics_window_exists(metrics_window_start)?;
        let metrics_to_persist = if bucket_already_exists {
            &[]
        } else {
            metric_rows.as_slice()
        };
        let snapshot_write = (!metrics_to_persist.is_empty()).then(|| {
            trusted_snapshot_write(
                TrustedSnapshotSourceKind::AdminMaterialization,
                TrustedSelectionState::TrustedCurrent,
                metrics_window_start,
                now,
                metrics_to_persist.len(),
                None,
                Some(metrics_window_start),
            )
        });
        let follow_delta = store.persist_discovery_cycle_with_snapshot_metadata(
            &wallet_rows,
            metrics_to_persist,
            &[],
            false,
            false,
            now,
            "admin_trusted_wallet_metrics_bootstrap_materialize",
            snapshot_write.as_ref(),
        )?;
        if follow_delta.activated > 0 || follow_delta.deactivated > 0 {
            return Err(anyhow!(
                "trusted wallet_metrics bootstrap materialization unexpectedly mutated followlist (activated={}, deactivated={})",
                follow_delta.activated,
                follow_delta.deactivated
            ));
        }
        if let Some(snapshot_write) = snapshot_write.as_ref() {
            self.persist_trusted_selection_state_from_snapshot(
                store,
                snapshot_write,
                store.discovery_trusted_selection_bootstrap_required()?,
                "admin_trusted_wallet_metrics_bootstrap_materialize",
                now,
            )?;
        }

        Ok(TrustedBootstrapWalletMetricsMaterializationSummary {
            metrics_window_start,
            observed_swaps_loaded,
            wallets_seen: snapshots.len(),
            eligible_wallets: ranked.len(),
            metrics_written: metrics_to_persist.len(),
            bucket_already_exists,
            top_wallets: top_wallet_labels(&ranked, 5),
        })
    }

    pub fn clone_latest_trusted_bootstrap_wallet_metrics(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        dry_run: bool,
        force_stale: bool,
    ) -> Result<CloneLatestTrustedBootstrapSummary> {
        let target_metrics_window_start = self.metrics_window_start(now);
        let source_metrics_window_start = store
            .latest_wallet_metrics_window_start()?
            .ok_or_else(|| anyhow!("no persisted wallet_metrics snapshot available to clone"))?;
        let source_snapshot_metadata = store
            .trusted_wallet_metrics_snapshot_metadata_for_window(source_metrics_window_start)?;
        let source_rows = store.wallet_metrics_row_count_for_window(source_metrics_window_start)?;
        if source_rows == 0 {
            return Err(anyhow!(
                "latest persisted wallet_metrics snapshot {} contains zero rows",
                source_metrics_window_start.to_rfc3339()
            ));
        }
        if store.wallet_metrics_window_exists(target_metrics_window_start)? {
            return Err(anyhow!(
                "target wallet_metrics bootstrap bucket already exists at {}",
                target_metrics_window_start.to_rfc3339()
            ));
        }

        let source_snapshot_age_seconds = snapshot_age_seconds_since_publish(
            now,
            source_metrics_window_start,
            self.config.scoring_window_days as i64,
        );
        let stale_source =
            source_snapshot_age_seconds > self.config.max_bootstrap_snapshot_age_seconds;
        if stale_source && !force_stale {
            return Err(anyhow!(
                "latest persisted wallet_metrics snapshot is stale for clone-latest bootstrap: source_window_start={} source_snapshot_age_seconds={} max_bootstrap_snapshot_age_seconds={} (pass force_stale=true to override)",
                source_metrics_window_start.to_rfc3339(),
                source_snapshot_age_seconds,
                self.config.max_bootstrap_snapshot_age_seconds
            ));
        }
        if let Some(metadata) = source_snapshot_metadata.as_ref() {
            match self.effective_trusted_snapshot_state(metadata, now) {
                TrustedSelectionState::Invalid => {
                    return Err(anyhow!(
                        "latest persisted wallet_metrics snapshot is marked invalid and cannot be cloned: source_window_start={}",
                        source_metrics_window_start.to_rfc3339()
                    ));
                }
                TrustedSelectionState::TrustedBridgedStale if !force_stale => {
                    return Err(anyhow!(
                        "latest persisted wallet_metrics snapshot is already trusted_bridged_stale and requires force_stale=true to clone: source_window_start={}",
                        source_metrics_window_start.to_rfc3339()
                    ));
                }
                _ => {}
            }
        }

        let source_snapshots = self.wallet_snapshots_from_persisted_metric_rows(
            now,
            store.load_wallet_metric_snapshots_for_window(source_metrics_window_start)?,
        );
        let ranked = rank_follow_candidates(&source_snapshots, self.config.min_score);
        let snapshot_write = trusted_snapshot_write(
            TrustedSnapshotSourceKind::CloneLatestBridge,
            if stale_source {
                TrustedSelectionState::TrustedBridgedStale
            } else {
                TrustedSelectionState::TrustedBridged
            },
            target_metrics_window_start,
            now,
            source_rows,
            source_snapshot_metadata
                .as_ref()
                .map(|metadata| metadata.snapshot_id.clone()),
            Some(source_metrics_window_start),
        );

        if dry_run {
            return Ok(CloneLatestTrustedBootstrapSummary {
                source_metrics_window_start,
                target_metrics_window_start,
                source_snapshot_age_seconds,
                source_rows,
                inserted_rows: 0,
                stale_source,
                forced_stale: force_stale,
                dry_run: true,
                top_wallets: top_wallet_labels(&ranked, 5),
            });
        }

        let active_follow_wallets_before = store.list_active_follow_wallets()?.len();
        let inserted_rows = store.clone_wallet_metrics_window_with_metadata(
            source_metrics_window_start,
            target_metrics_window_start,
            source_rows,
            Some(&snapshot_write),
        )?;
        if inserted_rows != source_rows {
            return Err(anyhow!(
                "clone-latest bootstrap wrote unexpected row count: source_rows={} inserted_rows={}",
                source_rows,
                inserted_rows
            ));
        }
        let active_follow_wallets_after = store.list_active_follow_wallets()?.len();
        if active_follow_wallets_before != active_follow_wallets_after {
            return Err(anyhow!(
                "clone-latest bootstrap unexpectedly mutated followlist (before={} after={})",
                active_follow_wallets_before,
                active_follow_wallets_after
            ));
        }
        self.persist_trusted_selection_state_from_snapshot(
            store,
            &snapshot_write,
            store.discovery_trusted_selection_bootstrap_required()?,
            "admin_clone_latest_wallet_metrics_bootstrap",
            now,
        )?;

        Ok(CloneLatestTrustedBootstrapSummary {
            source_metrics_window_start,
            target_metrics_window_start,
            source_snapshot_age_seconds,
            source_rows,
            inserted_rows,
            stale_source,
            forced_stale: force_stale,
            dry_run: false,
            top_wallets: top_wallet_labels(&ranked, 5),
        })
    }

    fn build_wallet_snapshots_from_cached(
        &self,
        store: &SqliteStore,
        swaps: &VecDeque<SwapEvent>,
        now: DateTime<Utc>,
    ) -> Result<Vec<WalletSnapshot>> {
        let mut ordered_swaps: Vec<&SwapEvent> = swaps.iter().collect();
        if ordered_swaps
            .windows(2)
            .any(|pair| cmp_swap_order(pair[1], pair[0]) == Ordering::Less)
        {
            ordered_swaps.sort_by(|a, b| cmp_swap_order(a, b));
            warn!(
                swaps_window = swaps.len(),
                "discovery swap order invariant violated before snapshot rebuild; normalizing cached window"
            );
        }
        let mut by_wallet: HashMap<String, WalletAccumulator> = HashMap::new();
        let mut seen_buy_mints = HashSet::new();
        let mut unique_buy_mints = Vec::new();
        for swap in ordered_swaps
            .iter()
            .copied()
            .filter(|swap| is_sol_buy(swap))
        {
            if seen_buy_mints.insert(swap.token_out.clone()) {
                unique_buy_mints.push(swap.token_out.clone());
            }
        }
        let token_quality_cache =
            self.resolve_token_quality_for_mints(store, &unique_buy_mints, now)?;
        let mut token_states: HashMap<String, TokenRollingState> = HashMap::new();
        let mut token_sol_history: HashMap<String, Vec<SolLegTrade>> = HashMap::new();
        for swap in ordered_swaps.iter().copied() {
            let buy_quality = self.update_token_quality_state(
                &mut token_states,
                &mut token_sol_history,
                &token_quality_cache,
                swap,
            );
            let entry = by_wallet.entry(swap.wallet.clone()).or_default();
            entry.observe_swap(swap, self.config.max_tx_per_minute, buy_quality);
        }

        self.wallet_snapshots_from_accumulators(store, by_wallet, now, &token_sol_history)
    }

    fn build_wallet_snapshots_from_persisted_stream_one_shot(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(Vec<WalletSnapshot>, usize)> {
        let unique_buy_mints = store.load_observed_buy_mints_in_window(window_start, now)?;
        let token_quality_cache =
            self.resolve_token_quality_for_mints(store, &unique_buy_mints, now)?;
        let lookahead = Duration::seconds(self.config.rug_lookahead_seconds.max(1) as i64);
        let mut by_wallet: HashMap<String, WalletAccumulator> = HashMap::new();
        let mut token_states: HashMap<String, TokenRollingState> = HashMap::new();
        let mut token_recent_sol_trades: HashMap<String, VecDeque<SolLegTrade>> = HashMap::new();
        let mut pending_rug_checks = VecDeque::new();
        let mut token_pending_buy_starts: HashMap<String, VecDeque<DateTime<Utc>>> = HashMap::new();
        let mut processed_swaps = 0usize;
        let observed_swaps_loaded =
            store.for_each_observed_swap_in_window(window_start, now, |swap| {
                processed_swaps = processed_swaps.saturating_add(1);
                let buy_quality = self.update_token_quality_state_streaming(
                    &mut token_states,
                    &mut token_recent_sol_trades,
                    &token_quality_cache,
                    &swap,
                );
                let entry = by_wallet.entry(swap.wallet.clone()).or_default();
                entry.observe_swap_streaming(&swap, self.config.max_tx_per_minute, buy_quality);

                let Some(token) = sol_leg_token(&swap) else {
                    return Ok(());
                };
                self.finalize_streaming_rug_metrics_up_to(
                    &mut by_wallet,
                    token,
                    &mut token_recent_sol_trades,
                    &mut pending_rug_checks,
                    &mut token_pending_buy_starts,
                    swap.ts_utc,
                    lookahead,
                    now,
                );
                if is_sol_buy(&swap) {
                    pending_rug_checks.push_back(PendingBuyRugCheck {
                        token: token.to_string(),
                        wallet_id: swap.wallet.clone(),
                        buy_ts: swap.ts_utc,
                    });
                    token_pending_buy_starts
                        .entry(token.to_string())
                        .or_default()
                        .push_back(swap.ts_utc);
                }
                if processed_swaps % STREAMING_RUG_TRADE_SWEEP_INTERVAL_SWAPS == 0 {
                    self.evict_idle_streaming_rug_trade_history(
                        &mut token_recent_sol_trades,
                        &token_pending_buy_starts,
                        swap.ts_utc - lookahead,
                    );
                }
                Ok(())
            })?;
        self.finalize_all_streaming_rug_metrics(
            &mut by_wallet,
            &mut token_recent_sol_trades,
            &mut pending_rug_checks,
            &mut token_pending_buy_starts,
            now,
            lookahead,
        );
        let empty_token_sol_history = HashMap::new();
        let snapshots = self.wallet_snapshots_from_accumulators(
            store,
            by_wallet,
            now,
            &empty_token_sol_history,
        )?;
        Ok((snapshots, observed_swaps_loaded))
    }

    fn wallet_snapshots_from_accumulators(
        &self,
        store: &SqliteStore,
        by_wallet: HashMap<String, WalletAccumulator>,
        now: DateTime<Utc>,
        token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
    ) -> Result<Vec<WalletSnapshot>> {
        let wallet_ids: Vec<String> = by_wallet.keys().cloned().collect();
        let persisted_active_day_counts = match store.wallet_active_day_counts_since(
            &wallet_ids,
            now - Duration::days(self.config.scoring_window_days.max(1) as i64),
        ) {
            Ok(counts) => counts,
            Err(error) => {
                if discovery_wallet_activity_day_count_error_requires_abort(&error) {
                    return Err(error).context(
                        "failed loading persisted wallet activity-day counts with fatal sqlite I/O",
                    );
                }
                warn!(
                    error = %error,
                    wallet_count = wallet_ids.len(),
                    "failed loading persisted wallet activity-day counts; falling back to cached discovery window"
                );
                HashMap::new()
            }
        };

        Ok(by_wallet
            .into_iter()
            .map(|(wallet_id, acc)| {
                let persisted_active_days = persisted_active_day_counts
                    .get(&wallet_id)
                    .copied()
                    .unwrap_or(0);
                self.snapshot_from_accumulator_with_persisted_active_days(
                    wallet_id,
                    acc,
                    now,
                    &token_sol_history,
                    persisted_active_days,
                )
            })
            .collect())
    }

    fn finalize_streaming_rug_metrics_up_to(
        &self,
        by_wallet: &mut HashMap<String, WalletAccumulator>,
        _token: &str,
        token_recent_sol_trades: &mut HashMap<String, VecDeque<SolLegTrade>>,
        pending_rug_checks: &mut VecDeque<PendingBuyRugCheck>,
        token_pending_buy_starts: &mut HashMap<String, VecDeque<DateTime<Utc>>>,
        up_to_ts: DateTime<Utc>,
        lookahead: Duration,
        now: DateTime<Utc>,
    ) {
        while pending_rug_checks
            .front()
            .map(|buy| buy.buy_ts + lookahead <= up_to_ts)
            .unwrap_or(false)
        {
            let pending = pending_rug_checks
                .pop_front()
                .expect("checked pending buy exists above");
            let rug_status = self.compute_streaming_buy_rug_status(
                token_recent_sol_trades
                    .get(&pending.token)
                    .expect("recent rug trades are initialized before pending buys are recorded"),
                pending.buy_ts,
                lookahead,
                now,
            );
            if let Some(wallet) = by_wallet.get_mut(&pending.wallet_id) {
                wallet.note_streaming_buy_rug_status(rug_status);
            }
            let Some(pending_starts) = token_pending_buy_starts.get_mut(&pending.token) else {
                continue;
            };
            let _ = pending_starts.pop_front();
            let next_needed_ts = pending_starts
                .front()
                .copied()
                .unwrap_or(up_to_ts - lookahead);
            self.evict_streaming_rug_trade_history(
                token_recent_sol_trades,
                &pending.token,
                next_needed_ts,
            );
            if pending_starts.is_empty() {
                token_pending_buy_starts.remove(&pending.token);
            }
        }
    }

    fn finalize_all_streaming_rug_metrics(
        &self,
        by_wallet: &mut HashMap<String, WalletAccumulator>,
        token_recent_sol_trades: &mut HashMap<String, VecDeque<SolLegTrade>>,
        pending_rug_checks: &mut VecDeque<PendingBuyRugCheck>,
        token_pending_buy_starts: &mut HashMap<String, VecDeque<DateTime<Utc>>>,
        now: DateTime<Utc>,
        lookahead: Duration,
    ) {
        self.finalize_streaming_rug_metrics_up_to(
            by_wallet,
            "",
            token_recent_sol_trades,
            pending_rug_checks,
            token_pending_buy_starts,
            now,
            lookahead,
            now,
        );
        while let Some(pending) = pending_rug_checks.pop_front() {
            if let Some(wallet) = by_wallet.get_mut(&pending.wallet_id) {
                wallet.note_streaming_buy_rug_status(BuyFactRugStatus::Unevaluated);
            }
        }
        token_pending_buy_starts.clear();
    }

    fn evict_streaming_rug_trade_history(
        &self,
        token_recent_sol_trades: &mut HashMap<String, VecDeque<SolLegTrade>>,
        token: &str,
        min_ts: DateTime<Utc>,
    ) {
        let Some(recent_trades) = token_recent_sol_trades.get_mut(token) else {
            return;
        };
        while recent_trades
            .front()
            .map(|trade| trade.ts < min_ts)
            .unwrap_or(false)
        {
            recent_trades.pop_front();
        }
        if recent_trades.is_empty() {
            token_recent_sol_trades.remove(token);
        }
    }

    fn evict_idle_streaming_rug_trade_history(
        &self,
        token_recent_sol_trades: &mut HashMap<String, VecDeque<SolLegTrade>>,
        token_pending_buy_starts: &HashMap<String, VecDeque<DateTime<Utc>>>,
        min_ts: DateTime<Utc>,
    ) {
        let tokens: Vec<String> = token_recent_sol_trades.keys().cloned().collect();
        for token in tokens {
            if token_pending_buy_starts.contains_key(&token) {
                continue;
            }
            self.evict_streaming_rug_trade_history(token_recent_sol_trades, &token, min_ts);
        }
    }

    fn compute_streaming_buy_rug_status(
        &self,
        recent_trades: &VecDeque<SolLegTrade>,
        buy_ts: DateTime<Utc>,
        lookahead: Duration,
        now: DateTime<Utc>,
    ) -> BuyFactRugStatus {
        let window_end = buy_ts + lookahead;
        if window_end > now {
            return BuyFactRugStatus::Unevaluated;
        }

        let mut volume_sol = 0.0;
        let mut unique_traders = HashSet::new();
        for trade in recent_trades {
            if trade.ts < buy_ts || trade.ts > window_end {
                continue;
            }
            volume_sol += trade.sol_notional;
            unique_traders.insert(trade.wallet_id.as_str());
        }
        let thin_volume = volume_sol + 1e-12 < self.config.thin_market_min_volume_sol;
        let thin_traders =
            unique_traders.len() < self.config.thin_market_min_unique_traders as usize;
        if thin_volume || thin_traders {
            BuyFactRugStatus::Rugged
        } else {
            BuyFactRugStatus::Healthy
        }
    }

    fn wallet_rows_from_snapshots(&self, snapshots: &[WalletSnapshot]) -> Vec<WalletUpsertRow> {
        snapshots
            .iter()
            .map(|snapshot| WalletUpsertRow {
                wallet_id: snapshot.wallet_id.clone(),
                first_seen: snapshot.first_seen,
                last_seen: snapshot.last_seen,
                status: if snapshot.eligible {
                    "candidate".to_string()
                } else {
                    "observed".to_string()
                },
            })
            .collect()
    }

    fn snapshot_from_components(
        &self,
        wallet_id: String,
        first_seen: DateTime<Utc>,
        last_seen: DateTime<Utc>,
        trades: u32,
        active_days: u32,
        spent_sol: f64,
        realized_pnl_sol: f64,
        max_buy_notional_sol: f64,
        wins: u32,
        closed_trades: u32,
        hold_samples_sec: &[i64],
        realized_pnl_by_day: &HashMap<NaiveDate, f64>,
        suspicious: bool,
        buy_total: u32,
        quality_resolved_buys: u32,
        tradable_buys: u32,
        rug_metrics: RugMetrics,
        now: DateTime<Utc>,
    ) -> WalletSnapshot {
        let resolved_buy_ratio = if buy_total > 0 {
            quality_resolved_buys as f64 / buy_total as f64
        } else {
            0.0
        };
        let tradable_ratio = if quality_resolved_buys > 0 {
            (tradable_buys as f64 / quality_resolved_buys as f64) * resolved_buy_ratio.sqrt()
        } else {
            0.0
        };
        let rug_ratio = if buy_total > 0 {
            (rug_metrics.rugged.saturating_add(rug_metrics.unevaluated)) as f64 / buy_total as f64
        } else {
            0.0
        };
        let win_rate = if closed_trades > 0 {
            wins as f64 / closed_trades as f64
        } else {
            0.0
        };
        let hold_median_seconds = median_i64(hold_samples_sec).unwrap_or(0);
        let consistency_ratio = if active_days > 0 {
            let positive_days = realized_pnl_by_day
                .values()
                .filter(|value| **value > 0.0)
                .count() as f64;
            positive_days / active_days as f64
        } else {
            0.0
        };
        let roi = if spent_sol > 1e-9 {
            realized_pnl_sol / spent_sol
        } else {
            0.0
        };
        let win_sample_factor = (closed_trades as f64 / 8.0).min(1.0);
        let hold_quality = hold_time_quality_score(hold_median_seconds);
        let pnl_component = tanh01(realized_pnl_sol / 2.0);
        let roi_component = tanh01(roi * 3.0);
        let win_component = (win_rate * win_sample_factor).clamp(0.0, 1.0);
        let consistency_component = consistency_ratio.clamp(0.0, 1.0);
        let penalty_component = if suspicious { 0.0 } else { 1.0 };
        let base_score = (0.35 * pnl_component)
            + (0.20 * roi_component)
            + (0.15 * win_component)
            + (0.15 * hold_quality)
            + (0.10 * consistency_component)
            + (0.05 * penalty_component);
        let tradable_penalty = tradable_ratio.powf(1.5);
        let rug_checks_disabled = self.config.max_rug_ratio >= 1.0;
        let rug_penalty = if rug_checks_disabled {
            1.0
        } else {
            (1.0 - rug_ratio).clamp(0.0, 1.0).powi(2)
        };
        let raw_score = (base_score * tradable_penalty * rug_penalty).clamp(0.0, 1.0);
        let decay_cutoff = now - Duration::days(self.config.decay_window_days.max(1) as i64);
        let eligible = trades >= self.config.min_trades
            && active_days >= self.config.min_active_days
            && !suspicious
            && max_buy_notional_sol >= self.config.min_leader_notional_sol
            && last_seen >= decay_cutoff
            && buy_total >= self.config.min_buy_count
            && tradable_ratio >= self.config.min_tradable_ratio
            && (rug_checks_disabled || rug_ratio <= self.config.max_rug_ratio);
        let score = if eligible { raw_score } else { 0.0 };

        WalletSnapshot {
            wallet_id,
            first_seen,
            last_seen,
            pnl_sol: realized_pnl_sol,
            win_rate,
            trades,
            closed_trades,
            hold_median_seconds,
            score,
            buy_total,
            tradable_ratio,
            rug_ratio,
            eligible,
        }
    }

    #[cfg(test)]
    fn snapshot_from_accumulator(
        &self,
        wallet_id: String,
        acc: WalletAccumulator,
        now: DateTime<Utc>,
        token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
    ) -> WalletSnapshot {
        self.snapshot_from_accumulator_with_persisted_active_days(
            wallet_id,
            acc,
            now,
            token_sol_history,
            0,
        )
    }

    fn snapshot_from_accumulator_with_persisted_active_days(
        &self,
        wallet_id: String,
        acc: WalletAccumulator,
        now: DateTime<Utc>,
        token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
        persisted_active_days: u32,
    ) -> WalletSnapshot {
        let first_seen = acc.first_seen.unwrap_or(now);
        let last_seen = acc.last_seen.unwrap_or(now);
        let active_days = acc
            .exact_active_day_count
            .unwrap_or(acc.active_days.len() as u32);
        let eligibility_active_days = active_days.max(persisted_active_days);
        let (buy_total, quality_resolved_buys, tradable_buys, rug_metrics) =
            if acc.buy_observations.is_empty() {
                (
                    acc.buy_total,
                    acc.quality_resolved_buys,
                    acc.tradable_buys,
                    acc.rug_metrics,
                )
            } else {
                let buy_total = acc.buy_observations.len() as u32;
                let quality_resolved_buys = acc
                    .buy_observations
                    .iter()
                    .filter(|buy| buy.quality_resolved)
                    .count() as u32;
                let tradable_buys = acc
                    .buy_observations
                    .iter()
                    .filter(|buy| buy.quality_resolved && buy.tradable)
                    .count() as u32;
                let rug_metrics =
                    self.compute_rug_metrics(&acc.buy_observations, token_sol_history, now);
                (buy_total, quality_resolved_buys, tradable_buys, rug_metrics)
            };
        self.snapshot_from_components(
            wallet_id,
            first_seen,
            last_seen,
            acc.trades,
            eligibility_active_days,
            acc.spent_sol,
            acc.realized_pnl_sol,
            acc.max_buy_notional_sol,
            acc.wins,
            acc.closed_trades,
            &acc.hold_samples_sec,
            &acc.realized_pnl_by_day,
            acc.suspicious,
            buy_total,
            quality_resolved_buys,
            tradable_buys,
            rug_metrics,
            now,
        )
    }

    fn snapshot_from_persisted_metrics(
        &self,
        row: PersistedWalletMetricSnapshotRow,
        decay_cutoff: DateTime<Utc>,
    ) -> WalletSnapshot {
        let eligible = row.score > 0.0 && row.last_seen >= decay_cutoff;
        WalletSnapshot {
            wallet_id: row.wallet_id,
            first_seen: row.first_seen,
            last_seen: row.last_seen,
            pnl_sol: row.pnl,
            win_rate: row.win_rate,
            trades: row.trades,
            closed_trades: row.closed_trades,
            hold_median_seconds: row.hold_median_seconds,
            score: row.score,
            buy_total: row.buy_total,
            tradable_ratio: row.tradable_ratio,
            rug_ratio: row.rug_ratio,
            eligible,
        }
    }

    fn compute_rug_metrics(
        &self,
        buys: &[BuyObservation],
        token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
        now: DateTime<Utc>,
    ) -> RugMetrics {
        if buys.is_empty() {
            return RugMetrics::default();
        }
        let lookahead = Duration::seconds(self.config.rug_lookahead_seconds.max(1) as i64);
        let mut metrics = RugMetrics::default();

        for buy in buys {
            let window_end = buy.ts + lookahead;
            if window_end > now {
                metrics.unevaluated = metrics.unevaluated.saturating_add(1);
                continue;
            }
            metrics.evaluated = metrics.evaluated.saturating_add(1);
            let Some(trades) = token_sol_history.get(&buy.token) else {
                metrics.rugged = metrics.rugged.saturating_add(1);
                continue;
            };

            let start_idx = trades.partition_point(|trade| trade.ts < buy.ts);
            let end_idx = trades.partition_point(|trade| trade.ts <= window_end);

            let mut volume_sol = 0.0;
            let mut unique_traders = HashSet::new();
            for trade in &trades[start_idx..end_idx] {
                volume_sol += trade.sol_notional;
                unique_traders.insert(trade.wallet_id.as_str());
            }
            let thin_volume = volume_sol + 1e-12 < self.config.thin_market_min_volume_sol;
            let thin_traders =
                unique_traders.len() < self.config.thin_market_min_unique_traders as usize;
            if thin_volume || thin_traders {
                metrics.rugged = metrics.rugged.saturating_add(1);
            }
        }

        metrics
    }
}

impl WalletAccumulator {
    fn observe_activity_only(&mut self, swap: &SwapEvent, max_tx_per_minute: u32) {
        self.trades = self.trades.saturating_add(1);
        self.first_seen = Some(
            self.first_seen
                .map(|current| current.min(swap.ts_utc))
                .unwrap_or(swap.ts_utc),
        );
        self.last_seen = Some(
            self.last_seen
                .map(|current| current.max(swap.ts_utc))
                .unwrap_or(swap.ts_utc),
        );
        self.exact_active_day_count = None;
        self.active_days.insert(swap.ts_utc.date_naive());
        self.mark_tx_minute(swap.ts_utc.timestamp() / 60, max_tx_per_minute);
    }

    fn observe_swap(
        &mut self,
        swap: &SwapEvent,
        max_tx_per_minute: u32,
        buy_tradability: Option<BuyTradability>,
    ) {
        self.observe_activity_only(swap, max_tx_per_minute);

        if is_sol_buy(swap) {
            self.observe_buy(
                swap.token_out.as_str(),
                swap.amount_out,
                swap.amount_in,
                swap.ts_utc,
                buy_tradability.unwrap_or(BuyTradability::Rejected),
            );
            return;
        }
        if is_sol_sell(swap) {
            self.observe_sell(
                swap.token_in.as_str(),
                swap.amount_in,
                swap.amount_out,
                swap.ts_utc,
            );
        }
    }

    fn observe_swap_streaming(
        &mut self,
        swap: &SwapEvent,
        max_tx_per_minute: u32,
        buy_tradability: Option<BuyTradability>,
    ) {
        self.observe_activity_only(swap, max_tx_per_minute);

        if is_sol_buy(swap) {
            self.observe_buy_streaming(
                swap.token_out.as_str(),
                swap.amount_out,
                swap.amount_in,
                swap.ts_utc,
                buy_tradability.unwrap_or(BuyTradability::Rejected),
            );
            return;
        }
        if is_sol_sell(swap) {
            self.observe_sell(
                swap.token_in.as_str(),
                swap.amount_in,
                swap.amount_out,
                swap.ts_utc,
            );
        }
    }

    fn observe_buy(
        &mut self,
        token: &str,
        qty: f64,
        cost_sol: f64,
        ts: DateTime<Utc>,
        tradability: BuyTradability,
    ) {
        if qty <= 0.0 || cost_sol <= 0.0 {
            return;
        }
        let (tradable, quality_resolved) = match tradability {
            BuyTradability::Tradable => (true, true),
            BuyTradability::Rejected => (false, true),
            BuyTradability::Deferred => (false, false),
        };
        self.buy_total = self.buy_total.saturating_add(1);
        if quality_resolved {
            self.quality_resolved_buys = self.quality_resolved_buys.saturating_add(1);
        }
        if tradable {
            self.tradable_buys = self.tradable_buys.saturating_add(1);
        }
        self.buy_observations.push(BuyObservation {
            token: token.to_string(),
            ts,
            tradable,
            quality_resolved,
        });
        self.spent_sol += cost_sol;
        if cost_sol > self.max_buy_notional_sol {
            self.max_buy_notional_sol = cost_sol;
        }
        self.positions
            .entry(token.to_string())
            .or_default()
            .push_back(Lot {
                qty,
                cost_sol,
                opened_at: ts,
            });
    }

    fn observe_buy_streaming(
        &mut self,
        token: &str,
        qty: f64,
        cost_sol: f64,
        ts: DateTime<Utc>,
        tradability: BuyTradability,
    ) {
        if qty <= 0.0 || cost_sol <= 0.0 {
            return;
        }
        let (tradable, quality_resolved) = match tradability {
            BuyTradability::Tradable => (true, true),
            BuyTradability::Rejected => (false, true),
            BuyTradability::Deferred => (false, false),
        };
        self.buy_total = self.buy_total.saturating_add(1);
        if quality_resolved {
            self.quality_resolved_buys = self.quality_resolved_buys.saturating_add(1);
        }
        if tradable {
            self.tradable_buys = self.tradable_buys.saturating_add(1);
        }
        self.spent_sol += cost_sol;
        if cost_sol > self.max_buy_notional_sol {
            self.max_buy_notional_sol = cost_sol;
        }
        self.positions
            .entry(token.to_string())
            .or_default()
            .push_back(Lot {
                qty,
                cost_sol,
                opened_at: ts,
            });
    }

    fn note_streaming_buy_rug_status(&mut self, rug_status: BuyFactRugStatus) {
        match rug_status {
            BuyFactRugStatus::Healthy => {
                self.rug_metrics.evaluated = self.rug_metrics.evaluated.saturating_add(1);
            }
            BuyFactRugStatus::Rugged => {
                self.rug_metrics.evaluated = self.rug_metrics.evaluated.saturating_add(1);
                self.rug_metrics.rugged = self.rug_metrics.rugged.saturating_add(1);
            }
            BuyFactRugStatus::Unevaluated => {
                self.rug_metrics.unevaluated = self.rug_metrics.unevaluated.saturating_add(1);
            }
        }
    }

    fn observe_sell(&mut self, token: &str, qty: f64, proceeds_sol: f64, ts: DateTime<Utc>) {
        if qty <= 0.0 || proceeds_sol <= 0.0 {
            return;
        }
        let Some(lots) = self.positions.get_mut(token) else {
            return;
        };

        let mut qty_remaining = qty;
        let mut matched_qty = 0.0;
        let mut sell_pnl = 0.0;
        while qty_remaining > 1e-12 {
            if lots.front().is_none() {
                break;
            }
            if lots.front().map(|lot| lot.qty <= 1e-12).unwrap_or(false) {
                let _ = lots.pop_front();
                continue;
            }

            let (take_qty, cost_part, opened_at, should_remove) = {
                let front_lot = lots.front_mut().expect("checked non-empty above");
                let take_qty = qty_remaining.min(front_lot.qty);
                let original_qty = front_lot.qty;
                let opened_at = front_lot.opened_at;
                let lot_fraction = take_qty / original_qty;
                let cost_part = front_lot.cost_sol * lot_fraction;
                front_lot.qty -= take_qty;
                front_lot.cost_sol -= cost_part;
                let should_remove = front_lot.qty <= 1e-12;
                (take_qty, cost_part, opened_at, should_remove)
            };
            if should_remove {
                let _ = lots.pop_front();
            }

            let proceeds_part = proceeds_sol * (take_qty / qty);
            sell_pnl += proceeds_part - cost_part;
            matched_qty += take_qty;
            qty_remaining -= take_qty;

            let hold_sec = (ts - opened_at).num_seconds().max(0);
            self.hold_samples_sec.push(hold_sec);
            if should_remove && lots.is_empty() {
                break;
            }
        }

        if matched_qty <= 1e-12 {
            return;
        }
        self.realized_pnl_sol += sell_pnl;
        self.closed_trades = self.closed_trades.saturating_add(1);
        if sell_pnl > 0.0 {
            self.wins = self.wins.saturating_add(1);
        }
        *self
            .realized_pnl_by_day
            .entry(ts.date_naive())
            .or_insert(0.0) += sell_pnl;
    }

    fn mark_tx_minute(&mut self, minute_bucket: i64, max_tx_per_minute: u32) {
        let next = self
            .tx_per_minute
            .entry(minute_bucket)
            .and_modify(|value| *value += 1)
            .or_insert(1);
        if *next > max_tx_per_minute.max(1) {
            self.suspicious = true;
        }
    }
}

fn is_sol_buy(swap: &SwapEvent) -> bool {
    swap.token_in == SOL_MINT && swap.token_out != SOL_MINT
}

fn is_sol_sell(swap: &SwapEvent) -> bool {
    swap.token_out == SOL_MINT && swap.token_in != SOL_MINT
}

fn sol_leg_token(swap: &SwapEvent) -> Option<&str> {
    if is_sol_buy(swap) {
        Some(swap.token_out.as_str())
    } else if is_sol_sell(swap) {
        Some(swap.token_in.as_str())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wallet_freshness_audit::{
        current_raw_truth_sample_call_count_for_tests,
        reset_current_raw_truth_sample_call_count_for_tests, wallet_freshness_capture_from_row,
        WalletFreshnessHistoryVerdict,
    };
    use anyhow::{anyhow, Context};
    use copybot_config::ShadowConfig;
    use copybot_storage::{
        CopySignalRow, DiscoveryAggregateWriteConfig, DiscoveryPersistedRebuildPhase,
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor, DiscoveryRuntimeMode, SqliteStore,
        WalletActivityDayRow, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE,
    };
    use rusqlite::Connection;
    use std::collections::HashSet;
    use std::path::Path;
    use tempfile::tempdir;

    fn permissive_shadow_quality() -> ShadowConfig {
        let mut config = ShadowConfig::default();
        config.min_token_age_seconds = 0;
        config.min_holders = 0;
        config.min_liquidity_sol = 0.0;
        config.min_volume_5m_sol = 0.0;
        config.min_unique_traders_5m = 0;
        config
    }

    fn seed_recent_published_universe(
        store: &SqliteStore,
        now: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        scoring_source: &str,
        published_wallet_ids: &HashSet<String>,
    ) -> Result<()> {
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "test_recent_published_universe".to_string(),
            last_published_at: Some(now),
            last_published_window_start: Some(metrics_window_start),
            published_scoring_source: Some(scoring_source.to_string()),
            published_wallet_ids: Some(published_wallet_ids.iter().cloned().collect()),
        })
    }

    fn seed_published_wallet_metrics_snapshot(
        store: &SqliteStore,
        metrics_window_start: DateTime<Utc>,
        eligible_wallets: usize,
        active_wallets: usize,
    ) -> Result<HashSet<String>> {
        let mut published_active_wallets = HashSet::new();
        for idx in 0..eligible_wallets {
            let wallet_id = format!("wallet_published_{idx:03}");
            store.upsert_wallet(
                &wallet_id,
                metrics_window_start - Duration::days(1),
                metrics_window_start + Duration::minutes(idx as i64),
                "candidate",
            )?;
            store.insert_wallet_metric(&WalletMetricRow {
                wallet_id: wallet_id.clone(),
                window_start: metrics_window_start,
                pnl: 2.0 + idx as f64 * 0.001,
                win_rate: 0.8,
                trades: 6,
                closed_trades: 6,
                hold_median_seconds: 120,
                score: 1.0 - idx as f64 * 0.001,
                buy_total: 6,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            })?;
            if idx < active_wallets {
                store.activate_follow_wallet(
                    &wallet_id,
                    metrics_window_start + Duration::minutes(idx as i64),
                    "seed-follow",
                )?;
                published_active_wallets.insert(wallet_id);
            }
        }
        Ok(published_active_wallets)
    }

    fn post_bootstrap_watchdog_config() -> DiscoveryConfig {
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 1;
        config.decay_window_days = 1;
        config.follow_top_n = 1;
        config.min_score = 0.0;
        config.metric_snapshot_interval_seconds = 60;
        config.max_window_swaps_in_memory = 8;
        config.max_fetch_swaps_per_cycle = 100;
        config.max_fetch_pages_per_cycle = 10;
        config.fetch_time_budget_ms = 1_000;
        config.thin_market_min_unique_traders = 1;
        config
    }

    fn stage1_runtime_config() -> DiscoveryConfig {
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.observed_swaps_retention_days = 14;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.metric_snapshot_interval_seconds = 60;
        config.max_window_swaps_in_memory = 8;
        config.max_fetch_swaps_per_cycle = 100;
        config.max_fetch_pages_per_cycle = 10;
        config.fetch_time_budget_ms = 1_000;
        config.thin_market_min_unique_traders = 1;
        config
    }

    fn bounded_stage1_runtime_config() -> DiscoveryConfig {
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 5;
        config.max_fetch_pages_per_cycle = 1;
        config.fetch_time_budget_ms = 60_000;
        config
    }

    fn metrics_window_start_for_test(
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
    ) -> DateTime<Utc> {
        let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
    }

    fn assert_sorted_strings(values: &[String]) {
        assert!(
            values
                .windows(2)
                .all(|pair| pair[0].as_str() <= pair[1].as_str()),
            "expected canonical sorted strings, got {values:?}"
        );
    }

    fn seed_stage1_persisted_stream_runtime_fixture(
        store: &SqliteStore,
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
        profitable_pairs: usize,
        tail_noise_rows: usize,
    ) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(config, now);

        for idx in 0..profitable_pairs {
            let offset = Duration::minutes((idx * 20) as i64);
            let token = format!("TokenStage1PersistedTop{idx:02}111111111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("stage1-persisted-top-buy-{idx}"),
                window_start + offset,
                SOL_MINT,
                token.as_str(),
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("stage1-persisted-top-sell-{idx}"),
                window_start + offset + Duration::minutes(5),
                token.as_str(),
                SOL_MINT,
                100.0,
                1.3,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("stage1-persisted-noise-early-buy-{idx}"),
                window_start + offset,
                SOL_MINT,
                token.as_str(),
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("stage1-persisted-noise-early-sell-{idx}"),
                window_start + offset + Duration::minutes(5),
                token.as_str(),
                SOL_MINT,
                100.0,
                0.7,
            ))?;
        }

        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..tail_noise_rows {
            let ts =
                now - Duration::minutes(tail_noise_rows as i64) + Duration::minutes(idx as i64);
            let swap = swap(
                "wallet_tail_noise",
                &format!("stage1-persisted-tail-noise-{idx}"),
                ts,
                SOL_MINT,
                "TokenStage1PersistedNoise1111111111111111",
                0.2,
                20.0,
            );
            latest_cursor = Some(DiscoveryRuntimeCursor {
                ts_utc: swap.ts_utc,
                slot: swap.slot,
                signature: swap.signature.clone(),
            });
            store.insert_observed_swap(&swap)?;
        }
        store.upsert_discovery_runtime_cursor(
            &latest_cursor.expect("latest cursor should be present"),
        )?;

        Ok((window_start, metrics_window_start))
    }

    fn load_persisted_stream_rebuild_state_for_test(
        store: &SqliteStore,
    ) -> Result<PersistedStreamRebuildState> {
        let row = store
            .load_discovery_persisted_rebuild_state()?
            .expect("persisted rebuild state should exist");
        DiscoveryService::persisted_stream_rebuild_state_from_row(row)
    }

    fn seed_stage1_replay_noise_fixture(
        store: &SqliteStore,
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
        profitable_pairs: usize,
        non_sol_noise_rows: usize,
    ) -> Result<(DateTime<Utc>, DateTime<Utc>, usize, usize)> {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(config, now);
        let mut total_rows = 0usize;
        let mut sol_leg_rows = 0usize;

        for idx in 0..profitable_pairs {
            let ts = window_start + Duration::minutes((idx * 12) as i64 + 1);
            let token = format!("TokenStage1ReplayOpt{idx:04}1111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_replay_top",
                &format!("stage1-replay-opt-buy-{idx}"),
                ts,
                SOL_MINT,
                token.as_str(),
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_replay_top",
                &format!("stage1-replay-opt-sell-{idx}"),
                ts + Duration::minutes(5),
                token.as_str(),
                SOL_MINT,
                100.0,
                1.2,
            ))?;
            total_rows = total_rows.saturating_add(2);
            sol_leg_rows = sol_leg_rows.saturating_add(2);
        }

        for idx in 0..non_sol_noise_rows {
            let ts = window_start
                + Duration::minutes((idx % 240) as i64)
                + Duration::seconds((idx / 240) as i64);
            let token_in = format!("TokenReplayNoiseIn{:03}111111111111111111", idx % 17);
            let token_out = format!("TokenReplayNoiseOut{:03}1111111111111111", idx % 19);
            store.insert_observed_swap(&swap(
                &format!("wallet_replay_noise_{:02}", idx % 11),
                &format!("stage1-replay-opt-noise-{idx}"),
                ts,
                token_in.as_str(),
                token_out.as_str(),
                2.0,
                3.0,
            ))?;
            total_rows = total_rows.saturating_add(1);
        }

        Ok((window_start, metrics_window_start, total_rows, sol_leg_rows))
    }

    fn seed_stage1_collect_buy_mints_noise_fixture(
        store: &SqliteStore,
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
        unique_buy_mints: usize,
        sell_noise_rows: usize,
    ) -> Result<DateTime<Utc>> {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);

        for idx in 0..unique_buy_mints {
            let ts = window_start + Duration::minutes(idx as i64);
            let token = format!("TokenStage1CollectBuyMint{idx:04}1111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_collect_buy_mints",
                &format!("stage1-collect-buy-mints-buy-{idx}"),
                ts,
                SOL_MINT,
                token.as_str(),
                0.5,
                50.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_collect_buy_mints",
                &format!("stage1-collect-buy-mints-sell-{idx}"),
                ts + Duration::minutes(1),
                token.as_str(),
                SOL_MINT,
                50.0,
                0.6,
            ))?;
        }

        for idx in 0..sell_noise_rows {
            let ts = window_start + Duration::hours(4) + Duration::seconds(idx as i64);
            let token = format!("TokenStage1SellNoise{idx:04}11111111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_sell_noise",
                &format!("stage1-collect-buy-mints-sell-noise-{idx}"),
                ts,
                token.as_str(),
                SOL_MINT,
                100.0,
                0.05,
            ))?;
        }

        Ok(window_start)
    }

    fn seed_stage1_collect_buy_mints_legacy_migration_fixture(
        store: &SqliteStore,
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
    ) -> Result<(DateTime<Utc>, Vec<String>)> {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let ordered = vec![
            "TokenStage1CollectBuyMintA1111111111111111".to_string(),
            "TokenStage1CollectBuyMintB1111111111111111".to_string(),
            "TokenStage1CollectBuyMintC1111111111111111".to_string(),
            "TokenStage1CollectBuyMintD1111111111111111".to_string(),
        ];
        let chronological = [
            ordered[3].as_str(),
            ordered[0].as_str(),
            ordered[1].as_str(),
            ordered[2].as_str(),
        ];
        for (idx, token) in chronological.iter().enumerate() {
            let ts = window_start + Duration::minutes(idx as i64);
            store.insert_observed_swap(&swap(
                "wallet_collect_buy_migration",
                &format!("stage1-collect-buy-migration-buy-{idx}"),
                ts,
                SOL_MINT,
                token,
                0.5,
                50.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_collect_buy_migration",
                &format!("stage1-collect-buy-migration-sell-{idx}"),
                ts + Duration::minutes(1),
                token,
                SOL_MINT,
                50.0,
                0.6,
            ))?;
        }

        Ok((window_start, ordered))
    }

    fn aggregate_readiness_config() -> DiscoveryConfig {
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.refresh_seconds = 600;
        config.metric_snapshot_interval_seconds = 1_800;
        config.scoring_aggregates_write_enabled = true;
        config.scoring_aggregates_enabled = true;
        config
    }

    fn seed_runtime_aggregate_ready_wallet(
        store: &SqliteStore,
        config: &DiscoveryConfig,
        wallet_id: &str,
        token: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let mut swaps = Vec::new();
        for idx in 0..4 {
            let buy_ts = now - Duration::days(3) + Duration::minutes((idx * 20) as i64);
            let sell_ts = buy_ts + Duration::minutes(6);
            let buy = swap(
                wallet_id,
                &format!("{wallet_id}-aggregate-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                token,
                1.0,
                100.0,
            );
            let sell = swap(
                wallet_id,
                &format!("{wallet_id}-aggregate-sell-{idx}"),
                sell_ts,
                token,
                SOL_MINT,
                100.0,
                1.2,
            );
            store.insert_observed_swap(&buy)?;
            store.insert_observed_swap(&sell)?;
            swaps.push(buy);
            swaps.push(sell);
        }

        store.reset_discovery_scoring_tables()?;
        store.apply_discovery_scoring_batch(&swaps, &aggregate_write_config(config))?;
        store.finalize_discovery_scoring_rug_facts(now)?;
        store.set_discovery_scoring_covered_since(window_start)?;
        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now,
            slot: 999,
            signature: format!("{wallet_id}-aggregate-covered-through"),
        })?;
        Ok(())
    }

    fn seed_bridged_bootstrap_followlist(
        store: &SqliteStore,
        discovery: &DiscoveryService,
        bootstrap_now: DateTime<Utc>,
        wallet_id: &str,
    ) -> Result<TrustedWalletMetricsSnapshotWrite> {
        let bootstrap_metrics_window_start = discovery.metrics_window_start(bootstrap_now);
        let snapshot_write = trusted_snapshot_write(
            TrustedSnapshotSourceKind::CloneLatestBridge,
            TrustedSelectionState::TrustedBridged,
            bootstrap_metrics_window_start,
            bootstrap_now,
            1,
            Some("seed-bridged-source".to_string()),
            Some(bootstrap_metrics_window_start - Duration::minutes(1)),
        );
        store.persist_discovery_cycle_with_snapshot_metadata(
            &[WalletUpsertRow {
                wallet_id: wallet_id.to_string(),
                first_seen: bootstrap_now - Duration::hours(12),
                last_seen: bootstrap_now - Duration::minutes(1),
                status: "candidate".to_string(),
            }],
            &[WalletMetricRow {
                wallet_id: wallet_id.to_string(),
                window_start: bootstrap_metrics_window_start,
                pnl: 2.0,
                win_rate: 0.8,
                trades: 6,
                closed_trades: 3,
                hold_median_seconds: 300,
                score: 0.7,
                buy_total: 3,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            }],
            &[wallet_id.to_string()],
            true,
            true,
            bootstrap_now,
            "seed-bridged-bootstrap-followlist",
            Some(&snapshot_write),
        )?;
        store.set_discovery_trusted_selection_state(&DiscoveryTrustedSelectionStateUpdate {
            bootstrap_required: false,
            reason: "trusted_selection_bootstrap_satisfied".to_string(),
            selection_state: TrustedSelectionState::TrustedBridged,
            active_snapshot_id: Some(snapshot_write.snapshot_id.clone()),
            active_snapshot_window_start: Some(bootstrap_metrics_window_start),
            last_bootstrap_source_kind: Some(TrustedSnapshotSourceKind::CloneLatestBridge),
            last_bootstrap_at: Some(bootstrap_now),
        })?;
        Ok(snapshot_write)
    }

    fn seed_cap_truncated_raw_tail(
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        count: usize,
    ) -> Result<()> {
        for idx in 0..count {
            let ts = window_start + Duration::seconds((idx + 1) as i64);
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("watchdog-noise-buy-{idx}"),
                ts,
                SOL_MINT,
                "TokenWatchdogNoise111111111111111111111111",
                0.2,
                20.0,
            ))?;
        }
        Ok(())
    }

    fn prime_running_discovery_cursor(
        discovery: &DiscoveryService,
        cursor_window_start: DateTime<Utc>,
    ) {
        let mut state = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        state.cursor = Some(DiscoveryCursor::bootstrap(cursor_window_start));
    }

    fn seed_current_trusted_source_snapshot(
        store: &SqliteStore,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        source_kind: TrustedSnapshotSourceKind,
        wallet_id: &str,
    ) -> Result<TrustedWalletMetricsSnapshotWrite> {
        let snapshot_write = trusted_snapshot_write(
            source_kind,
            TrustedSelectionState::TrustedCurrent,
            metrics_window_start,
            now,
            1,
            None,
            Some(metrics_window_start),
        );
        store.persist_discovery_cycle_with_snapshot_metadata(
            &[WalletUpsertRow {
                wallet_id: wallet_id.to_string(),
                first_seen: now - Duration::hours(6),
                last_seen: now - Duration::minutes(1),
                status: "candidate".to_string(),
            }],
            &[WalletMetricRow {
                wallet_id: wallet_id.to_string(),
                window_start: metrics_window_start,
                pnl: 3.0,
                win_rate: 0.9,
                trades: 7,
                closed_trades: 4,
                hold_median_seconds: 120,
                score: 0.9,
                buy_total: 4,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            }],
            &[],
            false,
            false,
            now,
            "seed-current-trusted-source",
            Some(&snapshot_write),
        )?;
        Ok(snapshot_write)
    }

    #[test]
    fn promotes_profitable_wallets_to_followlist() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::days(1);

        for idx in 0..12 {
            let buy_ts = start + Duration::minutes((idx * 20) as i64);
            let sell_ts = buy_ts + Duration::minutes(6);
            let signature_buy = format!("a-buy-{idx}");
            let signature_sell = format!("a-sell-{idx}");
            store.insert_observed_swap(&swap(
                "wallet_a",
                &signature_buy,
                buy_ts,
                SOL_MINT,
                "TokenA11111111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_a",
                &signature_sell,
                sell_ts,
                "TokenA11111111111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.35,
            ))?;

            let signature_b_buy = format!("b-buy-{idx}");
            let signature_b_sell = format!("b-sell-{idx}");
            store.insert_observed_swap(&swap(
                "wallet_b",
                &signature_b_buy,
                buy_ts,
                SOL_MINT,
                "TokenB11111111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_b",
                &signature_b_sell,
                sell_ts,
                "TokenB11111111111111111111111111111111111",
                SOL_MINT,
                100.0,
                0.70,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.55;
        config.max_tx_per_minute = 50;
        config.thin_market_min_unique_traders = 1;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.wallets_seen, 2);
        assert_eq!(summary.metrics_written, 2);
        assert!(summary.follow_promoted >= 1);

        let active = store.list_active_follow_wallets()?;
        assert!(active.contains("wallet_a"));
        assert!(!active.contains("wallet_b"));
        Ok(())
    }

    #[test]
    fn run_cycle_recovers_from_poisoned_window_mutex() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-poison.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());

        let state = discovery.window_state.clone();
        let _ = std::panic::catch_unwind(move || {
            let _guard = state.lock().expect("lock must succeed");
            panic!("poison discovery window state");
        });

        let now = Utc::now();
        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.wallets_seen, 0);
        assert_eq!(summary.metrics_written, 0);
        Ok(())
    }

    #[test]
    fn run_cycle_enforces_max_window_swaps_in_memory_cap() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-cap.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-04T11:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::minutes(30);
        for idx in 0..20 {
            let ts = start + Duration::seconds((idx * 5) as i64);
            store.insert_observed_swap(&swap(
                "wallet_cap",
                &format!("cap-sig-{idx:03}"),
                ts,
                SOL_MINT,
                "TokenCap1111111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.max_window_swaps_in_memory = 5;
        config.max_fetch_swaps_per_cycle = 100;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let _ = discovery.run_cycle(&store, now)?;

        let guard = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(
            guard.swaps.len() <= 5,
            "window swap cache must stay within configured cap"
        );
        assert!(
            guard.signatures.len() <= 5,
            "window signature cache must stay within configured cap"
        );
        Ok(())
    }

    #[test]
    fn run_cycle_uses_persisted_cursor_for_incremental_fetch_after_restart() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-cursor.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::minutes(20);
        for idx in 0..12 {
            let ts = start + Duration::seconds((idx * 10) as i64);
            store.insert_observed_swap(&swap(
                "wallet_cursor",
                &format!("cursor-sig-{idx:03}"),
                ts,
                SOL_MINT,
                "TokenCursor1111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.max_window_swaps_in_memory = 100;
        config.max_fetch_swaps_per_cycle = 4;
        config.max_fetch_pages_per_cycle = 1;

        let discovery_first = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let _ = discovery_first.run_cycle(&store, now)?;
        let cursor_after_first = store
            .load_discovery_runtime_cursor()?
            .expect("cursor must be persisted after first cycle");
        assert_eq!(cursor_after_first.signature, "cursor-sig-003");

        // Simulate process restart: new DiscoveryService should continue from persisted cursor.
        let discovery_second = DiscoveryService::new(config, permissive_shadow_quality());
        let _ = discovery_second.run_cycle(&store, now + Duration::minutes(1))?;
        let cursor_after_second = store
            .load_discovery_runtime_cursor()?
            .expect("cursor must stay persisted after second cycle");
        assert_eq!(cursor_after_second.signature, "cursor-sig-007");
        Ok(())
    }

    #[test]
    fn run_cycle_fetches_multiple_cursor_pages_within_single_cycle() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-multi-page-fetch.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::minutes(20);
        for idx in 0..10 {
            let ts = start + Duration::seconds((idx * 10) as i64);
            store.insert_observed_swap(&swap(
                "wallet_multi_page",
                &format!("multi-page-sig-{idx:03}"),
                ts,
                SOL_MINT,
                "TokenMultiPage111111111111111111111111111",
                1.0,
                100.0,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.max_window_swaps_in_memory = 100;
        config.max_fetch_swaps_per_cycle = 4;
        config.max_fetch_pages_per_cycle = 3;
        config.fetch_time_budget_ms = 60_000;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let _ = discovery.run_cycle(&store, now)?;
        let cursor = store
            .load_discovery_runtime_cursor()?
            .expect("cursor must be persisted after multi-page fetch");
        assert_eq!(
            cursor.signature, "multi-page-sig-009",
            "single cycle should page through all cursor rows until the short final page"
        );
        Ok(())
    }

    #[test]
    fn run_cycle_respects_fetch_page_budget_and_continues_next_cycle() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-fetch-page-budget.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::minutes(20);
        for idx in 0..12 {
            let ts = start + Duration::seconds((idx * 10) as i64);
            store.insert_observed_swap(&swap(
                "wallet_page_budget",
                &format!("page-budget-sig-{idx:03}"),
                ts,
                SOL_MINT,
                "TokenPageBudget1111111111111111111111111",
                1.0,
                100.0,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.max_window_swaps_in_memory = 100;
        config.max_fetch_swaps_per_cycle = 4;
        config.max_fetch_pages_per_cycle = 2;
        config.fetch_time_budget_ms = 60_000;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let _ = discovery.run_cycle(&store, now)?;
        let cursor_after_first = store
            .load_discovery_runtime_cursor()?
            .expect("cursor must be persisted after first bounded cycle");
        assert_eq!(cursor_after_first.signature, "page-budget-sig-007");

        let _ = discovery.run_cycle(&store, now + Duration::minutes(1))?;
        let cursor_after_second = store
            .load_discovery_runtime_cursor()?
            .expect("cursor must advance on next cycle");
        assert_eq!(cursor_after_second.signature, "page-budget-sig-011");
        Ok(())
    }

    #[test]
    fn run_cycle_advances_cursor_between_publish_ticks() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-fetch-vs-publish-cadence.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::minutes(20);
        for idx in 0..12 {
            let ts = start + Duration::seconds((idx * 10) as i64);
            store.insert_observed_swap(&swap(
                "wallet_publish_gate",
                &format!("publish-gate-sig-{idx:03}"),
                ts,
                SOL_MINT,
                "TokenPublishGate111111111111111111111111",
                1.0,
                100.0,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.refresh_seconds = 600;
        config.max_window_swaps_in_memory = 100;
        config.max_fetch_swaps_per_cycle = 4;
        config.max_fetch_pages_per_cycle = 1;
        config.fetch_time_budget_ms = 60_000;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_summary = discovery.run_cycle(&store, now)?;
        assert!(first_summary.published, "first live tick should publish");
        let cursor_after_first = store
            .load_discovery_runtime_cursor()?
            .expect("cursor must persist after first tick");
        assert_eq!(cursor_after_first.signature, "publish-gate-sig-003");

        let second_summary = discovery.run_cycle(&store, now + Duration::minutes(1))?;
        assert!(
            !second_summary.published,
            "next fast fetch tick inside publish cadence should stay fetch-only"
        );
        let cursor_after_second = store
            .load_discovery_runtime_cursor()?
            .expect("cursor must advance during fetch-only tick");
        assert_eq!(cursor_after_second.signature, "publish-gate-sig-007");
        Ok(())
    }

    #[test]
    fn restart_with_persisted_cursor_warm_load_does_not_false_demote_followlist() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-followlist-warm.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-04T13:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::hours(8);
        for idx in 0..12 {
            let buy_ts = start + Duration::minutes((idx * 20) as i64);
            let sell_ts = buy_ts + Duration::minutes(5);
            store.insert_observed_swap(&swap(
                "wallet_a",
                &format!("warm-a-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenWarmA11111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_a",
                &format!("warm-a-sell-{idx}"),
                sell_ts,
                "TokenWarmA11111111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.35,
            ))?;

            store.insert_observed_swap(&swap(
                "wallet_b",
                &format!("warm-b-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenWarmB11111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_b",
                &format!("warm-b-sell-{idx}"),
                sell_ts,
                "TokenWarmB11111111111111111111111111111111",
                SOL_MINT,
                100.0,
                0.70,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.55;
        config.max_tx_per_minute = 50;
        config.min_buy_count = 10;
        config.thin_market_min_unique_traders = 1;
        config.max_window_swaps_in_memory = 200;
        config.max_fetch_swaps_per_cycle = 200;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let _ = discovery.run_cycle(&store, now)?;
        let active_before = store.list_active_follow_wallets()?;
        assert!(active_before.contains("wallet_a"));

        // One fresh low-signal swap arrives after cursor checkpoint.
        store.insert_observed_swap(&swap(
            "wallet_noise",
            "warm-noise-buy-0",
            now + Duration::minutes(1),
            SOL_MINT,
            "TokenNoise111111111111111111111111111111111",
            0.2,
            20.0,
        ))?;

        // Simulate restart with narrow per-cycle fetch budget.
        let mut restart_config = config.clone();
        restart_config.max_fetch_swaps_per_cycle = 1;
        let discovery_after_restart =
            DiscoveryService::new(restart_config, permissive_shadow_quality());
        let _ = discovery_after_restart.run_cycle(&store, now + Duration::minutes(2))?;
        let active_after = store.list_active_follow_wallets()?;
        assert!(
            active_after.contains("wallet_a"),
            "wallet_a should not be false-demoted on restart cold state"
        );
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn restart_with_short_retention_uses_persisted_wallet_metrics_bootstrap() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-followlist-short-retention.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-08T13:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.observed_swaps_retention_days = 1;
        config.follow_top_n = 1;
        let metrics_window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        store.upsert_wallet(
            "wallet_a",
            now - Duration::days(4),
            now - Duration::days(2),
            "candidate",
        )?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_a".to_string(),
            window_start: metrics_window_start,
            pnl: 2.4,
            win_rate: 0.85,
            trades: 16,
            closed_trades: 8,
            hold_median_seconds: 360,
            score: 0.81,
            buy_total: 8,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
        store.activate_follow_wallet("wallet_a", now - Duration::minutes(5), "test-seed")?;
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::days(2),
            slot: 42,
            signature: "cursor-short-retention".to_string(),
        })?;
        let active_before = store.list_active_follow_wallets()?;
        assert!(active_before.contains("wallet_a"));

        let discovery_after_restart = DiscoveryService::new(config, permissive_shadow_quality());
        let summary_after_restart =
            discovery_after_restart.run_cycle(&store, now + Duration::minutes(1))?;
        assert!(
            summary_after_restart.eligible_wallets >= 1,
            "persisted wallet_metrics bootstrap should keep recent candidates eligible after short-retention restart"
        );
        let active_after = store.list_active_follow_wallets()?;
        assert!(
            active_after.contains("wallet_a"),
            "wallet_a should stay active even when raw observed_swaps history was purged below the scoring window"
        );
        Ok(())
    }

    #[test]
    fn cold_start_full_raw_window_without_trusted_snapshot_publishes_raw_top_n_stage1() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-cold-start-full-raw.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        for idx in 0..6 {
            let offset = Duration::minutes((idx * 20) as i64);
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("stage1-top-buy-{idx}"),
                now - Duration::days(2) + offset,
                SOL_MINT,
                "TokenStage1Top1111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("stage1-top-sell-{idx}"),
                now - Duration::days(2) + offset + Duration::minutes(5),
                "TokenStage1Top1111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.3,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("stage1-noise-buy-{idx}"),
                now - Duration::days(2) + offset,
                SOL_MINT,
                "TokenStage1Noise11111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("stage1-noise-sell-{idx}"),
                now - Duration::days(2) + offset + Duration::minutes(5),
                "TokenStage1Noise11111111111111111111111111",
                SOL_MINT,
                100.0,
                0.7,
            ))?;
        }

        let mut config = stage1_runtime_config();
        config.max_window_swaps_in_memory = 64;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(summary.scoring_source, "raw_window");
        assert!(!summary.trusted_selection_fail_closed);
        assert_eq!(summary.active_follow_wallets, 1);
        assert!(
            summary
                .top_wallets
                .iter()
                .any(|label| label.starts_with("wallet_top:")),
            "raw-window publish should surface the profitable wallet as leader"
        );
        let active_wallets = store.list_active_follow_wallets()?;
        assert_eq!(active_wallets.len(), 1);
        assert!(active_wallets.contains("wallet_top"));
        Ok(())
    }

    #[test]
    fn cold_start_truncated_in_memory_with_complete_persisted_observed_swaps_publishes_healthy_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-cold-start-persisted-stream-healthy.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = {
            let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
            let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
            let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
            bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
        };

        for idx in 0..6 {
            let offset = Duration::minutes((idx * 20) as i64);
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("stage1-persisted-top-buy-{idx}"),
                window_start + offset,
                SOL_MINT,
                "TokenStage1PersistedTop111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("stage1-persisted-top-sell-{idx}"),
                window_start + offset + Duration::minutes(5),
                "TokenStage1PersistedTop111111111111111111111",
                SOL_MINT,
                100.0,
                1.3,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("stage1-persisted-noise-early-buy-{idx}"),
                window_start + offset,
                SOL_MINT,
                "TokenStage1PersistedTop111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("stage1-persisted-noise-early-sell-{idx}"),
                window_start + offset + Duration::minutes(5),
                "TokenStage1PersistedTop111111111111111111111",
                SOL_MINT,
                100.0,
                0.7,
            ))?;
        }

        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..9 {
            let ts = now - Duration::minutes(9) + Duration::minutes(idx as i64);
            let swap = swap(
                "wallet_tail_noise",
                &format!("stage1-persisted-tail-noise-{idx}"),
                ts,
                SOL_MINT,
                "TokenStage1PersistedNoise1111111111111111",
                0.2,
                20.0,
            );
            latest_cursor = Some(DiscoveryRuntimeCursor {
                ts_utc: swap.ts_utc,
                slot: swap.slot,
                signature: swap.signature.clone(),
            });
            store.insert_observed_swap(&swap)?;
        }
        store.upsert_discovery_runtime_cursor(
            &latest_cursor.expect("latest cursor should be present"),
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(summary.scoring_source, "raw_window_persisted_stream");
        assert!(!summary.trusted_selection_fail_closed);
        assert!(
            summary.raw_window_cap_truncated,
            "persisted-stream runtime path should still expose that the in-memory raw cache was truncated"
        );
        assert!(summary.eligible_wallets >= 1);
        assert!(summary.follow_promoted >= 1);
        assert!(
            summary.metrics_written > 0,
            "healthy persisted-stream recompute should persist the snapshot bucket"
        );
        assert_eq!(
            store.latest_wallet_metrics_window_start()?,
            Some(metrics_window_start)
        );
        let active_wallets = store.list_active_follow_wallets()?;
        assert_eq!(active_wallets, HashSet::from([String::from("wallet_top")]));
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_is_bounded_and_observable_without_recent_published_universe_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-bounded-persisted-stream-observable.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(
            summary.scoring_source,
            "raw_window_incomplete_no_recent_published_universe"
        );
        assert_eq!(summary.active_follow_wallets, 0);

        let rebuild = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            rebuild.phase,
            DiscoveryPersistedRebuildPhase::CollectBuyMints
        );
        assert_eq!(rebuild.window_start, window_start);
        assert_eq!(rebuild.metrics_window_start, metrics_window_start);
        assert_eq!(rebuild.horizon_end, now);
        assert_eq!(rebuild.prepass_rows_processed, 5);
        assert_eq!(rebuild.prepass_pages_processed, 1);
        assert_eq!(rebuild.replay_rows_processed, 0);
        assert_eq!(rebuild.replay_pages_processed, 0);
        assert_eq!(rebuild.chunks_completed, 1);
        assert!(
            rebuild.payload.collect_buy_mints_cursor_token.is_some(),
            "bounded collect_buy_mints must persist its own resumable mint cursor"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_collect_buy_mints_reduces_total_work_on_large_noise_fixture_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-collect-buy-mints-large-noise-throughput.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let window_start =
            seed_stage1_collect_buy_mints_noise_fixture(&store, &config, now, 7, 500)?;
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let mut state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            20,
            1,
            Instant::now() + StdDuration::from_secs(1),
        )?;

        assert!(phase_advance.source_exhausted);
        assert_eq!(phase_advance.rows_processed, 7);
        assert_eq!(phase_advance.pages_processed, 1);
        assert_eq!(phase_advance.unique_buy_mints_discovered, 7);
        assert_eq!(state.payload.unique_buy_mints.len(), 7);
        assert!(
            phase_advance.collect_buy_mints_cursor_token.is_none(),
            "completed direct distinct-mint prepass should clear the mint cursor"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_collect_buy_mints_carry_forward_reconcile_reduces_work_on_large_noise_fixture_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-collect-buy-mints-carry-forward-noise-throughput.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let checkpoint_now = DateTime::parse_from_rfc3339("2026-03-17T12:00:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let rollover_now = checkpoint_now + Duration::seconds(20);
        let window_start =
            checkpoint_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, checkpoint_now);
        let next_window_start =
            rollover_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let next_metrics_window_start = metrics_window_start_for_test(&config, rollover_now);

        let token_old_only = "TokenStage1CarryThroughputOldOnly111111111".to_string();
        let token_survives = "TokenStage1CarryThroughputSurvives11111".to_string();
        let token_new_tail_a = "TokenStage1CarryThroughputNewTailA1111".to_string();
        let token_new_tail_b = "TokenStage1CarryThroughputNewTailB1111".to_string();

        for (idx, (token, ts)) in [
            (token_old_only.as_str(), window_start + Duration::seconds(5)),
            (
                token_survives.as_str(),
                next_window_start + Duration::seconds(5),
            ),
            (
                token_new_tail_a.as_str(),
                checkpoint_now + Duration::seconds(5),
            ),
            (
                token_new_tail_b.as_str(),
                checkpoint_now + Duration::seconds(6),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            store.insert_observed_swap(&swap(
                "wallet_carry_throughput",
                &format!("stage1-carry-throughput-buy-{idx}"),
                ts,
                SOL_MINT,
                token,
                0.5,
                50.0,
            ))?;
        }

        let expired_head_noise_rows = 2_000usize;
        for idx in 0..expired_head_noise_rows {
            let ts = window_start + Duration::milliseconds((idx % 20_000) as i64);
            store.insert_observed_swap(&swap(
                "wallet_expired_head_noise",
                &format!("stage1-expired-head-noise-{idx}"),
                ts,
                &format!("NoiseExpiredHeadToken{idx:05}111111111111"),
                SOL_MINT,
                100.0,
                0.01,
            ))?;
        }
        let new_tail_noise_rows = 2_000usize;
        for idx in 0..new_tail_noise_rows {
            let ts = checkpoint_now + Duration::milliseconds((idx % 19_000) as i64 + 1);
            store.insert_observed_swap(&swap(
                "wallet_new_tail_noise",
                &format!("stage1-new-tail-noise-{idx}"),
                ts,
                &format!("NoiseNewTailToken{idx:05}11111111111111"),
                SOL_MINT,
                100.0,
                0.02,
            ))?;
        }

        let mut stale_state = discovery.start_persisted_stream_rebuild_state(
            window_start,
            metrics_window_start,
            checkpoint_now,
        );
        stale_state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        stale_state.prepass_rows_processed = 2;
        stale_state.prepass_pages_processed = 1;
        stale_state.payload.unique_buy_mints = vec![token_old_only.clone(), token_survives.clone()];
        stale_state.payload.buy_mint_counts =
            BTreeMap::from([(token_old_only.clone(), 1), (token_survives.clone(), 1)]);
        stale_state.payload.collect_buy_mints_prepass_complete = true;
        stale_state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&stale_state, stale_state.horizon_end)?,
        )?;

        let (mut carried, restore_outcome) = discovery
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                next_window_start,
                next_metrics_window_start,
                rollover_now,
            )?;
        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow
        );
        assert_eq!(
            carried.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileExpiredHead
        );

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut carried,
            10,
            10,
            Instant::now() + StdDuration::from_secs(1),
        )?;

        assert!(
            phase_advance.source_exhausted,
            "grouped delta reconcile should finish the carry-forward collect_buy_mints phase in bounded pages on a large noisy fixture"
        );
        assert_eq!(phase_advance.pages_processed, 2);
        assert_eq!(phase_advance.rows_processed, 3);
        assert_eq!(phase_advance.unique_buy_mints_discovered, 2);
        assert!(
            phase_advance.rows_processed
                < expired_head_noise_rows.saturating_add(new_tail_noise_rows),
            "carry-forward grouped reconcile should process buy-mint delta groups, not every raw swap in the expired/new tail windows"
        );
        assert_eq!(
            carried.payload.unique_buy_mints,
            vec![token_new_tail_a, token_new_tail_b, token_survives]
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_resumes_across_cycles_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-bounded-persisted-stream-resume.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_summary = discovery.run_cycle(&store, now)?;
        assert_eq!(first_summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        let first_progress = store
            .load_discovery_persisted_rebuild_state()?
            .expect("first cycle should persist bounded rebuild progress");

        let second_summary = discovery.run_cycle(&store, now + Duration::minutes(1))?;
        assert_eq!(
            second_summary.runtime_mode,
            DiscoveryRuntimeMode::FailClosed
        );
        let second_progress = store
            .load_discovery_persisted_rebuild_state()?
            .expect("second cycle should keep persisted rebuild progress");

        assert_eq!(second_progress.window_start, first_progress.window_start);
        assert_eq!(second_progress.horizon_end, first_progress.horizon_end);
        assert_eq!(
            second_progress.metrics_window_start,
            first_progress.metrics_window_start
        );
        assert!(
            second_progress.prepass_rows_processed > first_progress.prepass_rows_processed,
            "next cycle must advance the bounded prepass instead of restarting from zero"
        );
        assert_eq!(
            second_progress.chunks_completed,
            first_progress.chunks_completed + 1
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_resumes_after_restart_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-bounded-persisted-stream-restart.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;

        let discovery_first = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let _ = discovery_first.run_cycle(&store, now)?;
        let first_progress = store
            .load_discovery_persisted_rebuild_state()?
            .expect("first cycle should persist rebuild progress");

        let discovery_after_restart = DiscoveryService::new(config, permissive_shadow_quality());
        let second_summary =
            discovery_after_restart.run_cycle(&store, now + Duration::minutes(1))?;
        assert_eq!(
            second_summary.runtime_mode,
            DiscoveryRuntimeMode::FailClosed
        );
        let second_progress = store
            .load_discovery_persisted_rebuild_state()?
            .expect("restart cycle should restore and advance rebuild progress");

        assert_eq!(second_progress.window_start, first_progress.window_start);
        assert_eq!(second_progress.horizon_end, first_progress.horizon_end);
        assert!(
            second_progress.prepass_rows_processed > first_progress.prepass_rows_processed,
            "restart must continue from the persisted checkpoint instead of replaying from zero"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_collect_buy_mints_migrates_legacy_raw_cursor_to_safe_prefix_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-collect-buy-mints-legacy-cursor-migration.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (window_start, ordered_mints) =
            seed_stage1_collect_buy_mints_legacy_migration_fixture(&store, &config, now)?;
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let mut legacy_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        legacy_state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        legacy_state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: window_start + Duration::minutes(1),
            slot: 3,
            signature: "legacy-raw-swap-cursor".to_string(),
        });
        legacy_state.prepass_rows_processed = 1_700_000;
        legacy_state.prepass_pages_processed = 17;
        legacy_state.chunks_completed = 17;
        legacy_state.payload.unique_buy_mints = vec![
            ordered_mints[3].clone(),
            ordered_mints[0].clone(),
            ordered_mints[1].clone(),
        ];
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&legacy_state, now)?,
        )?;

        let outcome = discovery.advance_persisted_stream_rebuild(
            &store,
            window_start,
            metrics_window_start,
            now + Duration::minutes(1),
            1,
            1,
            StdDuration::from_secs(1),
        )?;
        assert!(matches!(
            outcome,
            PersistedStreamRebuildAdvanceOutcome::InProgress { .. }
        ));
        let migrated = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            migrated.phase,
            DiscoveryPersistedRebuildPhase::CollectBuyMints
        );
        assert_eq!(migrated.chunks_completed, 18);
        assert!(
            migrated.prepass_rows_processed > legacy_state.prepass_rows_processed,
            "legacy collect_buy_mints checkpoints must keep advancing instead of restarting from zero"
        );
        assert!(
            migrated.phase_cursor.is_none(),
            "legacy raw-swap cursor should be replaced by the direct distinct-mint cursor"
        );
        assert_eq!(
            migrated.payload.collect_buy_mints_cursor_token.as_deref(),
            Some(ordered_mints[2].as_str()),
            "legacy migration must recover the safe sorted prefix and persist the next direct distinct-mint cursor"
        );
        assert_eq!(
            migrated.payload.unique_buy_mints,
            ordered_mints[..3].to_vec(),
            "legacy migration must normalize to the canonical sorted prefix and drop unsafe tail mints until they are rediscovered in order"
        );

        let next = discovery.advance_persisted_stream_rebuild(
            &store,
            window_start,
            metrics_window_start,
            now + Duration::minutes(2),
            1,
            1,
            StdDuration::from_secs(1),
        )?;
        assert!(matches!(
            next,
            PersistedStreamRebuildAdvanceOutcome::InProgress { .. }
        ));
        let resumed = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            resumed.payload.unique_buy_mints,
            ordered_mints[..4].to_vec(),
            "the next cycle must continue from the persisted direct distinct cursor instead of starting the sorted pagination from zero again"
        );
        assert_eq!(
            resumed.payload.collect_buy_mints_cursor_token.as_deref(),
            Some(ordered_mints[3].as_str()),
            "the resumed collect_buy_mints cursor must stay on the rediscovered sorted tail instead of rewinding to the start"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_collect_buy_mints_legacy_migration_preserves_canonical_order_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-collect-buy-mints-legacy-order-parity.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (window_start, ordered_mints) =
            seed_stage1_collect_buy_mints_legacy_migration_fixture(&store, &config, now)?;
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let mut legacy_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        legacy_state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        legacy_state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: window_start + Duration::minutes(1),
            slot: 3,
            signature: "legacy-raw-swap-cursor".to_string(),
        });
        legacy_state.payload.unique_buy_mints = vec![
            ordered_mints[3].clone(),
            ordered_mints[0].clone(),
            ordered_mints[1].clone(),
        ];
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&legacy_state, now)?,
        )?;

        let rebuild_time_budget = StdDuration::from_secs(1);
        for idx in 0..4 {
            let cycle_now = now + Duration::minutes(idx as i64 + 1);
            let _ = discovery.advance_persisted_stream_rebuild(
                &store,
                window_start,
                metrics_window_start,
                cycle_now,
                1,
                1,
                rebuild_time_budget,
            )?;
        }

        let repaired = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            repaired.phase,
            DiscoveryPersistedRebuildPhase::ResolveTokenQuality
        );
        assert_eq!(
            repaired.payload.unique_buy_mints,
            store.load_observed_buy_mints_in_window(window_start, now)?,
            "legacy collect_buy_mints migration must feed token-quality resolution with the exact canonical one-shot mint order"
        );
        assert_eq!(repaired.payload.unique_buy_mints, ordered_mints);
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_repairs_noncanonical_quality_checkpoint_before_resume_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-noncanonical-quality-checkpoint-repair.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (window_start, ordered_mints) =
            seed_stage1_collect_buy_mints_legacy_migration_fixture(&store, &config, now)?;
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let mut state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        state.phase = DiscoveryPersistedRebuildPhase::ResolveTokenQuality;
        state.payload.unique_buy_mints = vec![
            ordered_mints[3].clone(),
            ordered_mints[0].clone(),
            ordered_mints[1].clone(),
        ];
        state.payload.token_quality_progress.next_mint_index = 2;
        state.payload.token_quality_cache.insert(
            ordered_mints[3].clone(),
            quality_cache::TokenQualityResolution::Missing,
        );
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, now)?,
        )?;

        let outcome = discovery.advance_persisted_stream_rebuild(
            &store,
            window_start,
            metrics_window_start,
            now + Duration::minutes(1),
            1,
            1,
            StdDuration::from_secs(1),
        )?;
        assert!(matches!(
            outcome,
            PersistedStreamRebuildAdvanceOutcome::InProgress { .. }
        ));
        let repaired = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            repaired.phase,
            DiscoveryPersistedRebuildPhase::ResolveTokenQuality
        );
        assert_eq!(
            repaired.payload.unique_buy_mints,
            vec![
                ordered_mints[0].clone(),
                ordered_mints[1].clone(),
                ordered_mints[3].clone(),
            ],
            "resume repair must canonicalize non-collect checkpoints onto sorted buy-mint order"
        );
        assert_eq!(
            repaired.payload.token_quality_progress.next_mint_index,
            1,
            "resume repair must rewind token-quality positional progress before consuming the canonical order"
        );
        assert_eq!(repaired.payload.token_quality_cache.len(), 1);
        assert!(repaired
            .payload
            .token_quality_cache
            .contains_key(&ordered_mints[0]));
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_completes_to_healthy_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-bounded-persisted-stream-complete.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (_, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let mut final_summary = None;
        for idx in 0..20 {
            let cycle_now = now + Duration::minutes(idx as i64);
            let summary = discovery.run_cycle(&store, cycle_now)?;
            if summary.runtime_mode == DiscoveryRuntimeMode::Healthy {
                final_summary = Some(summary);
                break;
            }
        }

        let summary = final_summary.expect("bounded rebuild should complete within 20 cycles");
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(summary.scoring_source, "raw_window_persisted_stream");
        assert!(summary.metrics_written > 0);
        assert!(summary.active_follow_wallets > 0);
        assert_eq!(
            store.latest_wallet_metrics_window_start()?,
            Some(metrics_window_start)
        );
        assert_eq!(
            store.list_active_follow_wallets()?,
            HashSet::from([String::from("wallet_top")])
        );
        assert!(
            store.load_discovery_persisted_rebuild_state()?.is_none(),
            "completed rebuild must clear its durable progress row"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_matches_one_shot_semantics_across_chunk_boundaries_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-bounded-persisted-stream-parity.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut reference_config = stage1_runtime_config();
        reference_config.metric_snapshot_interval_seconds = 3_600;
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &reference_config, now, 2, 3)?;

        let reference_discovery =
            DiscoveryService::new(reference_config.clone(), permissive_shadow_quality());
        let (reference_snapshots, reference_observed_swaps_loaded) = reference_discovery
            .build_wallet_snapshots_from_persisted_stream_one_shot(&store, window_start, now)?;
        assert!(
            reference_observed_swaps_loaded > 0,
            "reference one-shot rebuild must observe persisted swaps"
        );
        let reference_ranked =
            rank_follow_candidates(&reference_snapshots, reference_config.min_score);
        let expected_active_wallets: std::collections::HashSet<_> =
            desired_wallets(&reference_ranked, reference_config.follow_top_n)
                .into_iter()
                .collect();

        let mut bounded_config = reference_config.clone();
        bounded_config.max_fetch_swaps_per_cycle = 1;
        bounded_config.max_fetch_pages_per_cycle = 1;
        bounded_config.fetch_time_budget_ms = 60_000;

        let bounded_discovery =
            DiscoveryService::new(bounded_config.clone(), permissive_shadow_quality());
        let rebuild_time_budget =
            StdDuration::from_millis(bounded_config.fetch_time_budget_ms.max(1));
        let mut saw_partial = false;
        let mut completed_snapshots = None;
        for idx in 0..30 {
            let cycle_now = now + Duration::minutes(idx as i64);
            match bounded_discovery.advance_persisted_stream_rebuild(
                &store,
                window_start,
                metrics_window_start,
                cycle_now,
                bounded_config.max_fetch_swaps_per_cycle.max(1),
                bounded_config.max_fetch_pages_per_cycle.max(1),
                rebuild_time_budget,
            )? {
                PersistedStreamRebuildAdvanceOutcome::Completed {
                    snapshots,
                    telemetry,
                } => {
                    assert!(telemetry.completed);
                    completed_snapshots = Some(snapshots);
                    break;
                }
                PersistedStreamRebuildAdvanceOutcome::InProgress { telemetry } => {
                    assert!(telemetry.partial);
                    saw_partial = true;
                    assert!(
                        store.load_discovery_persisted_rebuild_state()?.is_some(),
                        "partial bounded rebuild must persist state between chunks"
                    );
                }
            }
        }
        assert!(
            saw_partial,
            "chunk-boundary parity test must exercise multi-cycle partial rebuild"
        );
        let publish_pending = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            publish_pending.phase,
            DiscoveryPersistedRebuildPhase::PublishPending
        );
        let bounded_snapshots =
            completed_snapshots.expect("bounded rebuild should complete within 30 cycles");
        let bounded_ranked = rank_follow_candidates(&bounded_snapshots, bounded_config.min_score);
        let actual_active_wallets: std::collections::HashSet<_> =
            desired_wallets(&bounded_ranked, bounded_config.follow_top_n)
                .into_iter()
                .collect();
        assert_eq!(actual_active_wallets, expected_active_wallets);

        assert_eq!(bounded_snapshots.len(), reference_snapshots.len());
        let reference_by_wallet: std::collections::HashMap<_, _> = reference_snapshots
            .into_iter()
            .map(|snapshot| (snapshot.wallet_id.clone(), snapshot))
            .collect();
        for snapshot in bounded_snapshots {
            let reference = reference_by_wallet
                .get(&snapshot.wallet_id)
                .expect("bounded rebuild must preserve the same wallet set as one-shot rebuild");
            assert_eq!(snapshot.trades, reference.trades);
            assert_eq!(snapshot.closed_trades, reference.closed_trades);
            assert_eq!(snapshot.buy_total, reference.buy_total);
            assert!(
                (snapshot.score - reference.score).abs() < 1e-9,
                "bounded rebuild must preserve score parity for {}",
                snapshot.wallet_id
            );
            assert!(
                (snapshot.tradable_ratio - reference.tradable_ratio).abs() < 1e-9,
                "bounded rebuild must preserve tradable-ratio parity for {}",
                snapshot.wallet_id
            );
            assert!(
                (snapshot.rug_ratio - reference.rug_ratio).abs() < 1e-9,
                "bounded rebuild must preserve rug-ratio parity for {}",
                snapshot.wallet_id
            );
        }
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_optimized_reduces_heavy_rows_on_large_noise_fixture_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-replay-optimized-noise-throughput.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-18T13:54:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (window_start, metrics_window_start, total_rows, sol_leg_rows) =
            seed_stage1_replay_noise_fixture(&store, &config, now, 24, 720)?;
        assert!(sol_leg_rows < total_rows);
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let mut legacy_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        legacy_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        legacy_state.horizon_end = now;
        legacy_state.payload.unique_buy_mints =
            store.load_observed_buy_mints_in_window(window_start, now)?;
        legacy_state.payload.replay_mode = ReplayMode::LegacyFullWindow;

        let mut optimized_state = legacy_state.clone();
        optimized_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        optimized_state.payload.replay_wallet_stats_complete = false;

        let legacy_advance = discovery.advance_persisted_stream_replay_legacy(
            &store,
            &mut legacy_state,
            total_rows.saturating_add(16),
            total_rows.saturating_add(16),
            Instant::now() + StdDuration::from_secs(5),
        )?;
        let optimized_advance = discovery.advance_persisted_stream_replay_optimized(
            &store,
            &mut optimized_state,
            total_rows.saturating_add(16),
            total_rows.saturating_add(16),
            Instant::now() + StdDuration::from_secs(5),
        )?;

        assert!(legacy_advance.source_exhausted);
        assert!(optimized_advance.source_exhausted);
        assert_eq!(legacy_advance.rows_processed, total_rows);
        assert_eq!(
            optimized_advance.replay_wallet_stats_rows_processed,
            total_rows
        );
        assert_eq!(optimized_advance.rows_processed, sol_leg_rows);
        assert!(
            optimized_advance.rows_processed < legacy_advance.rows_processed,
            "optimized replay should only apply later-phase streaming state to SOL-leg swaps after exact wallet stats were buffered separately"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_legacy_checkpoint_rewinds_to_optimized_mode_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-replay-legacy-upgrade-repair.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-18T13:54:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (window_start, metrics_window_start, _, _) =
            seed_stage1_replay_noise_fixture(&store, &config, now, 6, 120)?;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

        let mut legacy_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        legacy_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        legacy_state.horizon_end = now;
        legacy_state.prepass_rows_processed = 6;
        legacy_state.prepass_pages_processed = 2;
        legacy_state.payload.unique_buy_mints =
            store.load_observed_buy_mints_in_window(window_start, now)?;
        legacy_state.payload.token_quality_cache.insert(
            legacy_state.payload.unique_buy_mints[0].clone(),
            quality_cache::TokenQualityResolution::Deferred,
        );
        legacy_state.payload.replay_mode = ReplayMode::LegacyFullWindow;
        legacy_state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: window_start + Duration::minutes(10),
            slot: 42,
            signature: "legacy-replay-cursor".to_string(),
        });
        legacy_state.replay_rows_processed = 111;
        legacy_state.replay_pages_processed = 7;
        legacy_state.payload.by_wallet.insert(
            "wallet_partial".to_string(),
            WalletAccumulator {
                trades: 11,
                ..WalletAccumulator::default()
            },
        );
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&legacy_state, now)?,
        )?;

        let (repaired, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            window_start,
            metrics_window_start,
            now,
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(repaired.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert_eq!(
            repaired.payload.replay_mode,
            ReplayMode::WalletStatsThenSolLeg
        );
        assert!(!repaired.payload.replay_wallet_stats_complete);
        assert_eq!(repaired.replay_rows_processed, 0);
        assert_eq!(repaired.replay_pages_processed, 0);
        assert_eq!(repaired.phase_cursor, None);
        assert!(repaired.payload.by_wallet.is_empty());
        assert_eq!(
            repaired.prepass_rows_processed,
            legacy_state.prepass_rows_processed
        );
        assert!(
            repaired
                .payload
                .token_quality_cache
                .contains_key(&legacy_state.payload.unique_buy_mints[0]),
            "rewinding legacy replay progress must preserve the resolved token-quality cache"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_zero_progress_legacy_checkpoint_upgrades_to_optimized_mode_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-replay-zero-progress-legacy-upgrade.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-18T13:54:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (window_start, metrics_window_start, _, _) =
            seed_stage1_replay_noise_fixture(&store, &config, now, 6, 120)?;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

        let mut legacy_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        legacy_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        legacy_state.horizon_end = now;
        legacy_state.prepass_rows_processed = 6;
        legacy_state.prepass_pages_processed = 2;
        legacy_state.payload.unique_buy_mints =
            store.load_observed_buy_mints_in_window(window_start, now)?;
        legacy_state.payload.token_quality_cache.insert(
            legacy_state.payload.unique_buy_mints[0].clone(),
            quality_cache::TokenQualityResolution::Deferred,
        );
        legacy_state.payload.replay_mode = ReplayMode::LegacyFullWindow;
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&legacy_state, now)?,
        )?;

        let (repaired, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            window_start,
            metrics_window_start,
            now,
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(repaired.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert_eq!(
            repaired.payload.replay_mode,
            ReplayMode::WalletStatsThenSolLeg
        );
        assert!(!repaired.payload.replay_wallet_stats_complete);
        assert_eq!(repaired.payload.replay_wallet_stats_rows_processed, 0);
        assert_eq!(repaired.payload.replay_wallet_stats_pages_processed, 0);
        assert_eq!(repaired.replay_rows_processed, 0);
        assert_eq!(repaired.replay_pages_processed, 0);
        assert_eq!(repaired.phase_cursor, None);
        assert!(repaired.payload.by_wallet.is_empty());
        assert!(repaired.payload.token_states.is_empty());
        assert!(repaired.payload.token_recent_sol_trades.is_empty());
        assert!(repaired.payload.pending_rug_checks.is_empty());
        assert!(repaired.payload.token_pending_buy_starts.is_empty());
        assert_eq!(
            repaired.payload.unique_buy_mints,
            legacy_state.payload.unique_buy_mints
        );
        assert_eq!(
            repaired.prepass_rows_processed,
            legacy_state.prepass_rows_processed
        );
        assert!(
            repaired
                .payload
                .token_quality_cache
                .contains_key(&legacy_state.payload.unique_buy_mints[0]),
            "upgrading a zero-progress legacy replay checkpoint must preserve the resolved token-quality cache"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_optimized_resumes_after_restart_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-replay-optimized-restart.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-18T13:54:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (window_start, metrics_window_start, total_rows, _) =
            seed_stage1_replay_noise_fixture(&store, &config, now, 6, 120)?;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

        let mut state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.horizon_end = now;
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.unique_buy_mints =
            store.load_observed_buy_mints_in_window(window_start, now)?;

        let stats_advance = discovery.advance_persisted_stream_replay_optimized(
            &store,
            &mut state,
            total_rows.saturating_add(16),
            1,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        state.payload.replay_wallet_stats_rows_processed = state
            .payload
            .replay_wallet_stats_rows_processed
            .saturating_add(stats_advance.replay_wallet_stats_rows_processed);
        state.payload.replay_wallet_stats_pages_processed = state
            .payload
            .replay_wallet_stats_pages_processed
            .saturating_add(stats_advance.replay_wallet_stats_pages_processed);
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .merge(stats_advance.replay_wallet_stats_day_count_source_progress);
        state.replay_rows_processed = state
            .replay_rows_processed
            .saturating_add(stats_advance.rows_processed);
        state.replay_pages_processed = state
            .replay_pages_processed
            .saturating_add(stats_advance.pages_processed);
        state.phase_cursor = stats_advance.phase_cursor;
        assert!(state.payload.replay_wallet_stats_complete);

        let replay_advance = discovery.advance_persisted_stream_replay_optimized(
            &store,
            &mut state,
            1,
            1,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        state.payload.replay_wallet_stats_rows_processed = state
            .payload
            .replay_wallet_stats_rows_processed
            .saturating_add(replay_advance.replay_wallet_stats_rows_processed);
        state.payload.replay_wallet_stats_pages_processed = state
            .payload
            .replay_wallet_stats_pages_processed
            .saturating_add(replay_advance.replay_wallet_stats_pages_processed);
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .merge(replay_advance.replay_wallet_stats_day_count_source_progress);
        state.replay_rows_processed = state
            .replay_rows_processed
            .saturating_add(replay_advance.rows_processed);
        state.replay_pages_processed = state
            .replay_pages_processed
            .saturating_add(replay_advance.pages_processed);
        state.phase_cursor = replay_advance.phase_cursor.clone();
        assert!(state.replay_rows_processed > 0);
        assert!(state.phase_cursor.is_some());

        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, now)?,
        )?;

        let discovery_after_restart =
            DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (resumed, restore_outcome) = discovery_after_restart
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                window_start,
                metrics_window_start,
                now,
            )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(resumed.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert_eq!(
            resumed.payload.replay_mode,
            ReplayMode::WalletStatsThenSolLeg
        );
        assert!(resumed.payload.replay_wallet_stats_complete);
        assert_eq!(
            resumed.payload.replay_wallet_stats_rows_processed,
            state.payload.replay_wallet_stats_rows_processed
        );
        assert_eq!(
            resumed
                .payload
                .replay_wallet_stats_day_count_source_progress,
            state.payload.replay_wallet_stats_day_count_source_progress
        );
        assert_eq!(resumed.replay_rows_processed, state.replay_rows_processed);
        assert_eq!(resumed.phase_cursor, state.phase_cursor);
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_wallet_stats_sql_summary_matches_activity_scan_stage1() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-replay-wallet-stats-sql-summary.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-20T06:45:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = bounded_stage1_runtime_config();
        config.max_tx_per_minute = 3;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);

        store.insert_observed_swap(&swap(
            "wallet-replay-a",
            "wallet-replay-a-1",
            window_start + Duration::hours(1),
            SOL_MINT,
            "TokenReplayA1111111111111111111111111111111",
            1.0,
            10.0,
        ))?;
        store.insert_observed_swap(&swap(
            "wallet-replay-a",
            "wallet-replay-a-2",
            window_start + Duration::days(1) + Duration::hours(2),
            "TokenReplayA1111111111111111111111111111111",
            SOL_MINT,
            10.0,
            1.2,
        ))?;
        store.insert_observed_swap(&swap(
            "wallet-replay-b",
            "wallet-replay-b-1",
            window_start + Duration::hours(3),
            SOL_MINT,
            "TokenReplayB1111111111111111111111111111111",
            1.0,
            10.0,
        ))?;
        for idx in 0..4 {
            store.insert_observed_swap(&swap(
                "wallet-replay-b",
                &format!("wallet-replay-b-burst-{idx}"),
                window_start + Duration::hours(4),
                if idx % 2 == 0 {
                    SOL_MINT
                } else {
                    "TokenReplayB1111111111111111111111111111111"
                },
                if idx % 2 == 0 {
                    "TokenReplayBurst1111111111111111111111111"
                } else {
                    SOL_MINT
                },
                1.0,
                1.0,
            ))?;
        }
        store.insert_observed_swap(&swap(
            "wallet-replay-c",
            "wallet-replay-c-1",
            window_start + Duration::days(2) + Duration::hours(1),
            "TokenReplayCIn11111111111111111111111111111",
            "TokenReplayCOut111111111111111111111111111",
            2.0,
            3.0,
        ))?;
        store.upsert_wallet_activity_days(&[
            WalletActivityDayRow {
                wallet_id: "wallet-replay-a".to_string(),
                activity_day: (window_start + Duration::hours(1)).date_naive(),
                last_seen: window_start + Duration::hours(1),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-replay-a".to_string(),
                activity_day: (window_start + Duration::days(1) + Duration::hours(2)).date_naive(),
                last_seen: window_start + Duration::days(1) + Duration::hours(2),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-replay-b".to_string(),
                activity_day: (window_start + Duration::hours(4)).date_naive(),
                last_seen: window_start + Duration::hours(4),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-replay-c".to_string(),
                activity_day: (window_start + Duration::days(2) + Duration::hours(1)).date_naive(),
                last_seen: window_start + Duration::days(2) + Duration::hours(1),
            },
        ])?;

        let mut replay_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        replay_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        replay_state.horizon_end = now;
        replay_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        replay_state.payload.unique_buy_mints =
            store.load_observed_buy_mints_in_window(window_start, now)?;

        loop {
            let advance = discovery.advance_persisted_stream_replay_wallet_stats(
                &store,
                &mut replay_state,
                2,
                2,
                Instant::now() + StdDuration::from_secs(5),
            )?;
            replay_state.payload.replay_wallet_stats_rows_processed = replay_state
                .payload
                .replay_wallet_stats_rows_processed
                .saturating_add(advance.replay_wallet_stats_rows_processed);
            replay_state.payload.replay_wallet_stats_pages_processed = replay_state
                .payload
                .replay_wallet_stats_pages_processed
                .saturating_add(advance.replay_wallet_stats_pages_processed);
            replay_state
                .payload
                .replay_wallet_stats_day_count_source_progress
                .merge(advance.replay_wallet_stats_day_count_source_progress);
            if advance.source_exhausted {
                break;
            }
            assert!(
                advance.budget_exhausted_reason.is_some(),
                "paged wallet-stats replay should remain bounded until the final summary page"
            );
        }

        let mut reference_by_wallet: HashMap<String, WalletAccumulator> = HashMap::new();
        let reference_page = store.for_each_observed_swap_in_window_after_cursor_with_budget(
            window_start,
            now,
            None,
            10_000,
            Instant::now() + StdDuration::from_secs(5),
            |swap| {
                reference_by_wallet
                    .entry(swap.wallet.clone())
                    .or_default()
                    .observe_activity_only(&swap, config.max_tx_per_minute);
                Ok(())
            },
        )?;
        assert!(!reference_page.time_budget_exhausted);

        assert_eq!(
            replay_state.payload.replay_wallet_stats_wallet_cursor, None,
            "wallet cursor must clear after exact wallet-stats summary completion"
        );
        assert_eq!(
            replay_state.payload.by_wallet.len(),
            reference_by_wallet.len()
        );
        assert!(
            replay_state
                .payload
                .replay_wallet_stats_day_count_source_progress
                .fast_path_pages_processed
                > 0,
            "wallet-stats replay should expose wallet_activity_days fast-path usage on the nominal path"
        );
        assert_eq!(
            replay_state
                .payload
                .replay_wallet_stats_day_count_source_progress
                .fallback_pages_processed,
            0,
            "wallet-stats replay should not report raw fallback usage when auxiliary day counts are complete"
        );
        for (wallet_id, reference) in reference_by_wallet {
            let actual = replay_state
                .payload
                .by_wallet
                .get(&wallet_id)
                .with_context(|| {
                    format!("missing replay wallet activity summary for {wallet_id}")
                })?;
            assert_eq!(actual.first_seen, reference.first_seen);
            assert_eq!(actual.last_seen, reference.last_seen);
            assert_eq!(actual.trades, reference.trades);
            assert_eq!(actual.suspicious, reference.suspicious);
            assert_eq!(
                actual
                    .exact_active_day_count
                    .unwrap_or(actual.active_days.len() as u32),
                reference.active_days.len() as u32
            );
        }
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_wallet_stats_width_is_wider_than_sol_leg_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-replay-wallet-stats-width.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-20T06:45:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = bounded_stage1_runtime_config();
        config.max_fetch_swaps_per_cycle = 1;
        config.max_fetch_pages_per_cycle = 1;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (window_start, metrics_window_start, _, _) =
            seed_stage1_replay_noise_fixture(&store, &config, now, 3, 6)?;
        let unique_buy_mints = store.load_observed_buy_mints_in_window(window_start, now)?;

        let mut wallet_stats_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        wallet_stats_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        wallet_stats_state.horizon_end = now;
        wallet_stats_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        wallet_stats_state.payload.unique_buy_mints = unique_buy_mints.clone();

        let wallet_stats_advance = discovery.advance_persisted_stream_replay_optimized(
            &store,
            &mut wallet_stats_state,
            1,
            1,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert_eq!(
            wallet_stats_advance.replay_wallet_stats_pages_processed, 2,
            "wallet-stats prepass should get a replay-only widened page budget"
        );
        assert!(wallet_stats_advance.replay_wallet_stats_rows_processed > 0);
        assert!(
            wallet_stats_advance
                .replay_wallet_stats_day_count_source_progress
                .fast_path_pages_processed
                + wallet_stats_advance
                    .replay_wallet_stats_day_count_source_progress
                    .fallback_pages_processed
                > 0
        );
        assert_eq!(wallet_stats_advance.rows_processed, 0);
        assert_eq!(
            wallet_stats_advance.budget_exhausted_reason,
            Some(PersistedStreamBudgetExhaustedReason::PageBudget)
        );

        let mut sol_leg_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        sol_leg_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        sol_leg_state.horizon_end = now;
        sol_leg_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        sol_leg_state.payload.replay_wallet_stats_complete = true;
        sol_leg_state.payload.unique_buy_mints = unique_buy_mints;

        let sol_leg_advance = discovery.advance_persisted_stream_replay_optimized(
            &store,
            &mut sol_leg_state,
            1,
            1,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert_eq!(
            sol_leg_advance.pages_processed, 1,
            "SOL-leg replay should still respect the base page budget"
        );
        assert_eq!(sol_leg_advance.replay_wallet_stats_pages_processed, 0);
        assert_eq!(
            sol_leg_advance.budget_exhausted_reason,
            Some(PersistedStreamBudgetExhaustedReason::PageBudget)
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_publish_failure_keeps_publish_pending_checkpoint_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-publish-pending.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let rebuild_time_budget = StdDuration::from_millis(config.fetch_time_budget_ms.max(1));

        for idx in 0..30 {
            let cycle_now = now + Duration::minutes(idx as i64);
            match discovery.advance_persisted_stream_rebuild(
                &store,
                window_start,
                metrics_window_start,
                cycle_now,
                config.max_fetch_swaps_per_cycle.max(1),
                config.max_fetch_pages_per_cycle.max(1),
                rebuild_time_budget,
            )? {
                PersistedStreamRebuildAdvanceOutcome::Completed { .. } => break,
                PersistedStreamRebuildAdvanceOutcome::InProgress { .. } => {}
            }
        }

        let publish_pending_before_failure = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            publish_pending_before_failure.phase,
            DiscoveryPersistedRebuildPhase::PublishPending
        );
        assert!(
            !publish_pending_before_failure
                .payload
                .completed_snapshots
                .is_empty(),
            "publish-pending checkpoint must persist completed snapshots"
        );

        let conn = Connection::open(Path::new(&db_path))?;
        conn.execute_batch(
            "CREATE TRIGGER fail_wallet_metrics_insert
             BEFORE INSERT ON wallet_metrics
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = discovery
            .run_cycle(&store, now + Duration::minutes(1))
            .expect_err("publish failure should bubble up");
        assert!(
            format!("{error:#}").contains("disk I/O error"),
            "unexpected publish failure: {error:#}"
        );

        let publish_pending_after_failure = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            publish_pending_after_failure.phase,
            DiscoveryPersistedRebuildPhase::PublishPending
        );
        assert_eq!(
            publish_pending_after_failure.replay_rows_processed,
            publish_pending_before_failure.replay_rows_processed
        );
        assert_eq!(
            publish_pending_after_failure.window_start,
            publish_pending_before_failure.window_start
        );
        assert_eq!(
            publish_pending_after_failure.metrics_window_start,
            publish_pending_before_failure.metrics_window_start
        );

        conn.execute_batch("DROP TRIGGER fail_wallet_metrics_insert;")?;
        let summary = discovery.run_cycle(&store, now + Duration::minutes(2))?;
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(summary.scoring_source, "raw_window_persisted_stream");
        assert!(
            store.load_discovery_persisted_rebuild_state()?.is_none(),
            "successful publish must clear the publish-pending checkpoint"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_carries_forward_partial_collect_buy_mints_across_metrics_bucket_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-persisted-stream-stale-bucket.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let checkpoint_now = DateTime::parse_from_rfc3339("2026-03-17T12:00:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let rollover_now = checkpoint_now + Duration::seconds(20);
        let window_start =
            checkpoint_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, checkpoint_now);
        let next_window_start =
            rollover_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let next_metrics_window_start = metrics_window_start_for_test(&config, rollover_now);

        let token_old_only = "TokenStage1CarryForwardAOldOnly111111111111".to_string();
        let token_survives = "TokenStage1CarryForwardBSurvives1111111111".to_string();
        let token_future_c = "TokenStage1CarryForwardCFuture11111111111".to_string();
        let token_future_d = "TokenStage1CarryForwardDFuture11111111111".to_string();
        let token_new_tail_before_cursor =
            "TokenStage1CarryForwardAANewTail1111111111111".to_string();

        for (idx, (token, ts)) in [
            (token_old_only.as_str(), window_start + Duration::seconds(5)),
            (
                token_survives.as_str(),
                window_start + Duration::seconds(70),
            ),
            (
                token_future_c.as_str(),
                window_start + Duration::seconds(80),
            ),
            (
                token_future_d.as_str(),
                window_start + Duration::seconds(90),
            ),
            (
                token_new_tail_before_cursor.as_str(),
                checkpoint_now + Duration::seconds(5),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            store.insert_observed_swap(&swap(
                "wallet_carry_forward",
                &format!("stage1-carry-forward-buy-{idx}"),
                ts,
                SOL_MINT,
                token,
                0.5,
                50.0,
            ))?;
        }

        let mut stale_state = discovery.start_persisted_stream_rebuild_state(
            window_start,
            metrics_window_start,
            checkpoint_now,
        );
        stale_state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        stale_state.prepass_rows_processed = 2;
        stale_state.prepass_pages_processed = 1;
        stale_state.payload.unique_buy_mints = vec![token_old_only.clone(), token_survives.clone()];
        stale_state.payload.buy_mint_counts =
            BTreeMap::from([(token_old_only.clone(), 1), (token_survives.clone(), 1)]);
        stale_state.payload.collect_buy_mints_cursor_token = Some(token_survives.clone());
        stale_state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&stale_state, stale_state.horizon_end)?,
        )?;

        let (mut carried, resumed_existing) = discovery
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                next_window_start,
                next_metrics_window_start,
                rollover_now,
            )?;
        assert_eq!(
            resumed_existing,
            PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow
        );
        assert_eq!(
            carried.phase,
            DiscoveryPersistedRebuildPhase::CollectBuyMints
        );
        assert_eq!(carried.window_start, next_window_start);
        assert_eq!(carried.metrics_window_start, next_metrics_window_start);
        assert_eq!(carried.horizon_end, rollover_now);
        assert_eq!(
            carried.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileExpiredHead
        );
        assert_eq!(
            carried.payload.collect_buy_mints_cursor_token.as_deref(),
            Some(token_survives.as_str())
        );

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut carried,
            1,
            10,
            Instant::now() + StdDuration::from_secs(1),
        )?;
        assert!(phase_advance.source_exhausted);
        assert_eq!(
            carried.payload.unique_buy_mints,
            store.load_observed_buy_mints_in_window(next_window_start, rollover_now)?,
            "carry-forward reconciliation must produce the exact canonical target-window mint set before token-quality resolution"
        );
        assert_eq!(
            carried.payload.unique_buy_mints,
            vec![
                token_new_tail_before_cursor,
                token_survives,
                token_future_c,
                token_future_d,
            ]
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_expired_head_stale_bucket_resumes_until_exact_checkpoint_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-expired-head-stale-bucket-resume.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-18T18:00:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_one_now = source_now + Duration::seconds(60);
        let target_two_now = target_one_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_one_window_start =
            target_one_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_one_metrics_window_start =
            metrics_window_start_for_test(&config, target_one_now);
        let target_two_window_start =
            target_two_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_two_metrics_window_start =
            metrics_window_start_for_test(&config, target_two_now);

        let token_expired_a = "TokenStage1StaleExpiredHeadA111111111111".to_string();
        let token_expired_b = "TokenStage1StaleExpiredHeadB111111111111".to_string();
        let token_survives = "TokenStage1StaleExpiredHeadSurvive11111".to_string();
        let token_new_tail = "TokenStage1StaleExpiredHeadNewTail11111".to_string();
        for (idx, (token, ts)) in [
            (
                token_expired_a.as_str(),
                source_window_start + Duration::seconds(5),
            ),
            (
                token_expired_b.as_str(),
                source_window_start + Duration::seconds(6),
            ),
            (
                token_survives.as_str(),
                target_one_window_start + Duration::seconds(5),
            ),
            (token_new_tail.as_str(), source_now + Duration::seconds(5)),
        ]
        .into_iter()
        .enumerate()
        {
            store.insert_observed_swap(&swap(
                "wallet_stage1_reconcile_expired",
                &format!("stage1-stale-expired-head-buy-{idx}"),
                ts,
                SOL_MINT,
                token,
                1.0,
                10.0,
            ))?;
        }

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.unique_buy_mints = vec![
            token_expired_a.clone(),
            token_expired_b.clone(),
            token_survives.clone(),
        ];
        state.payload.buy_mint_counts = BTreeMap::from([
            (token_expired_a, 1),
            (token_expired_b.clone(), 1),
            (token_survives.clone(), 1),
        ]);
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_one_window_start,
                target_one_metrics_window_start,
                target_one_now,
            )?
        );

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            1,
            1,
            Instant::now() + StdDuration::from_secs(1),
        )?;
        state.prepass_rows_processed = state
            .prepass_rows_processed
            .saturating_add(phase_advance.rows_processed);
        state.prepass_pages_processed = state
            .prepass_pages_processed
            .saturating_add(phase_advance.pages_processed);
        state.payload.collect_buy_mints_cursor_token =
            phase_advance.collect_buy_mints_cursor_token.clone();
        assert_eq!(
            state.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileExpiredHead
        );
        assert_eq!(
            state.payload.unique_buy_mints,
            vec![token_expired_b.clone(), token_survives.clone()],
            "partial stale expired-head reconcile should keep exact canonical membership incrementally without a full counts->vector rebuild"
        );
        assert_eq!(
            state.payload.buy_mint_counts.keys().cloned().collect::<Vec<_>>(),
            state.payload.unique_buy_mints,
            "authoritative buy-mint counts and resumable unique mint prefix must stay aligned after a partial stale reconcile page"
        );
        assert_sorted_strings(&state.payload.unique_buy_mints);
        assert!(
            DiscoveryService::state_can_resume_stale_metrics_window_until_exact_checkpoint(&state),
            "partial stale expired-head reconcile must remain eligible for stale-resume on the next bucket rollover"
        );
        assert!(state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor_token
            .is_some());
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, target_one_now)?,
        )?;

        let (resumed, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            target_two_window_start,
            target_two_metrics_window_start,
            target_two_now,
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
        );
        assert_eq!(
            resumed.metrics_window_start,
            target_one_metrics_window_start
        );
        assert_eq!(
            resumed.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileExpiredHead
        );
        assert_eq!(
            resumed
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor_token,
            state
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor_token
        );
        assert_eq!(resumed.prepass_rows_processed, state.prepass_rows_processed);
        Ok(())
    }

    #[test]
    fn persisted_stream_stale_reconcile_membership_stays_sorted_and_exact_without_full_resync_stage1(
    ) -> Result<()> {
        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let now = DateTime::parse_from_rfc3339("2026-03-18T22:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);

        let mut state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileExpiredHead;
        state.payload.collect_buy_mints_prepass_complete = true;
        for mint in [
            "TokenStage1IncrementalExactC1111111111111",
            "TokenStage1IncrementalExactA1111111111111",
            "TokenStage1IncrementalExactB1111111111111",
        ] {
            assert!(DiscoveryService::add_buy_mint_occurrences(
                &mut state.payload,
                mint,
                1,
            ));
        }
        assert_eq!(
            state.payload.unique_buy_mints,
            vec![
                "TokenStage1IncrementalExactA1111111111111".to_string(),
                "TokenStage1IncrementalExactB1111111111111".to_string(),
                "TokenStage1IncrementalExactC1111111111111".to_string(),
            ]
        );

        DiscoveryService::subtract_buy_mint_occurrences(
            &mut state.payload,
            "TokenStage1IncrementalExactB1111111111111",
            1,
        );
        assert_eq!(
            state.payload.unique_buy_mints,
            vec![
                "TokenStage1IncrementalExactA1111111111111".to_string(),
                "TokenStage1IncrementalExactC1111111111111".to_string(),
            ]
        );

        assert!(DiscoveryService::add_buy_mint_occurrences(
            &mut state.payload,
            "TokenStage1IncrementalExactB1111111111111",
            2,
        ));
        assert_eq!(
            state.payload.unique_buy_mints,
            vec![
                "TokenStage1IncrementalExactA1111111111111".to_string(),
                "TokenStage1IncrementalExactB1111111111111".to_string(),
                "TokenStage1IncrementalExactC1111111111111".to_string(),
            ]
        );
        assert_eq!(
            state
                .payload
                .buy_mint_counts
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            state.payload.unique_buy_mints
        );
        assert_sorted_strings(&state.payload.unique_buy_mints);
        assert!(
            DiscoveryService::state_can_resume_stale_metrics_window_until_exact_checkpoint(&state)
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_new_tail_stale_bucket_resumes_until_exact_checkpoint_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-new-tail-stale-bucket-resume.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-18T18:10:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_one_now = source_now + Duration::seconds(60);
        let target_two_now = target_one_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_one_window_start =
            target_one_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_one_metrics_window_start =
            metrics_window_start_for_test(&config, target_one_now);
        let target_two_window_start =
            target_two_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_two_metrics_window_start =
            metrics_window_start_for_test(&config, target_two_now);

        let token_survives = "TokenStage1StaleNewTailSurvive111111111".to_string();
        let token_new_tail_a = "TokenStage1StaleNewTailA1111111111111".to_string();
        let token_new_tail_b = "TokenStage1StaleNewTailB1111111111111".to_string();
        let token_future = "TokenStage1StaleNewTailFuture11111111".to_string();
        for (idx, (token, ts)) in [
            (
                token_survives.as_str(),
                target_one_window_start + Duration::seconds(5),
            ),
            (token_new_tail_a.as_str(), source_now + Duration::seconds(5)),
            (token_new_tail_b.as_str(), source_now + Duration::seconds(6)),
            (token_future.as_str(), target_one_now + Duration::seconds(5)),
        ]
        .into_iter()
        .enumerate()
        {
            store.insert_observed_swap(&swap(
                "wallet_stage1_reconcile_new_tail",
                &format!("stage1-stale-new-tail-buy-{idx}"),
                ts,
                SOL_MINT,
                token,
                1.0,
                10.0,
            ))?;
        }

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.unique_buy_mints = vec![token_survives.clone()];
        state.payload.buy_mint_counts = BTreeMap::from([(token_survives, 1)]);
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_one_window_start,
                target_one_metrics_window_start,
                target_one_now,
            )?
        );

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            1,
            2,
            Instant::now() + StdDuration::from_secs(1),
        )?;
        state.prepass_rows_processed = state
            .prepass_rows_processed
            .saturating_add(phase_advance.rows_processed);
        state.prepass_pages_processed = state
            .prepass_pages_processed
            .saturating_add(phase_advance.pages_processed);
        state.payload.collect_buy_mints_cursor_token =
            phase_advance.collect_buy_mints_cursor_token.clone();
        assert_eq!(
            state.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileNewTail
        );
        assert!(state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token
            .is_some());
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, target_one_now)?,
        )?;

        let (resumed, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            target_two_window_start,
            target_two_metrics_window_start,
            target_two_now,
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
        );
        assert_eq!(
            resumed.metrics_window_start,
            target_one_metrics_window_start
        );
        assert_eq!(
            resumed.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileNewTail
        );
        assert_eq!(
            resumed
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token,
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
        );
        assert_eq!(resumed.prepass_rows_processed, state.prepass_rows_processed);
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_new_tail_live_like_cycle_advances_exact_token_batches_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-new-tail-live-like-batch-advance.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-19T00:10:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_now = source_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_window_start =
            target_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_metrics_window_start = metrics_window_start_for_test(&config, target_now);

        let new_tail_token_count = STALE_RECONCILE_TOKEN_BATCH_CAP
            .saturating_mul(config.max_fetch_pages_per_cycle.max(1))
            .saturating_add(37);
        let survivor = "TokenStage1LiveLikeNewTailSurvivor11111111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_live_like_new_tail",
            "stage1-live-like-new-tail-survivor",
            target_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;
        let mut new_tail_tokens = Vec::new();
        for idx in 0..new_tail_token_count {
            let token = format!("TokenStage1LiveLikeNewTail{idx:05}1111111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_live_like_new_tail",
                &format!("stage1-live-like-new-tail-buy-{idx}"),
                source_now + Duration::seconds((idx % 30) as i64 + 1),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            new_tail_tokens.push(token);
        }
        let future_noise = "TokenStage1LiveLikeNewTailFuture1111111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_live_like_new_tail",
            "stage1-live-like-new-tail-future",
            target_now + Duration::seconds(5),
            SOL_MINT,
            &future_noise,
            1.0,
            10.0,
        ))?;

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = BTreeMap::from([(survivor.clone(), 1u32)]);
        state.payload.unique_buy_mints = vec![survivor.clone()];
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_window_start,
                target_metrics_window_start,
                target_now,
            )?
        );
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileNewTail;
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor = None;
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor_token = None;

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            Instant::now() + StdDuration::from_secs(5),
        )?;

        let expected_rows = STALE_RECONCILE_EXACT_COUNT_BATCH_CAP
            .saturating_mul(config.max_fetch_pages_per_cycle.max(1));
        assert_eq!(
            phase_advance.rows_processed,
            expected_rows,
            "live-like stale new-tail reconcile should advance by capped exact count sub-batches instead of letting one oversized exact candidate batch monopolize the cycle"
        );
        assert_eq!(
            phase_advance.pages_processed,
            config.max_fetch_pages_per_cycle
        );
        assert!(!phase_advance.source_exhausted);
        assert_eq!(
            state.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileNewTail
        );
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .as_deref(),
            new_tail_tokens
                .get(expected_rows.saturating_sub(1))
                .map(|token| token.as_str())
        );
        assert_eq!(
            state.payload.unique_buy_mints.len(),
            expected_rows.saturating_add(1),
            "new-tail reconcile should add the bounded candidate token batches while preserving the overlapping carried-forward mint"
        );
        assert_sorted_strings(&state.payload.unique_buy_mints);
        assert!(
            DiscoveryService::state_can_resume_stale_metrics_window_until_exact_checkpoint(&state),
            "batched live-like stale new-tail reconcile must stay exact and resumable after the first bounded cycle"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_new_tail_zero_row_timeout_narrows_slice_and_escapes_stall_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-new-tail-zero-row-stall.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 8;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-19T01:10:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_now = source_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_window_start =
            target_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_metrics_window_start = metrics_window_start_for_test(&config, target_now);

        let survivor = "TokenStage1StalledNewTailSurvivor11111111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_stalled_new_tail",
            "stage1-stalled-new-tail-survivor",
            target_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;
        let mut new_tail_tokens = Vec::new();
        for idx in 0..10usize {
            let token = format!("TokenStage1StalledNewTail{idx:05}1111111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_stalled_new_tail",
                &format!("stage1-stalled-new-tail-buy-{idx}"),
                source_now + Duration::seconds((idx % 10) as i64 + 1),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            new_tail_tokens.push(token);
        }

        let processed_prefix_len = 2usize;
        let processed_prefix = &new_tail_tokens[..processed_prefix_len];
        let stalled_tail = &new_tail_tokens[processed_prefix_len..];
        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = BTreeMap::from_iter(
            std::iter::once((survivor.clone(), 1u32))
                .chain(processed_prefix.iter().cloned().map(|token| (token, 1u32))),
        );
        state.payload.unique_buy_mints = state.payload.buy_mint_counts.keys().cloned().collect();
        assert_sorted_strings(&state.payload.unique_buy_mints);
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_window_start,
                target_metrics_window_start,
                target_now,
            )?
        );
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileNewTail;
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor = None;
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor_token = None;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token = processed_prefix.last().cloned();

        let original_cursor = state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token
            .clone()
            .expect("prefix cursor");
        arm_test_force_reconcile_new_tail_zero_row_timeout();
        let stalled_phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        state.prepass_rows_processed = state
            .prepass_rows_processed
            .saturating_add(stalled_phase_advance.rows_processed);
        state.prepass_pages_processed = state
            .prepass_pages_processed
            .saturating_add(stalled_phase_advance.pages_processed);
        assert_eq!(stalled_phase_advance.rows_processed, 0);
        assert_eq!(state.prepass_rows_processed, 0);
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .as_deref(),
            Some(original_cursor.as_str()),
            "zero-row timeout must not silently advance the stale cursor past uncounted candidate tokens"
        );
        let expected_narrowed_slice_end = stalled_tail
            .get((stalled_tail.len().saturating_sub(1)) / 2)
            .cloned();
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token,
            expected_narrowed_slice_end,
            "zero-row timeout must persist a narrowed exact token slice for retry instead of repeating the same wide stalled slice forever"
        );

        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, target_now)?,
        )?;
        let (mut resumed, restore_outcome) = discovery
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                target_window_start,
                target_metrics_window_start,
                target_now,
            )?;
        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(
            resumed
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token,
            Some(original_cursor.clone())
        );
        assert_eq!(
            resumed
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token,
            expected_narrowed_slice_end
        );

        let resumed_phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut resumed,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert!(
            resumed_phase_advance.rows_processed > 0,
            "after narrowing the exact token slice, the next bounded cycle must escape the zero-row stall and make durable progress"
        );
        assert!(
            resumed_phase_advance.source_exhausted
                || resumed
                    .payload
                    .collect_buy_mints_reconcile_new_tail_cursor_token
                    .as_deref()
                    > Some(original_cursor.as_str()),
            "resumed stale new-tail reconcile must either advance beyond the previously pinned cursor or finish the remaining stale tail work in the same bounded cycle"
        );
        assert!(
            resumed
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token
                .is_none(),
            "successful retry after a narrowed stale slice should clear the temporary slice cap"
        );
        assert!(
            DiscoveryService::state_can_resume_stale_metrics_window_until_exact_checkpoint(
                &resumed
            ),
            "narrowed-slice stall recovery must preserve exact carry-forward truth and stale-resume safety"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_new_tail_pending_exact_batch_survives_rollover_and_finishes_batch_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-new-tail-pending-batch-rollover.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 1;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-19T02:10:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_one_now = source_now + Duration::seconds(60);
        let target_two_now = target_one_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_one_window_start =
            target_one_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_one_metrics_window_start =
            metrics_window_start_for_test(&config, target_one_now);
        let target_two_window_start =
            target_two_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_two_metrics_window_start =
            metrics_window_start_for_test(&config, target_two_now);

        let survivor = "TokenStage1PendingBatchNewTailSurvivor11111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_pending_batch_new_tail",
            "stage1-pending-batch-new-tail-survivor",
            target_one_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;
        let mut new_tail_tokens = Vec::new();
        for idx in 0..STALE_RECONCILE_TOKEN_BATCH_CAP {
            let token = format!("TokenStage1PendingBatchNewTail{idx:05}111111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_pending_batch_new_tail",
                &format!("stage1-pending-batch-new-tail-buy-{idx}"),
                source_now + Duration::seconds((idx % 20) as i64 + 1),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            new_tail_tokens.push(token);
        }

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = BTreeMap::from([(survivor.clone(), 1u32)]);
        state.payload.unique_buy_mints = vec![survivor];
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_one_window_start,
                target_one_metrics_window_start,
                target_one_now,
            )?
        );
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileNewTail;

        let forced_first_cycle_rows = 8usize;
        arm_test_force_reconcile_new_tail_exact_batch_row_limit(forced_first_cycle_rows);
        let first_phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        state.prepass_rows_processed = state
            .prepass_rows_processed
            .saturating_add(first_phase_advance.rows_processed);
        state.prepass_pages_processed = state
            .prepass_pages_processed
            .saturating_add(first_phase_advance.pages_processed);
        assert_eq!(first_phase_advance.rows_processed, forced_first_cycle_rows);
        assert_eq!(first_phase_advance.pages_processed, 1);
        assert_eq!(
            state.payload.collect_buy_mints_reconcile_new_tail_pending_mints.len(),
            STALE_RECONCILE_TOKEN_BATCH_CAP.saturating_sub(forced_first_cycle_rows),
            "first bounded cycle should persist the remainder of the exact stale new-tail candidate batch instead of forcing the next cycle to rediscover the same candidates"
        );

        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, target_one_now)?,
        )?;
        let (mut resumed, restore_outcome) = discovery
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                target_two_window_start,
                target_two_metrics_window_start,
                target_two_now,
            )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
        );
        assert_eq!(
            resumed.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileNewTail
        );
        assert_eq!(
            resumed.payload.collect_buy_mints_reconcile_new_tail_pending_mints.len(),
            STALE_RECONCILE_TOKEN_BATCH_CAP.saturating_sub(forced_first_cycle_rows),
            "stale-resume across bucket rollover must preserve the persisted exact stale new-tail batch instead of rediscovering it from scratch"
        );

        let resumed_phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut resumed,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert_eq!(
            resumed_phase_advance.rows_processed,
            STALE_RECONCILE_EXACT_COUNT_BATCH_CAP,
            "once the persisted stale new-tail exact batch is resumed, the next bounded cycle should continue from that batch directly instead of rediscovering candidates from the same stale tail slice"
        );
        assert_eq!(resumed_phase_advance.pages_processed, 1);
        assert_eq!(
            resumed.payload.collect_buy_mints_reconcile_new_tail_pending_mints.len(),
            STALE_RECONCILE_TOKEN_BATCH_CAP
                .saturating_sub(forced_first_cycle_rows)
                .saturating_sub(STALE_RECONCILE_EXACT_COUNT_BATCH_CAP),
            "resumed stale new-tail exact batch should drain the next exact sub-batch without discarding the still-pending remainder"
        );
        assert_eq!(
            resumed
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .as_deref(),
            new_tail_tokens
                .get(
                    forced_first_cycle_rows
                        .saturating_add(STALE_RECONCILE_EXACT_COUNT_BATCH_CAP)
                        .saturating_sub(1),
                )
                .map(|token| token.as_str())
        );
        assert!(
            DiscoveryService::state_can_resume_stale_metrics_window_until_exact_checkpoint(
                &resumed
            ),
            "persisted exact stale new-tail batches must remain exact and stale-resumable after rollover-driven resume"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_new_tail_exact_subbatches_reduce_live_like_timeout_pressure_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-new-tail-exact-subbatches.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-19T03:10:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_now = source_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_window_start =
            target_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_metrics_window_start = metrics_window_start_for_test(&config, target_now);

        let survivor = "TokenStage1ExactSubbatchSurvivor111111111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_exact_subbatch_new_tail",
            "stage1-exact-subbatch-survivor",
            target_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;

        let pending_token_count = STALE_RECONCILE_EXACT_COUNT_BATCH_CAP
            .saturating_mul(config.max_fetch_pages_per_cycle.max(1))
            .saturating_add(7);
        let mut pending_tokens = Vec::new();
        for idx in 0..pending_token_count {
            let token = format!("TokenStage1ExactSubbatch{idx:05}111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_exact_subbatch_new_tail",
                &format!("stage1-exact-subbatch-buy-{idx}"),
                source_now + Duration::seconds((idx % 20) as i64 + 1),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            pending_tokens.push(token);
        }

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = BTreeMap::from([(survivor.clone(), 1u32)]);
        state.payload.unique_buy_mints = vec![survivor];
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_window_start,
                target_metrics_window_start,
                target_now,
            )?
        );
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileNewTail;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_pending_mints = pending_tokens.clone();

        arm_test_force_reconcile_new_tail_exact_batch_row_limit(
            STALE_RECONCILE_EXACT_COUNT_BATCH_CAP,
        );
        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            Instant::now() + StdDuration::from_secs(5),
        )?;

        let expected_rows = STALE_RECONCILE_EXACT_COUNT_BATCH_CAP
            .saturating_mul(config.max_fetch_pages_per_cycle.max(1));
        assert_eq!(
            phase_advance.rows_processed,
            expected_rows,
            "stale new-tail should process multiple exact sub-batches per bounded cycle instead of letting one oversized exact batch query consume the whole cycle"
        );
        assert_eq!(
            phase_advance.pages_processed,
            config.max_fetch_pages_per_cycle
        );
        assert_eq!(
            state.payload.collect_buy_mints_reconcile_new_tail_pending_mints.len(),
            pending_token_count.saturating_sub(expected_rows),
            "processing exact sub-batches should drain the persisted pending batch prefix across all available bounded pages"
        );
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .as_deref(),
            pending_tokens
                .get(expected_rows.saturating_sub(1))
                .map(|token| token.as_str())
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_expired_head_noisy_bucket_roll_does_not_restart_fresh_scan_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-expired-head-noisy-bucket-roll.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 32;
        config.max_fetch_pages_per_cycle = 1;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-18T18:20:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_one_now = source_now + Duration::seconds(60);
        let target_two_now = target_one_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_one_window_start =
            target_one_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_one_metrics_window_start =
            metrics_window_start_for_test(&config, target_one_now);
        let target_two_window_start =
            target_two_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_two_metrics_window_start =
            metrics_window_start_for_test(&config, target_two_now);

        let mut exact_counts = BTreeMap::new();
        for idx in 0..256usize {
            let token = format!("TokenStage1NoisyExpiredHead{idx:04}111111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_noisy_expired",
                &format!("stage1-noisy-expired-buy-{idx}"),
                source_window_start + Duration::seconds((idx % 30) as i64),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            exact_counts.insert(token, 1u32);
        }
        let survivor = "TokenStage1NoisyExpiredHeadSurvivor111111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_noisy_expired",
            "stage1-noisy-expired-survivor",
            target_one_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;
        exact_counts.insert(survivor.clone(), 1);

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = exact_counts.clone();
        state.payload.unique_buy_mints = exact_counts.keys().cloned().collect();
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_one_window_start,
                target_one_metrics_window_start,
                target_one_now,
            )?
        );

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            32,
            1,
            Instant::now() + StdDuration::from_secs(1),
        )?;
        state.prepass_rows_processed = 2_154_114usize;
        state.prepass_pages_processed = state
            .prepass_pages_processed
            .saturating_add(phase_advance.pages_processed)
            .saturating_add(208);
        state.payload.collect_buy_mints_cursor_token =
            phase_advance.collect_buy_mints_cursor_token.clone();
        assert_eq!(
            state.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileExpiredHead
        );
        assert!(state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor_token
            .is_some());
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, target_one_now)?,
        )?;

        let (resumed, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            target_two_window_start,
            target_two_metrics_window_start,
            target_two_now,
        )?;
        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
        );
        assert!(
            resumed.prepass_rows_processed >= state.prepass_rows_processed,
            "bucket rollover during noisy in-progress reconcile must preserve accumulated prepass progress instead of resetting to a fresh scan baseline"
        );
        assert!(
            resumed.metrics_window_start == target_one_metrics_window_start,
            "large noisy stale-bucket reconcile must stay on the frozen target until an exact carry-forward checkpoint becomes available instead of restarting fresh on the new bucket"
        );
        assert!(
            resumed.phase != DiscoveryPersistedRebuildPhase::CollectBuyMints
                || resumed.payload.collect_buy_mints_mode != CollectBuyMintsMode::FreshScan,
            "the noisy stale-bucket path must not operationally restart collect_buy_mints from a fresh-scan baseline"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_expired_head_live_like_cycle_advances_exact_token_batches_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-expired-head-live-like-batch-advance.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-18T23:00:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_now = source_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_window_start =
            target_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_metrics_window_start = metrics_window_start_for_test(&config, target_now);

        let expired_token_count = STALE_RECONCILE_TOKEN_BATCH_CAP
            .saturating_mul(config.max_fetch_pages_per_cycle.max(1))
            .saturating_add(37);
        let mut exact_counts = BTreeMap::new();
        let mut expired_tokens = Vec::new();
        for idx in 0..expired_token_count {
            let token = format!("TokenStage1LiveLikeExpiredHead{idx:05}111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_live_like_expired",
                &format!("stage1-live-like-expired-buy-{idx}"),
                source_window_start + Duration::seconds((idx % 30) as i64),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            exact_counts.insert(token.clone(), 1u32);
            expired_tokens.push(token);
        }
        let survivor = "TokenStage1LiveLikeExpiredHeadSurvivor111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_live_like_expired",
            "stage1-live-like-expired-survivor",
            target_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;
        exact_counts.insert(survivor.clone(), 1);

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = exact_counts;
        state.payload.unique_buy_mints = expired_tokens
            .iter()
            .cloned()
            .chain(std::iter::once(survivor.clone()))
            .collect();
        assert_sorted_strings(&state.payload.unique_buy_mints);
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_window_start,
                target_metrics_window_start,
                target_now,
            )?
        );

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            Instant::now() + StdDuration::from_secs(5),
        )?;

        let expected_rows = STALE_RECONCILE_EXACT_COUNT_BATCH_CAP
            .saturating_mul(config.max_fetch_pages_per_cycle.max(1));
        assert_eq!(
            phase_advance.rows_processed,
            expected_rows,
            "live-like stale expired-head reconcile should advance by capped exact count sub-batches instead of repeatedly recounting one large expired-head candidate range per bounded page"
        );
        assert_eq!(
            phase_advance.pages_processed,
            config.max_fetch_pages_per_cycle
        );
        assert!(!phase_advance.source_exhausted);
        assert_eq!(
            state.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileExpiredHead
        );
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor_token
                .as_deref(),
            expired_tokens
                .get(expected_rows.saturating_sub(1))
                .map(|token| token.as_str())
        );
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_expired_head_pending_mints
                .len(),
            expired_token_count
                .saturating_add(1)
                .min(STALE_RECONCILE_TOKEN_BATCH_CAP)
                .saturating_sub(expected_rows),
            "live-like stale expired-head reconcile should persist the unprocessed suffix of the current exact candidate batch instead of rediscovering it next cycle"
        );
        assert_eq!(
            state.payload.unique_buy_mints.len(),
            expired_token_count.saturating_sub(expected_rows).saturating_add(1),
            "expired-head reconcile should subtract the bounded candidate token batches while keeping the surviving overlap mint"
        );
        assert!(
            DiscoveryService::state_can_resume_stale_metrics_window_until_exact_checkpoint(&state),
            "batched live-like stale reconcile must stay exact and resumable after the first bounded cycle"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_expired_head_pending_exact_batch_survives_rollover_and_finishes_batch_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-expired-head-pending-batch.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 1;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-19T12:00:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_one_now = source_now + Duration::seconds(60);
        let target_two_now = target_one_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_one_window_start =
            target_one_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_one_metrics_window_start =
            metrics_window_start_for_test(&config, target_one_now);
        let target_two_window_start =
            target_two_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_two_metrics_window_start =
            metrics_window_start_for_test(&config, target_two_now);

        let expired_token_count = STALE_RECONCILE_TOKEN_BATCH_CAP.saturating_add(17);
        let mut exact_counts = BTreeMap::new();
        let mut expired_tokens = Vec::new();
        for idx in 0..expired_token_count {
            let token = format!("TokenStage1ExpiredHeadPending{idx:05}111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_expired_head_pending",
                &format!("stage1-expired-head-pending-buy-{idx}"),
                source_window_start + Duration::seconds((idx % 20) as i64 + 1),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            exact_counts.insert(token.clone(), 1u32);
            expired_tokens.push(token);
        }
        let survivor = "TokenStage1ExpiredHeadPendingSurvivor111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_expired_head_pending",
            "stage1-expired-head-pending-survivor",
            target_one_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;
        exact_counts.insert(survivor.clone(), 1);

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = exact_counts;
        state.payload.unique_buy_mints = expired_tokens
            .iter()
            .cloned()
            .chain(std::iter::once(survivor))
            .collect();
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_one_window_start,
                target_one_metrics_window_start,
                target_one_now,
            )?
        );

        let first_phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        state.prepass_rows_processed = state
            .prepass_rows_processed
            .saturating_add(first_phase_advance.rows_processed);
        state.prepass_pages_processed = state
            .prepass_pages_processed
            .saturating_add(first_phase_advance.pages_processed);
        assert_eq!(
            first_phase_advance.rows_processed,
            STALE_RECONCILE_EXACT_COUNT_BATCH_CAP
        );
        assert_eq!(first_phase_advance.pages_processed, 1);
        assert_eq!(
            state.payload.collect_buy_mints_reconcile_expired_head_pending_mints.len(),
            STALE_RECONCILE_TOKEN_BATCH_CAP
                .saturating_sub(STALE_RECONCILE_EXACT_COUNT_BATCH_CAP),
            "first bounded cycle should persist the remainder of the stale expired-head exact candidate batch instead of rediscovering it after rollover"
        );

        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, target_one_now)?,
        )?;
        let (mut resumed, restore_outcome) = discovery
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                target_two_window_start,
                target_two_metrics_window_start,
                target_two_now,
            )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
        );
        assert_eq!(
            resumed.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileExpiredHead
        );
        assert_eq!(
            resumed
                .payload
                .collect_buy_mints_reconcile_expired_head_pending_mints
                .len(),
            STALE_RECONCILE_TOKEN_BATCH_CAP
                .saturating_sub(STALE_RECONCILE_EXACT_COUNT_BATCH_CAP),
            "stale-resume across bucket rollover must preserve the remaining expired-head exact candidate batch"
        );

        let resumed_phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut resumed,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert_eq!(
            resumed_phase_advance.rows_processed,
            STALE_RECONCILE_EXACT_COUNT_BATCH_CAP,
            "once stale expired-head exact batch progress is resumed, the next bounded cycle should drain the next exact sub-batch directly"
        );
        assert_eq!(resumed_phase_advance.pages_processed, 1);
        assert_eq!(
            resumed
                .payload
                .collect_buy_mints_reconcile_expired_head_pending_mints
                .len(),
            STALE_RECONCILE_TOKEN_BATCH_CAP
                .saturating_sub(STALE_RECONCILE_EXACT_COUNT_BATCH_CAP * 2),
            "resumed expired-head reconcile should keep draining the persisted pending batch prefix without rediscovering the same canonical candidates"
        );
        assert_eq!(
            resumed
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor_token
                .as_deref(),
            expired_tokens
                .get(
                    STALE_RECONCILE_EXACT_COUNT_BATCH_CAP
                        .saturating_mul(2)
                        .saturating_sub(1)
                )
                .map(|token| token.as_str())
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_expired_head_exact_subbatches_reduce_live_like_timeout_pressure_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-expired-head-exact-subbatches.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-19T12:10:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_now = source_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_window_start =
            target_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_metrics_window_start = metrics_window_start_for_test(&config, target_now);

        let expired_token_count = STALE_RECONCILE_EXACT_COUNT_BATCH_CAP
            .saturating_mul(config.max_fetch_pages_per_cycle.max(1))
            .saturating_add(9);
        let mut exact_counts = BTreeMap::new();
        let mut expired_tokens = Vec::new();
        for idx in 0..expired_token_count {
            let token = format!("TokenStage1ExpiredHeadSubbatch{idx:05}111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_expired_head_subbatch",
                &format!("stage1-expired-head-subbatch-buy-{idx}"),
                source_window_start + Duration::seconds((idx % 20) as i64 + 1),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            exact_counts.insert(token.clone(), 1u32);
            expired_tokens.push(token);
        }
        let survivor = "TokenStage1ExpiredHeadSubbatchSurvivor111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_expired_head_subbatch",
            "stage1-expired-head-subbatch-survivor",
            target_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;
        exact_counts.insert(survivor.clone(), 1);

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = exact_counts;
        state.payload.unique_buy_mints = expired_tokens
            .iter()
            .cloned()
            .chain(std::iter::once(survivor))
            .collect();
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_window_start,
                target_metrics_window_start,
                target_now,
            )?
        );

        arm_test_force_reconcile_expired_head_exact_batch_row_limit(
            STALE_RECONCILE_EXACT_COUNT_BATCH_CAP,
        );
        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            Instant::now() + StdDuration::from_secs(5),
        )?;

        let expected_rows = STALE_RECONCILE_EXACT_COUNT_BATCH_CAP
            .saturating_mul(config.max_fetch_pages_per_cycle.max(1));
        assert_eq!(
            phase_advance.rows_processed,
            expected_rows,
            "stale expired-head should process multiple exact sub-batches per bounded cycle instead of letting one oversized exact candidate batch dominate the return-to-Replay path"
        );
        assert_eq!(
            phase_advance.pages_processed,
            config.max_fetch_pages_per_cycle
        );
        assert_eq!(
            state.payload.unique_buy_mints.len(),
            expired_token_count.saturating_sub(expected_rows).saturating_add(1),
            "processing expired-head exact sub-batches should subtract the processed prefix while preserving the surviving overlap mint"
        );
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_expired_head_pending_mints
                .len(),
            expired_token_count.saturating_sub(expected_rows),
            "expired-head exact sub-batches should keep only the still-unprocessed suffix pending after all bounded pages are used"
        );
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor_token
                .as_deref(),
            expired_tokens
                .get(expected_rows.saturating_sub(1))
                .map(|token| token.as_str())
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_collect_buy_mints_reconcile_legacy_raw_cursor_repairs_to_grouped_delta_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-collect-buy-mints-reconcile-upgrade-repair.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let now = DateTime::parse_from_rfc3339("2026-03-18T18:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);

        let mut legacy_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        legacy_state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        legacy_state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileNewTail;
        legacy_state.payload.collect_buy_mints_prepass_complete = false;
        legacy_state.payload.collect_buy_mints_cursor_token =
            Some("TokenStage1FreshResumeCursor111111111111".to_string());
        legacy_state
            .payload
            .collect_buy_mints_reconcile_source_window_start =
            Some(window_start - Duration::seconds(20));
        legacy_state
            .payload
            .collect_buy_mints_reconcile_source_horizon_end = Some(now - Duration::seconds(20));
        legacy_state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: now - Duration::seconds(10),
            slot: 42,
            signature: "legacy-reconcile-raw-cursor".to_string(),
        });
        legacy_state.payload.unique_buy_mints =
            vec!["TokenStage1CarryResumeMint1111111111111".to_string()];
        legacy_state.payload.buy_mint_counts =
            BTreeMap::from([("TokenStage1CarryResumeMint1111111111111".to_string(), 1)]);
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&legacy_state, now)?,
        )?;

        let (repaired, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            window_start,
            metrics_window_start,
            now,
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(
            repaired.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileNewTail
        );
        assert_eq!(
            repaired.payload.collect_buy_mints_cursor_token.as_deref(),
            legacy_state.payload.collect_buy_mints_cursor_token.as_deref(),
            "repair must preserve the fresh-scan resume cursor instead of resetting the whole collect_buy_mints attempt"
        );
        assert!(
            repaired
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor
                .is_none(),
            "legacy raw-swap reconcile cursor must be cleared before grouped delta pagination resumes"
        );
        assert!(
            repaired
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .is_none(),
            "grouped delta reconciliation should restart only the current delta subphase from token cursor None"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_eventually_completes_healthy_after_metrics_bucket_roll_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-carry-forward-eventual-healthy.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:59Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.max_fetch_swaps_per_cycle = 1;
        config.max_fetch_pages_per_cycle = 1;
        config.fetch_time_budget_ms = 1_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 2, 3)?;

        let first_summary = discovery.run_cycle(&store, now)?;
        assert_eq!(first_summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        let first_progress = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            first_progress.phase,
            DiscoveryPersistedRebuildPhase::CollectBuyMints
        );

        let rollover_now = now + Duration::seconds(2);
        let second_summary = discovery.run_cycle(&store, rollover_now)?;
        assert_eq!(
            second_summary.runtime_mode,
            DiscoveryRuntimeMode::FailClosed
        );
        let second_progress = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            second_progress.metrics_window_start,
            metrics_window_start_for_test(&config, rollover_now)
        );
        assert!(
            second_progress.prepass_rows_processed >= first_progress.prepass_rows_processed,
            "carry-forward after bucket rollover must preserve bounded collect_buy_mints progress instead of restarting from zero"
        );

        for step in 1..=75 {
            let cycle_now = rollover_now + Duration::seconds(step);
            let summary = discovery.run_cycle(&store, cycle_now)?;
            if summary.runtime_mode == DiscoveryRuntimeMode::Healthy {
                assert_eq!(summary.scoring_source, "raw_window_persisted_stream");
                assert!(
                    store.load_discovery_persisted_rebuild_state()?.is_none(),
                    "healthy completion after carried-forward rebuild must clear the persisted rebuild checkpoint"
                );
                return Ok(());
            }
        }

        anyhow::bail!(
            "carried-forward persisted rebuild did not reach healthy completion before the next test cutoff"
        );
    }

    #[test]
    fn persisted_stream_rebuild_carried_forward_collect_buy_mints_resumes_after_restart_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-carry-forward-restart.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:59Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.max_fetch_swaps_per_cycle = 1;
        config.max_fetch_pages_per_cycle = 1;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 2, 3)?;

        let _ = discovery.run_cycle(&store, now)?;
        let rollover_now = now + Duration::seconds(2);
        let _ = discovery.run_cycle(&store, rollover_now)?;
        let carried_before_restart = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            carried_before_restart.metrics_window_start,
            metrics_window_start_for_test(&config, rollover_now)
        );

        let discovery_after_restart =
            DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let _ = discovery_after_restart.run_cycle(&store, rollover_now + Duration::seconds(1))?;
        let carried_after_restart = load_persisted_stream_rebuild_state_for_test(&store)?;

        assert_eq!(
            carried_after_restart.metrics_window_start,
            carried_before_restart.metrics_window_start
        );
        assert!(
            carried_after_restart.prepass_rows_processed
                > carried_before_restart.prepass_rows_processed
                || carried_after_restart.prepass_pages_processed
                    > carried_before_restart.prepass_pages_processed
                || carried_after_restart.phase != carried_before_restart.phase
                || carried_after_restart.payload.collect_buy_mints_mode
                    != carried_before_restart.payload.collect_buy_mints_mode,
            "restart must continue the carried-forward collect_buy_mints reconciliation instead of resetting it"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_resumes_publish_pending_after_long_restart_within_same_bucket_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-persisted-stream-stale-horizon.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let checkpoint_now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let restart_now = checkpoint_now + Duration::minutes(15);
        let window_start = restart_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, restart_now);

        let mut stale_state = discovery.start_persisted_stream_rebuild_state(
            window_start,
            metrics_window_start,
            checkpoint_now,
        );
        stale_state.phase = DiscoveryPersistedRebuildPhase::PublishPending;
        stale_state
            .payload
            .completed_snapshots
            .push(WalletSnapshot {
                wallet_id: "wallet_stale".to_string(),
                first_seen: stale_state.window_start,
                last_seen: stale_state.horizon_end,
                pnl_sol: 1.0,
                win_rate: 1.0,
                trades: 4,
                closed_trades: 4,
                hold_median_seconds: 60,
                score: 1.0,
                buy_total: 4,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
                eligible: true,
            });
        stale_state.updated_at =
            checkpoint_now - Duration::seconds(config.refresh_seconds.max(1) as i64 + 1);
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&stale_state, stale_state.updated_at)?,
        )?;

        let (resumed, resumed_existing) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            window_start,
            metrics_window_start,
            restart_now,
        )?;

        assert_eq!(
            resumed_existing,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(
            resumed.phase,
            DiscoveryPersistedRebuildPhase::PublishPending
        );
        assert_eq!(resumed.horizon_end, checkpoint_now);
        assert_eq!(resumed.metrics_window_start, metrics_window_start);
        assert_eq!(resumed.payload.completed_snapshots.len(), 1);
        assert!(
            store.load_discovery_persisted_rebuild_state()?.is_some(),
            "long same-bucket restart must keep the persisted checkpoint"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_publish_pending_bucket_roll_does_not_publish_stale_healthy_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-publish-pending-bucket-roll.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.max_fetch_swaps_per_cycle = 1;
        config.max_fetch_pages_per_cycle = 1;
        config.fetch_time_budget_ms = 1_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;
        let count_page = store.load_observed_buy_mint_counts_in_window_after_token_with_budget(
            window_start,
            now,
            None,
            None,
            64,
            Instant::now() + StdDuration::from_secs(1),
        )?;
        let mut publish_pending =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        publish_pending.phase = DiscoveryPersistedRebuildPhase::PublishPending;
        publish_pending.payload.collect_buy_mints_prepass_complete = true;
        publish_pending.payload.unique_buy_mints =
            count_page.rows.iter().map(|row| row.mint.clone()).collect();
        publish_pending.payload.buy_mint_counts = count_page
            .rows
            .iter()
            .map(|row| (row.mint.clone(), row.buy_count as u32))
            .collect();
        publish_pending
            .payload
            .completed_snapshots
            .push(WalletSnapshot {
                wallet_id: "wallet_publish_pending".to_string(),
                first_seen: window_start,
                last_seen: now,
                pnl_sol: 1.0,
                win_rate: 1.0,
                trades: 4,
                closed_trades: 4,
                hold_median_seconds: 60,
                score: 1.0,
                buy_total: 4,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
                eligible: true,
            });
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&publish_pending, now)?,
        )?;

        let rollover_now = now + Duration::seconds(20);
        let summary = discovery.run_cycle(&store, rollover_now)?;
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(
            summary.scoring_source,
            "raw_window_incomplete_no_recent_published_universe"
        );
        assert_eq!(summary.active_follow_wallets, 0);
        let carried = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            carried.phase,
            DiscoveryPersistedRebuildPhase::CollectBuyMints
        );
        assert_ne!(
            carried.metrics_window_start,
            metrics_window_start,
            "publish-pending checkpoint must not publish stale healthy truth after bucket rollover; it should be reset into a fresh target-window collect_buy_mints carry-forward"
        );
        assert!(
            carried.payload.completed_snapshots.is_empty(),
            "stale publish-pending snapshots must be discarded before the carried-forward target-window rebuild resumes"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_discards_checkpoint_when_horizon_is_in_future_stage1() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-future-horizon.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let now = DateTime::parse_from_rfc3339("2026-03-17T12:20:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);

        let mut invalid_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        invalid_state.phase = DiscoveryPersistedRebuildPhase::PublishPending;
        invalid_state.horizon_end = now + Duration::seconds(1);
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(
                &invalid_state,
                invalid_state.updated_at,
            )?,
        )?;

        let (restarted, resumed_existing) = discovery
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                window_start,
                metrics_window_start,
                now,
            )?;
        assert_eq!(
            resumed_existing,
            PersistedStreamRebuildRestoreOutcome::StartedFresh
        );
        assert_eq!(
            restarted.phase,
            DiscoveryPersistedRebuildPhase::CollectBuyMints
        );
        assert_eq!(restarted.horizon_end, now);
        assert!(
            store.load_discovery_persisted_rebuild_state()?.is_none(),
            "future frozen horizon must be cleared before restart"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_partial_fail_closed_clears_active_followlist_stage1() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-fail-closed-clears-followlist.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let stale_metrics_window_start =
            metrics_window_start_for_test(&config, now) - Duration::hours(1);
        let stale_published_wallets =
            seed_published_wallet_metrics_snapshot(&store, stale_metrics_window_start, 1, 1)?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "test_stale_publication".to_string(),
            last_published_at: Some(
                now - discovery.published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(stale_metrics_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(stale_published_wallets.into_iter().collect()),
        })?;
        assert_eq!(store.list_active_follow_wallets()?.len(), 1);

        seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;
        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(summary.active_follow_wallets, 0);
        assert!(
            store.list_active_follow_wallets()?.is_empty(),
            "partial no-fallback fail-closed must clear stale active follow wallets immediately"
        );
        Ok(())
    }

    #[test]
    fn recent_runtime_publication_truth_rejects_stale_exact_published_universe() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("runtime-publication-truth-stale.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "test_stale_exact_published_universe".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(vec!["wallet_published_exact".to_string()]),
        })?;

        assert!(
            discovery
                .recent_runtime_publication_truth(&store, now)?
                .is_none(),
            "stale publication truth must be rejected even when an exact published wallet set is stored"
        );
        Ok(())
    }

    #[test]
    fn bootstrap_degraded_runtime_publication_truth_accepts_explicit_stale_artifact() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("runtime-publication-truth-bootstrap-degraded.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-23T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let published_wallets =
            seed_published_wallet_metrics_snapshot(&store, metrics_window_start, 4, 2)?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "stale_imported_runtime_artifact".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(published_wallets.iter().cloned().collect()),
        })?;
        store.set_discovery_bootstrap_degraded_state(
            true,
            Some("runtime_artifact_restore_bootstrap_degraded"),
            Some(now - Duration::minutes(5)),
        )?;

        assert!(
            discovery
                .recent_runtime_publication_truth(&store, now)?
                .is_none(),
            "stale publication truth must still be rejected as recent truth"
        );
        let bootstrap_truth = discovery
            .bootstrap_degraded_runtime_publication_truth(&store, now)?
            .expect(
                "explicit bootstrap-degraded marker should keep imported publication truth usable",
            );
        assert_eq!(
            bootstrap_truth.active_wallets(),
            published_wallets,
            "bootstrap-degraded runtime truth must preserve the exact imported published wallet set"
        );
        Ok(())
    }

    #[test]
    fn cold_start_unavailable_raw_window_with_bootstrap_degraded_publication_truth_enters_bootstrap_degraded_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-cold-start-bootstrap-degraded.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-23T12:05:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let expected_active_wallets =
            seed_published_wallet_metrics_snapshot(&store, metrics_window_start, 6, 3)?;
        let stale_published_at =
            now - discovery.runtime_published_universe_max_age() - Duration::seconds(1);
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "stale_imported_runtime_artifact".to_string(),
            last_published_at: Some(stale_published_at),
            last_published_window_start: Some(metrics_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(expected_active_wallets.iter().cloned().collect()),
        })?;
        store.set_discovery_bootstrap_degraded_state(
            true,
            Some("runtime_artifact_restore_bootstrap_degraded"),
            Some(now - Duration::minutes(5)),
        )?;

        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(
            summary.runtime_mode,
            DiscoveryRuntimeMode::BootstrapDegraded
        );
        assert_eq!(
            summary.scoring_source,
            "bootstrap_degraded_publication_truth_raw_window_unavailable"
        );
        assert_eq!(summary.active_follow_wallets, expected_active_wallets.len());
        assert_eq!(store.list_active_follow_wallets()?, expected_active_wallets);
        assert!(store.discovery_bootstrap_degraded_state()?.active);
        let publication_state = store
            .discovery_publication_state()?
            .expect("publication state should persist");
        assert_eq!(
            publication_state.last_published_at,
            Some(stale_published_at)
        );
        assert_eq!(
            publication_state.last_published_window_start,
            Some(metrics_window_start)
        );
        Ok(())
    }

    #[test]
    fn bootstrap_degraded_runtime_leaves_only_through_fresh_raw_recovery_semantics() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-bootstrap-degraded-fresh-recovery.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let bootstrap_now = DateTime::parse_from_rfc3339("2026-03-23T12:05:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let recovery_now = bootstrap_now + Duration::minutes(2);
        let config = stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let metrics_window_start = metrics_window_start_for_test(&config, bootstrap_now);
        let expected_active_wallets =
            seed_published_wallet_metrics_snapshot(&store, metrics_window_start, 6, 1)?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "stale_imported_runtime_artifact".to_string(),
            last_published_at: Some(
                bootstrap_now
                    - discovery.runtime_published_universe_max_age()
                    - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(expected_active_wallets.iter().cloned().collect()),
        })?;
        store.set_discovery_bootstrap_degraded_state(
            true,
            Some("runtime_artifact_restore_bootstrap_degraded"),
            Some(bootstrap_now - Duration::minutes(5)),
        )?;

        let bootstrap_summary = discovery.run_cycle(&store, bootstrap_now)?;
        assert_eq!(
            bootstrap_summary.runtime_mode,
            DiscoveryRuntimeMode::BootstrapDegraded
        );
        assert!(store.discovery_bootstrap_degraded_state()?.active);

        seed_runtime_aggregate_ready_wallet(
            &store,
            &config,
            "wallet_raw_truth",
            "TokenBootstrapRecovery111111111111111111111",
            recovery_now,
        )?;

        let recovery_summary = discovery.run_cycle(&store, recovery_now)?;
        assert_eq!(recovery_summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(recovery_summary.scoring_source, "raw_window");
        assert!(
            !store.discovery_bootstrap_degraded_state()?.active,
            "bootstrap-degraded marker must clear only after a real healthy raw recovery cycle succeeds"
        );
        let publication_state = store
            .discovery_publication_state()?
            .expect("publication state should exist after recovery");
        assert_eq!(
            publication_state.runtime_mode,
            DiscoveryRuntimeMode::Healthy
        );
        assert_eq!(publication_state.last_published_at, Some(recovery_now));
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_token_quality_phase_is_bounded_and_resumable_stage1() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-quality-phase-bounded.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);

        let mut state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        state.phase = DiscoveryPersistedRebuildPhase::ResolveTokenQuality;
        state.payload.unique_buy_mints = vec![
            "MintQualityBoundedA1111111111111111111111".to_string(),
            "MintQualityBoundedB1111111111111111111111".to_string(),
            "MintQualityBoundedC1111111111111111111111".to_string(),
            "MintQualityBoundedD1111111111111111111111".to_string(),
            "MintQualityBoundedE1111111111111111111111".to_string(),
        ];
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, now)?,
        )?;

        let rebuild_time_budget = StdDuration::from_millis(config.fetch_time_budget_ms.max(1));
        let first = discovery.advance_persisted_stream_rebuild(
            &store,
            window_start,
            metrics_window_start,
            now,
            2,
            1,
            rebuild_time_budget,
        )?;
        assert!(matches!(
            first,
            PersistedStreamRebuildAdvanceOutcome::InProgress { .. }
        ));
        let first_state = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            first_state.phase,
            DiscoveryPersistedRebuildPhase::ResolveTokenQuality
        );
        assert_eq!(
            first_state.payload.token_quality_progress.next_mint_index,
            2
        );
        assert_eq!(first_state.payload.token_quality_cache.len(), 2);

        let second = discovery.advance_persisted_stream_rebuild(
            &store,
            window_start,
            metrics_window_start,
            now + Duration::minutes(1),
            2,
            1,
            rebuild_time_budget,
        )?;
        assert!(matches!(
            second,
            PersistedStreamRebuildAdvanceOutcome::InProgress { .. }
        ));
        let second_state = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            second_state.phase,
            DiscoveryPersistedRebuildPhase::ResolveTokenQuality
        );
        assert_eq!(
            second_state.payload.token_quality_progress.next_mint_index,
            4
        );
        assert_eq!(second_state.payload.token_quality_cache.len(), 4);
        Ok(())
    }

    #[test]
    fn cold_start_incomplete_raw_window_with_recent_published_universe_enters_degraded_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-cold-start-degraded.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:05:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..9 {
            let ts = now - Duration::minutes(9) + Duration::minutes(idx as i64);
            let swap = swap(
                "wallet_noise",
                &format!("stage1-degraded-noise-{idx}"),
                ts,
                SOL_MINT,
                "TokenStage1DegradedNoise1111111111111111",
                0.2,
                20.0,
            );
            latest_cursor = Some(DiscoveryRuntimeCursor {
                ts_utc: swap.ts_utc,
                slot: swap.slot,
                signature: swap.signature.clone(),
            });
            store.insert_observed_swap(&swap)?;
        }
        let mut config = stage1_runtime_config();
        config.follow_top_n = 15;
        let metrics_window_start = {
            let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
            let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
            let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
            bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
        };
        let expected_active_wallets =
            seed_published_wallet_metrics_snapshot(&store, metrics_window_start, 80, 15)?;
        seed_recent_published_universe(
            &store,
            now - Duration::seconds(30),
            metrics_window_start,
            "raw_window",
            &expected_active_wallets,
        )?;
        store.upsert_discovery_runtime_cursor(
            &latest_cursor.expect("latest cursor should be present"),
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
        assert!(!summary.trusted_selection_fail_closed);
        assert!(summary.raw_window_cap_truncated);
        assert_eq!(
            summary.scoring_source,
            "published_universe_raw_window_degraded"
        );
        assert_eq!(summary.eligible_wallets, 80);
        assert_eq!(summary.active_follow_wallets, 15);
        assert_eq!(summary.follow_promoted, 0);
        assert_eq!(summary.follow_demoted, 0);
        let active_wallets = store.list_active_follow_wallets()?;
        assert_eq!(active_wallets, expected_active_wallets);
        Ok(())
    }

    #[test]
    fn cold_start_truncated_in_memory_with_incomplete_persisted_observed_swaps_and_no_recent_published_universe_fail_closes_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-cold-start-truncated-incomplete-fail-close.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:08:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..9 {
            let ts = now - Duration::minutes(9) + Duration::minutes(idx as i64);
            let swap = swap(
                "wallet_noise",
                &format!("stage1-fail-close-noise-{idx}"),
                ts,
                SOL_MINT,
                "TokenStage1FailCloseNoise111111111111111",
                0.2,
                20.0,
            );
            latest_cursor = Some(DiscoveryRuntimeCursor {
                ts_utc: swap.ts_utc,
                slot: swap.slot,
                signature: swap.signature.clone(),
            });
            store.insert_observed_swap(&swap)?;
        }
        store.activate_follow_wallet("wallet_stale", now - Duration::minutes(1), "seed-follow")?;
        store.upsert_discovery_runtime_cursor(
            &latest_cursor.expect("latest cursor should be present"),
        )?;

        let discovery = DiscoveryService::new(stage1_runtime_config(), permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert!(summary.trusted_selection_fail_closed);
        assert!(summary.raw_window_cap_truncated);
        assert_eq!(
            summary.scoring_source,
            "raw_window_incomplete_no_recent_published_universe"
        );
        assert_eq!(summary.eligible_wallets, 0);
        assert_eq!(summary.follow_demoted, 1);
        assert!(store.list_active_follow_wallets()?.is_empty());
        Ok(())
    }

    #[test]
    fn cold_start_stale_persisted_history_with_recent_published_universe_enters_degraded_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-cold-start-stale-persisted-degraded.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:10:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.follow_top_n = 15;
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let stale_ts = window_start - Duration::minutes(5);
        let stale_swap = swap(
            "wallet_stale_history",
            "stage1-stale-persisted-old-swap",
            stale_ts,
            SOL_MINT,
            "TokenStage1StalePersisted111111111111111",
            0.5,
            50.0,
        );
        store.insert_observed_swap(&stale_swap)?;
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: stale_swap.ts_utc,
            slot: stale_swap.slot,
            signature: stale_swap.signature.clone(),
        })?;

        let metrics_window_start = {
            let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
            let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
            let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
            bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
        };
        let expected_active_wallets =
            seed_published_wallet_metrics_snapshot(&store, metrics_window_start, 80, 15)?;
        seed_recent_published_universe(
            &store,
            now - Duration::seconds(30),
            metrics_window_start,
            "raw_window",
            &expected_active_wallets,
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
        assert!(!summary.trusted_selection_fail_closed);
        assert_eq!(
            summary.scoring_source,
            "published_universe_raw_window_unavailable"
        );
        assert!(!summary.raw_window_cap_truncated);
        assert_eq!(summary.eligible_wallets, 80);
        assert_eq!(summary.active_follow_wallets, 15);
        assert_eq!(summary.follow_promoted, 0);
        assert_eq!(summary.follow_demoted, 0);
        assert_eq!(store.list_active_follow_wallets()?, expected_active_wallets);
        Ok(())
    }

    #[test]
    fn restart_with_recent_published_universe_replaces_stale_followlist_residue_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-restart-published-universe-replaces-stale-followlist.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:11:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.follow_top_n = 3;
        let metrics_window_start = {
            let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
            let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
            let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
            bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
        };
        let published_active_wallets =
            seed_published_wallet_metrics_snapshot(&store, metrics_window_start, 6, 3)?;
        seed_recent_published_universe(
            &store,
            now - Duration::seconds(30),
            metrics_window_start,
            "raw_window",
            &published_active_wallets,
        )?;
        store.activate_follow_wallet(
            "wallet_legacy_residue",
            now - Duration::minutes(10),
            "legacy_bootstrap_residue",
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
        assert_eq!(
            summary.scoring_source,
            "published_universe_raw_window_unavailable"
        );
        assert!(
            summary.follow_demoted >= 1,
            "degraded restart should clear stale followlist residue that does not belong to the published universe"
        );
        assert_eq!(
            summary.active_follow_wallets,
            published_active_wallets.len()
        );
        assert_eq!(
            store.list_active_follow_wallets()?,
            published_active_wallets,
            "runtime degraded restart must restore the last published universe itself, not whatever active followlist residue happened to survive restart"
        );
        Ok(())
    }

    #[test]
    fn restart_with_recent_published_universe_uses_exact_wallet_set_when_current_ranking_drifted_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-restart-published-universe-exact-wallet-set.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:11:30Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.follow_top_n = 1;
        config.min_score = 0.95;
        let metrics_window_start = {
            let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
            let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
            let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
            bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
        };

        store.upsert_wallet(
            "wallet_ranked_now",
            now - Duration::days(2),
            now - Duration::minutes(1),
            "candidate",
        )?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_ranked_now".to_string(),
            window_start: metrics_window_start,
            pnl: 3.2,
            win_rate: 0.9,
            trades: 8,
            closed_trades: 8,
            hold_median_seconds: 90,
            score: 1.2,
            buy_total: 8,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
        let expected_published_wallets = HashSet::from([
            "wallet_published_exact".to_string(),
            "wallet_published_second".to_string(),
        ]);
        seed_recent_published_universe(
            &store,
            now - Duration::seconds(30),
            metrics_window_start,
            "raw_window",
            &expected_published_wallets,
        )?;
        store.activate_follow_wallet(
            "wallet_legacy_residue",
            now - Duration::minutes(10),
            "legacy_bootstrap_residue",
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
        assert_eq!(
            summary.scoring_source,
            "published_universe_raw_window_unavailable"
        );
        assert_eq!(
            summary.active_follow_wallets,
            expected_published_wallets.len()
        );
        assert_eq!(
            store.list_active_follow_wallets()?,
            expected_published_wallets
        );
        assert!(
            !store.list_active_follow_wallets()?.contains("wallet_ranked_now"),
            "degraded restart must read the exact published universe control plane, not reconstruct it from current wallet_metrics ranking"
        );
        Ok(())
    }

    #[test]
    fn cold_start_stale_persisted_history_without_recent_published_universe_fail_closes_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-cold-start-stale-persisted-fail-close.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:12:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let stale_ts = window_start - Duration::minutes(5);
        let stale_swap = swap(
            "wallet_stale_history",
            "stage1-stale-persisted-old-swap-fail-close",
            stale_ts,
            SOL_MINT,
            "TokenStage1StalePersistedFailClose11111",
            0.5,
            50.0,
        );
        store.insert_observed_swap(&stale_swap)?;
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: stale_swap.ts_utc,
            slot: stale_swap.slot,
            signature: stale_swap.signature.clone(),
        })?;
        store.activate_follow_wallet("wallet_stale", now - Duration::minutes(1), "seed-follow")?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert!(summary.trusted_selection_fail_closed);
        assert_eq!(
            summary.scoring_source,
            "raw_window_unusable_no_recent_published_universe"
        );
        assert!(!summary.raw_window_cap_truncated);
        assert_eq!(summary.eligible_wallets, 0);
        assert_eq!(summary.follow_demoted, 1);
        assert!(store.list_active_follow_wallets()?.is_empty());
        Ok(())
    }

    #[test]
    fn cold_start_unusable_raw_window_without_recent_published_universe_fail_closes_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-cold-start-fail-close.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:10:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        store.activate_follow_wallet("wallet_stale", now - Duration::minutes(5), "seed-follow")?;

        let mut config = stage1_runtime_config();
        config.max_window_swaps_in_memory = 128;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert!(summary.trusted_selection_fail_closed);
        assert_eq!(
            summary.scoring_source,
            "raw_window_unusable_no_recent_published_universe"
        );
        assert_eq!(summary.follow_demoted, 1);
        assert!(store.list_active_follow_wallets()?.is_empty());
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn restart_with_recovered_historical_followlist_uses_trusted_persisted_top_n_selection(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-trusted-persisted-bootstrap-top-n.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-15T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.decay_window_days = 5;
        config.observed_swaps_retention_days = 7;
        config.follow_top_n = 1;
        config.min_score = 0.1;
        let metrics_window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);

        for wallet in ["wallet_top", "wallet_legacy"] {
            store.upsert_wallet(
                wallet,
                now - Duration::days(4),
                now - Duration::minutes(5),
                "candidate",
            )?;
            store.activate_follow_wallet(wallet, now - Duration::minutes(5), "seed-follow")?;
        }
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_top".to_string(),
            window_start: metrics_window_start,
            pnl: 3.1,
            win_rate: 0.91,
            trades: 20,
            closed_trades: 10,
            hold_median_seconds: 420,
            score: 0.95,
            buy_total: 12,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_legacy".to_string(),
            window_start: metrics_window_start,
            pnl: 0.5,
            win_rate: 0.51,
            trades: 8,
            closed_trades: 4,
            hold_median_seconds: 900,
            score: -0.2,
            buy_total: 4,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::days(1),
            slot: 7,
            signature: "cursor-trusted-bootstrap".to_string(),
        })?;

        let discovery_after_restart = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery_after_restart.run_cycle(&store, now)?;
        assert_eq!(
            summary.scoring_source,
            "trusted_persisted_wallet_metrics_bootstrap"
        );
        assert_eq!(summary.eligible_wallets, 1);
        assert_eq!(summary.active_follow_wallets, 1);
        assert_eq!(summary.follow_promoted, 0);
        assert_eq!(
            summary.follow_demoted, 1,
            "trusted bootstrap must deactivate recovered historical wallets outside desired top-N"
        );
        let active_after = store.list_active_follow_wallets()?;
        assert_eq!(active_after.len(), 1);
        assert!(active_after.contains("wallet_top"));
        assert!(!active_after.contains("wallet_legacy"));
        assert!(
            !store.discovery_trusted_selection_bootstrap_required()?,
            "trusted persisted bootstrap should clear the durable bootstrap-required flag"
        );
        let state = discovery_after_restart
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(
            !state.trusted_selection_bootstrap_pending,
            "trusted persisted bootstrap should clear bootstrap-pending mode"
        );
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn restart_with_recovered_historical_followlist_fail_closes_without_trusted_persisted_selection(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-trusted-bootstrap-fail-closed.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-15T12:05:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.decay_window_days = 5;
        config.observed_swaps_retention_days = 7;
        config.follow_top_n = 15;

        for wallet in ["wallet_hist_a", "wallet_hist_b", "wallet_hist_c"] {
            store.upsert_wallet(
                wallet,
                now - Duration::days(4),
                now - Duration::minutes(5),
                "candidate",
            )?;
            store.activate_follow_wallet(wallet, now - Duration::minutes(5), "seed-follow")?;
        }
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::days(1),
            slot: 8,
            signature: "cursor-invalid-bootstrap".to_string(),
        })?;

        let discovery_after_restart = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery_after_restart.run_cycle(&store, now)?;
        assert_eq!(
            summary.scoring_source,
            "trusted_persisted_wallet_metrics_bootstrap_unavailable"
        );
        assert_eq!(summary.eligible_wallets, 0);
        assert!(summary.top_wallets.is_empty());
        assert_eq!(summary.follow_promoted, 0);
        assert_eq!(summary.active_follow_wallets, 0);
        assert_eq!(
            summary.follow_demoted, 3,
            "invalid bootstrap state must fail-close the recovered historical followlist"
        );
        assert!(
            store.list_active_follow_wallets()?.is_empty(),
            "invalid bootstrap state must not keep running on the recovered historical active set"
        );
        assert!(
            store.discovery_trusted_selection_bootstrap_required()?,
            "invalid bootstrap state must persist its bootstrap-required fail-close marker across process restarts"
        );
        let state = discovery_after_restart
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(
            state.trusted_selection_bootstrap_pending,
            "fail-closed bootstrap should keep retrying trusted persisted selection instead of falling back to raw-window trust"
        );
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn second_restart_after_fail_close_keeps_trusted_selection_bootstrap_pending_without_falling_back_to_raw_window(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-trusted-bootstrap-fail-close-second-restart.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-15T12:10:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let restart_now = now + Duration::minutes(1);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.decay_window_days = 5;
        config.observed_swaps_retention_days = 7;
        config.follow_top_n = 15;

        for wallet in ["wallet_hist_a", "wallet_hist_b"] {
            store.upsert_wallet(
                wallet,
                now - Duration::days(4),
                now - Duration::minutes(5),
                "candidate",
            )?;
            store.activate_follow_wallet(wallet, now - Duration::minutes(5), "seed-follow")?;
        }
        store.upsert_wallet(
            "wallet_raw_only",
            now - Duration::hours(2),
            restart_now - Duration::seconds(5),
            "candidate",
        )?;
        store.insert_observed_swap(&SwapEvent {
            signature: "sig-raw-only-bootstrap".to_string(),
            wallet: "wallet_raw_only".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-raw-only".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            slot: 44,
            ts_utc: restart_now - Duration::minutes(1),
            exact_amounts: None,
        })?;
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::days(1),
            slot: 9,
            signature: "cursor-invalid-bootstrap-second-restart".to_string(),
        })?;

        let first_restart = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let first_summary = first_restart.run_cycle(&store, now)?;
        assert_eq!(
            first_summary.scoring_source,
            "trusted_persisted_wallet_metrics_bootstrap_unavailable"
        );
        assert!(store.list_active_follow_wallets()?.is_empty());
        assert!(store.discovery_trusted_selection_bootstrap_required()?);

        let second_restart = DiscoveryService::new(config, permissive_shadow_quality());
        let second_summary = second_restart.run_cycle(&store, restart_now)?;
        assert_eq!(
            second_summary.scoring_source,
            "trusted_persisted_wallet_metrics_bootstrap_unavailable",
            "durable invalid-selection state must survive a second restart instead of falling back to raw-window selection"
        );
        assert_eq!(second_summary.follow_promoted, 0);
        assert_eq!(second_summary.follow_demoted, 0);
        assert_eq!(second_summary.active_follow_wallets, 0);
        assert!(second_summary.top_wallets.is_empty());
        assert!(
            store.list_active_follow_wallets()?.is_empty(),
            "raw-window-only candidates must not become the new active followlist while trusted bootstrap remains unavailable"
        );
        assert!(store.discovery_trusted_selection_bootstrap_required()?);
        let state = second_restart
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(state.trusted_selection_bootstrap_pending);
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn materialized_trusted_wallet_metrics_bootstrap_unblocks_fail_closed_restart_without_mutating_followlist_directly(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-materialized-trusted-wallet-metrics-bootstrap.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-15T12:14:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let next_cycle_now = now + Duration::minutes(1);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.decay_window_days = 5;
        config.observed_swaps_retention_days = 7;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.0;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.max_rug_ratio = 1.0;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.max_fetch_swaps_per_cycle = 100;
        config.thin_market_min_unique_traders = 1;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let fresh_metrics_window_start = discovery.metrics_window_start(now);
        let stale_metrics_window_start = fresh_metrics_window_start - Duration::hours(1);

        for wallet in ["wallet_hist_a", "wallet_hist_b"] {
            store.upsert_wallet(
                wallet,
                now - Duration::days(4),
                now - Duration::minutes(5),
                "candidate",
            )?;
            store.activate_follow_wallet(wallet, now - Duration::minutes(5), "seed-follow")?;
        }

        for idx in 0..4 {
            let buy_ts = fresh_metrics_window_start + Duration::hours((idx * 8) as i64 + 1);
            let sell_ts = buy_ts + Duration::minutes(8);
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("top-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenTop11111111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("top-sell-{idx}"),
                sell_ts,
                "TokenTop11111111111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.35,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_runner_up",
                &format!("runner-up-buy-{idx}"),
                buy_ts + Duration::minutes(2),
                SOL_MINT,
                "TokenRunnerUp111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_runner_up",
                &format!("runner-up-sell-{idx}"),
                sell_ts + Duration::minutes(2),
                "TokenRunnerUp111111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.2,
            ))?;
        }

        for wallet in ["wallet_top", "wallet_runner_up"] {
            store.upsert_wallet(
                wallet,
                now - Duration::days(4),
                now - Duration::minutes(1),
                "candidate",
            )?;
        }
        store.insert_observed_swap(&swap(
            "wallet_history_anchor",
            "history-anchor-buy",
            fresh_metrics_window_start - Duration::minutes(5),
            SOL_MINT,
            "TokenHistoryAnchor11111111111111111111111111",
            0.8,
            80.0,
        ))?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_top".to_string(),
            window_start: stale_metrics_window_start,
            pnl: 2.0,
            win_rate: 0.8,
            trades: 8,
            closed_trades: 4,
            hold_median_seconds: 480,
            score: 0.7,
            buy_total: 4,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;

        let fail_closed_summary = discovery.run_cycle(&store, now)?;
        assert_eq!(
            fail_closed_summary.scoring_source,
            "trusted_persisted_wallet_metrics_bootstrap_unavailable",
            "stale persisted wallet_metrics must keep discovery fail-closed"
        );
        assert!(store.list_active_follow_wallets()?.is_empty());
        assert!(store.discovery_trusted_selection_bootstrap_required()?);

        let materialized = discovery.materialize_trusted_bootstrap_wallet_metrics(&store, now)?;
        assert_eq!(
            materialized.metrics_window_start,
            fresh_metrics_window_start
        );
        assert_eq!(materialized.observed_swaps_loaded, 16);
        assert!(materialized.wallets_seen >= 2);
        assert!(materialized.eligible_wallets >= 1);
        assert!(materialized.metrics_written >= 2);
        assert!(
            !materialized.bucket_already_exists,
            "test fixture should require writing a fresh bootstrap bucket"
        );
        assert_eq!(
            store.latest_wallet_metrics_window_start()?,
            Some(fresh_metrics_window_start),
            "tool must materialize a fresh wallet_metrics bucket for the current bootstrap window"
        );
        assert!(
            store.list_active_follow_wallets()?.is_empty(),
            "tool must not directly mutate followlist; discovery should do that on the next cycle"
        );

        let discovery_after_materialization =
            DiscoveryService::new(config, permissive_shadow_quality());
        let recovered_summary =
            discovery_after_materialization.run_cycle(&store, next_cycle_now)?;
        assert_eq!(
            recovered_summary.scoring_source,
            "trusted_persisted_wallet_metrics_bootstrap"
        );
        assert!(
            recovered_summary.active_follow_wallets <= 1,
            "followlist must converge to follow_top_n after trusted bootstrap"
        );
        assert!(
            recovered_summary.eligible_wallets > 0,
            "fresh bootstrap bucket should restore a non-empty trusted eligible universe"
        );
        assert!(
            !recovered_summary.top_wallets.is_empty(),
            "fresh trusted bootstrap should restore top-wallet labels"
        );
        assert!(
            store.list_active_follow_wallets()?.contains("wallet_top"),
            "next discovery cycle should activate the trusted top-ranked wallet"
        );
        assert!(
            !store.discovery_trusted_selection_bootstrap_required()?,
            "trusted bootstrap flag should clear after discovery consumes the fresh bucket"
        );
        Ok(())
    }

    #[test]
    fn materialize_trusted_wallet_metrics_bootstrap_rejects_incomplete_persisted_history(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-materialized-wallet-metrics-bootstrap-incomplete-history.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-15T12:44:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.decay_window_days = 5;
        config.observed_swaps_retention_days = 7;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 2;
        config.min_active_days = 1;
        config.min_score = 0.0;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.max_rug_ratio = 1.0;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.max_fetch_swaps_per_cycle = 100;
        config.thin_market_min_unique_traders = 1;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let metrics_window_start = discovery.metrics_window_start(now);

        for idx in 0..2 {
            let buy_ts = metrics_window_start + Duration::hours((idx * 12) as i64 + 1);
            let sell_ts = buy_ts + Duration::minutes(5);
            store.insert_observed_swap(&swap(
                "wallet_partial",
                &format!("partial-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenPartialBootstrap1111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_partial",
                &format!("partial-sell-{idx}"),
                sell_ts,
                "TokenPartialBootstrap1111111111111111111111",
                SOL_MINT,
                100.0,
                1.2,
            ))?;
        }

        let error = discovery
            .materialize_trusted_bootstrap_wallet_metrics(&store, now)
            .expect_err(
                "incomplete persisted history must reject trusted bootstrap materialization",
            );
        assert!(
            error
                .to_string()
                .contains("persisted observed_swaps history is incomplete"),
            "unexpected error: {error:#}"
        );
        assert_eq!(
            store.latest_wallet_metrics_window_start()?,
            None,
            "tool must not write a fresh wallet_metrics bucket from incomplete persisted history"
        );
        assert!(store.list_active_follow_wallets()?.is_empty());
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn clone_latest_trusted_wallet_metrics_bootstrap_unblocks_fail_closed_restart_without_mutating_followlist_directly(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-clone-latest-trusted-wallet-metrics-bootstrap.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-15T12:14:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let next_cycle_now = now + Duration::minutes(1);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.decay_window_days = 5;
        config.observed_swaps_retention_days = 7;
        config.follow_top_n = 1;
        config.min_score = 0.0;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.max_bootstrap_snapshot_age_seconds = 4 * 60 * 60;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let target_metrics_window_start = discovery.metrics_window_start(now);
        let source_metrics_window_start = target_metrics_window_start - Duration::hours(2);

        for (wallet, score) in [("wallet_top", 0.9_f64), ("wallet_runner_up", 0.7_f64)] {
            store.upsert_wallet(
                wallet,
                now - Duration::days(4),
                now - Duration::minutes(1),
                "candidate",
            )?;
            store.insert_wallet_metric(&WalletMetricRow {
                wallet_id: wallet.to_string(),
                window_start: source_metrics_window_start,
                pnl: 2.0,
                win_rate: 0.8,
                trades: 8,
                closed_trades: 4,
                hold_median_seconds: 480,
                score,
                buy_total: 4,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            })?;
        }
        store.set_discovery_trusted_selection_bootstrap_required(true, "test_clone_latest")?;

        let fail_closed_summary = discovery.run_cycle(&store, now)?;
        assert_eq!(
            fail_closed_summary.scoring_source,
            "trusted_persisted_wallet_metrics_bootstrap_unavailable",
            "stale persisted wallet_metrics must keep discovery fail-closed before clone-latest bridge"
        );
        assert!(store.list_active_follow_wallets()?.is_empty());
        assert!(store.discovery_trusted_selection_bootstrap_required()?);

        let cloned =
            discovery.clone_latest_trusted_bootstrap_wallet_metrics(&store, now, false, false)?;
        assert_eq!(
            cloned.source_metrics_window_start,
            source_metrics_window_start
        );
        assert_eq!(
            cloned.target_metrics_window_start,
            target_metrics_window_start
        );
        assert_eq!(cloned.source_snapshot_age_seconds, 2 * 60 * 60 + 14 * 60);
        assert_eq!(cloned.source_rows, 2);
        assert_eq!(cloned.inserted_rows, 2);
        assert!(!cloned.stale_source);
        assert!(!cloned.dry_run);
        assert!(
            store.list_active_follow_wallets()?.is_empty(),
            "clone-latest tool must not directly mutate followlist"
        );
        assert_eq!(
            store.latest_wallet_metrics_window_start()?,
            Some(target_metrics_window_start),
            "clone-latest tool must create the current bootstrap bucket"
        );

        let discovery_after_clone = DiscoveryService::new(config, permissive_shadow_quality());
        let recovered_summary = discovery_after_clone.run_cycle(&store, next_cycle_now)?;
        assert_eq!(
            recovered_summary.scoring_source,
            "trusted_persisted_wallet_metrics_bootstrap"
        );
        assert_eq!(recovered_summary.active_follow_wallets, 1);
        assert!(store.list_active_follow_wallets()?.contains("wallet_top"));
        assert!(
            !store.discovery_trusted_selection_bootstrap_required()?,
            "trusted bootstrap flag should clear after discovery consumes the cloned bucket"
        );
        Ok(())
    }

    #[test]
    fn clone_latest_trusted_wallet_metrics_bootstrap_rejects_stale_source_without_force_stale(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-clone-latest-trusted-wallet-metrics-bootstrap-stale.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-15T12:14:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.decay_window_days = 5;
        config.max_bootstrap_snapshot_age_seconds = 60 * 60;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.min_score = 0.0;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let target_metrics_window_start = discovery.metrics_window_start(now);
        let stale_source_window_start = target_metrics_window_start - Duration::hours(2);

        store.upsert_wallet(
            "wallet_stale",
            now - Duration::days(4),
            now - Duration::minutes(1),
            "candidate",
        )?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_stale".to_string(),
            window_start: stale_source_window_start,
            pnl: 1.0,
            win_rate: 0.7,
            trades: 6,
            closed_trades: 3,
            hold_median_seconds: 300,
            score: 0.6,
            buy_total: 3,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;

        let error = discovery
            .clone_latest_trusted_bootstrap_wallet_metrics(&store, now, false, false)
            .expect_err("stale source snapshot must reject without explicit override");
        assert!(
            error
                .to_string()
                .contains("stale for clone-latest bootstrap"),
            "unexpected error: {error:#}"
        );
        assert!(
            !store.wallet_metrics_window_exists(target_metrics_window_start)?,
            "rejecting stale source must not create the target bootstrap bucket"
        );

        let dry_run =
            discovery.clone_latest_trusted_bootstrap_wallet_metrics(&store, now, true, true)?;
        assert!(dry_run.stale_source);
        assert!(dry_run.forced_stale);
        assert!(dry_run.dry_run);
        assert_eq!(dry_run.inserted_rows, 0);
        assert!(
            !store.wallet_metrics_window_exists(target_metrics_window_start)?,
            "dry-run must not create the target bootstrap bucket even with stale override"
        );

        let forced_write =
            discovery.clone_latest_trusted_bootstrap_wallet_metrics(&store, now, false, true)?;
        assert!(forced_write.stale_source);
        assert!(forced_write.forced_stale);
        assert!(!forced_write.dry_run);
        assert_eq!(forced_write.source_rows, 1);
        assert_eq!(forced_write.inserted_rows, 1);
        assert!(
            store.wallet_metrics_window_exists(target_metrics_window_start)?,
            "force_stale write must create the target bootstrap bucket when explicitly requested"
        );
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn run_cycle_marks_aged_bridged_snapshot_as_stale_and_fail_closes() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-aged-bridged-snapshot-fail-close.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-15T12:14:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.decay_window_days = 5;
        config.follow_top_n = 1;
        config.min_score = 0.0;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.max_bootstrap_snapshot_age_seconds = 60 * 60;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let effective_window_start = discovery.metrics_window_start(now);
        let source_window_start = effective_window_start - Duration::hours(2);
        let wallet_id = "wallet_bridged".to_string();
        let snapshot_write = trusted_snapshot_write(
            TrustedSnapshotSourceKind::CloneLatestBridge,
            TrustedSelectionState::TrustedBridged,
            effective_window_start,
            now - Duration::minutes(1),
            1,
            Some("snapshot-source-001".to_string()),
            Some(source_window_start),
        );

        store.persist_discovery_cycle_with_snapshot_metadata(
            &[WalletUpsertRow {
                wallet_id: wallet_id.clone(),
                first_seen: now - Duration::days(4),
                last_seen: now - Duration::minutes(1),
                status: "candidate".to_string(),
            }],
            &[WalletMetricRow {
                wallet_id,
                window_start: effective_window_start,
                pnl: 2.0,
                win_rate: 0.8,
                trades: 8,
                closed_trades: 4,
                hold_median_seconds: 480,
                score: 0.9,
                buy_total: 4,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            }],
            &[],
            false,
            false,
            now,
            "seed-aged-bridge",
            Some(&snapshot_write),
        )?;
        store.set_discovery_trusted_selection_bootstrap_required(true, "seed-aged-bridge")?;

        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(
            summary.scoring_source,
            "trusted_persisted_wallet_metrics_bootstrap_unavailable"
        );
        assert!(summary.trusted_selection_fail_closed);

        let state = store
            .discovery_trusted_selection_state()?
            .expect("trusted selection state should be persisted on fail-close");
        assert!(state.bootstrap_required);
        assert_eq!(
            state.selection_state,
            TrustedSelectionState::TrustedBridgedStale
        );
        assert_eq!(
            state.active_snapshot_id,
            Some(snapshot_write.snapshot_id.clone())
        );
        assert_eq!(
            state.active_snapshot_window_start,
            Some(effective_window_start)
        );
        assert_eq!(
            state.last_bootstrap_source_kind,
            Some(TrustedSnapshotSourceKind::CloneLatestBridge)
        );
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn run_cycle_startup_gate_uses_typed_aged_bridged_state_even_when_legacy_bool_is_false(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-startup-gate-aged-bridged-typed-state.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-15T12:14:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.decay_window_days = 5;
        config.follow_top_n = 1;
        config.min_score = 0.0;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.max_bootstrap_snapshot_age_seconds = 60 * 60;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let effective_window_start = discovery.metrics_window_start(now);
        let source_window_start = effective_window_start - Duration::hours(2);
        let snapshot_write = trusted_snapshot_write(
            TrustedSnapshotSourceKind::CloneLatestBridge,
            TrustedSelectionState::TrustedBridged,
            effective_window_start,
            now - Duration::minutes(1),
            1,
            Some("snapshot-source-002".to_string()),
            Some(source_window_start),
        );

        store.persist_discovery_cycle_with_snapshot_metadata(
            &[WalletUpsertRow {
                wallet_id: "wallet_startup_bridge".to_string(),
                first_seen: now - Duration::days(4),
                last_seen: now - Duration::minutes(1),
                status: "candidate".to_string(),
            }],
            &[WalletMetricRow {
                wallet_id: "wallet_startup_bridge".to_string(),
                window_start: effective_window_start,
                pnl: 1.5,
                win_rate: 0.7,
                trades: 6,
                closed_trades: 3,
                hold_median_seconds: 300,
                score: 0.6,
                buy_total: 3,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            }],
            &[],
            false,
            false,
            now,
            "seed-startup-aged-bridge",
            Some(&snapshot_write),
        )?;
        store.set_discovery_trusted_selection_state(&DiscoveryTrustedSelectionStateUpdate {
            bootstrap_required: false,
            reason: "typed_bridged".to_string(),
            selection_state: TrustedSelectionState::TrustedBridged,
            active_snapshot_id: Some(snapshot_write.snapshot_id.clone()),
            active_snapshot_window_start: Some(effective_window_start),
            last_bootstrap_source_kind: Some(TrustedSnapshotSourceKind::CloneLatestBridge),
            last_bootstrap_at: Some(now - Duration::minutes(1)),
        })?;

        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(
            summary.scoring_source,
            "trusted_persisted_wallet_metrics_bootstrap_unavailable"
        );
        assert!(summary.trusted_selection_fail_closed);
        assert!(
            store.discovery_trusted_selection_bootstrap_required()?,
            "startup gate should mirror typed fail-close into the legacy bool latch for compatibility"
        );
        let persisted_state = store
            .discovery_trusted_selection_state()?
            .expect("typed state should persist across startup fail-close");
        assert_eq!(
            persisted_state.selection_state,
            TrustedSelectionState::TrustedBridgedStale
        );
        let state = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(state.trusted_selection_bootstrap_pending);
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn run_cycle_startup_gate_uses_snapshot_metadata_aged_bridged_state_without_legacy_fallback(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-startup-gate-aged-bridged-metadata-only.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-15T12:14:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.decay_window_days = 5;
        config.follow_top_n = 1;
        config.min_score = 0.0;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.max_bootstrap_snapshot_age_seconds = 60 * 60;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let effective_window_start = discovery.metrics_window_start(now);
        let source_window_start = effective_window_start - Duration::hours(2);
        let snapshot_write = trusted_snapshot_write(
            TrustedSnapshotSourceKind::CloneLatestBridge,
            TrustedSelectionState::TrustedBridged,
            effective_window_start,
            now - Duration::minutes(1),
            1,
            Some("snapshot-source-003".to_string()),
            Some(source_window_start),
        );

        store.persist_discovery_cycle_with_snapshot_metadata(
            &[WalletUpsertRow {
                wallet_id: "wallet_startup_bridge_metadata".to_string(),
                first_seen: now - Duration::days(4),
                last_seen: now - Duration::minutes(1),
                status: "candidate".to_string(),
            }],
            &[WalletMetricRow {
                wallet_id: "wallet_startup_bridge_metadata".to_string(),
                window_start: effective_window_start,
                pnl: 1.5,
                win_rate: 0.7,
                trades: 6,
                closed_trades: 3,
                hold_median_seconds: 300,
                score: 0.6,
                buy_total: 3,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            }],
            &[],
            false,
            false,
            now,
            "seed-startup-aged-bridge-metadata-only",
            Some(&snapshot_write),
        )?;

        let status = store.startup_trusted_selection_gate_status()?;
        assert_eq!(
            status.selection_state,
            Some(TrustedSelectionState::TrustedBridged)
        );
        assert!(
            !status.legacy_bool_fallback_used,
            "metadata-backed startup status should stay on the typed path even without a typed state row"
        );

        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(
            summary.scoring_source,
            "trusted_persisted_wallet_metrics_bootstrap_unavailable"
        );
        assert!(summary.trusted_selection_fail_closed);
        assert!(
            store.discovery_trusted_selection_bootstrap_required()?,
            "startup gate should still mirror metadata-derived typed fail-close into the compatibility latch"
        );
        let persisted_state = store
            .discovery_trusted_selection_state()?
            .expect("typed state should persist across metadata-derived startup fail-close");
        assert_eq!(
            persisted_state.selection_state,
            TrustedSelectionState::TrustedBridgedStale
        );
        assert_eq!(
            persisted_state.active_snapshot_id,
            Some(snapshot_write.snapshot_id.clone())
        );
        let state = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(state.trusted_selection_bootstrap_pending);
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn run_cycle_startup_gate_fail_closes_for_stale_trusted_current_snapshot_metadata_without_typed_state(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-startup-gate-stale-current-metadata-only.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-15T22:05:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.decay_window_days = 5;
        config.follow_top_n = 1;
        config.min_score = 0.0;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.max_bootstrap_snapshot_age_seconds = 60 * 60;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let stale_window_start = DateTime::parse_from_rfc3339("2026-03-10T21:00:00+00:00")
            .expect("timestamp")
            .with_timezone(&Utc);
        let snapshot_write = trusted_snapshot_write(
            TrustedSnapshotSourceKind::DiscoveryRefresh,
            TrustedSelectionState::TrustedCurrent,
            stale_window_start,
            stale_window_start + Duration::minutes(1),
            1,
            None,
            Some(stale_window_start),
        );

        store.persist_discovery_cycle_with_snapshot_metadata(
            &[WalletUpsertRow {
                wallet_id: "wallet_stale_current_metadata".to_string(),
                first_seen: stale_window_start - Duration::days(4),
                last_seen: stale_window_start,
                status: "candidate".to_string(),
            }],
            &[WalletMetricRow {
                wallet_id: "wallet_stale_current_metadata".to_string(),
                window_start: stale_window_start,
                pnl: 1.5,
                win_rate: 0.7,
                trades: 6,
                closed_trades: 3,
                hold_median_seconds: 300,
                score: 0.6,
                buy_total: 3,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            }],
            &[],
            false,
            false,
            stale_window_start + Duration::minutes(1),
            "seed-startup-stale-current-metadata-only",
            Some(&snapshot_write),
        )?;

        let status = store.startup_trusted_selection_gate_status()?;
        assert_eq!(
            status.selection_state,
            Some(TrustedSelectionState::TrustedCurrent)
        );
        assert!(!status.legacy_bool_fallback_used);
        assert!(
            discovery.effective_startup_trusted_selection_fail_closed(&status, now),
            "stale trusted_current metadata should no longer bypass startup fail-close when the typed state row is absent"
        );

        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(
            summary.scoring_source,
            "trusted_persisted_wallet_metrics_bootstrap_unavailable"
        );
        assert!(summary.trusted_selection_fail_closed);
        assert!(store.discovery_trusted_selection_bootstrap_required()?);
        let persisted_state = store
            .discovery_trusted_selection_state()?
            .expect("typed state should persist across stale-current startup fail-close");
        assert_eq!(
            persisted_state.selection_state,
            TrustedSelectionState::Invalid
        );
        let state = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(state.trusted_selection_bootstrap_pending);
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn post_bootstrap_rotation_watchdog_triggers_on_first_next_bucket_and_fail_closes() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-post-bootstrap-rotation-watchdog-trigger.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let config = post_bootstrap_watchdog_config();
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let bootstrap_now = DateTime::parse_from_rfc3339("2026-03-16T12:00:20Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let same_bucket_now = DateTime::parse_from_rfc3339("2026-03-16T12:00:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let next_bucket_now = DateTime::parse_from_rfc3339("2026-03-16T12:01:10Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let snapshot_write =
            seed_bridged_bootstrap_followlist(&store, &discovery, bootstrap_now, "wallet_bridge")?;
        let same_bucket_window_start = same_bucket_now - Duration::days(1);
        prime_running_discovery_cursor(&discovery, same_bucket_window_start);
        seed_cap_truncated_raw_tail(&store, same_bucket_window_start + Duration::minutes(10), 9)?;

        let first_summary = discovery.run_cycle(&store, same_bucket_now)?;
        assert_eq!(
            first_summary.scoring_source, "raw_window",
            "same-bucket cap-truncated raw discovery should remain suppressed but must not trigger the watchdog early"
        );
        assert!(!first_summary.trusted_selection_fail_closed);
        assert!(
            !store.discovery_trusted_selection_bootstrap_required()?,
            "watchdog must not raise bootstrap-required until the first next effective bucket arrives"
        );
        assert!(store
            .list_active_follow_wallets()?
            .contains("wallet_bridge"));

        let second_summary = discovery.run_cycle(&store, next_bucket_now)?;
        assert_eq!(
            second_summary.scoring_source,
            POST_BOOTSTRAP_ROTATION_BLOCKED_REASON
        );
        assert!(
            second_summary.trusted_selection_fail_closed,
            "watchdog must degrade a frozen bridged bootstrap set back to invalid/fail-close on the first next bucket"
        );
        assert_eq!(second_summary.follow_demoted, 1);
        assert!(second_summary.raw_window_cap_truncated);
        assert!(
            store.discovery_trusted_selection_bootstrap_required()?,
            "watchdog must raise the durable bootstrap-required latch again"
        );
        assert!(
            store.list_active_follow_wallets()?.is_empty(),
            "watchdog must fail-close the previously active bridged bootstrap followlist"
        );

        let persisted_state = store
            .discovery_trusted_selection_state()?
            .expect("watchdog must persist invalid typed state");
        assert!(persisted_state.bootstrap_required);
        assert_eq!(
            persisted_state.reason,
            POST_BOOTSTRAP_ROTATION_BLOCKED_REASON
        );
        assert_eq!(
            persisted_state.selection_state,
            TrustedSelectionState::Invalid
        );
        assert_eq!(
            persisted_state.active_snapshot_id,
            Some(snapshot_write.snapshot_id)
        );
        assert_eq!(
            persisted_state.active_snapshot_window_start,
            Some(discovery.metrics_window_start(bootstrap_now))
        );
        assert_eq!(
            persisted_state.last_bootstrap_source_kind,
            Some(TrustedSnapshotSourceKind::CloneLatestBridge)
        );
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn post_bootstrap_rotation_watchdog_does_not_fire_when_fresh_trusted_current_snapshot_exists(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-post-bootstrap-rotation-watchdog-fresh-current.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let config = post_bootstrap_watchdog_config();
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let bootstrap_now = DateTime::parse_from_rfc3339("2026-03-16T12:00:20Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let next_bucket_now = DateTime::parse_from_rfc3339("2026-03-16T12:01:10Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        seed_bridged_bootstrap_followlist(&store, &discovery, bootstrap_now, "wallet_bridge")?;
        seed_current_trusted_source_snapshot(
            &store,
            discovery.metrics_window_start(next_bucket_now),
            next_bucket_now - Duration::seconds(1),
            TrustedSnapshotSourceKind::DiscoveryRefresh,
            "wallet_fresh_current",
        )?;
        let next_bucket_window_start = next_bucket_now - Duration::days(1);
        prime_running_discovery_cursor(&discovery, next_bucket_window_start);
        seed_cap_truncated_raw_tail(&store, next_bucket_window_start + Duration::minutes(10), 9)?;

        let summary = discovery.run_cycle(&store, next_bucket_now)?;
        assert_ne!(
            summary.scoring_source,
            POST_BOOTSTRAP_ROTATION_BLOCKED_REASON
        );
        assert!(!summary.trusted_selection_fail_closed);
        assert!(
            !store.discovery_trusted_selection_bootstrap_required()?,
            "a fresh trusted current snapshot should prevent the watchdog from raising bootstrap-required"
        );
        let persisted_state = store
            .discovery_trusted_selection_state()?
            .expect("typed bridged state should remain intact");
        assert_eq!(
            persisted_state.selection_state,
            TrustedSelectionState::TrustedBridged
        );
        assert!(store
            .list_active_follow_wallets()?
            .contains("wallet_bridge"));
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn post_bootstrap_rotation_watchdog_does_not_fire_when_alternate_trusted_source_is_ready(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-post-bootstrap-rotation-watchdog-alternate-source.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let config = post_bootstrap_watchdog_config();
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let bootstrap_now = DateTime::parse_from_rfc3339("2026-03-16T12:00:20Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let next_bucket_now = DateTime::parse_from_rfc3339("2026-03-16T12:01:10Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        seed_bridged_bootstrap_followlist(&store, &discovery, bootstrap_now, "wallet_bridge")?;
        seed_current_trusted_source_snapshot(
            &store,
            discovery.metrics_window_start(next_bucket_now),
            next_bucket_now - Duration::seconds(1),
            TrustedSnapshotSourceKind::AdminMaterialization,
            "wallet_admin_source",
        )?;
        let next_bucket_window_start = next_bucket_now - Duration::days(1);
        prime_running_discovery_cursor(&discovery, next_bucket_window_start);
        seed_cap_truncated_raw_tail(&store, next_bucket_window_start + Duration::minutes(10), 9)?;

        let summary = discovery.run_cycle(&store, next_bucket_now)?;
        assert_ne!(
            summary.scoring_source,
            POST_BOOTSTRAP_ROTATION_BLOCKED_REASON
        );
        assert!(!summary.trusted_selection_fail_closed);
        assert!(
            !store.discovery_trusted_selection_bootstrap_required()?,
            "an alternate trusted source should suppress the watchdog degradation"
        );
        let persisted_state = store
            .discovery_trusted_selection_state()?
            .expect("typed bridged state should remain intact");
        assert_eq!(
            persisted_state.selection_state,
            TrustedSelectionState::TrustedBridged
        );
        assert!(store
            .list_active_follow_wallets()?
            .contains("wallet_bridge"));
        Ok(())
    }

    #[test]
    fn clone_latest_trusted_wallet_metrics_bootstrap_force_stale_allows_stale_bridged_source_metadata(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-clone-latest-stale-bridged-source-metadata.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-15T12:14:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.decay_window_days = 5;
        config.follow_top_n = 1;
        config.min_score = 0.0;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.max_bootstrap_snapshot_age_seconds = 60 * 60;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let target_metrics_window_start = discovery.metrics_window_start(now);
        let source_metrics_window_start = target_metrics_window_start - Duration::hours(2);
        let source_snapshot_write = trusted_snapshot_write(
            TrustedSnapshotSourceKind::CloneLatestBridge,
            TrustedSelectionState::TrustedBridgedStale,
            source_metrics_window_start,
            now - Duration::minutes(2),
            1,
            Some("snapshot-source-root".to_string()),
            Some(source_metrics_window_start - Duration::hours(2)),
        );

        store.persist_discovery_cycle_with_snapshot_metadata(
            &[WalletUpsertRow {
                wallet_id: "wallet_stale_bridge".to_string(),
                first_seen: now - Duration::days(4),
                last_seen: now - Duration::minutes(1),
                status: "candidate".to_string(),
            }],
            &[WalletMetricRow {
                wallet_id: "wallet_stale_bridge".to_string(),
                window_start: source_metrics_window_start,
                pnl: 1.0,
                win_rate: 0.7,
                trades: 6,
                closed_trades: 3,
                hold_median_seconds: 300,
                score: 0.6,
                buy_total: 3,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            }],
            &[],
            false,
            false,
            now,
            "seed-stale-bridge-source",
            Some(&source_snapshot_write),
        )?;
        store
            .set_discovery_trusted_selection_bootstrap_required(true, "seed-stale-bridge-source")?;

        let cloned =
            discovery.clone_latest_trusted_bootstrap_wallet_metrics(&store, now, false, true)?;
        assert!(cloned.stale_source);
        assert!(cloned.forced_stale);
        assert_eq!(cloned.source_rows, 1);
        assert_eq!(cloned.inserted_rows, 1);

        let target_metadata = store
            .trusted_wallet_metrics_snapshot_metadata_for_window(target_metrics_window_start)?
            .expect("force_stale clone should write target snapshot metadata");
        assert_eq!(
            target_metadata.trust_state,
            TrustedSelectionState::TrustedBridgedStale
        );
        assert_eq!(
            target_metadata.source_snapshot_id,
            Some(source_snapshot_write.snapshot_id.clone())
        );
        assert_eq!(
            target_metadata.source_window_start,
            Some(source_metrics_window_start)
        );

        let state = store
            .discovery_trusted_selection_state()?
            .expect("clone tool should persist trusted selection state");
        assert!(state.bootstrap_required);
        assert_eq!(
            state.selection_state,
            TrustedSelectionState::TrustedBridgedStale
        );
        assert_eq!(state.active_snapshot_id, Some(target_metadata.snapshot_id));
        Ok(())
    }

    #[test]
    fn short_retention_bootstrap_does_not_republish_every_tick_or_repersist_metrics() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-short-retention-bootstrap-cadence.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let base_now = DateTime::parse_from_rfc3339("2026-03-08T13:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.observed_swaps_retention_days = 1;
        config.follow_top_n = 1;

        let persisted_bucket = base_now - Duration::days(config.scoring_window_days.max(1) as i64);
        store.upsert_wallet(
            "wallet_a",
            base_now - Duration::days(4),
            base_now - Duration::days(2),
            "candidate",
        )?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_a".to_string(),
            window_start: persisted_bucket,
            pnl: 2.4,
            win_rate: 0.85,
            trades: 16,
            closed_trades: 8,
            hold_median_seconds: 360,
            score: 0.81,
            buy_total: 8,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
        store.activate_follow_wallet("wallet_a", base_now - Duration::minutes(5), "test-seed")?;
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: base_now - Duration::days(2),
            slot: 42,
            signature: "cursor-short-retention-cadence".to_string(),
        })?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_now = base_now + Duration::minutes(31);
        let first_summary = discovery.run_cycle(&store, first_now)?;
        assert!(
            first_summary.published,
            "first bootstrap tick should publish"
        );
        assert!(
            !store.wallet_metrics_window_exists(discovery.metrics_window_start(first_now))?,
            "bootstrap-only cycle must not write a new wallet_metrics bucket from carried persisted snapshots"
        );

        let second_summary = discovery.run_cycle(&store, first_now + Duration::minutes(1))?;
        assert!(
            !second_summary.published,
            "bootstrap path must still respect refresh_seconds publish cadence"
        );
        assert!(
            !store.wallet_metrics_window_exists(
                discovery.metrics_window_start(first_now + Duration::minutes(1))
            )?,
            "bootstrap follow-up tick must not materialize synthetic wallet_metrics buckets"
        );
        Ok(())
    }

    #[test]
    #[ignore = "legacy trusted-bootstrap runtime contract replaced by publication truth"]
    fn short_retention_restart_prefers_trusted_persisted_selection_over_truncated_raw_window(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-short-retention-no-false-demote.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-08T13:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.observed_swaps_retention_days = 1;
        config.follow_top_n = 1;

        let metrics_window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        store.upsert_wallet(
            "wallet_a",
            now - Duration::days(4),
            now - Duration::days(2),
            "candidate",
        )?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_a".to_string(),
            window_start: metrics_window_start,
            pnl: 2.4,
            win_rate: 0.85,
            trades: 16,
            closed_trades: 8,
            hold_median_seconds: 360,
            score: 0.81,
            buy_total: 8,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
        store.activate_follow_wallet("wallet_a", now - Duration::minutes(5), "test-seed")?;
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::days(2),
            slot: 42,
            signature: "cursor-short-retention-raw".to_string(),
        })?;
        store.insert_observed_swap(&swap(
            "wallet_noise",
            "short-retention-raw-0",
            now - Duration::hours(2),
            SOL_MINT,
            "TokenShortRetentionNoise1111111111111111111",
            0.2,
            20.0,
        ))?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now + Duration::minutes(1))?;
        assert_eq!(
            summary.follow_promoted, 0,
            "trusted bootstrap must not promote new leaders from a truncated raw window"
        );
        assert_eq!(
            summary.follow_demoted, 0,
            "trusted persisted bootstrap should keep the trusted top-N wallet active instead of demoting from truncated raw data"
        );
        let active_after = store.list_active_follow_wallets()?;
        assert!(
            active_after.contains("wallet_a"),
            "trusted persisted bootstrap should keep wallet_a as the active top-N selection"
        );
        assert!(
            !active_after.contains("wallet_noise"),
            "noise wallet must not be activated from truncated raw data"
        );
        Ok(())
    }

    #[test]
    fn warm_restore_and_cursor_delta_keep_cache_ordered_before_cap_eviction() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-ordering-cap.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-04T14:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::minutes(40);
        for idx in 0..20 {
            let ts = start + Duration::minutes(idx as i64);
            store.insert_observed_swap(&swap(
                "wallet_mix",
                &format!("mix-sig-{idx:03}"),
                ts,
                SOL_MINT,
                "TokenMix1111111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
        }

        // Simulate persisted cursor far behind recent tail.
        let cursor = DiscoveryRuntimeCursor {
            ts_utc: start + Duration::minutes(5),
            slot: (start + Duration::minutes(5)).timestamp().max(0) as u64,
            signature: "mix-sig-005".to_string(),
        };
        store.upsert_discovery_runtime_cursor(&cursor)?;

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.max_window_swaps_in_memory = 5;
        config.max_fetch_swaps_per_cycle = 3;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let _ = discovery.run_cycle(&store, now)?;

        let guard = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        let signatures: Vec<String> = guard
            .swaps
            .iter()
            .map(|swap| swap.signature.clone())
            .collect();
        assert_eq!(signatures.len(), 5);
        assert_eq!(
            signatures,
            vec![
                "mix-sig-015".to_string(),
                "mix-sig-016".to_string(),
                "mix-sig-017".to_string(),
                "mix-sig-018".to_string(),
                "mix-sig-019".to_string(),
            ],
            "cache must keep latest swaps after ordering normalization + cap eviction"
        );
        Ok(())
    }

    #[test]
    fn rug_ratio_treats_unevaluated_buys_as_risky_until_they_mature() {
        let now = DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let buy_ts = now - Duration::seconds(30);

        let mut config = DiscoveryConfig::default();
        config.min_trades = 1;
        config.min_active_days = 1;
        config.min_leader_notional_sol = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.max_rug_ratio = 0.60;
        config.rug_lookahead_seconds = 300;
        let discovery = DiscoveryService::new(config, copybot_config::ShadowConfig::default());

        let mut acc = WalletAccumulator::default();
        acc.first_seen = Some(buy_ts);
        acc.last_seen = Some(buy_ts);
        acc.trades = 1;
        acc.max_buy_notional_sol = 1.0;
        acc.active_days.insert(buy_ts.date_naive());
        acc.buy_observations.push(BuyObservation {
            token: "TokenRecent11111111111111111111111111111111".to_string(),
            ts: buy_ts,
            tradable: true,
            quality_resolved: true,
        });

        let snapshot = discovery.snapshot_from_accumulator(
            "wallet_recent".to_string(),
            acc,
            now,
            &HashMap::new(),
        );

        assert!(
            (snapshot.rug_ratio - 1.0).abs() < 1e-9,
            "fresh unevaluated buys must count as risky until lookahead matures"
        );
        assert!(
            !snapshot.eligible,
            "wallet with only unevaluated buys must not pass rug gating as safe"
        );
    }

    #[test]
    fn rug_ratio_uses_total_buy_count_when_some_buys_are_still_unevaluated() {
        let now = DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let mut config = DiscoveryConfig::default();
        config.min_trades = 5;
        config.min_active_days = 1;
        config.min_leader_notional_sol = 0.1;
        config.min_buy_count = 5;
        config.min_tradable_ratio = 0.0;
        config.max_rug_ratio = 0.60;
        config.rug_lookahead_seconds = 300;
        config.thin_market_min_volume_sol = 1.0;
        config.thin_market_min_unique_traders = 1;
        let discovery = DiscoveryService::new(config, copybot_config::ShadowConfig::default());

        let mut acc = WalletAccumulator::default();
        acc.trades = 5;
        acc.max_buy_notional_sol = 1.0;

        let mut token_sol_history = HashMap::new();
        for idx in 0..4 {
            let buy_ts = now - Duration::minutes(20 + idx as i64);
            let token = format!("TokenMature{idx:02}");
            if acc.first_seen.is_none() {
                acc.first_seen = Some(buy_ts);
            }
            acc.last_seen = Some(
                acc.last_seen
                    .map(|current| current.max(buy_ts))
                    .unwrap_or(buy_ts),
            );
            acc.active_days.insert(buy_ts.date_naive());
            acc.buy_observations.push(BuyObservation {
                token: token.clone(),
                ts: buy_ts,
                tradable: true,
                quality_resolved: true,
            });
            token_sol_history.insert(
                token,
                vec![SolLegTrade {
                    ts: buy_ts + Duration::seconds(30),
                    wallet_id: format!("wallet-{idx}"),
                    sol_notional: 2.0,
                }],
            );
        }

        let fresh_buy_ts = now - Duration::seconds(60);
        acc.last_seen = Some(
            acc.last_seen
                .map(|current| current.max(fresh_buy_ts))
                .unwrap_or(fresh_buy_ts),
        );
        acc.active_days.insert(fresh_buy_ts.date_naive());
        acc.buy_observations.push(BuyObservation {
            token: "TokenFresh999999999999999999999999999999999".to_string(),
            ts: fresh_buy_ts,
            tradable: true,
            quality_resolved: true,
        });

        let snapshot = discovery.snapshot_from_accumulator(
            "wallet_mixed".to_string(),
            acc,
            now,
            &token_sol_history,
        );

        assert!(
            (snapshot.rug_ratio - 0.2).abs() < 1e-9,
            "one fresh buy out of five total buys must contribute to rug_ratio denominator"
        );
        assert!(
            snapshot.eligible,
            "a mostly healthy wallet should remain eligible when unevaluated buys stay below max_rug_ratio"
        );
    }

    #[test]
    fn tradable_ratio_soft_penalizes_deferred_quality_buys() {
        let now = DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let mut config = DiscoveryConfig::default();
        config.min_trades = 3;
        config.min_active_days = 1;
        config.min_leader_notional_sol = 0.1;
        config.min_buy_count = 3;
        config.min_tradable_ratio = 0.5;
        config.max_rug_ratio = 1.0;
        let discovery = DiscoveryService::new(config, copybot_config::ShadowConfig::default());

        let mut acc = WalletAccumulator::default();
        acc.first_seen = Some(now - Duration::minutes(10));
        acc.last_seen = Some(now);
        acc.trades = 3;
        acc.max_buy_notional_sol = 1.0;
        acc.active_days.insert(now.date_naive());
        acc.buy_observations.push(BuyObservation {
            token: "TokenTradable111111111111111111111111111111".to_string(),
            ts: now - Duration::minutes(10),
            tradable: true,
            quality_resolved: true,
        });
        acc.buy_observations.push(BuyObservation {
            token: "TokenRejected111111111111111111111111111111".to_string(),
            ts: now - Duration::minutes(9),
            tradable: false,
            quality_resolved: true,
        });
        acc.buy_observations.push(BuyObservation {
            token: "TokenDeferred11111111111111111111111111111".to_string(),
            ts: now - Duration::minutes(8),
            tradable: false,
            quality_resolved: false,
        });

        let snapshot = discovery.snapshot_from_accumulator(
            "wallet_tradability".to_string(),
            acc,
            now,
            &HashMap::new(),
        );

        let expected = 0.5 * (2.0_f64 / 3.0).sqrt();
        assert!(
            (snapshot.tradable_ratio - expected).abs() < 1e-9,
            "deferred buys must apply a soft penalty to tradable_ratio"
        );
        assert!(
            !snapshot.eligible,
            "deferred buys should no longer be neutral for min_tradable_ratio eligibility"
        );
    }

    #[test]
    fn tradable_ratio_blocks_wallet_when_most_buys_are_deferred() {
        let now = DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let mut config = DiscoveryConfig::default();
        config.min_trades = 10;
        config.min_active_days = 1;
        config.min_leader_notional_sol = 0.1;
        config.min_buy_count = 10;
        config.min_tradable_ratio = 0.5;
        config.max_rug_ratio = 1.0;
        let discovery = DiscoveryService::new(config, copybot_config::ShadowConfig::default());

        let mut acc = WalletAccumulator::default();
        acc.first_seen = Some(now - Duration::minutes(20));
        acc.last_seen = Some(now);
        acc.trades = 10;
        acc.max_buy_notional_sol = 1.0;
        acc.active_days.insert(now.date_naive());
        acc.buy_observations.push(BuyObservation {
            token: "TokenResolved11111111111111111111111111111".to_string(),
            ts: now - Duration::minutes(20),
            tradable: true,
            quality_resolved: true,
        });
        for idx in 0..9 {
            acc.buy_observations.push(BuyObservation {
                token: format!("TokenDeferred{idx:02}111111111111111111111111111"),
                ts: now - Duration::minutes(19 - idx as i64),
                tradable: false,
                quality_resolved: false,
            });
        }

        let snapshot = discovery.snapshot_from_accumulator(
            "wallet_mostly_deferred".to_string(),
            acc,
            now,
            &HashMap::new(),
        );

        assert!(
            (snapshot.tradable_ratio - 0.1_f64.sqrt()).abs() < 1e-9,
            "tradable_ratio should be penalized when most buys remain unresolved"
        );
        assert!(
            !snapshot.eligible,
            "wallet should not pass tradability gating when only a small minority of buys are resolved"
        );
    }

    #[test]
    fn max_rug_ratio_one_disables_rug_penalty_and_gate() {
        let now = DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let mut config = DiscoveryConfig::default();
        config.min_trades = 10;
        config.min_active_days = 3;
        config.min_leader_notional_sol = 0.5;
        config.min_buy_count = 10;
        config.min_tradable_ratio = 0.25;
        config.max_rug_ratio = 1.0;
        let discovery = DiscoveryService::new(config, copybot_config::ShadowConfig::default());

        let snapshot = discovery.snapshot_from_components(
            "wallet_rug_disabled".to_string(),
            now - Duration::days(3),
            now,
            16,
            3,
            10.0,
            3.5,
            1.2,
            5,
            5,
            &[114, 120, 98, 130, 117],
            &HashMap::new(),
            false,
            12,
            12,
            12,
            RugMetrics {
                evaluated: 12,
                rugged: 12,
                unevaluated: 0,
            },
            now,
        );

        assert!(
            snapshot.eligible,
            "rug gate must be bypassed at max_rug_ratio=1.0"
        );
        assert!(
            snapshot.score > 0.4,
            "rug penalty must no longer zero the score when emergency rug disable is active"
        );
        assert!(
            (snapshot.rug_ratio - 1.0).abs() < 1e-9,
            "the raw rug_ratio can stay 1.0 while the emergency override bypasses it"
        );
    }

    #[test]
    fn run_cycle_persists_wallet_metrics_only_once_per_snapshot_bucket() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-metric-bucket.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let buy_ts = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let sell_ts = buy_ts + Duration::minutes(6);
        for idx in 0..6 {
            let offset = Duration::minutes((idx * 20) as i64);
            store.insert_observed_swap(&swap(
                "wallet_bucket",
                &format!("bucket-buy-{idx}"),
                buy_ts + offset,
                SOL_MINT,
                "TokenBucket1111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_bucket",
                &format!("bucket-sell-{idx}"),
                sell_ts + offset,
                "TokenBucket1111111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.2,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.metric_snapshot_interval_seconds = 3600;
        config.observed_swaps_retention_days = 45;
        config.thin_market_min_unique_traders = 1;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_now = DateTime::parse_from_rfc3339("2026-03-04T12:40:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let second_now = first_now + Duration::minutes(10);
        let third_now = first_now + Duration::hours(1);

        let summary_first = discovery.run_cycle(&store, first_now)?;
        assert_eq!(summary_first.metrics_written, 1);
        let first_window = store
            .latest_wallet_metrics_window_start()?
            .expect("expected first metrics window to persist");

        let summary_second = discovery.run_cycle(&store, second_now)?;
        assert_eq!(
            summary_second.metrics_written, 0,
            "same snapshot bucket must not rewrite wallet_metrics"
        );
        let second_window = store
            .latest_wallet_metrics_window_start()?
            .expect("metrics window should remain available");
        assert_eq!(second_window, first_window);

        let summary_third = discovery.run_cycle(&store, third_now)?;
        assert_eq!(summary_third.metrics_written, 1);
        let third_window = store
            .latest_wallet_metrics_window_start()?
            .expect("next snapshot bucket must persist a new wallet_metrics window");
        assert!(third_window > second_window);
        Ok(())
    }

    #[test]
    fn run_cycle_persists_wallet_metrics_after_scoring_window_change_moves_window_backward(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-metric-window-config-change.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let buy_ts = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let sell_ts = buy_ts + Duration::minutes(6);
        for idx in 0..6 {
            let offset = Duration::minutes((idx * 20) as i64);
            store.insert_observed_swap(&swap(
                "wallet_config_shift",
                &format!("shift-buy-{idx}"),
                buy_ts + offset,
                SOL_MINT,
                "TokenShift11111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_config_shift",
                &format!("shift-sell-{idx}"),
                sell_ts + offset,
                "TokenShift11111111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.2,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.observed_swaps_retention_days = 30;
        config.metric_snapshot_interval_seconds = 3600;
        config.thin_market_min_unique_traders = 1;

        let now = DateTime::parse_from_rfc3339("2026-03-04T12:40:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let discovery_initial = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let summary_initial = discovery_initial.run_cycle(&store, now)?;
        assert_eq!(summary_initial.metrics_written, 1);

        let first_window = store
            .latest_wallet_metrics_window_start()?
            .expect("expected first metrics window to persist");

        config.scoring_window_days = 30;
        config.decay_window_days = 30;
        let discovery_shifted = DiscoveryService::new(config, permissive_shadow_quality());
        let summary_shifted = discovery_shifted.run_cycle(&store, now)?;
        assert_eq!(
            summary_shifted.metrics_written, 1,
            "a backward-shifted metrics window caused by config change must still persist"
        );

        let second_window = store
            .latest_wallet_metrics_window_start()?
            .expect("expected second metrics window to persist");
        assert_eq!(
            second_window, first_window,
            "an older config-shifted bucket should not advance the global MAX(window_start)"
        );
        assert!(
            store.wallet_metrics_window_exists(discovery_shifted.metrics_window_start(now))?,
            "the backward-shifted metrics bucket must still be inserted"
        );
        Ok(())
    }

    #[test]
    fn run_cycle_defers_full_snapshot_recompute_until_next_snapshot_bucket() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-snapshot-recompute-cadence.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let base_ts = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        for idx in 0..6 {
            let offset = Duration::minutes((idx * 20) as i64);
            let buy_ts = base_ts + offset;
            let sell_ts = buy_ts + Duration::minutes(6);
            store.insert_observed_swap(&swap(
                "wallet_recompute_a",
                &format!("recompute-a-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenRecomputeA111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_recompute_a",
                &format!("recompute-a-sell-{idx}"),
                sell_ts,
                "TokenRecomputeA111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.2,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.follow_top_n = 2;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.metric_snapshot_interval_seconds = 3600;
        config.thin_market_min_unique_traders = 1;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_now = DateTime::parse_from_rfc3339("2026-03-04T15:40:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let second_now = first_now + Duration::minutes(10);
        let third_now = first_now + Duration::hours(1);

        let summary_first = discovery.run_cycle(&store, first_now)?;
        assert_eq!(summary_first.wallets_seen, 1);
        assert_eq!(summary_first.metrics_written, 1);

        for idx in 0..6 {
            let offset = Duration::minutes((idx * 20) as i64);
            let buy_ts = base_ts + Duration::minutes(5) + offset;
            let sell_ts = buy_ts + Duration::minutes(6);
            store.insert_observed_swap(&swap(
                "wallet_recompute_b",
                &format!("recompute-b-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenRecomputeB111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_recompute_b",
                &format!("recompute-b-sell-{idx}"),
                sell_ts,
                "TokenRecomputeB111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.2,
            ))?;
        }

        let summary_second = discovery.run_cycle(&store, second_now)?;
        assert_eq!(
            summary_second.wallets_seen, 1,
            "same snapshot bucket should reuse cached discovery summary instead of full recompute"
        );
        assert_eq!(summary_second.metrics_written, 0);

        let summary_third = discovery.run_cycle(&store, third_now)?;
        assert_eq!(
            summary_third.wallets_seen, 2,
            "next snapshot bucket must recompute and include swaps accumulated while cached"
        );
        assert_eq!(summary_third.metrics_written, 2);
        Ok(())
    }

    #[test]
    fn cap_truncation_temporarily_suppresses_false_followlist_demotions_before_guard_expires(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-cap-truncation-followlist-suppression.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let base_ts = DateTime::parse_from_rfc3339("2026-03-04T10:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        for idx in 0..4 {
            let buy_ts = base_ts + Duration::minutes((idx * 20) as i64);
            let sell_ts = buy_ts + Duration::minutes(6);
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("leader-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenLeader111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("leader-sell-{idx}"),
                sell_ts,
                "TokenLeader111111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.3,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.metric_snapshot_interval_seconds = 60;
        config.max_window_swaps_in_memory = 8;
        config.max_fetch_swaps_per_cycle = 100;
        config.thin_market_min_unique_traders = 1;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_now = DateTime::parse_from_rfc3339("2026-03-04T15:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let second_now = first_now + Duration::minutes(1);

        let first_summary = discovery.run_cycle(&store, first_now)?;
        assert!(
            first_summary.follow_promoted >= 1,
            "seed cycle should promote the profitable leader"
        );
        let active_before = store.list_active_follow_wallets()?;
        assert!(active_before.contains("wallet_leader"));

        for idx in 0..8 {
            let ts = first_now + Duration::seconds((idx + 1) as i64);
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("noise-buy-{idx}"),
                ts,
                SOL_MINT,
                "TokenNoise11111111111111111111111111111111",
                0.2,
                20.0,
            ))?;
        }

        let second_summary = discovery.run_cycle(&store, second_now)?;
        assert_eq!(
            second_summary.follow_demoted, 0,
            "first cap-truncated recompute must still suppress followlist demotions while the bounded guard is active"
        );
        assert!(
            second_summary.raw_window_cap_truncated,
            "cap-truncated raw recompute summary must report that the raw window is still truncated"
        );
        assert!(
            second_summary.cap_truncation_deactivation_guard_active,
            "cap-truncated raw recompute summary must report active temporary deactivation suppression"
        );
        assert_eq!(
            second_summary.cap_truncation_deactivation_guard_reason,
            Some("live_cap_eviction"),
            "summary must expose why cap-truncation suppression is active"
        );
        assert!(
            second_summary.cap_truncation_floor_signature.is_some(),
            "summary must expose the retained truncation floor signature for diagnostics"
        );
        assert_eq!(
            second_summary.scoring_source, "published_universe_raw_window_degraded",
            "cap-truncated recompute now degrades onto the last published universe instead of rotating from partial raw data"
        );
        assert_eq!(second_summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
        let active_after = store.list_active_follow_wallets()?;
        assert!(
            active_after.contains("wallet_leader"),
            "leader must remain active while discovery window is known truncated by the cap"
        );
        let state = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(
            state.cap_truncation_floor.is_some(),
            "cap eviction should leave a truncation marker while raw history remains incomplete"
        );
        Ok(())
    }

    #[test]
    fn cap_truncation_keeps_followlist_demotions_suppressed_while_raw_window_remains_incomplete(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-cap-truncation-followlist-guard-expiry.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let base_ts = DateTime::parse_from_rfc3339("2026-03-04T10:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        for idx in 0..4 {
            let buy_ts = base_ts + Duration::minutes((idx * 20) as i64);
            let sell_ts = buy_ts + Duration::minutes(6);
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("leader-guard-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenLeaderGuard1111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("leader-guard-sell-{idx}"),
                sell_ts,
                "TokenLeaderGuard1111111111111111111111111",
                SOL_MINT,
                100.0,
                1.3,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.metric_snapshot_interval_seconds = 60;
        config.max_window_swaps_in_memory = 8;
        config.max_fetch_swaps_per_cycle = 100;
        config.thin_market_min_unique_traders = 1;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_now = DateTime::parse_from_rfc3339("2026-03-04T15:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let first_summary = discovery.run_cycle(&store, first_now)?;
        assert!(
            first_summary.follow_promoted >= 1,
            "seed cycle should promote the profitable leader"
        );
        assert!(store
            .list_active_follow_wallets()?
            .contains("wallet_leader"));

        for idx in 0..8 {
            let ts = first_now + Duration::seconds((idx + 1) as i64);
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("noise-guard-buy-{idx}"),
                ts,
                SOL_MINT,
                "TokenNoiseGuard11111111111111111111111111",
                0.2,
                20.0,
            ))?;
        }

        let second_summary = discovery.run_cycle(&store, first_now + Duration::minutes(1))?;
        assert_eq!(
            second_summary.follow_demoted, 0,
            "first cap-truncated cycle should still honor the temporary deactivation guard"
        );

        let third_summary = discovery.run_cycle(&store, first_now + Duration::minutes(2))?;
        assert_eq!(
            third_summary.follow_demoted, 0,
            "second cap-truncated cycle should consume the remaining temporary deactivation guard"
        );

        let fourth_summary = discovery.run_cycle(&store, first_now + Duration::minutes(3))?;
        assert_eq!(
            fourth_summary.follow_demoted, 0,
            "cap-truncated raw discovery must not demote followlist entries while the retained raw window still covers only a tiny fraction of the intended scoring horizon"
        );
        let active_after = store.list_active_follow_wallets()?;
        assert!(
            active_after.contains("wallet_leader"),
            "leader must stay active while cap-truncated raw discovery still represents incomplete history"
        );
        let state = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(
            state.cap_truncation_floor.is_some(),
            "raw-window truncation floor should still describe the incomplete history gap after the guard expires"
        );
        assert_eq!(
            state.cap_truncation_deactivation_guard_cycles_remaining, 0,
            "bounded countdown should still reach zero even when safety suppression remains active for an incomplete raw window"
        );
        assert!(
            fourth_summary.cap_truncation_deactivation_guard_active,
            "summary must continue advertising deactivation suppression while the raw window remains incomplete after the bounded countdown expires"
        );
        Ok(())
    }

    #[test]
    fn cap_truncated_partial_raw_window_suppresses_followlist_promotions_and_metrics_even_after_most_of_horizon_is_retained(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-cap-truncation-activation-boundary.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let window_start = now - Duration::days(1);

        for (idx, offset_minutes) in [5, 45].into_iter().enumerate() {
            let buy_ts = window_start + Duration::minutes(offset_minutes);
            let sell_ts = buy_ts + Duration::minutes(10);
            let token = format!("TokenLeaderBoundary{idx:02}111111111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("leader-boundary-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                token.as_str(),
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("leader-boundary-sell-{idx}"),
                sell_ts,
                token.as_str(),
                SOL_MINT,
                100.0,
                1.3,
            ))?;
        }

        for (idx, offset_minutes) in [125, 485, 845, 1380].into_iter().enumerate() {
            let buy_ts = window_start + Duration::minutes(offset_minutes);
            let sell_ts = buy_ts + Duration::minutes(10);
            let token = format!("TokenCandidateBoundary{idx:02}11111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_candidate",
                &format!("candidate-boundary-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                token.as_str(),
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_candidate",
                &format!("candidate-boundary-sell-{idx}"),
                sell_ts,
                token.as_str(),
                SOL_MINT,
                100.0,
                1.35,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 1;
        config.decay_window_days = 1;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.metric_snapshot_interval_seconds = 60;
        config.max_window_swaps_in_memory = 8;
        config.max_fetch_swaps_per_cycle = 100;
        config.thin_market_min_unique_traders = 1;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;
        assert!(
            summary.raw_window_cap_truncated,
            "the retained raw window must still advertise cap truncation while the earliest leader slice is missing"
        );
        assert_eq!(
            summary.follow_promoted, 0,
            "cap-truncated raw recompute must not promote from a partial tail even when the retained tail spans most of the scoring horizon"
        );
        assert_eq!(
            summary.metrics_written, 0,
            "cap-truncated raw recompute must not persist wallet_metrics from a partial tail even when the retained span exceeds the old coverage heuristic"
        );

        let active_follow_wallets = store.list_active_follow_wallets()?;
        assert!(
            !active_follow_wallets.contains("wallet_candidate"),
            "partial-tail candidate must not activate while raw discovery is still source-invalid"
        );
        assert!(
            active_follow_wallets.is_empty(),
            "cap-truncated raw recompute must not publish a new active follow universe from an incomplete tail"
        );
        assert_eq!(
            store.latest_wallet_metrics_window_start()?,
            None,
            "partial raw discovery should not publish a fresh wallet_metrics bucket while cap truncation remains active"
        );
        let state = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(
            state.cap_truncation_floor.is_some(),
            "activation suppression should remain tied to the actual truncation marker rather than a coverage heuristic"
        );
        Ok(())
    }

    #[test]
    fn warm_restore_keeps_followlist_demotions_suppressed_while_raw_window_remains_truncated_after_guard_countdown(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-warm-restore-capped-tail-demotion-guard.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-13T08:21:30Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        for idx in 0..4 {
            let buy_ts = now - Duration::minutes(40) + Duration::minutes((idx * 4) as i64);
            let sell_ts = buy_ts + Duration::minutes(2);
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("restart-leader-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenRestartLeader111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("restart-leader-sell-{idx}"),
                sell_ts,
                "TokenRestartLeader111111111111111111111111",
                SOL_MINT,
                100.0,
                1.3,
            ))?;
        }

        let mut latest_noise_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..9 {
            let ts = now - Duration::minutes(9) + Duration::minutes(idx as i64);
            let signature = format!("restart-noise-buy-{idx}");
            let swap = swap(
                "wallet_noise",
                signature.as_str(),
                ts,
                SOL_MINT,
                "TokenRestartNoise1111111111111111111111111",
                0.2,
                20.0,
            );
            latest_noise_cursor = Some(DiscoveryRuntimeCursor {
                ts_utc: swap.ts_utc,
                slot: swap.slot,
                signature: swap.signature.clone(),
            });
            store.insert_observed_swap(&swap)?;
        }

        store.activate_follow_wallet("wallet_leader", now - Duration::minutes(1), "seed-follow")?;
        store.upsert_discovery_runtime_cursor(
            &latest_noise_cursor.expect("latest noise cursor should be present"),
        )?;

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.metric_snapshot_interval_seconds = 60;
        config.max_window_swaps_in_memory = 8;
        config.max_fetch_swaps_per_cycle = 100;
        config.thin_market_min_unique_traders = 1;

        let metrics_window_start = {
            let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
            let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
            let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
            bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
        };
        store.upsert_wallet(
            "wallet_leader",
            now - Duration::days(2),
            now - Duration::minutes(1),
            "candidate",
        )?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_leader".to_string(),
            window_start: metrics_window_start,
            pnl: 2.4,
            win_rate: 0.85,
            trades: 8,
            closed_trades: 4,
            hold_median_seconds: 360,
            score: 0.81,
            buy_total: 4,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
        let published_wallets = HashSet::from(["wallet_leader".to_string()]);
        seed_recent_published_universe(
            &store,
            now - Duration::seconds(30),
            metrics_window_start,
            "raw_window",
            &published_wallets,
        )?;

        let discovery_after_restart = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery_after_restart.run_cycle(&store, now)?;
        assert_eq!(
            summary.metrics_written, 0,
            "warm-restored capped tail should not persist a fresh wallet_metrics bucket from partial raw data"
        );
        assert_eq!(
            summary.follow_promoted, 0,
            "warm-restored capped tail should not promote from partial raw data"
        );
        assert_eq!(
            summary.follow_demoted, 0,
            "warm-restore on an already capped recent tail must suppress false followlist demotions"
        );
        assert!(
            summary.raw_window_cap_truncated,
            "warm-restored capped-tail bootstrap summary must report that raw history remains truncated"
        );
        assert!(
            summary.cap_truncation_deactivation_guard_active,
            "warm-restored capped-tail bootstrap summary must report that the temporary deactivation guard is active"
        );
        assert_eq!(
            summary.cap_truncation_deactivation_guard_reason,
            Some("warm_load_truncated"),
            "warm-restored capped-tail bootstrap summary must expose the warm-load truncation reason"
        );
        assert_eq!(
            summary.scoring_source,
            "published_universe_raw_window_degraded",
            "warm-restored capped-tail restart must degrade to the last published universe while raw history remains truncated"
        );
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
        assert_eq!(summary.eligible_wallets, 1);
        let active_after = store.list_active_follow_wallets()?;
        assert!(
            active_after.contains("wallet_leader"),
            "existing followed wallet must remain active on first post-restart recompute when warm slice is already truncated"
        );
        assert_eq!(
            store.latest_wallet_metrics_window_start()?,
            Some(metrics_window_start),
            "bootstrap on truncated warm restore must not write a newer wallet_metrics bucket from partial raw data"
        );
        let state = discovery_after_restart
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert_eq!(state.swaps.len(), 8);
        assert!(
            state.cap_truncation_floor.is_some(),
            "warm-restore capped tail must immediately latch truncation marker"
        );
        assert!(
            !state.truncated_warm_restore_bootstrap,
            "warm-restore degraded mode should not leave the legacy persisted-bootstrap bridge armed"
        );
        assert_eq!(
            state
                .cap_truncation_floor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some("restart-noise-buy-1"),
            "warm-restore truncation floor should point at the oldest retained row"
        );
        drop(state);

        let summary_follow_up =
            discovery_after_restart.run_cycle(&store, now + Duration::minutes(1))?;
        assert_eq!(
            summary_follow_up.follow_demoted, 0,
            "immediate follow-up raw recompute should still honor the bounded cap-truncation deactivation guard"
        );
        let summary_guard_expired =
            discovery_after_restart.run_cycle(&store, now + Duration::minutes(2))?;
        assert_eq!(
            summary_guard_expired.follow_demoted, 0,
            "warm-restored truncated raw discovery must keep deactivations suppressed while the retained raw window is still incomplete"
        );
        let active_after_guard_expiry = store.list_active_follow_wallets()?;
        assert!(
            active_after_guard_expiry.contains("wallet_leader"),
            "warm-restored followlist entries must not collapse while the raw window still represents only the capped tail"
        );
        assert!(
            store.latest_wallet_metrics_window_start()? == Some(metrics_window_start),
            "while raw history remains incomplete after warm restore, discovery must not persist a newer wallet_metrics snapshot from the capped tail"
        );
        let state_after_guard_expiry = discovery_after_restart
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert_eq!(
            state_after_guard_expiry.cap_truncation_deactivation_guard_cycles_remaining,
            0,
            "warm-restore countdown should still reach zero even when incomplete raw history keeps deactivations suppressed"
        );
        assert!(
            summary_guard_expired.cap_truncation_deactivation_guard_active,
            "summary must continue exposing active deactivation suppression while the warm-restored raw window remains truncated"
        );
        Ok(())
    }

    #[test]
    fn build_wallet_snapshots_normalizes_out_of_order_swaps_before_rug_partition_point(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-out-of-order-rug-history.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let buy_ts = DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut swaps = VecDeque::new();
        swaps.push_back(swap(
            "wallet_target",
            "target-buy",
            buy_ts,
            SOL_MINT,
            "TokenOrder11111111111111111111111111111111",
            1.0,
            100.0,
        ));
        swaps.push_back(swap(
            "wallet_post",
            "post-sell",
            buy_ts + Duration::minutes(1),
            "TokenOrder11111111111111111111111111111111",
            SOL_MINT,
            100.0,
            0.01,
        ));
        swaps.push_back(swap(
            "wallet_pre",
            "pre-sell",
            buy_ts - Duration::minutes(1),
            "TokenOrder11111111111111111111111111111111",
            SOL_MINT,
            100.0,
            10.0,
        ));

        let mut config = DiscoveryConfig::default();
        config.rug_lookahead_seconds = 300;
        config.thin_market_min_volume_sol = 2.0;
        config.thin_market_min_unique_traders = 1;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());

        let snapshots = discovery.build_wallet_snapshots_from_cached(
            &store,
            &swaps,
            buy_ts + Duration::minutes(10),
        )?;
        let target_snapshot = snapshots
            .into_iter()
            .find(|snapshot| snapshot.wallet_id == "wallet_target")
            .expect("target wallet snapshot must exist");

        assert!(
            (target_snapshot.rug_ratio - 1.0).abs() < 1e-9,
            "pre-buy trades that appear later in an unsorted swap window must not leak into rug lookahead volume"
        );
        Ok(())
    }

    #[test]
    fn build_wallet_snapshots_uses_persisted_activity_days_for_eligibility() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("persisted-activity-days.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let wallet_id = "wallet_active_days";
        store.upsert_wallet_activity_days(&[
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: (now - Duration::days(3)).date_naive(),
                last_seen: now - Duration::days(3),
            },
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: (now - Duration::days(2)).date_naive(),
                last_seen: now - Duration::days(2),
            },
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: (now - Duration::days(1)).date_naive(),
                last_seen: now - Duration::days(1),
            },
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: now.date_naive(),
                last_seen: now,
            },
        ])?;

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.min_trades = 1;
        config.min_active_days = 4;
        config.min_leader_notional_sol = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.max_rug_ratio = 1.0;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());

        let swaps = VecDeque::from([swap(
            wallet_id,
            "sig-active-days-1",
            now,
            SOL_MINT,
            "TokenActiveDays1111111111111111111111111111",
            1.0,
            100.0,
        )]);
        let snapshots = discovery.build_wallet_snapshots_from_cached(&store, &swaps, now)?;
        let snapshot = snapshots.into_iter().next().context("expected snapshot")?;

        assert!(
            snapshot.eligible,
            "persisted day-level activity should satisfy min_active_days even when the capped tail only contains one day"
        );
        Ok(())
    }

    #[test]
    fn discovery_wallet_activity_day_count_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(discovery_wallet_activity_day_count_error_requires_abort(
            &error
        ));
    }

    #[test]
    fn discovery_wallet_activity_day_count_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!discovery_wallet_activity_day_count_error_requires_abort(
            &error
        ));
    }

    #[test]
    fn run_cycle_uses_existing_persisted_activity_days_for_eligibility() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-eligibility.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let wallet_id = "wallet_backfill";
        store.upsert_wallet_activity_days(&[
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: (now - Duration::days(6)).date_naive(),
                last_seen: now - Duration::days(6),
            },
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: (now - Duration::days(4)).date_naive(),
                last_seen: now - Duration::days(4),
            },
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: (now - Duration::days(2)).date_naive(),
                last_seen: now - Duration::days(2),
            },
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: now.date_naive(),
                last_seen: now,
            },
        ])?;

        store.insert_observed_swap(&swap(
            wallet_id,
            "backfill-eligibility-0",
            now,
            SOL_MINT,
            "TokenBackfillElig11111111111111111111111111",
            1.0,
            100.0,
        ))?;

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.min_trades = 1;
        config.min_active_days = 4;
        config.min_leader_notional_sol = 0.1;
        config.min_buy_count = 1;
        config.min_score = 0.0;
        config.min_tradable_ratio = 0.0;
        config.max_rug_ratio = 1.0;
        config.max_fetch_swaps_per_cycle = 1;
        config.max_fetch_pages_per_cycle = 1;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());

        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.eligible_wallets, 1);

        let counts = store
            .wallet_active_day_counts_since(&[wallet_id.to_string()], now - Duration::days(7))?;
        assert_eq!(
            counts.get(wallet_id),
            Some(&4),
            "persisted wallet_activity_days should satisfy eligibility even when the in-memory tail remains short"
        );
        Ok(())
    }

    #[test]
    fn run_cycle_persists_in_band_wallet_freshness_capture_without_standalone_raw_rebuild(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage3-in-band-capture.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-25T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.scoring_aggregates_enabled = true;
        config.scoring_aggregates_write_enabled = true;
        seed_runtime_aggregate_ready_wallet(
            &store,
            &config,
            "wallet_aggregate_only",
            "TokenAggregateOnlyStage31111111111111111",
            now,
        )?;
        seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;
        insert_recent_profitable_pair(&store, "wallet_top", now, "stage3-in-band-capture-current")?;
        insert_shadow_recorded_signal(
            &store,
            "wallet_top",
            "TokenStage3Shadow1111111111111111111111111",
            now - Duration::seconds(30),
            "shadow:stage3:wallet-top",
        )?;

        reset_current_raw_truth_sample_call_count_for_tests();
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(summary.wallet_freshness_capture_state, Some("persisted"));
        assert_eq!(summary.wallet_freshness_capture_reason, None);
        assert_eq!(current_raw_truth_sample_call_count_for_tests(), 0);

        let captures = store.list_discovery_wallet_freshness_captures(5)?;
        assert_eq!(captures.len(), 1);
        assert_eq!(
            summary.wallet_freshness_capture_id,
            Some(captures[0].capture_id)
        );
        let capture = wallet_freshness_capture_from_row(captures[0].clone())?;
        assert_eq!(capture.audit.verdict.as_str(), "fresh_current");
        assert_eq!(
            capture.audit.published_wallet_ids,
            vec!["wallet_top".to_string()]
        );
        assert_eq!(
            capture.audit.active_follow_wallet_ids,
            vec!["wallet_top".to_string()]
        );
        assert_eq!(
            capture.audit.current_raw_top_wallet_ids,
            vec!["wallet_top".to_string()]
        );
        assert!(
            !capture
                .audit
                .current_raw_top_wallet_ids
                .contains(&"wallet_aggregate_only".to_string()),
            "in-band capture must stay anchored to raw-window truth even when aggregate state exists"
        );
        assert_eq!(
            capture
                .shadow_signal
                .selected_wallets_with_recent_shadow_signal,
            1
        );
        Ok(())
    }

    #[test]
    fn run_cycle_capture_persistence_failure_is_fail_open_for_publication() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage3-in-band-capture-fail-open.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-25T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;
        insert_recent_profitable_pair(
            &store,
            "wallet_top",
            now,
            "stage3-in-band-capture-fail-open-current",
        )?;
        insert_shadow_recorded_signal(
            &store,
            "wallet_top",
            "TokenStage3ShadowFailOpen1111111111111111111",
            now - Duration::seconds(30),
            "shadow:stage3:wallet-top-fail-open",
        )?;

        assert!(store
            .list_discovery_wallet_freshness_captures(1)?
            .is_empty());
        let conn = Connection::open(Path::new(&db_path))?;
        conn.execute_batch(
            "CREATE TRIGGER fail_discovery_wallet_freshness_capture_insert
             BEFORE INSERT ON discovery_wallet_freshness_history
             BEGIN
                 SELECT RAISE(FAIL, 'simulated wallet freshness capture persistence failure');
             END;",
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(
            summary.wallet_freshness_capture_state,
            Some("persistence_failed")
        );
        assert!(summary
            .wallet_freshness_capture_reason
            .as_deref()
            .is_some_and(
                |reason| reason.contains("simulated wallet freshness capture persistence failure")
            ));
        assert_eq!(store.list_discovery_wallet_freshness_captures(5)?.len(), 0);
        assert!(
            discovery
                .recent_published_follow_universe_wallets(&store, now)?
                .is_some_and(|wallets| wallets.contains("wallet_top")),
            "capture persistence failure must not block exact publication truth"
        );
        assert!(
            store.list_active_follow_wallets()?.contains("wallet_top"),
            "capture persistence failure must not block active follow updates"
        );
        Ok(())
    }

    #[test]
    fn wallet_freshness_history_report_reads_in_band_captures_from_run_cycle() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage3-in-band-capture-history.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let first_now = DateTime::parse_from_rfc3339("2026-03-25T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let second_now = first_now + Duration::minutes(10);
        let third_now = first_now + Duration::minutes(20);
        let config = stage1_runtime_config();
        seed_stage1_persisted_stream_runtime_fixture(&store, &config, first_now, 6, 9)?;
        insert_recent_profitable_pair(
            &store,
            "wallet_top",
            first_now,
            "stage3-in-band-history-current-1",
        )?;
        insert_shadow_recorded_signal(
            &store,
            "wallet_top",
            "TokenStage3ShadowHistory1111111111111111111",
            first_now - Duration::seconds(30),
            "shadow:stage3:wallet-top-history-1",
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_summary = discovery.run_cycle(&store, first_now)?;
        assert_eq!(
            first_summary.wallet_freshness_capture_state,
            Some("persisted")
        );

        insert_recent_tail_noise_swaps(
            &store,
            first_now + Duration::minutes(1),
            10,
            "stage3-in-band-history-2",
        )?;
        insert_recent_profitable_pair(
            &store,
            "wallet_top",
            second_now,
            "stage3-in-band-history-current-2",
        )?;
        insert_shadow_recorded_signal(
            &store,
            "wallet_top",
            "TokenStage3ShadowHistory1111111111111111111",
            second_now - Duration::seconds(30),
            "shadow:stage3:wallet-top-history-2",
        )?;
        let second_summary = discovery.run_cycle(&store, second_now)?;
        assert_eq!(
            second_summary.wallet_freshness_capture_state,
            Some("persisted")
        );

        insert_recent_tail_noise_swaps(
            &store,
            second_now + Duration::minutes(1),
            10,
            "stage3-in-band-history-3",
        )?;
        insert_recent_profitable_pair(
            &store,
            "wallet_top",
            third_now,
            "stage3-in-band-history-current-3",
        )?;
        insert_shadow_recorded_signal(
            &store,
            "wallet_top",
            "TokenStage3ShadowHistory1111111111111111111",
            third_now - Duration::seconds(30),
            "shadow:stage3:wallet-top-history-3",
        )?;
        let third_summary = discovery.run_cycle(&store, third_now)?;
        assert_eq!(
            third_summary.wallet_freshness_capture_state,
            Some("persisted")
        );

        let report = discovery.wallet_freshness_history_report(&store, third_now, 5)?;

        assert_eq!(report.captures_loaded, 3);
        assert_eq!(report.captures_considered, 3);
        assert_eq!(report.captures_within_recent_horizon, 3);
        assert_eq!(
            report.verdict,
            WalletFreshnessHistoryVerdict::PartiallyValidatedButLowRotation
        );
        assert_eq!(report.shadow_signal_present_capture_count, 3);
        assert_eq!(report.current_raw_change_count, 0);
        assert_eq!(report.active_follow_change_count, 0);
        Ok(())
    }

    #[test]
    fn aggregate_ready_state_does_not_override_raw_window_truth_on_restart() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-ready-raw-truth-restart.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.scoring_aggregates_enabled = true;
        config.scoring_aggregates_write_enabled = true;

        seed_runtime_aggregate_ready_wallet(
            &store,
            &config,
            "wallet_raw_truth",
            "TokenAggregateReadyRawTruth111111111111111",
            now,
        )?;

        let first = DiscoveryService::new(config.clone(), permissive_shadow_quality())
            .run_cycle(&store, now)?;
        let second =
            DiscoveryService::new(config, permissive_shadow_quality()).run_cycle(&store, now)?;

        for summary in [&first, &second] {
            assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
            assert_eq!(summary.scoring_source, "raw_window");
            assert_eq!(summary.active_follow_wallets, 1);
            assert!(
                summary
                    .top_wallets
                    .iter()
                    .any(|label| label.starts_with("wallet_raw_truth:")),
                "restart must keep using raw-window truth even when aggregate state is ready"
            );
        }
        assert!(
            store
                .list_active_follow_wallets()?
                .contains("wallet_raw_truth"),
            "raw-window winner should stay active after restart"
        );
        Ok(())
    }

    #[test]
    fn aggregate_ready_state_degrades_to_recent_publication_truth_on_restart() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-ready-degraded-restart.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.follow_top_n = 1;
        config.scoring_aggregates_enabled = true;
        config.scoring_aggregates_write_enabled = true;

        seed_runtime_aggregate_ready_wallet(
            &store,
            &config,
            "wallet_aggregate_only",
            "TokenAggregateOnlyDegraded111111111111111",
            now,
        )?;
        store.delete_observed_swaps_before(now + Duration::seconds(1))?;
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let expected_active_wallets =
            seed_published_wallet_metrics_snapshot(&store, metrics_window_start, 4, 1)?;
        seed_recent_published_universe(
            &store,
            now - Duration::seconds(30),
            metrics_window_start,
            "raw_window",
            &expected_active_wallets,
        )?;

        let first = DiscoveryService::new(config.clone(), permissive_shadow_quality())
            .run_cycle(&store, now)?;
        let second =
            DiscoveryService::new(config, permissive_shadow_quality()).run_cycle(&store, now)?;

        for summary in [&first, &second] {
            assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
            assert_eq!(
                summary.scoring_source,
                "published_universe_raw_window_unavailable"
            );
            assert_eq!(summary.active_follow_wallets, expected_active_wallets.len());
        }
        assert_eq!(store.list_active_follow_wallets()?, expected_active_wallets);
        Ok(())
    }

    #[test]
    fn aggregate_ready_state_does_not_override_fail_closed_truth_on_restart() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-ready-fail-closed-restart.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.scoring_aggregates_enabled = true;
        config.scoring_aggregates_write_enabled = true;

        seed_runtime_aggregate_ready_wallet(
            &store,
            &config,
            "wallet_aggregate_only_fail_closed",
            "TokenAggregateOnlyFailClosed1111111111111",
            now,
        )?;
        store.delete_observed_swaps_before(now + Duration::seconds(1))?;
        store.activate_follow_wallet(
            "wallet_stale_follow",
            now - Duration::minutes(1),
            "seed-follow",
        )?;

        let first = DiscoveryService::new(config.clone(), permissive_shadow_quality())
            .run_cycle(&store, now)?;
        let second =
            DiscoveryService::new(config, permissive_shadow_quality()).run_cycle(&store, now)?;

        for summary in [&first, &second] {
            assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
            assert_eq!(
                summary.scoring_source,
                "raw_window_unusable_no_recent_published_universe"
            );
            assert_eq!(summary.active_follow_wallets, 0);
        }
        assert!(store.list_active_follow_wallets()?.is_empty());
        Ok(())
    }

    #[test]
    fn aggregate_readiness_status_reports_default_disabled_blockers() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-readiness-default.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-16T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let discovery =
            DiscoveryService::new(DiscoveryConfig::default(), permissive_shadow_quality());

        let status = discovery.aggregate_readiness_status(&store, now)?;
        assert_eq!(
            status.write_blockers,
            vec![
                AggregateReadinessBlocker::WritesDisabledByConfig,
                AggregateReadinessBlocker::MissingCoveredThroughCursor,
            ]
        );
        assert_eq!(
            status.read_blockers,
            vec![
                AggregateReadinessBlocker::ReadsDisabledByConfig,
                AggregateReadinessBlocker::MissingCoveredSince,
                AggregateReadinessBlocker::MissingCoveredThroughCursor,
            ]
        );
        assert!(!status.storage_ready_for_runtime_gate);
        assert!(!status.effective_writes_ready);
        assert!(!status.effective_reads_ready);
        Ok(())
    }

    #[test]
    fn aggregate_readiness_status_requires_exact_covered_through_cursor() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("aggregate-readiness-covered-through-cursor.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-16T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = aggregate_readiness_config();
        let window_start = now - Duration::days(config.scoring_window_days as i64);
        store.set_discovery_scoring_covered_since(window_start - Duration::hours(1))?;
        let conn = Connection::open(&db_path)?;
        conn.execute(
            "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
             VALUES ('covered_through_ts', ?1, ?2)
             ON CONFLICT(state_key) DO UPDATE SET
                state_value = excluded.state_value,
                updated_at = excluded.updated_at",
            rusqlite::params![
                (now - Duration::minutes(5)).to_rfc3339(),
                Utc::now().to_rfc3339(),
            ],
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let status = discovery.aggregate_readiness_status(&store, now)?;
        assert_eq!(status.covered_through_ts, Some(now - Duration::minutes(5)));
        assert_eq!(status.covered_through_cursor, None);
        assert_eq!(
            status.write_blockers,
            vec![AggregateReadinessBlocker::MissingCoveredThroughCursor]
        );
        assert_eq!(
            status.read_blockers,
            vec![AggregateReadinessBlocker::MissingCoveredThroughCursor]
        );
        assert!(!status.storage_ready_for_runtime_gate);
        Ok(())
    }

    #[test]
    fn aggregate_readiness_status_reports_gap_staleness_and_backfill_blockers() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-readiness-blockers.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-16T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = aggregate_readiness_config();
        let window_start = now - Duration::days(config.scoring_window_days as i64);
        store.set_discovery_scoring_covered_since(window_start + Duration::hours(1))?;
        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::hours(2),
            slot: 77,
            signature: "aggregate-covered-through-stale".to_string(),
        })?;
        store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(30),
            slot: 88,
            signature: "aggregate-gap".to_string(),
        })?;
        store.set_discovery_scoring_backfill_progress(
            window_start - Duration::days(10),
            &DiscoveryRuntimeCursor {
                ts_utc: now - Duration::days(1),
                slot: 91,
                signature: "aggregate-backfill-progress".to_string(),
            },
        )?;
        store.set_discovery_scoring_backfill_source_protection(
            now - Duration::hours(4),
            now + Duration::hours(2),
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let status = discovery.aggregate_readiness_status(&store, now)?;
        assert_eq!(
            status.write_blockers,
            vec![
                AggregateReadinessBlocker::MaterializationGapLatched,
                AggregateReadinessBlocker::BackfillInProgress,
            ]
        );
        assert_eq!(
            status.read_blockers,
            vec![
                AggregateReadinessBlocker::CoveredSinceAfterWindowStart,
                AggregateReadinessBlocker::CoveredThroughTooStaleForRuntimeGate,
                AggregateReadinessBlocker::MaterializationGapLatched,
                AggregateReadinessBlocker::CoveredThroughTooStaleForAuditLag,
                AggregateReadinessBlocker::BackfillInProgress,
            ]
        );
        assert_eq!(status.covered_through_lag_seconds, Some(7_200));
        assert!(status.backfill_active);
        assert!(!status.backfill_resume_required);
        assert!(!status.coverage_markers_pending_backfill_completion);
        assert!(!status.scoring_horizon_covered);
        assert!(!status.covered_through_within_runtime_lag);
        assert!(!status.covered_through_within_audit_lag);
        assert!(!status.storage_ready_for_runtime_gate);
        assert!(!status.effective_writes_ready);
        assert!(!status.effective_reads_ready);
        Ok(())
    }

    #[test]
    fn aggregate_readiness_status_reports_resumable_partial_backfill_without_coverage_markers(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-readiness-backfill-resume.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-16T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = aggregate_readiness_config();
        let window_start = now - Duration::days(config.scoring_window_days as i64);
        store.set_discovery_scoring_backfill_progress(
            window_start,
            &DiscoveryRuntimeCursor {
                ts_utc: now - Duration::hours(2),
                slot: 404,
                signature: "aggregate-backfill-resume".to_string(),
            },
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let status = discovery.aggregate_readiness_status(&store, now)?;
        assert!(!status.backfill_active);
        assert!(status.backfill_resume_required);
        assert!(status.coverage_markers_pending_backfill_completion);
        assert_eq!(
            status.write_blockers,
            vec![
                AggregateReadinessBlocker::CoveredThroughCursorPendingBackfillCompletion,
                AggregateReadinessBlocker::BackfillResumeRequired,
            ]
        );
        assert_eq!(
            status.read_blockers,
            vec![
                AggregateReadinessBlocker::CoveredSincePendingBackfillCompletion,
                AggregateReadinessBlocker::CoveredThroughCursorPendingBackfillCompletion,
                AggregateReadinessBlocker::BackfillResumeRequired,
            ]
        );
        assert_eq!(status.covered_since, None);
        assert_eq!(status.covered_through_ts, None);
        assert_eq!(status.covered_through_cursor, None);
        assert!(!status.effective_writes_ready);
        assert!(!status.effective_reads_ready);
        Ok(())
    }

    #[test]
    fn aggregate_readiness_status_reports_ready_when_markers_are_current() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-readiness-ready.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-16T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = aggregate_readiness_config();
        let window_start = now - Duration::days(config.scoring_window_days as i64);
        store.set_discovery_scoring_covered_since(window_start - Duration::hours(1))?;
        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(5),
            slot: 101,
            signature: "aggregate-covered-through-ready".to_string(),
        })?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let status = discovery.aggregate_readiness_status(&store, now)?;
        assert!(status.scoring_horizon_covered);
        assert!(status.covered_through_within_runtime_lag);
        assert!(status.covered_through_within_audit_lag);
        assert!(status.storage_ready_for_runtime_gate);
        assert!(status.effective_writes_ready);
        assert!(status.effective_reads_ready);
        assert!(status.write_blockers.is_empty());
        assert!(status.read_blockers.is_empty());
        Ok(())
    }

    #[test]
    fn discovery_runtime_cursor_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(discovery_runtime_cursor_error_requires_abort(&error));
    }

    #[test]
    fn discovery_runtime_cursor_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!discovery_runtime_cursor_error_requires_abort(&error));
    }

    #[test]
    fn run_cycle_returns_error_on_fatal_runtime_cursor_write_failure() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-discovery-runtime-cursor-fatal.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        assert_eq!(store.load_discovery_runtime_cursor()?, None);

        let now = DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        store.insert_observed_swap(&swap(
            "wallet_cursor_fatal",
            "cursor-fatal-sig-001",
            now - Duration::minutes(5),
            SOL_MINT,
            "TokenCursorFatal111111111111111111111111",
            1.0,
            100.0,
        ))?;

        let conn = Connection::open(Path::new(&db_path))?;
        conn.execute_batch(
            "CREATE TRIGGER fail_discovery_runtime_cursor_insert
             BEFORE INSERT ON discovery_runtime_state
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.max_window_swaps_in_memory = 100;
        config.max_fetch_swaps_per_cycle = 10;
        config.max_fetch_pages_per_cycle = 1;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());

        let error = discovery
            .run_cycle(&store, now)
            .expect_err("fatal discovery cursor persist must propagate");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains("failed persisting discovery runtime cursor with fatal sqlite I/O"),
            "expected fatal discovery cursor context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed updating discovery runtime cursor"),
            "expected sqlite cursor upsert context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert_eq!(
            store.load_discovery_runtime_cursor()?,
            None,
            "fatal cursor persist failure must leave runtime cursor unset"
        );
        Ok(())
    }

    #[test]
    fn discovery_runtime_cursor_load_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(discovery_runtime_cursor_load_error_requires_abort(&error));
    }

    #[test]
    fn discovery_runtime_cursor_load_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!discovery_runtime_cursor_load_error_requires_abort(&error));
    }

    #[test]
    fn discovery_recent_window_load_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(discovery_recent_window_load_error_requires_abort(&error));
    }

    #[test]
    fn discovery_recent_window_load_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!discovery_recent_window_load_error_requires_abort(&error));
    }

    fn catch_up_test_telemetry(
        phase: DiscoveryPersistedRebuildPhase,
        mode: CollectBuyMintsMode,
        replay_wallet_stats_complete: bool,
        budget_exhausted_reason: Option<PersistedStreamBudgetExhaustedReason>,
    ) -> PersistedStreamProgressTelemetry {
        let now = DateTime::parse_from_rfc3339("2026-03-19T18:30:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        PersistedStreamProgressTelemetry {
            phase,
            collect_buy_mints_mode: mode,
            replay_mode: ReplayMode::WalletStatsThenSolLeg,
            window_start: now - Duration::days(5),
            horizon_end: now,
            metrics_window_start: now,
            phase_cursor: None,
            collect_buy_mints_cursor_token: None,
            collect_buy_mints_reconcile_source_window_start: None,
            collect_buy_mints_reconcile_source_horizon_end: None,
            collect_buy_mints_reconcile_expired_head_cursor: None,
            collect_buy_mints_reconcile_new_tail_cursor: None,
            collect_buy_mints_reconcile_expired_head_cursor_token: None,
            collect_buy_mints_reconcile_new_tail_cursor_token: None,
            collect_buy_mints_reconcile_expired_head_pending_mints: 0,
            collect_buy_mints_reconcile_new_tail_slice_end_token: None,
            collect_buy_mints_reconcile_new_tail_pending_mints: 0,
            prepass_rows_processed: 0,
            prepass_pages_processed: 0,
            replay_wallet_stats_complete,
            replay_wallet_stats_rows_processed: 0,
            replay_wallet_stats_pages_processed: 0,
            replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress::default(),
            replay_sol_leg_access_path: None,
            replay_rows_processed: 0,
            replay_pages_processed: 0,
            chunks_completed: 0,
            cycle_rows_processed: 0,
            cycle_pages_processed: 0,
            cycle_replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress::default(),
            cycle_unique_buy_mints_discovered: 0,
            observed_swaps_loaded: 0,
            unique_buy_mints: 0,
            quality_next_mint_index: 0,
            quality_rpc_attempted: 0,
            quality_rpc_spent_ms: 0,
            wallets_buffered: 0,
            started_at: now,
            cycle_elapsed_ms: 0,
            total_elapsed_ms: 0,
            partial: true,
            completed: false,
            budget_exhausted_reason,
        }
    }

    #[test]
    fn persisted_stream_catch_up_request_targets_collect_buy_mints_and_replay_wallet_stats_only() {
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::ReconcileExpiredHead,
                false,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget),
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::ReconcileNewTail,
                false,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget),
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::FreshScan,
                false,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            )
        ));
        assert!(!should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::ReconcileExpiredHead,
                false,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            )
        ));
        assert!(!should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::ReconcileNewTail,
                false,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                false,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                false,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget),
            )
        ));
        assert!(!should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                true,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget),
            )
        ));
        assert!(!should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                false,
                None,
            )
        ));
    }

    fn aggregate_write_config(config: &DiscoveryConfig) -> DiscoveryAggregateWriteConfig {
        DiscoveryAggregateWriteConfig {
            max_tx_per_minute: config.max_tx_per_minute,
            rug_lookahead_seconds: config.rug_lookahead_seconds as u32,
            helius_http_url: None,
            min_token_age_hint_seconds: None,
        }
    }

    fn insert_shadow_recorded_signal(
        store: &SqliteStore,
        wallet_id: &str,
        token: &str,
        ts: DateTime<Utc>,
        signal_id: &str,
    ) -> Result<()> {
        store.insert_copy_signal(&CopySignalRow {
            signal_id: signal_id.to_string(),
            wallet_id: wallet_id.to_string(),
            side: "buy".to_string(),
            token: token.to_string(),
            notional_sol: 0.2,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts,
            status: "shadow_recorded".to_string(),
        })?;
        Ok(())
    }

    fn insert_recent_tail_noise_swaps(
        store: &SqliteStore,
        start: DateTime<Utc>,
        count: usize,
        signature_prefix: &str,
    ) -> Result<()> {
        for idx in 0..count {
            let ts = start + Duration::minutes(idx as i64);
            store.insert_observed_swap(&swap(
                "wallet_tail_noise",
                &format!("{signature_prefix}-{idx}"),
                ts,
                SOL_MINT,
                "TokenStage3TailNoise1111111111111111111111",
                0.2,
                20.0,
            ))?;
        }
        Ok(())
    }

    fn insert_recent_profitable_pair(
        store: &SqliteStore,
        wallet_id: &str,
        now: DateTime<Utc>,
        signature_prefix: &str,
    ) -> Result<()> {
        let token = format!("Token{signature_prefix}111111111111111111111111");
        store.insert_observed_swap(&swap(
            wallet_id,
            &format!("{signature_prefix}-buy"),
            now - Duration::minutes(2),
            SOL_MINT,
            token.as_str(),
            1.0,
            100.0,
        ))?;
        store.insert_observed_swap(&swap(
            wallet_id,
            &format!("{signature_prefix}-sell"),
            now - Duration::minutes(1),
            token.as_str(),
            SOL_MINT,
            100.0,
            1.2,
        ))?;
        Ok(())
    }

    fn swap(
        wallet: &str,
        signature: &str,
        ts_utc: DateTime<Utc>,
        token_in: &str,
        token_out: &str,
        amount_in: f64,
        amount_out: f64,
    ) -> SwapEvent {
        SwapEvent {
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: token_in.to_string(),
            token_out: token_out.to_string(),
            amount_in,
            amount_out,
            signature: signature.to_string(),
            slot: ts_utc.timestamp().max(0) as u64,
            ts_utc,
            exact_amounts: None,
        }
    }
}
