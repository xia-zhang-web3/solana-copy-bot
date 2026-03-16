use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_storage::{
    is_fatal_sqlite_anyhow_error, DiscoveryRuntimeCursor, DiscoveryTrustedSelectionStateUpdate,
    PersistedWalletMetricSnapshotRow, SqliteStore, StartupTrustedSelectionGateStatus,
    TrustedSelectionState, TrustedSnapshotSourceKind, TrustedWalletMetricsSnapshotRow,
    TrustedWalletMetricsSnapshotWrite, WalletMetricRow, WalletScoringBuyFactRow,
    WalletScoringQualitySource, WalletUpsertRow,
};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration as StdDuration, Instant};
use tracing::{info, warn};

mod followlist;
mod quality_cache;
mod scoring;
mod windows;
use self::followlist::{desired_wallets, rank_follow_candidates, top_wallet_labels};
use self::scoring::{hold_time_quality_score, median_i64, tanh01};
use self::windows::{
    cmp_swap_order, CapTruncationDeactivationGuardReason, DiscoveryCursor, DiscoveryWindowState,
};
use quality_cache::BuyTradability;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const QUALITY_CACHE_TTL_SECONDS: i64 = 10 * 60;
const QUALITY_RPC_TIMEOUT_MS: u64 = 700;
const QUALITY_MAX_SIGNATURE_PAGES: u32 = 1;
const QUALITY_MAX_FETCH_PER_CYCLE: usize = 20;
const QUALITY_RPC_BUDGET_MS: u64 = 1_500;
const AGGREGATE_FOLLOWLIST_TRANSITION_GUARD_CYCLES: u32 = 3;
const AGGREGATE_READINESS_MAX_LAG_BUCKETS: u64 = 2;
const CAP_TRUNCATION_FOLLOWLIST_DEACTIVATION_GUARD_CYCLES: u32 = 2;
const STREAMING_RUG_TRADE_SWEEP_INTERVAL_SWAPS: usize = 2_048;
const POST_BOOTSTRAP_ROTATION_BLOCKED_REASON: &str =
    "post_bootstrap_rotation_blocked_cap_truncated";

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
    pub scoring_source: &'static str,
    pub trusted_selection_fail_closed: bool,
    pub raw_window_cap_truncated: bool,
    pub cap_truncation_deactivation_guard_active: bool,
    pub cap_truncation_deactivation_guard_reason: Option<&'static str>,
    pub cap_truncation_deactivation_guard_started_at: Option<DateTime<Utc>>,
    pub cap_truncation_floor_ts_utc: Option<DateTime<Utc>>,
    pub cap_truncation_floor_signature: Option<String>,
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
    MaterializationGapLatched,
    CoveredSinceAfterWindowStart,
    CoveredThroughTooStaleForRuntimeGate,
    CoveredThroughTooStaleForAuditLag,
    BackfillInProgress,
    BackfillProtectionActive,
}

impl AggregateReadinessBlocker {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WritesDisabledByConfig => "writes_disabled_by_config",
            Self::ReadsDisabledByConfig => "reads_disabled_by_config",
            Self::MissingCoveredSince => "missing_covered_since",
            Self::MissingCoveredThroughCursor => "missing_covered_through_cursor",
            Self::MaterializationGapLatched => "materialization_gap_latched",
            Self::CoveredSinceAfterWindowStart => "covered_since_after_window_start",
            Self::CoveredThroughTooStaleForRuntimeGate => {
                "covered_through_too_stale_for_runtime_gate"
            }
            Self::CoveredThroughTooStaleForAuditLag => "covered_through_too_stale_for_audit_lag",
            Self::BackfillInProgress => "backfill_in_progress",
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
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Copy, Default)]
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

#[derive(Debug, Clone)]
struct Lot {
    qty: f64,
    cost_sol: f64,
    opened_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct BuyObservation {
    token: String,
    ts: DateTime<Utc>,
    tradable: bool,
    quality_resolved: bool,
}

#[derive(Debug, Clone)]
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
    AggregateRecompute {
        publish_due: bool,
        followlist_activations_suppressed: bool,
        followlist_deactivations_suppressed: bool,
    },
    Empty {
        publish_due: bool,
        followlist_deactivations_suppressed: bool,
        bootstrap_from_persisted_metrics: bool,
    },
    Cached {
        publish_due: bool,
        followlist_activations_suppressed: bool,
        followlist_deactivations_suppressed: bool,
        summary: DiscoverySummary,
    },
    Recompute {
        publish_due: bool,
        followlist_activations_suppressed: bool,
        followlist_deactivations_suppressed: bool,
        metrics_persistence_suppressed: bool,
        swaps: VecDeque<SwapEvent>,
    },
    PersistedBootstrap {
        publish_due: bool,
        followlist_activations_suppressed: bool,
        followlist_deactivations_suppressed: bool,
        scoring_source: &'static str,
    },
}

impl PreparedCycleState {
    fn publish_due(&self) -> bool {
        match self {
            Self::AggregateRecompute { publish_due, .. }
            | Self::Empty { publish_due, .. }
            | Self::Cached { publish_due, .. }
            | Self::Recompute { publish_due, .. }
            | Self::PersistedBootstrap { publish_due, .. } => *publish_due,
        }
    }

    fn followlist_activations_suppressed(&self) -> bool {
        match self {
            Self::AggregateRecompute {
                followlist_activations_suppressed,
                ..
            }
            | Self::Cached {
                followlist_activations_suppressed,
                ..
            }
            | Self::Recompute {
                followlist_activations_suppressed,
                ..
            }
            | Self::PersistedBootstrap {
                followlist_activations_suppressed,
                ..
            } => *followlist_activations_suppressed,
            Self::Empty { .. } => false,
        }
    }

    fn followlist_deactivations_suppressed(&self) -> bool {
        match self {
            Self::AggregateRecompute {
                followlist_deactivations_suppressed,
                ..
            }
            | Self::Empty {
                followlist_deactivations_suppressed,
                ..
            }
            | Self::Cached {
                followlist_deactivations_suppressed,
                ..
            }
            | Self::Recompute {
                followlist_deactivations_suppressed,
                ..
            }
            | Self::PersistedBootstrap {
                followlist_deactivations_suppressed,
                ..
            } => *followlist_deactivations_suppressed,
        }
    }
}

#[derive(Debug, Clone)]
struct SolLegTrade {
    ts: DateTime<Utc>,
    wallet_id: String,
    sol_notional: f64,
}

#[derive(Debug, Default)]
struct TokenRollingState {
    first_seen: Option<DateTime<Utc>>,
    wallets_seen: HashSet<String>,
    sol_trades_5m: VecDeque<SolLegTrade>,
    sol_volume_5m: f64,
    sol_traders_5m: HashMap<String, u32>,
}

#[derive(Debug, Default)]
struct WalletAccumulator {
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    trades: u32,
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

#[derive(Debug, Default)]
struct AggregateWalletAccumulator {
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    trades: u32,
    spent_sol: f64,
    realized_pnl_sol: f64,
    max_buy_notional_sol: f64,
    wins: u32,
    closed_trades: u32,
    hold_samples_sec: Vec<i64>,
    active_days: HashSet<NaiveDate>,
    realized_pnl_by_day: HashMap<NaiveDate, f64>,
    suspicious: bool,
    buy_total: u32,
    quality_resolved_buys: u32,
    tradable_buys: u32,
    rug_metrics: RugMetrics,
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

        let mut write_blockers = Vec::new();
        if !self.config.scoring_aggregates_write_enabled {
            write_blockers.push(AggregateReadinessBlocker::WritesDisabledByConfig);
        }
        if covered_through_cursor.is_none() {
            write_blockers.push(AggregateReadinessBlocker::MissingCoveredThroughCursor);
        }
        if materialization_gap_cursor.is_some() {
            write_blockers.push(AggregateReadinessBlocker::MaterializationGapLatched);
        }
        if backfill_progress.is_some() {
            write_blockers.push(AggregateReadinessBlocker::BackfillInProgress);
        }
        if backfill_protected_since.is_some() {
            write_blockers.push(AggregateReadinessBlocker::BackfillProtectionActive);
        }

        let mut read_blockers = Vec::new();
        if !self.config.scoring_aggregates_enabled {
            read_blockers.push(AggregateReadinessBlocker::ReadsDisabledByConfig);
        }
        match covered_since {
            None => read_blockers.push(AggregateReadinessBlocker::MissingCoveredSince),
            Some(covered_since) if covered_since > window_start => {
                read_blockers.push(AggregateReadinessBlocker::CoveredSinceAfterWindowStart);
            }
            Some(_) => {}
        }
        if covered_through_cursor.is_none() {
            read_blockers.push(AggregateReadinessBlocker::MissingCoveredThroughCursor);
        } else if !covered_through_within_runtime_lag {
            read_blockers.push(AggregateReadinessBlocker::CoveredThroughTooStaleForRuntimeGate);
        }
        if materialization_gap_cursor.is_some() {
            read_blockers.push(AggregateReadinessBlocker::MaterializationGapLatched);
        }
        if covered_through_ts.is_some() && !covered_through_within_audit_lag {
            read_blockers.push(AggregateReadinessBlocker::CoveredThroughTooStaleForAuditLag);
        }
        if backfill_progress.is_some() {
            read_blockers.push(AggregateReadinessBlocker::BackfillInProgress);
        }
        if backfill_protected_since.is_some() {
            read_blockers.push(AggregateReadinessBlocker::BackfillProtectionActive);
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
        let aggregate_scoring_ready = self.config.scoring_aggregates_enabled
            && store.discovery_scoring_ready_for_window(
                window_start,
                now,
                Duration::seconds(self.config.refresh_seconds.max(1) as i64),
            )?;
        let recovered_active_follow_wallets = !store.list_active_follow_wallets()?.is_empty();
        let startup_trusted_selection_gate_status =
            store
                .startup_trusted_selection_gate_status()
                .context("failed to load discovery startup trusted selection gate status")?;
        let startup_trusted_selection_fail_closed = startup_trusted_selection_gate_status
            .effective_startup_fail_closed(
                now,
                self.config.scoring_window_days as i64,
                self.config.metric_snapshot_interval_seconds,
                self.config.max_bootstrap_snapshot_age_seconds,
            );
        let (
            swaps_window,
            fetch_progress,
            cap_truncation_telemetry,
            trusted_selection_bootstrap_pending,
            prepared_cycle,
        ) = {
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
                if (recovered_active_follow_wallets || startup_trusted_selection_fail_closed)
                    && !aggregate_scoring_ready
                {
                    state.trusted_selection_bootstrap_pending = true;
                    if let Err(error) = store.set_discovery_trusted_selection_bootstrap_required(
                        true,
                        "startup_selection_bootstrap_required",
                    ) {
                        return Err(error)
                            .context("failed persisting discovery trusted bootstrap requirement");
                    }
                }
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
                            if !aggregate_scoring_ready {
                                maybe_arm_cap_truncation_deactivation_guard(
                                    &mut state,
                                    now,
                                    CapTruncationDeactivationGuardReason::WarmLoadTruncated,
                                );
                            }
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
                        if evicted > 0 && !aggregate_scoring_ready {
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
            let trusted_selection_bootstrap_pending =
                state.trusted_selection_bootstrap_pending && !aggregate_scoring_ready;
            let bootstrap_from_persisted_metrics =
                state.bootstrap_from_persisted_metrics && state.swaps.is_empty();
            let truncated_warm_restore_bootstrap =
                state.truncated_warm_restore_bootstrap && state.cap_truncation_floor.is_some();
            let publish_due = state.last_publish_at.map_or(true, |last_publish_at| {
                now.signed_duration_since(last_publish_at).num_seconds() >= publish_interval_seconds
            });
            if !aggregate_scoring_ready && state.consume_cap_truncation_deactivation_guard_cycle() {
                maybe_warn_on_cap_truncation_deactivation_guard_expiry(
                    &state,
                    followlist_deactivations_suppressed,
                );
            }

            let swaps_window = state.swaps.len();
            if aggregate_scoring_ready {
                state.bootstrap_from_persisted_metrics = false;
                state.truncated_warm_restore_bootstrap = false;
                state.trusted_selection_bootstrap_pending = false;
                if !state.last_summary_from_aggregates {
                    state.arm_aggregate_transition_guard(
                        AGGREGATE_FOLLOWLIST_TRANSITION_GUARD_CYCLES,
                    );
                }
                let aggregate_transition_suppressed = state.aggregate_transition_suppressed();
                (
                    swaps_window,
                    fetch_progress,
                    cap_truncation_telemetry,
                    trusted_selection_bootstrap_pending,
                    if state.last_snapshot_bucket == Some(metrics_window_start)
                        && state.last_summary_from_aggregates
                        && state.last_summary.is_some()
                    {
                        PreparedCycleState::Cached {
                            publish_due,
                            followlist_activations_suppressed: aggregate_transition_suppressed,
                            followlist_deactivations_suppressed: aggregate_transition_suppressed,
                            summary: state
                                .last_summary
                                .clone()
                                .expect("checked last_summary exists above"),
                        }
                    } else {
                        PreparedCycleState::AggregateRecompute {
                            publish_due,
                            followlist_activations_suppressed: aggregate_transition_suppressed,
                            followlist_deactivations_suppressed: aggregate_transition_suppressed,
                        }
                    },
                )
            } else if swaps_window == 0 {
                state.clear_cap_truncation();
                state.last_snapshot_bucket = None;
                state.last_summary = None;
                state.note_scoring_source(false);
                (
                    swaps_window,
                    fetch_progress,
                    cap_truncation_telemetry,
                    trusted_selection_bootstrap_pending,
                    PreparedCycleState::Empty {
                        publish_due,
                        followlist_deactivations_suppressed: false,
                        bootstrap_from_persisted_metrics,
                    },
                )
            } else if truncated_warm_restore_bootstrap {
                state.truncated_warm_restore_bootstrap = false;
                (
                    swaps_window,
                    fetch_progress,
                    cap_truncation_telemetry,
                    trusted_selection_bootstrap_pending,
                    PreparedCycleState::PersistedBootstrap {
                        publish_due,
                        followlist_activations_suppressed: true,
                        followlist_deactivations_suppressed: true,
                        scoring_source: "persisted_wallet_metrics_truncated_warm_restore",
                    },
                )
            } else if state.last_snapshot_bucket == Some(metrics_window_start)
                && !state.last_summary_from_aggregates
                && state.last_summary.is_some()
            {
                (
                    swaps_window,
                    fetch_progress,
                    cap_truncation_telemetry,
                    trusted_selection_bootstrap_pending,
                    PreparedCycleState::Cached {
                        publish_due,
                        followlist_activations_suppressed: false,
                        followlist_deactivations_suppressed,
                        summary: state
                            .last_summary
                            .clone()
                            .expect("checked last_summary exists above"),
                    },
                )
            } else {
                (
                    swaps_window,
                    fetch_progress,
                    cap_truncation_telemetry,
                    trusted_selection_bootstrap_pending,
                    PreparedCycleState::Recompute {
                        publish_due,
                        followlist_activations_suppressed: raw_window_history_incomplete
                            || short_retention_window,
                        followlist_deactivations_suppressed,
                        metrics_persistence_suppressed: raw_window_history_incomplete,
                        swaps: state.swaps.clone(),
                    },
                )
            }
        };

        if let Some(summary) = self.maybe_fail_close_post_bootstrap_rotation_blocked(
            store,
            &startup_trusted_selection_gate_status,
            now,
            window_start,
            metrics_window_start,
            aggregate_scoring_ready,
            &cap_truncation_telemetry,
            &prepared_cycle,
        )? {
            return Ok(summary);
        }

        if trusted_selection_bootstrap_pending {
            let (publish_due, bootstrap_scoring_source, invalid_scoring_source) =
                match &prepared_cycle {
                    PreparedCycleState::PersistedBootstrap {
                        publish_due,
                        scoring_source,
                        ..
                    } => (
                        *publish_due,
                        *scoring_source,
                        "persisted_wallet_metrics_truncated_warm_restore_empty",
                    ),
                    PreparedCycleState::AggregateRecompute { publish_due, .. }
                    | PreparedCycleState::Empty { publish_due, .. }
                    | PreparedCycleState::Cached { publish_due, .. }
                    | PreparedCycleState::Recompute { publish_due, .. } => (
                        *publish_due,
                        "trusted_persisted_wallet_metrics_bootstrap",
                        "trusted_persisted_wallet_metrics_bootstrap_unavailable",
                    ),
                };
            if let Some(snapshots) =
                self.build_wallet_snapshots_from_latest_metrics(store, now, metrics_window_start)?
            {
                let summary = self.persist_trusted_selection_from_snapshots(
                    store,
                    window_start,
                    &snapshots,
                    publish_due,
                    &cap_truncation_telemetry,
                    bootstrap_scoring_source,
                    now,
                    "discovery_trusted_persisted_metrics_bootstrap",
                )?;
                if summary.published {
                    self.record_live_publish(now);
                }
                {
                    let mut state = match self.window_state.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => {
                            warn!("discovery window mutex poisoned while clearing trusted bootstrap pending state; continuing");
                            poisoned.into_inner()
                        }
                    };
                    state.trusted_selection_bootstrap_pending = false;
                    state.bootstrap_from_persisted_metrics = false;
                    state.truncated_warm_restore_bootstrap = false;
                    state.last_snapshot_bucket = Some(metrics_window_start);
                    state.last_summary = Some(summary.clone());
                    state.note_scoring_source(false);
                }
                store
                    .set_discovery_trusted_selection_bootstrap_required(
                        false,
                        "trusted_selection_bootstrap_satisfied",
                    )
                    .context("failed clearing discovery trusted bootstrap requirement")?;
                warn!(
                    metrics_window_start = %metrics_window_start,
                    follow_top_n = self.config.follow_top_n,
                    trusted_wallets_seen = summary.wallets_seen,
                    trusted_eligible_wallets = summary.eligible_wallets,
                    active_follow_wallets = summary.active_follow_wallets,
                    "discovery restored trusted top-N selection from persisted wallet_metrics bootstrap"
                );
                return Ok(summary);
            }

            let follow_delta = store.persist_discovery_cycle(
                &[],
                &[],
                &[],
                false,
                true,
                now,
                "discovery_invalid_selection_fail_closed",
            )?;
            store
                .set_discovery_trusted_selection_bootstrap_required(
                    true,
                    "trusted_selection_bootstrap_unavailable",
                )
                .context("failed persisting discovery invalid selection fail-close state")?;
            if let Some(metadata) =
                store.trusted_wallet_metrics_snapshot_metadata_for_window(metrics_window_start)?
            {
                self.persist_trusted_selection_state(
                    store,
                    self.effective_trusted_snapshot_state(&metadata, now),
                    Some(metadata.snapshot_id),
                    Some(metadata.effective_window_start),
                    Some(metadata.source_kind),
                    true,
                    "trusted_selection_bootstrap_unavailable",
                    now,
                )?;
            } else {
                self.persist_trusted_selection_state(
                    store,
                    TrustedSelectionState::Invalid,
                    None,
                    None,
                    None,
                    true,
                    "trusted_selection_bootstrap_unavailable",
                    now,
                )?;
            }
            let active_follow_wallets = store.list_active_follow_wallets()?.len();
            if publish_due {
                self.record_live_publish(now);
            }
            warn!(
                metrics_window_start = %metrics_window_start,
                recovered_follow_wallets_cleared = follow_delta.deactivated,
                "discovery trusted bootstrap unavailable; fail-closing recovered followlist until a trusted persisted selection source exists"
            );
            return Ok(DiscoverySummary {
                window_start,
                wallets_seen: 0,
                eligible_wallets: 0,
                metrics_written: 0,
                follow_promoted: 0,
                follow_demoted: follow_delta.deactivated,
                active_follow_wallets,
                top_wallets: Vec::new(),
                published: publish_due,
                trusted_selection_fail_closed: true,
                ..DiscoverySummary::default()
            }
            .with_scoring_source(invalid_scoring_source)
            .with_cap_truncation_telemetry(&cap_truncation_telemetry));
        }

        let (
            publish_due,
            followlist_activations_suppressed,
            followlist_deactivations_suppressed,
            metrics_persistence_suppressed,
            snapshots,
            scoring_source,
        ) = match prepared_cycle {
            PreparedCycleState::AggregateRecompute {
                publish_due,
                followlist_activations_suppressed,
                followlist_deactivations_suppressed,
            } => {
                store.finalize_discovery_scoring_rug_facts(now)?;
                (
                    publish_due,
                    followlist_activations_suppressed,
                    followlist_deactivations_suppressed,
                    false,
                    self.build_wallet_snapshots_from_aggregates(store, now)?,
                    "aggregates",
                )
            }
            PreparedCycleState::Empty {
                publish_due,
                followlist_deactivations_suppressed,
                bootstrap_from_persisted_metrics,
            } => {
                if bootstrap_from_persisted_metrics {
                    if let Some(snapshots) = self.build_wallet_snapshots_from_latest_metrics(
                        store,
                        now,
                        metrics_window_start,
                    )? {
                        let ranked = rank_follow_candidates(&snapshots, self.config.min_score);
                        let summary = DiscoverySummary {
                            window_start,
                            wallets_seen: snapshots.len(),
                            eligible_wallets: ranked.len(),
                            metrics_written: 0,
                            follow_promoted: 0,
                            follow_demoted: 0,
                            active_follow_wallets: store.list_active_follow_wallets()?.len(),
                            top_wallets: top_wallet_labels(&ranked, 5),
                            published: publish_due,
                            ..DiscoverySummary::default()
                        }
                        .with_scoring_source("persisted_wallet_metrics_bootstrap")
                        .with_cap_truncation_telemetry(&cap_truncation_telemetry);
                        if publish_due {
                            self.record_live_publish(now);
                        }
                        let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                        warn!(
                            metrics_window_start = %metrics_window_start,
                            scoring_window_days = window_days,
                            observed_swaps_retention_days = retention_days,
                            "discovery using persisted wallet_metrics bootstrap because raw observed_swaps window is empty after restart"
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
                            scoring_source = "persisted_wallet_metrics_bootstrap",
                            metrics_persisted = false,
                            snapshot_recomputed = false,
                            discovery_published = summary.published,
                            followlist_deactivations_suppressed,
                            discovery_cycle_duration_ms = elapsed_ms,
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
                                swaps_fetch_page_budget_exhausted =
                                    fetch_progress.page_budget_exhausted,
                                swaps_fetch_time_budget_exhausted =
                                    fetch_progress.time_budget_exhausted,
                                "discovery swap fetch exhausted bounded per-cycle budget; backlog processing continues next cycle"
                            );
                        }
                        return Ok(summary);
                    } else {
                        let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                        if publish_due {
                            self.record_live_publish(now);
                        }
                        info!(
                            window_start = %window_start,
                            wallets_seen = 0usize,
                            eligible_wallets = 0usize,
                            metrics_written = 0usize,
                            follow_promoted = 0usize,
                            follow_demoted = 0usize,
                            active_follow_wallets = 0usize,
                            swaps_window = 0usize,
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
                            scoring_source = "persisted_wallet_metrics_bootstrap_empty",
                            discovery_published = publish_due,
                            discovery_cycle_duration_ms = elapsed_ms,
                            followlist_deactivations_suppressed,
                            "discovery cycle completed"
                        );
                        return Ok(DiscoverySummary {
                            window_start,
                            published: publish_due,
                            ..DiscoverySummary::default()
                        }
                        .with_scoring_source("persisted_wallet_metrics_bootstrap_empty")
                        .with_cap_truncation_telemetry(&cap_truncation_telemetry));
                    }
                } else {
                    let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                    if publish_due {
                        self.record_live_publish(now);
                    }
                    info!(
                        window_start = %window_start,
                        wallets_seen = 0usize,
                        eligible_wallets = 0usize,
                        metrics_written = 0usize,
                        follow_promoted = 0usize,
                        follow_demoted = 0usize,
                        active_follow_wallets = 0usize,
                        swaps_window = 0usize,
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
                        scoring_source = "raw_window_empty",
                        discovery_published = publish_due,
                        discovery_cycle_duration_ms = elapsed_ms,
                        followlist_deactivations_suppressed,
                        "discovery cycle completed"
                    );
                    return Ok(DiscoverySummary {
                        window_start,
                        published: publish_due,
                        ..DiscoverySummary::default()
                    }
                    .with_scoring_source("raw_window_empty")
                    .with_cap_truncation_telemetry(&cap_truncation_telemetry));
                }
            }
            PreparedCycleState::PersistedBootstrap {
                publish_due,
                followlist_activations_suppressed,
                followlist_deactivations_suppressed,
                scoring_source,
            } => {
                if let Some(snapshots) = self.build_wallet_snapshots_from_latest_metrics(
                    store,
                    now,
                    metrics_window_start,
                )? {
                    let ranked = rank_follow_candidates(&snapshots, self.config.min_score);
                    let summary = DiscoverySummary {
                        window_start,
                        wallets_seen: snapshots.len(),
                        eligible_wallets: ranked.len(),
                        metrics_written: 0,
                        follow_promoted: 0,
                        follow_demoted: 0,
                        active_follow_wallets: store.list_active_follow_wallets()?.len(),
                        top_wallets: top_wallet_labels(&ranked, 5),
                        published: publish_due,
                        ..DiscoverySummary::default()
                    }
                    .with_scoring_source(scoring_source)
                    .with_cap_truncation_telemetry(&cap_truncation_telemetry);
                    if publish_due {
                        self.record_live_publish(now);
                    }
                    let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                    warn!(
                        metrics_window_start = %metrics_window_start,
                        scoring_window_days = window_days,
                        observed_swaps_retention_days = retention_days,
                        swaps_window,
                        "discovery using persisted wallet_metrics bootstrap because warm-restored observed_swaps window is truncated by in-memory cap"
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
                        scoring_source,
                        metrics_persisted = false,
                        snapshot_recomputed = false,
                        discovery_published = summary.published,
                        followlist_activations_suppressed,
                        followlist_deactivations_suppressed,
                        discovery_cycle_duration_ms = elapsed_ms,
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
                            swaps_fetch_page_budget_exhausted =
                                fetch_progress.page_budget_exhausted,
                            swaps_fetch_time_budget_exhausted =
                                fetch_progress.time_budget_exhausted,
                            "discovery swap fetch exhausted bounded per-cycle budget; backlog processing continues next cycle"
                        );
                    }
                    return Ok(summary);
                }

                let active_follow_wallets = store.list_active_follow_wallets()?.len();
                let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                if publish_due {
                    self.record_live_publish(now);
                }
                warn!(
                    metrics_window_start = %metrics_window_start,
                    scoring_window_days = window_days,
                    observed_swaps_retention_days = retention_days,
                    swaps_window,
                    "discovery warm-restored observed_swaps window is truncated by in-memory cap and no persisted wallet_metrics snapshot is available"
                );
                info!(
                    window_start = %window_start,
                    wallets_seen = 0usize,
                    eligible_wallets = 0usize,
                    metrics_written = 0usize,
                    follow_promoted = 0usize,
                    follow_demoted = 0usize,
                    active_follow_wallets,
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
                    scoring_source = "persisted_wallet_metrics_truncated_warm_restore_empty",
                    discovery_published = publish_due,
                    discovery_cycle_duration_ms = elapsed_ms,
                    followlist_activations_suppressed,
                    followlist_deactivations_suppressed,
                    "discovery cycle completed"
                );
                return Ok(DiscoverySummary {
                    window_start,
                    active_follow_wallets,
                    published: publish_due,
                    ..DiscoverySummary::default()
                }
                .with_scoring_source("persisted_wallet_metrics_truncated_warm_restore_empty")
                .with_cap_truncation_telemetry(&cap_truncation_telemetry));
            }
            PreparedCycleState::Cached {
                publish_due,
                followlist_activations_suppressed,
                followlist_deactivations_suppressed,
                summary: previous_summary,
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
                    top_wallets: previous_summary.top_wallets,
                    published: publish_due,
                    ..DiscoverySummary::default()
                }
                .with_scoring_source(previous_summary.scoring_source)
                .with_cap_truncation_telemetry(&cap_truncation_telemetry);
                if publish_due {
                    self.record_live_publish(now);
                }
                if aggregate_scoring_ready {
                    let mut state = match self.window_state.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => {
                            warn!("discovery window mutex poisoned while advancing aggregate transition guard; continuing");
                            poisoned.into_inner()
                        }
                    };
                    state.note_scoring_source(true);
                }
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
                "raw_window",
            ),
        };
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
            });
        }

        let should_persist_metrics = !store.wallet_metrics_window_exists(metrics_window_start)?;
        let metrics_to_persist = if should_persist_metrics && !metrics_persistence_suppressed {
            metric_rows.as_slice()
        } else {
            &[]
        };
        let snapshot_write = (!metrics_to_persist.is_empty()).then(|| {
            trusted_snapshot_write(
                TrustedSnapshotSourceKind::DiscoveryRefresh,
                TrustedSelectionState::TrustedCurrent,
                metrics_window_start,
                now,
                metrics_to_persist.len(),
                None,
                Some(metrics_window_start),
            )
        });

        let ranked = rank_follow_candidates(&snapshots, self.config.min_score);
        let desired_wallets = desired_wallets(&ranked, self.config.follow_top_n);
        let follow_delta = store.persist_discovery_cycle_with_snapshot_metadata(
            &wallet_rows,
            metrics_to_persist,
            &desired_wallets,
            !followlist_activations_suppressed,
            !followlist_deactivations_suppressed,
            now,
            "discovery_score_refresh",
            snapshot_write.as_ref(),
        )?;
        let active_follow_wallets = store.list_active_follow_wallets()?.len();
        let top_wallets = top_wallet_labels(&ranked, 5);

        let summary = DiscoverySummary {
            window_start,
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
        .with_scoring_source(scoring_source)
        .with_cap_truncation_telemetry(&cap_truncation_telemetry);
        if publish_due {
            self.record_live_publish(now);
        }
        {
            let mut state = match self.window_state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    warn!("discovery window mutex poisoned while caching summary; continuing");
                    poisoned.into_inner()
                }
            };
            state.last_snapshot_bucket = Some(metrics_window_start);
            state.last_summary = Some(summary.clone());
            state.note_scoring_source(scoring_source == "aggregates");
        }
        if let Some(snapshot_write) = snapshot_write.as_ref() {
            self.persist_trusted_selection_state_from_snapshot(
                store,
                snapshot_write,
                false,
                "discovery_score_refresh",
                now,
            )?;
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
            metrics_window_start = %metrics_window_start,
            scoring_source,
            metrics_persisted = should_persist_metrics,
            snapshot_recomputed = true,
            discovery_published = summary.published,
            followlist_activations_suppressed,
            followlist_deactivations_suppressed,
            discovery_cycle_duration_ms = elapsed_ms,
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

    fn trusted_snapshot_ready_for_window(
        &self,
        store: &SqliteStore,
        effective_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let Some(metadata) =
            store.trusted_wallet_metrics_snapshot_metadata_for_window(effective_window_start)?
        else {
            return Ok(false);
        };
        Ok(!matches!(
            self.effective_trusted_snapshot_state(&metadata, now),
            TrustedSelectionState::TrustedBridgedStale | TrustedSelectionState::Invalid
        ))
    }

    fn desired_wallets_for_persisted_metrics_window(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Option<HashSet<String>>> {
        let rows = store.load_wallet_metric_snapshots_for_window(window_start)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let snapshots = self.wallet_snapshots_from_persisted_metric_rows(now, rows);
        let ranked = rank_follow_candidates(&snapshots, self.config.min_score);
        Ok(Some(
            desired_wallets(&ranked, self.config.follow_top_n)
                .into_iter()
                .collect(),
        ))
    }

    fn maybe_fail_close_post_bootstrap_rotation_blocked(
        &self,
        store: &SqliteStore,
        gate_status: &StartupTrustedSelectionGateStatus,
        now: DateTime<Utc>,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        aggregate_scoring_ready: bool,
        cap_truncation_telemetry: &CapTruncationTelemetrySnapshot,
        prepared_cycle: &PreparedCycleState,
    ) -> Result<Option<DiscoverySummary>> {
        if gate_status.bootstrap_required
            || self.effective_startup_trusted_selection_state(gate_status, now)
                != Some(TrustedSelectionState::TrustedBridged)
            || gate_status.last_bootstrap_source_kind
                != Some(TrustedSnapshotSourceKind::CloneLatestBridge)
        {
            return Ok(None);
        }

        let Some(active_snapshot_window_start) = gate_status.active_snapshot_window_start else {
            return Ok(None);
        };
        if metrics_window_start <= active_snapshot_window_start
            || !cap_truncation_telemetry.raw_window_cap_truncated
            || aggregate_scoring_ready
            || self.trusted_snapshot_ready_for_window(store, metrics_window_start, now)?
        {
            return Ok(None);
        }

        let Some(bootstrap_wallets) = self.desired_wallets_for_persisted_metrics_window(
            store,
            active_snapshot_window_start,
            now,
        )?
        else {
            return Ok(None);
        };
        let active_follow_wallets = store.list_active_follow_wallets()?;
        if active_follow_wallets.is_empty() || active_follow_wallets != bootstrap_wallets {
            return Ok(None);
        }

        let follow_delta = store.persist_discovery_cycle(
            &[],
            &[],
            &[],
            false,
            true,
            now,
            POST_BOOTSTRAP_ROTATION_BLOCKED_REASON,
        )?;
        store
            .set_discovery_trusted_selection_bootstrap_required(
                true,
                POST_BOOTSTRAP_ROTATION_BLOCKED_REASON,
            )
            .context("failed raising trusted bootstrap-required latch after rotation watchdog")?;
        self.persist_trusted_selection_state(
            store,
            TrustedSelectionState::Invalid,
            gate_status.active_snapshot_id.clone(),
            Some(active_snapshot_window_start),
            gate_status.last_bootstrap_source_kind,
            true,
            POST_BOOTSTRAP_ROTATION_BLOCKED_REASON,
            now,
        )?;
        let active_follow_wallets = store.list_active_follow_wallets()?.len();
        if prepared_cycle.publish_due() {
            self.record_live_publish(now);
        }
        warn!(
            reason = POST_BOOTSTRAP_ROTATION_BLOCKED_REASON,
            active_snapshot_id = gate_status.active_snapshot_id.as_deref(),
            active_snapshot_window_start = %active_snapshot_window_start,
            current_metrics_window_start = %metrics_window_start,
            raw_window_cap_truncated = cap_truncation_telemetry.raw_window_cap_truncated,
            followlist_activations_suppressed =
                prepared_cycle.followlist_activations_suppressed(),
            followlist_deactivations_suppressed =
                prepared_cycle.followlist_deactivations_suppressed(),
            bootstrap_active_wallets = bootstrap_wallets.len(),
            cleared_follow_wallets = follow_delta.deactivated,
            "discovery degraded bridged bootstrap selection back to invalid because steady-state trusted rotation did not resume on the first next effective bucket"
        );
        Ok(Some(
            DiscoverySummary {
                window_start,
                wallets_seen: 0,
                eligible_wallets: 0,
                metrics_written: 0,
                follow_promoted: 0,
                follow_demoted: follow_delta.deactivated,
                active_follow_wallets,
                top_wallets: Vec::new(),
                published: prepared_cycle.publish_due(),
                trusted_selection_fail_closed: true,
                ..DiscoverySummary::default()
            }
            .with_scoring_source(POST_BOOTSTRAP_ROTATION_BLOCKED_REASON)
            .with_cap_truncation_telemetry(cap_truncation_telemetry),
        ))
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
        let (snapshots, observed_swaps_loaded) =
            self.build_wallet_snapshots_from_persisted_stream(store, metrics_window_start, now)?;
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

    fn build_wallet_snapshots_from_persisted_stream(
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

    fn build_wallet_snapshots_from_latest_metrics(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        expected_window_start: DateTime<Utc>,
    ) -> Result<Option<Vec<WalletSnapshot>>> {
        let snapshots = store.load_latest_wallet_metric_snapshots()?;
        let Some(latest_window_start) = snapshots.first().map(|row| row.window_start) else {
            return Ok(None);
        };
        let max_lag = Duration::seconds(self.config.metric_snapshot_interval_seconds.max(1) as i64);
        if latest_window_start + max_lag < expected_window_start {
            return Ok(None);
        }
        if let Some(metadata) =
            store.trusted_wallet_metrics_snapshot_metadata_for_window(latest_window_start)?
        {
            if matches!(
                self.effective_trusted_snapshot_state(&metadata, now),
                TrustedSelectionState::TrustedBridgedStale | TrustedSelectionState::Invalid
            ) {
                return Ok(None);
            }
        }
        // Missing metadata is an intentional legacy-grace path for pre-lineage snapshots.
        // Until they are rewritten by a metadata-aware refresh or recovery tool, they still
        // rely on the existing latest-window freshness guard above rather than fail-closing
        // purely because trusted snapshot metadata does not exist yet.

        Ok(Some(self.wallet_snapshots_from_persisted_metric_rows(
            now, snapshots,
        )))
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

    fn persist_trusted_selection_from_snapshots(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        snapshots: &[WalletSnapshot],
        publish_due: bool,
        cap_truncation_telemetry: &CapTruncationTelemetrySnapshot,
        scoring_source: &'static str,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<DiscoverySummary> {
        let wallet_rows = self.wallet_rows_from_snapshots(snapshots);
        let ranked = rank_follow_candidates(snapshots, self.config.min_score);
        let desired_wallets = desired_wallets(&ranked, self.config.follow_top_n);
        let follow_delta = store.persist_discovery_cycle(
            &wallet_rows,
            &[],
            &desired_wallets,
            true,
            true,
            now,
            reason,
        )?;
        let active_follow_wallets = store.list_active_follow_wallets()?.len();
        store
            .set_discovery_trusted_selection_bootstrap_required(
                false,
                "trusted_selection_bootstrap_satisfied",
            )
            .context("failed clearing discovery trusted bootstrap requirement")?;
        if let Some(metadata) =
            store.trusted_wallet_metrics_snapshot_metadata_for_window(window_start)?
        {
            self.persist_trusted_selection_state(
                store,
                self.effective_trusted_snapshot_state(&metadata, now),
                Some(metadata.snapshot_id),
                Some(metadata.effective_window_start),
                Some(metadata.source_kind),
                false,
                "trusted_selection_bootstrap_satisfied",
                now,
            )?;
        } else {
            self.persist_trusted_selection_state(
                store,
                TrustedSelectionState::TrustedCurrent,
                None,
                Some(window_start),
                Some(TrustedSnapshotSourceKind::Legacy),
                false,
                "trusted_selection_bootstrap_satisfied_legacy",
                now,
            )?;
        }

        Ok(DiscoverySummary {
            window_start,
            wallets_seen: snapshots.len(),
            eligible_wallets: ranked.len(),
            metrics_written: 0,
            follow_promoted: follow_delta.activated,
            follow_demoted: follow_delta.deactivated,
            active_follow_wallets,
            top_wallets: top_wallet_labels(&ranked, 5),
            published: publish_due,
            ..DiscoverySummary::default()
        }
        .with_scoring_source(scoring_source)
        .with_cap_truncation_telemetry(cap_truncation_telemetry))
    }

    fn build_wallet_snapshots_from_aggregates(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<Vec<WalletSnapshot>> {
        let window_start = now - Duration::days(self.config.scoring_window_days.max(1) as i64);
        let snapshot = store.load_wallet_scoring_snapshot_since(window_start)?;
        let mut by_wallet: HashMap<String, AggregateWalletAccumulator> = HashMap::new();

        for row in snapshot.days {
            let entry = by_wallet.entry(row.wallet_id).or_default();
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
            entry.trades = entry.trades.saturating_add(row.trades);
            entry.spent_sol += row.spent_sol.max(0.0);
            entry.max_buy_notional_sol = entry
                .max_buy_notional_sol
                .max(row.max_buy_notional_sol.max(0.0));
            entry.active_days.insert(row.activity_day);
        }

        for row in snapshot.buy_facts {
            let entry = by_wallet.entry(row.wallet_id.clone()).or_default();
            entry.buy_total = entry.buy_total.saturating_add(1);
            match self.evaluate_buy_fact_tradability(&row) {
                BuyTradability::Tradable => {
                    entry.quality_resolved_buys = entry.quality_resolved_buys.saturating_add(1);
                    entry.tradable_buys = entry.tradable_buys.saturating_add(1);
                }
                BuyTradability::Rejected => {
                    entry.quality_resolved_buys = entry.quality_resolved_buys.saturating_add(1);
                }
                BuyTradability::Deferred => {}
            }
            match self.evaluate_buy_fact_rug_status(&row, now) {
                BuyFactRugStatus::Healthy => {
                    entry.rug_metrics.evaluated = entry.rug_metrics.evaluated.saturating_add(1);
                }
                BuyFactRugStatus::Rugged => {
                    entry.rug_metrics.evaluated = entry.rug_metrics.evaluated.saturating_add(1);
                    entry.rug_metrics.rugged = entry.rug_metrics.rugged.saturating_add(1);
                }
                BuyFactRugStatus::Unevaluated => {
                    entry.rug_metrics.unevaluated = entry.rug_metrics.unevaluated.saturating_add(1);
                }
            }
        }

        for row in snapshot.close_facts {
            let entry = by_wallet.entry(row.wallet_id).or_default();
            entry.realized_pnl_sol += row.pnl_sol;
            entry.closed_trades = entry.closed_trades.saturating_add(1);
            if row.win {
                entry.wins = entry.wins.saturating_add(1);
            }
            entry.hold_samples_sec.push(row.hold_seconds.max(0));
            *entry
                .realized_pnl_by_day
                .entry(row.closed_ts.date_naive())
                .or_insert(0.0) += row.pnl_sol;
        }

        for (wallet_id, max_count) in snapshot.max_tx_counts {
            if max_count > self.config.max_tx_per_minute.max(1) {
                by_wallet.entry(wallet_id).or_default().suspicious = true;
            }
        }

        Ok(by_wallet
            .into_iter()
            .map(|(wallet_id, acc)| self.snapshot_from_aggregate_accumulator(wallet_id, acc, now))
            .collect())
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

    fn snapshot_from_aggregate_accumulator(
        &self,
        wallet_id: String,
        acc: AggregateWalletAccumulator,
        now: DateTime<Utc>,
    ) -> WalletSnapshot {
        self.snapshot_from_components(
            wallet_id,
            acc.first_seen.unwrap_or(now),
            acc.last_seen.unwrap_or(now),
            acc.trades,
            acc.active_days.len() as u32,
            acc.spent_sol,
            acc.realized_pnl_sol,
            acc.max_buy_notional_sol,
            acc.wins,
            acc.closed_trades,
            &acc.hold_samples_sec,
            &acc.realized_pnl_by_day,
            acc.suspicious,
            acc.buy_total,
            acc.quality_resolved_buys,
            acc.tradable_buys,
            acc.rug_metrics,
            now,
        )
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
        let active_days = acc.active_days.len() as u32;
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

    fn evaluate_buy_fact_tradability(&self, row: &WalletScoringBuyFactRow) -> BuyTradability {
        if self.shadow_quality.min_volume_5m_sol > 0.0
            && row.market_volume_5m_sol + 1e-12 < self.shadow_quality.min_volume_5m_sol
        {
            return BuyTradability::Rejected;
        }
        if self.shadow_quality.min_unique_traders_5m > 0
            && u64::from(row.market_unique_traders_5m) < self.shadow_quality.min_unique_traders_5m
        {
            return BuyTradability::Rejected;
        }

        let mut deferred = false;
        if self.shadow_quality.min_token_age_seconds > 0 {
            let Some(token_age_seconds) = row.quality_token_age_seconds else {
                return if row.quality_source == WalletScoringQualitySource::Deferred {
                    BuyTradability::Deferred
                } else {
                    BuyTradability::Rejected
                };
            };
            if token_age_seconds < self.shadow_quality.min_token_age_seconds {
                return BuyTradability::Rejected;
            }
        }

        if self.shadow_quality.min_holders > 0 {
            match row.quality_source {
                WalletScoringQualitySource::Fresh => {
                    let Some(holders) = row.quality_holders else {
                        return BuyTradability::Rejected;
                    };
                    if holders < self.shadow_quality.min_holders {
                        return BuyTradability::Rejected;
                    }
                }
                WalletScoringQualitySource::Stale => {
                    if let Some(holders) = row.quality_holders {
                        if holders < self.shadow_quality.min_holders {
                            return BuyTradability::Rejected;
                        }
                    }
                    deferred = true;
                }
                WalletScoringQualitySource::Deferred => deferred = true,
                WalletScoringQualitySource::Missing => return BuyTradability::Rejected,
            }
        }

        if self.shadow_quality.min_liquidity_sol > 0.0 {
            let liquidity_sol = match row.quality_source {
                WalletScoringQualitySource::Fresh => row
                    .quality_liquidity_sol
                    .unwrap_or(row.market_liquidity_proxy_sol),
                _ => row.market_liquidity_proxy_sol,
            };
            if liquidity_sol + 1e-12 < self.shadow_quality.min_liquidity_sol {
                return BuyTradability::Rejected;
            }
        }

        if deferred {
            BuyTradability::Deferred
        } else {
            BuyTradability::Tradable
        }
    }

    fn evaluate_buy_fact_rug_status(
        &self,
        row: &WalletScoringBuyFactRow,
        now: DateTime<Utc>,
    ) -> BuyFactRugStatus {
        if row.rug_check_after_ts > now
            || row.rug_volume_lookahead_sol.is_none()
            || row.rug_unique_traders_lookahead.is_none()
        {
            return BuyFactRugStatus::Unevaluated;
        }

        let thin_volume = row.rug_volume_lookahead_sol.unwrap_or(0.0) + 1e-12
            < self.config.thin_market_min_volume_sol;
        let thin_traders = row.rug_unique_traders_lookahead.unwrap_or(0)
            < self.config.thin_market_min_unique_traders;
        if thin_volume || thin_traders {
            BuyFactRugStatus::Rugged
        } else {
            BuyFactRugStatus::Healthy
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
    fn observe_swap(
        &mut self,
        swap: &SwapEvent,
        max_tx_per_minute: u32,
        buy_tradability: Option<BuyTradability>,
    ) {
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
        let day = swap.ts_utc.date_naive();
        self.active_days.insert(day);
        self.mark_tx_minute(swap.ts_utc.timestamp() / 60, max_tx_per_minute);

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
        let day = swap.ts_utc.date_naive();
        self.active_days.insert(day);
        self.mark_tx_minute(swap.ts_utc.timestamp() / 60, max_tx_per_minute);

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
    use anyhow::{anyhow, Context};
    use copybot_config::ShadowConfig;
    use copybot_storage::{
        DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, SqliteStore, WalletActivityDayRow,
    };
    use rusqlite::Connection;
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
            second_summary.scoring_source, "raw_window",
            "cap-truncated recompute summary must preserve its raw-window scoring source for downstream scoping"
        );
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
            "persisted_wallet_metrics_truncated_warm_restore",
            "warm-restored capped-tail bootstrap summary must preserve its truncated warm-restore scoring source for downstream scoping"
        );
        assert!(
            summary.eligible_wallets >= 1,
            "persisted wallet_metrics bootstrap should keep the latest ranked wallet snapshot visible after restart"
        );
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
            "warm-restore persisted-metrics bootstrap should be a one-shot bridge, not a sticky mode"
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
    fn aggregate_scoring_requires_explicit_coverage_activation() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-scoring-coverage-gate.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
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
        config.max_rug_ratio = 1.0;
        config.thin_market_min_unique_traders = 1;
        config.scoring_aggregates_enabled = true;

        let mut swaps = Vec::new();
        for idx in 0..6 {
            let buy_ts = now - Duration::days(3) + Duration::minutes((idx * 20) as i64);
            let sell_ts = buy_ts + Duration::minutes(6);
            let buy = swap(
                "wallet_aggregate_gate",
                &format!("aggregate-gate-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenAggregateGate111111111111111111111111",
                1.0,
                100.0,
            );
            let sell = swap(
                "wallet_aggregate_gate",
                &format!("aggregate-gate-sell-{idx}"),
                sell_ts,
                "TokenAggregateGate111111111111111111111111",
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
        store.apply_discovery_scoring_batch(&swaps, &aggregate_write_config(&config))?;
        store.delete_observed_swaps_before(now + Duration::seconds(1))?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(
            summary.eligible_wallets, 0,
            "aggregate rows alone must not activate until coverage is marked ready"
        );
        Ok(())
    }

    #[test]
    fn aggregate_scoring_transition_guard_suppresses_initial_followlist_flip() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-scoring-transition-guard.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.refresh_seconds = 3600;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.max_rug_ratio = 1.0;
        config.thin_market_min_unique_traders = 1;
        config.scoring_aggregates_enabled = true;

        let window_start = now - Duration::days(config.scoring_window_days as i64);
        let mut swaps = Vec::new();
        for idx in 0..6 {
            let buy_ts = now - Duration::days(3) + Duration::minutes((idx * 20) as i64);
            let sell_ts = buy_ts + Duration::minutes(6);
            let buy = swap(
                "wallet_aggregate_new",
                &format!("aggregate-transition-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenAggregateTransition11111111111111111111",
                1.0,
                100.0,
            );
            let sell = swap(
                "wallet_aggregate_new",
                &format!("aggregate-transition-sell-{idx}"),
                sell_ts,
                "TokenAggregateTransition11111111111111111111",
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
        store.apply_discovery_scoring_batch(&swaps, &aggregate_write_config(&config))?;
        store.finalize_discovery_scoring_rug_facts(now)?;
        store.set_discovery_scoring_covered_since(window_start)?;
        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now,
            slot: 999,
            signature: "aggregate-transition-covered-through".to_string(),
        })?;
        store.delete_observed_swaps_before(now + Duration::seconds(1))?;
        store.persist_discovery_cycle(
            &[],
            &[],
            &[String::from("wallet_legacy_follow")],
            true,
            true,
            now - Duration::days(1),
            "seed_followlist",
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());

        let first = discovery.run_cycle(&store, now)?;
        assert_eq!(first.follow_promoted, 0);
        assert_eq!(first.follow_demoted, 0);
        let active = store.list_active_follow_wallets()?;
        assert!(active.contains("wallet_legacy_follow"));
        assert!(!active.contains("wallet_aggregate_new"));

        let second = discovery.run_cycle(&store, now + Duration::minutes(10))?;
        assert_eq!(second.follow_promoted, 0);
        assert_eq!(second.follow_demoted, 0);

        let third = discovery.run_cycle(&store, now + Duration::minutes(20))?;
        assert_eq!(third.follow_promoted, 0);
        assert_eq!(third.follow_demoted, 0);

        let fourth = discovery.run_cycle(&store, now + Duration::minutes(30))?;
        assert_eq!(fourth.follow_promoted, 1);
        assert_eq!(fourth.follow_demoted, 1);
        let active = store.list_active_follow_wallets()?;
        assert!(!active.contains("wallet_legacy_follow"));
        assert!(active.contains("wallet_aggregate_new"));
        Ok(())
    }

    #[test]
    fn aggregate_scoring_can_score_wallets_without_raw_hot_window() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-scoring-live-path.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
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
        config.max_rug_ratio = 1.0;
        config.thin_market_min_unique_traders = 1;
        config.scoring_aggregates_enabled = true;

        let window_start = now - Duration::days(config.scoring_window_days as i64);
        let mut swaps = Vec::new();
        for idx in 0..6 {
            let buy_ts = now - Duration::days(3) + Duration::minutes((idx * 20) as i64);
            let sell_ts = buy_ts + Duration::minutes(6);
            let buy = swap(
                "wallet_aggregate_live",
                &format!("aggregate-live-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenAggregateLive111111111111111111111111",
                1.0,
                100.0,
            );
            let sell = swap(
                "wallet_aggregate_live",
                &format!("aggregate-live-sell-{idx}"),
                sell_ts,
                "TokenAggregateLive111111111111111111111111",
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
        store.apply_discovery_scoring_batch(&swaps, &aggregate_write_config(&config))?;
        store.finalize_discovery_scoring_rug_facts(now)?;
        store.set_discovery_scoring_covered_since(window_start)?;
        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now,
            slot: 999,
            signature: "aggregate-covered-through".to_string(),
        })?;
        store.delete_observed_swaps_before(now + Duration::seconds(1))?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.eligible_wallets, 1);
        assert_eq!(summary.active_follow_wallets, 0);
        assert!(
            summary
                .top_wallets
                .iter()
                .any(|label| label.starts_with("wallet_aggregate_live:")),
            "aggregate scoring should produce the profitable wallet even with an empty raw window"
        );
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
                AggregateReadinessBlocker::BackfillProtectionActive,
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
                AggregateReadinessBlocker::BackfillProtectionActive,
            ]
        );
        assert_eq!(status.covered_through_lag_seconds, Some(7_200));
        assert!(!status.scoring_horizon_covered);
        assert!(!status.covered_through_within_runtime_lag);
        assert!(!status.covered_through_within_audit_lag);
        assert!(!status.storage_ready_for_runtime_gate);
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

    fn aggregate_write_config(config: &DiscoveryConfig) -> DiscoveryAggregateWriteConfig {
        DiscoveryAggregateWriteConfig {
            max_tx_per_minute: config.max_tx_per_minute,
            rug_lookahead_seconds: config.rug_lookahead_seconds as u32,
            helius_http_url: None,
            min_token_age_hint_seconds: None,
        }
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
