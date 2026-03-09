use anyhow::Result;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_storage::{
    DiscoveryRuntimeCursor, PersistedWalletMetricSnapshotRow, SqliteStore, WalletMetricRow,
    WalletScoringBuyFactRow, WalletScoringQualitySource, WalletUpsertRow,
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
use self::windows::{cmp_swap_order, DiscoveryCursor, DiscoveryWindowState};
use quality_cache::BuyTradability;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const QUALITY_CACHE_TTL_SECONDS: i64 = 10 * 60;
const QUALITY_RPC_TIMEOUT_MS: u64 = 700;
const QUALITY_MAX_SIGNATURE_PAGES: u32 = 1;
const QUALITY_MAX_FETCH_PER_CYCLE: usize = 20;
const QUALITY_RPC_BUDGET_MS: u64 = 1_500;
const AGGREGATE_FOLLOWLIST_TRANSITION_GUARD_CYCLES: u32 = 3;

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
struct BuyObservation {
    token: String,
    ts: DateTime<Utc>,
    tradable: bool,
    quality_resolved: bool,
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
        followlist_deactivations_suppressed: bool,
        swaps: VecDeque<SwapEvent>,
    },
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
        let (swaps_window, fetch_progress, prepared_cycle) = {
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
                    Ok(swaps) => {
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
                    }
                    Err(error) => {
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
                        swaps_evicted_due_cap = swaps_evicted_due_cap.saturating_add(
                            state.push_swap_capped(swap, max_window_swaps_in_memory),
                        );
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
            let followlist_deactivations_suppressed = state.cap_truncation_floor.is_some();
            let bootstrap_from_persisted_metrics =
                state.bootstrap_from_persisted_metrics && state.swaps.is_empty();
            let publish_due = state.last_publish_at.map_or(true, |last_publish_at| {
                now.signed_duration_since(last_publish_at).num_seconds() >= publish_interval_seconds
            });

            let swaps_window = state.swaps.len();
            if aggregate_scoring_ready {
                state.bootstrap_from_persisted_metrics = false;
                if !state.last_summary_from_aggregates {
                    state.arm_aggregate_transition_guard(
                        AGGREGATE_FOLLOWLIST_TRANSITION_GUARD_CYCLES,
                    );
                }
                let aggregate_transition_suppressed = state.aggregate_transition_suppressed();
                (
                    swaps_window,
                    fetch_progress,
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
                state.cap_truncation_floor = None;
                state.last_snapshot_bucket = None;
                state.last_summary = None;
                state.note_scoring_source(false);
                (
                    swaps_window,
                    fetch_progress,
                    PreparedCycleState::Empty {
                        publish_due,
                        followlist_deactivations_suppressed: false,
                        bootstrap_from_persisted_metrics,
                    },
                )
            } else if state.last_snapshot_bucket == Some(metrics_window_start)
                && !state.last_summary_from_aggregates
                && state.last_summary.is_some()
            {
                (
                    swaps_window,
                    fetch_progress,
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
                    PreparedCycleState::Recompute {
                        publish_due,
                        followlist_deactivations_suppressed,
                        swaps: state.swaps.clone(),
                    },
                )
            }
        };

        let (
            publish_due,
            followlist_activations_suppressed,
            followlist_deactivations_suppressed,
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
                        };
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
                                swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                                swaps_fetch_time_budget_exhausted = fetch_progress.time_budget_exhausted,
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
                        });
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
                    });
                }
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
                };
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
                    scoring_source = if aggregate_scoring_ready { "aggregates" } else { "raw_window" },
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
                followlist_deactivations_suppressed,
                swaps,
            } => (
                publish_due,
                short_retention_window,
                followlist_deactivations_suppressed,
                self.build_wallet_snapshots_from_cached(store, &swaps, now),
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
        let metrics_to_persist = if should_persist_metrics {
            metric_rows.as_slice()
        } else {
            &[]
        };

        let ranked = rank_follow_candidates(&snapshots, self.config.min_score);
        let desired_wallets = desired_wallets(&ranked, self.config.follow_top_n);
        let follow_delta = store.persist_discovery_cycle(
            &wallet_rows,
            metrics_to_persist,
            &desired_wallets,
            !followlist_activations_suppressed,
            !followlist_deactivations_suppressed,
            now,
            "discovery_score_refresh",
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
        };
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

    fn build_wallet_snapshots_from_cached(
        &self,
        store: &SqliteStore,
        swaps: &VecDeque<SwapEvent>,
        now: DateTime<Utc>,
    ) -> Vec<WalletSnapshot> {
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
            self.resolve_token_quality_for_mints(store, &unique_buy_mints, now);
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

        let wallet_ids: Vec<String> = by_wallet.keys().cloned().collect();
        let persisted_active_day_counts = match store.wallet_active_day_counts_since(
            &wallet_ids,
            now - Duration::days(self.config.scoring_window_days.max(1) as i64),
        ) {
            Ok(counts) => counts,
            Err(error) => {
                warn!(
                    error = %error,
                    wallet_count = wallet_ids.len(),
                    "failed loading persisted wallet activity-day counts; falling back to cached discovery window"
                );
                HashMap::new()
            }
        };

        by_wallet
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
            .collect()
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

        let decay_cutoff = now - Duration::days(self.config.decay_window_days.max(1) as i64);
        Ok(Some(
            snapshots
                .into_iter()
                .map(|row| self.snapshot_from_persisted_metrics(row, decay_cutoff))
                .collect(),
        ))
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
        let rug_penalty = (1.0 - rug_ratio).clamp(0.0, 1.0).powi(2);
        let raw_score = (base_score * tradable_penalty * rug_penalty).clamp(0.0, 1.0);
        let decay_cutoff = now - Duration::days(self.config.decay_window_days.max(1) as i64);
        let eligible = trades >= self.config.min_trades
            && active_days >= self.config.min_active_days
            && !suspicious
            && max_buy_notional_sol >= self.config.min_leader_notional_sol
            && last_seen >= decay_cutoff
            && buy_total >= self.config.min_buy_count
            && tradable_ratio >= self.config.min_tradable_ratio
            && rug_ratio <= self.config.max_rug_ratio;
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
        let rug_metrics = self.compute_rug_metrics(&acc.buy_observations, token_sol_history, now);
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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use copybot_config::ShadowConfig;
    use copybot_storage::{
        DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, SqliteStore, WalletActivityDayRow,
    };
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
    fn short_retention_raw_recompute_suppresses_false_promotions_but_allows_demotions() -> Result<()>
    {
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
            "short-retention recompute must not promote new leaders from a truncated raw window"
        );
        assert_eq!(
            summary.follow_demoted, 1,
            "short-retention recompute should no longer keep deactivation globally disabled"
        );
        let active_after = store.list_active_follow_wallets()?;
        assert!(
            !active_after.contains("wallet_a"),
            "previously followed wallet_a should be removable again once short-retention raw recompute runs"
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
    fn cap_truncation_suppresses_false_followlist_demotions_until_window_catches_up() -> Result<()>
    {
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
            "cap-truncated recompute must suppress followlist demotions"
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
            "cap eviction should leave a truncation marker until the time window catches up"
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
        );
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
        let snapshots = discovery.build_wallet_snapshots_from_cached(&store, &swaps, now);
        let snapshot = snapshots.into_iter().next().context("expected snapshot")?;

        assert!(
            snapshot.eligible,
            "persisted day-level activity should satisfy min_active_days even when the capped tail only contains one day"
        );
        Ok(())
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
