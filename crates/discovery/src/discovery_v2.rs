use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::{SwapEvent, WalletMetricRow, WalletUpsertRow};
use copybot_storage::{
    DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor, DiscoveryRuntimeMode, SqliteStore,
    TokenQualityCacheRow,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{Duration as StdDuration, Instant};

pub const DISCOVERY_V2_SCORING_SOURCE: &str = "discovery_v2_operational_window";

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const TOKEN_QUALITY_TTL_SECONDS: i64 = 10 * 60;
const ACTIONABLE_OPEN_POSITION_HOLD_MULTIPLIER: i64 = 4;
const ACTIONABLE_OPEN_POSITION_MIN_HOLD_SAMPLES: usize = 3;

#[derive(Debug, Clone)]
pub struct DiscoveryV2BuildOptions {
    pub now: DateTime<Utc>,
    pub window_minutes: u64,
    pub max_tail_lag_seconds: u64,
    pub max_rows: usize,
    pub time_budget_ms: u64,
    pub execution_enabled: bool,
}

impl DiscoveryV2BuildOptions {
    pub fn from_config(
        discovery: &DiscoveryConfig,
        execution_enabled: bool,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            now,
            window_minutes: 60,
            max_tail_lag_seconds: discovery.refresh_seconds.max(1),
            max_rows: discovery
                .max_window_swaps_in_memory
                .max(discovery.max_fetch_swaps_per_cycle)
                .max(1),
            time_budget_ms: discovery.fetch_time_budget_ms.max(1),
            execution_enabled,
        }
    }

    pub fn window_start(&self) -> DateTime<Utc> {
        self.now - Duration::minutes(self.window_minutes.min(i64::MAX as u64) as i64)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2Status {
    pub source: String,
    pub now: DateTime<Utc>,
    pub window_start: DateTime<Utc>,
    pub window_minutes: u64,
    pub max_tail_lag_seconds: u64,
    pub tail: Option<DiscoveryV2TailStatus>,
    pub coverage_sample: Option<DiscoveryV2CoverageSample>,
    pub scan: DiscoveryV2ScanStatus,
    pub filters: DiscoveryV2FilterStatus,
    pub wallet_metrics: Vec<DiscoveryV2WalletMetric>,
    pub candidate_wallets: Vec<String>,
    pub execution_enabled: bool,
    pub execution_disabled: bool,
    pub blockers: Vec<String>,
    pub production_green: bool,
    pub policy_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2TailStatus {
    pub cursor: DiscoveryRuntimeCursor,
    pub lag_seconds: i64,
    pub fresh: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2CoverageSample {
    pub ts: DateTime<Utc>,
    pub slot: u64,
    pub signature: String,
    pub wallet_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2ScanStatus {
    pub max_rows: usize,
    pub time_budget_ms: u64,
    pub rows_scanned: usize,
    pub unique_wallets: usize,
    pub max_rows_exhausted: bool,
    pub time_budget_exhausted: bool,
    pub budget_exhausted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2FilterStatus {
    pub total_wallets: usize,
    pub eligible_wallets: usize,
    pub rejected_wallets: usize,
    pub reject_breakdown: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2WalletMetric {
    pub wallet_id: String,
    pub trades: u32,
    pub active_days: u32,
    pub buys: u32,
    pub sells: u32,
    pub max_buy_notional_sol: f64,
    pub pnl_sol: f64,
    pub win_rate: f64,
    pub closed_trades: u32,
    pub hold_median_seconds: i64,
    pub buy_total: u32,
    pub tradable_ratio: f64,
    pub rug_ratio: f64,
    pub rug_lookahead_evaluated: u32,
    pub rug_lookahead_unevaluated: u32,
    pub eligible: bool,
    pub reject_reasons: Vec<String>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2PublishReport {
    pub dry_run: bool,
    pub committed: bool,
    pub runtime_mode: String,
    pub reason: String,
    pub scoring_source: String,
    pub policy_fingerprint: String,
    pub published_wallet_count: usize,
    pub status: DiscoveryV2Status,
}

#[derive(Debug, Clone)]
struct WalletAccumulator {
    trades: u32,
    buys: u32,
    sells: u32,
    spent_sol: f64,
    max_buy_notional_sol: f64,
    realized_pnl_sol: f64,
    wins: u32,
    closed_trades: u32,
    hold_samples_sec: Vec<i64>,
    realized_pnl_by_day: HashMap<NaiveDate, f64>,
    active_days: HashSet<NaiveDate>,
    tx_per_minute: HashMap<i64, u32>,
    suspicious: bool,
    positions: HashMap<String, VecDeque<OpenLot>>,
    buy_observations: Vec<BuyObservation>,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct OpenLot {
    qty: f64,
    cost_sol: f64,
    opened_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct BuyObservation {
    token: String,
    ts: DateTime<Utc>,
    tradable: bool,
}

#[derive(Debug, Clone)]
struct SolLegTrade {
    ts: DateTime<Utc>,
    wallet_id: String,
    sol_notional: f64,
}

#[derive(Debug, Clone, Copy)]
struct RugEvaluation {
    ratio: f64,
    evaluated: u32,
    unevaluated: u32,
}

#[derive(Debug, Clone, Default)]
struct TokenRollingState {
    sol_trades_5m: VecDeque<SolLegTrade>,
    sol_volume_5m: f64,
    sol_traders_5m: HashMap<String, u32>,
}

#[derive(Debug, Clone, Copy)]
enum BuyTradability {
    Tradable,
    Rejected,
}

pub fn build_discovery_v2_status(
    store: &SqliteStore,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    options: DiscoveryV2BuildOptions,
) -> Result<DiscoveryV2Status> {
    let window_start = options.window_start();
    let tail = load_tail_status(store, options.now, options.max_tail_lag_seconds)?;
    let coverage_sample = load_coverage_sample(store, window_start)?;
    let deadline = Instant::now() + StdDuration::from_millis(options.time_budget_ms);
    let mut swaps = Vec::new();

    let scan_limit = options.max_rows.saturating_add(1);
    let mut accepted_rows = 0usize;
    let page = store.for_each_observed_swap_in_window_after_cursor_with_budget(
        window_start,
        options.now,
        None,
        scan_limit,
        deadline,
        |swap| {
            if accepted_rows < options.max_rows {
                swaps.push(swap);
            }
            accepted_rows = accepted_rows.saturating_add(1);
            Ok(())
        },
    )?;

    let token_quality_cache = load_token_quality_cache_for_swaps(store, &swaps, options.now)?;
    let wallet_accumulators =
        build_wallet_accumulators(&swaps, discovery, shadow, &token_quality_cache, options.now);
    let token_sol_history = build_token_sol_history(&swaps);
    let mut wallet_metrics = wallet_accumulators
        .into_iter()
        .map(|(wallet_id, value)| {
            wallet_metric_from_accumulator(
                wallet_id,
                value,
                discovery,
                &token_sol_history,
                options.now,
            )
        })
        .collect::<Vec<_>>();
    wallet_metrics.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.trades.cmp(&left.trades))
            .then_with(|| left.wallet_id.cmp(&right.wallet_id))
    });

    let filters = build_filter_status(&wallet_metrics);
    let candidate_wallets = wallet_metrics
        .iter()
        .filter(|metric| metric.eligible && metric.score >= discovery.min_score)
        .take(discovery.follow_top_n.max(1) as usize)
        .map(|metric| metric.wallet_id.clone())
        .collect::<Vec<_>>();

    let max_rows_exhausted = page.rows_seen > options.max_rows;
    let budget_exhausted = max_rows_exhausted || page.time_budget_exhausted;
    let scan = DiscoveryV2ScanStatus {
        max_rows: options.max_rows,
        time_budget_ms: options.time_budget_ms,
        rows_scanned: accepted_rows.min(options.max_rows),
        unique_wallets: wallet_metrics.len(),
        max_rows_exhausted,
        time_budget_exhausted: page.time_budget_exhausted,
        budget_exhausted,
    };

    let mut blockers = Vec::new();
    if !shadow.quality_gates_enabled {
        blockers.push("discovery_v2_quality_gates_disabled".to_string());
    }
    if !discovery.require_open_positions_for_publication {
        blockers.push("discovery_v2_open_position_gate_disabled".to_string());
    }
    if discovery.max_rug_ratio >= 1.0 {
        blockers.push("discovery_v2_rug_gate_disabled".to_string());
    }
    if discovery.thin_market_min_volume_sol <= 0.0 {
        blockers.push("discovery_v2_thin_market_volume_gate_disabled".to_string());
    }
    if discovery.thin_market_min_unique_traders == 0 {
        blockers.push("discovery_v2_thin_market_trader_gate_disabled".to_string());
    }
    if !tail.as_ref().is_some_and(|status| status.fresh) {
        blockers.push("observed_swaps_tail_stale_or_missing".to_string());
    }
    if coverage_sample.is_none() {
        blockers.push("observed_swaps_window_sample_missing".to_string());
    }
    if scan.rows_scanned == 0 {
        blockers.push("observed_swaps_window_scan_empty".to_string());
    }
    if candidate_wallets.is_empty() {
        blockers.push("discovery_v2_candidate_wallets_empty".to_string());
    }
    if options.execution_enabled {
        blockers.push("execution_enabled".to_string());
    }
    if scan.budget_exhausted {
        blockers.push("discovery_v2_scan_budget_exhausted".to_string());
    }

    let production_green = blockers.is_empty();
    let policy_fingerprint = discovery_v2_policy_fingerprint(discovery, &options);

    Ok(DiscoveryV2Status {
        source: DISCOVERY_V2_SCORING_SOURCE.to_string(),
        now: options.now,
        window_start,
        window_minutes: options.window_minutes,
        max_tail_lag_seconds: options.max_tail_lag_seconds,
        tail,
        coverage_sample,
        scan,
        filters,
        wallet_metrics,
        candidate_wallets,
        execution_enabled: options.execution_enabled,
        execution_disabled: !options.execution_enabled,
        blockers,
        production_green,
        policy_fingerprint,
    })
}

pub fn publish_discovery_v2_status(
    store: &SqliteStore,
    status: DiscoveryV2Status,
    commit: bool,
) -> Result<DiscoveryV2PublishReport> {
    let runtime_mode = if status.production_green {
        DiscoveryRuntimeMode::Healthy
    } else {
        DiscoveryRuntimeMode::FailClosed
    };
    let reason = if status.production_green {
        "discovery_v2_operational_window_ready"
    } else {
        "discovery_v2_operational_window_blocked"
    };

    if commit {
        if !status.production_green {
            bail!(
                "discovery v2 publication is blocked; refusing to mutate publication state: {}",
                status.blockers.join(",")
            );
        }

        let wallets = status
            .wallet_metrics
            .iter()
            .map(|metric| WalletUpsertRow {
                wallet_id: metric.wallet_id.clone(),
                first_seen: metric.first_seen,
                last_seen: metric.last_seen,
                status: "active".to_string(),
            })
            .collect::<Vec<_>>();
        let metrics = status
            .wallet_metrics
            .iter()
            .map(|metric| WalletMetricRow {
                wallet_id: metric.wallet_id.clone(),
                window_start: status.window_start,
                pnl: metric.pnl_sol,
                win_rate: metric.win_rate,
                trades: metric.trades,
                closed_trades: metric.closed_trades,
                hold_median_seconds: metric.hold_median_seconds,
                score: metric.score,
                buy_total: metric.buy_total,
                tradable_ratio: metric.tradable_ratio,
                rug_ratio: metric.rug_ratio,
            })
            .collect::<Vec<_>>();

        store.persist_discovery_cycle(
            &wallets,
            &metrics,
            &status.candidate_wallets,
            true,
            true,
            status.now,
            reason,
        )?;
        store.set_discovery_publication_state_with_options(
            &DiscoveryPublicationStateUpdate {
                runtime_mode,
                reason: reason.to_string(),
                last_published_at: Some(status.now),
                last_published_window_start: Some(status.window_start),
                published_scoring_source: Some(DISCOVERY_V2_SCORING_SOURCE.to_string()),
                published_wallet_ids: Some(status.candidate_wallets.clone()),
            },
            false,
            Some(status.policy_fingerprint.as_str()),
        )?;
    }

    Ok(DiscoveryV2PublishReport {
        dry_run: !commit,
        committed: commit,
        runtime_mode: runtime_mode.as_str().to_string(),
        reason: reason.to_string(),
        scoring_source: DISCOVERY_V2_SCORING_SOURCE.to_string(),
        policy_fingerprint: status.policy_fingerprint.clone(),
        published_wallet_count: if status.production_green {
            status.candidate_wallets.len()
        } else {
            0
        },
        status,
    })
}

fn load_tail_status(
    store: &SqliteStore,
    now: DateTime<Utc>,
    max_tail_lag_seconds: u64,
) -> Result<Option<DiscoveryV2TailStatus>> {
    let Some(cursor) = store.observed_swaps_tail_cursor_read_only()? else {
        return Ok(None);
    };
    let lag_seconds = now
        .signed_duration_since(cursor.ts_utc)
        .num_seconds()
        .max(0);
    let fresh = lag_seconds <= max_tail_lag_seconds.min(i64::MAX as u64) as i64;
    Ok(Some(DiscoveryV2TailStatus {
        cursor,
        lag_seconds,
        fresh,
    }))
}

fn load_coverage_sample(
    store: &SqliteStore,
    window_start: DateTime<Utc>,
) -> Result<Option<DiscoveryV2CoverageSample>> {
    let (rows, _) = store
        .load_recent_observed_swaps_since(window_start, 1)
        .context("failed loading discovery v2 coverage sample")?;
    Ok(rows
        .into_iter()
        .next()
        .map(|swap| DiscoveryV2CoverageSample {
            ts: swap.ts_utc,
            slot: swap.slot,
            signature: swap.signature,
            wallet_id: swap.wallet,
        }))
}

fn load_token_quality_cache_for_swaps(
    store: &SqliteStore,
    swaps: &[SwapEvent],
    now: DateTime<Utc>,
) -> Result<HashMap<String, TokenQualityCacheRow>> {
    let mut mints = HashSet::new();
    for swap in swaps {
        if is_sol_buy(swap) {
            mints.insert(swap.token_out.clone());
        }
    }

    let ttl = Duration::seconds(TOKEN_QUALITY_TTL_SECONDS);
    let mut cache = HashMap::new();
    for mint in mints {
        if let Some(row) = store.get_token_quality_cache(&mint)? {
            if now - row.fetched_at <= ttl {
                cache.insert(mint, row);
            }
        }
    }
    Ok(cache)
}

fn build_wallet_accumulators(
    swaps: &[SwapEvent],
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    token_quality_cache: &HashMap<String, TokenQualityCacheRow>,
    now: DateTime<Utc>,
) -> HashMap<String, WalletAccumulator> {
    let mut wallets = HashMap::new();
    let mut token_states = HashMap::new();

    for swap in swaps {
        let tradability = update_token_state_and_buy_tradability(
            &mut token_states,
            token_quality_cache,
            shadow,
            swap,
        );
        let entry = wallets
            .entry(swap.wallet.clone())
            .or_insert_with(|| WalletAccumulator::new(swap.ts_utc));
        entry.observe_swap(swap, discovery, tradability, now);
    }
    wallets
}

fn build_token_sol_history(swaps: &[SwapEvent]) -> HashMap<String, Vec<SolLegTrade>> {
    let mut history: HashMap<String, Vec<SolLegTrade>> = HashMap::new();
    for swap in swaps {
        let Some((token, sol_notional)) = sol_leg_token_and_notional(swap) else {
            continue;
        };
        history
            .entry(token.to_string())
            .or_default()
            .push(SolLegTrade {
                ts: swap.ts_utc,
                wallet_id: swap.wallet.clone(),
                sol_notional: sol_notional.max(0.0),
            });
    }
    for trades in history.values_mut() {
        trades.sort_by(|left, right| {
            left.ts
                .cmp(&right.ts)
                .then_with(|| left.wallet_id.cmp(&right.wallet_id))
        });
    }
    history
}

fn wallet_metric_from_accumulator(
    wallet_id: String,
    acc: WalletAccumulator,
    discovery: &DiscoveryConfig,
    token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
    now: DateTime<Utc>,
) -> DiscoveryV2WalletMetric {
    let active_days = acc.active_days.len() as u32;
    let hold_median_seconds = median_i64(&acc.hold_samples_sec).unwrap_or(0);
    let win_rate = if acc.closed_trades > 0 {
        acc.wins as f64 / acc.closed_trades as f64
    } else {
        0.0
    };
    let consistency_ratio = if !acc.realized_pnl_by_day.is_empty() {
        let positive_days = acc
            .realized_pnl_by_day
            .values()
            .filter(|value| **value > 0.0)
            .count() as f64;
        positive_days / acc.realized_pnl_by_day.len() as f64
    } else {
        0.0
    };
    let roi = if acc.spent_sol > 1e-9 {
        acc.realized_pnl_sol / acc.spent_sol
    } else {
        0.0
    };
    let buy_total = acc.buy_observations.len() as u32;
    let tradable_buys = acc
        .buy_observations
        .iter()
        .filter(|buy| buy.tradable)
        .count() as u32;
    let tradable_ratio = if buy_total > 0 {
        tradable_buys as f64 / buy_total as f64
    } else {
        0.0
    };
    let rug_evaluation =
        compute_rug_evaluation(&acc.buy_observations, token_sol_history, discovery, now);
    let rug_ratio = rug_evaluation.ratio;
    let base_score = (0.35 * tanh01(acc.realized_pnl_sol / 2.0))
        + (0.20 * tanh01(roi * 3.0))
        + (0.15 * (win_rate * (acc.closed_trades as f64 / 8.0).min(1.0)).clamp(0.0, 1.0))
        + (0.15 * hold_time_quality_score(hold_median_seconds))
        + (0.10 * consistency_ratio.clamp(0.0, 1.0))
        + (0.05 * if acc.suspicious { 0.0 } else { 1.0 });
    let rug_checks_disabled = discovery.max_rug_ratio >= 1.0;
    let rug_penalty = if rug_checks_disabled {
        1.0
    } else {
        (1.0 - rug_ratio).clamp(0.0, 1.0).powi(2)
    };
    let raw_score = (base_score * tradable_ratio.powf(1.5) * rug_penalty).clamp(0.0, 1.0);
    let decay_cutoff = now - Duration::days(discovery.decay_window_days.max(1) as i64);

    let mut reject_reasons = Vec::new();
    if acc.trades < discovery.min_trades {
        reject_reasons.push("insufficient_trades".to_string());
    }
    if active_days < discovery.min_active_days {
        reject_reasons.push("insufficient_active_days".to_string());
    }
    if acc.suspicious {
        reject_reasons.push("suspicious_activity".to_string());
    }
    if acc.max_buy_notional_sol < discovery.min_leader_notional_sol {
        reject_reasons.push("low_notional".to_string());
    }
    if acc.last_seen < decay_cutoff {
        reject_reasons.push("stale_last_seen".to_string());
    }
    if buy_total < discovery.min_buy_count {
        reject_reasons.push("insufficient_buy_count".to_string());
    }
    if tradable_ratio < discovery.min_tradable_ratio {
        reject_reasons.push("low_tradable_ratio".to_string());
    }
    if !rug_checks_disabled && rug_evaluation.unevaluated > 0 {
        reject_reasons.push("rug_lookahead_unevaluated".to_string());
    }
    if !rug_checks_disabled && rug_ratio > discovery.max_rug_ratio {
        reject_reasons.push("rug_gate".to_string());
    }
    if discovery.require_open_positions_for_publication
        && !acc.has_actionable_open_positions(now, discovery.metric_snapshot_interval_seconds)
    {
        reject_reasons.push("open_position_required_missing".to_string());
    }

    let mut score = raw_score;
    if !reject_reasons.is_empty() {
        score = 0.0;
    } else if score < discovery.min_score {
        reject_reasons.push("below_min_score".to_string());
    }
    let eligible = reject_reasons.is_empty();

    DiscoveryV2WalletMetric {
        wallet_id,
        trades: acc.trades,
        active_days,
        buys: buy_total,
        sells: acc.sells,
        max_buy_notional_sol: acc.max_buy_notional_sol,
        pnl_sol: acc.realized_pnl_sol,
        win_rate,
        closed_trades: acc.closed_trades,
        hold_median_seconds,
        buy_total,
        tradable_ratio,
        rug_ratio,
        rug_lookahead_evaluated: rug_evaluation.evaluated,
        rug_lookahead_unevaluated: rug_evaluation.unevaluated,
        eligible,
        reject_reasons,
        first_seen: acc.first_seen,
        last_seen: acc.last_seen,
        score,
    }
}

fn build_filter_status(wallet_metrics: &[DiscoveryV2WalletMetric]) -> DiscoveryV2FilterStatus {
    let mut reject_breakdown = BTreeMap::new();
    for metric in wallet_metrics {
        for reason in &metric.reject_reasons {
            *reject_breakdown.entry(reason.clone()).or_insert(0) += 1;
        }
    }
    let eligible_wallets = wallet_metrics
        .iter()
        .filter(|metric| metric.eligible)
        .count();
    DiscoveryV2FilterStatus {
        total_wallets: wallet_metrics.len(),
        eligible_wallets,
        rejected_wallets: wallet_metrics.len().saturating_sub(eligible_wallets),
        reject_breakdown,
    }
}

fn update_token_state_and_buy_tradability(
    token_states: &mut HashMap<String, TokenRollingState>,
    token_quality_cache: &HashMap<String, TokenQualityCacheRow>,
    shadow: &ShadowConfig,
    swap: &SwapEvent,
) -> Option<BuyTradability> {
    let Some((token, sol_notional)) = sol_leg_token_and_notional(swap) else {
        return None;
    };
    let state = token_states.entry(token.to_string()).or_default();
    evict_expired_token_trades(state, swap.ts_utc);
    let trade = SolLegTrade {
        ts: swap.ts_utc,
        wallet_id: swap.wallet.clone(),
        sol_notional: sol_notional.max(0.0),
    };
    state.sol_volume_5m += trade.sol_notional;
    state
        .sol_traders_5m
        .entry(trade.wallet_id.clone())
        .and_modify(|count| *count += 1)
        .or_insert(1);
    state.sol_trades_5m.push_back(trade);

    if !is_sol_buy(swap) {
        return None;
    }
    Some(evaluate_buy_tradability(
        state,
        token_quality_cache.get(token),
        shadow,
    ))
}

fn evaluate_buy_tradability(
    state: &TokenRollingState,
    quality: Option<&TokenQualityCacheRow>,
    shadow: &ShadowConfig,
) -> BuyTradability {
    if !shadow.quality_gates_enabled {
        return BuyTradability::Tradable;
    }
    if shadow.min_volume_5m_sol > 0.0 && state.sol_volume_5m + 1e-12 < shadow.min_volume_5m_sol {
        return BuyTradability::Rejected;
    }
    if shadow.min_unique_traders_5m > 0
        && state.sol_traders_5m.len() < shadow.min_unique_traders_5m as usize
    {
        return BuyTradability::Rejected;
    }
    if shadow.min_token_age_seconds > 0 {
        let Some(token_age_seconds) = quality.and_then(|row| row.token_age_seconds) else {
            return BuyTradability::Rejected;
        };
        if token_age_seconds < shadow.min_token_age_seconds {
            return BuyTradability::Rejected;
        }
    }
    if shadow.min_holders > 0 {
        let Some(holders) = quality.and_then(|row| row.holders) else {
            return BuyTradability::Rejected;
        };
        if holders < shadow.min_holders {
            return BuyTradability::Rejected;
        }
    }
    if shadow.min_liquidity_sol > 0.0 {
        let Some(liquidity_sol) = quality.and_then(|row| row.liquidity_sol) else {
            return BuyTradability::Rejected;
        };
        if liquidity_sol + 1e-12 < shadow.min_liquidity_sol {
            return BuyTradability::Rejected;
        }
    }
    BuyTradability::Tradable
}

fn evict_expired_token_trades(state: &mut TokenRollingState, now: DateTime<Utc>) {
    let cutoff = now - Duration::minutes(5);
    while let Some(front) = state.sol_trades_5m.front() {
        if front.ts >= cutoff {
            break;
        }
        let expired = state
            .sol_trades_5m
            .pop_front()
            .expect("checked front exists");
        state.sol_volume_5m = (state.sol_volume_5m - expired.sol_notional).max(0.0);
        if let Some(count) = state.sol_traders_5m.get_mut(&expired.wallet_id) {
            *count -= 1;
            if *count == 0 {
                state.sol_traders_5m.remove(&expired.wallet_id);
            }
        }
    }
}

fn compute_rug_evaluation(
    buys: &[BuyObservation],
    token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
    discovery: &DiscoveryConfig,
    now: DateTime<Utc>,
) -> RugEvaluation {
    if buys.is_empty() {
        return RugEvaluation {
            ratio: 0.0,
            evaluated: 0,
            unevaluated: 0,
        };
    }
    let lookahead = Duration::seconds(discovery.rug_lookahead_seconds.max(1) as i64);
    let mut evaluated = 0u32;
    let mut rugged = 0u32;
    let mut unevaluated = 0u32;
    for buy in buys {
        let window_end = buy.ts + lookahead;
        if window_end > now {
            unevaluated = unevaluated.saturating_add(1);
            continue;
        }
        evaluated = evaluated.saturating_add(1);
        let Some(trades) = token_sol_history.get(&buy.token) else {
            rugged = rugged.saturating_add(1);
            continue;
        };
        let mut volume_sol = 0.0;
        let mut unique_traders = HashSet::new();
        for trade in trades {
            if trade.ts < buy.ts {
                continue;
            }
            if trade.ts > window_end {
                break;
            }
            volume_sol += trade.sol_notional;
            unique_traders.insert(trade.wallet_id.as_str());
        }
        let thin_volume = volume_sol + 1e-12 < discovery.thin_market_min_volume_sol;
        let thin_traders = unique_traders.len() < discovery.thin_market_min_unique_traders as usize;
        if thin_volume || thin_traders {
            rugged = rugged.saturating_add(1);
        }
    }
    let risky = rugged.saturating_add(unevaluated);
    let total = evaluated.saturating_add(unevaluated);
    let ratio = if total == 0 {
        0.0
    } else {
        risky as f64 / total as f64
    };
    RugEvaluation {
        ratio,
        evaluated,
        unevaluated,
    }
}

fn is_sol_buy(swap: &SwapEvent) -> bool {
    swap.token_in == SOL_MINT && swap.token_out != SOL_MINT
}

fn is_sol_sell(swap: &SwapEvent) -> bool {
    swap.token_out == SOL_MINT && swap.token_in != SOL_MINT
}

fn sol_leg_token_and_notional(swap: &SwapEvent) -> Option<(&str, f64)> {
    if is_sol_buy(swap) {
        Some((swap.token_out.as_str(), swap.amount_in))
    } else if is_sol_sell(swap) {
        Some((swap.token_in.as_str(), swap.amount_out))
    } else {
        None
    }
}

fn tanh01(value: f64) -> f64 {
    ((value.tanh() + 1.0) * 0.5).clamp(0.0, 1.0)
}

fn hold_time_quality_score(median_seconds: i64) -> f64 {
    if median_seconds <= 0 {
        0.0
    } else if median_seconds < 45 {
        0.2
    } else if median_seconds < 120 {
        0.5
    } else if median_seconds <= 6 * 60 * 60 {
        1.0
    } else if median_seconds <= 24 * 60 * 60 {
        0.75
    } else {
        0.4
    }
}

fn median_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        Some(sorted[mid])
    } else {
        Some((sorted[mid - 1] + sorted[mid]) / 2)
    }
}

impl WalletAccumulator {
    fn new(ts: DateTime<Utc>) -> Self {
        Self {
            trades: 0,
            buys: 0,
            sells: 0,
            spent_sol: 0.0,
            max_buy_notional_sol: 0.0,
            realized_pnl_sol: 0.0,
            wins: 0,
            closed_trades: 0,
            hold_samples_sec: Vec::new(),
            realized_pnl_by_day: HashMap::new(),
            active_days: HashSet::new(),
            tx_per_minute: HashMap::new(),
            suspicious: false,
            positions: HashMap::new(),
            buy_observations: Vec::new(),
            first_seen: ts,
            last_seen: ts,
        }
    }

    fn observe_swap(
        &mut self,
        swap: &SwapEvent,
        discovery: &DiscoveryConfig,
        tradability: Option<BuyTradability>,
        _now: DateTime<Utc>,
    ) {
        self.trades = self.trades.saturating_add(1);
        self.active_days.insert(swap.ts_utc.date_naive());
        self.first_seen = self.first_seen.min(swap.ts_utc);
        self.last_seen = self.last_seen.max(swap.ts_utc);
        self.mark_tx_minute(swap.ts_utc.timestamp() / 60, discovery.max_tx_per_minute);

        if is_sol_buy(swap) {
            self.observe_buy(
                swap.token_out.as_str(),
                swap.amount_out,
                swap.amount_in,
                swap.ts_utc,
                tradability.unwrap_or(BuyTradability::Rejected),
            );
        } else if is_sol_sell(swap) {
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
        self.buys = self.buys.saturating_add(1);
        if qty <= 0.0 || cost_sol <= 0.0 {
            return;
        }
        self.spent_sol += cost_sol;
        self.max_buy_notional_sol = self.max_buy_notional_sol.max(cost_sol);
        self.buy_observations.push(BuyObservation {
            token: token.to_string(),
            ts,
            tradable: matches!(tradability, BuyTradability::Tradable),
        });
        self.positions
            .entry(token.to_string())
            .or_default()
            .push_back(OpenLot {
                qty,
                cost_sol,
                opened_at: ts,
            });
    }

    fn observe_sell(&mut self, token: &str, qty: f64, proceeds_sol: f64, ts: DateTime<Utc>) {
        self.sells = self.sells.saturating_add(1);
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
            let Some(front) = lots.front_mut() else {
                break;
            };
            if front.qty <= 1e-12 {
                lots.pop_front();
                continue;
            }
            let take_qty = qty_remaining.min(front.qty);
            let lot_fraction = take_qty / front.qty;
            let cost_part = front.cost_sol * lot_fraction;
            let opened_at = front.opened_at;
            front.qty -= take_qty;
            front.cost_sol -= cost_part;
            qty_remaining -= take_qty;
            matched_qty += take_qty;
            sell_pnl += proceeds_sol * (take_qty / qty) - cost_part;
            self.hold_samples_sec
                .push((ts - opened_at).num_seconds().max(0));
            if front.qty <= 1e-12 {
                lots.pop_front();
            }
        }
        if lots.is_empty() {
            self.positions.remove(token);
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

    fn has_actionable_open_positions(
        &self,
        now: DateTime<Utc>,
        metric_snapshot_interval_seconds: u64,
    ) -> bool {
        let max_open_age_seconds =
            self.actionable_open_position_max_age_seconds(metric_snapshot_interval_seconds);
        self.positions.values().flatten().any(|lot| {
            lot.qty > 1e-12
                && lot.cost_sol > 1e-12
                && max_open_age_seconds
                    .is_none_or(|age_limit| (now - lot.opened_at).num_seconds().max(0) <= age_limit)
        })
    }

    fn actionable_open_position_max_age_seconds(
        &self,
        metric_snapshot_interval_seconds: u64,
    ) -> Option<i64> {
        if self.hold_samples_sec.len() < ACTIONABLE_OPEN_POSITION_MIN_HOLD_SAMPLES {
            return None;
        }
        let cadence_floor_seconds =
            i64::try_from(metric_snapshot_interval_seconds.max(1)).unwrap_or(i64::MAX);
        let historical_hold_allowance_seconds = self
            .hold_samples_sec
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
            .saturating_mul(ACTIONABLE_OPEN_POSITION_HOLD_MULTIPLIER);
        Some(cadence_floor_seconds.max(historical_hold_allowance_seconds))
    }
}

fn discovery_v2_policy_fingerprint(
    discovery: &DiscoveryConfig,
    options: &DiscoveryV2BuildOptions,
) -> String {
    format!(
        concat!(
            "scoring_source={};",
            "window_minutes={};",
            "max_tail_lag_seconds={};",
            "max_rows={};",
            "time_budget_ms={};",
            "follow_top_n={};",
            "min_leader_notional_sol={:.6};",
            "min_trades={};",
            "min_active_days={};",
            "min_score={:.6};",
            "max_tx_per_minute={};",
            "min_buy_count={};",
            "min_tradable_ratio={:.6};",
            "require_open_positions_for_publication={};",
            "max_rug_ratio={:.6};",
            "thin_market_min_volume_sol={:.6};",
            "thin_market_min_unique_traders={};",
            "execution_required_disabled=true"
        ),
        DISCOVERY_V2_SCORING_SOURCE,
        options.window_minutes,
        options.max_tail_lag_seconds,
        options.max_rows,
        options.time_budget_ms,
        discovery.follow_top_n,
        discovery.min_leader_notional_sol,
        discovery.min_trades,
        discovery.min_active_days,
        discovery.min_score,
        discovery.max_tx_per_minute,
        discovery.min_buy_count,
        discovery.min_tradable_ratio,
        discovery.require_open_positions_for_publication,
        discovery.max_rug_ratio,
        discovery.thin_market_min_volume_sol,
        discovery.thin_market_min_unique_traders,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_core_types::SwapEvent;
    use std::path::PathBuf;
    use tempfile::tempdir;

    const TOKEN_MINT: &str = "TokenMint111111111111111111111111111111111";

    fn test_store() -> Result<(tempfile::TempDir, SqliteStore)> {
        let dir = tempdir()?;
        let path = dir.path().join("runtime.db");
        let mut store = SqliteStore::open(&path)?;
        let migration_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        Ok((dir, store))
    }

    fn swap(wallet: &str, signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
        SwapEvent {
            wallet: wallet.to_string(),
            dex: "test".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: TOKEN_MINT.to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot,
            ts_utc,
            exact_amounts: None,
        }
    }

    fn options(now: DateTime<Utc>) -> DiscoveryV2BuildOptions {
        DiscoveryV2BuildOptions {
            now,
            window_minutes: 60,
            max_tail_lag_seconds: 1_200,
            max_rows: 100,
            time_budget_ms: 5_000,
            execution_enabled: false,
        }
    }

    fn strict_policy() -> (DiscoveryConfig, ShadowConfig) {
        let mut discovery = DiscoveryConfig::default();
        discovery.min_leader_notional_sol = 0.0;
        discovery.min_trades = 1;
        discovery.min_active_days = 1;
        discovery.min_score = 0.0;
        discovery.min_buy_count = 1;
        discovery.min_tradable_ratio = 0.25;
        discovery.require_open_positions_for_publication = true;
        discovery.max_rug_ratio = 0.60;
        discovery.rug_lookahead_seconds = 60;
        discovery.thin_market_min_volume_sol = 0.5;
        discovery.thin_market_min_unique_traders = 1;
        let mut shadow = ShadowConfig::default();
        shadow.quality_gates_enabled = true;
        shadow.min_token_age_seconds = 30;
        shadow.min_holders = 5;
        shadow.min_liquidity_sol = 1.0;
        shadow.min_volume_5m_sol = 0.5;
        shadow.min_unique_traders_5m = 1;
        (discovery, shadow)
    }

    fn insert_quality(
        store: &SqliteStore,
        now: DateTime<Utc>,
        liquidity_sol: Option<f64>,
    ) -> Result<()> {
        store.upsert_token_quality_cache(TOKEN_MINT, Some(5), liquidity_sol, Some(60), now)
    }

    #[test]
    fn status_ready_when_tail_sample_scan_and_candidates_are_valid() -> Result<()> {
        let (_dir, store) = test_store()?;
        let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
        store.insert_observed_swaps_batch(&[
            swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)),
            swap("wallet_b", "sig-b", 11, now - Duration::minutes(5)),
        ])?;
        insert_quality(&store, now, Some(1.0))?;

        let (discovery, shadow) = strict_policy();
        let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

        assert!(status.production_green);
        assert!(status.blockers.is_empty());
        assert_eq!(status.scan.rows_scanned, 2);
        assert_eq!(status.candidate_wallets.len(), 2);
        assert_eq!(status.filters.eligible_wallets, 2);
        assert_eq!(status.source, DISCOVERY_V2_SCORING_SOURCE);
        Ok(())
    }

    #[test]
    fn status_blocks_when_row_budget_is_exhausted() -> Result<()> {
        let (_dir, store) = test_store()?;
        let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
        store.insert_observed_swaps_batch(&[
            swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)),
            swap("wallet_b", "sig-b", 11, now - Duration::minutes(9)),
        ])?;
        insert_quality(&store, now, Some(1.0))?;
        let mut build_options = options(now);
        build_options.max_rows = 1;

        let (discovery, shadow) = strict_policy();
        let status = build_discovery_v2_status(&store, &discovery, &shadow, build_options)?;

        assert!(!status.production_green);
        assert!(status.scan.budget_exhausted);
        assert!(status
            .blockers
            .contains(&"discovery_v2_scan_budget_exhausted".to_string()));
        Ok(())
    }

    #[test]
    fn status_applies_selector_filters_before_candidate_publication() -> Result<()> {
        let (_dir, store) = test_store()?;
        let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
        store.insert_observed_swaps_batch(&[
            swap("wallet_pass", "sig-pass-1", 10, now - Duration::minutes(10)),
            swap("wallet_pass", "sig-pass-2", 11, now - Duration::minutes(9)),
            swap(
                "wallet_fail_trades",
                "sig-fail-1",
                12,
                now - Duration::minutes(8),
            ),
        ])?;
        insert_quality(&store, now, Some(1.0))?;
        let (mut discovery, shadow) = strict_policy();
        discovery.min_trades = 2;

        let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

        assert!(status.production_green);
        assert_eq!(status.candidate_wallets, vec!["wallet_pass".to_string()]);
        assert_eq!(status.filters.total_wallets, 2);
        assert_eq!(status.filters.eligible_wallets, 1);
        assert_eq!(
            status
                .filters
                .reject_breakdown
                .get("insufficient_trades")
                .copied(),
            Some(1)
        );
        assert!(!status
            .candidate_wallets
            .contains(&"wallet_fail_trades".to_string()));
        Ok(())
    }

    #[test]
    fn status_blocks_when_filters_remove_the_publishable_universe() -> Result<()> {
        let (_dir, store) = test_store()?;
        let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
        store.insert_observed_swap(&swap(
            "wallet_fail_trades",
            "sig-fail-1",
            12,
            now - Duration::minutes(8),
        ))?;
        insert_quality(&store, now, Some(1.0))?;
        let (mut discovery, shadow) = strict_policy();
        discovery.min_trades = 2;

        let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

        assert!(!status.production_green);
        assert!(status.candidate_wallets.is_empty());
        assert!(status
            .blockers
            .contains(&"discovery_v2_candidate_wallets_empty".to_string()));
        assert_eq!(status.filters.eligible_wallets, 0);
        Ok(())
    }

    #[test]
    fn status_blocks_when_liquidity_quality_evidence_is_missing() -> Result<()> {
        let (_dir, store) = test_store()?;
        let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
        store.insert_observed_swap(&swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)))?;
        insert_quality(&store, now, None)?;

        let (discovery, shadow) = strict_policy();
        let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

        assert!(!status.production_green);
        assert!(status.candidate_wallets.is_empty());
        assert_eq!(status.wallet_metrics[0].tradable_ratio, 0.0);
        assert!(status.wallet_metrics[0]
            .reject_reasons
            .contains(&"low_tradable_ratio".to_string()));
        assert_eq!(
            status
                .filters
                .reject_breakdown
                .get("low_tradable_ratio")
                .copied(),
            Some(1)
        );
        Ok(())
    }

    #[test]
    fn status_blocks_when_rug_lookahead_is_unevaluated() -> Result<()> {
        let (_dir, store) = test_store()?;
        let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
        store.insert_observed_swap(&swap("wallet_a", "sig-a", 10, now - Duration::minutes(1)))?;
        insert_quality(&store, now, Some(1.0))?;

        let (mut discovery, shadow) = strict_policy();
        discovery.rug_lookahead_seconds = 900;
        let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

        assert!(!status.production_green);
        assert!(status.candidate_wallets.is_empty());
        assert_eq!(status.wallet_metrics[0].rug_lookahead_evaluated, 0);
        assert_eq!(status.wallet_metrics[0].rug_lookahead_unevaluated, 1);
        assert_eq!(status.wallet_metrics[0].rug_ratio, 1.0);
        assert!(status.wallet_metrics[0]
            .reject_reasons
            .contains(&"rug_lookahead_unevaluated".to_string()));
        assert!(status.wallet_metrics[0]
            .reject_reasons
            .contains(&"rug_gate".to_string()));
        Ok(())
    }

    #[test]
    fn status_blocks_when_required_publication_gates_are_disabled() -> Result<()> {
        let (_dir, store) = test_store()?;
        let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
        store.insert_observed_swap(&swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)))?;
        insert_quality(&store, now, Some(1.0))?;

        let (mut discovery, mut shadow) = strict_policy();
        discovery.require_open_positions_for_publication = false;
        discovery.max_rug_ratio = 1.0;
        discovery.thin_market_min_volume_sol = 0.0;
        discovery.thin_market_min_unique_traders = 0;
        shadow.quality_gates_enabled = false;
        let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

        assert!(!status.production_green);
        assert!(status
            .blockers
            .contains(&"discovery_v2_quality_gates_disabled".to_string()));
        assert!(status
            .blockers
            .contains(&"discovery_v2_open_position_gate_disabled".to_string()));
        assert!(status
            .blockers
            .contains(&"discovery_v2_rug_gate_disabled".to_string()));
        assert!(status
            .blockers
            .contains(&"discovery_v2_thin_market_volume_gate_disabled".to_string()));
        assert!(status
            .blockers
            .contains(&"discovery_v2_thin_market_trader_gate_disabled".to_string()));
        Ok(())
    }

    #[test]
    fn publish_dry_run_does_not_write_publication_state() -> Result<()> {
        let (_dir, store) = test_store()?;
        let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
        store.insert_observed_swap(&swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)))?;
        insert_quality(&store, now, Some(1.0))?;
        let (discovery, shadow) = strict_policy();
        let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

        let report = publish_discovery_v2_status(&store, status, false)?;

        assert!(report.dry_run);
        assert!(!report.committed);
        assert!(store.discovery_publication_state_read_only()?.is_none());
        assert!(store.list_active_follow_wallets()?.is_empty());
        Ok(())
    }

    #[test]
    fn publish_commit_writes_followlist_and_publication_state_when_green() -> Result<()> {
        let (_dir, store) = test_store()?;
        let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
        store.insert_observed_swap(&swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)))?;
        insert_quality(&store, now, Some(1.0))?;
        let (discovery, shadow) = strict_policy();
        let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

        let report = publish_discovery_v2_status(&store, status, true)?;

        assert!(!report.dry_run);
        assert!(report.committed);
        assert_eq!(report.published_wallet_count, 1);
        assert!(store.list_active_follow_wallets()?.contains("wallet_a"));
        let state = store
            .discovery_publication_state_read_only()?
            .expect("publication state");
        assert_eq!(state.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(
            state.published_scoring_source.as_deref(),
            Some(DISCOVERY_V2_SCORING_SOURCE)
        );
        assert_eq!(
            state.published_wallet_ids,
            Some(vec!["wallet_a".to_string()])
        );
        assert!(state.publication_policy_fingerprint.is_some());
        Ok(())
    }

    #[test]
    fn publish_commit_refuses_to_mutate_when_blocked() -> Result<()> {
        let (_dir, store) = test_store()?;
        let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
        let (discovery, shadow) = strict_policy();
        let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;
        assert!(!status.production_green);

        let err = publish_discovery_v2_status(&store, status, true).expect_err("blocked publish");

        assert!(err
            .to_string()
            .contains("refusing to mutate publication state"));
        assert!(store.discovery_publication_state_read_only()?.is_none());
        assert!(store.list_active_follow_wallets()?.is_empty());
        Ok(())
    }
}
