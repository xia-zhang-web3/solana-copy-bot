use super::{
    DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, DiscoveryScoringBatchStageTimings,
    DiscoveryScoringBoundarySeedLot, DiscoveryScoringBoundarySeedSnapshot,
    DiscoveryScoringCheckpointedBatchTimings, SqliteStore, TokenQualityCacheRow,
    WalletScoringQualitySource,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use copybot_core_types::SwapEvent;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

const QUALITY_CACHE_TTL_SECONDS: i64 = 10 * 60;
const QUALITY_RPC_TIMEOUT_MS: u64 = 700;
const QUALITY_MAX_SIGNATURE_PAGES: u32 = 1;
const QUALITY_MAX_FETCH_PER_BATCH: usize = 20;
const QUALITY_RPC_BUDGET_MS: u64 = 1_500;
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const BOUNDARY_LOT_BUILDER_STATE_KEY: &str = "boundary_lot_builder_state_json";

#[derive(Debug, Clone)]
struct BuilderLot {
    buy_signature: String,
    wallet_id: String,
    token: String,
    qty: f64,
    cost_sol: f64,
    opened_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct MarketWindowEvent {
    ts: DateTime<Utc>,
    wallet_id: String,
    sol_notional: f64,
}

#[derive(Debug, Default)]
struct RollingTokenMarketState {
    events: VecDeque<MarketWindowEvent>,
    wallet_counts: HashMap<String, usize>,
    volume_sol: f64,
    liquidity_sol_proxy: f64,
}

#[derive(Debug, Clone)]
struct BuilderQualitySnapshot {
    source: WalletScoringQualitySource,
    token_age_seconds: Option<u64>,
    holders: Option<u64>,
    liquidity_sol: Option<f64>,
}

#[derive(Debug, Clone)]
struct BuilderQualityCacheState {
    holders: Option<u64>,
    liquidity_sol: Option<f64>,
    token_age_seconds: Option<u64>,
    fetched_at: DateTime<Utc>,
}

#[derive(Debug, Default)]
struct QualityFetchBudget {
    rpc_attempted: usize,
    started_at: Option<Instant>,
}

#[derive(Debug, Clone)]
struct QualityCacheUpsert {
    mint: String,
    holders: Option<u64>,
    liquidity_sol: Option<f64>,
    token_age_seconds: Option<u64>,
    fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct PendingBuyFactRow {
    buy_signature: String,
    wallet_id: String,
    token: String,
    ts: DateTime<Utc>,
    activity_day: NaiveDate,
    notional_sol: f64,
    market_volume_5m_sol: f64,
    market_unique_traders_5m: u32,
    market_liquidity_proxy_sol: f64,
    quality_source: WalletScoringQualitySource,
    quality_token_age_seconds: Option<u64>,
    quality_holders: Option<u64>,
    quality_liquidity_sol: Option<f64>,
    rug_check_after_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct PendingCloseFactRow {
    sell_signature: String,
    segment_index: i64,
    wallet_id: String,
    token: String,
    closed_ts: DateTime<Utc>,
    activity_day: NaiveDate,
    pnl_sol: f64,
    hold_seconds: i64,
    win: bool,
}

#[derive(Debug, Default)]
struct PreparedDiscoveryScoringBuilderBatch {
    day_deltas: HashMap<(String, NaiveDate), DayDelta>,
    tx_minute_deltas: HashMap<(String, i64), u32>,
    buy_rows: Vec<PendingBuyFactRow>,
    close_rows: Vec<PendingCloseFactRow>,
    lot_mutations: HashMap<String, Option<BuilderLot>>,
    quality_upserts: HashMap<String, QualityCacheUpsert>,
}

#[derive(Debug, Clone, Default)]
struct DayDelta {
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    trades: u32,
    spent_sol: f64,
    max_buy_notional_sol: f64,
}

#[derive(Debug, Clone, Copy)]
struct TokenMarketSnapshot {
    volume_5m_sol: f64,
    unique_traders_5m: u32,
    liquidity_sol_proxy: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WalletTokenKey {
    wallet_id: String,
    token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CursorRef<'a> {
    ts: DateTime<Utc>,
    slot: u64,
    signature: &'a str,
}

#[derive(Debug)]
pub struct DiscoveryScoringReplayBuilder {
    open_lots: HashMap<WalletTokenKey, VecDeque<BuilderLot>>,
    loaded_open_lot_keys: HashSet<WalletTokenKey>,
    lazy_open_lot_loading: bool,
    market_windows: HashMap<String, RollingTokenMarketState>,
    quality_cache: HashMap<String, Option<BuilderQualityCacheState>>,
}

#[derive(Debug)]
pub struct DiscoveryScoringBoundaryLotBuilder {
    open_lots: HashMap<WalletTokenKey, VecDeque<BuilderLot>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedBoundaryLotBuilderState {
    progress_start_ts: DateTime<Utc>,
    cursor: DiscoveryRuntimeCursor,
    open_lots: Vec<DiscoveryScoringBoundarySeedLot>,
}

#[derive(Debug, Clone)]
enum LotMutationAction {
    Upsert(BuilderLot),
    Delete(String),
}

#[derive(Debug, Default)]
struct LotAccountingStep {
    close_rows: Vec<PendingCloseFactRow>,
    mutations: Vec<LotMutationAction>,
}

fn parse_ts(raw: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("invalid {field} rfc3339 value: {raw}"))
}

fn cmp_swap_to_cursor(swap: &SwapEvent, cursor: CursorRef<'_>) -> std::cmp::Ordering {
    swap.ts_utc
        .cmp(&cursor.ts)
        .then_with(|| swap.slot.cmp(&cursor.slot))
        .then_with(|| swap.signature.as_str().cmp(cursor.signature))
}

fn is_sol_buy(swap: &SwapEvent) -> bool {
    swap.token_in == SOL_MINT && swap.token_out != SOL_MINT
}

fn is_sol_sell(swap: &SwapEvent) -> bool {
    swap.token_out == SOL_MINT && swap.token_in != SOL_MINT
}

fn sol_pair_market_event(swap: &SwapEvent) -> Option<(&str, f64)> {
    if is_sol_buy(swap) {
        Some((swap.token_out.as_str(), swap.amount_in.max(0.0)))
    } else if is_sol_sell(swap) {
        Some((swap.token_in.as_str(), swap.amount_out.max(0.0)))
    } else {
        None
    }
}

impl RollingTokenMarketState {
    fn evict_before(&mut self, cutoff: DateTime<Utc>) {
        let mut recompute_max = false;
        while self.events.front().is_some_and(|event| event.ts < cutoff) {
            let Some(event) = self.events.pop_front() else {
                break;
            };
            self.volume_sol = (self.volume_sol - event.sol_notional).max(0.0);
            if let Some(count) = self.wallet_counts.get_mut(&event.wallet_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.wallet_counts.remove(&event.wallet_id);
                }
            }
            if (self.liquidity_sol_proxy - event.sol_notional).abs() <= 1e-12 {
                recompute_max = true;
            }
        }
        if recompute_max {
            self.liquidity_sol_proxy = self
                .events
                .iter()
                .map(|event| event.sol_notional)
                .fold(0.0, f64::max);
        }
    }

    fn push(&mut self, swap: &SwapEvent, sol_notional: f64) {
        self.events.push_back(MarketWindowEvent {
            ts: swap.ts_utc,
            wallet_id: swap.wallet.clone(),
            sol_notional,
        });
        self.volume_sol += sol_notional;
        self.liquidity_sol_proxy = self.liquidity_sol_proxy.max(sol_notional);
        *self.wallet_counts.entry(swap.wallet.clone()).or_insert(0) += 1;
    }

    fn snapshot(&self) -> TokenMarketSnapshot {
        TokenMarketSnapshot {
            volume_5m_sol: self.volume_sol.max(0.0),
            unique_traders_5m: self.wallet_counts.len() as u32,
            liquidity_sol_proxy: self.liquidity_sol_proxy.max(0.0),
        }
    }
}
