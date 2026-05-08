use crate::discovery_quality_types::QualityCacheUpsert;
use crate::{DiscoveryRuntimeCursor, DiscoveryScoringBoundarySeedLot, WalletScoringQualitySource};
use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use copybot_core_types::SwapEvent;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

pub(super) const QUALITY_CACHE_TTL_SECONDS: i64 = 10 * 60;
pub(super) const QUALITY_RPC_TIMEOUT_MS: u64 = 700;
pub(super) const QUALITY_MAX_SIGNATURE_PAGES: u32 = 1;
pub(super) const QUALITY_MAX_FETCH_PER_BATCH: usize = 20;
pub(super) const QUALITY_RPC_BUDGET_MS: u64 = 1_500;
pub(super) const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub(super) const BOUNDARY_LOT_BUILDER_STATE_KEY: &str = "boundary_lot_builder_state_json";

#[derive(Debug, Clone)]
pub(super) struct BuilderLot {
    pub(super) buy_signature: String,
    pub(super) wallet_id: String,
    pub(super) token: String,
    pub(super) qty: f64,
    pub(super) cost_sol: f64,
    pub(super) opened_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(super) struct MarketWindowEvent {
    pub(super) ts: DateTime<Utc>,
    pub(super) wallet_id: String,
    pub(super) sol_notional: f64,
}

#[derive(Debug, Default)]
pub(super) struct RollingTokenMarketState {
    pub(super) events: VecDeque<MarketWindowEvent>,
    pub(super) wallet_counts: HashMap<String, usize>,
    pub(super) volume_sol: f64,
    pub(super) liquidity_sol_proxy: f64,
}

#[derive(Debug, Clone)]
pub(super) struct BuilderQualitySnapshot {
    pub(super) source: WalletScoringQualitySource,
    pub(super) token_age_seconds: Option<u64>,
    pub(super) holders: Option<u64>,
    pub(super) liquidity_sol: Option<f64>,
}

#[derive(Debug, Clone)]
pub(super) struct BuilderQualityCacheState {
    pub(super) holders: Option<u64>,
    pub(super) liquidity_sol: Option<f64>,
    pub(super) token_age_seconds: Option<u64>,
    pub(super) fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(super) struct PendingBuyFactRow {
    pub(super) buy_signature: String,
    pub(super) wallet_id: String,
    pub(super) token: String,
    pub(super) ts: DateTime<Utc>,
    pub(super) activity_day: NaiveDate,
    pub(super) notional_sol: f64,
    pub(super) market_volume_5m_sol: f64,
    pub(super) market_unique_traders_5m: u32,
    pub(super) market_liquidity_proxy_sol: f64,
    pub(super) quality_source: WalletScoringQualitySource,
    pub(super) quality_token_age_seconds: Option<u64>,
    pub(super) quality_holders: Option<u64>,
    pub(super) quality_liquidity_sol: Option<f64>,
    pub(super) rug_check_after_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(super) struct PendingCloseFactRow {
    pub(super) sell_signature: String,
    pub(super) segment_index: i64,
    pub(super) wallet_id: String,
    pub(super) token: String,
    pub(super) closed_ts: DateTime<Utc>,
    pub(super) activity_day: NaiveDate,
    pub(super) pnl_sol: f64,
    pub(super) hold_seconds: i64,
    pub(super) win: bool,
}

#[derive(Debug, Default)]
pub(super) struct PreparedDiscoveryScoringBuilderBatch {
    pub(super) day_deltas: HashMap<(String, NaiveDate), DayDelta>,
    pub(super) tx_minute_deltas: HashMap<(String, i64), u32>,
    pub(super) buy_rows: Vec<PendingBuyFactRow>,
    pub(super) close_rows: Vec<PendingCloseFactRow>,
    pub(super) lot_mutations: HashMap<String, Option<BuilderLot>>,
    pub(super) quality_upserts: HashMap<String, QualityCacheUpsert>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct DayDelta {
    pub(super) first_seen: Option<DateTime<Utc>>,
    pub(super) last_seen: Option<DateTime<Utc>>,
    pub(super) trades: u32,
    pub(super) spent_sol: f64,
    pub(super) max_buy_notional_sol: f64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct TokenMarketSnapshot {
    pub(super) volume_5m_sol: f64,
    pub(super) unique_traders_5m: u32,
    pub(super) liquidity_sol_proxy: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct WalletTokenKey {
    pub(super) wallet_id: String,
    pub(super) token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CursorRef<'a> {
    pub(super) ts: DateTime<Utc>,
    pub(super) slot: u64,
    pub(super) signature: &'a str,
}

#[derive(Debug)]
pub struct DiscoveryScoringReplayBuilder {
    pub(super) open_lots: HashMap<WalletTokenKey, VecDeque<BuilderLot>>,
    pub(super) loaded_open_lot_keys: HashSet<WalletTokenKey>,
    pub(super) lazy_open_lot_loading: bool,
    pub(super) market_windows: HashMap<String, RollingTokenMarketState>,
    pub(super) quality_cache: HashMap<String, Option<BuilderQualityCacheState>>,
}

#[derive(Debug)]
pub struct DiscoveryScoringBoundaryLotBuilder {
    pub(super) open_lots: HashMap<WalletTokenKey, VecDeque<BuilderLot>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PersistedBoundaryLotBuilderState {
    pub(super) progress_start_ts: DateTime<Utc>,
    pub(super) cursor: DiscoveryRuntimeCursor,
    pub(super) open_lots: Vec<DiscoveryScoringBoundarySeedLot>,
}

#[derive(Debug, Clone)]
pub(super) enum LotMutationAction {
    Upsert(BuilderLot),
    Delete(String),
}

#[derive(Debug, Default)]
pub(super) struct LotAccountingStep {
    pub(super) close_rows: Vec<PendingCloseFactRow>,
    pub(super) mutations: Vec<LotMutationAction>,
}

pub(super) fn parse_ts(raw: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("invalid {field} rfc3339 value: {raw}"))
}

pub(super) fn cmp_swap_to_cursor(swap: &SwapEvent, cursor: CursorRef<'_>) -> std::cmp::Ordering {
    swap.ts_utc
        .cmp(&cursor.ts)
        .then_with(|| swap.slot.cmp(&cursor.slot))
        .then_with(|| swap.signature.as_str().cmp(cursor.signature))
}

pub(super) fn is_sol_buy(swap: &SwapEvent) -> bool {
    swap.token_in == SOL_MINT && swap.token_out != SOL_MINT
}

pub(super) fn is_sol_sell(swap: &SwapEvent) -> bool {
    swap.token_out == SOL_MINT && swap.token_in != SOL_MINT
}

pub(super) fn sol_pair_market_event(swap: &SwapEvent) -> Option<(&str, f64)> {
    if is_sol_buy(swap) {
        Some((swap.token_out.as_str(), swap.amount_in.max(0.0)))
    } else if is_sol_sell(swap) {
        Some((swap.token_in.as_str(), swap.amount_out.max(0.0)))
    } else {
        None
    }
}

impl RollingTokenMarketState {
    pub(super) fn evict_before(&mut self, cutoff: DateTime<Utc>) {
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

    pub(super) fn push(&mut self, swap: &SwapEvent, sol_notional: f64) {
        self.events.push_back(MarketWindowEvent {
            ts: swap.ts_utc,
            wallet_id: swap.wallet.clone(),
            sol_notional,
        });
        self.volume_sol += sol_notional;
        self.liquidity_sol_proxy = self.liquidity_sol_proxy.max(sol_notional);
        *self.wallet_counts.entry(swap.wallet.clone()).or_insert(0) += 1;
    }

    pub(super) fn snapshot(&self) -> TokenMarketSnapshot {
        TokenMarketSnapshot {
            volume_5m_sol: self.volume_sol.max(0.0),
            unique_traders_5m: self.wallet_counts.len() as u32,
            liquidity_sol_proxy: self.liquidity_sol_proxy.max(0.0),
        }
    }
}
