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

fn load_all_open_lots(conn: &Connection) -> Result<HashMap<WalletTokenKey, VecDeque<BuilderLot>>> {
    let mut stmt = conn
        .prepare(
            "SELECT buy_signature, wallet_id, token, qty, cost_sol, opened_ts
             FROM wallet_scoring_open_lots
             ORDER BY wallet_id ASC, token ASC, opened_ts ASC, buy_signature ASC",
        )
        .context("failed preparing wallet_scoring_open_lots builder bootstrap query")?;
    let mut rows = stmt
        .query([])
        .context("failed querying wallet_scoring_open_lots for builder bootstrap")?;
    let mut out: HashMap<WalletTokenKey, VecDeque<BuilderLot>> = HashMap::new();
    while let Some(row) = rows
        .next()
        .context("failed iterating wallet_scoring_open_lots for builder bootstrap")?
    {
        let wallet_id: String = row.get(1).context("failed reading builder lot wallet_id")?;
        let token: String = row.get(2).context("failed reading builder lot token")?;
        out.entry(WalletTokenKey {
            wallet_id: wallet_id.clone(),
            token: token.clone(),
        })
        .or_default()
        .push_back(BuilderLot {
            buy_signature: row
                .get(0)
                .context("failed reading builder lot buy_signature")?,
            wallet_id,
            token,
            qty: row.get(3).context("failed reading builder lot qty")?,
            cost_sol: row.get(4).context("failed reading builder lot cost_sol")?,
            opened_ts: parse_ts(
                &row.get::<_, String>(5)
                    .context("failed reading builder lot opened_ts")?,
                "wallet_scoring_open_lots.opened_ts",
            )?,
        });
    }
    Ok(out)
}

fn load_open_lots_for_wallet_token(
    conn: &Connection,
    wallet_id: &str,
    token: &str,
) -> Result<VecDeque<BuilderLot>> {
    let mut stmt = conn
        .prepare(
            "SELECT buy_signature, qty, cost_sol, opened_ts
             FROM wallet_scoring_open_lots
             WHERE wallet_id = ?1
               AND token = ?2
             ORDER BY opened_ts ASC, buy_signature ASC",
        )
        .context("failed preparing wallet_scoring_open_lots lazy builder query")?;
    let mut rows = stmt
        .query(params![wallet_id, token])
        .context("failed querying wallet_scoring_open_lots for lazy builder bootstrap")?;
    let mut out = VecDeque::new();
    while let Some(row) = rows
        .next()
        .context("failed iterating wallet_scoring_open_lots for lazy builder bootstrap")?
    {
        out.push_back(BuilderLot {
            buy_signature: row
                .get(0)
                .context("failed reading lazy builder lot buy_signature")?,
            wallet_id: wallet_id.to_string(),
            token: token.to_string(),
            qty: row.get(1).context("failed reading lazy builder lot qty")?,
            cost_sol: row
                .get(2)
                .context("failed reading lazy builder lot cost_sol")?,
            opened_ts: parse_ts(
                &row.get::<_, String>(3)
                    .context("failed reading lazy builder lot opened_ts")?,
                "wallet_scoring_open_lots.opened_ts",
            )?,
        });
    }
    Ok(out)
}

fn load_boundary_seed_lots_into_open_lots(
    seed_lots: &[DiscoveryScoringBoundarySeedLot],
) -> HashMap<WalletTokenKey, VecDeque<BuilderLot>> {
    let mut ordered = seed_lots.to_vec();
    ordered.sort_by(|left, right| {
        left.wallet_id
            .cmp(&right.wallet_id)
            .then_with(|| left.token.cmp(&right.token))
            .then_with(|| left.opened_ts.cmp(&right.opened_ts))
            .then_with(|| left.buy_signature.cmp(&right.buy_signature))
    });

    let mut out: HashMap<WalletTokenKey, VecDeque<BuilderLot>> = HashMap::new();
    for lot in ordered {
        out.entry(WalletTokenKey {
            wallet_id: lot.wallet_id.clone(),
            token: lot.token.clone(),
        })
        .or_default()
        .push_back(BuilderLot {
            buy_signature: lot.buy_signature,
            wallet_id: lot.wallet_id,
            token: lot.token,
            qty: lot.qty,
            cost_sol: lot.cost_sol,
            opened_ts: lot.opened_ts,
        });
    }
    out
}

fn load_persisted_boundary_lot_builder_state_on_conn(
    conn: &Connection,
) -> Result<Option<PersistedBoundaryLotBuilderState>> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT state_value
             FROM discovery_scoring_state
             WHERE state_key = ?1",
            params![BOUNDARY_LOT_BUILDER_STATE_KEY],
            |row| row.get(0),
        )
        .optional()
        .context("failed querying persisted boundary lot builder state")?;
    raw.map(|raw| {
        serde_json::from_str::<PersistedBoundaryLotBuilderState>(&raw)
            .context("failed decoding persisted boundary lot builder state json")
    })
    .transpose()
}

fn upsert_persisted_boundary_lot_builder_state_on_conn(
    conn: &Connection,
    progress_start_ts: DateTime<Utc>,
    progress_cursor: &DiscoveryRuntimeCursor,
    builder: &DiscoveryScoringBoundaryLotBuilder,
    updated_at: &str,
) -> Result<()> {
    let state = PersistedBoundaryLotBuilderState {
        progress_start_ts,
        cursor: progress_cursor.clone(),
        open_lots: export_boundary_seed_lots_from_open_lots(&builder.open_lots),
    };
    let state_json = serde_json::to_string(&state)
        .context("failed encoding persisted boundary lot builder state json")?;
    conn.execute(
        "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(state_key) DO UPDATE SET
            state_value = excluded.state_value,
            updated_at = excluded.updated_at",
        params![BOUNDARY_LOT_BUILDER_STATE_KEY, state_json, updated_at],
    )
    .context("failed upserting persisted boundary lot builder state")?;
    Ok(())
}

fn seed_market_windows_from_lookback(
    store: &SqliteStore,
    starting_cursor: CursorRef<'_>,
) -> Result<HashMap<String, RollingTokenMarketState>> {
    let lookback_start = starting_cursor.ts - Duration::minutes(5);
    let mut out = HashMap::<String, RollingTokenMarketState>::new();
    store.for_each_observed_swap_in_window(lookback_start, starting_cursor.ts, |swap| {
        if cmp_swap_to_cursor(&swap, starting_cursor) == std::cmp::Ordering::Greater {
            return Ok(());
        }
        let Some((token, sol_notional)) = sol_pair_market_event(&swap) else {
            return Ok(());
        };
        let state = out.entry(token.to_string()).or_default();
        state.evict_before(swap.ts_utc - Duration::minutes(5));
        state.push(&swap, sol_notional);
        Ok(())
    })?;
    Ok(out)
}

fn load_quality_cache_state(
    store: &SqliteStore,
    mint: &str,
) -> Result<Option<BuilderQualityCacheState>> {
    Ok(store
        .get_token_quality_cache(mint)?
        .map(|row: TokenQualityCacheRow| BuilderQualityCacheState {
            holders: row.holders,
            liquidity_sol: row.liquidity_sol,
            token_age_seconds: row.token_age_seconds,
            fetched_at: row.fetched_at,
        }))
}

fn resolve_quality_snapshot(
    store: &SqliteStore,
    cache: &mut HashMap<String, Option<BuilderQualityCacheState>>,
    signal_ts: DateTime<Utc>,
    mint: &str,
    config: &DiscoveryAggregateWriteConfig,
    budget: &mut QualityFetchBudget,
) -> Result<(BuilderQualitySnapshot, Option<QualityCacheUpsert>)> {
    let cached = if let Some(cached) = cache.get(mint) {
        cached.clone()
    } else {
        let loaded = load_quality_cache_state(store, mint)?;
        cache.insert(mint.to_string(), loaded.clone());
        loaded
    };
    let ttl = Duration::seconds(QUALITY_CACHE_TTL_SECONDS);
    if let Some(row) = cached.as_ref() {
        if signal_ts.signed_duration_since(row.fetched_at) <= ttl {
            return Ok((
                BuilderQualitySnapshot {
                    source: WalletScoringQualitySource::Fresh,
                    token_age_seconds: row.token_age_seconds,
                    holders: row.holders,
                    liquidity_sol: row.liquidity_sol,
                },
                None,
            ));
        }
    }

    let stale_snapshot = |row: &BuilderQualityCacheState| BuilderQualitySnapshot {
        source: WalletScoringQualitySource::Stale,
        token_age_seconds: row.token_age_seconds.map(|age| {
            age.saturating_add(
                signal_ts
                    .signed_duration_since(row.fetched_at)
                    .num_seconds()
                    .max(0) as u64,
            )
        }),
        holders: row.holders,
        liquidity_sol: row.liquidity_sol,
    };

    let deferred_or_missing = || match cached.as_ref() {
        Some(row) => stale_snapshot(row),
        None => BuilderQualitySnapshot {
            source: if config.helius_http_url.is_some() {
                WalletScoringQualitySource::Deferred
            } else {
                WalletScoringQualitySource::Missing
            },
            token_age_seconds: None,
            holders: None,
            liquidity_sol: None,
        },
    };

    let Some(helius_http_url) = config.helius_http_url.as_deref() else {
        return Ok((deferred_or_missing(), None));
    };
    if budget.rpc_attempted >= QUALITY_MAX_FETCH_PER_BATCH {
        return Ok((deferred_or_missing(), None));
    }
    let started_at = budget.started_at.get_or_insert_with(Instant::now);
    if started_at.elapsed().as_millis() as u64 >= QUALITY_RPC_BUDGET_MS {
        return Ok((deferred_or_missing(), None));
    }

    budget.rpc_attempted = budget.rpc_attempted.saturating_add(1);
    match SqliteStore::fetch_token_quality_from_helius(
        helius_http_url,
        mint,
        QUALITY_RPC_TIMEOUT_MS,
        QUALITY_MAX_SIGNATURE_PAGES,
        config.min_token_age_hint_seconds,
    ) {
        Ok(fetched) => {
            let updated = BuilderQualityCacheState {
                holders: fetched.holders,
                liquidity_sol: fetched.liquidity_sol,
                token_age_seconds: fetched.token_age_seconds,
                fetched_at: signal_ts,
            };
            cache.insert(mint.to_string(), Some(updated));
            Ok((
                BuilderQualitySnapshot {
                    source: WalletScoringQualitySource::Fresh,
                    token_age_seconds: fetched.token_age_seconds,
                    holders: fetched.holders,
                    liquidity_sol: fetched.liquidity_sol,
                },
                Some(QualityCacheUpsert {
                    mint: mint.to_string(),
                    holders: fetched.holders,
                    liquidity_sol: fetched.liquidity_sol,
                    token_age_seconds: fetched.token_age_seconds,
                    fetched_at: signal_ts,
                }),
            ))
        }
        Err(_) => Ok((deferred_or_missing(), None)),
    }
}

fn upsert_wallet_scoring_day_delta_on_conn(
    conn: &Connection,
    wallet_id: &str,
    activity_day: NaiveDate,
    delta: &DayDelta,
) -> Result<()> {
    if delta.trades == 0 {
        return Ok(());
    }
    conn.execute(
        "INSERT INTO wallet_scoring_days(
            wallet_id,
            activity_day,
            first_seen,
            last_seen,
            trades,
            spent_sol,
            max_buy_notional_sol
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(wallet_id, activity_day) DO UPDATE SET
            first_seen = CASE
                WHEN excluded.first_seen < wallet_scoring_days.first_seen
                    THEN excluded.first_seen
                ELSE wallet_scoring_days.first_seen
            END,
            last_seen = CASE
                WHEN excluded.last_seen > wallet_scoring_days.last_seen
                    THEN excluded.last_seen
                ELSE wallet_scoring_days.last_seen
            END,
            trades = wallet_scoring_days.trades + excluded.trades,
            spent_sol = wallet_scoring_days.spent_sol + excluded.spent_sol,
            max_buy_notional_sol = MAX(
                wallet_scoring_days.max_buy_notional_sol,
                excluded.max_buy_notional_sol
            )",
        params![
            wallet_id,
            activity_day.format("%Y-%m-%d").to_string(),
            delta.first_seen.unwrap_or_else(Utc::now).to_rfc3339(),
            delta.last_seen.unwrap_or_else(Utc::now).to_rfc3339(),
            delta.trades as i64,
            delta.spent_sol,
            delta.max_buy_notional_sol,
        ],
    )
    .context("failed builder upserting wallet_scoring_days row")?;
    Ok(())
}

fn upsert_wallet_scoring_tx_minute_delta_on_conn(
    conn: &Connection,
    wallet_id: &str,
    minute_bucket: i64,
    tx_count: u32,
) -> Result<()> {
    if tx_count == 0 {
        return Ok(());
    }
    conn.execute(
        "INSERT INTO wallet_scoring_tx_minutes(wallet_id, minute_bucket, tx_count)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(wallet_id, minute_bucket) DO UPDATE SET
            tx_count = wallet_scoring_tx_minutes.tx_count + excluded.tx_count",
        params![wallet_id, minute_bucket, tx_count as i64],
    )
    .context("failed builder upserting wallet_scoring_tx_minutes row")?;
    Ok(())
}

fn upsert_token_quality_cache_on_conn(conn: &Connection, row: &QualityCacheUpsert) -> Result<()> {
    conn.execute(
        "INSERT INTO token_quality_cache(
            mint,
            holders,
            liquidity_sol,
            token_age_seconds,
            fetched_at
         ) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(mint) DO UPDATE SET
            holders = excluded.holders,
            liquidity_sol = excluded.liquidity_sol,
            token_age_seconds = excluded.token_age_seconds,
            fetched_at = excluded.fetched_at",
        params![
            &row.mint,
            row.holders.map(|value| value as i64),
            row.liquidity_sol,
            row.token_age_seconds.map(|value| value as i64),
            row.fetched_at.to_rfc3339(),
        ],
    )
    .context("failed builder upserting token_quality_cache row")?;
    Ok(())
}

fn export_boundary_seed_lots_from_open_lots(
    open_lots: &HashMap<WalletTokenKey, VecDeque<BuilderLot>>,
) -> Vec<DiscoveryScoringBoundarySeedLot> {
    let mut lots = open_lots
        .values()
        .flat_map(|wallet_token_lots| wallet_token_lots.iter())
        .map(|lot| DiscoveryScoringBoundarySeedLot {
            buy_signature: lot.buy_signature.clone(),
            wallet_id: lot.wallet_id.clone(),
            token: lot.token.clone(),
            qty: lot.qty,
            cost_sol: lot.cost_sol,
            opened_ts: lot.opened_ts,
        })
        .collect::<Vec<_>>();
    lots.sort_by(|left, right| {
        left.wallet_id
            .cmp(&right.wallet_id)
            .then_with(|| left.token.cmp(&right.token))
            .then_with(|| left.opened_ts.cmp(&right.opened_ts))
            .then_with(|| left.buy_signature.cmp(&right.buy_signature))
    });
    lots
}

fn apply_swap_lot_accounting(
    open_lots: &mut HashMap<WalletTokenKey, VecDeque<BuilderLot>>,
    swap: &SwapEvent,
) -> LotAccountingStep {
    let mut step = LotAccountingStep::default();
    if is_sol_buy(swap) {
        let token = swap.token_out.clone();
        let lot = BuilderLot {
            buy_signature: swap.signature.clone(),
            wallet_id: swap.wallet.clone(),
            token: token.clone(),
            qty: swap.amount_out.max(0.0),
            cost_sol: swap.amount_in.max(0.0),
            opened_ts: swap.ts_utc,
        };
        open_lots
            .entry(WalletTokenKey {
                wallet_id: swap.wallet.clone(),
                token,
            })
            .or_default()
            .push_back(lot.clone());
        step.mutations.push(LotMutationAction::Upsert(lot));
        return step;
    }

    if !is_sol_sell(swap) {
        return step;
    }

    let key = WalletTokenKey {
        wallet_id: swap.wallet.clone(),
        token: swap.token_in.clone(),
    };
    let lots = open_lots.entry(key.clone()).or_default();
    let mut qty_remaining = swap.amount_in.max(0.0);
    if qty_remaining <= 1e-12 || swap.amount_out <= 0.0 {
        if lots.is_empty() {
            open_lots.remove(&key);
        }
        return step;
    }
    let mut segment_index = 0i64;
    while qty_remaining > 1e-12 {
        let Some(mut lot) = lots.pop_front() else {
            break;
        };
        if lot.qty <= 1e-12 {
            step.mutations
                .push(LotMutationAction::Delete(lot.buy_signature.clone()));
            continue;
        }
        let take_qty = qty_remaining.min(lot.qty);
        let lot_fraction = take_qty / lot.qty;
        let cost_part = lot.cost_sol * lot_fraction;
        let proceeds_part = swap.amount_out * (take_qty / swap.amount_in.max(1e-12));
        let pnl_sol = proceeds_part - cost_part;
        let remaining_qty = (lot.qty - take_qty).max(0.0);
        let remaining_cost = (lot.cost_sol - cost_part).max(0.0);
        step.close_rows.push(PendingCloseFactRow {
            sell_signature: swap.signature.clone(),
            segment_index,
            wallet_id: swap.wallet.clone(),
            token: swap.token_in.clone(),
            closed_ts: swap.ts_utc,
            activity_day: swap.ts_utc.date_naive(),
            pnl_sol,
            hold_seconds: (swap.ts_utc - lot.opened_ts).num_seconds().max(0),
            win: pnl_sol > 0.0,
        });
        if remaining_qty <= 1e-12 {
            step.mutations
                .push(LotMutationAction::Delete(lot.buy_signature.clone()));
        } else {
            lot.qty = remaining_qty;
            lot.cost_sol = remaining_cost;
            step.mutations.push(LotMutationAction::Upsert(lot.clone()));
            lots.push_front(lot);
        }
        qty_remaining -= take_qty;
        segment_index += 1;
    }
    if lots.is_empty() {
        open_lots.remove(&key);
    }

    step
}

fn ensure_builder_open_lots_loaded(
    store: &SqliteStore,
    builder: &mut DiscoveryScoringReplayBuilder,
    wallet_id: &str,
    token: &str,
) -> Result<()> {
    if !builder.lazy_open_lot_loading {
        return Ok(());
    }
    let key = WalletTokenKey {
        wallet_id: wallet_id.to_string(),
        token: token.to_string(),
    };
    if !builder.loaded_open_lot_keys.insert(key.clone()) {
        return Ok(());
    }
    let loaded = load_open_lots_for_wallet_token(&store.conn, wallet_id, token)?;
    if !loaded.is_empty() {
        builder.open_lots.insert(key, loaded);
    }
    Ok(())
}

fn prepare_discovery_scoring_builder_batch(
    store: &SqliteStore,
    builder: &mut DiscoveryScoringReplayBuilder,
    swaps: &[SwapEvent],
    config: &DiscoveryAggregateWriteConfig,
) -> Result<(PreparedDiscoveryScoringBuilderBatch, u64)> {
    if swaps.is_empty() {
        return Ok((PreparedDiscoveryScoringBuilderBatch::default(), 0));
    }

    let prepare_started_at = Instant::now();
    let mut ordered = swaps.to_vec();
    ordered.sort_by(|left, right| {
        left.ts_utc
            .cmp(&right.ts_utc)
            .then_with(|| left.slot.cmp(&right.slot))
            .then_with(|| left.signature.cmp(&right.signature))
    });

    let mut prepared = PreparedDiscoveryScoringBuilderBatch::default();
    let mut quality_budget = QualityFetchBudget::default();

    let mut offset = 0usize;
    while offset < ordered.len() {
        let ts = ordered[offset].ts_utc;
        let group_end = ordered[offset..]
            .iter()
            .take_while(|swap| swap.ts_utc == ts)
            .count()
            + offset;

        for state in builder.market_windows.values_mut() {
            state.evict_before(ts - Duration::minutes(5));
        }

        let mut group_market_stats = HashMap::<String, TokenMarketSnapshot>::new();
        for swap in &ordered[offset..group_end] {
            let day_key = (swap.wallet.clone(), swap.ts_utc.date_naive());
            let delta = prepared.day_deltas.entry(day_key).or_default();
            delta.first_seen = Some(
                delta
                    .first_seen
                    .map(|current| current.min(swap.ts_utc))
                    .unwrap_or(swap.ts_utc),
            );
            delta.last_seen = Some(
                delta
                    .last_seen
                    .map(|current| current.max(swap.ts_utc))
                    .unwrap_or(swap.ts_utc),
            );
            delta.trades = delta.trades.saturating_add(1);
            if is_sol_buy(swap) {
                delta.spent_sol += swap.amount_in.max(0.0);
                delta.max_buy_notional_sol =
                    delta.max_buy_notional_sol.max(swap.amount_in.max(0.0));
            }
            *prepared
                .tx_minute_deltas
                .entry((swap.wallet.clone(), swap.ts_utc.timestamp().div_euclid(60)))
                .or_insert(0) += 1;

            if let Some((token, sol_notional)) = sol_pair_market_event(swap) {
                let state = builder.market_windows.entry(token.to_string()).or_default();
                state.push(swap, sol_notional);
                group_market_stats.insert(token.to_string(), state.snapshot());
            }
        }

        for swap in &ordered[offset..group_end] {
            if is_sol_buy(swap) {
                let token = swap.token_out.clone();
                ensure_builder_open_lots_loaded(store, builder, &swap.wallet, &token)?;
                let market_stats =
                    group_market_stats
                        .get(&token)
                        .copied()
                        .unwrap_or(TokenMarketSnapshot {
                            volume_5m_sol: 0.0,
                            unique_traders_5m: 0,
                            liquidity_sol_proxy: 0.0,
                        });
                let (quality, quality_upsert) = resolve_quality_snapshot(
                    store,
                    &mut builder.quality_cache,
                    swap.ts_utc,
                    &token,
                    config,
                    &mut quality_budget,
                )?;
                if let Some(quality_upsert) = quality_upsert {
                    prepared
                        .quality_upserts
                        .insert(quality_upsert.mint.clone(), quality_upsert);
                }
                prepared.buy_rows.push(PendingBuyFactRow {
                    buy_signature: swap.signature.clone(),
                    wallet_id: swap.wallet.clone(),
                    token: token.clone(),
                    ts: swap.ts_utc,
                    activity_day: swap.ts_utc.date_naive(),
                    notional_sol: swap.amount_in.max(0.0),
                    market_volume_5m_sol: market_stats.volume_5m_sol,
                    market_unique_traders_5m: market_stats.unique_traders_5m,
                    market_liquidity_proxy_sol: market_stats.liquidity_sol_proxy,
                    quality_source: quality.source,
                    quality_token_age_seconds: quality.token_age_seconds,
                    quality_holders: quality.holders,
                    quality_liquidity_sol: quality.liquidity_sol,
                    rug_check_after_ts: swap.ts_utc
                        + Duration::seconds(config.rug_lookahead_seconds.max(1) as i64),
                });
                for mutation in apply_swap_lot_accounting(&mut builder.open_lots, swap).mutations {
                    match mutation {
                        LotMutationAction::Upsert(lot) => {
                            prepared
                                .lot_mutations
                                .insert(lot.buy_signature.clone(), Some(lot));
                        }
                        LotMutationAction::Delete(buy_signature) => {
                            prepared.lot_mutations.insert(buy_signature, None);
                        }
                    }
                }
            } else if is_sol_sell(swap) {
                ensure_builder_open_lots_loaded(store, builder, &swap.wallet, &swap.token_in)?;
                let accounting_step = apply_swap_lot_accounting(&mut builder.open_lots, swap);
                prepared.close_rows.extend(accounting_step.close_rows);
                for mutation in accounting_step.mutations {
                    match mutation {
                        LotMutationAction::Upsert(lot) => {
                            prepared
                                .lot_mutations
                                .insert(lot.buy_signature.clone(), Some(lot));
                        }
                        LotMutationAction::Delete(buy_signature) => {
                            prepared.lot_mutations.insert(buy_signature, None);
                        }
                    }
                }
            }
        }

        offset = group_end;
    }

    Ok((prepared, prepare_started_at.elapsed().as_millis() as u64))
}

fn prepare_discovery_scoring_boundary_lot_batch(
    builder: &mut DiscoveryScoringBoundaryLotBuilder,
    swaps: &[SwapEvent],
) -> Result<u64> {
    if swaps.is_empty() {
        return Ok(0);
    }

    let prepare_started_at = Instant::now();
    let mut ordered = swaps.to_vec();
    ordered.sort_by(|left, right| {
        left.ts_utc
            .cmp(&right.ts_utc)
            .then_with(|| left.slot.cmp(&right.slot))
            .then_with(|| left.signature.cmp(&right.signature))
    });

    for swap in &ordered {
        let _ = apply_swap_lot_accounting(&mut builder.open_lots, swap);
    }

    Ok(prepare_started_at.elapsed().as_millis() as u64)
}

fn flush_prepared_discovery_scoring_builder_batch_on_conn(
    conn: &Connection,
    prepared: &PreparedDiscoveryScoringBuilderBatch,
) -> Result<()> {
    for row in prepared.quality_upserts.values() {
        upsert_token_quality_cache_on_conn(conn, row)?;
    }
    for ((wallet_id, activity_day), delta) in &prepared.day_deltas {
        upsert_wallet_scoring_day_delta_on_conn(conn, wallet_id, *activity_day, delta)?;
    }
    for ((wallet_id, minute_bucket), tx_count) in &prepared.tx_minute_deltas {
        upsert_wallet_scoring_tx_minute_delta_on_conn(conn, wallet_id, *minute_bucket, *tx_count)?;
    }
    for row in &prepared.buy_rows {
        conn.execute(
            "INSERT OR IGNORE INTO wallet_scoring_buy_facts(
                buy_signature,
                wallet_id,
                token,
                ts,
                activity_day,
                notional_sol,
                market_volume_5m_sol,
                market_unique_traders_5m,
                market_liquidity_proxy_sol,
                quality_source,
                quality_token_age_seconds,
                quality_holders,
                quality_liquidity_sol,
                rug_check_after_ts,
                rug_volume_lookahead_sol,
                rug_unique_traders_lookahead
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, NULL, NULL)",
            params![
                &row.buy_signature,
                &row.wallet_id,
                &row.token,
                row.ts.to_rfc3339(),
                row.activity_day.format("%Y-%m-%d").to_string(),
                row.notional_sol,
                row.market_volume_5m_sol,
                row.market_unique_traders_5m as i64,
                row.market_liquidity_proxy_sol,
                match row.quality_source {
                    WalletScoringQualitySource::Fresh => "fresh",
                    WalletScoringQualitySource::Stale => "stale",
                    WalletScoringQualitySource::Deferred => "deferred",
                    WalletScoringQualitySource::Missing => "missing",
                },
                row.quality_token_age_seconds.map(|value| value as i64),
                row.quality_holders.map(|value| value as i64),
                row.quality_liquidity_sol,
                row.rug_check_after_ts.to_rfc3339(),
            ],
        )
        .context("failed builder inserting wallet_scoring_buy_facts row")?;
    }
    for row in &prepared.close_rows {
        conn.execute(
            "INSERT OR IGNORE INTO wallet_scoring_close_facts(
                sell_signature,
                segment_index,
                wallet_id,
                token,
                closed_ts,
                activity_day,
                pnl_sol,
                hold_seconds,
                win
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                &row.sell_signature,
                row.segment_index,
                &row.wallet_id,
                &row.token,
                row.closed_ts.to_rfc3339(),
                row.activity_day.format("%Y-%m-%d").to_string(),
                row.pnl_sol,
                row.hold_seconds,
                if row.win { 1 } else { 0 },
            ],
        )
        .context("failed builder inserting wallet_scoring_close_facts row")?;
    }
    for (buy_signature, lot) in &prepared.lot_mutations {
        match lot {
            Some(lot) => {
                conn.execute(
                    "INSERT INTO wallet_scoring_open_lots(
                        buy_signature,
                        wallet_id,
                        token,
                        qty,
                        cost_sol,
                        opened_ts
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(buy_signature) DO UPDATE SET
                        qty = excluded.qty,
                        cost_sol = excluded.cost_sol,
                        opened_ts = excluded.opened_ts",
                    params![
                        buy_signature,
                        &lot.wallet_id,
                        &lot.token,
                        lot.qty,
                        lot.cost_sol,
                        lot.opened_ts.to_rfc3339(),
                    ],
                )
                .context("failed builder upserting wallet_scoring_open_lot")?;
            }
            None => {
                conn.execute(
                    "DELETE FROM wallet_scoring_open_lots WHERE buy_signature = ?1",
                    params![buy_signature],
                )
                .context("failed builder deleting wallet_scoring_open_lot")?;
            }
        }
    }
    Ok(())
}

impl SqliteStore {
    pub fn begin_discovery_scoring_replay_builder(
        &self,
        starting_cursor_ts: DateTime<Utc>,
        starting_cursor_slot: u64,
        starting_cursor_signature: &str,
    ) -> Result<DiscoveryScoringReplayBuilder> {
        self.begin_discovery_scoring_replay_builder_with_lot_bootstrap(
            starting_cursor_ts,
            starting_cursor_slot,
            starting_cursor_signature,
            false,
        )
    }

    pub fn begin_discovery_scoring_replay_builder_lazy_open_lots(
        &self,
        starting_cursor_ts: DateTime<Utc>,
        starting_cursor_slot: u64,
        starting_cursor_signature: &str,
    ) -> Result<DiscoveryScoringReplayBuilder> {
        self.begin_discovery_scoring_replay_builder_with_lot_bootstrap(
            starting_cursor_ts,
            starting_cursor_slot,
            starting_cursor_signature,
            true,
        )
    }

    fn begin_discovery_scoring_replay_builder_with_lot_bootstrap(
        &self,
        starting_cursor_ts: DateTime<Utc>,
        starting_cursor_slot: u64,
        starting_cursor_signature: &str,
        lazy_open_lot_loading: bool,
    ) -> Result<DiscoveryScoringReplayBuilder> {
        let carryover_count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM wallet_scoring_carryover_lots",
                [],
                |row| row.get(0),
            )
            .context("failed counting wallet_scoring_carryover_lots for builder init")?;
        if carryover_count != 0 {
            anyhow::bail!(
                "builder replay does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_count}"
            );
        }
        Ok(DiscoveryScoringReplayBuilder {
            open_lots: if lazy_open_lot_loading {
                HashMap::new()
            } else {
                load_all_open_lots(&self.conn)?
            },
            loaded_open_lot_keys: HashSet::new(),
            lazy_open_lot_loading,
            market_windows: seed_market_windows_from_lookback(
                self,
                CursorRef {
                    ts: starting_cursor_ts,
                    slot: starting_cursor_slot,
                    signature: starting_cursor_signature,
                },
            )?,
            quality_cache: HashMap::new(),
        })
    }

    pub fn export_discovery_scoring_builder_boundary_seed_snapshot(
        &self,
        builder: &DiscoveryScoringReplayBuilder,
        boundary_start_ts: DateTime<Utc>,
        boundary_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<DiscoveryScoringBoundarySeedSnapshot> {
        let carryover_count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM wallet_scoring_carryover_lots",
                [],
                |row| row.get(0),
            )
            .context("failed counting wallet_scoring_carryover_lots for builder boundary export")?;
        if carryover_count != 0 {
            anyhow::bail!(
                "builder boundary export does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_count}"
            );
        }
        Ok(DiscoveryScoringBoundarySeedSnapshot {
            boundary_start_ts,
            boundary_cursor: boundary_cursor.clone(),
            open_lots: export_boundary_seed_lots_from_open_lots(&builder.open_lots),
        })
    }

    pub fn begin_discovery_scoring_boundary_lot_builder(
        &self,
        progress_start_ts: DateTime<Utc>,
        starting_cursor_ts: DateTime<Utc>,
        starting_cursor_slot: u64,
        starting_cursor_signature: &str,
    ) -> Result<DiscoveryScoringBoundaryLotBuilder> {
        let carryover_count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM wallet_scoring_carryover_lots",
                [],
                |row| row.get(0),
            )
            .context(
                "failed counting wallet_scoring_carryover_lots for boundary lot builder init",
            )?;
        if carryover_count != 0 {
            anyhow::bail!(
                "boundary lot builder does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_count}"
            );
        }
        if let Some(state) = load_persisted_boundary_lot_builder_state_on_conn(&self.conn)? {
            let expected_cursor = DiscoveryRuntimeCursor {
                ts_utc: starting_cursor_ts,
                slot: starting_cursor_slot,
                signature: starting_cursor_signature.to_string(),
            };
            if state.progress_start_ts == progress_start_ts && state.cursor == expected_cursor {
                return Ok(DiscoveryScoringBoundaryLotBuilder {
                    open_lots: load_boundary_seed_lots_into_open_lots(&state.open_lots),
                });
            }
        }
        Ok(DiscoveryScoringBoundaryLotBuilder {
            open_lots: load_all_open_lots(&self.conn)?,
        })
    }

    pub fn export_discovery_scoring_boundary_lot_seed_snapshot(
        &self,
        builder: &DiscoveryScoringBoundaryLotBuilder,
        boundary_start_ts: DateTime<Utc>,
        boundary_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<DiscoveryScoringBoundarySeedSnapshot> {
        let carryover_count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM wallet_scoring_carryover_lots",
                [],
                |row| row.get(0),
            )
            .context("failed counting wallet_scoring_carryover_lots for boundary lot export")?;
        if carryover_count != 0 {
            anyhow::bail!(
                "boundary lot export does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_count}"
            );
        }
        Ok(DiscoveryScoringBoundarySeedSnapshot {
            boundary_start_ts,
            boundary_cursor: boundary_cursor.clone(),
            open_lots: export_boundary_seed_lots_from_open_lots(&builder.open_lots),
        })
    }

    pub fn advance_discovery_scoring_boundary_lot_builder_in_memory_with_timings(
        &self,
        builder: &mut DiscoveryScoringBoundaryLotBuilder,
        swaps: &[SwapEvent],
    ) -> Result<DiscoveryScoringBatchStageTimings> {
        let prepare_ms = prepare_discovery_scoring_boundary_lot_batch(builder, swaps)?;
        Ok(DiscoveryScoringBatchStageTimings {
            prepare_ms,
            apply_ms: 0,
            rug_finalize_ms: 0,
        })
    }

    pub fn advance_discovery_scoring_boundary_lot_builder_and_checkpoint_with_timings(
        &self,
        builder: &mut DiscoveryScoringBoundaryLotBuilder,
        swaps: &[SwapEvent],
        progress_start_ts: DateTime<Utc>,
        progress_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<DiscoveryScoringCheckpointedBatchTimings> {
        let prepare_ms = prepare_discovery_scoring_boundary_lot_batch(builder, swaps)?;

        let progress_update_ms = self.with_immediate_transaction_retry(
            "discovery scoring boundary lot batch with checkpoint",
            |conn| {
                super::discovery_scoring::maybe_fail_after_materialization_before_checkpoint()?;

                let progress_started_at = Instant::now();
                let updated_at = Utc::now().to_rfc3339();
                super::discovery_scoring::upsert_discovery_scoring_backfill_progress_on_conn(
                    conn,
                    progress_start_ts,
                    progress_cursor,
                    &updated_at,
                )?;
                upsert_persisted_boundary_lot_builder_state_on_conn(
                    conn,
                    progress_start_ts,
                    progress_cursor,
                    builder,
                    &updated_at,
                )?;
                Ok(progress_started_at.elapsed().as_millis() as u64)
            },
        )?;

        Ok(DiscoveryScoringCheckpointedBatchTimings {
            prepare_ms,
            apply_ms: 0,
            progress_update_ms,
        })
    }

    pub fn advance_discovery_scoring_builder_batch_in_memory_with_timings(
        &self,
        builder: &mut DiscoveryScoringReplayBuilder,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
    ) -> Result<DiscoveryScoringBatchStageTimings> {
        let (_prepared, prepare_ms) =
            prepare_discovery_scoring_builder_batch(self, builder, swaps, config)?;
        Ok(DiscoveryScoringBatchStageTimings {
            prepare_ms,
            apply_ms: 0,
            rug_finalize_ms: 0,
        })
    }

    pub fn apply_discovery_scoring_builder_batch_with_timings(
        &self,
        builder: &mut DiscoveryScoringReplayBuilder,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
    ) -> Result<DiscoveryScoringBatchStageTimings> {
        let (prepared, prepare_ms) =
            prepare_discovery_scoring_builder_batch(self, builder, swaps, config)?;
        let apply_started_at = Instant::now();
        self.with_immediate_transaction_retry("discovery scoring builder batch", |conn| {
            flush_prepared_discovery_scoring_builder_batch_on_conn(conn, &prepared)?;
            Ok(())
        })?;
        let apply_ms = apply_started_at.elapsed().as_millis() as u64;

        Ok(DiscoveryScoringBatchStageTimings {
            prepare_ms,
            apply_ms,
            rug_finalize_ms: 0,
        })
    }

    pub fn apply_discovery_scoring_builder_batch_and_checkpoint_with_timings(
        &self,
        builder: &mut DiscoveryScoringReplayBuilder,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
        progress_start_ts: DateTime<Utc>,
        progress_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<DiscoveryScoringCheckpointedBatchTimings> {
        let (prepared, prepare_ms) =
            prepare_discovery_scoring_builder_batch(self, builder, swaps, config)?;

        let (apply_ms, progress_update_ms) = self.with_immediate_transaction_retry(
            "discovery scoring builder batch with checkpoint",
            |conn| {
                let apply_started_at = Instant::now();
                flush_prepared_discovery_scoring_builder_batch_on_conn(conn, &prepared)?;
                let apply_ms = apply_started_at.elapsed().as_millis() as u64;

                super::discovery_scoring::maybe_fail_after_materialization_before_checkpoint()?;

                let progress_started_at = Instant::now();
                let updated_at = Utc::now().to_rfc3339();
                super::discovery_scoring::upsert_discovery_scoring_backfill_progress_on_conn(
                    conn,
                    progress_start_ts,
                    progress_cursor,
                    &updated_at,
                )?;
                let progress_update_ms = progress_started_at.elapsed().as_millis() as u64;
                Ok((apply_ms, progress_update_ms))
            },
        )?;

        Ok(DiscoveryScoringCheckpointedBatchTimings {
            prepare_ms,
            apply_ms,
            progress_update_ms,
        })
    }
}
