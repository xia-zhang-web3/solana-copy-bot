use anyhow::Result;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_storage::{SqliteStore, TokenQualityCacheRow, WalletMetricRow};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::{info, warn};

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const QUALITY_CACHE_TTL_SECONDS: i64 = 10 * 60;
const QUALITY_RPC_TIMEOUT_MS: u64 = 2_500;
const QUALITY_MAX_SIGNATURE_PAGES: u32 = 4;
const QUALITY_MAX_FETCH_PER_CYCLE: usize = 200;

#[derive(Debug, Clone)]
pub struct DiscoveryService {
    config: DiscoveryConfig,
    shadow_quality: ShadowConfig,
    helius_http_url: Option<String>,
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
        }
    }

    pub fn run_cycle(&self, store: &SqliteStore, now: DateTime<Utc>) -> Result<DiscoverySummary> {
        let window_days = self.config.scoring_window_days.max(1);
        let window_start = now - Duration::days(window_days as i64);
        let swaps = store.load_observed_swaps_since(window_start)?;

        if swaps.is_empty() {
            return Ok(DiscoverySummary {
                window_start,
                ..DiscoverySummary::default()
            });
        }

        let snapshots = self.build_wallet_snapshots(store, &swaps, now);
        for snapshot in snapshots.iter() {
            let status = if snapshot.eligible {
                "candidate"
            } else {
                "observed"
            };
            store.upsert_wallet(
                &snapshot.wallet_id,
                snapshot.first_seen,
                snapshot.last_seen,
                status,
            )?;
            store.insert_wallet_metric(&WalletMetricRow {
                wallet_id: snapshot.wallet_id.clone(),
                window_start,
                pnl: snapshot.pnl_sol,
                win_rate: snapshot.win_rate,
                trades: snapshot.trades,
                closed_trades: snapshot.closed_trades,
                hold_median_seconds: snapshot.hold_median_seconds,
                score: snapshot.score,
                buy_total: snapshot.buy_total,
                tradable_ratio: snapshot.tradable_ratio,
                rug_ratio: snapshot.rug_ratio,
            })?;
        }

        let mut ranked: Vec<&WalletSnapshot> = snapshots
            .iter()
            .filter(|item| item.eligible && item.score >= self.config.min_score)
            .collect();
        ranked.sort_by(|a, b| cmp_score_then_trades(a, b));

        let desired_wallets: Vec<String> = ranked
            .iter()
            .take(self.config.follow_top_n as usize)
            .map(|item| item.wallet_id.clone())
            .collect();
        let follow_delta =
            store.reconcile_followlist(&desired_wallets, now, "discovery_score_refresh")?;
        let active_follow_wallets = store.list_active_follow_wallets()?.len();
        let top_wallets = ranked
            .iter()
            .take(5)
            .map(|item| {
                format!(
                    "{}:{:.3}:t{:.2}:r{:.2}:b{}",
                    item.wallet_id, item.score, item.tradable_ratio, item.rug_ratio, item.buy_total
                )
            })
            .collect::<Vec<_>>();

        let summary = DiscoverySummary {
            window_start,
            wallets_seen: snapshots.len(),
            eligible_wallets: ranked.len(),
            metrics_written: snapshots.len(),
            follow_promoted: follow_delta.activated,
            follow_demoted: follow_delta.deactivated,
            active_follow_wallets,
            top_wallets,
        };

        info!(
            window_start = %summary.window_start,
            wallets_seen = summary.wallets_seen,
            eligible_wallets = summary.eligible_wallets,
            metrics_written = summary.metrics_written,
            follow_promoted = summary.follow_promoted,
            follow_demoted = summary.follow_demoted,
            active_follow_wallets = summary.active_follow_wallets,
            top_wallets = ?summary.top_wallets,
            "discovery cycle completed"
        );

        Ok(summary)
    }

    fn build_wallet_snapshots(
        &self,
        store: &SqliteStore,
        swaps: &[SwapEvent],
        now: DateTime<Utc>,
    ) -> Vec<WalletSnapshot> {
        let mut sorted = swaps.to_vec();
        sorted.sort_by(|a, b| {
            a.ts_utc
                .cmp(&b.ts_utc)
                .then_with(|| a.slot.cmp(&b.slot))
                .then_with(|| a.signature.cmp(&b.signature))
        });

        let mut by_wallet: HashMap<String, WalletAccumulator> = HashMap::new();
        let unique_buy_mints: HashSet<String> = sorted
            .iter()
            .filter(|swap| is_sol_buy(swap))
            .map(|swap| swap.token_out.clone())
            .collect();
        let token_quality_cache =
            self.resolve_token_quality_for_mints(store, &unique_buy_mints, now);
        let mut token_states: HashMap<String, TokenRollingState> = HashMap::new();
        let mut token_sol_history: HashMap<String, Vec<SolLegTrade>> = HashMap::new();
        for swap in sorted.iter() {
            let buy_quality = self.update_token_quality_state(
                &mut token_states,
                &mut token_sol_history,
                &token_quality_cache,
                swap,
            );
            let entry = by_wallet.entry(swap.wallet.clone()).or_default();
            entry.observe_swap(swap, self.config.max_tx_per_minute, buy_quality);
        }

        by_wallet
            .into_iter()
            .map(|(wallet_id, acc)| {
                self.snapshot_from_accumulator(wallet_id, acc, now, &token_sol_history)
            })
            .collect()
    }

    fn snapshot_from_accumulator(
        &self,
        wallet_id: String,
        acc: WalletAccumulator,
        now: DateTime<Utc>,
        token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
    ) -> WalletSnapshot {
        let first_seen = acc.first_seen.unwrap_or(now);
        let last_seen = acc.last_seen.unwrap_or(now);
        let active_days = acc.active_days.len() as u32;
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
        let (rug_evaluated, rugged_buys) =
            self.compute_rug_metrics(&acc.buy_observations, token_sol_history, now);
        let rug_ratio = if rug_evaluated > 0 {
            rugged_buys as f64 / rug_evaluated as f64
        } else {
            0.0
        };
        let win_rate = if acc.closed_trades > 0 {
            acc.wins as f64 / acc.closed_trades as f64
        } else {
            0.0
        };
        let hold_median_seconds = median_i64(&acc.hold_samples_sec).unwrap_or(0);
        let consistency_ratio = if active_days > 0 {
            let positive_days = acc
                .realized_pnl_by_day
                .values()
                .filter(|value| **value > 0.0)
                .count() as f64;
            positive_days / active_days as f64
        } else {
            0.0
        };
        let roi = if acc.spent_sol > 1e-9 {
            acc.realized_pnl_sol / acc.spent_sol
        } else {
            0.0
        };
        let win_sample_factor = (acc.closed_trades as f64 / 8.0).min(1.0);
        let hold_quality = hold_time_quality_score(hold_median_seconds);
        let pnl_component = tanh01(acc.realized_pnl_sol / 2.0);
        let roi_component = tanh01(roi * 3.0);
        let win_component = (win_rate * win_sample_factor).clamp(0.0, 1.0);
        let consistency_component = consistency_ratio.clamp(0.0, 1.0);
        let penalty_component = if acc.suspicious { 0.0 } else { 1.0 };
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
        let eligible = acc.trades >= self.config.min_trades
            && active_days >= self.config.min_active_days
            && !acc.suspicious
            && acc.max_buy_notional_sol >= self.config.min_leader_notional_sol
            && last_seen >= decay_cutoff
            && buy_total >= self.config.min_buy_count
            && tradable_ratio >= self.config.min_tradable_ratio
            && rug_ratio <= self.config.max_rug_ratio;
        let score = if eligible { raw_score } else { 0.0 };

        WalletSnapshot {
            wallet_id,
            first_seen,
            last_seen,
            pnl_sol: acc.realized_pnl_sol,
            win_rate,
            trades: acc.trades,
            closed_trades: acc.closed_trades,
            hold_median_seconds,
            score,
            buy_total,
            tradable_ratio,
            rug_ratio,
            eligible,
        }
    }

    fn compute_rug_metrics(
        &self,
        buys: &[BuyObservation],
        token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
        now: DateTime<Utc>,
    ) -> (u32, u32) {
        if buys.is_empty() {
            return (0, 0);
        }
        let lookahead = Duration::seconds(self.config.rug_lookahead_seconds.max(1) as i64);
        let mut evaluated = 0u32;
        let mut rugged = 0u32;

        for buy in buys {
            let window_end = buy.ts + lookahead;
            if window_end > now {
                continue;
            }
            evaluated = evaluated.saturating_add(1);
            let Some(trades) = token_sol_history.get(&buy.token) else {
                rugged = rugged.saturating_add(1);
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
                rugged = rugged.saturating_add(1);
            }
        }

        (evaluated, rugged)
    }

    fn resolve_token_quality_for_mints(
        &self,
        store: &SqliteStore,
        mints: &HashSet<String>,
        now: DateTime<Utc>,
    ) -> HashMap<String, TokenQualityCacheRow> {
        if mints.is_empty() {
            return HashMap::new();
        }

        let mut out = HashMap::new();
        let ttl = Duration::seconds(QUALITY_CACHE_TTL_SECONDS);
        let mut to_fetch = Vec::new();
        let mut fresh_hits = 0usize;
        let mut stale_hits = 0usize;
        let mut misses = 0usize;

        for mint in mints {
            match store.get_token_quality_cache(mint) {
                Ok(Some(row)) => {
                    if now - row.fetched_at <= ttl {
                        fresh_hits += 1;
                        out.insert(mint.clone(), row);
                    } else {
                        stale_hits += 1;
                        to_fetch.push((mint.clone(), Some(row)));
                    }
                }
                Ok(None) => {
                    misses += 1;
                    to_fetch.push((mint.clone(), None))
                }
                Err(error) => {
                    warn!(error = %error, mint = %mint, "failed reading token quality cache");
                }
            }
        }

        let Some(helius_http_url) = self.helius_http_url.as_deref() else {
            for (mint, stale_row) in to_fetch {
                if let Some(row) = stale_row {
                    out.insert(mint, row);
                }
            }
            return out;
        };

        let mut fetched_ok = 0usize;
        let mut fetched_fail = 0usize;
        for (index, (mint, stale_row)) in to_fetch.into_iter().enumerate() {
            if index >= QUALITY_MAX_FETCH_PER_CYCLE {
                if let Some(row) = stale_row {
                    out.insert(mint, row);
                }
                continue;
            }

            match SqliteStore::fetch_token_quality_from_helius(
                helius_http_url,
                &mint,
                QUALITY_RPC_TIMEOUT_MS,
                QUALITY_MAX_SIGNATURE_PAGES,
                Some(self.shadow_quality.min_token_age_seconds),
            ) {
                Ok(fetched) => {
                    if let Err(error) = store.upsert_token_quality_cache(
                        &mint,
                        fetched.holders,
                        fetched.liquidity_sol,
                        fetched.token_age_seconds,
                        now,
                    ) {
                        warn!(error = %error, mint = %mint, "failed updating token quality cache");
                    }
                    match store.get_token_quality_cache(&mint) {
                        Ok(Some(row)) => {
                            fetched_ok += 1;
                            out.insert(mint, row);
                        }
                        Ok(None) => {}
                        Err(error) => {
                            warn!(
                                error = %error,
                                mint = %mint,
                                "failed reading token quality cache after refresh"
                            );
                        }
                    }
                }
                Err(error) => {
                    fetched_fail += 1;
                    warn!(
                        error = %error,
                        mint = %mint,
                        "failed to refresh token quality via helius, using fallback"
                    );
                    if let Some(row) = stale_row {
                        out.insert(mint, row);
                    }
                }
            }
        }

        info!(
            quality_source = "rpc+db_proxy",
            mints_total = mints.len(),
            cache_fresh = fresh_hits,
            cache_stale = stale_hits,
            cache_miss = misses,
            fetched_ok,
            fetched_fail,
            "discovery token quality cache summary"
        );

        out
    }

    fn update_token_quality_state(
        &self,
        token_states: &mut HashMap<String, TokenRollingState>,
        token_sol_history: &mut HashMap<String, Vec<SolLegTrade>>,
        token_quality_cache: &HashMap<String, TokenQualityCacheRow>,
        swap: &SwapEvent,
    ) -> Option<bool> {
        self.touch_token_state(token_states, &swap.token_in, &swap.wallet, swap.ts_utc);
        self.touch_token_state(token_states, &swap.token_out, &swap.wallet, swap.ts_utc);

        let (token, sol_notional) = if is_sol_buy(swap) {
            (swap.token_out.as_str(), swap.amount_in)
        } else if is_sol_sell(swap) {
            (swap.token_in.as_str(), swap.amount_out)
        } else {
            return None;
        };

        self.push_sol_leg_trade(
            token_states,
            token_sol_history,
            token,
            swap.wallet.as_str(),
            swap.ts_utc,
            sol_notional.max(0.0),
        );

        if !is_sol_buy(swap) {
            return None;
        }

        let state = token_states
            .get_mut(token)
            .expect("token state is initialized when push_sol_leg_trade is called");
        Self::evict_expired_5m(state, swap.ts_utc);
        Some(self.is_tradable_token(state, token_quality_cache.get(token), swap.ts_utc))
    }

    fn touch_token_state(
        &self,
        token_states: &mut HashMap<String, TokenRollingState>,
        token: &str,
        wallet_id: &str,
        ts: DateTime<Utc>,
    ) {
        let state = token_states.entry(token.to_string()).or_default();
        state.first_seen = Some(
            state
                .first_seen
                .map(|current| current.min(ts))
                .unwrap_or(ts),
        );
        state.wallets_seen.insert(wallet_id.to_string());
    }

    fn push_sol_leg_trade(
        &self,
        token_states: &mut HashMap<String, TokenRollingState>,
        token_sol_history: &mut HashMap<String, Vec<SolLegTrade>>,
        token: &str,
        wallet_id: &str,
        ts: DateTime<Utc>,
        sol_notional: f64,
    ) {
        let trade = SolLegTrade {
            ts,
            wallet_id: wallet_id.to_string(),
            sol_notional,
        };
        token_sol_history
            .entry(token.to_string())
            .or_default()
            .push(trade.clone());

        let state = token_states.entry(token.to_string()).or_default();
        Self::evict_expired_5m(state, ts);
        state.sol_volume_5m += trade.sol_notional;
        state
            .sol_traders_5m
            .entry(trade.wallet_id.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);
        state.sol_trades_5m.push_back(trade);
    }

    fn evict_expired_5m(state: &mut TokenRollingState, now: DateTime<Utc>) {
        let cutoff = now - Duration::minutes(5);
        while let Some(front) = state.sol_trades_5m.front() {
            if front.ts >= cutoff {
                break;
            }
            let expired = state
                .sol_trades_5m
                .pop_front()
                .expect("checked front exists above");
            state.sol_volume_5m = (state.sol_volume_5m - expired.sol_notional).max(0.0);
            if let Some(count) = state.sol_traders_5m.get_mut(&expired.wallet_id) {
                *count -= 1;
                if *count == 0 {
                    state.sol_traders_5m.remove(&expired.wallet_id);
                }
            }
        }
    }

    fn is_tradable_token(
        &self,
        state: &TokenRollingState,
        rpc_quality: Option<&TokenQualityCacheRow>,
        signal_ts: DateTime<Utc>,
    ) -> bool {
        let proxy_age_seconds = state
            .first_seen
            .map(|first_seen| (signal_ts - first_seen).num_seconds().max(0) as u64)
            .unwrap_or(0);
        let token_age_seconds = rpc_quality
            .and_then(|row| row.token_age_seconds)
            .unwrap_or(proxy_age_seconds);
        let holders = rpc_quality
            .and_then(|row| row.holders)
            .unwrap_or(state.wallets_seen.len() as u64);
        let liquidity_proxy = state
            .sol_trades_5m
            .iter()
            .map(|trade| trade.sol_notional)
            .fold(0.0, f64::max);
        let liquidity_sol = rpc_quality
            .and_then(|row| row.liquidity_sol)
            .unwrap_or(liquidity_proxy);

        if self.shadow_quality.min_token_age_seconds > 0 {
            if token_age_seconds < self.shadow_quality.min_token_age_seconds {
                return false;
            }
        }
        if self.shadow_quality.min_holders > 0 && holders < self.shadow_quality.min_holders {
            return false;
        }
        if self.shadow_quality.min_volume_5m_sol > 0.0
            && state.sol_volume_5m + 1e-12 < self.shadow_quality.min_volume_5m_sol
        {
            return false;
        }
        if self.shadow_quality.min_unique_traders_5m > 0
            && state.sol_traders_5m.len() < self.shadow_quality.min_unique_traders_5m as usize
        {
            return false;
        }
        if self.shadow_quality.min_liquidity_sol > 0.0
            && liquidity_sol + 1e-12 < self.shadow_quality.min_liquidity_sol
        {
            return false;
        }
        true
    }
}

impl WalletAccumulator {
    fn observe_swap(
        &mut self,
        swap: &SwapEvent,
        max_tx_per_minute: u32,
        buy_tradable: Option<bool>,
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
                buy_tradable.unwrap_or(false),
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
        tradable: bool,
    ) {
        if qty <= 0.0 || cost_sol <= 0.0 {
            return;
        }
        self.buy_observations.push(BuyObservation {
            token: token.to_string(),
            ts,
            tradable,
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

fn cmp_score_then_trades(a: &WalletSnapshot, b: &WalletSnapshot) -> Ordering {
    b.score
        .partial_cmp(&a.score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| b.trades.cmp(&a.trades))
        .then_with(|| a.wallet_id.cmp(&b.wallet_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use copybot_storage::SqliteStore;
    use std::path::Path;
    use tempfile::tempdir;

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

        let discovery = DiscoveryService::new(config, copybot_config::ShadowConfig::default());
        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.wallets_seen, 2);
        assert_eq!(summary.metrics_written, 2);
        assert!(summary.follow_promoted >= 1);

        let active = store.list_active_follow_wallets()?;
        assert!(active.contains("wallet_a"));
        assert!(!active.contains("wallet_b"));
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
        }
    }
}
