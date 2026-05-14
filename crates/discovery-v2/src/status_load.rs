use super::status_types::{DiscoveryV2CoverageSample, DiscoveryV2TailStatus};
use super::TOKEN_QUALITY_TTL_SECONDS;
use crate::accumulator::WalletAccumulator;
use crate::token_market::is_sol_buy;
use crate::tradability::DiscoveryV2WindowAccumulator;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::{SwapEvent, TokenQualityCacheRow};
use copybot_storage_core::SqliteDiscoveryStore;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

pub(super) fn load_tail_status(
    store: &SqliteDiscoveryStore,
    now: DateTime<Utc>,
    max_tail_lag_seconds: u64,
) -> Result<Option<DiscoveryV2TailStatus>> {
    let Some(cursor) = store.observed_swaps_tail_cursor_read_only()? else {
        return Ok(None);
    };
    let raw_lag_seconds = now.signed_duration_since(cursor.ts_utc).num_seconds();
    let future_dated = raw_lag_seconds < 0;
    let lag_seconds = raw_lag_seconds.max(0);
    Ok(Some(DiscoveryV2TailStatus {
        cursor,
        lag_seconds,
        fresh: !future_dated && lag_seconds <= max_tail_lag_seconds.min(i64::MAX as u64) as i64,
        future_dated,
    }))
}

pub(super) fn load_coverage_sample(
    store: &SqliteDiscoveryStore,
    window_start: DateTime<Utc>,
) -> Result<Option<DiscoveryV2CoverageSample>> {
    let sample = store
        .observed_swaps_coverage_start_read_only()
        .context("failed loading discovery v2 coverage sample")?;
    Ok(sample.map(|swap| DiscoveryV2CoverageSample {
        ts: swap.ts_utc,
        slot: swap.slot,
        signature: swap.signature,
        wallet_id: swap.wallet,
        covers_window_start: swap.ts_utc <= window_start,
    }))
}

pub(super) struct DiscoveryV2WindowScan {
    pub(super) wallets: HashMap<String, WalletAccumulator>,
    pub(super) rows_seen: usize,
    pub(super) unique_wallets_seen: usize,
    pub(super) time_budget_exhausted: bool,
}

pub(super) fn scan_window_metrics(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
    evidence_through: DateTime<Utc>,
    max_rows: usize,
    deadline: Instant,
) -> Result<DiscoveryV2WindowScan> {
    let prefilter =
        prefilter_candidate_wallets(store, discovery, window_start, now, max_rows, deadline)
            .context("failed prefiltering discovery v2 candidate wallets")?;
    if prefilter.time_budget_exhausted || prefilter.rows_seen > max_rows {
        return Ok(DiscoveryV2WindowScan {
            wallets: HashMap::new(),
            rows_seen: prefilter.rows_seen,
            unique_wallets_seen: prefilter.unique_wallets_seen,
            time_budget_exhausted: prefilter.time_budget_exhausted,
        });
    }
    if prefilter.candidate_wallets.is_empty() {
        return Ok(DiscoveryV2WindowScan {
            wallets: HashMap::new(),
            rows_seen: prefilter.rows_seen,
            unique_wallets_seen: prefilter.unique_wallets_seen,
            time_budget_exhausted: false,
        });
    }
    let ttl = Duration::seconds(TOKEN_QUALITY_TTL_SECONDS);
    let mut token_quality_cache = HashMap::new();
    let mut token_quality_lookups = HashSet::new();
    let mut accumulator = DiscoveryV2WindowAccumulator::default();
    let page = store
        .for_each_sol_leg_observed_swap_in_window_after_cursor_with_budget(
            window_start,
            now,
            None,
            max_rows.saturating_add(1),
            deadline,
            |swap| {
                let include_wallet = prefilter.candidate_wallets.contains(&swap.wallet);
                if include_wallet {
                    load_quality_for_buy_once(
                        store,
                        &swap,
                        now,
                        ttl,
                        &mut token_quality_lookups,
                        &mut token_quality_cache,
                    )?;
                }
                accumulator.observe_swap_with_wallet_filter(
                    &swap,
                    discovery,
                    shadow,
                    &token_quality_cache,
                    include_wallet,
                );
                Ok(())
            },
        )
        .context("failed streaming discovery v2 current window")?;
    if page.time_budget_exhausted {
        return Ok(DiscoveryV2WindowScan {
            wallets: HashMap::new(),
            rows_seen: prefilter.rows_seen,
            unique_wallets_seen: prefilter.unique_wallets_seen,
            time_budget_exhausted: true,
        });
    }
    accumulator.finalize_rug_lookahead(evidence_through, discovery);
    let (wallets, _trader_ids) = accumulator.into_parts();
    Ok(DiscoveryV2WindowScan {
        wallets,
        rows_seen: prefilter.rows_seen,
        unique_wallets_seen: prefilter.unique_wallets_seen,
        time_budget_exhausted: false,
    })
}

struct WalletPrefilterScan {
    candidate_wallets: HashSet<String>,
    rows_seen: usize,
    unique_wallets_seen: usize,
    time_budget_exhausted: bool,
}

#[derive(Debug)]
struct WalletPrefilter {
    trades: u32,
    buy_total: u32,
    max_buy_notional_sol: f64,
    active_days: HashSet<chrono::NaiveDate>,
    tx_per_minute: HashMap<i64, u32>,
    suspicious: bool,
    last_seen: DateTime<Utc>,
}

impl WalletPrefilter {
    fn new(ts: DateTime<Utc>) -> Self {
        Self {
            trades: 0,
            buy_total: 0,
            max_buy_notional_sol: 0.0,
            active_days: HashSet::new(),
            tx_per_minute: HashMap::new(),
            suspicious: false,
            last_seen: ts,
        }
    }

    fn observe(&mut self, swap: &SwapEvent, discovery: &DiscoveryConfig) {
        self.trades = self.trades.saturating_add(1);
        self.active_days.insert(swap.ts_utc.date_naive());
        self.last_seen = self.last_seen.max(swap.ts_utc);
        let minute_bucket = swap.ts_utc.timestamp() / 60;
        let count = self
            .tx_per_minute
            .entry(minute_bucket)
            .and_modify(|value| *value += 1)
            .or_insert(1);
        if *count > discovery.max_tx_per_minute.max(1) {
            self.suspicious = true;
        }
        if is_sol_buy(swap) && swap.amount_in > 0.0 && swap.amount_out > 0.0 {
            self.buy_total = self.buy_total.saturating_add(1);
            self.max_buy_notional_sol = self.max_buy_notional_sol.max(swap.amount_in);
        }
    }

    fn can_pass_hard_gates(&self, discovery: &DiscoveryConfig, now: DateTime<Utc>) -> bool {
        let decay_cutoff = now - Duration::days(discovery.decay_window_days.max(1) as i64);
        self.trades >= discovery.min_trades
            && self.active_days.len() >= discovery.min_active_days as usize
            && !self.suspicious
            && self.max_buy_notional_sol + 1e-12 >= discovery.min_leader_notional_sol
            && self.last_seen >= decay_cutoff
            && self.buy_total >= discovery.min_buy_count
    }
}

fn prefilter_candidate_wallets(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
    max_rows: usize,
    deadline: Instant,
) -> Result<WalletPrefilterScan> {
    let mut rows_seen = 0usize;
    let mut wallets = HashMap::<String, WalletPrefilter>::new();
    let page = store.for_each_sol_leg_observed_swap_in_window_after_cursor_with_budget(
        window_start,
        now,
        None,
        max_rows.saturating_add(1),
        deadline,
        |swap| {
            rows_seen = rows_seen.saturating_add(1);
            if rows_seen > max_rows {
                return Ok(());
            }
            match wallets.entry(swap.wallet.clone()) {
                Entry::Occupied(mut entry) => entry.get_mut().observe(&swap, discovery),
                Entry::Vacant(entry) => {
                    let mut prefilter = WalletPrefilter::new(swap.ts_utc);
                    prefilter.observe(&swap, discovery);
                    entry.insert(prefilter);
                }
            }
            Ok(())
        },
    )?;
    let unique_wallets_seen = wallets.len();
    let candidate_wallets = wallets
        .into_iter()
        .filter_map(|(wallet_id, prefilter)| {
            prefilter
                .can_pass_hard_gates(discovery, now)
                .then_some(wallet_id)
        })
        .collect::<HashSet<_>>();
    Ok(WalletPrefilterScan {
        candidate_wallets,
        rows_seen,
        unique_wallets_seen,
        time_budget_exhausted: page.time_budget_exhausted,
    })
}

fn load_quality_for_buy_once(
    store: &SqliteDiscoveryStore,
    swap: &SwapEvent,
    now: DateTime<Utc>,
    ttl: Duration,
    looked_up: &mut HashSet<String>,
    cache: &mut HashMap<String, TokenQualityCacheRow>,
) -> Result<()> {
    if !is_sol_buy(swap) || !looked_up.insert(swap.token_out.clone()) {
        return Ok(());
    }
    if let Some(row) = store.get_token_quality_cache(&swap.token_out)? {
        if row.fetched_at <= now && now - row.fetched_at <= ttl {
            cache.insert(swap.token_out.clone(), row);
        }
    }
    Ok(())
}
