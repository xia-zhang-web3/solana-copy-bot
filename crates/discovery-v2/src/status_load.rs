use super::status_types::{DiscoveryV2CoverageSample, DiscoveryV2TailStatus};
use super::TOKEN_QUALITY_TTL_SECONDS;
use crate::accumulator::WalletAccumulator;
use crate::metric::rug_history_tokens_for_pre_eligible_wallets;
use crate::token_market::{is_sol_buy, sol_leg_token_and_notional, SolLegTrade};
use crate::tradability::DiscoveryV2WindowAccumulator;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::{SwapEvent, TokenQualityCacheRow};
use copybot_storage_core::SqliteDiscoveryStore;
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
    pub(super) token_sol_history: HashMap<String, Vec<SolLegTrade>>,
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
    max_rows: usize,
    deadline: Instant,
) -> Result<DiscoveryV2WindowScan> {
    let ttl = Duration::seconds(TOKEN_QUALITY_TTL_SECONDS);
    let mut token_quality_cache = HashMap::new();
    let mut token_quality_lookups = HashSet::new();
    let mut accumulator = DiscoveryV2WindowAccumulator::default();
    let mut rows_seen = 0usize;
    let page = store
        .for_each_sol_leg_observed_swap_in_window_after_cursor_with_budget(
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
                load_quality_for_buy_once(
                    store,
                    &swap,
                    now,
                    ttl,
                    &mut token_quality_lookups,
                    &mut token_quality_cache,
                )?;
                accumulator.observe_swap(&swap, discovery, shadow, &token_quality_cache);
                Ok(())
            },
        )
        .context("failed streaming discovery v2 current window")?;
    let unique_wallets_seen = accumulator.wallet_count();
    if page.time_budget_exhausted || rows_seen > max_rows {
        return Ok(DiscoveryV2WindowScan {
            wallets: HashMap::new(),
            token_sol_history: HashMap::new(),
            rows_seen,
            unique_wallets_seen,
            time_budget_exhausted: page.time_budget_exhausted,
        });
    }
    let (wallets, trader_ids) = accumulator.into_parts();
    let unique_wallets_seen = wallets.len();
    let token_history = collect_needed_token_sol_history(
        store,
        discovery,
        window_start,
        now,
        max_rows,
        deadline,
        &wallets,
        &trader_ids,
    )?;
    if token_history.time_budget_exhausted || token_history.rows_seen > max_rows {
        return Ok(DiscoveryV2WindowScan {
            wallets: HashMap::new(),
            token_sol_history: HashMap::new(),
            rows_seen: if token_history.rows_seen > max_rows {
                max_rows.saturating_add(1)
            } else {
                rows_seen
            },
            unique_wallets_seen,
            time_budget_exhausted: token_history.time_budget_exhausted,
        });
    }
    Ok(DiscoveryV2WindowScan {
        wallets,
        token_sol_history: token_history.history,
        rows_seen,
        unique_wallets_seen,
        time_budget_exhausted: page.time_budget_exhausted,
    })
}

struct TokenSolHistoryScan {
    history: HashMap<String, Vec<SolLegTrade>>,
    rows_seen: usize,
    time_budget_exhausted: bool,
}

fn collect_needed_token_sol_history(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
    max_rows: usize,
    deadline: Instant,
    wallets: &HashMap<String, WalletAccumulator>,
    trader_ids: &HashMap<String, u32>,
) -> Result<TokenSolHistoryScan> {
    let needed_tokens = rug_history_tokens_for_pre_eligible_wallets(wallets, discovery, now);
    if needed_tokens.is_empty() {
        return Ok(TokenSolHistoryScan {
            history: HashMap::new(),
            rows_seen: 0,
            time_budget_exhausted: false,
        });
    }

    let mut rows_seen = 0usize;
    let mut history = HashMap::<String, Vec<SolLegTrade>>::new();
    let page = store
        .for_each_sol_leg_observed_swap_in_window_after_cursor_with_budget(
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
                let Some((token, sol_notional, token_qty)) = sol_leg_token_and_notional(&swap)
                else {
                    return Ok(());
                };
                if !needed_tokens.contains(token) {
                    return Ok(());
                }
                let Some(trader_id) = trader_ids.get(&swap.wallet).copied() else {
                    return Ok(());
                };
                history
                    .entry(token.to_string())
                    .or_default()
                    .push(SolLegTrade {
                        ts: swap.ts_utc,
                        trader_id,
                        sol_notional: sol_notional.max(0.0),
                        token_qty: token_qty.max(0.0),
                    });
                Ok(())
            },
        )
        .context("failed streaming discovery v2 selective token history")?;
    for trades in history.values_mut() {
        sort_sol_trades(trades);
    }
    Ok(TokenSolHistoryScan {
        history,
        rows_seen,
        time_budget_exhausted: page.time_budget_exhausted,
    })
}

fn sort_sol_trades(trades: &mut [SolLegTrade]) {
    trades.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then_with(|| left.trader_id.cmp(&right.trader_id))
    });
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
