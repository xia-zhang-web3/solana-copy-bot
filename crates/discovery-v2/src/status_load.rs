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
use std::collections::{HashMap, HashSet};
use std::time::Instant;

const FULL_METRIC_SCAN_ROW_THRESHOLD: usize = 100_000;

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
    let ttl = Duration::seconds(TOKEN_QUALITY_TTL_SECONDS);
    let mut token_quality_cache = HashMap::new();
    let mut token_quality_lookups = HashSet::new();
    let prefilter = store
        .discovery_v2_wallet_prefilter_read_only(
            window_start,
            now,
            max_rows.saturating_add(1),
            discovery.min_trades,
            discovery.min_buy_count,
            discovery.min_active_days,
            discovery.min_leader_notional_sol,
            deadline,
        )
        .context("failed loading discovery v2 wallet prefilter")?;
    if prefilter.time_budget_exhausted || prefilter.rows_seen > max_rows {
        let unique_wallets_seen = if prefilter.rows_seen > max_rows {
            prefilter.unique_wallets.min(max_rows)
        } else {
            prefilter.unique_wallets
        };
        return Ok(DiscoveryV2WindowScan {
            wallets: HashMap::new(),
            rows_seen: prefilter.rows_seen,
            unique_wallets_seen,
            time_budget_exhausted: prefilter.time_budget_exhausted,
        });
    }
    if prefilter.rows_seen > FULL_METRIC_SCAN_ROW_THRESHOLD && prefilter.wallet_ids.is_empty() {
        return Ok(DiscoveryV2WindowScan {
            wallets: HashMap::new(),
            rows_seen: prefilter.rows_seen,
            unique_wallets_seen: prefilter.unique_wallets,
            time_budget_exhausted: false,
        });
    }
    let unique_wallets_seen = prefilter.unique_wallets;
    let wallet_allowlist = if prefilter.rows_seen > FULL_METRIC_SCAN_ROW_THRESHOLD {
        Some(prefilter.wallet_ids.into_iter().collect::<HashSet<_>>())
    } else {
        None
    };
    let mut accumulator = if let Some(allowlist) = wallet_allowlist.clone() {
        DiscoveryV2WindowAccumulator::with_wallet_allowlist(allowlist)
    } else {
        DiscoveryV2WindowAccumulator::default()
    };
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
                if wallet_allowed_for_metrics(&swap.wallet, wallet_allowlist.as_ref()) {
                    load_quality_for_buy_once(
                        store,
                        &swap,
                        now,
                        ttl,
                        &mut token_quality_lookups,
                        &mut token_quality_cache,
                    )?;
                }
                accumulator.observe_swap(&swap, discovery, shadow, &token_quality_cache);
                Ok(())
            },
        )
        .context("failed streaming discovery v2 current window")?;
    if page.time_budget_exhausted || rows_seen > max_rows {
        return Ok(DiscoveryV2WindowScan {
            wallets: HashMap::new(),
            rows_seen,
            unique_wallets_seen,
            time_budget_exhausted: page.time_budget_exhausted,
        });
    }
    accumulator.finalize_rug_lookahead(evidence_through, discovery);
    let (wallets, _trader_ids) = accumulator.into_parts();
    Ok(DiscoveryV2WindowScan {
        wallets,
        rows_seen,
        unique_wallets_seen,
        time_budget_exhausted: page.time_budget_exhausted,
    })
}

fn wallet_allowed_for_metrics(wallet: &str, allowlist: Option<&HashSet<String>>) -> bool {
    match allowlist {
        Some(allowlist) => allowlist.contains(wallet),
        None => true,
    }
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
