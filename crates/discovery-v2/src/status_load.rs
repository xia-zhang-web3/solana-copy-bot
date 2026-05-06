use super::status_types::{DiscoveryV2CoverageSample, DiscoveryV2TailStatus};
use super::TOKEN_QUALITY_TTL_SECONDS;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
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

pub(super) fn load_window_tail_swaps(
    store: &SqliteDiscoveryStore,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
    max_rows: usize,
    deadline: Instant,
) -> Result<(Vec<SwapEvent>, bool, bool)> {
    store
        .load_recent_observed_swaps_in_window_with_budget(window_start, now, max_rows, deadline)
        .context("failed loading discovery v2 current window tail")
}

pub(super) fn load_token_quality_cache_for_swaps(
    store: &SqliteDiscoveryStore,
    swaps: &[SwapEvent],
    now: DateTime<Utc>,
) -> Result<HashMap<String, TokenQualityCacheRow>> {
    let mut mints = HashSet::new();
    for swap in swaps {
        if crate::token_market::is_sol_buy(swap) {
            mints.insert(swap.token_out.clone());
        }
    }
    let ttl = Duration::seconds(TOKEN_QUALITY_TTL_SECONDS);
    let mut cache = HashMap::new();
    for mint in mints {
        if let Some(row) = store.get_token_quality_cache(&mint)? {
            if row.fetched_at <= now && now - row.fetched_at <= ttl {
                cache.insert(mint, row);
            }
        }
    }
    Ok(cache)
}
