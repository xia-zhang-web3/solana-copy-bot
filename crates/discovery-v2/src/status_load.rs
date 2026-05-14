use super::status_types::{DiscoveryV2CoverageSample, DiscoveryV2TailStatus};
use super::TOKEN_QUALITY_TTL_SECONDS;
use crate::accumulator::WalletAccumulator;
use crate::tradability::DiscoveryV2WindowAccumulator;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_storage_core::SqliteDiscoveryStore;
use std::collections::HashMap;
use std::time::Instant;

const TAIL_CLOCK_SKEW_TOLERANCE_SECONDS: i64 = 30;

pub(super) fn load_tail_status(
    store: &SqliteDiscoveryStore,
    now: DateTime<Utc>,
    max_tail_lag_seconds: u64,
) -> Result<Option<DiscoveryV2TailStatus>> {
    let Some(raw_tail) = store.observed_swaps_tail_cursor_read_only()? else {
        return Ok(None);
    };
    let raw_lag_seconds = now.signed_duration_since(raw_tail.ts_utc).num_seconds();
    let cursor = if raw_lag_seconds < -TAIL_CLOCK_SKEW_TOLERANCE_SECONDS {
        raw_tail
    } else if raw_lag_seconds < 0 {
        store
            .observed_swaps_tail_cursor_at_or_before_read_only(now)?
            .unwrap_or(raw_tail)
    } else {
        raw_tail
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
    let token_quality_cache = store
        .fresh_token_quality_cache_read_only(now, ttl)
        .context("failed loading fresh token quality cache for discovery v2 scan")?
        .into_iter()
        .map(|row| (row.mint.clone(), row))
        .collect::<HashMap<_, _>>();
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
                accumulator.observe_swap(&swap, discovery, shadow, &token_quality_cache);
                Ok(())
            },
        )
        .context("failed streaming discovery v2 current window")?;
    let unique_wallets_seen = accumulator.wallet_count();
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
    let unique_wallets_seen = wallets.len();
    Ok(DiscoveryV2WindowScan {
        wallets,
        rows_seen,
        unique_wallets_seen,
        time_budget_exhausted: page.time_budget_exhausted,
    })
}
