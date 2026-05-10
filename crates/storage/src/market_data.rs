use crate::{
    discovery::upsert_wallet_activity_days_on_conn, DiscoveryPersistedRebuildPhase,
    DiscoveryPersistedRebuildStateMetaLiteRawRow, DiscoveryPersistedRebuildStateMetaRow,
    DiscoveryPersistedRebuildStateRow, DiscoveryRuntimeCursor, ObservedSwapBatchWriteMetrics,
    ObservedSwapsCoverageSnapshot, RecentRawJournalReplaySummary, RecentRawJournalStateRow,
    RecentRawJournalWriteSummary, SqliteBatchedDeleteSummary, SqliteStore, TokenMarketStats,
    TokenQualityCacheRow, TokenQualityRpcRow, WalletActivityDayRow,
    WalletActivityDaysCoverageSnapshot, WalletRecentActivityCountRow,
};
use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use copybot_core_types::{ExactSwapAmounts, SwapEvent};
use reqwest::blocking::Client;
use rusqlite::{
    params, params_from_iter, types::Value as SqlValue, Connection, ErrorCode, OptionalExtension,
};
use serde_json::{json, Value};
#[cfg(test)]
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::time::{Duration as StdDuration, Instant};

#[path = "market_data_parts/02_recent_raw_helpers.rs"]
mod recent_raw_helpers;
#[path = "market_data_parts/02_recent_raw_state_helpers.rs"]
mod recent_raw_state_helpers;
#[path = "market_data_parts/02_recent_raw_timestamp_guard.rs"]
mod recent_raw_timestamp_guard;
#[path = "market_data_parts/90_rpc_quality_helpers.rs"]
mod rpc_quality_helpers;
#[path = "market_data_parts/13_store_impl_activity_helpers.rs"]
mod store_impl_activity_helpers;
#[path = "market_data_parts/10_store_impl_bulk_recent_raw.rs"]
mod store_impl_bulk_recent_raw;
#[path = "market_data_parts/15_store_impl_buy_mint_counts.rs"]
mod store_impl_buy_mint_counts;
#[path = "market_data_parts/15_store_impl.rs"]
mod store_impl_buy_mints;
#[path = "market_data_parts/16_store_impl_coverage.rs"]
mod store_impl_coverage;
#[path = "market_data_parts/16_store_impl_discovery_cursor.rs"]
mod store_impl_discovery_cursor;
#[path = "market_data_parts/14_store_impl_filters.rs"]
mod store_impl_filters;
#[path = "market_data_parts/11_store_impl.rs"]
mod store_impl_observed_swaps;
#[path = "market_data_parts/11_store_impl_streaming.rs"]
mod store_impl_observed_swaps_streaming;
#[path = "market_data_parts/18_store_impl.rs"]
mod store_impl_quality_cache;
#[path = "market_data_parts/17_store_impl.rs"]
mod store_impl_rebuild;
#[path = "market_data_parts/17_store_impl_rebuild_state.rs"]
mod store_impl_rebuild_state;
#[path = "market_data_parts/10_store_impl.rs"]
mod store_impl_recent_raw;
#[path = "market_data_parts/16_store_impl.rs"]
mod store_impl_runtime_cursor;
#[path = "market_data_parts/14_store_impl.rs"]
mod store_impl_sol_leg;
#[path = "market_data_parts/13_store_impl.rs"]
mod store_impl_wallet_activity;
#[path = "market_data_parts/12_store_impl.rs"]
mod store_impl_wallet_activity_exact;
#[path = "market_data_parts/01_types_and_progress.rs"]
mod types_and_progress;

use self::recent_raw_helpers::*;
use self::recent_raw_state_helpers::*;
use self::rpc_quality_helpers::*;
pub(crate) use self::types_and_progress::OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY;
use self::types_and_progress::*;
pub use self::types_and_progress::{
    ObservedBuyMintCount, ObservedBuyMintCountPage, ObservedBuyMintCountRow,
    ObservedBuyMintOccurrenceCount, ObservedBuyMintPage, ObservedSolLegCursorAccessPath,
    ObservedSolLegCursorPage, ObservedSwapCursorPage, ObservedWalletActivityDayCountSource,
    ObservedWalletActivityPage, ObservedWalletActivityRow,
};

pub(crate) fn validate_observed_swaps_timestamps_canonical_utc(conn: &Connection) -> Result<()> {
    ensure_recent_raw_observed_swaps_timestamps_canonical_utc(conn)
}

pub(crate) fn validate_wallet_activity_days_last_seen_canonical_utc(
    conn: &Connection,
) -> Result<()> {
    let invalid_ts: Option<String> = conn
        .query_row(
            "SELECT last_seen
             FROM wallet_activity_days
             WHERE NOT (
                (
                    (length(last_seen) = 25 AND substr(last_seen, 20, 6) = '+00:00')
                    OR (
                        length(last_seen) BETWEEN 27 AND 35
                        AND substr(last_seen, 20, 1) = '.'
                        AND substr(last_seen, -6) = '+00:00'
                        AND substr(last_seen, 21, length(last_seen) - 26) GLOB '[0-9]*'
                        AND substr(last_seen, 21, length(last_seen) - 26) NOT GLOB '*[^0-9]*'
                    )
                )
                AND substr(last_seen, 5, 1) = '-'
                AND substr(last_seen, 8, 1) = '-'
                AND substr(last_seen, 11, 1) = 'T'
                AND substr(last_seen, 14, 1) = ':'
                AND substr(last_seen, 17, 1) = ':'
                AND substr(last_seen, 1, 4) GLOB '[0-9][0-9][0-9][0-9]'
                AND substr(last_seen, 6, 2) GLOB '[0-9][0-9]'
                AND CAST(substr(last_seen, 6, 2) AS INTEGER) BETWEEN 1 AND 12
                AND substr(last_seen, 9, 2) GLOB '[0-9][0-9]'
                AND CAST(substr(last_seen, 9, 2) AS INTEGER) BETWEEN 1 AND 31
                AND substr(last_seen, 12, 2) GLOB '[0-9][0-9]'
                AND CAST(substr(last_seen, 12, 2) AS INTEGER) BETWEEN 0 AND 23
                AND substr(last_seen, 15, 2) GLOB '[0-9][0-9]'
                AND CAST(substr(last_seen, 15, 2) AS INTEGER) BETWEEN 0 AND 59
                AND substr(last_seen, 18, 2) GLOB '[0-9][0-9]'
                AND CAST(substr(last_seen, 18, 2) AS INTEGER) BETWEEN 0 AND 59
                AND julianday(last_seen) IS NOT NULL
                AND strftime('%Y-%m-%dT%H:%M:%S', last_seen) = substr(last_seen, 1, 19)
             )
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("failed validating wallet_activity_days.last_seen canonical UTC")?;
    if let Some(raw) = invalid_ts {
        bail!("wallet_activity_days.last_seen is not canonical UTC: {raw}");
    }
    Ok(())
}

#[cfg(test)]
#[path = "market_data/tests.rs"]
mod tests;
