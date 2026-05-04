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

include!("market_data_parts/01_types_and_progress.rs");
include!("market_data_parts/02_recent_raw_helpers.rs");
include!("market_data_parts/02_recent_raw_state_helpers.rs");
include!("market_data_parts/10_store_impl.rs");
include!("market_data_parts/10_store_impl_bulk_recent_raw.rs");
include!("market_data_parts/11_store_impl.rs");
include!("market_data_parts/11_store_impl_streaming.rs");
include!("market_data_parts/12_store_impl.rs");
include!("market_data_parts/13_store_impl.rs");
include!("market_data_parts/13_store_impl_activity_helpers.rs");
include!("market_data_parts/14_store_impl_filters.rs");
include!("market_data_parts/14_store_impl.rs");
include!("market_data_parts/15_store_impl.rs");
include!("market_data_parts/15_store_impl_buy_mint_counts.rs");
include!("market_data_parts/16_store_impl.rs");
include!("market_data_parts/16_store_impl_coverage.rs");
include!("market_data_parts/16_store_impl_discovery_cursor.rs");
include!("market_data_parts/17_store_impl_rebuild_state.rs");
include!("market_data_parts/17_store_impl.rs");
include!("market_data_parts/18_store_impl.rs");
include!("market_data_parts/90_rpc_quality_helpers.rs");

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_core_types::SwapEvent;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration as StdDuration, Instant};
    use tempfile::tempdir;

    include!("market_data_tests/00_market_data_tests.rs");
    include!("market_data_tests/01_market_data_tests.rs");
    include!("market_data_tests/02_market_data_tests.rs");
    include!("market_data_tests/03_market_data_tests.rs");
    include!("market_data_tests/04_market_data_tests.rs");
}
