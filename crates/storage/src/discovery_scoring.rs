use super::{
    DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, DiscoveryScoringBatchStageTimings,
    DiscoveryScoringBoundarySeedLot, DiscoveryScoringBoundarySeedSnapshot,
    DiscoveryScoringSeedBoundaryInstallMarker, SqliteBatchedDeleteSummary, SqliteStore,
    TokenMarketStats, WalletScoringBuyFactRow, WalletScoringCloseFactRow, WalletScoringDayRow,
    WalletScoringQualitySource, WalletScoringSnapshot,
};
use crate::market_data::OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use copybot_core_types::SwapEvent;
use rusqlite::{params, Connection, OptionalExtension};
#[cfg(debug_assertions)]
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::{Duration as StdDuration, Instant};

include!("discovery_scoring/types_and_failpoints.rs");
include!("discovery_scoring/state_helpers.rs");
include!("discovery_scoring/state_repair_helpers.rs");
include!("discovery_scoring/state_lot_helpers.rs");
include!("discovery_scoring/prepare_quality_helpers.rs");
include!("discovery_scoring/prepare_quality.rs");
include!("discovery_scoring/scoring_lots_prepare.rs");
include!("discovery_scoring/scoring_lots_carryover.rs");
include!("discovery_scoring/scoring_lots.rs");
include!("discovery_scoring/rug_lookahead_apply.rs");
include!("discovery_scoring/store_state_and_repair.rs");
include!("discovery_scoring/store_apply_boundary.rs");
include!("discovery_scoring/store_apply_reset.rs");
include!("discovery_scoring/store_load_state.rs");
include!("discovery_scoring/store_prune_snapshot.rs");
include!("discovery_scoring/tests.rs");
