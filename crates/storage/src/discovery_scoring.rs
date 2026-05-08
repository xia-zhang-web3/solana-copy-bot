use super::{
    DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, DiscoveryScoringBatchStageTimings,
    DiscoveryScoringBoundarySeedLot, DiscoveryScoringBoundarySeedSnapshot,
    DiscoveryScoringSeedBoundaryInstallMarker, SqliteBatchedDeleteSummary, SqliteStore,
    TokenMarketStats, WalletScoringBuyFactRow, WalletScoringCloseFactRow, WalletScoringDayRow,
    WalletScoringQualitySource, WalletScoringSnapshot,
};
use crate::discovery_quality_types::{QualityCacheUpsert, QualityFetchBudget};
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

#[path = "discovery_scoring/prepare_quality.rs"]
mod prepare_quality;
#[path = "discovery_scoring/prepare_quality_helpers.rs"]
mod prepare_quality_helpers;
#[path = "discovery_scoring/rug_lookahead_apply.rs"]
mod rug_lookahead_apply;
#[path = "discovery_scoring/scoring_lots.rs"]
mod scoring_lots;
#[path = "discovery_scoring/scoring_lots_carryover.rs"]
mod scoring_lots_carryover;
#[path = "discovery_scoring/scoring_lots_prepare.rs"]
mod scoring_lots_prepare;
#[path = "discovery_scoring/state_helpers.rs"]
mod state_helpers;
#[path = "discovery_scoring/state_lot_helpers.rs"]
mod state_lot_helpers;
#[path = "discovery_scoring/state_repair_helpers.rs"]
mod state_repair_helpers;
#[path = "discovery_scoring/store_apply_boundary.rs"]
mod store_apply_boundary;
#[path = "discovery_scoring/store_apply_reset.rs"]
mod store_apply_reset;
#[path = "discovery_scoring/store_load_state.rs"]
mod store_load_state;
#[path = "discovery_scoring/store_prune_snapshot.rs"]
mod store_prune_snapshot;
#[path = "discovery_scoring/store_state_and_repair.rs"]
mod store_state_and_repair;
#[cfg(test)]
#[path = "discovery_scoring/tests.rs"]
mod tests;
#[path = "discovery_scoring/types_and_failpoints.rs"]
mod types_and_failpoints;

use self::prepare_quality::*;
use self::prepare_quality_helpers::*;
use self::rug_lookahead_apply::*;
use self::scoring_lots::*;
use self::scoring_lots_carryover::*;
use self::scoring_lots_prepare::*;
pub(crate) use self::state_helpers::upsert_discovery_scoring_backfill_progress_on_conn;
use self::state_helpers::*;
use self::state_lot_helpers::*;
use self::state_repair_helpers::*;
pub(crate) use self::types_and_failpoints::maybe_fail_after_materialization_before_checkpoint;
use self::types_and_failpoints::*;
