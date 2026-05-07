use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_runtime_artifacts as runtime_artifacts;
use copybot_storage::{
    is_fatal_sqlite_anyhow_error, DiscoveryPersistedRebuildPhase,
    DiscoveryPersistedRebuildStateRow, DiscoveryPublicationFreshnessGate,
    DiscoveryPublicationStateRow, DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor,
    DiscoveryRuntimeMode, DiscoveryTrustedSelectionStateUpdate,
    DiscoveryWalletFreshnessCaptureWrite, ObservedSolLegCursorAccessPath,
    ObservedWalletActivityDayCountSource, ObservedWalletActivityRow,
    PersistedWalletMetricSnapshotRow, RecentRawJournalStateRow, SqliteStore,
    StartupTrustedSelectionGateStatus, TrustedSelectionState, TrustedSnapshotSourceKind,
    TrustedWalletMetricsSnapshotWrite, WalletMetricRow, WalletUpsertRow,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{Duration as StdDuration, Instant};
use tracing::{info, warn};

mod followlist;
mod quality_cache;
pub mod restore_verdict;
mod scoring;
pub mod wallet_freshness_audit;
mod windows;
use self::followlist::{desired_wallets, rank_follow_candidates, top_wallet_labels};
use self::scoring::{hold_time_quality_score, median_i64, tanh01};
use self::wallet_freshness_audit::PrecomputedWalletFreshnessCurrentRawTruth;
use self::windows::{
    cmp_swap_order, CachedCurrentRawTruthSample, CapTruncationDeactivationGuardReason,
    DiscoveryCursor, DiscoveryWindowState,
};
use quality_cache::BuyTradability;

#[path = "discovery_parts/prelude_00.rs"]
mod discovery_prelude_00;
#[path = "discovery_parts/prelude_01.rs"]
mod discovery_prelude_01;
#[path = "discovery_parts/prelude_02.rs"]
mod discovery_prelude_02;
#[path = "discovery_parts/prelude_03.rs"]
mod discovery_prelude_03;

pub use discovery_prelude_00::*;
pub(crate) use discovery_prelude_01::*;
pub use discovery_prelude_02::*;
pub use discovery_prelude_03::*;

#[path = "discovery_parts/service_methods_00.rs"]
mod discovery_service_methods_00;
#[path = "discovery_parts/service_methods_01.rs"]
mod discovery_service_methods_01;
#[path = "discovery_parts/service_methods_02.rs"]
mod discovery_service_methods_02;
#[path = "discovery_parts/service_methods_03.rs"]
mod discovery_service_methods_03;
#[path = "discovery_parts/service_methods_04.rs"]
mod discovery_service_methods_04;
#[path = "discovery_parts/service_methods_05.rs"]
mod discovery_service_methods_05;
#[path = "discovery_parts/service_methods_06.rs"]
mod discovery_service_methods_06;
#[path = "discovery_parts/service_methods_07.rs"]
mod discovery_service_methods_07;
#[path = "discovery_parts/service_methods_08.rs"]
mod discovery_service_methods_08;
#[path = "discovery_parts/service_methods_09.rs"]
mod discovery_service_methods_09;
#[path = "discovery_parts/service_methods_10.rs"]
mod discovery_service_methods_10;
#[path = "discovery_parts/service_methods_11.rs"]
mod discovery_service_methods_11;
#[path = "discovery_parts/service_methods_12.rs"]
mod discovery_service_methods_12;
#[path = "discovery_parts/service_methods_13.rs"]
mod discovery_service_methods_13;
#[path = "discovery_parts/service_methods_14.rs"]
mod discovery_service_methods_14;
#[path = "discovery_parts/service_methods_15.rs"]
mod discovery_service_methods_15;
#[path = "discovery_parts/service_methods_16.rs"]
mod discovery_service_methods_16;
#[path = "discovery_parts/service_methods_17.rs"]
mod discovery_service_methods_17;
#[path = "discovery_parts/service_methods_18.rs"]
mod discovery_service_methods_18;
#[path = "discovery_parts/service_methods_19.rs"]
mod discovery_service_methods_19;
#[path = "discovery_parts/service_methods_20.rs"]
mod discovery_service_methods_20;
#[path = "discovery_parts/service_methods_21.rs"]
mod discovery_service_methods_21;
#[path = "discovery_parts/service_methods_22.rs"]
mod discovery_service_methods_22;
#[path = "discovery_parts/service_methods_28_collect_buy_mints_prepass_exact_batches.rs"]
mod discovery_service_methods_28_exact_batches;
#[path = "discovery_parts/service_methods_28_collect_buy_mints_prepass.rs"]
mod discovery_service_methods_28_prepass;
#[path = "discovery_parts/service_methods_23.rs"]
mod discovery_service_methods_23;
#[path = "discovery_parts/service_methods_24.rs"]
mod discovery_service_methods_24;
#[path = "discovery_parts/service_methods_25.rs"]
mod discovery_service_methods_25;
#[path = "discovery_parts/service_methods_29_persisted_stream_rebuild_logging.rs"]
mod discovery_service_methods_29_logging;
#[path = "discovery_parts/service_methods_29_persisted_stream_rebuild_telemetry.rs"]
mod discovery_service_methods_29_telemetry;
#[path = "discovery_parts/service_methods_29_persisted_stream_rebuild.rs"]
mod discovery_service_methods_29_rebuild;
#[path = "discovery_parts/service_methods_30_run_cycle_prepare.rs"]
mod discovery_service_methods_30_prepare;
#[path = "discovery_parts/service_methods_30_run_cycle_persisted.rs"]
mod discovery_service_methods_30_persisted;
#[path = "discovery_parts/service_methods_30_run_cycle_resolve.rs"]
mod discovery_service_methods_30_resolve;
#[path = "discovery_parts/service_methods_30_run_cycle.rs"]
mod discovery_service_methods_30_run_cycle;
#[path = "discovery_parts/service_methods_26.rs"]
mod discovery_service_methods_26;
#[path = "discovery_parts/service_methods_27.rs"]
mod discovery_service_methods_27;
#[path = "discovery_parts/tail_00.rs"]
mod discovery_tail_00;

pub(crate) use discovery_service_methods_29_telemetry::*;
pub(crate) use discovery_service_methods_30_prepare::*;
pub(crate) use discovery_service_methods_30_resolve::*;
pub(crate) use discovery_tail_00::{is_sol_buy, is_sol_sell, sol_leg_token};

#[cfg(test)]
mod tests;
