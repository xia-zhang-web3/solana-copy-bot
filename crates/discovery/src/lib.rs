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

include!("discovery_parts/service_methods_00.rs");

include!("discovery_parts/service_methods_01.rs");

include!("discovery_parts/service_methods_02.rs");

include!("discovery_parts/service_methods_03.rs");

include!("discovery_parts/service_methods_04.rs");

include!("discovery_parts/service_methods_05.rs");

include!("discovery_parts/service_methods_06.rs");

include!("discovery_parts/service_methods_07.rs");

include!("discovery_parts/service_methods_08.rs");

include!("discovery_parts/service_methods_09.rs");

include!("discovery_parts/service_methods_10.rs");

include!("discovery_parts/service_methods_11.rs");

include!("discovery_parts/service_methods_12.rs");

include!("discovery_parts/service_methods_13.rs");

include!("discovery_parts/service_methods_14.rs");

include!("discovery_parts/service_methods_15.rs");

include!("discovery_parts/service_methods_16.rs");

include!("discovery_parts/service_methods_17.rs");

include!("discovery_parts/service_methods_18.rs");

include!("discovery_parts/service_methods_19.rs");

include!("discovery_parts/service_methods_20.rs");

include!("discovery_parts/service_methods_21.rs");

include!("discovery_parts/service_methods_22.rs");

include!("discovery_parts/service_methods_28_collect_buy_mints_prepass_exact_batches.rs");
include!("discovery_parts/service_methods_28_collect_buy_mints_prepass.rs");

include!("discovery_parts/service_methods_23.rs");

include!("discovery_parts/service_methods_24.rs");

include!("discovery_parts/service_methods_25.rs");

include!("discovery_parts/service_methods_29_persisted_stream_rebuild_logging.rs");
include!("discovery_parts/service_methods_29_persisted_stream_rebuild_telemetry.rs");
include!("discovery_parts/service_methods_29_persisted_stream_rebuild.rs");

include!("discovery_parts/service_methods_30_run_cycle_prepare.rs");
include!("discovery_parts/service_methods_30_run_cycle_persisted.rs");
include!("discovery_parts/service_methods_30_run_cycle_resolve.rs");
include!("discovery_parts/service_methods_30_run_cycle.rs");

include!("discovery_parts/service_methods_26.rs");

include!("discovery_parts/service_methods_27.rs");

include!("discovery_parts/tail_00.rs");

#[cfg(test)]
mod tests;
