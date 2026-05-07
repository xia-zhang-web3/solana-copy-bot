use crate::{
    followlist::{desired_wallets, rank_follow_candidates},
    DiscoveryService, RuntimePublishedUniverseTruth,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_storage::{
    DiscoveryPublicationStateRow, DiscoveryRuntimeCursor, DiscoveryWalletFreshnessCaptureRow,
    DiscoveryWalletFreshnessCaptureWrite, ObservedSwapsCoverageSnapshot, SqliteStore,
    WalletRecentActivityCountRow,
};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::BTreeSet, time::Instant};

#[path = "wallet_freshness_audit/10_capture_api.rs"]
mod capture_api;
#[path = "wallet_freshness_audit/50_compare_helpers.rs"]
mod compare_helpers;
#[path = "wallet_freshness_audit/60_storage_history.rs"]
mod storage_history;
#[path = "wallet_freshness_audit/20_audit_report.rs"]
mod audit_report;
#[path = "wallet_freshness_audit/30_shadow_and_raw.rs"]
mod shadow_and_raw;
#[path = "wallet_freshness_audit/01_types_and_hooks.rs"]
mod types_and_hooks;
#[path = "wallet_freshness_audit/40_rotation.rs"]
mod rotation;

use self::compare_helpers::*;
use self::storage_history::*;
#[cfg(test)]
use self::types_and_hooks::CURRENT_RAW_TRUTH_SAMPLE_CALLS;
use self::types_and_hooks::{RawTruthCyclePoint, RawTruthSample};
pub use self::storage_history::wallet_freshness_capture_from_row;
pub use self::types_and_hooks::*;

#[cfg(test)]
#[path = "wallet_freshness_audit/tests.rs"]
mod tests;
