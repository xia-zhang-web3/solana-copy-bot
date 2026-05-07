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

include!("wallet_freshness_audit/01_types_and_hooks.rs");

include!("wallet_freshness_audit/10_capture_api.rs");
include!("wallet_freshness_audit/20_audit_report.rs");
include!("wallet_freshness_audit/30_shadow_and_raw.rs");
include!("wallet_freshness_audit/40_rotation.rs");
include!("wallet_freshness_audit/50_compare_helpers.rs");
include!("wallet_freshness_audit/60_storage_history.rs");

#[cfg(test)]
#[path = "wallet_freshness_audit/tests.rs"]
mod tests;
