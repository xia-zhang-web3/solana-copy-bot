use super::{
    compare_wallet_universes, WalletFreshnessAuditReport, WalletFreshnessCaptureSnapshot,
    WalletFreshnessHistoryVerdict, WalletFreshnessRawCycleSample, WalletFreshnessRawTruthStatus,
    WalletFreshnessRotationSignal, WalletFreshnessShadowSignalEvidence, WalletFreshnessVerdict,
    WalletShadowSignalVerdict, WalletUniverseComparison, DEFAULT_RECENT_CYCLES,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::{CopySignalRow, SwapEvent, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE};
use copybot_storage::{
    DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor, DiscoveryRuntimeMode, SqliteStore,
    WalletRecentActivityCountRow,
};
use std::path::Path;
use tempfile::tempdir;

use crate::DiscoveryService;

#[path = "tests_00_helpers.rs"]
mod helpers;
#[path = "tests_01_audit.rs"]
mod audit;
#[path = "tests_02_capture.rs"]
mod capture;
#[path = "tests_03_history.rs"]
mod history;

use helpers::*;
