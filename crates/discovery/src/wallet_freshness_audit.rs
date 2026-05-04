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
mod tests {
    use super::{
        compare_wallet_universes, WalletFreshnessAuditReport, WalletFreshnessCaptureSnapshot,
        WalletFreshnessHistoryVerdict, WalletFreshnessRawCycleSample,
        WalletFreshnessRawTruthStatus, WalletFreshnessRotationSignal,
        WalletFreshnessShadowSignalEvidence, WalletFreshnessVerdict, WalletShadowSignalVerdict,
        WalletUniverseComparison, DEFAULT_RECENT_CYCLES,
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

    include!("wallet_freshness_audit/tests_00_helpers.rs");
    include!("wallet_freshness_audit/tests_01_audit.rs");
    include!("wallet_freshness_audit/tests_02_capture.rs");
    include!("wallet_freshness_audit/tests_03_history.rs");
}
