use super::{
    DiscoveryBootstrapDegradedStateRow, DiscoveryPublicationFreshnessGate,
    DiscoveryPublicationStateRow, DiscoveryPublicationStateUpdate,
    DiscoveryRecentRawRestoreStateRow, DiscoveryRecentRawRestoreStateUpdate,
    DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor, DiscoveryRuntimeMode,
    DiscoveryTrustedSelectionStateRow, DiscoveryTrustedSelectionStateUpdate,
    DiscoveryWalletFreshnessCaptureRow, DiscoveryWalletFreshnessCaptureWrite, SqliteStore,
    StartupTrustedSelectionGateStatus, TrustedSelectionState, TrustedSnapshotSourceKind,
    TrustedWalletMetricsSnapshotRow, TrustedWalletMetricsSnapshotWrite, WalletActivityDayRow,
    DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
};
use anyhow::{Context, Result};

include!("discovery/01_helpers.rs");
include!("discovery/02_runtime_restore_helpers.rs");

include!("discovery/10_publication_state.rs");
include!("discovery/10_publication_state_restore.rs");
include!("discovery/20_trusted_selection.rs");
include!("discovery/30_wallet_metrics_export.rs");
include!("discovery/40_runtime_artifact_restore.rs");
include!("discovery/50_publication_followlist.rs");
include!("discovery/50_publication_followlist_freshness.rs");
include!("discovery/60_schema.rs");

#[cfg(test)]
#[path = "discovery/tests.rs"]
mod tests;
