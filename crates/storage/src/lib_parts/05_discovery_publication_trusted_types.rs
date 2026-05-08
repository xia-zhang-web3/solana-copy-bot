use super::*;

#[derive(Debug, Clone)]
pub struct TrustedWalletMetricsSnapshotWrite {
    pub snapshot_id: String,
    pub source_snapshot_id: Option<String>,
    pub source_window_start: Option<DateTime<Utc>>,
    pub effective_window_start: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub source_kind: TrustedSnapshotSourceKind,
    pub row_count: usize,
    pub trust_state: TrustedSelectionState,
}

#[derive(Debug, Clone)]
pub struct TrustedWalletMetricsSnapshotRow {
    pub snapshot_id: String,
    pub source_snapshot_id: Option<String>,
    pub source_window_start: Option<DateTime<Utc>>,
    pub effective_window_start: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub source_kind: TrustedSnapshotSourceKind,
    pub row_count: usize,
    pub trust_state: TrustedSelectionState,
}

pub use copybot_storage_core::{
    DiscoveryPublicationStateRow, DiscoveryPublicationStateUpdate,
    DiscoveryTrustedSelectionStateRow, DiscoveryTrustedSelectionStateUpdate,
};
