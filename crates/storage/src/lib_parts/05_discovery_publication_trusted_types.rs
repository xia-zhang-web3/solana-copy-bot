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

#[derive(Debug, Clone)]
pub struct DiscoveryTrustedSelectionStateUpdate {
    pub bootstrap_required: bool,
    pub reason: String,
    pub selection_state: TrustedSelectionState,
    pub active_snapshot_id: Option<String>,
    pub active_snapshot_window_start: Option<DateTime<Utc>>,
    pub last_bootstrap_source_kind: Option<TrustedSnapshotSourceKind>,
    pub last_bootstrap_at: Option<DateTime<Utc>>,
}

pub use copybot_storage_core::{DiscoveryPublicationStateRow, DiscoveryPublicationStateUpdate};

#[derive(Debug, Clone)]
pub struct DiscoveryTrustedSelectionStateRow {
    pub bootstrap_required: bool,
    pub reason: String,
    pub selection_state: TrustedSelectionState,
    pub active_snapshot_id: Option<String>,
    pub active_snapshot_window_start: Option<DateTime<Utc>>,
    pub last_bootstrap_source_kind: Option<TrustedSnapshotSourceKind>,
    pub last_bootstrap_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}
