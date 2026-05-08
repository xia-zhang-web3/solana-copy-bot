use super::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryScoringBoundarySeedLot {
    pub buy_signature: String,
    pub wallet_id: String,
    pub token: String,
    pub qty: f64,
    pub cost_sol: f64,
    pub opened_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveryScoringBoundarySeedSnapshot {
    pub boundary_start_ts: DateTime<Utc>,
    pub boundary_cursor: DiscoveryRuntimeCursor,
    pub open_lots: Vec<DiscoveryScoringBoundarySeedLot>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveryScoringSeedBoundaryInstallMarker {
    pub boundary_start_ts: DateTime<Utc>,
    pub boundary_cursor: DiscoveryRuntimeCursor,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiscoveryScoringBatchStageTimings {
    pub prepare_ms: u64,
    pub apply_ms: u64,
    pub rug_finalize_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiscoveryScoringCheckpointedBatchTimings {
    pub prepare_ms: u64,
    pub apply_ms: u64,
    pub progress_update_ms: u64,
}
