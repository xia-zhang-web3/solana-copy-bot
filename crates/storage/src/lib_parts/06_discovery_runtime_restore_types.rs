use super::*;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscoveryBootstrapDegradedStateRow {
    pub active: bool,
    pub reason: Option<String>,
    pub armed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscoveryRecentRawRestoreStateRow {
    pub journal_available: bool,
    pub journal_replayed: bool,
    pub required_window_start: Option<DateTime<Utc>>,
    pub journal_covered_since: Option<DateTime<Utc>>,
    pub journal_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub gap_fill_replayed: bool,
    pub gap_fill_covered_since: Option<DateTime<Utc>>,
    pub gap_fill_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub effective_covered_since: Option<DateTime<Utc>>,
    pub effective_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub artifact_runtime_cursor: Option<DiscoveryRuntimeCursor>,
    pub journal_covers_artifact_cursor: bool,
    pub raw_coverage_satisfied: bool,
    pub gap_fill_replayed_rows: usize,
    pub replayed_rows: usize,
    pub reason: Option<String>,
    pub replay_started_at: Option<DateTime<Utc>>,
    pub replay_completed_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct DiscoveryRecentRawRestoreStateUpdate {
    pub journal_available: bool,
    pub journal_replayed: bool,
    pub required_window_start: Option<DateTime<Utc>>,
    pub journal_covered_since: Option<DateTime<Utc>>,
    pub journal_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub gap_fill_replayed: bool,
    pub gap_fill_covered_since: Option<DateTime<Utc>>,
    pub gap_fill_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub effective_covered_since: Option<DateTime<Utc>>,
    pub effective_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub artifact_runtime_cursor: Option<DiscoveryRuntimeCursor>,
    pub journal_covers_artifact_cursor: bool,
    pub raw_coverage_satisfied: bool,
    pub gap_fill_replayed_rows: usize,
    pub replayed_rows: usize,
    pub reason: Option<String>,
    pub replay_started_at: Option<DateTime<Utc>>,
    pub replay_completed_at: Option<DateTime<Utc>>,
}

pub use copybot_storage_core::{
    DiscoveryRuntimeArtifact, DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
};
