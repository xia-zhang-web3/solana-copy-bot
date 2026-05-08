use super::*;

pub use copybot_storage_core::{
    DiscoveryRuntimeMode, FollowlistUpdateResult, ObservedSwapBatchWriteMetrics,
    RecentRawJournalStateRow, RecentRawJournalWriteSummary,
};

#[derive(Debug, Clone)]
pub struct WalletActivityDayRow {
    pub wallet_id: String,
    pub activity_day: NaiveDate,
    pub last_seen: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalletActivityDayCoverageSummary {
    pub window_min_day_utc: Option<DateTime<Utc>>,
    pub window_max_day_utc: Option<DateTime<Utc>>,
    pub rows_for_wallets: u64,
    pub distinct_wallets_for_wallets: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedSwapsCoverageSnapshot {
    pub covered_since: Option<DateTime<Utc>>,
    pub covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub row_count: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletActivityDaysCoverageSnapshot {
    pub covered_since_day_utc: Option<DateTime<Utc>>,
    pub covered_through_day_utc: Option<DateTime<Utc>>,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletRecentActivityCountRow {
    pub wallet_id: String,
    pub row_count: usize,
    pub latest_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryWalletFreshnessCaptureWrite {
    pub captured_at: DateTime<Utc>,
    pub recent_cycles: usize,
    pub verdict: String,
    pub reason: String,
    pub publication_age_seconds: Option<u64>,
    pub raw_truth_sufficient: bool,
    pub raw_truth_reason: String,
    pub shadow_signal_verdict: String,
    pub shadow_signal_reason: String,
    pub published_wallet_ids: Vec<String>,
    pub active_follow_wallet_ids: Vec<String>,
    pub current_raw_top_wallet_ids: Vec<String>,
    pub audit_json: String,
    pub shadow_signal_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryWalletFreshnessCaptureRow {
    pub capture_id: i64,
    pub captured_at: DateTime<Utc>,
    pub recent_cycles: usize,
    pub verdict: String,
    pub reason: String,
    pub publication_age_seconds: Option<u64>,
    pub raw_truth_sufficient: bool,
    pub raw_truth_reason: String,
    pub shadow_signal_verdict: String,
    pub shadow_signal_reason: String,
    pub published_wallet_ids: Vec<String>,
    pub active_follow_wallet_ids: Vec<String>,
    pub current_raw_top_wallet_ids: Vec<String>,
    pub audit_json: String,
    pub shadow_signal_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecentRawJournalReplaySummary {
    pub required_window_start: DateTime<Utc>,
    pub artifact_runtime_cursor: DiscoveryRuntimeCursor,
    pub journal_available: bool,
    pub journal_covered_since: Option<DateTime<Utc>>,
    pub journal_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub journal_covers_artifact_cursor: bool,
    pub replayed_rows: usize,
    pub raw_coverage_satisfied: bool,
}
