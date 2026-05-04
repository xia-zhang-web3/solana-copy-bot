#[derive(Debug, Clone, Copy, Default)]
pub struct FollowlistUpdateResult {
    pub activated: usize,
    pub deactivated: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscoveryRuntimeMode {
    Healthy,
    Degraded,
    BootstrapDegraded,
    #[default]
    FailClosed,
}

impl DiscoveryRuntimeMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::BootstrapDegraded => "bootstrap_degraded",
            Self::FailClosed => "fail_closed",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "healthy" => Ok(Self::Healthy),
            "degraded" => Ok(Self::Degraded),
            "bootstrap_degraded" => Ok(Self::BootstrapDegraded),
            "fail_closed" => Ok(Self::FailClosed),
            _ => Err(anyhow!("invalid discovery runtime mode: {raw}")),
        }
    }
}

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

#[derive(Debug, Clone)]
pub struct ObservedSwapBatchWriteMetrics {
    pub inserted: Vec<bool>,
    pub observed_swaps_insert_ms: u64,
    pub wallet_activity_days_upsert_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecentRawJournalStateRow {
    pub covered_since: Option<DateTime<Utc>>,
    pub covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub row_count: usize,
    pub last_batch_rows: usize,
    pub last_batch_completed_at: Option<DateTime<Utc>>,
    pub last_pruned_rows: usize,
    pub last_pruned_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecentRawJournalWriteSummary {
    pub batch_rows: usize,
    pub inserted_rows: usize,
    pub covered_since: Option<DateTime<Utc>>,
    pub covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub row_count: usize,
    pub last_batch_completed_at: Option<DateTime<Utc>>,
    // Bulk-write telemetry uses 0/false when the bulk path or subphase was not reached.
    pub recent_raw_bulk_sqlite_variable_limit: usize,
    pub recent_raw_bulk_statement_params_per_row: usize,
    pub recent_raw_bulk_statement_chunk_row_cap: usize,
    pub recent_raw_bulk_effective_statement_chunk_rows: usize,
    pub recent_raw_bulk_statement_count: usize,
    pub recent_raw_bulk_rows_processed: usize,
    pub recent_raw_bulk_rows_inserted: usize,
    pub recent_raw_bulk_value_build_duration_ms: u64,
    pub recent_raw_bulk_prepare_duration_ms: u64,
    pub recent_raw_bulk_execute_duration_ms: u64,
    pub recent_raw_bulk_state_refresh_duration_ms: u64,
    pub recent_raw_bulk_state_upsert_duration_ms: u64,
    pub recent_raw_bulk_transaction_duration_ms: u64,
    pub recent_raw_bulk_deadline_exhausted_before_statement: bool,
    pub recent_raw_bulk_deadline_exhausted_during_execute: bool,
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
