use chrono::{DateTime, NaiveDate, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedSwapBatchWriteMetrics {
    pub inserted: Vec<bool>,
    pub observed_swaps_insert_ms: u64,
    pub wallet_activity_days_upsert_ms: u64,
}

#[derive(Debug, Clone)]
pub struct WalletActivityDayRow {
    pub wallet_id: String,
    pub activity_day: NaiveDate,
    pub last_seen: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqliteBatchedDeleteSummary {
    pub deleted_rows: usize,
    pub batches: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqliteBatchedDeleteSummaryWithCompletion {
    pub deleted_rows: usize,
    pub batches: usize,
    pub completed_full_sweep: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqliteContentionSnapshot {
    pub write_retry_total: u64,
    pub busy_error_total: u64,
}
