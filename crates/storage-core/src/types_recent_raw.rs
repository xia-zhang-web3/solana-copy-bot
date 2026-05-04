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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecentRawJournalWriteSummary {
    pub batch_rows: usize,
    pub inserted_rows: usize,
    pub covered_since: Option<DateTime<Utc>>,
    pub covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub row_count: usize,
    pub last_batch_completed_at: Option<DateTime<Utc>>,
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
