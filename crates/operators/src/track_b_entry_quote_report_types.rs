use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct TrackBEntryQuoteSummary {
    pub metric_basis: String,
    pub caveats: Vec<String>,
    pub counts: SummaryCounts,
    pub price_ratio_stats: NumericStats,
    pub price_impact_stats: NumericStats,
    pub market_exit_quote_ratio_stats: NumericStats,
    pub market_exit_decision_delay_ms_stats: NumericStats,
    pub by_close_bucket: Vec<BucketSummary>,
    pub by_exit_executability: Vec<BucketSummary>,
    pub price_impact_sweep: Vec<SweepRow>,
    pub quote_shadow_ratio_sweep: Vec<SweepRow>,
}

#[derive(Debug, Default, Serialize)]
pub struct SummaryCounts {
    pub total_events: u64,
    pub ok_events: u64,
    pub error_events: u64,
    pub open_or_unmatched_events: u64,
    pub closed_events: u64,
    pub clean_closed_usable_events: u64,
    pub ok_null_quote_price_events: u64,
    pub contaminated_ratio_events: u64,
    pub multi_close_match_events: u64,
    pub truncated_at_close_match_limit_events: u64,
    pub mixed_close_context_events: u64,
    pub market_exit_quote_events: u64,
    pub market_exit_error_events: u64,
    pub market_exit_missing_quote_events: u64,
    pub market_exit_zero_exit_events: u64,
    pub first_event_ts: Option<String>,
    pub last_event_ts: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct NumericStats {
    pub count: u64,
    pub avg: Option<f64>,
    pub p50: Option<f64>,
    pub p90: Option<f64>,
    pub p95: Option<f64>,
    pub max: Option<f64>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct BucketSummary {
    pub bucket: String,
    pub events: u64,
    pub shadow_pnl_sol: f64,
    pub entry_adjusted_pnl_sol: f64,
    pub entry_adjusted_delta_sol: f64,
    pub fully_executable_events: u64,
    pub fully_executable_pnl_sol: Option<f64>,
    pub fully_executable_delta_sol: Option<f64>,
    pub market_exit_quote_events: u64,
    pub market_exit_error_events: u64,
    pub market_exit_missing_quote_events: u64,
    pub market_exit_zero_exit_events: u64,
    pub avg_quote_shadow_ratio: Option<f64>,
    pub avg_price_impact_pct: Option<f64>,
    pub avg_market_exit_quote_shadow_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SweepRow {
    pub metric: String,
    pub threshold_gt: f64,
    pub rejected_events: u64,
    pub rejected_market_events: u64,
    pub rejected_stale_quote_events: u64,
    pub rejected_stale_market_events: u64,
    pub rejected_terminal_events: u64,
    pub rejected_mixed_events: u64,
    pub rejected_shadow_pnl_sol: f64,
    pub rejected_entry_adjusted_pnl_sol: f64,
    pub rejected_fully_executable_pnl_sol: Option<f64>,
    pub delta_if_rejected_entry_adjusted_sol: f64,
    pub delta_if_rejected_fully_executable_sol: Option<f64>,
    pub warning: String,
}
