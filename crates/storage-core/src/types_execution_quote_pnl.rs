use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionCanaryQuotePnlSummary {
    pub as_of: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub limit: u32,
    pub shadow_close_breakdown: ExecutionCanaryShadowCloseBreakdown,
    pub total_closed_trades: u64,
    pub matched_quote_trades: u64,
    pub pnl_counted_trades: u64,
    pub skipped_trades: u64,
    pub unknown_trades: u64,
    pub missing_entry_quote_trades: u64,
    pub missing_exit_quote_trades: u64,
    pub invalid_quote_amount_trades: u64,
    pub shadow_win_count: u64,
    pub shadow_loss_count: u64,
    pub quote_win_count: u64,
    pub quote_loss_count: u64,
    pub shadow_pnl_sol: f64,
    pub quote_adjusted_pnl_sol: f64,
    pub quote_adjusted_pnl_after_priority_fee_sol: f64,
    pub quote_vs_shadow_delta_sol: f64,
    pub quote_after_fee_vs_shadow_delta_sol: f64,
    pub skipped_shadow_pnl_sol: f64,
    pub skipped_counterfactual_pnl_sol: f64,
    pub skipped_counterfactual_pnl_after_priority_fee_sol: f64,
    pub skipped_counterfactual_after_fee_vs_shadow_delta_sol: f64,
    pub force_exit_counted_trades: u64,
    pub force_exit_skipped_entry_trades: u64,
    pub quote_diagnostics: ExecutionCanaryQuoteDiagnosticsSummary,
    pub threshold_summaries: Vec<ExecutionCanaryQuotePnlThresholdSummary>,
    pub buy_slippage_buckets: Vec<ExecutionCanaryQuoteBucketSummary>,
    pub entry_decision_delay_buckets: Vec<ExecutionCanaryQuoteBucketSummary>,
    pub buy_leader_notional_buckets: Vec<ExecutionCanaryQuoteBucketSummary>,
    pub route_counts: Vec<ExecutionCanaryQuoteRouteCount>,
    pub priority_fee_status_counts: Vec<ExecutionCanaryQuoteStatusCount>,
    pub priority_fee_lamports_sum: u64,
    pub readiness_gate: ExecutionCanaryQuoteReadinessGate,
    pub trades: Vec<ExecutionCanaryQuotePnlTrade>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionCanaryShadowCloseBreakdown {
    pub total_closed_trades: u64,
    pub total_win_count: u64,
    pub total_loss_count: u64,
    pub total_pnl_sol: f64,
    pub market_closed_trades: u64,
    pub market_pnl_sol: f64,
    pub stale_closed_trades: u64,
    pub stale_pnl_sol: f64,
    pub stale_rug_like_closed_trades: u64,
    pub stale_rug_like_pnl_sol: f64,
    pub non_market_closed_trades: u64,
    pub non_market_pnl_sol: f64,
    pub contexts: Vec<ExecutionCanaryShadowCloseContextSummary>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionCanaryShadowCloseContextSummary {
    pub close_context: String,
    pub closed_trades: u64,
    pub win_count: u64,
    pub loss_count: u64,
    pub pnl_sol: f64,
    pub rug_like_closed_trades: u64,
    pub rug_like_pnl_sol: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionCanaryQuoteDiagnosticsSummary {
    pub entry_all: ExecutionCanaryQuoteSideDiagnostics,
    pub entry_counted: ExecutionCanaryQuoteSideDiagnostics,
    pub entry_skipped: ExecutionCanaryQuoteSideDiagnostics,
    pub exit_all: ExecutionCanaryQuoteSideDiagnostics,
    pub exit_counted: ExecutionCanaryQuoteSideDiagnostics,
    pub exit_skipped_entry: ExecutionCanaryQuoteSideDiagnostics,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionCanaryQuoteSideDiagnostics {
    pub events: u64,
    pub decision_delay_ms_samples: u64,
    pub decision_delay_ms_avg: f64,
    pub decision_delay_ms_max: u64,
    pub quote_latency_ms_samples: u64,
    pub quote_latency_ms_avg: f64,
    pub quote_latency_ms_max: u64,
    pub slippage_bps_samples: u64,
    pub slippage_bps_avg: f64,
    pub slippage_bps_max: f64,
    pub price_impact_pct_samples: u64,
    pub price_impact_pct_avg: f64,
    pub price_impact_pct_max: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionCanaryQuotePnlThresholdSummary {
    pub threshold_bps: u64,
    pub total_closed_trades: u64,
    pub counted_trades: u64,
    pub skipped_trades: u64,
    pub unknown_trades: u64,
    pub quote_win_count: u64,
    pub quote_loss_count: u64,
    pub shadow_pnl_sol: f64,
    pub quote_adjusted_pnl_after_priority_fee_sol: f64,
    pub skipped_shadow_pnl_sol: f64,
    pub skipped_counterfactual_pnl_after_priority_fee_sol: f64,
    pub force_exit_counted_trades: u64,
    pub force_exit_skipped_entry_trades: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionCanaryQuoteThresholdCandidate {
    pub threshold_bps: u64,
    pub counted_trades: u64,
    pub skipped_trades: u64,
    pub unknown_trades: u64,
    pub skip_rate_pct: f64,
    pub quote_win_rate_pct: f64,
    pub quote_adjusted_pnl_after_priority_fee_sol: f64,
    pub clears_skip_rate_blocker: bool,
    pub pnl_positive: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionCanaryQuoteBucketSummary {
    pub bucket: String,
    pub trades: u64,
    pub shadow_pnl_sol: f64,
    pub quote_adjusted_pnl_after_priority_fee_sol: f64,
    pub buy_slippage_bps_avg: f64,
    pub sell_slippage_bps_avg: f64,
    pub entry_decision_delay_ms_avg: f64,
    pub exit_decision_delay_ms_avg: f64,
    pub buy_leader_notional_sol_avg: f64,
    pub top_routes: Vec<ExecutionCanaryQuoteRouteCount>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionCanaryQuoteRouteCount {
    pub side: String,
    pub label: String,
    pub events: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionCanaryQuoteStatusCount {
    pub side: String,
    pub status: String,
    pub events: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionQuoteCanaryProviderComparisonSummary {
    pub as_of: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub limit: u32,
    pub total_events: u64,
    pub paired_events: u64,
    pub both_ok_events: u64,
    pub generic_only_ok_events: u64,
    pub pump_fun_only_ok_events: u64,
    pub both_error_events: u64,
    pub pump_fun_better_slippage_events: u64,
    pub generic_better_slippage_events: u64,
    pub equal_slippage_events: u64,
    pub avg_generic_latency_ms: f64,
    pub avg_pump_fun_latency_ms: f64,
    pub avg_generic_slippage_bps: f64,
    pub avg_pump_fun_slippage_bps: f64,
    pub avg_pump_fun_minus_generic_slippage_bps: f64,
    pub latest: Vec<ExecutionQuoteCanaryProviderComparisonEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionQuoteCanaryProviderComparisonEvent {
    pub event_id: String,
    pub side: String,
    pub token: String,
    pub request_ts: DateTime<Utc>,
    pub generic_status: Option<String>,
    pub pump_fun_status: Option<String>,
    pub generic_latency_ms: Option<u64>,
    pub pump_fun_latency_ms: Option<u64>,
    pub generic_slippage_bps: Option<f64>,
    pub pump_fun_slippage_bps: Option<f64>,
    pub slippage_delta_bps: Option<f64>,
    pub latency_delta_ms: Option<i64>,
    pub generic_quote_price_sol: Option<f64>,
    pub pump_fun_quote_price_sol: Option<f64>,
    pub shadow_price_sol: Option<f64>,
    pub better_provider: Option<String>,
    pub generic_error: Option<String>,
    pub pump_fun_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionQuoteCanaryPublicPaidComparisonSummary {
    pub as_of: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub limit: u32,
    pub total_events: u64,
    pub paired_events: u64,
    pub both_ok_events: u64,
    pub public_only_ok_events: u64,
    pub paid_only_ok_events: u64,
    pub both_error_events: u64,
    pub paid_better_slippage_events: u64,
    pub public_better_slippage_events: u64,
    pub equal_slippage_events: u64,
    pub avg_public_latency_ms: f64,
    pub avg_paid_latency_ms: f64,
    pub avg_public_slippage_bps: f64,
    pub avg_paid_slippage_bps: f64,
    pub avg_paid_minus_public_slippage_bps: f64,
    pub latest: Vec<ExecutionQuoteCanaryPublicPaidComparisonEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionQuoteCanaryPublicPaidComparisonEvent {
    pub event_id: String,
    pub side: String,
    pub token: String,
    pub request_ts: DateTime<Utc>,
    pub public_status: Option<String>,
    pub paid_status: Option<String>,
    pub public_latency_ms: Option<u64>,
    pub paid_latency_ms: Option<u64>,
    pub public_slippage_bps: Option<f64>,
    pub paid_slippage_bps: Option<f64>,
    pub slippage_delta_bps: Option<f64>,
    pub latency_delta_ms: Option<i64>,
    pub public_quote_price_sol: Option<f64>,
    pub paid_quote_price_sol: Option<f64>,
    pub shadow_price_sol: Option<f64>,
    pub better_provider: Option<String>,
    pub public_error: Option<String>,
    pub paid_error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionCanaryQuoteReadinessGate {
    pub status: String,
    pub can_start_tiny_execution: bool,
    pub blocker_count: u64,
    pub warning_count: u64,
    pub min_market_closed_trades: u64,
    pub market_closed_trades: u64,
    pub sampled_market_trades: u64,
    pub open_position_count: u64,
    pub quote_after_fee_pnl_sol: f64,
    pub quote_win_rate_pct: f64,
    pub skip_rate_pct: f64,
    pub unknown_rate_pct: f64,
    pub non_ok_priority_fee_rate_pct: f64,
    pub avg_entry_quote_latency_ms: f64,
    pub avg_entry_decision_delay_ms: f64,
    pub threshold_candidate: Option<ExecutionCanaryQuoteThresholdCandidate>,
    pub checks: Vec<ExecutionCanaryQuoteReadinessCheck>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionCanaryQuoteReadinessCheck {
    pub name: String,
    pub status: String,
    pub value: String,
    pub threshold: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionCanaryQuotePnlTrade {
    pub shadow_closed_trade_id: i64,
    pub signal_id: String,
    pub wallet_id: String,
    pub token: String,
    pub opened_ts: DateTime<Utc>,
    pub closed_ts: DateTime<Utc>,
    pub status: String,
    pub reason: String,
    pub shadow_pnl_sol: f64,
    pub quote_adjusted_pnl_sol: Option<f64>,
    pub quote_adjusted_pnl_after_priority_fee_sol: Option<f64>,
    pub quote_vs_shadow_delta_sol: Option<f64>,
    pub quote_after_fee_vs_shadow_delta_sol: Option<f64>,
    pub skipped_counterfactual_pnl_sol: Option<f64>,
    pub skipped_counterfactual_pnl_after_priority_fee_sol: Option<f64>,
    pub skipped_counterfactual_after_fee_vs_shadow_delta_sol: Option<f64>,
    pub entry_quote_event_id: Option<String>,
    pub exit_quote_event_id: Option<String>,
    pub entry_decision_status: Option<String>,
    pub exit_decision_status: Option<String>,
    pub entry_quote_status: Option<String>,
    pub exit_quote_status: Option<String>,
    pub entry_priority_fee_status: Option<String>,
    pub exit_priority_fee_status: Option<String>,
    pub entry_cost_sol: Option<f64>,
    pub exit_quote_sol: Option<f64>,
    pub closed_qty_ratio: Option<f64>,
    pub buy_leader_notional_sol: Option<f64>,
    pub sell_leader_notional_sol: Option<f64>,
    pub buy_slippage_bps: Option<f64>,
    pub sell_slippage_bps: Option<f64>,
    pub buy_price_impact_pct: Option<f64>,
    pub sell_price_impact_pct: Option<f64>,
    pub entry_decision_delay_ms: Option<u64>,
    pub exit_decision_delay_ms: Option<u64>,
    pub entry_quote_latency_ms: Option<u64>,
    pub exit_quote_latency_ms: Option<u64>,
    pub entry_quote_price_sol: Option<f64>,
    pub exit_quote_price_sol: Option<f64>,
    pub entry_shadow_price_sol: Option<f64>,
    pub exit_shadow_price_sol: Option<f64>,
    pub entry_route_labels: Vec<String>,
    pub exit_route_labels: Vec<String>,
    pub priority_fee_lamports_total: Option<u64>,
}

pub const EXECUTION_CANARY_QUOTE_PNL_STATUS_COUNTED: &str = "pnl_counted";
pub const EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED: &str = "would_skip";
pub const EXECUTION_CANARY_QUOTE_PNL_STATUS_UNKNOWN: &str = "unknown";
