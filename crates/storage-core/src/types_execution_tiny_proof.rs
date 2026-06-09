use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionTinyProofReport {
    pub as_of: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub limit: u32,
    pub summary: ExecutionTinyProofSummary,
    pub entry_funnel: ExecutionTinyEntryFunnel,
    pub latency: ExecutionTinyProofLatencySummary,
    pub reason_counts: Vec<ExecutionTinyProofReasonCount>,
    pub order_failure_counts: Vec<ExecutionTinyOrderFailureCount>,
    pub trades: Vec<ExecutionTinyProofTrade>,
    pub recent_orders: Vec<ExecutionTinyProofOrder>,
    pub open_positions: Vec<ExecutionTinyProofOpenPosition>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionTinyProofSummary {
    pub shadow_market_closed_trades: u64,
    pub canary_entry_would_execute_trades: u64,
    pub canary_exit_would_execute_trades: u64,
    pub tiny_entry_ordered_trades: u64,
    pub tiny_entry_confirmed_trades: u64,
    pub tiny_exit_ordered_trades: u64,
    pub tiny_exit_confirmed_trades: u64,
    pub tiny_closed_positions: u64,
    pub tiny_unique_closed_positions: u64,
    pub tiny_open_positions: u64,
    pub shadow_pnl_sol: f64,
    pub tiny_realized_pnl_sol: f64,
    pub tiny_vs_shadow_delta_sol: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionTinyProofLatencySummary {
    pub entry_quote_latency_ms: ExecutionTinyProofLatencyStats,
    pub exit_quote_latency_ms: ExecutionTinyProofLatencyStats,
    pub entry_decision_delay_ms: ExecutionTinyProofLatencyStats,
    pub exit_decision_delay_ms: ExecutionTinyProofLatencyStats,
    pub entry_signal_to_submit_ms: ExecutionTinyProofLatencyStats,
    pub exit_signal_to_submit_ms: ExecutionTinyProofLatencyStats,
    pub entry_quote_to_submit_ms: ExecutionTinyProofLatencyStats,
    pub exit_quote_to_submit_ms: ExecutionTinyProofLatencyStats,
    pub entry_submit_to_confirm_ms: ExecutionTinyProofLatencyStats,
    pub exit_submit_to_confirm_ms: ExecutionTinyProofLatencyStats,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionTinyProofLatencyStats {
    pub samples: u64,
    pub avg_ms: f64,
    pub max_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionTinyProofReasonCount {
    pub stage: String,
    pub reason: String,
    pub trades: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ExecutionTinyEntryFunnel {
    pub total_buy_quote_events: u64,
    pub quote_ok_events: u64,
    pub quote_would_execute_events: u64,
    pub quote_would_skip_events: u64,
    pub shadow_recorded_events: u64,
    pub shadow_dropped_events: u64,
    pub shadow_pending_events: u64,
    pub quote_would_execute_shadow_recorded_events: u64,
    pub quote_would_execute_shadow_dropped_events: u64,
    pub quote_would_execute_shadow_pending_events: u64,
    pub tiny_ordered_events: u64,
    pub tiny_confirmed_events: u64,
    pub tiny_failed_events: u64,
    pub tiny_submit_disabled_events: u64,
    pub tiny_missing_order_events: u64,
    pub tiny_missing_order_shadow_recorded_events: u64,
    pub tiny_missing_order_shadow_dropped_events: u64,
    pub tiny_missing_order_shadow_pending_events: u64,
    pub shadow_drop_reason_counts: Vec<ExecutionTinyEntryFunnelDropReasonCount>,
    pub quote_would_execute_shadow_drop_reason_counts: Vec<ExecutionTinyEntryFunnelDropReasonCount>,
    pub buckets: Vec<ExecutionTinyEntryFunnelBucket>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionTinyEntryFunnelDropReasonCount {
    pub reason: String,
    pub events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionTinyEntryFunnelBucket {
    pub quote_source: String,
    pub quote_status: String,
    pub decision_status: String,
    pub shadow_gate_status: String,
    pub shadow_gate_reason: String,
    pub order_status: String,
    pub err_code: String,
    pub simulation_status: String,
    pub events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionTinyOrderFailureCount {
    pub side: String,
    pub status: String,
    pub err_code: String,
    pub simulation_status: String,
    pub simulation_error_class: String,
    pub decision_reason: String,
    pub route: String,
    pub quote_source: String,
    pub orders: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionTinyProofTrade {
    pub shadow_closed_trade_id: i64,
    pub signal_id: String,
    pub wallet_id: String,
    pub token: String,
    pub opened_ts: DateTime<Utc>,
    pub closed_ts: DateTime<Utc>,
    pub proof_status: String,
    pub proof_stage: String,
    pub proof_reason: String,
    pub shadow_pnl_sol: f64,
    pub tiny_position_id: Option<String>,
    pub tiny_position_state: Option<String>,
    pub tiny_position_opened_ts: Option<DateTime<Utc>>,
    pub tiny_position_closed_ts: Option<DateTime<Utc>>,
    pub tiny_position_cost_sol: Option<f64>,
    pub tiny_realized_pnl_sol: Option<f64>,
    pub tiny_vs_shadow_delta_sol: Option<f64>,
    pub entry_quote_event_id: Option<String>,
    pub entry_quote_status: Option<String>,
    pub entry_decision_status: Option<String>,
    pub entry_decision_reason: Option<String>,
    pub entry_quote_latency_ms: Option<u64>,
    pub entry_decision_delay_ms: Option<u64>,
    pub entry_priority_fee_lamports: Option<u64>,
    pub exit_quote_event_id: Option<String>,
    pub exit_quote_status: Option<String>,
    pub exit_decision_status: Option<String>,
    pub exit_decision_reason: Option<String>,
    pub exit_quote_latency_ms: Option<u64>,
    pub exit_decision_delay_ms: Option<u64>,
    pub exit_priority_fee_lamports: Option<u64>,
    pub tiny_buy_order: Option<ExecutionTinyProofOrder>,
    pub tiny_sell_order: Option<ExecutionTinyProofOrder>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionTinyProofOrder {
    pub order_id: String,
    pub signal_id: String,
    pub side: Option<String>,
    pub token: Option<String>,
    pub route: String,
    pub status: String,
    pub err_code: Option<String>,
    pub attempt: u32,
    pub submit_ts: DateTime<Utc>,
    pub confirm_ts: Option<DateTime<Utc>>,
    pub tx_signature_present: bool,
    pub simulation_status: Option<String>,
    pub simulation_error: Option<String>,
    pub submit_to_confirm_ms: Option<i64>,
    pub signal_to_submit_ms: Option<i64>,
    pub quote_to_submit_ms: Option<i64>,
    pub quote_source: Option<String>,
    pub quote_event_id: Option<String>,
    pub priority_fee_lamports: Option<u64>,
    pub decision_status: Option<String>,
    pub decision_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionTinyProofOpenPosition {
    pub position_id: String,
    pub token: String,
    pub qty: f64,
    pub cost_sol: f64,
    pub opened_ts: DateTime<Utc>,
}
