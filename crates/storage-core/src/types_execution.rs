use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionDryRunOrder {
    pub order_id: String,
    pub signal_id: String,
    pub route: String,
    pub submit_ts: DateTime<Utc>,
    pub confirm_ts: Option<DateTime<Utc>>,
    pub status: String,
    pub client_order_id: String,
    pub simulation_status: String,
    pub attempt: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionDryRunRecordOutcome {
    Inserted,
    Existing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionCanaryObservedLeg {
    pub signature: String,
    pub wallet_id: String,
    pub is_buy: bool,
    pub token_mint: String,
    pub token_qty: f64,
    pub sol_notional: f64,
    pub token_raw_amount: Option<String>,
    pub token_decimals: Option<u8>,
    pub slot: u64,
    pub ts_utc: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionCanaryCloseCandidate {
    pub id: i64,
    pub signal_id: String,
    pub wallet_id: String,
    pub token: String,
    pub qty: f64,
    pub qty_raw: Option<String>,
    pub qty_decimals: Option<u8>,
    pub exit_value_sol: f64,
    pub closed_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionQuoteCanaryEventInsert {
    pub event_id: String,
    pub signal_id: Option<String>,
    pub shadow_closed_trade_id: Option<i64>,
    pub wallet_id: String,
    pub token: String,
    pub side: String,
    pub quote_status: String,
    pub request_ts: DateTime<Utc>,
    pub signal_ts: Option<DateTime<Utc>>,
    pub decision_delay_ms: Option<u64>,
    pub quote_latency_ms: Option<u64>,
    pub leader_notional_sol: Option<f64>,
    pub quote_in_amount_raw: Option<String>,
    pub quote_out_amount_raw: Option<String>,
    pub quote_price_sol: Option<f64>,
    pub shadow_price_sol: Option<f64>,
    pub slippage_bps: Option<f64>,
    pub price_impact_pct: Option<f64>,
    pub route_plan_json: Option<String>,
    pub priority_fee_status: Option<String>,
    pub priority_fee_lamports: Option<u64>,
    pub priority_fee_json: Option<String>,
    pub decision_status: Option<String>,
    pub decision_reason: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionQuoteCanaryRecordOutcome {
    Inserted,
    Existing,
}

pub const EXECUTION_STATUS_DRY_RUN_CONFIRMED: &str = "execution_dry_run_confirmed";
pub const EXECUTION_SIMULATION_STATUS_DRY_RUN_SKIPPED: &str = "dry_run_skipped";
