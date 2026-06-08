use chrono::{DateTime, Utc};
use copybot_core_types::{Lamports, SignedLamports, TokenQuantity};
use serde::Serialize;

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
pub struct ExecutionCanaryOrder {
    pub order_id: String,
    pub signal_id: String,
    pub route: String,
    pub submit_ts: DateTime<Utc>,
    pub confirm_ts: Option<DateTime<Utc>>,
    pub status: String,
    pub err_code: Option<String>,
    pub client_order_id: String,
    pub tx_signature: Option<String>,
    pub simulation_status: Option<String>,
    pub simulation_error: Option<String>,
    pub attempt: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionCanaryRecordOutcome {
    Inserted,
    Existing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionCanaryReserveResult {
    pub outcome: ExecutionCanaryRecordOutcome,
    pub order: ExecutionCanaryOrder,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionCanaryStatusReport {
    pub as_of: DateTime<Utc>,
    pub total: u64,
    pub candidate: u64,
    pub built: u64,
    pub simulated: u64,
    pub submitted: u64,
    pub confirmed: u64,
    pub failed: u64,
    pub expired: u64,
    pub submit_disabled: u64,
    pub other: u64,
    pub latest_order: Option<ExecutionCanaryOrder>,
    pub latest_error_order: Option<ExecutionCanaryOrder>,
    pub latest_build_plan_metadata: Option<ExecutionCanaryBuildPlanMetadata>,
}

impl ExecutionCanaryStatusReport {
    pub fn active_count(&self) -> u64 {
        self.candidate + self.built + self.simulated + self.submitted
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionCanaryOwnedPosition {
    pub position_id: String,
    pub token: String,
    pub accounting_bucket: String,
    pub qty: f64,
    pub qty_exact: Option<TokenQuantity>,
    pub cost_sol: f64,
    pub cost_lamports: Option<Lamports>,
    pub opened_ts: DateTime<Utc>,
    pub state: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionCanaryPositionRecordOutcome {
    Inserted,
    Existing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionCanaryOwnedPositionRecordResult {
    pub outcome: ExecutionCanaryPositionRecordOutcome,
    pub position: ExecutionCanaryOwnedPosition,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionCanaryPositionCloseResult {
    pub close_status: String,
    pub position_id: Option<String>,
    pub token: String,
    pub closed_qty: f64,
    pub closed_qty_exact: Option<TokenQuantity>,
    pub remaining_qty: f64,
    pub remaining_qty_exact: Option<TokenQuantity>,
    pub entry_cost_sol: f64,
    pub exit_value_sol: f64,
    pub pnl_sol: f64,
    pub entry_cost_lamports: Option<Lamports>,
    pub exit_value_lamports: Option<Lamports>,
    pub pnl_lamports: Option<SignedLamports>,
    pub remaining_position: Option<ExecutionCanaryOwnedPosition>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionCanarySellDecision {
    pub decision_status: String,
    pub decision_reason: String,
    pub position: Option<ExecutionCanaryOwnedPosition>,
    pub slippage_bps: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionCanaryConfirmTimeoutDecision {
    pub decision_status: String,
    pub decision_reason: String,
    pub order: ExecutionCanaryOrder,
    pub elapsed_seconds: i64,
    pub timeout_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionCanaryBuildPlanMetadata {
    pub order_id: String,
    pub signal_id: String,
    pub client_order_id: String,
    pub recorded_ts: DateTime<Utc>,
    pub quote_source: Option<String>,
    pub quote_event_id: Option<String>,
    pub quote_status: Option<String>,
    pub quote_in_amount_raw: Option<String>,
    pub quote_out_amount_raw: Option<String>,
    pub quote_response_json: Option<String>,
    pub quote_price_sol: Option<f64>,
    pub price_impact_pct: Option<f64>,
    pub route_plan_json: Option<String>,
    pub priority_fee_source: Option<String>,
    pub priority_fee_status: Option<String>,
    pub priority_fee_lamports: Option<u64>,
    pub priority_fee_json: Option<String>,
    pub slippage_bps: Option<f64>,
    pub decision_status: Option<String>,
    pub decision_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionCanaryBuildPlanMetadataRecordOutcome {
    Inserted,
    Existing,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionCanaryReadinessSummary {
    pub as_of: DateTime<Utc>,
    pub readiness_status: String,
    pub readiness_reason: String,
    pub total_orders: u64,
    pub active_orders: u64,
    pub failed_orders: u64,
    pub submit_disabled_orders: u64,
    pub latest: Option<ExecutionCanaryReadinessLatestOrder>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionCanaryReadinessWindowSummary {
    pub as_of: DateTime<Utc>,
    pub limit: u32,
    pub total_orders: u64,
    pub metadata_orders: u64,
    pub missing_metadata_orders: u64,
    pub would_enter_orders: u64,
    pub would_skip_orders: u64,
    pub unknown_orders: u64,
    pub latest_metadata_age_seconds: Option<i64>,
    pub latest_route: Option<String>,
    pub latest_price_impact_pct: Option<f64>,
    pub latest_priority_fee_lamports: Option<u64>,
    pub decision_reasons: Vec<ExecutionCanaryReadinessCount>,
    pub provider_errors: Vec<ExecutionCanaryReadinessCount>,
    pub routes: Vec<ExecutionCanaryReadinessCount>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionCanaryReadinessCount {
    pub key: String,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionCanaryReadinessLatestOrder {
    pub order_id: String,
    pub signal_id: String,
    pub client_order_id: String,
    pub order_status: String,
    pub route: String,
    pub submit_ts: DateTime<Utc>,
    pub metadata_recorded_ts: Option<DateTime<Utc>>,
    pub quote_source: Option<String>,
    pub quote_event_id: Option<String>,
    pub quote_status: Option<String>,
    pub quote_in_amount_raw: Option<String>,
    pub quote_out_amount_raw: Option<String>,
    pub quote_price_sol: Option<f64>,
    pub slippage_bps: Option<f64>,
    pub price_impact_pct: Option<f64>,
    pub route_plan_json: Option<String>,
    pub priority_fee_source: Option<String>,
    pub priority_fee_status: Option<String>,
    pub priority_fee_lamports: Option<u64>,
    pub decision_status: Option<String>,
    pub decision_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionCanarySubmitRiskSummary {
    pub as_of: DateTime<Utc>,
    pub max_submit_attempts: u32,
    pub active_orders: u64,
    pub submitted_orders: u64,
    pub submitted_with_signature_orders: u64,
    pub submitted_without_signature_orders: u64,
    pub retry_ready_orders: u64,
    pub retry_budget_blocked_orders: u64,
    pub max_active_attempt: u32,
    pub latest_active_order: Option<ExecutionCanarySubmitRiskOrder>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionCanarySubmitRiskOrder {
    pub order_id: String,
    pub status: String,
    pub attempt: u32,
    pub tx_signature_present: bool,
    pub simulation_error: Option<String>,
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
    pub quote_response_json: Option<String>,
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

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionQuoteCanaryProviderSampleInsert {
    pub event_id: String,
    pub provider: String,
    pub side: String,
    pub quote_status: String,
    pub request_ts: DateTime<Utc>,
    pub quote_latency_ms: Option<u64>,
    pub quote_in_amount_raw: Option<String>,
    pub quote_out_amount_raw: Option<String>,
    pub quote_response_json: Option<String>,
    pub quote_price_sol: Option<f64>,
    pub shadow_price_sol: Option<f64>,
    pub slippage_bps: Option<f64>,
    pub price_impact_pct: Option<f64>,
    pub route_plan_json: Option<String>,
    pub decision_status: Option<String>,
    pub decision_reason: Option<String>,
    pub error: Option<String>,
}

pub const EXECUTION_STATUS_DRY_RUN_CONFIRMED: &str = "execution_dry_run_confirmed";
pub const EXECUTION_SIMULATION_STATUS_DRY_RUN_SKIPPED: &str = "dry_run_skipped";
pub const EXECUTION_STATUS_CANARY_CANDIDATE: &str = "execution_canary_candidate";
pub const EXECUTION_STATUS_CANARY_BUILT: &str = "execution_canary_built";
pub const EXECUTION_STATUS_CANARY_SIMULATED: &str = "execution_canary_simulated";
pub const EXECUTION_STATUS_CANARY_SUBMITTED: &str = "execution_canary_submitted";
pub const EXECUTION_STATUS_CANARY_CONFIRMED: &str = "execution_canary_confirmed";
pub const EXECUTION_STATUS_CANARY_FAILED: &str = "execution_canary_failed";
pub const EXECUTION_STATUS_CANARY_EXPIRED: &str = "execution_canary_expired";
pub const EXECUTION_STATUS_CANARY_SUBMIT_DISABLED: &str = "execution_canary_submit_disabled";
pub const EXECUTION_SIMULATION_STATUS_NOT_RUN: &str = "not_run";
pub const EXECUTION_SIMULATION_STATUS_PASSED: &str = "passed";
pub const EXECUTION_SIMULATION_STATUS_FAILED: &str = "failed";
pub const EXECUTION_SIMULATION_STATUS_SKIPPED_NO_SUBMIT: &str = "skipped_no_submit";
pub const EXECUTION_ERROR_BUILD_FAILED: &str = "build_failed";
pub const EXECUTION_ERROR_SIMULATION_FAILED: &str = "simulation_failed";
pub const EXECUTION_ERROR_SUBMIT_PLAN_FAILED: &str = "submit_plan_failed";
pub const EXECUTION_ERROR_SIGNING_ENVELOPE_FAILED: &str = "signing_envelope_failed";
pub const EXECUTION_ERROR_EXPIRED: &str = "expired";
pub const EXECUTION_ERROR_SUBMIT_DISABLED: &str = "submit_disabled";
pub const EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET: &str = "execution_canary";
pub const EXECUTION_CANARY_POSITION_STATE_OPEN: &str = "open";
pub const EXECUTION_CANARY_POSITION_STATE_CLOSED: &str = "closed";
pub const EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION: &str = "no_position";
pub const EXECUTION_CANARY_POSITION_CLOSE_PARTIAL: &str = "partial";
pub const EXECUTION_CANARY_POSITION_CLOSE_CLOSED: &str = "closed";
pub const EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED: &str = "dust_closed";
pub const EXECUTION_CANARY_SELL_DECISION_NO_POSITION: &str = "no_position";
pub const EXECUTION_CANARY_SELL_DECISION_EXECUTE: &str = "execute";
pub const EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT: &str = "force_exit";
pub const EXECUTION_CANARY_CONFIRM_DECISION_NOT_SUBMITTED: &str = "not_submitted";
pub const EXECUTION_CANARY_CONFIRM_DECISION_WAIT: &str = "wait";
pub const EXECUTION_CANARY_CONFIRM_DECISION_RETRY: &str = "retry";
pub const EXECUTION_CANARY_CONFIRM_DECISION_EXPIRE_UNSAFE: &str = "expire_without_retry";
