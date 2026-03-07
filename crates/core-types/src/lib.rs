use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapEvent {
    pub wallet: String,
    pub dex: String,
    pub token_in: String,
    pub token_out: String,
    pub amount_in: f64,
    pub amount_out: f64,
    pub signature: String,
    pub slot: u64,
    pub ts_utc: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct WalletMetricRow {
    pub wallet_id: String,
    pub window_start: DateTime<Utc>,
    pub pnl: f64,
    pub win_rate: f64,
    pub trades: u32,
    pub closed_trades: u32,
    pub hold_median_seconds: i64,
    pub score: f64,
    pub buy_total: u32,
    pub tradable_ratio: f64,
    pub rug_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct WalletUpsertRow {
    pub wallet_id: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct CopySignalRow {
    pub signal_id: String,
    pub wallet_id: String,
    pub side: String,
    pub token: String,
    pub notional_sol: f64,
    pub ts: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct ExecutionOrderRow {
    pub order_id: String,
    pub signal_id: String,
    pub client_order_id: String,
    pub route: String,
    pub applied_tip_lamports: Option<u64>,
    pub ata_create_rent_lamports: Option<u64>,
    pub network_fee_lamports_hint: Option<u64>,
    pub base_fee_lamports_hint: Option<u64>,
    pub priority_fee_lamports_hint: Option<u64>,
    pub submit_ts: DateTime<Utc>,
    pub confirm_ts: Option<DateTime<Utc>>,
    pub status: String,
    pub err_code: Option<String>,
    pub tx_signature: Option<String>,
    pub simulation_status: Option<String>,
    pub simulation_error: Option<String>,
    pub attempt: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertExecutionOrderPendingOutcome {
    Inserted,
    Duplicate,
}

pub const EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS: &str =
    "execution_submitted_reconcile_pending";

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExecutionConfirmStateSnapshot {
    pub total_exposure_sol: f64,
    pub token_exposure_sol: f64,
    pub open_positions: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FinalizeExecutionConfirmOutcome {
    Applied(ExecutionConfirmStateSnapshot),
    AlreadyConfirmed,
}

#[derive(Debug, Clone)]
pub struct TokenQualityCacheRow {
    pub mint: String,
    pub holders: Option<u64>,
    pub liquidity_sol: Option<f64>,
    pub token_age_seconds: Option<u64>,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct TokenQualityRpcRow {
    pub holders: Option<u64>,
    pub liquidity_sol: Option<f64>,
    pub token_age_seconds: Option<u64>,
}
