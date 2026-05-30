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

pub const EXECUTION_STATUS_DRY_RUN_CONFIRMED: &str = "execution_dry_run_confirmed";
pub const EXECUTION_SIMULATION_STATUS_DRY_RUN_SKIPPED: &str = "dry_run_skipped";
