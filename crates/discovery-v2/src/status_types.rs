use crate::filters::DiscoveryV2FilterStatus;
use crate::metric::DiscoveryV2WalletMetric;
use chrono::{DateTime, Utc};
use copybot_storage_core::DiscoveryRuntimeCursor;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2Status {
    pub source: String,
    pub now: DateTime<Utc>,
    pub window_start: DateTime<Utc>,
    pub window_minutes: u64,
    pub max_tail_lag_seconds: u64,
    pub tail: Option<DiscoveryV2TailStatus>,
    pub coverage_sample: Option<DiscoveryV2CoverageSample>,
    pub scan: DiscoveryV2ScanStatus,
    pub filters: DiscoveryV2FilterStatus,
    pub wallet_metrics: Vec<DiscoveryV2WalletMetric>,
    pub candidate_wallets: Vec<String>,
    pub execution_enabled: bool,
    pub execution_disabled: bool,
    pub blockers: Vec<String>,
    pub production_green: bool,
    pub policy_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2TailStatus {
    pub cursor: DiscoveryRuntimeCursor,
    pub lag_seconds: i64,
    pub fresh: bool,
    pub future_dated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2CoverageSample {
    pub ts: DateTime<Utc>,
    pub slot: u64,
    pub signature: String,
    pub wallet_id: String,
    pub covers_window_start: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2ScanStatus {
    pub max_rows: usize,
    pub time_budget_ms: u64,
    pub rows_scanned: usize,
    pub unique_wallets: usize,
    pub max_rows_exhausted: bool,
    pub time_budget_exhausted: bool,
    pub budget_exhausted: bool,
}
