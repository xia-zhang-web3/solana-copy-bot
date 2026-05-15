use super::types_runtime::DiscoveryRuntimeCursor;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveryV2QualityEvidenceAggregate {
    pub mint: String,
    pub first_seen: DateTime<Utc>,
    pub max_sol_notional: f64,
    pub buy_count: u64,
    pub sol_trade_count: u64,
    pub wallet_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryV2QualityPrepareState {
    pub covered_from_ts: DateTime<Utc>,
    pub cursor: DiscoveryRuntimeCursor,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveryV2QualityPrepareUpsert {
    pub signature: String,
    pub mint: String,
    pub wallet_id: String,
    pub ts_utc: DateTime<Utc>,
    pub slot: u64,
    pub sol_notional: f64,
    pub is_buy: bool,
}
