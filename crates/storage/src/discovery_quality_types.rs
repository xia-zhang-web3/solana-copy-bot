use chrono::{DateTime, Utc};
use std::time::Instant;

#[derive(Debug, Default)]
pub(crate) struct QualityFetchBudget {
    pub(crate) rpc_attempted: usize,
    pub(crate) started_at: Option<Instant>,
}

#[derive(Debug, Clone)]
pub(crate) struct QualityCacheUpsert {
    pub(crate) mint: String,
    pub(crate) holders: Option<u64>,
    pub(crate) liquidity_sol: Option<f64>,
    pub(crate) token_age_seconds: Option<u64>,
    pub(crate) fetched_at: DateTime<Utc>,
}
