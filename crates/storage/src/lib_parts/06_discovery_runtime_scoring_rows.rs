use super::*;

#[derive(Debug, Clone, Default)]
pub struct DiscoveryAggregateWriteConfig {
    pub max_tx_per_minute: u32,
    pub rug_lookahead_seconds: u32,
    pub helius_http_url: Option<String>,
    pub min_token_age_hint_seconds: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct WalletScoringDayRow {
    pub wallet_id: String,
    pub activity_day: NaiveDate,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub trades: u32,
    pub spent_sol: f64,
    pub max_buy_notional_sol: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalletScoringQualitySource {
    Fresh,
    Stale,
    Deferred,
    Missing,
}

#[derive(Debug, Clone)]
pub struct WalletScoringBuyFactRow {
    pub wallet_id: String,
    pub token: String,
    pub ts: DateTime<Utc>,
    pub notional_sol: f64,
    pub market_volume_5m_sol: f64,
    pub market_unique_traders_5m: u32,
    pub market_liquidity_proxy_sol: f64,
    pub quality_source: WalletScoringQualitySource,
    pub quality_token_age_seconds: Option<u64>,
    pub quality_holders: Option<u64>,
    pub quality_liquidity_sol: Option<f64>,
    pub rug_check_after_ts: DateTime<Utc>,
    pub rug_volume_lookahead_sol: Option<f64>,
    pub rug_unique_traders_lookahead: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct WalletScoringCloseFactRow {
    pub wallet_id: String,
    pub token: String,
    pub closed_ts: DateTime<Utc>,
    pub pnl_sol: f64,
    pub hold_seconds: i64,
    pub win: bool,
}

#[derive(Debug, Clone)]
pub struct WalletScoringSnapshot {
    pub days: Vec<WalletScoringDayRow>,
    pub buy_facts: Vec<WalletScoringBuyFactRow>,
    pub close_facts: Vec<WalletScoringCloseFactRow>,
    pub max_tx_counts: std::collections::HashMap<String, u32>,
}

pub use copybot_storage_core::{ShadowCloseOutcome, ShadowLotRow, TokenMarketStats};
