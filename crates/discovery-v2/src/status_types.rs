use crate::filters::DiscoveryV2FilterStatus;
use crate::live_portfolio::DiscoveryV2LivePortfolioStatus;
use crate::metric::DiscoveryV2WalletMetric;
use chrono::{DateTime, Utc};
use copybot_storage_core::DiscoveryRuntimeCursor;
use serde::{Deserialize, Serialize};

pub const OPERATOR_WALLET_METRIC_LIMIT: usize = 250;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2Status {
    pub source: String,
    pub now: DateTime<Utc>,
    pub build_elapsed_ms: u64,
    #[serde(default)]
    pub build_timing: DiscoveryV2BuildTiming,
    pub window_start: DateTime<Utc>,
    pub window_minutes: u64,
    pub max_tail_lag_seconds: u64,
    pub tail: Option<DiscoveryV2TailStatus>,
    pub coverage_sample: Option<DiscoveryV2CoverageSample>,
    pub scan: DiscoveryV2ScanStatus,
    pub maturity: DiscoveryV2MaturityStatus,
    pub live_portfolio: Option<DiscoveryV2LivePortfolioStatus>,
    #[serde(default)]
    pub shadow_signals_24h: Option<DiscoveryV2ShadowSignalStatus>,
    pub filters: DiscoveryV2FilterStatus,
    pub wallet_metrics_total: usize,
    pub wallet_metrics_returned: usize,
    pub wallet_metrics_truncated: bool,
    pub wallet_metrics: Vec<DiscoveryV2WalletMetric>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rug_quarantine_candidates: Vec<DiscoveryV2RugQuarantineCandidate>,
    pub candidate_wallets: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidate_wallet_sources: Vec<DiscoveryV2CandidateWalletSource>,
    pub execution_enabled: bool,
    pub execution_disabled: bool,
    pub blockers: Vec<String>,
    pub production_green: bool,
    pub policy_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryV2CandidateWalletSource {
    pub wallet_id: String,
    pub source_cohort: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryV2RugQuarantineCandidate {
    pub wallet_id: String,
    pub closed_trades: Option<u32>,
    pub stale_terminal_closes: Option<u32>,
    pub stale_terminal_rate: Option<f64>,
    pub stale_terminal_pnl_sol: Option<f64>,
}

impl DiscoveryV2Status {
    pub fn bounded_operator_wallet_metrics(mut self) -> Self {
        let total = self.wallet_metrics_total.max(self.wallet_metrics.len());
        if self.wallet_metrics.len() > OPERATOR_WALLET_METRIC_LIMIT {
            self.wallet_metrics.truncate(OPERATOR_WALLET_METRIC_LIMIT);
        }
        self.wallet_metrics_total = total;
        self.wallet_metrics_returned = self.wallet_metrics.len();
        self.wallet_metrics_truncated = self.wallet_metrics_returned < total;
        self.rug_quarantine_candidates.clear();
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2ShadowSignalStatus {
    pub since: DateTime<Utc>,
    pub buy_signals: u64,
    pub sell_signals_total: u64,
    pub sell_signals_matched: u64,
    pub sell_signals_no_position: u64,
    pub closed_trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub pnl_sol: f64,
    pub entry_cost_sol: f64,
    pub roi: Option<f64>,
    pub avg_hold_seconds: Option<f64>,
    pub open_lots: u64,
    pub open_notional_sol: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryV2BuildTiming {
    pub tail_elapsed_ms: u64,
    pub coverage_elapsed_ms: u64,
    pub scan_elapsed_ms: u64,
    pub metric_elapsed_ms: u64,
    pub maturity_elapsed_ms: u64,
    pub live_portfolio_elapsed_ms: u64,
    pub total_elapsed_ms: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2MaturityStatus {
    pub enabled: bool,
    pub window_days: u32,
    pub min_active_days: u32,
    pub score_bonus: f64,
    pub evaluated_wallets: usize,
    pub preferred_wallets: usize,
    #[serde(default)]
    pub selected_primary_wallets: usize,
    #[serde(default)]
    pub selected_secondary_wallets: usize,
    #[serde(default)]
    pub selected_emergency_wallets: usize,
    pub time_budget_exhausted: bool,
}
