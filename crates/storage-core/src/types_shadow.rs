pub const STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES: i64 = 30;
pub const STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL: f64 = 0.05;
pub const STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES: usize = 3;
pub const STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES: usize = 60;

pub const SHADOW_CLOSE_CONTEXT_MARKET: &str = "market";
pub const SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE: &str = "stale_terminal_zero_price";
pub const SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE: &str = "recovery_terminal_zero_price";
pub const SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY: &str = "quarantined_legacy";

pub const SHADOW_RISK_CONTEXT_MARKET: &str = "market";
pub const SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY: &str = "quarantined_legacy";

use chrono::{DateTime, Utc};
use copybot_core_types::{Lamports, TokenQuantity};

pub const SHADOW_LOT_OPEN_EPS: f64 = 1e-12;

pub const POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER: &str = "legacy_pre_cutover";
pub const POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER: &str = "exact_post_cutover";

#[derive(Debug, Clone)]
pub struct ShadowLotRow {
    pub id: i64,
    pub wallet_id: String,
    pub token: String,
    pub accounting_bucket: String,
    pub risk_context: String,
    pub qty: f64,
    pub qty_exact: Option<TokenQuantity>,
    pub cost_sol: f64,
    pub cost_lamports: Option<Lamports>,
    pub opened_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ShadowCloseOutcome {
    pub closed_qty: f64,
    pub realized_pnl_sol: f64,
    pub has_open_lots_after: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ShadowWalletFeedback {
    pub closed_trades: u64,
    pub entry_cost_sol: f64,
    pub pnl_sol: f64,
}

impl ShadowWalletFeedback {
    pub fn roi(&self) -> Option<f64> {
        if self.entry_cost_sol > 0.0 {
            Some(self.pnl_sol / self.entry_cost_sol)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TokenMarketStats {
    pub first_seen: Option<DateTime<Utc>>,
    pub holders_proxy: u64,
    pub liquidity_sol_proxy: f64,
    pub volume_5m_sol: f64,
    pub unique_traders_5m: u64,
}
