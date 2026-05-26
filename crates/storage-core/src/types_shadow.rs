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
    pub worst_trade_entry_cost_sol: f64,
    pub worst_trade_pnl_sol: f64,
    pub worst_trade_roi: Option<f64>,
    pub worst_fast_loss_entry_cost_sol: f64,
    pub worst_fast_loss_pnl_sol: f64,
    pub worst_fast_loss_roi: Option<f64>,
    pub worst_fast_loss_hold_seconds: i64,
    pub worst_stale_priced_loss_entry_cost_sol: f64,
    pub worst_stale_priced_loss_pnl_sol: f64,
    pub worst_stale_priced_loss_roi: Option<f64>,
    pub worst_stale_priced_loss_hold_seconds: i64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ShadowSignalSummary {
    pub buy_signals: u64,
    pub sell_signals_total: u64,
    pub sell_signals_matched: u64,
    pub sell_signals_no_position: u64,
    pub closed_trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub pnl_sol: f64,
    pub entry_cost_sol: f64,
    pub avg_hold_seconds: Option<f64>,
    pub open_lots: u64,
    pub open_notional_sol: f64,
}

impl ShadowSignalSummary {
    pub fn roi(&self) -> Option<f64> {
        if self.entry_cost_sol > 0.0 {
            Some(self.pnl_sol / self.entry_cost_sol)
        } else {
            None
        }
    }
}

impl ShadowWalletFeedback {
    pub fn roi(&self) -> Option<f64> {
        if self.entry_cost_sol > 0.0 {
            Some(self.pnl_sol / self.entry_cost_sol)
        } else {
            None
        }
    }

    pub fn record_risk_trade(&mut self, entry_cost_sol: f64, pnl_sol: f64) {
        self.closed_trades = self.closed_trades.saturating_add(1);
        self.entry_cost_sol += entry_cost_sol;
        self.pnl_sol += pnl_sol;
        self.record_worst_trade(entry_cost_sol, pnl_sol);
    }

    pub fn record_fast_loss(&mut self, entry_cost_sol: f64, pnl_sol: f64, hold_seconds: i64) {
        let Some(roi) = roi(entry_cost_sol, pnl_sol) else {
            return;
        };
        if self.worst_fast_loss_roi.is_none_or(|worst| roi < worst) {
            self.worst_fast_loss_entry_cost_sol = entry_cost_sol;
            self.worst_fast_loss_pnl_sol = pnl_sol;
            self.worst_fast_loss_roi = Some(roi);
            self.worst_fast_loss_hold_seconds = hold_seconds;
        }
    }

    pub fn record_stale_priced_loss(
        &mut self,
        entry_cost_sol: f64,
        pnl_sol: f64,
        hold_seconds: i64,
    ) {
        let Some(roi) = roi(entry_cost_sol, pnl_sol) else {
            return;
        };
        if self
            .worst_stale_priced_loss_roi
            .is_none_or(|worst| roi < worst)
        {
            self.worst_stale_priced_loss_entry_cost_sol = entry_cost_sol;
            self.worst_stale_priced_loss_pnl_sol = pnl_sol;
            self.worst_stale_priced_loss_roi = Some(roi);
            self.worst_stale_priced_loss_hold_seconds = hold_seconds;
        }
    }

    fn record_worst_trade(&mut self, entry_cost_sol: f64, pnl_sol: f64) {
        let Some(roi) = roi(entry_cost_sol, pnl_sol) else {
            return;
        };
        if self.worst_trade_roi.is_none_or(|worst| roi < worst) {
            self.worst_trade_entry_cost_sol = entry_cost_sol;
            self.worst_trade_pnl_sol = pnl_sol;
            self.worst_trade_roi = Some(roi);
        }
    }
}

fn roi(entry_cost_sol: f64, pnl_sol: f64) -> Option<f64> {
    (entry_cost_sol > 0.0).then_some(pnl_sol / entry_cost_sol)
}

#[derive(Debug, Clone)]
pub struct ShadowWalletTokenFastLossCooldown {
    pub wallet_id: String,
    pub token: String,
    pub entry_cost_sol: f64,
    pub pnl_sol: f64,
    pub roi: f64,
    pub hold_seconds: i64,
    pub closed_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct ShadowTokenLossCooldown {
    pub token: String,
    pub loss_count: u64,
    pub catastrophe_count: u64,
    pub sampled_trades: u64,
    pub entry_cost_sol: f64,
    pub catastrophe_entry_cost_sol: f64,
    pub pnl_sol: f64,
    pub worst_roi: Option<f64>,
    pub catastrophe_worst_roi: Option<f64>,
    pub last_closed_ts: Option<DateTime<Utc>>,
}

impl ShadowTokenLossCooldown {
    pub fn aggregate_roi(&self) -> Option<f64> {
        if self.entry_cost_sol > 0.0 {
            Some(self.pnl_sol / self.entry_cost_sol)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShadowTokenRecentClose {
    pub token: String,
    pub entry_cost_sol: f64,
    pub pnl_sol: f64,
    pub roi: Option<f64>,
    pub closed_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct TokenMarketStats {
    pub first_seen: Option<DateTime<Utc>>,
    pub holders_proxy: u64,
    pub liquidity_sol_proxy: f64,
    pub volume_5m_sol: f64,
    pub unique_traders_5m: u64,
}
