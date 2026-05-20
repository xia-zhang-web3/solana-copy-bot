use chrono::{DateTime, NaiveDate, Utc};
use copybot_core_types::SwapEvent;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedSwapBatchWriteMetrics {
    pub inserted: Vec<bool>,
    pub observed_swaps_insert_ms: u64,
    pub wallet_activity_days_upsert_ms: u64,
}

#[derive(Debug, Clone)]
pub struct WalletActivityDayRow {
    pub wallet_id: String,
    pub activity_day: NaiveDate,
    pub last_seen: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalletSolLegActivityWindow {
    pub trades: u32,
    pub active_days: u32,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
    pub time_budget_exhausted: bool,
}

#[derive(Debug, Clone)]
pub struct ObservedSolLegSwap {
    pub signature: String,
    pub wallet_id: String,
    pub is_buy: bool,
    pub token_mint: String,
    pub token_qty: f64,
    pub sol_notional: f64,
    pub slot: u64,
    pub ts_utc: DateTime<Utc>,
}

impl ObservedSolLegSwap {
    pub fn into_swap_event(self) -> SwapEvent {
        let (token_in, token_out, amount_in, amount_out) = if self.is_buy {
            (
                SOL_MINT.to_string(),
                self.token_mint,
                self.sol_notional,
                self.token_qty,
            )
        } else {
            (
                self.token_mint,
                SOL_MINT.to_string(),
                self.token_qty,
                self.sol_notional,
            )
        };
        SwapEvent {
            signature: self.signature,
            wallet: self.wallet_id,
            dex: String::new(),
            token_in,
            token_out,
            amount_in,
            amount_out,
            slot: self.slot,
            ts_utc: self.ts_utc,
            exact_amounts: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqliteBatchedDeleteSummary {
    pub deleted_rows: usize,
    pub batches: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqliteBatchedDeleteSummaryWithCompletion {
    pub deleted_rows: usize,
    pub batches: usize,
    pub completed_full_sweep: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqliteContentionSnapshot {
    pub write_retry_total: u64,
    pub busy_error_total: u64,
}
