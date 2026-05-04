use super::SqliteStore;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

const TERMINAL_EXECUTION_STATUSES: [&str; 3] = [
    "execution_confirmed",
    "execution_failed",
    "execution_dropped",
];
pub(crate) const HISTORY_RETENTION_RISK_EVENTS_BATCH_SIZE: usize = 500;
pub(crate) const HISTORY_RETENTION_EXECUTION_ORDER_BATCH_SIZE: usize = 250;
pub(crate) const HISTORY_RETENTION_COPY_SIGNALS_BATCH_SIZE: usize = 250;
pub(crate) const HISTORY_RETENTION_SHADOW_CLOSED_TRADES_BATCH_SIZE: usize = 500;

#[derive(Debug, Clone, Copy)]
pub struct HistoryRetentionCutoffs {
    pub risk_events_before: DateTime<Utc>,
    pub copy_signals_before: DateTime<Utc>,
    pub orders_before: DateTime<Utc>,
    pub shadow_closed_trades_before: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HistoryRetentionSummary {
    pub risk_events_deleted: u64,
    pub copy_signals_deleted: u64,
    pub orders_deleted: u64,
    pub fills_deleted: u64,
    pub shadow_closed_trades_deleted: u64,
    pub risk_events_batches: usize,
    pub execution_order_batches: usize,
    pub copy_signals_batches: usize,
    pub shadow_closed_trades_batches: usize,
    pub completed_full_sweep: bool,
}

impl HistoryRetentionSummary {
    pub fn is_empty(&self) -> bool {
        self.risk_events_deleted == 0
            && self.copy_signals_deleted == 0
            && self.orders_deleted == 0
            && self.fills_deleted == 0
            && self.shadow_closed_trades_deleted == 0
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ExecutionHistoryRetentionSummary {
    fills_deleted: u64,
    orders_deleted: u64,
    copy_signals_deleted: u64,
    order_batches: usize,
    copy_signal_batches: usize,
    orders_completed_full_sweep: bool,
    copy_signals_completed_full_sweep: bool,
}
