pub(crate) use anyhow::{Context, Result};
pub(crate) use chrono::{DateTime, Utc};
pub use copybot_storage_core::{HistoryRetentionCutoffs, HistoryRetentionSummary};
pub(crate) use rusqlite::params;

pub(crate) const TERMINAL_EXECUTION_STATUSES: [&str; 3] = [
    "execution_confirmed",
    "execution_failed",
    "execution_dropped",
];
pub(crate) const HISTORY_RETENTION_RISK_EVENTS_BATCH_SIZE: usize = 500;
pub(crate) const HISTORY_RETENTION_EXECUTION_ORDER_BATCH_SIZE: usize = 250;
pub(crate) const HISTORY_RETENTION_COPY_SIGNALS_BATCH_SIZE: usize = 250;
pub(crate) const HISTORY_RETENTION_SHADOW_CLOSED_TRADES_BATCH_SIZE: usize = 500;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ExecutionHistoryRetentionSummary {
    pub(crate) fills_deleted: u64,
    pub(crate) orders_deleted: u64,
    pub(crate) copy_signals_deleted: u64,
    pub(crate) order_batches: usize,
    pub(crate) copy_signal_batches: usize,
    pub(crate) orders_completed_full_sweep: bool,
    pub(crate) copy_signals_completed_full_sweep: bool,
}
