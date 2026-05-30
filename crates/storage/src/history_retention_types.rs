pub(crate) use anyhow::{Context, Result};
pub(crate) use chrono::{DateTime, Utc};
pub(crate) use copybot_storage_core::ExecutionHistoryRetentionSummary;
pub use copybot_storage_core::{HistoryRetentionCutoffs, HistoryRetentionSummary};
pub(crate) use rusqlite::params;

pub(crate) const TERMINAL_EXECUTION_STATUSES: [&str; 4] = [
    "execution_confirmed",
    "execution_dry_run_confirmed",
    "execution_failed",
    "execution_dropped",
];
pub(crate) const HISTORY_RETENTION_RISK_EVENTS_BATCH_SIZE: usize = 500;
pub(crate) const HISTORY_RETENTION_EXECUTION_ORDER_BATCH_SIZE: usize = 250;
pub(crate) const HISTORY_RETENTION_COPY_SIGNALS_BATCH_SIZE: usize = 250;
pub(crate) const HISTORY_RETENTION_SHADOW_CLOSED_TRADES_BATCH_SIZE: usize = 500;
