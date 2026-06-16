use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy)]
pub struct HistoryRetentionCutoffs {
    pub risk_events_before: DateTime<Utc>,
    pub copy_signals_before: DateTime<Utc>,
    pub orders_before: DateTime<Utc>,
    pub shadow_closed_trades_before: DateTime<Utc>,
    pub execution_quote_canary_before: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HistoryRetentionSummary {
    pub risk_events_deleted: u64,
    pub copy_signals_deleted: u64,
    pub orders_deleted: u64,
    pub fills_deleted: u64,
    pub shadow_closed_trades_deleted: u64,
    pub execution_quote_canary_events_deleted: u64,
    pub execution_quote_canary_provider_samples_deleted: u64,
    pub execution_quote_canary_shadow_gate_events_deleted: u64,
    pub risk_events_batches: usize,
    pub execution_order_batches: usize,
    pub copy_signals_batches: usize,
    pub shadow_closed_trades_batches: usize,
    pub execution_quote_canary_event_batches: usize,
    pub execution_quote_canary_provider_sample_batches: usize,
    pub execution_quote_canary_shadow_gate_batches: usize,
    pub completed_full_sweep: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionHistoryRetentionSummary {
    pub fills_deleted: u64,
    pub orders_deleted: u64,
    pub copy_signals_deleted: u64,
    pub order_batches: usize,
    pub copy_signal_batches: usize,
    pub orders_completed_full_sweep: bool,
    pub copy_signals_completed_full_sweep: bool,
}

impl HistoryRetentionSummary {
    pub fn is_empty(&self) -> bool {
        self.risk_events_deleted == 0
            && self.copy_signals_deleted == 0
            && self.orders_deleted == 0
            && self.fills_deleted == 0
            && self.shadow_closed_trades_deleted == 0
            && self.execution_quote_canary_events_deleted == 0
            && self.execution_quote_canary_provider_samples_deleted == 0
            && self.execution_quote_canary_shadow_gate_events_deleted == 0
    }
}
