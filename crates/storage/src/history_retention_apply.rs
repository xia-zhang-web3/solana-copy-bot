use super::*;

impl SqliteStore {
    pub fn apply_history_retention(
        &self,
        cutoffs: HistoryRetentionCutoffs,
        protect_undelivered_alerts: bool,
    ) -> Result<HistoryRetentionSummary> {
        self.apply_history_retention_bounded(
            cutoffs,
            protect_undelivered_alerts,
            usize::MAX,
            usize::MAX,
            usize::MAX,
            usize::MAX,
        )
    }

    pub fn apply_history_retention_bounded(
        &self,
        cutoffs: HistoryRetentionCutoffs,
        protect_undelivered_alerts: bool,
        max_risk_event_batches: usize,
        max_execution_order_batches: usize,
        max_copy_signal_batches: usize,
        max_shadow_closed_trade_batches: usize,
    ) -> Result<HistoryRetentionSummary> {
        let risk_events = self
            .delete_risk_events_before_batched(
                cutoffs.risk_events_before,
                protect_undelivered_alerts,
                HISTORY_RETENTION_RISK_EVENTS_BATCH_SIZE,
                max_risk_event_batches,
            )
            .context("failed to apply risk_events retention")?;
        let execution_history = self
            .delete_execution_history_before_batched(
                cutoffs.orders_before,
                cutoffs.copy_signals_before,
                HISTORY_RETENTION_EXECUTION_ORDER_BATCH_SIZE,
                HISTORY_RETENTION_COPY_SIGNALS_BATCH_SIZE,
                max_execution_order_batches,
                max_copy_signal_batches,
            )
            .context("failed to apply execution history retention")?;
        let shadow_closed_trades = self
            .delete_shadow_closed_trades_before_batched(
                cutoffs.shadow_closed_trades_before,
                HISTORY_RETENTION_SHADOW_CLOSED_TRADES_BATCH_SIZE,
                max_shadow_closed_trade_batches,
            )
            .context("failed to apply shadow closed trade retention")?;

        Ok(HistoryRetentionSummary {
            risk_events_deleted: risk_events.deleted_rows as u64,
            copy_signals_deleted: execution_history.copy_signals_deleted,
            orders_deleted: execution_history.orders_deleted,
            fills_deleted: execution_history.fills_deleted,
            shadow_closed_trades_deleted: shadow_closed_trades.deleted_rows as u64,
            risk_events_batches: risk_events.batches,
            execution_order_batches: execution_history.order_batches,
            copy_signals_batches: execution_history.copy_signal_batches,
            shadow_closed_trades_batches: shadow_closed_trades.batches,
            completed_full_sweep: risk_events.completed_full_sweep
                && execution_history.orders_completed_full_sweep
                && execution_history.copy_signals_completed_full_sweep
                && shadow_closed_trades.completed_full_sweep,
        })
    }
}
