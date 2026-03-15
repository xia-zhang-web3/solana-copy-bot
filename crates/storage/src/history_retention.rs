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

    fn delete_risk_events_before_batched(
        &self,
        cutoff: DateTime<Utc>,
        protect_undelivered_alerts: bool,
        batch_size: usize,
        max_batches: usize,
    ) -> Result<super::SqliteBatchedDeleteSummaryWithCompletion> {
        let cutoff = cutoff.to_rfc3339();
        let batch_limit = batch_size.max(1).min(i64::MAX as usize) as i64;
        let max_batches = max_batches.max(1);
        let mut summary = super::SqliteBatchedDeleteSummaryWithCompletion::default();
        loop {
            if summary.batches >= max_batches {
                break;
            }
            let deleted = self
                .execute_with_retry_result(|conn| -> rusqlite::Result<usize> {
                    let delivered_cursor: Option<i64> = conn.query_row(
                        "SELECT MIN(last_rowid) FROM alert_delivery_state",
                        [],
                        |row| row.get(0),
                    )?;
                    let deleted = if let Some(delivered_cursor) = delivered_cursor {
                        conn.execute(
                            "DELETE FROM risk_events
                             WHERE rowid IN (
                                 SELECT rowid
                                 FROM risk_events
                                 WHERE ts < ?1
                                   AND (
                                        severity NOT IN ('warn', 'error')
                                        OR rowid <= ?2
                                   )
                                 ORDER BY ts ASC, rowid ASC
                                 LIMIT ?3
                             )",
                            params![&cutoff, delivered_cursor, batch_limit],
                        )?
                    } else if protect_undelivered_alerts {
                        conn.execute(
                            "DELETE FROM risk_events
                             WHERE rowid IN (
                                 SELECT rowid
                                 FROM risk_events
                                 WHERE ts < ?1
                                   AND severity NOT IN ('warn', 'error')
                                 ORDER BY ts ASC, rowid ASC
                                 LIMIT ?2
                             )",
                            params![&cutoff, batch_limit],
                        )?
                    } else {
                        conn.execute(
                            "DELETE FROM risk_events
                             WHERE rowid IN (
                                 SELECT rowid
                                 FROM risk_events
                                 WHERE ts < ?1
                                 ORDER BY ts ASC, rowid ASC
                                 LIMIT ?2
                             )",
                            params![&cutoff, batch_limit],
                        )?
                    };
                    Ok(deleted)
                })
                .context("failed deleting retained risk events")?;
            if deleted == 0 {
                summary.completed_full_sweep = true;
                break;
            }
            summary.deleted_rows += deleted;
            summary.batches += 1;
        }
        Ok(summary)
    }

    fn delete_execution_history_before_batched(
        &self,
        orders_cutoff: DateTime<Utc>,
        copy_signals_cutoff: DateTime<Utc>,
        order_batch_size: usize,
        copy_signal_batch_size: usize,
        max_order_batches: usize,
        max_copy_signal_batches: usize,
    ) -> Result<ExecutionHistoryRetentionSummary> {
        let orders_cutoff = orders_cutoff.to_rfc3339();
        let copy_signals_cutoff = copy_signals_cutoff.to_rfc3339();
        let order_batch_limit = order_batch_size.max(1).min(i64::MAX as usize) as i64;
        let copy_signal_batch_limit = copy_signal_batch_size.max(1).min(i64::MAX as usize) as i64;
        let max_order_batches = max_order_batches.max(1);
        let max_copy_signal_batches = max_copy_signal_batches.max(1);
        let mut summary = ExecutionHistoryRetentionSummary::default();

        loop {
            if summary.order_batches >= max_order_batches {
                break;
            }
            let (fills_deleted, orders_deleted) = self
                .with_immediate_transaction_retry(
                    "execution history retention order batch",
                    |conn| {
                        let fills_deleted = conn.execute(
                            "DELETE FROM fills
                         WHERE order_id IN (
                            SELECT order_id
                            FROM orders
                            WHERE status IN (?1, ?2, ?3)
                              AND COALESCE(confirm_ts, submit_ts) < ?4
                            ORDER BY COALESCE(confirm_ts, submit_ts) ASC, order_id ASC
                            LIMIT ?5
                         )",
                            params![
                                TERMINAL_EXECUTION_STATUSES[0],
                                TERMINAL_EXECUTION_STATUSES[1],
                                TERMINAL_EXECUTION_STATUSES[2],
                                &orders_cutoff,
                                order_batch_limit,
                            ],
                        )? as u64;

                        let orders_deleted = conn.execute(
                            "DELETE FROM orders
                         WHERE rowid IN (
                            SELECT rowid
                            FROM orders
                            WHERE status IN (?1, ?2, ?3)
                              AND COALESCE(confirm_ts, submit_ts) < ?4
                            ORDER BY COALESCE(confirm_ts, submit_ts) ASC, order_id ASC
                            LIMIT ?5
                         )",
                            params![
                                TERMINAL_EXECUTION_STATUSES[0],
                                TERMINAL_EXECUTION_STATUSES[1],
                                TERMINAL_EXECUTION_STATUSES[2],
                                &orders_cutoff,
                                order_batch_limit,
                            ],
                        )? as u64;

                        Ok((fills_deleted, orders_deleted))
                    },
                )
                .context("failed deleting retained execution history order batch")?;
            if fills_deleted == 0 && orders_deleted == 0 {
                summary.orders_completed_full_sweep = true;
                break;
            }
            summary.fills_deleted += fills_deleted;
            summary.orders_deleted += orders_deleted;
            summary.order_batches += 1;
        }

        loop {
            if summary.copy_signal_batches >= max_copy_signal_batches {
                break;
            }
            let deleted = self
                .execute_with_retry(|conn| {
                    conn.execute(
                        "DELETE FROM copy_signals
                         WHERE rowid IN (
                            SELECT rowid
                            FROM copy_signals
                            WHERE status IN (?1, ?2, ?3)
                              AND ts < ?4
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM orders
                                  WHERE orders.signal_id = copy_signals.signal_id
                              )
                            ORDER BY ts ASC, signal_id ASC
                            LIMIT ?5
                         )",
                        params![
                            TERMINAL_EXECUTION_STATUSES[0],
                            TERMINAL_EXECUTION_STATUSES[1],
                            TERMINAL_EXECUTION_STATUSES[2],
                            &copy_signals_cutoff,
                            copy_signal_batch_limit,
                        ],
                    )
                })
                .context("failed deleting retained copy_signals history slice")?;
            if deleted == 0 {
                summary.copy_signals_completed_full_sweep = true;
                break;
            }
            summary.copy_signals_deleted += deleted as u64;
            summary.copy_signal_batches += 1;
        }

        Ok(summary)
    }

    fn delete_shadow_closed_trades_before_batched(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
        max_batches: usize,
    ) -> Result<super::SqliteBatchedDeleteSummaryWithCompletion> {
        let cutoff = cutoff.to_rfc3339();
        let batch_limit = batch_size.max(1).min(i64::MAX as usize) as i64;
        let max_batches = max_batches.max(1);
        let mut summary = super::SqliteBatchedDeleteSummaryWithCompletion::default();
        loop {
            if summary.batches >= max_batches {
                break;
            }
            let deleted = self
                .execute_with_retry_result(|conn| {
                    conn.execute(
                        "DELETE FROM shadow_closed_trades
                         WHERE rowid IN (
                            SELECT rowid
                            FROM shadow_closed_trades
                            WHERE closed_ts < ?1
                            ORDER BY closed_ts ASC, rowid ASC
                            LIMIT ?2
                         )",
                        params![&cutoff, batch_limit],
                    )
                })
                .context("failed deleting retained shadow closed trades")?;
            if deleted == 0 {
                summary.completed_full_sweep = true;
                break;
            }
            summary.deleted_rows += deleted;
            summary.batches += 1;
        }
        Ok(summary)
    }
}
