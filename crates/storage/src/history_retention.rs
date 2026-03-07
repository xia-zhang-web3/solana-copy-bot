use super::SqliteStore;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

const TERMINAL_EXECUTION_STATUSES: [&str; 3] = [
    "execution_confirmed",
    "execution_failed",
    "execution_dropped",
];

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

impl SqliteStore {
    pub fn apply_history_retention(
        &self,
        cutoffs: HistoryRetentionCutoffs,
        protect_undelivered_alerts: bool,
    ) -> Result<HistoryRetentionSummary> {
        let risk_events_deleted = self
            .delete_risk_events_before(cutoffs.risk_events_before, protect_undelivered_alerts)
            .context("failed to apply risk_events retention")?;
        let (fills_deleted, orders_deleted, copy_signals_deleted) = self
            .delete_execution_history_before(cutoffs.orders_before, cutoffs.copy_signals_before)
            .context("failed to apply execution history retention")?;
        let shadow_closed_trades_deleted = self
            .delete_shadow_closed_trades_before(cutoffs.shadow_closed_trades_before)
            .context("failed to apply shadow closed trade retention")?;

        Ok(HistoryRetentionSummary {
            risk_events_deleted,
            copy_signals_deleted,
            orders_deleted,
            fills_deleted,
            shadow_closed_trades_deleted,
        })
    }

    fn delete_risk_events_before(
        &self,
        cutoff: DateTime<Utc>,
        protect_undelivered_alerts: bool,
    ) -> Result<u64> {
        let cutoff = cutoff.to_rfc3339();
        self.execute_with_retry_result(|conn| -> rusqlite::Result<u64> {
            let delivered_cursor: Option<i64> = conn.query_row(
                "SELECT MIN(last_rowid) FROM alert_delivery_state",
                [],
                |row| row.get(0),
            )?;
            let deleted = if let Some(delivered_cursor) = delivered_cursor {
                conn.execute(
                    "DELETE FROM risk_events
                     WHERE ts < ?1
                       AND (
                            severity NOT IN ('warn', 'error')
                            OR rowid <= ?2
                       )",
                    params![cutoff, delivered_cursor],
                )?
            } else if protect_undelivered_alerts {
                conn.execute(
                    "DELETE FROM risk_events
                     WHERE ts < ?1
                       AND severity NOT IN ('warn', 'error')",
                    params![cutoff],
                )?
            } else {
                conn.execute("DELETE FROM risk_events WHERE ts < ?1", params![cutoff])?
            };
            Ok(deleted as u64)
        })
        .context("failed deleting retained risk events")
    }

    fn delete_execution_history_before(
        &self,
        orders_cutoff: DateTime<Utc>,
        copy_signals_cutoff: DateTime<Utc>,
    ) -> Result<(u64, u64, u64)> {
        let orders_cutoff = orders_cutoff.to_rfc3339();
        let copy_signals_cutoff = copy_signals_cutoff.to_rfc3339();
        self.with_immediate_transaction_retry("execution history retention", |conn| {
            let fills_deleted = conn.execute(
                "DELETE FROM fills
                 WHERE order_id IN (
                    SELECT order_id
                    FROM orders
                    WHERE status IN (?1, ?2, ?3)
                      AND COALESCE(confirm_ts, submit_ts) < ?4
                 )",
                params![
                    TERMINAL_EXECUTION_STATUSES[0],
                    TERMINAL_EXECUTION_STATUSES[1],
                    TERMINAL_EXECUTION_STATUSES[2],
                    orders_cutoff,
                ],
            )? as u64;

            let orders_deleted = conn.execute(
                "DELETE FROM orders
                 WHERE status IN (?1, ?2, ?3)
                   AND COALESCE(confirm_ts, submit_ts) < ?4",
                params![
                    TERMINAL_EXECUTION_STATUSES[0],
                    TERMINAL_EXECUTION_STATUSES[1],
                    TERMINAL_EXECUTION_STATUSES[2],
                    orders_cutoff,
                ],
            )? as u64;

            let copy_signals_deleted = conn.execute(
                "DELETE FROM copy_signals
                 WHERE status IN (?1, ?2, ?3)
                   AND ts < ?4
                   AND NOT EXISTS (
                       SELECT 1
                       FROM orders
                       WHERE orders.signal_id = copy_signals.signal_id
                   )",
                params![
                    TERMINAL_EXECUTION_STATUSES[0],
                    TERMINAL_EXECUTION_STATUSES[1],
                    TERMINAL_EXECUTION_STATUSES[2],
                    copy_signals_cutoff,
                ],
            )? as u64;

            Ok((fills_deleted, orders_deleted, copy_signals_deleted))
        })
    }

    fn delete_shadow_closed_trades_before(&self, cutoff: DateTime<Utc>) -> Result<u64> {
        let cutoff = cutoff.to_rfc3339();
        self.execute_with_retry_result(|conn| {
            conn.execute(
                "DELETE FROM shadow_closed_trades
                 WHERE closed_ts < ?1",
                params![cutoff],
            )
            .map(|deleted| deleted as u64)
        })
        .context("failed deleting retained shadow closed trades")
    }
}
