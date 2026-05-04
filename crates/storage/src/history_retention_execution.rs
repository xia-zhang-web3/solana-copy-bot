impl SqliteStore {
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
}
