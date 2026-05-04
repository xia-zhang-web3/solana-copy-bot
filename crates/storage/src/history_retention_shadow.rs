impl SqliteStore {
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
