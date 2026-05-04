impl SqliteStore {
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
}
