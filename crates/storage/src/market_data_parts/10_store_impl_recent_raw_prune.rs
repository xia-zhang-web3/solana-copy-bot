use super::*;

impl SqliteStore {
    pub fn prune_recent_raw_journal_before_batch(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
        pruned_at: DateTime<Utc>,
    ) -> Result<usize> {
        self.ensure_recent_raw_journal_tables()?;
        let cutoff_ts = cutoff.to_rfc3339();
        let batch_limit = batch_size.max(1).min(i64::MAX as usize) as i64;
        self.with_immediate_transaction_retry("recent raw journal retention prune", |conn| {
            ensure_recent_raw_journal_tables_on_conn(conn)?;
            let deleted = conn
                .execute(
                    "DELETE FROM observed_swaps
                     WHERE rowid IN (
                        SELECT rowid
                        FROM observed_swaps
                        WHERE ts < ?1
                        ORDER BY ts ASC, slot ASC, signature ASC
                        LIMIT ?2
                     )",
                    params![&cutoff_ts, batch_limit],
                )
                .context("failed deleting recent raw journal retention slice")?;
            let mut state = recent_raw_journal_state_query(conn)?;
            state.last_pruned_rows = deleted.max(0) as usize;
            state.last_pruned_at = Some(pruned_at);
            state.updated_at = Some(pruned_at);
            upsert_recent_raw_journal_state_on_conn(conn, &state)?;
            Ok(deleted.max(0) as usize)
        })
    }
}
