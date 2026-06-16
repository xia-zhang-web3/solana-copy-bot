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
            ensure_recent_raw_observed_swaps_timestamps_canonical_utc(conn)?;
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
            let deleted = deleted.max(0) as usize;
            let mut state = recent_raw_journal_state_cached_query(conn)?;
            state.row_count = state.row_count.saturating_sub(deleted);
            if state.row_count == 0 {
                state.covered_since = None;
                state.covered_through_cursor = None;
            } else if deleted > 0 {
                let covered_since_raw: Option<String> = conn
                    .query_row(
                        "SELECT ts
                         FROM observed_swaps
                         ORDER BY ts ASC
                         LIMIT 1",
                        [],
                        |row| row.get(0),
                    )
                    .optional()
                    .context("failed loading recent raw journal covered_since after prune")?;
                state.covered_since = covered_since_raw
                    .as_deref()
                    .map(|raw| parse_rfc3339_utc(raw, "recent_raw_journal_state.covered_since_ts"))
                    .transpose()?;
            }
            state.last_pruned_rows = deleted;
            state.last_pruned_at = Some(pruned_at);
            state.updated_at = Some(pruned_at);
            upsert_recent_raw_journal_state_on_conn(conn, &state)?;
            Ok(deleted)
        })
    }
}
