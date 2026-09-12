use super::*;

impl SqliteStore {
    pub fn prune_recent_raw_journal_before_batch(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
        pruned_at: DateTime<Utc>,
    ) -> Result<usize> {
        let batch_limit = batch_size.max(1).min(i64::MAX as usize) as i64;
        self.with_immediate_transaction_retry("recent raw journal retention prune", |conn| {
            copybot_storage_core::observed_retention::validate_schema(conn)?;
            ensure_recent_raw_journal_tables_on_conn(conn)?;
            copybot_storage_core::observed_retention::delete_before_batch(
                conn,
                cutoff,
                batch_limit,
                Some(pruned_at),
            )
        })
    }
}
