impl SqliteStore {
    pub fn prune_discovery_scoring_before_batch(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<usize> {
        let cutoff_day = cutoff.date_naive().format("%Y-%m-%d").to_string();
        let cutoff_ts = cutoff.to_rfc3339();
        let cutoff_minute = cutoff.timestamp().div_euclid(60);
        let batch_limit = batch_size.max(1).min(i64::MAX as usize) as i64;
        self.with_immediate_transaction_retry("discovery scoring retention prune batch", |conn| {
            let mut deleted = 0usize;
            let mut remaining = batch_limit;
            if remaining <= 0 {
                return Ok(0usize);
            }

            let buy_deleted = conn
                .execute(
                    "DELETE FROM wallet_scoring_buy_facts
                         WHERE rowid IN (
                             SELECT rowid
                             FROM wallet_scoring_buy_facts
                             WHERE ts < ?1
                             ORDER BY ts ASC, buy_signature ASC
                             LIMIT ?2
                         )",
                    params![&cutoff_ts, remaining],
                )
                .context("failed pruning wallet_scoring_buy_facts")?;
            deleted += buy_deleted;
            remaining -= buy_deleted as i64;

            if remaining > 0 {
                let close_deleted = conn
                    .execute(
                        "DELETE FROM wallet_scoring_close_facts
                             WHERE rowid IN (
                                 SELECT rowid
                                 FROM wallet_scoring_close_facts
                                 WHERE closed_ts < ?1
                                 ORDER BY closed_ts ASC, sell_signature ASC, segment_index ASC
                                 LIMIT ?2
                             )",
                        params![&cutoff_ts, remaining],
                    )
                    .context("failed pruning wallet_scoring_close_facts")?;
                deleted += close_deleted;
                remaining -= close_deleted as i64;
            }

            if remaining > 0 {
                let tx_minutes_deleted = conn
                    .execute(
                        "DELETE FROM wallet_scoring_tx_minutes
                             WHERE rowid IN (
                                 SELECT rowid
                                 FROM wallet_scoring_tx_minutes
                                 WHERE minute_bucket < ?1
                                 ORDER BY minute_bucket ASC, wallet_id ASC
                                 LIMIT ?2
                             )",
                        params![cutoff_minute, remaining],
                    )
                    .context("failed pruning wallet_scoring_tx_minutes")?;
                deleted += tx_minutes_deleted;
                remaining -= tx_minutes_deleted as i64;
            }

            if remaining > 0 {
                let days_deleted = conn
                    .execute(
                        "DELETE FROM wallet_scoring_days
                             WHERE rowid IN (
                                 SELECT rowid
                                 FROM wallet_scoring_days
                                 WHERE activity_day < ?1
                                 ORDER BY activity_day ASC, wallet_id ASC
                                 LIMIT ?2
                             )",
                        params![&cutoff_day, remaining],
                    )
                    .context("failed pruning wallet_scoring_days")?;
                deleted += days_deleted;
            }
            Ok(deleted)
        })
    }
}
