impl SqliteStore {
    pub fn discovery_scoring_ready_for_window(
        &self,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        max_lag: Duration,
    ) -> Result<bool> {
        let Some(covered_since) = self.load_discovery_scoring_covered_since()? else {
            return Ok(false);
        };
        if self
            .load_discovery_scoring_materialization_gap_cursor()?
            .is_some()
        {
            return Ok(false);
        }
        let Some(covered_through) = self.load_discovery_scoring_covered_through_cursor()? else {
            return Ok(false);
        };
        Ok(covered_since <= window_start && covered_through.ts_utc + max_lag >= now)
    }

    pub fn prune_discovery_scoring_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        self.prune_discovery_scoring_before_batched(cutoff, usize::MAX)
            .map(|summary| summary.deleted_rows)
    }

    pub fn prune_discovery_scoring_before_batched(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<SqliteBatchedDeleteSummary> {
        let mut summary = SqliteBatchedDeleteSummary::default();
        loop {
            let deleted = self.prune_discovery_scoring_before_batch(cutoff, batch_size)?;
            if deleted == 0 {
                break;
            }
            summary.deleted_rows += deleted;
            summary.batches += 1;
        }
        Ok(summary)
    }
}
