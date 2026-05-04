impl SqliteStore {
    pub fn replay_recent_raw_journal_into_runtime_store(
        &self,
        runtime_store: &SqliteStore,
        required_window_start: DateTime<Utc>,
        artifact_runtime_cursor: &DiscoveryRuntimeCursor,
        replay_batch_size: usize,
    ) -> Result<RecentRawJournalReplaySummary> {
        let journal_state = self.recent_raw_journal_state_read_only()?;
        let journal_available = journal_state.row_count > 0;
        let journal_covers_artifact_cursor = journal_state
            .covered_through_cursor
            .as_ref()
            .is_some_and(|cursor| {
                discovery_runtime_cursor_cmp(cursor, artifact_runtime_cursor) != Ordering::Less
            });

        let mut replayed_rows = 0usize;
        if journal_available {
            let mut batch = Vec::with_capacity(replay_batch_size.max(1));
            self.for_each_observed_swap_since(required_window_start, |swap| {
                batch.push(swap);
                if batch.len() >= replay_batch_size.max(1) {
                    replayed_rows = replayed_rows.saturating_add(
                        runtime_store
                            .insert_observed_swaps_batch_with_activity_days(&batch)?
                            .into_iter()
                            .filter(|inserted| *inserted)
                            .count(),
                    );
                    batch.clear();
                }
                Ok(())
            })?;
            if !batch.is_empty() {
                replayed_rows = replayed_rows.saturating_add(
                    runtime_store
                        .insert_observed_swaps_batch_with_activity_days(&batch)?
                        .into_iter()
                        .filter(|inserted| *inserted)
                        .count(),
                );
            }
        }

        let runtime_window_has_rows = !runtime_store
            .load_recent_observed_swaps_since(required_window_start, 1)?
            .0
            .is_empty();
        let raw_coverage_satisfied = journal_available
            && journal_state
                .covered_since
                .is_some_and(|covered_since| covered_since <= required_window_start)
            && journal_covers_artifact_cursor
            && runtime_window_has_rows;

        Ok(RecentRawJournalReplaySummary {
            required_window_start,
            artifact_runtime_cursor: artifact_runtime_cursor.clone(),
            journal_available,
            journal_covered_since: journal_state.covered_since,
            journal_covered_through_cursor: journal_state.covered_through_cursor,
            journal_covers_artifact_cursor,
            replayed_rows,
            raw_coverage_satisfied,
        })
    }
}
