use crate::*;

impl DiscoveryService {
    #[cfg(test)]
    pub(crate) fn advance_persisted_stream_rebuild(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        fetch_limit: usize,
        fetch_page_limit: usize,
        rebuild_time_budget: StdDuration,
    ) -> Result<PersistedStreamRebuildAdvanceOutcome> {
        self.advance_persisted_stream_rebuild_with_phase_page_limits(
            store,
            window_start,
            metrics_window_start,
            now,
            fetch_limit,
            fetch_page_limit,
            rebuild_time_budget,
            None,
            None,
            None,
        )
    }

}
