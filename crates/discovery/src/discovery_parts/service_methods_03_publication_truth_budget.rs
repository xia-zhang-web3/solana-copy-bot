impl DiscoveryService {
    fn persisted_observed_swaps_cover_window(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
    ) -> Result<bool> {
        let Some(oldest_persisted_observed_swap_ts) = store.oldest_observed_swap_timestamp()?
        else {
            return Ok(false);
        };
        if oldest_persisted_observed_swap_ts > window_start {
            return Ok(false);
        }
        let (recent_window_swaps, _) = store.load_recent_observed_swaps_since(window_start, 1)?;
        Ok(!recent_window_swaps.is_empty())
    }

    pub fn recommended_publication_truth_repair_time_budget(
        &self,
        runtime_store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<StdDuration> {
        let gate = self.publication_freshness_gate();
        let required_window_start = now - Duration::days(self.runtime_scoring_window_days());
        let publication_truth_fresh = runtime_store
            .discovery_publication_state_read_only()?
            .as_ref()
            .is_some_and(|state| state.is_fresh_under_gate(&gate, now));
        if publication_truth_fresh {
            return Ok(StdDuration::from_millis(
                DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEFAULT_TIME_BUDGET_MS,
            ));
        }

        let runtime_window_complete =
            self.persisted_observed_swaps_cover_window(runtime_store, required_window_start)?;
        if runtime_window_complete {
            return Ok(StdDuration::from_millis(
                self.config.fetch_time_budget_ms.max(
                    DISCOVERY_PUBLICATION_TRUTH_REPAIR_RUNTIME_WINDOW_REFRESH_MIN_TIME_BUDGET_MS,
                ),
            ));
        }

        Ok(StdDuration::from_millis(
            DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEFAULT_TIME_BUDGET_MS,
        ))
    }

}
