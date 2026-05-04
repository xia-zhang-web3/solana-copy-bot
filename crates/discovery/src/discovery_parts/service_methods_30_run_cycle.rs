include!("service_methods_30_run_cycle_complete.rs");

impl DiscoveryService {
    pub fn run_cycle(&self, store: &SqliteStore, now: DateTime<Utc>) -> Result<DiscoverySummary> {
        let cycle_started = Instant::now();
        let publish_interval_seconds = self.config.refresh_seconds.max(1) as i64;
        let window_days = self.config.scoring_window_days.max(1);
        let window_start = now - Duration::days(window_days as i64);
        let metrics_window_start = self.metrics_window_start(now);
        let max_window_swaps_in_memory = self.config.max_window_swaps_in_memory.max(1);
        let fetch_limit = self.config.max_fetch_swaps_per_cycle.max(1);
        let fetch_page_limit = self.config.max_fetch_pages_per_cycle.max(1);
        let fetch_time_budget = StdDuration::from_millis(self.config.fetch_time_budget_ms.max(1));
        let retention_days = self.config.observed_swaps_retention_days.max(1);
        let short_retention_window = retention_days < window_days;
        let runtime_publication_truth_resolution =
            self.runtime_publication_truth_resolution(store, now)?;
        let recent_published_follow_wallets = match runtime_publication_truth_resolution.as_ref() {
            Some(RuntimePublicationTruthResolution::Recent(truth)) => Some(truth.active_wallets()),
            Some(RuntimePublicationTruthResolution::BootstrapDegraded(_)) | None => None,
        };
        let bootstrap_degraded_follow_wallets = match runtime_publication_truth_resolution.as_ref()
        {
            Some(RuntimePublicationTruthResolution::BootstrapDegraded(truth)) => {
                Some(truth.active_wallets())
            }
            Some(RuntimePublicationTruthResolution::Recent(_)) | None => None,
        };
        let RunCycleWindowPreparation {
            swaps_window,
            fetch_progress,
            cap_truncation_telemetry,
            delta_fetched,
            swaps_evicted_due_cap,
            swaps_warm_loaded,
            prepared_cycle,
        } = self.prepare_run_cycle_window(
            store,
            now,
            publish_interval_seconds,
            window_start,
            metrics_window_start,
            max_window_swaps_in_memory,
            fetch_limit,
            fetch_page_limit,
            fetch_time_budget,
            short_retention_window,
            recent_published_follow_wallets.as_ref(),
            bootstrap_degraded_follow_wallets.as_ref(),
        )?;

        let healthy_inputs = match self.resolve_run_cycle_prepared_cycle(
            store,
            now,
            cycle_started,
            window_start,
            metrics_window_start,
            fetch_limit,
            fetch_page_limit,
            fetch_time_budget,
            swaps_window,
            &fetch_progress,
            delta_fetched,
            swaps_warm_loaded,
            swaps_evicted_due_cap,
            &cap_truncation_telemetry,
            prepared_cycle,
            recent_published_follow_wallets,
            bootstrap_degraded_follow_wallets,
        )? {
            RunCyclePreparedResolution::Returned(summary) => return Ok(summary),
            RunCyclePreparedResolution::Continue(inputs) => inputs,
        };

        self.complete_healthy_run_cycle(
            store,
            now,
            cycle_started,
            fetch_limit,
            fetch_page_limit,
            swaps_window,
            &fetch_progress,
            delta_fetched,
            swaps_warm_loaded,
            swaps_evicted_due_cap,
            &cap_truncation_telemetry,
            healthy_inputs,
        )
    }
}
