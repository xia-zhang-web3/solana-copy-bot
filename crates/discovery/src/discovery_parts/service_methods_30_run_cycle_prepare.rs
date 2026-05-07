#[path = "service_methods_30_run_cycle_prepare_fetch.rs"]
mod service_methods_30_run_cycle_prepare_fetch;
#[path = "service_methods_30_run_cycle_prepare_decide.rs"]
mod service_methods_30_run_cycle_prepare_decide;

use self::service_methods_30_run_cycle_prepare_fetch::RunCycleWindowFetchPreparation;

struct RunCycleWindowPreparation {
    swaps_window: usize,
    fetch_progress: FetchProgress,
    cap_truncation_telemetry: CapTruncationTelemetrySnapshot,
    delta_fetched: usize,
    swaps_evicted_due_cap: usize,
    swaps_warm_loaded: usize,
    prepared_cycle: PreparedCycleState,
}

impl DiscoveryService {
    fn prepare_run_cycle_window(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        publish_interval_seconds: i64,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        max_window_swaps_in_memory: usize,
        fetch_limit: usize,
        fetch_page_limit: usize,
        fetch_time_budget: StdDuration,
        short_retention_window: bool,
        recent_published_follow_wallets: Option<&HashSet<String>>,
        bootstrap_degraded_follow_wallets: Option<&HashSet<String>>,
    ) -> Result<RunCycleWindowPreparation> {
        let mut state = match self.window_state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("discovery window mutex poisoned; continuing with recovered state");
                poisoned.into_inner()
            }
        };
        let fetch_preparation = self.prepare_run_cycle_window_fetch(
            store,
            &mut state,
            now,
            publish_interval_seconds,
            window_start,
            max_window_swaps_in_memory,
            fetch_limit,
            fetch_page_limit,
            fetch_time_budget,
            short_retention_window,
        )?;
        self.prepare_run_cycle_window_decide(
            store,
            &mut state,
            window_start,
            metrics_window_start,
            short_retention_window,
            fetch_preparation,
            recent_published_follow_wallets,
            bootstrap_degraded_follow_wallets,
        )
    }
}
