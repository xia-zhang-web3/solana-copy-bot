use crate::*;

pub(crate) struct RunCycleHealthyInputs {
    pub(crate) publish_due: bool,
    pub(crate) followlist_activations_suppressed: bool,
    pub(crate) followlist_deactivations_suppressed: bool,
    pub(crate) metrics_persistence_suppressed: bool,
    pub(crate) snapshots: Vec<WalletSnapshot>,
    pub(crate) observed_swaps_loaded_for_capture: usize,
    pub(crate) scoring_source: &'static str,
    pub(crate) effective_window_start: DateTime<Utc>,
    pub(crate) effective_metrics_window_start: DateTime<Utc>,
}

pub(crate) enum RunCyclePreparedResolution {
    Returned(DiscoverySummary),
    Continue(RunCycleHealthyInputs),
}

impl DiscoveryService {
    pub(crate) fn resolve_run_cycle_prepared_cycle(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        cycle_started: Instant,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        fetch_limit: usize,
        fetch_page_limit: usize,
        fetch_time_budget: StdDuration,
        swaps_window: usize,
        fetch_progress: &FetchProgress,
        delta_fetched: usize,
        swaps_warm_loaded: usize,
        swaps_evicted_due_cap: usize,
        cap_truncation_telemetry: &CapTruncationTelemetrySnapshot,
        prepared_cycle: PreparedCycleState,
        recent_published_follow_wallets: Option<HashSet<String>>,
        bootstrap_degraded_follow_wallets: Option<HashSet<String>>,
    ) -> Result<RunCyclePreparedResolution> {
        let (
            publish_due,
            followlist_activations_suppressed,
            followlist_deactivations_suppressed,
            metrics_persistence_suppressed,
            snapshots,
            observed_swaps_loaded_for_capture,
            scoring_source,
            effective_window_start,
            effective_metrics_window_start,
        ) = match prepared_cycle {
            PreparedCycleState::Degraded {
                publish_due,
                active_wallets,
                scoring_source,
            } => include!("service_methods_30_run_cycle_resolve_degraded.rs"),
            PreparedCycleState::BootstrapDegraded {
                active_wallets,
                scoring_source,
            } => include!("service_methods_30_run_cycle_resolve_bootstrap.rs"),
            PreparedCycleState::Unusable {
                publish_due,
                scoring_source,
                ..
            } => include!("service_methods_30_run_cycle_resolve_unusable.rs"),
            PreparedCycleState::Cached {
                publish_due,
                followlist_activations_suppressed,
                followlist_deactivations_suppressed,
                summary: previous_summary,
                current_raw,
            } => include!("service_methods_30_run_cycle_resolve_cached.rs"),
            PreparedCycleState::Recompute {
                publish_due,
                followlist_activations_suppressed,
                followlist_deactivations_suppressed,
                metrics_persistence_suppressed,
                swaps,
            } => include!("service_methods_30_run_cycle_resolve_recompute.rs"),
            PreparedCycleState::PersistedRecompute {
                publish_due,
                scoring_source,
                empty_window_degraded_scoring_source,
                empty_window_bootstrap_degraded_scoring_source,
                empty_window_unusable_scoring_source,
            } => include!("service_methods_30_run_cycle_resolve_persisted.rs"),
        };
        Ok(RunCyclePreparedResolution::Continue(RunCycleHealthyInputs {
            publish_due,
            followlist_activations_suppressed,
            followlist_deactivations_suppressed,
            metrics_persistence_suppressed,
            snapshots,
            observed_swaps_loaded_for_capture,
            scoring_source,
            effective_window_start,
            effective_metrics_window_start,
        }))
    }
}
