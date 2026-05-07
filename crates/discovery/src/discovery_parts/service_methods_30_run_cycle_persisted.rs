use crate::*;

impl DiscoveryService {
    pub(crate) fn resolve_run_cycle_persisted_recompute(
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
        publish_due: bool,
        scoring_source: &'static str,
        empty_window_degraded_scoring_source: &'static str,
        empty_window_bootstrap_degraded_scoring_source: &'static str,
        empty_window_unusable_scoring_source: &'static str,
        recent_published_follow_wallets: Option<HashSet<String>>,
        bootstrap_degraded_follow_wallets: Option<HashSet<String>>,
    ) -> Result<RunCyclePreparedResolution> {
                let diagnostics = self.snapshot_run_cycle_publication_boundary_diagnostics(
                    store,
                    "persisted_recompute",
                    publish_due,
                    false,
                )?;
                Self::log_run_cycle_publication_boundary(&diagnostics);
                let persisted_checkpoint_present =
                    store.load_discovery_persisted_rebuild_state()?.is_some();
                let priority_recovery_contract = self.persisted_stream_priority_recovery_contract(
                    store,
                    now,
                    fetch_limit,
                    fetch_page_limit,
                    fetch_time_budget,
                )?;
                if priority_recovery_contract.time_budget > fetch_time_budget {
                    info!(
                        rebuild_window_start = %window_start,
                        rebuild_metrics_window_start = %metrics_window_start,
                        rebuild_priority_recovery_contract_scope = if persisted_checkpoint_present {
                            "base_pre_resume"
                        } else {
                            "effective_no_existing_checkpoint"
                        },
                        rebuild_existing_persisted_checkpoint_present =
                            persisted_checkpoint_present,
                        rebuild_priority_recovery_contract_reason =
                            priority_recovery_contract.reason,
                        rebuild_time_budget_ms =
                            priority_recovery_contract.time_budget.as_millis(),
                        rebuild_collect_buy_mints_phase_page_limit = priority_recovery_contract
                            .collect_buy_mints_phase_page_limit_override,
                        rebuild_replay_wallet_stats_phase_page_limit =
                            priority_recovery_contract
                                .replay_wallet_stats_phase_page_limit_override,
                        rebuild_replay_sol_leg_phase_page_limit =
                            priority_recovery_contract
                                .replay_sol_leg_phase_page_limit_override,
                        "prepared base bounded persisted observed_swaps recovery contract before any checkpoint-specific deepening for stale or incomplete publication truth"
                    );
                }
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
                ) = match self.advance_persisted_stream_rebuild_with_phase_page_limits(
                    store,
                    window_start,
                    metrics_window_start,
                    now,
                    fetch_limit,
                    fetch_page_limit,
                    priority_recovery_contract.time_budget,
                    priority_recovery_contract.collect_buy_mints_phase_page_limit_override,
                    priority_recovery_contract.replay_wallet_stats_phase_page_limit_override,
                    priority_recovery_contract.replay_sol_leg_phase_page_limit_override,
                )? {
                    PersistedStreamRebuildAdvanceOutcome::Completed {
                        snapshots,
                        telemetry,
                    } => include!("service_methods_30_run_cycle_persisted_completed.rs"),
                    PersistedStreamRebuildAdvanceOutcome::InProgress { telemetry } => {
                        include!("service_methods_30_run_cycle_persisted_in_progress.rs")
                    }
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
