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
                    } => {
                        if telemetry.observed_swaps_loaded == 0 {
                            warn!(
                                window_start = %telemetry.window_start,
                                metrics_window_start = %telemetry.metrics_window_start,
                                rebuild_horizon_end = %telemetry.horizon_end,
                                rebuild_total_elapsed_ms = telemetry.total_elapsed_ms,
                                "persisted observed_swaps runtime fallback completed with zero rows in the frozen scoring horizon; treating persisted raw truth as unavailable"
                            );
                            if let Some(active_wallets) = recent_published_follow_wallets.clone() {
                                store.clear_discovery_persisted_rebuild_state()?;
                                let summary = self.degraded_summary_from_published_universe(
                                    store,
                                    window_start,
                                    metrics_window_start,
                                    publish_due,
                                    active_wallets,
                                    &cap_truncation_telemetry,
                                    empty_window_degraded_scoring_source,
                                    empty_window_degraded_scoring_source,
                                    now,
                                )?;
                                let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                                info!(
                                    window_start = %summary.window_start,
                                    wallets_seen = summary.wallets_seen,
                                    eligible_wallets = summary.eligible_wallets,
                                    metrics_written = summary.metrics_written,
                                    follow_promoted = summary.follow_promoted,
                                    follow_demoted = summary.follow_demoted,
                                    active_follow_wallets = summary.active_follow_wallets,
                                    swaps_window,
                                    swaps_query_rows = fetch_progress.query_rows,
                                    swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                                    swaps_delta_fetched = delta_fetched,
                                    swaps_warm_loaded,
                                    swaps_evicted_due_cap,
                                    swaps_fetch_limit = fetch_limit,
                                    swaps_fetch_pages = fetch_progress.pages,
                                    swaps_fetch_page_limit = fetch_page_limit,
                                    swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                                    swaps_fetch_limit_reached = fetch_progress.saturated,
                                    swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                                    swaps_fetch_time_budget_exhausted =
                                        fetch_progress.time_budget_exhausted,
                                    metrics_window_start = %metrics_window_start,
                                    scoring_source = summary.scoring_source,
                                    discovery_runtime_mode = summary.runtime_mode.as_str(),
                                    metrics_persisted = false,
                                    snapshot_recomputed = false,
                                    discovery_published = summary.published,
                                    discovery_cycle_duration_ms = elapsed_ms,
                                    "discovery cycle completed"
                                );
                                return Ok(RunCyclePreparedResolution::Returned(summary));
                            }
                            if let Some(active_wallets) = bootstrap_degraded_follow_wallets.clone()
                            {
                                store.clear_discovery_persisted_rebuild_state()?;
                                let summary = self
                                    .bootstrap_degraded_summary_from_published_universe(
                                        store,
                                        window_start,
                                        active_wallets,
                                        &cap_truncation_telemetry,
                                        empty_window_bootstrap_degraded_scoring_source,
                                        empty_window_bootstrap_degraded_scoring_source,
                                        now,
                                    )?;
                                let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                                warn!(
                                    metrics_window_start = %metrics_window_start,
                                    scoring_source = summary.scoring_source,
                                    bootstrap_degraded_active = true,
                                    "discovery runtime completed persisted fallback with zero rows and remains in explicit bootstrap-degraded mode"
                                );
                                info!(
                                    window_start = %summary.window_start,
                                    wallets_seen = summary.wallets_seen,
                                    eligible_wallets = summary.eligible_wallets,
                                    metrics_written = summary.metrics_written,
                                    follow_promoted = summary.follow_promoted,
                                    follow_demoted = summary.follow_demoted,
                                    active_follow_wallets = summary.active_follow_wallets,
                                    swaps_window,
                                    swaps_query_rows = fetch_progress.query_rows,
                                    swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                                    swaps_delta_fetched = delta_fetched,
                                    swaps_warm_loaded,
                                    swaps_evicted_due_cap,
                                    swaps_fetch_limit = fetch_limit,
                                    swaps_fetch_pages = fetch_progress.pages,
                                    swaps_fetch_page_limit = fetch_page_limit,
                                    swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                                    swaps_fetch_limit_reached = fetch_progress.saturated,
                                    swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                                    swaps_fetch_time_budget_exhausted =
                                        fetch_progress.time_budget_exhausted,
                                    metrics_window_start = %metrics_window_start,
                                    scoring_source = summary.scoring_source,
                                    discovery_runtime_mode = summary.runtime_mode.as_str(),
                                    metrics_persisted = false,
                                    snapshot_recomputed = false,
                                    discovery_published = summary.published,
                                    discovery_cycle_duration_ms = elapsed_ms,
                                    "discovery cycle completed"
                                );
                                return Ok(RunCyclePreparedResolution::Returned(summary));
                            }
                            store.clear_discovery_persisted_rebuild_state()?;
                            let summary = self.fail_close_without_recent_universe(
                                store,
                                window_start,
                                metrics_window_start,
                                publish_due,
                                true,
                                &cap_truncation_telemetry,
                                empty_window_unusable_scoring_source,
                                empty_window_unusable_scoring_source,
                                now,
                            )?;
                            let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                            warn!(
                                metrics_window_start = %metrics_window_start,
                                scoring_source = summary.scoring_source,
                                cleared_follow_wallets = summary.follow_demoted,
                                "discovery fail-closed because persisted observed_swaps runtime fallback completed with zero rows and no recent published universe exists"
                            );
                            info!(
                                window_start = %summary.window_start,
                                wallets_seen = summary.wallets_seen,
                                eligible_wallets = summary.eligible_wallets,
                                metrics_written = summary.metrics_written,
                                follow_promoted = summary.follow_promoted,
                                follow_demoted = summary.follow_demoted,
                                active_follow_wallets = summary.active_follow_wallets,
                                swaps_window,
                                swaps_query_rows = fetch_progress.query_rows,
                                swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                                swaps_delta_fetched = delta_fetched,
                                swaps_warm_loaded,
                                swaps_evicted_due_cap,
                                swaps_fetch_limit = fetch_limit,
                                swaps_fetch_pages = fetch_progress.pages,
                                swaps_fetch_page_limit = fetch_page_limit,
                                swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                                swaps_fetch_limit_reached = fetch_progress.saturated,
                                swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                                swaps_fetch_time_budget_exhausted = fetch_progress.time_budget_exhausted,
                                metrics_window_start = %metrics_window_start,
                                scoring_source = summary.scoring_source,
                                discovery_runtime_mode = summary.runtime_mode.as_str(),
                                metrics_persisted = false,
                                snapshot_recomputed = false,
                                discovery_published = summary.published,
                                discovery_cycle_duration_ms = elapsed_ms,
                                "discovery cycle completed"
                            );
                            return Ok(RunCyclePreparedResolution::Returned(summary));
                        }
                        (
                            true,
                            false,
                            false,
                            false,
                            snapshots,
                            telemetry.observed_swaps_loaded,
                            scoring_source,
                            telemetry.window_start,
                            telemetry.metrics_window_start,
                        )
}
,
                    PersistedStreamRebuildAdvanceOutcome::InProgress { telemetry } => {
                        return self.resolve_run_cycle_persisted_recompute_in_progress(
                            store,
                            now,
                            cycle_started,
                            window_start,
                            metrics_window_start,
                            fetch_limit,
                            fetch_page_limit,
                            swaps_window,
                            fetch_progress,
                            delta_fetched,
                            swaps_warm_loaded,
                            swaps_evicted_due_cap,
                            cap_truncation_telemetry,
                            empty_window_degraded_scoring_source,
                            empty_window_bootstrap_degraded_scoring_source,
                            empty_window_unusable_scoring_source,
                            recent_published_follow_wallets,
                            bootstrap_degraded_follow_wallets,
                            telemetry,
                        );
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
