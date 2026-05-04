{
                let diagnostics = self.snapshot_run_cycle_publication_boundary_diagnostics(
                    store,
                    "bootstrap_degraded",
                    false,
                    false,
                )?;
                Self::log_run_cycle_publication_boundary(&diagnostics);
                let summary = self.bootstrap_degraded_summary_from_published_universe(
                    store,
                    window_start,
                    active_wallets,
                    &cap_truncation_telemetry,
                    scoring_source,
                    scoring_source,
                    now,
                )?;
                store.clear_discovery_persisted_rebuild_state()?;
                let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                warn!(
                    metrics_window_start = %metrics_window_start,
                    scoring_source = summary.scoring_source,
                    bootstrap_degraded_active = true,
                    "discovery runtime remains in explicit bootstrap-degraded mode while fresh raw truth is unavailable"
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
                    top_wallets = ?summary.top_wallets,
                    "discovery cycle completed"
                );
                return Ok(RunCyclePreparedResolution::Returned(summary));
}
