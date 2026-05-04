{
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
