{
                        let catch_up_requested =
                            should_request_persisted_stream_catch_up(&telemetry);
                        let catch_up_pressure_override_requested =
                            should_request_persisted_stream_catch_up_pressure_override(&telemetry);
                        if let Some(active_wallets) = recent_published_follow_wallets.clone() {
                            let summary = self
                                .degraded_summary_from_published_universe(
                                    store,
                                    window_start,
                                    metrics_window_start,
                                    false,
                                    active_wallets,
                                    &cap_truncation_telemetry,
                                    empty_window_degraded_scoring_source,
                                    empty_window_degraded_scoring_source,
                                    now,
                                )?
                                .with_persisted_stream_catch_up_requested(false)
                                .with_persisted_stream_catch_up_pressure_override_requested(false);
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
                                rebuild_phase = telemetry.phase.as_str(),
                                rebuild_horizon_end = %telemetry.horizon_end,
                                discovery_persisted_stream_catch_up_requested =
                                    summary.persisted_stream_catch_up_requested,
                                rebuild_budget_exhausted_reason = telemetry
                                    .budget_exhausted_reason
                                    .map(PersistedStreamBudgetExhaustedReason::as_str),
                                "discovery cycle completed"
                            );
                            return Ok(RunCyclePreparedResolution::Returned(summary));
                        }
                        if let Some(active_wallets) = bootstrap_degraded_follow_wallets.clone() {
                            let summary = self
                                .bootstrap_degraded_summary_from_published_universe(
                                    store,
                                    window_start,
                                    active_wallets,
                                    &cap_truncation_telemetry,
                                    empty_window_bootstrap_degraded_scoring_source,
                                    empty_window_bootstrap_degraded_scoring_source,
                                    now,
                                )?
                                .with_persisted_stream_catch_up_requested(catch_up_requested)
                                .with_persisted_stream_catch_up_pressure_override_requested(
                                    catch_up_pressure_override_requested,
                                );
                            let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                            warn!(
                                metrics_window_start = %metrics_window_start,
                                scoring_source = summary.scoring_source,
                                bootstrap_degraded_active = true,
                                rebuild_phase = telemetry.phase.as_str(),
                                rebuild_horizon_end = %telemetry.horizon_end,
                                rebuild_budget_exhausted_reason = telemetry
                                    .budget_exhausted_reason
                                    .map(PersistedStreamBudgetExhaustedReason::as_str),
                                "discovery runtime remains in explicit bootstrap-degraded mode while bounded persisted rebuild continues"
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
                                rebuild_phase = telemetry.phase.as_str(),
                                rebuild_horizon_end = %telemetry.horizon_end,
                                discovery_persisted_stream_catch_up_requested =
                                    summary.persisted_stream_catch_up_requested,
                                rebuild_budget_exhausted_reason = telemetry
                                    .budget_exhausted_reason
                                    .map(PersistedStreamBudgetExhaustedReason::as_str),
                                "discovery cycle completed"
                            );
                            return Ok(RunCyclePreparedResolution::Returned(summary));
                        }
                        let summary = self
                            .fail_close_without_recent_universe(
                                store,
                                window_start,
                                metrics_window_start,
                                false,
                                true,
                                &cap_truncation_telemetry,
                                empty_window_unusable_scoring_source,
                                empty_window_unusable_scoring_source,
                                now,
                            )?
                            .with_persisted_stream_catch_up_requested(catch_up_requested)
                            .with_persisted_stream_catch_up_pressure_override_requested(
                                catch_up_pressure_override_requested,
                            );
                        let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                        warn!(
                            metrics_window_start = %metrics_window_start,
                            scoring_source = summary.scoring_source,
                            cleared_follow_wallets = summary.follow_demoted,
                            rebuild_phase = telemetry.phase.as_str(),
                            rebuild_horizon_end = %telemetry.horizon_end,
                            rebuild_budget_exhausted_reason = telemetry
                                .budget_exhausted_reason
                                .map(PersistedStreamBudgetExhaustedReason::as_str),
                            "discovery fail-closed while bounded persisted observed_swaps rebuild continues without a recent published universe"
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
                            rebuild_phase = telemetry.phase.as_str(),
                            rebuild_horizon_end = %telemetry.horizon_end,
                            discovery_persisted_stream_catch_up_requested =
                                summary.persisted_stream_catch_up_requested,
                            rebuild_budget_exhausted_reason = telemetry
                                .budget_exhausted_reason
                                .map(PersistedStreamBudgetExhaustedReason::as_str),
                            "discovery cycle completed"
                        );
                        return Ok(RunCyclePreparedResolution::Returned(summary));
}
