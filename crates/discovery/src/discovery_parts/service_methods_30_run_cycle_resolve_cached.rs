{
                let diagnostics = self.snapshot_run_cycle_publication_boundary_diagnostics(
                    store,
                    "cached",
                    publish_due,
                    previous_summary.runtime_mode != DiscoveryRuntimeMode::BootstrapDegraded,
                )?;
                Self::log_run_cycle_publication_boundary(&diagnostics);
                let active_follow_wallets = store.list_active_follow_wallets()?.len();
                let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
                let mut summary = DiscoverySummary {
                    window_start,
                    wallets_seen: previous_summary.wallets_seen,
                    eligible_wallets: previous_summary.eligible_wallets,
                    metrics_written: 0,
                    follow_promoted: 0,
                    follow_demoted: 0,
                    active_follow_wallets,
                    top_wallets: previous_summary.top_wallets.clone(),
                    published: publish_due,
                    ..DiscoverySummary::default()
                }
                .with_runtime_mode(previous_summary.runtime_mode)
                .with_scoring_source(previous_summary.scoring_source)
                .with_cap_truncation_telemetry(&cap_truncation_telemetry);
                let exact_empty_current_raw = current_raw
                    .as_ref()
                    .is_some_and(|current_raw| current_raw.top_wallet_ids.is_empty());
                if exact_empty_current_raw {
                    if let Some(current_raw) = current_raw.as_ref() {
                        summary.eligible_wallets = current_raw.eligible_wallet_count;
                        summary.top_wallets.clear();
                    }
                    summary = summary.with_runtime_mode(DiscoveryRuntimeMode::FailClosed);
                }
                if summary.runtime_mode != DiscoveryRuntimeMode::BootstrapDegraded {
                    let publication_write_due = publish_due || exact_empty_current_raw;
                    let publication_reason = if exact_empty_current_raw {
                        RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON
                    } else {
                        summary.scoring_source
                    };
                    let publication_outcome = self.persist_publication_state(
                        store,
                        summary.runtime_mode,
                        publication_write_due,
                        metrics_window_start,
                        Self::cached_cycle_exact_published_wallet_ids(
                            summary.runtime_mode,
                            current_raw.as_ref(),
                        ),
                        summary.scoring_source,
                        publication_reason,
                        now,
                    )?;
                    if publish_due && publication_outcome.published_universe_persisted {
                        self.record_live_publish(now);
                    }
                    summary.published =
                        publish_due && publication_outcome.published_universe_persisted;
                    summary = summary.with_runtime_mode(publication_outcome.runtime_mode);
                }
                let fail_closed_zero_universe_capture = exact_empty_current_raw
                    && summary.scoring_source == "raw_window"
                    && summary.runtime_mode == DiscoveryRuntimeMode::FailClosed
                    && summary.eligible_wallets == 0
                    && summary.active_follow_wallets == 0
                    && summary.wallets_seen > 0
                    && current_raw.as_ref().is_some_and(|current_raw| {
                        current_raw.observed_swaps_loaded > 0
                            && current_raw.eligible_wallet_count == 0
                            && current_raw.top_wallet_ids.is_empty()
                    });
                let capture_telemetry = if fail_closed_zero_universe_capture {
                    self.persist_cached_zero_universe_wallet_freshness_capture(
                        store,
                        now,
                        &summary,
                        current_raw
                            .as_ref()
                            .expect("zero-universe capture requires cached current_raw"),
                    )
                } else {
                    self.maybe_persist_in_band_wallet_freshness_capture(
                        store,
                        now,
                        publish_due,
                        summary.runtime_mode,
                        current_raw.map(|current_raw| PrecomputedWalletFreshnessCurrentRawTruth {
                            window_start: current_raw.window_start,
                            observed_swaps_loaded: current_raw.observed_swaps_loaded,
                            eligible_wallet_count: current_raw.eligible_wallet_count,
                            top_wallet_ids: current_raw.top_wallet_ids,
                        }),
                    )
                };
                let summary = summary.with_wallet_freshness_capture(&capture_telemetry);
                store.clear_discovery_persisted_rebuild_state()?;
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
                    metrics_persisted = false,
                    snapshot_recomputed = false,
                    discovery_published = summary.published,
                    followlist_activations_suppressed,
                    followlist_deactivations_suppressed,
                    discovery_cycle_duration_ms = elapsed_ms,
                    wallet_freshness_capture_state = summary.wallet_freshness_capture_state,
                    wallet_freshness_capture_reason = summary.wallet_freshness_capture_reason.as_deref(),
                    wallet_freshness_capture_id = summary.wallet_freshness_capture_id,
                    wallet_freshness_capture_captured_at = ?summary.wallet_freshness_capture_captured_at,
                    top_wallets = ?summary.top_wallets,
                    "discovery cycle completed"
                );
                if fetch_progress.saturated {
                    warn!(
                        swaps_query_rows = fetch_progress.query_rows,
                        swaps_query_rows_last_page = fetch_progress.query_rows_last_page,
                        swaps_fetch_limit = fetch_limit,
                        swaps_fetch_pages = fetch_progress.pages,
                        swaps_fetch_page_limit = fetch_page_limit,
                        swaps_fetch_time_budget_ms = self.config.fetch_time_budget_ms,
                        swaps_fetch_page_budget_exhausted = fetch_progress.page_budget_exhausted,
                        swaps_fetch_time_budget_exhausted = fetch_progress.time_budget_exhausted,
                        "discovery swap fetch exhausted bounded per-cycle budget; backlog processing continues next cycle"
                    );
                }
                return Ok(RunCyclePreparedResolution::Returned(summary));
}
