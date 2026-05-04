impl DiscoveryService {
    fn complete_healthy_run_cycle(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        cycle_started: Instant,
        fetch_limit: usize,
        fetch_page_limit: usize,
        swaps_window: usize,
        fetch_progress: &FetchProgress,
        delta_fetched: usize,
        swaps_warm_loaded: usize,
        swaps_evicted_due_cap: usize,
        cap_truncation_telemetry: &CapTruncationTelemetrySnapshot,
        healthy_inputs: RunCycleHealthyInputs,
    ) -> Result<DiscoverySummary> {
        let RunCycleHealthyInputs {
            publish_due,
            followlist_activations_suppressed,
            followlist_deactivations_suppressed,
            metrics_persistence_suppressed,
            snapshots,
            observed_swaps_loaded_for_capture,
            scoring_source,
            effective_window_start,
            effective_metrics_window_start,
        } = healthy_inputs;
        if scoring_source != "raw_window_persisted_stream" {
            store.clear_discovery_persisted_rebuild_state()?;
        }
        let mut wallet_rows: Vec<WalletUpsertRow> = Vec::with_capacity(snapshots.len());
        let mut metric_rows: Vec<WalletMetricRow> = Vec::with_capacity(snapshots.len());
        for snapshot in snapshots.iter() {
            let status = if snapshot.eligible {
                "candidate"
            } else {
                "observed"
            };
            wallet_rows.push(WalletUpsertRow {
                wallet_id: snapshot.wallet_id.clone(),
                first_seen: snapshot.first_seen,
                last_seen: snapshot.last_seen,
                status: status.to_string(),
            });
            metric_rows.push(WalletMetricRow {
                wallet_id: snapshot.wallet_id.clone(),
                window_start: effective_metrics_window_start,
                pnl: snapshot.pnl_sol,
                win_rate: snapshot.win_rate,
                trades: snapshot.trades,
                closed_trades: snapshot.closed_trades,
                hold_median_seconds: snapshot.hold_median_seconds,
                score: snapshot.score,
                buy_total: snapshot.buy_total,
                tradable_ratio: snapshot.tradable_ratio,
                rug_ratio: snapshot.rug_ratio,
            });
        }

        let should_persist_metrics =
            !store.wallet_metrics_window_exists(effective_metrics_window_start)?;
        let metrics_to_persist = if should_persist_metrics && !metrics_persistence_suppressed {
            metric_rows.as_slice()
        } else {
            &[]
        };
        let snapshot_write = (!metrics_to_persist.is_empty()).then(|| {
            trusted_snapshot_write(
                TrustedSnapshotSourceKind::DiscoveryRefresh,
                TrustedSelectionState::TrustedCurrent,
                effective_metrics_window_start,
                now,
                metrics_to_persist.len(),
                None,
                Some(effective_metrics_window_start),
            )
        });

        let ranked = rank_follow_candidates(&snapshots, self.config.min_score);
        let publish_pending_state = if scoring_source == "raw_window_persisted_stream" {
            store
                .load_discovery_persisted_rebuild_state()?
                .map(Self::persisted_stream_rebuild_state_from_row)
                .transpose()?
                .filter(|state| state.phase == DiscoveryPersistedRebuildPhase::PublishPending)
        } else {
            None
        };
        let desired_wallets = if let Some(state) = publish_pending_state.as_ref() {
            self.publish_pending_requested_wallet_ids_from_state(state)
        } else {
            desired_wallets(&ranked, self.config.follow_top_n)
        };
        let current_raw_for_capture = PrecomputedWalletFreshnessCurrentRawTruth {
            window_start: effective_window_start,
            observed_swaps_loaded: observed_swaps_loaded_for_capture,
            eligible_wallet_count: ranked.len(),
            top_wallet_ids: desired_wallets.clone(),
        };
        let follow_delta = store.persist_discovery_cycle_with_snapshot_metadata(
            &wallet_rows,
            metrics_to_persist,
            &desired_wallets,
            publish_due && !followlist_activations_suppressed,
            publish_due && !followlist_deactivations_suppressed,
            now,
            "discovery_score_refresh",
            snapshot_write.as_ref(),
        )?;
        let active_follow_wallets = store.list_active_follow_wallets()?.len();
        let top_wallets = top_wallet_labels(&ranked, 5);

        let mut summary = DiscoverySummary {
            window_start: effective_window_start,
            wallets_seen: snapshots.len(),
            eligible_wallets: ranked.len(),
            metrics_written: metrics_to_persist.len(),
            follow_promoted: follow_delta.activated,
            follow_demoted: follow_delta.deactivated,
            active_follow_wallets,
            top_wallets,
            published: publish_due,
            ..DiscoverySummary::default()
        }
        .with_runtime_mode(DiscoveryRuntimeMode::Healthy)
        .with_scoring_source(scoring_source)
        .with_cap_truncation_telemetry(&cap_truncation_telemetry);
        let diagnostics = self.snapshot_run_cycle_publication_boundary_diagnostics(
            store,
            if scoring_source == "raw_window_persisted_stream" {
                "healthy_recompute_from_persisted_rebuild"
            } else {
                "healthy_recompute_from_raw_window"
            },
            publish_due,
            true,
        )?;
        Self::log_run_cycle_publication_boundary(&diagnostics);
        let publication_outcome = self.persist_publication_state(
            store,
            DiscoveryRuntimeMode::Healthy,
            publish_due,
            effective_metrics_window_start,
            Some(&desired_wallets),
            scoring_source,
            "discovery_score_refresh",
            now,
        )?;
        if publish_due && publication_outcome.published_universe_persisted {
            self.record_live_publish(now);
        }
        summary.published = publish_due && publication_outcome.published_universe_persisted;
        summary = summary.with_runtime_mode(publication_outcome.runtime_mode);
        let capture_telemetry = self.maybe_persist_in_band_wallet_freshness_capture(
            store,
            now,
            publish_due,
            summary.runtime_mode,
            Some(current_raw_for_capture.clone()),
        );
        let summary = summary.with_wallet_freshness_capture(&capture_telemetry);
        {
            let mut state = match self.window_state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    warn!("discovery window mutex poisoned while caching summary; continuing");
                    poisoned.into_inner()
                }
            };
            state.last_snapshot_bucket = Some(effective_metrics_window_start);
            state.last_summary = Some(summary.clone());
            state.last_exact_current_raw_truth = Some(CachedCurrentRawTruthSample {
                window_start: current_raw_for_capture.window_start,
                observed_swaps_loaded: current_raw_for_capture.observed_swaps_loaded,
                eligible_wallet_count: current_raw_for_capture.eligible_wallet_count,
                top_wallet_ids: current_raw_for_capture.top_wallet_ids.clone(),
            });
        }
        if let Some(snapshot_write) = snapshot_write.as_ref() {
            self.persist_trusted_selection_state_from_snapshot(
                store,
                snapshot_write,
                false,
                "discovery_score_refresh",
                now,
            )?;
        } else if let Some(metadata) = store
            .trusted_wallet_metrics_snapshot_metadata_for_window(effective_metrics_window_start)?
        {
            self.persist_trusted_selection_state(
                store,
                metadata.trust_state,
                Some(metadata.snapshot_id),
                Some(metadata.effective_window_start),
                Some(metadata.source_kind),
                false,
                "discovery_score_refresh",
                now,
            )?;
        } else {
            self.persist_trusted_selection_state(
                store,
                TrustedSelectionState::TrustedCurrent,
                None,
                Some(effective_metrics_window_start),
                Some(TrustedSnapshotSourceKind::Legacy),
                false,
                "discovery_score_refresh_legacy",
                now,
            )?;
        }
        store.set_discovery_bootstrap_degraded_state(false, None, None)?;
        if scoring_source == "raw_window_persisted_stream"
            && publication_outcome.published_universe_persisted
        {
            store.clear_discovery_persisted_rebuild_state()?;
        } else if scoring_source == "raw_window_persisted_stream" {
            let publish_pending_blocker = publish_pending_state
                .as_ref()
                .map(Self::persisted_stream_publishable_checkpoint_blocker_from_state)
                .unwrap_or_else(|| {
                    if desired_wallets.is_empty() {
                        "publish_pending_exact_publish_set_empty"
                    } else {
                        "publish_pending_flush"
                    }
                });
            warn!(
                publication_truth_flush_withheld = true,
                publication_runtime_mode = summary.runtime_mode.as_str(),
                publication_requested_wallet_count = desired_wallets.len(),
                rebuild_publishable_checkpoint_blocker = publish_pending_blocker,
                "retaining the persisted publish-pending checkpoint because exact publication truth was not materialized"
            );
        }
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
            swaps_fetch_time_budget_exhausted = fetch_progress.time_budget_exhausted,
            metrics_window_start = %effective_metrics_window_start,
            scoring_source,
            metrics_persisted = should_persist_metrics,
            snapshot_recomputed = true,
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

        Ok(summary)

    }
}
