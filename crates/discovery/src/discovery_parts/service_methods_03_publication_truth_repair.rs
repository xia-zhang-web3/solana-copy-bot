use super::*;

impl DiscoveryService {
    pub(crate) fn repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed_with_options(
        &self,
        runtime_store: &SqliteStore,
        journal_store: Option<&SqliteStore>,
        now: DateTime<Utc>,
        replay_batch_size: usize,
        deadline: Instant,
        resume_exact_target_surface_repair_deadline_override: Option<Instant>,
        trace_collector: Option<DiscoveryPublicationTruthRepairTraceCollector>,
        defer_resume_exact_target_surface_repair_to_runtime_cycle: bool,
    ) -> Result<DiscoveryPublicationTruthRepairTelemetry> {
        let gate = self.publication_freshness_gate();
        let required_window_start = now - Duration::days(self.runtime_scoring_window_days());
        let publication_state = runtime_store.discovery_publication_state_read_only()?;
        let publication_state_exists_before = publication_state.is_some();
        let publication_truth_complete_before = publication_state
            .as_ref()
            .is_some_and(DiscoveryPublicationStateRow::has_complete_publication_truth);
        let publication_truth_fresh_before = publication_state
            .as_ref()
            .is_some_and(|state| state.is_fresh_under_gate(&gate, now));
        let runtime_cursor_before = runtime_store.load_discovery_runtime_cursor()?;
        let runtime_cursor_exists_before = runtime_cursor_before.is_some();
        let journal_store_exists = journal_store.is_some();
        let required_window_ahead_of_runtime_cursor = runtime_cursor_before
            .as_ref()
            .is_some_and(|cursor| required_window_start > cursor.ts_utc);
        let runtime_window_complete_before = if required_window_ahead_of_runtime_cursor {
            false
        } else {
            self.persisted_observed_swaps_cover_window(runtime_store, required_window_start)?
        };
        let trace_context = DiscoveryPublicationTruthRepairTraceContext::new(
            publication_state_exists_before,
            publication_truth_complete_before,
            publication_truth_fresh_before,
            runtime_window_complete_before,
            runtime_cursor_exists_before,
            deadline,
            trace_collector,
        );
        info!(
            publication_truth_repair_trace_id = trace_context.trace_id,
            publication_state_exists_before,
            publication_truth_complete_before,
            publication_truth_fresh_before,
            runtime_window_complete_before,
            runtime_cursor_exists_before,
            journal_store_exists,
            "discovery publication truth repair helper entered"
        );
        let _helper_region = DiscoveryPublicationTruthRepairRegionScope::new(
            Some(&trace_context),
            "repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed_with_options",
            None,
        );
        let log_and_return = |telemetry: DiscoveryPublicationTruthRepairTelemetry| {
            let telemetry = telemetry.with_entry_context(
                publication_state_exists_before,
                runtime_cursor_exists_before,
                journal_store_exists,
            );
            Self::log_publication_truth_repair_helper_return(&telemetry, Some(&trace_context));
            Ok(telemetry)
        };
        if let Some(runtime_cursor) = runtime_cursor_before
            .as_ref()
            .filter(|cursor| required_window_start > cursor.ts_utc)
        {
            return log_and_return(
                DiscoveryPublicationTruthRepairTelemetry::skipped(
                    "skipped_recent_raw_journal_required_window_ahead_of_runtime_cursor",
                    "required_window_start_is_after_runtime_cursor",
                    required_window_start,
                    publication_truth_complete_before,
                    publication_truth_fresh_before,
                    runtime_window_complete_before,
                    None,
                    false,
                )
                .with_replay_window_context(None, Some(runtime_cursor.clone())),
            );
        }
        if publication_truth_fresh_before {
            return log_and_return(DiscoveryPublicationTruthRepairTelemetry::skipped(
                "skipped_fresh_publication_truth",
                "publication_truth_already_fresh_under_gate",
                required_window_start,
                publication_truth_complete_before,
                publication_truth_fresh_before,
                runtime_window_complete_before,
                None,
                false,
            ));
        }
        if runtime_window_complete_before {
            let refresh_budget = deadline.saturating_duration_since(Instant::now());
            if refresh_budget.is_zero() {
                return log_and_return(DiscoveryPublicationTruthRepairTelemetry::skipped(
                    "skipped_runtime_window_truth_refresh_budget_exhausted",
                    "runtime_window_complete_but_publication_truth_refresh_budget_exhausted",
                    required_window_start,
                    publication_truth_complete_before,
                    publication_truth_fresh_before,
                    runtime_window_complete_before,
                    None,
                    false,
                ));
            }

            let metrics_window_start = self.metrics_window_start(now);
            let fetch_limit = self.config.max_fetch_swaps_per_cycle.max(1);
            let fetch_page_limit = self.config.max_fetch_pages_per_cycle.max(1);
            let base_recovery_contract = {
                let _region = DiscoveryPublicationTruthRepairRegionScope::new(
                    Some(&trace_context),
                    "persisted_stream_priority_recovery_contract",
                    Some("repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed_with_options"),
                );
                self.persisted_stream_priority_recovery_contract(
                    runtime_store,
                    now,
                    fetch_limit,
                    fetch_page_limit,
                    refresh_budget,
                )?
            };
            let (state, _, resume_exact_target_surface_repair) = self
                .load_or_start_persisted_stream_rebuild_state_with_options(
                    runtime_store,
                    required_window_start,
                    metrics_window_start,
                    now,
                    Some(resume_exact_target_surface_repair_deadline_override.unwrap_or(deadline)),
                    Some(&trace_context),
                    defer_resume_exact_target_surface_repair_to_runtime_cycle,
                )?;
            let recovery_contract = self
                .deepen_persisted_stream_priority_recovery_contract_for_state_at(
                    &state,
                    fetch_limit,
                    fetch_page_limit,
                    base_recovery_contract,
                    Some(now),
                );
            let telemetry = self.persisted_stream_progress_telemetry_from_state(&state, now);
            let checkpoint_blocker =
                Self::persisted_stream_publishable_checkpoint_blocker(&telemetry);
            let helper_write_attempted = publication_state_exists_before;
            info!(
                publication_state_write_attempted_from_helper = helper_write_attempted,
                publication_state_exists_before,
                publication_truth_refresh_publishable_checkpoint_blocker = checkpoint_blocker,
                "discovery publication truth repair deferred branch write attempt"
            );
            match self.refresh_fail_closed_publication_runtime_surface_for_deferred_runtime_cycle(
                runtime_store,
                publication_state.as_ref(),
                checkpoint_blocker,
            ) {
                Ok(()) => {
                    let publication_state_after =
                        runtime_store.discovery_publication_state_read_only()?;
                    let resulting_reason = publication_state_after
                        .as_ref()
                        .map(|state| state.reason.clone());
                    let resulting_updated_at = publication_state_after
                        .as_ref()
                        .map(|state| state.updated_at);
                    info!(
                        publication_state_write_attempted_from_helper =
                            helper_write_attempted,
                        publication_state_exists_before,
                        publication_truth_refresh_publishable_checkpoint_blocker =
                            checkpoint_blocker,
                        publication_state_write_succeeded_from_helper =
                            helper_write_attempted,
                        publication_state_write_resulting_reason = ?resulting_reason,
                        publication_state_write_resulting_updated_at = ?resulting_updated_at
                            .map(|ts| ts.to_rfc3339()),
                        "discovery publication truth repair deferred branch write attempt completed"
                    );
                    return log_and_return(
                        DiscoveryPublicationTruthRepairTelemetry::deferred_to_runtime_cycle(
                            required_window_start,
                            publication_truth_complete_before,
                            publication_truth_fresh_before,
                            runtime_window_complete_before,
                            recovery_contract,
                            &telemetry,
                        )
                        .with_resume_exact_target_surface_repair(
                            &resume_exact_target_surface_repair,
                        )
                        .with_helper_write_result(
                            helper_write_attempted,
                            helper_write_attempted,
                            resulting_reason,
                            resulting_updated_at,
                        ),
                    );
                }
                Err(error) => {
                    warn!(
                        publication_state_write_attempted_from_helper =
                            helper_write_attempted,
                        publication_state_exists_before,
                        publication_truth_refresh_publishable_checkpoint_blocker =
                            checkpoint_blocker,
                        publication_state_write_succeeded_from_helper = false,
                        error = %error,
                        "discovery publication truth repair deferred branch write attempt failed"
                    );
                    return Err(error);
                }
            }
        }

        {
        let Some(runtime_cursor) = runtime_cursor_before else {
            return log_and_return(DiscoveryPublicationTruthRepairTelemetry::skipped(
                "skipped_missing_runtime_cursor",
                "discovery_runtime_cursor_missing",
                required_window_start,
                publication_truth_complete_before,
                publication_truth_fresh_before,
                runtime_window_complete_before,
                None,
                false,
            ));
        };

        let Some(journal_store) = journal_store else {
            return log_and_return(DiscoveryPublicationTruthRepairTelemetry::skipped(
                "skipped_journal_unavailable",
                "recent_raw_journal_unavailable",
                required_window_start,
                publication_truth_complete_before,
                publication_truth_fresh_before,
                runtime_window_complete_before,
                None,
                false,
            ));
        };

        let journal_state = journal_store.recent_raw_journal_state_read_only()?;
        if journal_state.row_count == 0 {
            return log_and_return(DiscoveryPublicationTruthRepairTelemetry::skipped(
                "skipped_journal_unavailable",
                "recent_raw_journal_unavailable",
                required_window_start,
                publication_truth_complete_before,
                publication_truth_fresh_before,
                runtime_window_complete_before,
                journal_state.covered_since,
                false,
            ));
        }
        if !journal_state
            .covered_since
            .is_some_and(|covered_since| covered_since <= required_window_start)
        {
            return log_and_return(DiscoveryPublicationTruthRepairTelemetry::skipped(
                "skipped_journal_window_unavailable",
                "recent_raw_journal_does_not_cover_required_window_start",
                required_window_start,
                publication_truth_complete_before,
                publication_truth_fresh_before,
                runtime_window_complete_before,
                journal_state.covered_since,
                false,
            ));
        }
        if required_window_start > runtime_cursor.ts_utc {
            return log_and_return(
                DiscoveryPublicationTruthRepairTelemetry::skipped(
                    "skipped_recent_raw_journal_required_window_ahead_of_runtime_cursor",
                    "required_window_start_is_after_runtime_cursor",
                    required_window_start,
                    publication_truth_complete_before,
                    publication_truth_fresh_before,
                    runtime_window_complete_before,
                    journal_state.covered_since,
                    false,
                )
                .with_replay_window_context(None, Some(runtime_cursor)),
            );
        }
        let journal_metadata_covers_runtime_cursor = journal_state
            .covered_through_cursor
            .as_ref()
            .is_some_and(|cursor| {
                Self::runtime_cursor_cmp(cursor, &runtime_cursor) != Ordering::Less
            });
        if !journal_metadata_covers_runtime_cursor {
            return log_and_return(DiscoveryPublicationTruthRepairTelemetry::skipped(
                "skipped_journal_cursor_lineage_mismatch",
                "recent_raw_journal_does_not_cover_runtime_cursor",
                required_window_start,
                publication_truth_complete_before,
                publication_truth_fresh_before,
                runtime_window_complete_before,
                journal_state.covered_since,
                false,
            ));
        }

        let (runtime_window_first_cursor, _runtime_window_first_cursor_time_budget_exhausted) =
            self.first_observed_swap_cursor_in_window(
                runtime_store,
                required_window_start,
                &runtime_cursor,
                deadline,
            )?;
        let replay_until_cursor = runtime_window_first_cursor
            .clone()
            .unwrap_or_else(|| runtime_cursor.clone());
        if Self::runtime_cursor_cmp(&replay_until_cursor, &runtime_cursor) == Ordering::Greater {
            return log_and_return(DiscoveryPublicationTruthRepairTelemetry::skipped(
                "skipped_runtime_window_cursor_invalid",
                "runtime_window_floor_exceeds_runtime_cursor",
                required_window_start,
                publication_truth_complete_before,
                publication_truth_fresh_before,
                runtime_window_complete_before,
                journal_state.covered_since,
                journal_metadata_covers_runtime_cursor,
            ));
        }
        let (journal_window_first_cursor, journal_window_first_cursor_time_budget_exhausted) = self
            .first_observed_swap_cursor_in_window(
                journal_store,
                required_window_start,
                &replay_until_cursor,
                deadline,
            )?;
        if journal_window_first_cursor.is_none()
            && !journal_window_first_cursor_time_budget_exhausted
        {
            return log_and_return(
                DiscoveryPublicationTruthRepairTelemetry::skipped(
                    "skipped_recent_raw_journal_repair_interval_unavailable",
                    "recent_raw_journal_does_not_cover_repairable_interval_before_runtime_cursor",
                    required_window_start,
                    publication_truth_complete_before,
                    publication_truth_fresh_before,
                    runtime_window_complete_before,
                    journal_state.covered_since,
                    false,
                )
                .with_replay_window_context(runtime_window_first_cursor, Some(replay_until_cursor)),
            );
        }
        let journal_covers_runtime_cursor = true;

        let mut replay_cursor: Option<DiscoveryRuntimeCursor> = None;
        let mut replay_batches_completed = 0usize;
        let mut replay_rows_loaded = 0usize;
        let mut replay_rows_inserted = 0usize;
        let mut replay_time_budget_exhausted = false;
        let page_limit = replay_batch_size.max(1);

        while Instant::now() < deadline {
            let mut batch = Vec::with_capacity(page_limit);
            let mut page_last_cursor = replay_cursor.clone();
            let page = journal_store.for_each_observed_swap_in_window_after_cursor_with_budget(
                required_window_start,
                replay_until_cursor.ts_utc,
                replay_cursor.as_ref(),
                page_limit,
                deadline,
                |swap| {
                    page_last_cursor = Some(Self::runtime_cursor_from_swap(&swap));
                    batch.push(swap);
                    Ok(())
                },
            )?;
            if batch.is_empty() {
                replay_time_budget_exhausted = page.time_budget_exhausted;
                break;
            }
            replay_rows_loaded = replay_rows_loaded.saturating_add(page.rows_seen);
            replay_rows_inserted = replay_rows_inserted.saturating_add(
                runtime_store
                    .insert_observed_swaps_batch_with_activity_days(&batch)?
                    .into_iter()
                    .filter(|inserted| *inserted)
                    .count(),
            );
            replay_batches_completed = replay_batches_completed.saturating_add(1);
            replay_cursor = page_last_cursor;
            if page.rows_seen < page_limit && !page.time_budget_exhausted {
                break;
            }
            if page.time_budget_exhausted {
                replay_time_budget_exhausted = true;
                break;
            }
        }

        if replay_batches_completed == 0
            && replay_rows_loaded == 0
            && replay_rows_inserted == 0
            && Instant::now() >= deadline
        {
            replay_time_budget_exhausted = true;
        }

        let runtime_window_complete_after =
            self.persisted_observed_swaps_cover_window(runtime_store, required_window_start)?;
        let zero_effective_replay_work = !runtime_window_complete_after
            && replay_batches_completed == 0
            && replay_rows_loaded == 0
            && replay_rows_inserted == 0;
        return log_and_return(DiscoveryPublicationTruthRepairTelemetry {
            state: if runtime_window_complete_after {
                "replayed_recent_raw_journal_head_gap"
            } else if zero_effective_replay_work {
                "skipped_recent_raw_journal_head_gap_zero_effective_replay_work"
            } else {
                "replayed_recent_raw_journal_head_gap_partial"
            },
            reason: zero_effective_replay_work.then(|| {
                if replay_time_budget_exhausted {
                    "recent_raw_journal_head_gap_replay_budget_exhausted_before_first_batch"
                } else {
                    "recent_raw_journal_head_gap_replay_produced_zero_effective_work_before_first_batch"
                }
                .to_string()
            }),
            required_window_start,
            journal_covered_since: journal_state.covered_since,
            journal_covers_runtime_cursor,
            publication_state_exists_before,
            publication_truth_complete_before,
            publication_truth_fresh_before,
            runtime_cursor_exists_before,
            journal_store_exists,
            runtime_window_complete_before,
            runtime_window_complete_after,
            runtime_window_first_cursor,
            replay_until_cursor: Some(replay_until_cursor),
            replay_batches_completed,
            replay_rows_loaded,
            replay_rows_inserted,
            replay_time_budget_exhausted,
            publication_truth_refresh_attempted: false,
            publication_truth_refresh_completed: false,
            publication_truth_refresh_phase: None,
            publication_truth_refresh_replay_subphase: None,
            publication_truth_refresh_replay_wallet_stats_complete: false,
            publication_truth_refresh_replay_wallet_stats_wallet_cursor: None,
            publication_truth_refresh_delegated_to_runtime_cycle: false,
            publication_truth_refresh_priority_recovery_contract_reason: None,
            publication_truth_refresh_publishable_checkpoint_blocker: None,
            publication_truth_refresh_effective_time_budget_ms: None,
            publication_truth_refresh_collect_buy_mints_phase_page_limit: None,
            publication_truth_refresh_replay_wallet_stats_phase_page_limit: None,
            publication_truth_refresh_replay_sol_leg_phase_page_limit: None,
            publication_truth_refresh_observed_swaps_loaded: 0,
            publication_truth_refresh_replay_rows_processed: 0,
            publication_truth_refresh_replay_pages_processed: 0,
            publication_truth_refresh_wallets_buffered: 0,
            publication_truth_refresh_cycle_rows_processed: 0,
            publication_truth_refresh_cycle_pages_processed: 0,
            publication_truth_refresh_budget_exhausted_reason: None,
            publication_truth_refresh_helper_write_attempted: false,
            publication_truth_refresh_helper_write_succeeded: false,
            publication_truth_refresh_helper_write_resulting_reason: None,
            publication_truth_refresh_helper_write_resulting_updated_at: None,
            publication_truth_refresh_resume_exact_target_surface_repair_attempted: false,
            publication_truth_refresh_resume_exact_target_surface_repair_completed: false,
            publication_truth_refresh_resume_exact_target_surface_repair_time_budget_exhausted:
                false,
            publication_truth_refresh_resume_exact_target_surface_repair_wallet_pages: 0,
            publication_truth_refresh_resume_exact_target_surface_repair_wallet_rows: 0,
            publication_truth_refresh_resume_exact_target_surface_repair_target_buy_mints_restored:
                0,
        });
}
;
    }
}
