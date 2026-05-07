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

        include!("service_methods_03_publication_truth_replay_journal.rs");
    }
}
