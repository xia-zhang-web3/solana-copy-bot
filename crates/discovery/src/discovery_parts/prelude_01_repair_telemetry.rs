use super::*;

impl<'a> DiscoveryPublicationTruthRepairRegionScope<'a> {
    pub(crate) fn new(
        context: Option<&'a DiscoveryPublicationTruthRepairTraceContext>,
        region: &'static str,
        parent_region: Option<&'static str>,
    ) -> Self {
        let scope = Self {
            context,
            region,
            parent_region,
            started_at: Instant::now(),
            progress: DiscoveryPublicationTruthRepairRegionTraceProgress::default(),
        };
        scope.emit("entered", 0);
        scope
    }

    pub(crate) fn progress_mut(
        &mut self,
    ) -> &mut DiscoveryPublicationTruthRepairRegionTraceProgress {
        &mut self.progress
    }

    pub(crate) fn emit(&self, state: &'static str, elapsed_ms: u64) {
        let Some(context) = self.context else {
            return;
        };
        let deadline_remaining_ms = context
            .helper_deadline
            .saturating_duration_since(Instant::now())
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let event = DiscoveryPublicationTruthRepairRegionTraceEvent {
            trace_id: context.trace_id,
            region: self.region,
            parent_region: self.parent_region,
            state,
            elapsed_ms,
            deadline_remaining_ms,
            publication_state_exists_before: context.publication_state_exists_before,
            publication_truth_complete_before: context.publication_truth_complete_before,
            publication_truth_fresh_before: context.publication_truth_fresh_before,
            runtime_window_complete_before: context.runtime_window_complete_before,
            runtime_cursor_exists_before: context.runtime_cursor_exists_before,
            rebuild_phase: self.progress.rebuild_phase,
            rebuild_replay_subphase: self.progress.rebuild_replay_subphase,
            persisted_rebuild_restore_outcome: self.progress.persisted_rebuild_restore_outcome,
            pages_scanned: self.progress.pages_scanned,
            rows_scanned: self.progress.rows_scanned,
            wallets_scanned: self.progress.wallets_scanned,
            time_budget_exhausted: self.progress.time_budget_exhausted,
            rebuilt_target_mint_count: self.progress.rebuilt_target_mint_count,
            state_repaired_for_resume: self.progress.state_repaired_for_resume,
        };
        if let Some(collector) = context.collector.as_ref() {
            collector.record(event.clone());
        }
        info!(
            publication_truth_repair_trace_id = event.trace_id,
            publication_truth_repair_region = event.region,
            publication_truth_repair_region_parent = event.parent_region,
            publication_truth_repair_region_state = event.state,
            publication_truth_repair_region_elapsed_ms = event.elapsed_ms,
            publication_truth_repair_deadline_remaining_ms = event.deadline_remaining_ms,
            publication_state_exists_before = event.publication_state_exists_before,
            publication_truth_complete_before = event.publication_truth_complete_before,
            publication_truth_fresh_before = event.publication_truth_fresh_before,
            runtime_window_complete_before = event.runtime_window_complete_before,
            runtime_cursor_exists_before = event.runtime_cursor_exists_before,
            publication_truth_repair_region_rebuild_phase = event.rebuild_phase,
            publication_truth_repair_region_rebuild_replay_subphase =
                event.rebuild_replay_subphase,
            publication_truth_repair_region_persisted_rebuild_restore_outcome =
                event.persisted_rebuild_restore_outcome,
            publication_truth_repair_region_pages_scanned = event.pages_scanned,
            publication_truth_repair_region_rows_scanned = event.rows_scanned,
            publication_truth_repair_region_wallets_scanned = event.wallets_scanned,
            publication_truth_repair_region_time_budget_exhausted =
                ?event.time_budget_exhausted,
            publication_truth_repair_region_rebuilt_target_mint_count =
                event.rebuilt_target_mint_count,
            publication_truth_repair_region_state_repaired_for_resume =
                ?event.state_repaired_for_resume,
            "discovery publication truth repair helper region"
        );
    }
}

impl Drop for DiscoveryPublicationTruthRepairRegionScope<'_> {
    fn drop(&mut self) {
        self.emit(
            "exited",
            self.started_at.elapsed().as_millis().min(u64::MAX as u128) as u64,
        );
    }
}

impl DiscoveryPublicationTruthRepairTelemetry {
    pub(crate) fn skipped(
        state: &'static str,
        reason: impl Into<String>,
        required_window_start: DateTime<Utc>,
        publication_truth_complete_before: bool,
        publication_truth_fresh_before: bool,
        runtime_window_complete_before: bool,
        journal_covered_since: Option<DateTime<Utc>>,
        journal_covers_runtime_cursor: bool,
    ) -> Self {
        Self {
            state,
            reason: Some(reason.into()),
            required_window_start,
            journal_covered_since,
            journal_covers_runtime_cursor,
            publication_state_exists_before: false,
            publication_truth_complete_before,
            publication_truth_fresh_before,
            runtime_cursor_exists_before: false,
            journal_store_exists: false,
            runtime_window_complete_before,
            runtime_window_complete_after: runtime_window_complete_before,
            runtime_window_first_cursor: None,
            replay_until_cursor: None,
            replay_batches_completed: 0,
            replay_rows_loaded: 0,
            replay_rows_inserted: 0,
            replay_time_budget_exhausted: false,
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
        }
    }

    pub(crate) fn deferred_to_runtime_cycle(
        required_window_start: DateTime<Utc>,
        publication_truth_complete_before: bool,
        publication_truth_fresh_before: bool,
        runtime_window_complete_before: bool,
        recovery_contract: PersistedStreamPriorityRecoveryContract,
        telemetry: &PersistedStreamProgressTelemetry,
    ) -> Self {
        Self {
            state: "deferred_runtime_window_truth_refresh_to_run_cycle",
            reason: Some(
                "runtime_window_complete_publication_truth_refresh_delegated_to_runtime_cycle"
                    .to_string(),
            ),
            required_window_start,
            journal_covered_since: None,
            journal_covers_runtime_cursor: false,
            publication_state_exists_before: false,
            publication_truth_complete_before,
            publication_truth_fresh_before,
            runtime_cursor_exists_before: false,
            journal_store_exists: false,
            runtime_window_complete_before,
            runtime_window_complete_after: runtime_window_complete_before,
            runtime_window_first_cursor: None,
            replay_until_cursor: None,
            replay_batches_completed: 0,
            replay_rows_loaded: 0,
            replay_rows_inserted: 0,
            replay_time_budget_exhausted: false,
            publication_truth_refresh_attempted: false,
            publication_truth_refresh_completed: false,
            publication_truth_refresh_phase: Some(telemetry.phase.as_str()),
            publication_truth_refresh_replay_subphase: telemetry.replay_subphase,
            publication_truth_refresh_replay_wallet_stats_complete: telemetry
                .replay_wallet_stats_complete,
            publication_truth_refresh_replay_wallet_stats_wallet_cursor: telemetry
                .replay_wallet_stats_wallet_cursor
                .clone(),
            publication_truth_refresh_delegated_to_runtime_cycle: true,
            publication_truth_refresh_priority_recovery_contract_reason: recovery_contract.reason,
            publication_truth_refresh_publishable_checkpoint_blocker: Some(
                DiscoveryService::persisted_stream_publishable_checkpoint_blocker(telemetry),
            ),
            publication_truth_refresh_effective_time_budget_ms: Some(
                recovery_contract
                    .time_budget
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
            ),
            publication_truth_refresh_collect_buy_mints_phase_page_limit: recovery_contract
                .collect_buy_mints_phase_page_limit_override,
            publication_truth_refresh_replay_wallet_stats_phase_page_limit: recovery_contract
                .replay_wallet_stats_phase_page_limit_override,
            publication_truth_refresh_replay_sol_leg_phase_page_limit: recovery_contract
                .replay_sol_leg_phase_page_limit_override,
            publication_truth_refresh_observed_swaps_loaded: telemetry.observed_swaps_loaded,
            publication_truth_refresh_replay_rows_processed: telemetry.replay_rows_processed,
            publication_truth_refresh_replay_pages_processed: telemetry.replay_pages_processed,
            publication_truth_refresh_wallets_buffered: telemetry.wallets_buffered,
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
        }
    }

    pub(crate) fn with_entry_context(
        mut self,
        publication_state_exists_before: bool,
        runtime_cursor_exists_before: bool,
        journal_store_exists: bool,
    ) -> Self {
        self.publication_state_exists_before = publication_state_exists_before;
        self.runtime_cursor_exists_before = runtime_cursor_exists_before;
        self.journal_store_exists = journal_store_exists;
        self
    }

    pub(crate) fn with_helper_write_result(
        mut self,
        attempted: bool,
        succeeded: bool,
        resulting_reason: Option<String>,
        resulting_updated_at: Option<DateTime<Utc>>,
    ) -> Self {
        self.publication_truth_refresh_helper_write_attempted = attempted;
        self.publication_truth_refresh_helper_write_succeeded = succeeded;
        self.publication_truth_refresh_helper_write_resulting_reason = resulting_reason;
        self.publication_truth_refresh_helper_write_resulting_updated_at = resulting_updated_at;
        self
    }

    pub(crate) fn with_replay_window_context(
        mut self,
        runtime_window_first_cursor: Option<DiscoveryRuntimeCursor>,
        replay_until_cursor: Option<DiscoveryRuntimeCursor>,
    ) -> Self {
        self.runtime_window_first_cursor = runtime_window_first_cursor;
        self.replay_until_cursor = replay_until_cursor;
        self
    }

    pub(crate) fn with_resume_exact_target_surface_repair(
        mut self,
        diagnostics: &ResumeExactTargetBuyMintSurfaceRepairDiagnostics,
    ) -> Self {
        self.publication_truth_refresh_resume_exact_target_surface_repair_attempted =
            diagnostics.attempted;
        self.publication_truth_refresh_resume_exact_target_surface_repair_completed =
            diagnostics.completed;
        self.publication_truth_refresh_resume_exact_target_surface_repair_time_budget_exhausted =
            diagnostics.time_budget_exhausted;
        self.publication_truth_refresh_resume_exact_target_surface_repair_wallet_pages =
            diagnostics.wallet_pages;
        self.publication_truth_refresh_resume_exact_target_surface_repair_wallet_rows =
            diagnostics.wallet_rows;
        self.publication_truth_refresh_resume_exact_target_surface_repair_target_buy_mints_restored =
            diagnostics.target_buy_mints_restored;
        self
    }
}
