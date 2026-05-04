impl DiscoveryService {
    fn state_needs_deep_replay_sol_leg_priority_recovery_contract(
        state: &PersistedStreamRebuildState,
    ) -> bool {
        state.phase == DiscoveryPersistedRebuildPhase::Replay
            && state.payload.replay_wallet_stats_complete
            && !state.payload.replay_candidate_activity_backfill_pending
            && state.phase_cursor.is_some()
            && state.replay_rows_processed > 0
    }

    fn state_needs_deep_replay_candidate_activity_backfill_priority_recovery_contract(
        state: &PersistedStreamRebuildState,
    ) -> bool {
        state.phase == DiscoveryPersistedRebuildPhase::Replay
            && state.payload.replay_candidate_activity_backfill_pending
            && (!state.payload.by_wallet.is_empty() || state.phase_cursor.is_some())
    }

    fn replay_sol_leg_processed_floor_pages(
        fetch_limit: usize,
        state: &PersistedStreamRebuildState,
    ) -> usize {
        let rows_floor_pages = state
            .replay_rows_processed
            .max(1)
            .div_ceil(fetch_limit.max(1));
        state.replay_pages_processed.max(rows_floor_pages)
    }

    fn replay_sol_leg_budget_floor_pages(
        fetch_limit: usize,
        state: &PersistedStreamRebuildState,
    ) -> usize {
        Self::replay_sol_leg_processed_floor_pages(fetch_limit, state)
            .max(state.payload.replay_sol_leg_budget_floor_pages)
    }

    fn replay_sol_leg_last_partial_cycle_frontier_saturated(
        fetch_limit: usize,
        state: &PersistedStreamRebuildState,
    ) -> bool {
        let pages_processed = state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed;
        if pages_processed == 0 {
            return false;
        }
        let rows_processed = state
            .payload
            .replay_sol_leg_last_partial_cycle_rows_processed;
        if rows_processed == 0 {
            return false;
        }
        rows_processed.saturating_mul(
            DISCOVERY_PUBLICATION_TRUTH_REPLAY_WALLET_STATS_FRONTIER_SATURATION_DENOMINATOR,
        ) >= pages_processed
            .max(1)
            .saturating_mul(fetch_limit.max(1))
            .saturating_mul(
                DISCOVERY_PUBLICATION_TRUTH_REPLAY_WALLET_STATS_FRONTIER_SATURATION_NUMERATOR,
            )
    }

    fn replay_sol_leg_open_frontier_floor_pages(
        fetch_limit: usize,
        state: &PersistedStreamRebuildState,
    ) -> usize {
        if Self::replay_sol_leg_last_partial_cycle_frontier_saturated(fetch_limit, state) {
            state
                .payload
                .replay_sol_leg_last_partial_cycle_pages_processed
        } else {
            0
        }
    }

    fn replay_sol_leg_remaining_publishable_horizon_ms(
        &self,
        state: &PersistedStreamRebuildState,
        now: DateTime<Utc>,
    ) -> Option<u64> {
        if !Self::state_needs_deep_replay_sol_leg_priority_recovery_contract(state)
            || !self.state_can_resume_stale_metrics_window_until_publish_checkpoint(state, now)
        {
            return None;
        }
        let snapshot_interval =
            Duration::seconds(self.runtime_metric_snapshot_interval_seconds().max(1) as i64);
        let horizon_publishable_horizon_end = state.horizon_end + snapshot_interval;
        let horizon_remaining_ms = horizon_publishable_horizon_end
            .signed_duration_since(now)
            .num_milliseconds()
            .max(0) as u64;

        let metrics_window_bucket_anchor =
            state.metrics_window_start + Duration::days(self.runtime_scoring_window_days());
        let metrics_window_publishable_horizon_end =
            metrics_window_bucket_anchor + snapshot_interval + snapshot_interval;
        let metrics_window_remaining_ms = metrics_window_publishable_horizon_end
            .signed_duration_since(now)
            .num_milliseconds()
            .max(0) as u64;

        let remaining_ms = horizon_remaining_ms.min(metrics_window_remaining_ms);
        (remaining_ms > 0).then_some(remaining_ms)
    }

    fn replay_sol_leg_target_ms_per_page_from_observed_cycle(
        fetch_time_budget_ms: u64,
        fetch_page_limit: usize,
        last_partial_cycle_pages_processed: usize,
        last_partial_cycle_elapsed_ms: u64,
    ) -> u64 {
        let baseline_target_ms_per_page = fetch_time_budget_ms
            .max(1)
            .div_ceil(fetch_page_limit.max(1) as u64);
        let observed_ms_per_page = if last_partial_cycle_pages_processed > 0
            && last_partial_cycle_elapsed_ms > 0
        {
            last_partial_cycle_elapsed_ms.div_ceil(last_partial_cycle_pages_processed.max(1) as u64)
        } else {
            baseline_target_ms_per_page
        };
        observed_ms_per_page.clamp(
            baseline_target_ms_per_page,
            DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_SOL_LEG_MAX_OBSERVED_MS_PER_PAGE
                .max(baseline_target_ms_per_page),
        )
    }

    fn replay_sol_leg_target_ms_per_page(
        &self,
        fetch_page_limit: usize,
        state: &PersistedStreamRebuildState,
    ) -> u64 {
        Self::replay_sol_leg_target_ms_per_page_from_observed_cycle(
            self.config.fetch_time_budget_ms,
            fetch_page_limit,
            state
                .payload
                .replay_sol_leg_last_partial_cycle_pages_processed,
            state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms,
        )
    }

    fn deep_replay_sol_leg_target_time_budget_uncapped(
        baseline_time_budget: StdDuration,
        target_floor_pages: usize,
        target_ms_per_page: u64,
    ) -> StdDuration {
        let floor_pages_with_headroom = target_floor_pages
            .saturating_mul(
                DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_PAGE_HEADROOM_NUMERATOR,
            )
            .div_ceil(
                DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_PAGE_HEADROOM_DENOMINATOR,
            );
        let floor_budget_ms =
            (floor_pages_with_headroom as u128).saturating_mul(target_ms_per_page.max(1) as u128);
        let target_budget_ms = baseline_time_budget.as_millis().max(floor_budget_ms).max(1);
        StdDuration::from_millis(target_budget_ms.min(u64::MAX as u128) as u64)
    }

    fn deep_replay_sol_leg_target_time_budget(
        baseline_time_budget: StdDuration,
        target_floor_pages: usize,
        target_ms_per_page: u64,
    ) -> StdDuration {
        let target_budget_ms = Self::deep_replay_sol_leg_target_time_budget_uncapped(
            baseline_time_budget,
            target_floor_pages,
            target_ms_per_page,
        )
        .as_millis()
        .min(DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_SOL_LEG_MAX_TIME_BUDGET_MS as u128)
        .max(1);
        StdDuration::from_millis(target_budget_ms.min(u64::MAX as u128) as u64)
    }

    fn replay_sol_leg_repair_phase_page_limit(
        &self,
        fetch_page_limit: usize,
        repair_time_budget: StdDuration,
    ) -> usize {
        let normal_fetch_budget_ms = self.config.fetch_time_budget_ms.max(1) as u128;
        let repair_budget_ms = repair_time_budget.as_millis().max(1);
        let budget_multiplier = repair_budget_ms.div_ceil(normal_fetch_budget_ms);
        fetch_page_limit
            .max(1)
            .saturating_mul(budget_multiplier.min(usize::MAX as u128).max(1) as usize)
    }

    fn replay_sol_leg_time_budget_for_phase_page_limit_floor(
        &self,
        fetch_page_limit: usize,
        phase_page_limit_floor: usize,
    ) -> StdDuration {
        let budget_multiplier = phase_page_limit_floor
            .max(1)
            .div_ceil(fetch_page_limit.max(1))
            .max(1) as u128;
        let budget_ms = budget_multiplier
            .saturating_mul(self.config.fetch_time_budget_ms.max(1) as u128)
            .min(DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_SOL_LEG_MAX_TIME_BUDGET_MS as u128)
            .max(1);
        StdDuration::from_millis(budget_ms.min(u64::MAX as u128) as u64)
    }

    #[cfg(test)]
    fn deepen_persisted_stream_priority_recovery_contract_for_state(
        &self,
        state: &PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        contract: PersistedStreamPriorityRecoveryContract,
    ) -> PersistedStreamPriorityRecoveryContract {
        self.deepen_persisted_stream_priority_recovery_contract_for_state_at(
            state,
            fetch_limit,
            fetch_page_limit,
            contract,
            None,
        )
    }
}
