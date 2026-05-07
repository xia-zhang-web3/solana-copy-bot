use super::*;

impl DiscoveryService {
    pub(crate) fn persisted_stream_priority_recovery_contract(
        &self,
        runtime_store: &SqliteStore,
        now: DateTime<Utc>,
        fetch_limit: usize,
        fetch_page_limit: usize,
        fetch_time_budget: StdDuration,
    ) -> Result<PersistedStreamPriorityRecoveryContract> {
        let recommended_time_budget =
            self.recommended_publication_truth_repair_time_budget(runtime_store, now)?;
        let time_budget = fetch_time_budget.max(recommended_time_budget);
        if time_budget <= fetch_time_budget {
            return Ok(PersistedStreamPriorityRecoveryContract {
                time_budget,
                collect_buy_mints_phase_page_limit_override: None,
                replay_wallet_stats_phase_page_limit_override: None,
                replay_sol_leg_phase_page_limit_override: None,
                reason: None,
            });
        }

        Ok(PersistedStreamPriorityRecoveryContract {
            time_budget,
            collect_buy_mints_phase_page_limit_override: Some(
                self.collect_buy_mints_repair_phase_page_limit(
                    fetch_limit,
                    fetch_page_limit,
                    time_budget,
                ),
            ),
            replay_wallet_stats_phase_page_limit_override: Some(
                self.replay_wallet_stats_repair_phase_page_limit(
                    fetch_limit,
                    fetch_page_limit,
                    time_budget,
                ),
            ),
            replay_sol_leg_phase_page_limit_override: None,
            reason: Some("runtime_window_complete_stale_publication_truth"),
        })
    }

    pub(crate) fn state_needs_deep_replay_wallet_stats_priority_recovery_contract(
        state: &PersistedStreamRebuildState,
    ) -> bool {
        state.phase == DiscoveryPersistedRebuildPhase::Replay
            && !state.payload.replay_wallet_stats_complete
            && state.payload.replay_wallet_stats_wallet_cursor.is_some()
            && state.payload.replay_wallet_stats_rows_processed > 0
            && Self::replay_wallet_stats_buffered_wallet_backlog_floor_wallets(state) > 0
    }

    pub(crate) fn replay_wallet_stats_current_observed_wallet_floor_wallets(
        state: &PersistedStreamRebuildState,
    ) -> usize {
        state.payload.by_wallet.len().max(
            state
                .payload
                .replay_wallet_stats_day_count_source_progress
                .fast_path_wallets_processed
                .saturating_add(
                    state
                        .payload
                        .replay_wallet_stats_day_count_source_progress
                        .fallback_wallets_processed,
                ),
        )
    }

    pub(crate) fn replay_wallet_stats_buffered_wallet_backlog_floor_wallets(
        state: &PersistedStreamRebuildState,
    ) -> usize {
        Self::replay_wallet_stats_current_observed_wallet_floor_wallets(state)
            .max(state.payload.replay_wallet_stats_budget_floor_wallets)
    }

    pub(crate) fn replay_wallet_stats_total_wallets_processed(state: &PersistedStreamRebuildState) -> usize {
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .total_wallets_processed()
    }

    pub(crate) fn replay_wallet_stats_buffered_wallet_floor_pages(
        fetch_limit: usize,
        buffered_wallets: usize,
    ) -> usize {
        let wallet_batch_size = Self::replay_wallet_stats_wallet_batch_size(fetch_limit).max(1);
        buffered_wallets.max(1).div_ceil(wallet_batch_size)
    }

    pub(crate) fn replay_wallet_stats_progress_floor_pages(
        fetch_limit: usize,
        state: &PersistedStreamRebuildState,
    ) -> usize {
        let buffered_wallet_floor_pages = Self::replay_wallet_stats_buffered_wallet_floor_pages(
            fetch_limit,
            Self::replay_wallet_stats_buffered_wallet_backlog_floor_wallets(state),
        );
        state
            .payload
            .replay_wallet_stats_pages_processed
            .max(buffered_wallet_floor_pages)
            .max(1)
    }

    pub(crate) fn replay_wallet_stats_last_partial_cycle_frontier_saturated(
        fetch_limit: usize,
        state: &PersistedStreamRebuildState,
    ) -> bool {
        let last_partial_cycle_pages_processed = state
            .payload
            .replay_wallet_stats_last_partial_cycle_pages_processed;
        if last_partial_cycle_pages_processed == 0 {
            return false;
        }
        let wallet_batch_size = Self::replay_wallet_stats_wallet_batch_size(fetch_limit).max(1);
        let required_wallets_for_saturation =
            last_partial_cycle_pages_processed.saturating_mul(wallet_batch_size);
        if state
            .payload
            .replay_wallet_stats_last_partial_cycle_wallets_processed
            > 0
        {
            return state
                .payload
                .replay_wallet_stats_last_partial_cycle_wallets_processed
                >= required_wallets_for_saturation;
        }

        let total_pages_processed = state.payload.replay_wallet_stats_pages_processed.max(1);
        let total_wallets_processed = Self::replay_wallet_stats_total_wallets_processed(state);
        total_wallets_processed.saturating_mul(
            DISCOVERY_PUBLICATION_TRUTH_REPLAY_WALLET_STATS_FRONTIER_SATURATION_DENOMINATOR,
        ) >= total_pages_processed
            .saturating_mul(wallet_batch_size)
            .saturating_mul(
                DISCOVERY_PUBLICATION_TRUTH_REPLAY_WALLET_STATS_FRONTIER_SATURATION_NUMERATOR,
            )
    }

    pub(crate) fn replay_wallet_stats_open_frontier_floor_pages(
        fetch_limit: usize,
        state: &PersistedStreamRebuildState,
    ) -> usize {
        if Self::replay_wallet_stats_last_partial_cycle_frontier_saturated(fetch_limit, state) {
            state
                .payload
                .replay_wallet_stats_last_partial_cycle_pages_processed
        } else {
            0
        }
    }

    pub(crate) fn replay_wallet_stats_target_ms_per_page(state: &PersistedStreamRebuildState) -> u64 {
        let observed_ms_per_page = if state
            .payload
            .replay_wallet_stats_last_partial_cycle_pages_processed
            > 0
            && state
                .payload
                .replay_wallet_stats_last_partial_cycle_elapsed_ms
                > 0
        {
            state
                .payload
                .replay_wallet_stats_last_partial_cycle_elapsed_ms
                .div_ceil(
                    state
                        .payload
                        .replay_wallet_stats_last_partial_cycle_pages_processed
                        as u64,
                )
        } else {
            DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_TARGET_MS_PER_PAGE
        };
        observed_ms_per_page.clamp(
            DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_TARGET_MS_PER_PAGE,
            DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_MAX_OBSERVED_MS_PER_PAGE,
        )
    }

    pub(crate) fn deep_replay_wallet_stats_target_time_budget(
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
        let buffered_floor_budget_ms =
            (floor_pages_with_headroom as u128).saturating_mul(target_ms_per_page.max(1) as u128);
        let target_budget_ms = baseline_time_budget
            .as_millis()
            .max(buffered_floor_budget_ms)
            .min(
                DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_MAX_TIME_BUDGET_MS
                    as u128,
            )
            .max(1);
        StdDuration::from_millis(target_budget_ms.min(u64::MAX as u128) as u64)
    }

    pub(crate) fn replay_wallet_stats_remaining_publishable_horizon_ms(
        &self,
        state: &PersistedStreamRebuildState,
        now: DateTime<Utc>,
    ) -> Option<u64> {
        if !Self::state_needs_deep_replay_wallet_stats_priority_recovery_contract(state)
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
        if remaining_ms == 0 {
            return None;
        }
        Some(
            remaining_ms.min(
                DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_MAX_TIME_BUDGET_MS,
            ),
        )
    }

}
