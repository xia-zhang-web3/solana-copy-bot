impl DiscoveryService {
    fn advance_persisted_stream_replay_with_phase_page_limits(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        replay_wallet_stats_phase_page_limit_override: Option<usize>,
        replay_sol_leg_phase_page_limit_override: Option<usize>,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        match state.payload.replay_mode {
            ReplayMode::LegacyCompleteReplay => self.advance_persisted_stream_replay_legacy(
                store,
                state,
                fetch_limit,
                fetch_page_limit,
                deadline,
            ),
            ReplayMode::WalletStatsThenSolLeg => self
                .advance_persisted_stream_replay_optimized_with_wallet_stats_phase_page_limit(
                    store,
                    state,
                    fetch_limit,
                    fetch_page_limit,
                    replay_wallet_stats_phase_page_limit_override,
                    replay_sol_leg_phase_page_limit_override,
                    replay_wallet_stats_phase_page_limit_override.is_some()
                        && self.config.min_buy_count > 0,
                    deadline,
                ),
        }
    }

    fn persist_partial_replay_frontier_hints_after_cycle(
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        cycle_started_in_replay_sol_leg: bool,
        cycle_replay_wallet_stats_day_count_source_progress: ReplayWalletStatsDayCountSourceProgress,
        cycle_elapsed_ms: u64,
        cycle_budget_exhausted_reason: Option<PersistedStreamBudgetExhaustedReason>,
        cycle_replay_sol_leg_phase_page_limit: Option<usize>,
        cycle_replay_sol_leg_pages_processed: usize,
        cycle_replay_sol_leg_rows_processed: usize,
        cycle_replay_sol_leg_elapsed_ms: u64,
    ) {
        let cycle_replay_wallet_stats_pages_processed =
            cycle_replay_wallet_stats_day_count_source_progress.total_pages_processed();
        let cycle_replay_wallet_stats_wallets_processed =
            cycle_replay_wallet_stats_day_count_source_progress.total_wallets_processed();
        if state.phase == DiscoveryPersistedRebuildPhase::Replay
            && !state.payload.replay_wallet_stats_complete
            && cycle_replay_wallet_stats_pages_processed > 0
        {
            state.payload.replay_wallet_stats_budget_floor_wallets = state
                .payload
                .replay_wallet_stats_budget_floor_wallets
                .max(Self::replay_wallet_stats_current_observed_wallet_floor_wallets(state));
            state
                .payload
                .replay_wallet_stats_last_partial_cycle_pages_processed =
                cycle_replay_wallet_stats_pages_processed;
            state
                .payload
                .replay_wallet_stats_last_partial_cycle_wallets_processed =
                cycle_replay_wallet_stats_wallets_processed;
            state
                .payload
                .replay_wallet_stats_last_partial_cycle_elapsed_ms = cycle_elapsed_ms;
        } else if state.phase == DiscoveryPersistedRebuildPhase::Replay
            && state.payload.replay_wallet_stats_complete
            && !state.payload.replay_candidate_activity_backfill_pending
            && cycle_started_in_replay_sol_leg
            && cycle_replay_sol_leg_pages_processed > 0
        {
            let total_replay_rows_floor_pages = state
                .replay_rows_processed
                .max(1)
                .div_ceil(fetch_limit.max(1));
            let total_replay_pages_processed = state
                .replay_pages_processed
                .max(total_replay_rows_floor_pages);
            state
                .payload
                .replay_sol_leg_last_partial_cycle_pages_processed =
                cycle_replay_sol_leg_pages_processed;
            state
                .payload
                .replay_sol_leg_last_partial_cycle_rows_processed =
                cycle_replay_sol_leg_rows_processed;
            state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms =
                cycle_replay_sol_leg_elapsed_ms;
            state.payload.replay_sol_leg_budget_floor_pages = state
                .payload
                .replay_sol_leg_budget_floor_pages
                .max(total_replay_pages_processed);
            if let Some(phase_page_limit) = cycle_replay_sol_leg_phase_page_limit.filter(|limit| {
                cycle_budget_exhausted_reason
                    == Some(PersistedStreamBudgetExhaustedReason::PageBudget)
                    && cycle_replay_sol_leg_pages_processed >= (*limit).max(1)
            }) {
                state.payload.replay_sol_leg_retained_contract_floor_pages = state
                    .payload
                    .replay_sol_leg_retained_contract_floor_pages
                    .max(phase_page_limit.max(1));
            }
        }
    }
}
