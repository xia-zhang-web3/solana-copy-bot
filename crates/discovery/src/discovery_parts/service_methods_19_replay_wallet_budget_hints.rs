impl DiscoveryService {
    fn repair_replay_wallet_stats_budget_hints_for_resume(
        &self,
        state: &mut PersistedStreamRebuildState,
    ) -> bool {
        if state.phase != DiscoveryPersistedRebuildPhase::Replay
            || state.payload.replay_wallet_stats_complete
        {
            return false;
        }

        let mut changed = false;
        let inferred_budget_floor_wallets =
            Self::replay_wallet_stats_current_observed_wallet_floor_wallets(state);
        if inferred_budget_floor_wallets > 0
            && state.payload.replay_wallet_stats_budget_floor_wallets
                < inferred_budget_floor_wallets
        {
            let previous_budget_floor_wallets =
                state.payload.replay_wallet_stats_budget_floor_wallets;
            state.payload.replay_wallet_stats_budget_floor_wallets = inferred_budget_floor_wallets;
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_replay_subphase =
                    Self::replay_subphase(
                        state.phase,
                        state.payload.replay_wallet_stats_complete,
                        state.payload.replay_candidate_activity_backfill_pending,
                    ),
                rebuild_replay_wallet_stats_budget_floor_wallets_before =
                    previous_budget_floor_wallets,
                rebuild_replay_wallet_stats_budget_floor_wallets_after =
                    state.payload.replay_wallet_stats_budget_floor_wallets,
                rebuild_replay_wallet_stats_pages_processed =
                    state.payload.replay_wallet_stats_pages_processed,
                rebuild_replay_wallet_stats_rows_processed =
                    state.payload.replay_wallet_stats_rows_processed,
                rebuild_replay_wallet_stats_wallet_cursor =
                    state.payload.replay_wallet_stats_wallet_cursor.as_deref(),
                rebuild_wallets_buffered = state.payload.by_wallet.len(),
                rebuild_replay_wallet_stats_last_partial_cycle_pages_processed =
                    state.payload.replay_wallet_stats_last_partial_cycle_pages_processed,
                rebuild_replay_wallet_stats_last_partial_cycle_wallets_processed =
                    state.payload.replay_wallet_stats_last_partial_cycle_wallets_processed,
                rebuild_replay_wallet_stats_last_partial_cycle_elapsed_ms =
                    state.payload.replay_wallet_stats_last_partial_cycle_elapsed_ms,
                rebuild_replay_wallet_stats_resume_budget_hint_source =
                    "current_observed_frontier",
                "backfilled missing replay wallet-stats budgeting floor from resumed checkpoint progress because the persisted checkpoint did not yet carry an explicit wallet-frontier hint"
            );
            changed = true;
        }

        if state
            .payload
            .replay_wallet_stats_last_partial_cycle_pages_processed
            > 0
            && state
                .payload
                .replay_wallet_stats_last_partial_cycle_wallets_processed
                == 0
            && Self::replay_wallet_stats_last_partial_cycle_frontier_saturated(
                self.config.max_fetch_swaps_per_cycle,
                state,
            )
        {
            state
                .payload
                .replay_wallet_stats_last_partial_cycle_wallets_processed = state
                .payload
                .replay_wallet_stats_last_partial_cycle_pages_processed
                .saturating_mul(Self::replay_wallet_stats_wallet_batch_size(
                    self.config.max_fetch_swaps_per_cycle,
                ));
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_replay_subphase =
                    Self::replay_subphase(
                        state.phase,
                        state.payload.replay_wallet_stats_complete,
                        state.payload.replay_candidate_activity_backfill_pending,
                    ),
                rebuild_replay_wallet_stats_pages_processed =
                    state.payload.replay_wallet_stats_pages_processed,
                rebuild_replay_wallet_stats_rows_processed =
                    state.payload.replay_wallet_stats_rows_processed,
                rebuild_replay_wallet_stats_last_partial_cycle_pages_processed =
                    state.payload.replay_wallet_stats_last_partial_cycle_pages_processed,
                rebuild_replay_wallet_stats_last_partial_cycle_wallets_processed =
                    state.payload.replay_wallet_stats_last_partial_cycle_wallets_processed,
                rebuild_replay_wallet_stats_resume_budget_hint_source =
                    "overall_wallet_frontier_density",
                "backfilled missing replay wallet-stats partial-cycle wallet count from the resumed checkpoint density so deep recovery can recognize an already open wallet frontier immediately"
            );
            changed = true;
        }

        changed
    }
}
