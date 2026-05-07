use super::*;

impl DiscoveryService {
    pub(crate) fn repair_replay_sol_leg_budget_hints_for_resume(
        &self,
        state: &mut PersistedStreamRebuildState,
    ) -> bool {
        let in_sol_leg = state.phase == DiscoveryPersistedRebuildPhase::Replay
            && state.payload.replay_wallet_stats_complete
            && !state.payload.replay_candidate_activity_backfill_pending
            && state.phase_cursor.is_some();
        if in_sol_leg {
            let inferred_budget_floor_pages = Self::replay_sol_leg_processed_floor_pages(
                self.config.max_fetch_swaps_per_cycle,
                state,
            );
            if inferred_budget_floor_pages > 0
                && state.payload.replay_sol_leg_budget_floor_pages < inferred_budget_floor_pages
            {
                let previous_budget_floor_pages = state.payload.replay_sol_leg_budget_floor_pages;
                state.payload.replay_sol_leg_budget_floor_pages = inferred_budget_floor_pages;
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
                    rebuild_replay_sol_leg_budget_floor_pages_before =
                        previous_budget_floor_pages,
                    rebuild_replay_sol_leg_budget_floor_pages_after =
                        state.payload.replay_sol_leg_budget_floor_pages,
                    rebuild_replay_rows_processed = state.replay_rows_processed,
                    rebuild_replay_pages_processed = state.replay_pages_processed,
                    rebuild_replay_sol_leg_last_partial_cycle_pages_processed =
                        state.payload.replay_sol_leg_last_partial_cycle_pages_processed,
                    rebuild_replay_sol_leg_last_partial_cycle_rows_processed =
                        state.payload.replay_sol_leg_last_partial_cycle_rows_processed,
                    rebuild_replay_sol_leg_last_partial_cycle_elapsed_ms =
                        state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms,
                    rebuild_replay_sol_leg_resume_budget_hint_source =
                        "persisted_replay_progress_prefix",
                    "backfilled missing replay SOL-leg budgeting floor from resumed checkpoint progress so carried target-window replay does not relearn from a much smaller suffix-only frontier"
                );
                return true;
            }
            return false;
        }
        if state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed
            == 0
            && state
                .payload
                .replay_sol_leg_last_partial_cycle_rows_processed
                == 0
            && state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms == 0
            && state.payload.replay_sol_leg_budget_floor_pages == 0
            && state.payload.replay_sol_leg_retained_contract_floor_pages == 0
        {
            return false;
        }
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
            rebuild_replay_sol_leg_last_partial_cycle_pages_processed =
                state.payload.replay_sol_leg_last_partial_cycle_pages_processed,
            rebuild_replay_sol_leg_last_partial_cycle_rows_processed =
                state.payload.replay_sol_leg_last_partial_cycle_rows_processed,
            rebuild_replay_sol_leg_last_partial_cycle_elapsed_ms =
                state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms,
            rebuild_replay_sol_leg_budget_floor_pages =
                state.payload.replay_sol_leg_budget_floor_pages,
            rebuild_replay_sol_leg_retained_contract_floor_pages =
                state.payload.replay_sol_leg_retained_contract_floor_pages,
            "clearing stale replay SOL-leg frontier hints from a resumed checkpoint because the active state is no longer the partial SOL-leg blocker"
        );
        state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed = 0;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_rows_processed = 0;
        state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms = 0;
        state.payload.replay_sol_leg_budget_floor_pages = 0;
        state.payload.replay_sol_leg_retained_contract_floor_pages = 0;
        true
    }
}
