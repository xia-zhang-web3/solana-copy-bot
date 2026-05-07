use super::*;

impl DiscoveryService {
    pub(super) fn log_adjusted_persisted_stream_rebuild_contract(
        &self,
        state: &PersistedStreamRebuildState,
        now: DateTime<Utc>,
        fetch_limit: usize,
        adjusted_contract: PersistedStreamPriorityRecoveryContract,
        inputs: &PersistedStreamRebuildRecoveryInputs,
    ) {
        info!(
            rebuild_priority_recovery_contract_scope = "checkpoint_specific",
            rebuild_phase = state.phase.as_str(),
            rebuild_replay_subphase =
                Self::replay_subphase(
                    state.phase,
                    state.payload.replay_wallet_stats_complete,
                    state.payload.replay_candidate_activity_backfill_pending,
                ),
            rebuild_window_start = %state.window_start,
            rebuild_horizon_end = %state.horizon_end,
            rebuild_metrics_window_start = %state.metrics_window_start,
            rebuild_publishable_checkpoint_blocker =
                Self::persisted_stream_publishable_checkpoint_blocker_from_state(state),
            rebuild_priority_recovery_contract_reason = adjusted_contract.reason,
            rebuild_time_budget_ms = adjusted_contract.time_budget.as_millis(),
            rebuild_collect_buy_mints_phase_page_limit = adjusted_contract
                .collect_buy_mints_phase_page_limit_override,
            rebuild_replay_wallet_stats_phase_page_limit = adjusted_contract
                .replay_wallet_stats_phase_page_limit_override,
            rebuild_replay_sol_leg_phase_page_limit = adjusted_contract
                .replay_sol_leg_phase_page_limit_override,
            rebuild_replay_wallet_stats_buffered_wallet_backlog_floor =
                inputs.buffered_wallet_backlog_floor,
            rebuild_replay_wallet_stats_buffered_wallet_floor_pages =
                inputs.buffered_wallet_floor_pages,
            rebuild_replay_wallet_stats_progress_floor_pages =
                inputs.replay_wallet_stats_progress_floor_pages,
            rebuild_replay_wallet_stats_budget_floor_wallets =
                state.payload.replay_wallet_stats_budget_floor_wallets,
            rebuild_replay_wallet_stats_current_observed_wallet_floor =
                Self::replay_wallet_stats_current_observed_wallet_floor_wallets(state),
            rebuild_replay_wallet_stats_budget_floor_carried_forward =
                state.payload.replay_wallet_stats_budget_floor_wallets
                    > Self::replay_wallet_stats_current_observed_wallet_floor_wallets(state),
            rebuild_replay_wallet_stats_last_partial_cycle_pages_processed = state
                .payload
                .replay_wallet_stats_last_partial_cycle_pages_processed,
            rebuild_replay_wallet_stats_last_partial_cycle_wallets_processed = state
                .payload
                .replay_wallet_stats_last_partial_cycle_wallets_processed,
            rebuild_replay_wallet_stats_last_partial_cycle_elapsed_ms = state
                .payload
                .replay_wallet_stats_last_partial_cycle_elapsed_ms,
            rebuild_replay_wallet_stats_publishable_horizon_remaining_ms = self
                .replay_wallet_stats_remaining_publishable_horizon_ms(state, now),
            rebuild_replay_wallet_stats_milestone_reached =
                state.payload.replay_wallet_stats_milestone_reached,
            rebuild_replay_sol_leg_reentry_pending =
                state.payload.replay_sol_leg_reentry_pending,
            rebuild_replay_candidate_activity_backfill_required =
                state.payload.replay_candidate_activity_backfill_required,
            rebuild_replay_sol_leg_last_partial_cycle_pages_processed = state
                .payload
                .replay_sol_leg_last_partial_cycle_pages_processed,
            rebuild_replay_sol_leg_last_partial_cycle_rows_processed = state
                .payload
                .replay_sol_leg_last_partial_cycle_rows_processed,
            rebuild_replay_sol_leg_last_partial_cycle_elapsed_ms = state
                .payload
                .replay_sol_leg_last_partial_cycle_elapsed_ms,
            rebuild_replay_sol_leg_retained_contract_floor_pages = state
                .payload
                .replay_sol_leg_retained_contract_floor_pages,
            rebuild_replay_sol_leg_publishable_horizon_remaining_ms = self
                .replay_sol_leg_remaining_publishable_horizon_ms(state, now),
            rebuild_replay_sol_leg_target_ms_per_page =
                inputs.replay_sol_leg_target_ms_per_page,
            rebuild_replay_sol_leg_target_time_budget_before_retained_contract_floor_ms =
                inputs.replay_sol_leg_target_time_budget_before_retained_contract_floor.as_millis(),
            rebuild_replay_sol_leg_phase_page_limit_before_retained_contract_floor =
                inputs.replay_sol_leg_phase_page_limit_before_retained_contract_floor,
            rebuild_replay_sol_leg_retained_contract_floor_applied =
                adjusted_contract.reason
                    == Some("deep_replay_sol_leg_retained_contract_floor"),
            rebuild_replay_sol_leg_publishable_horizon_budget_extension_applied =
                adjusted_contract.reason
                    == Some("deep_replay_sol_leg_publishable_horizon_backlog"),
            rebuild_replay_sol_leg_publishable_horizon_budget_cap_applied =
                adjusted_contract.reason
                    == Some("deep_replay_sol_leg_publishable_horizon_cap"),
            rebuild_replay_wallet_stats_target_time_budget_before_publishable_horizon_cap_ms =
                inputs.replay_wallet_stats_target_time_budget_before_publishable_horizon_cap.as_millis(),
            rebuild_replay_wallet_stats_publishable_horizon_budget_cap_applied =
                adjusted_contract.reason
                    == Some("deep_replay_wallet_stats_publishable_horizon_cap"),
            rebuild_replay_wallet_stats_target_ms_per_page =
                Self::replay_wallet_stats_target_ms_per_page(state),
            rebuild_replay_wallet_stats_wallet_batch_size =
                Self::replay_wallet_stats_wallet_batch_size(fetch_limit),
            rebuild_replay_wallet_stats_open_frontier_floor_pages =
                inputs.replay_wallet_stats_open_frontier_floor_pages,
            rebuild_replay_wallet_stats_remaining_frontier_min_pages =
                inputs.replay_wallet_stats_remaining_frontier_min_pages,
            rebuild_replay_wallet_stats_remaining_frontier_min_wallets =
                inputs.replay_wallet_stats_remaining_frontier_min_wallets,
            rebuild_replay_wallet_stats_persistently_open_frontier =
                inputs.replay_wallet_stats_persistently_open_frontier,
            rebuild_replay_wallet_stats_frontier_saturated =
                inputs.replay_wallet_stats_frontier_saturated,
            rebuild_replay_wallet_stats_frontier_saturated_inferred =
                inputs.replay_wallet_stats_frontier_saturated
                    && state
                        .payload
                        .replay_wallet_stats_last_partial_cycle_wallets_processed
                        == 0,
            rebuild_replay_wallet_stats_completion_requirement = if !state
                .payload
                .replay_wallet_stats_complete
            {
                Some("wallet_id_source_exhaustion")
            } else {
                None
            },
            rebuild_replay_activity_backfill_completion_requirement = if state
                .payload
                .replay_candidate_activity_backfill_pending
            {
                Some("candidate_wallet_swap_source_exhaustion")
            } else {
                None
            },
            rebuild_replay_sol_leg_completion_requirement = if state.phase
                == DiscoveryPersistedRebuildPhase::Replay
                && state.payload.replay_wallet_stats_complete
                && !state.payload.replay_candidate_activity_backfill_pending
            {
                Some("sol_leg_swap_source_exhaustion")
            } else {
                None
            },
            rebuild_replay_sol_leg_processed_floor_pages =
                inputs.replay_sol_leg_processed_floor_pages,
            rebuild_replay_sol_leg_open_frontier_floor_pages =
                inputs.replay_sol_leg_open_frontier_floor_pages,
            rebuild_replay_sol_leg_remaining_frontier_min_pages =
                inputs.replay_sol_leg_remaining_frontier_min_pages,
            rebuild_replay_sol_leg_remaining_frontier_min_rows =
                inputs.replay_sol_leg_remaining_frontier_min_rows,
            rebuild_replay_sol_leg_frontier_saturated =
                inputs.replay_sol_leg_frontier_saturated,
            rebuild_replay_rows_processed = state.replay_rows_processed,
            rebuild_replay_pages_processed = state.replay_pages_processed,
            rebuild_replay_wallet_stats_rows_processed =
                state.payload.replay_wallet_stats_rows_processed,
            rebuild_replay_wallet_stats_pages_processed =
                state.payload.replay_wallet_stats_pages_processed,
            rebuild_wallets_buffered = state.payload.by_wallet.len(),
            "widening bounded persisted observed_swaps rebuild contract for the current fail-closed recovery checkpoint"
        );
    }
}
