use super::*;

impl DiscoveryService {
    pub(crate) fn log_persisted_stream_progress(
        &self,
        telemetry: &PersistedStreamProgressTelemetry,
        message: &'static str,
    ) {
        let derived = self.persisted_stream_progress_log_derived(telemetry);
        info!(
            rebuild_phase = telemetry.phase.as_str(),
            rebuild_collect_buy_mints_mode = telemetry.collect_buy_mints_mode.as_str(),
            rebuild_replay_mode = telemetry.replay_mode.as_str(),
            rebuild_replay_subphase = telemetry.replay_subphase,
            rebuild_replay_sol_leg_access_path = telemetry
                .replay_sol_leg_access_path
                .map(ObservedSolLegCursorAccessPath::as_str),
            rebuild_window_start = %telemetry.window_start,
            rebuild_horizon_end = %telemetry.horizon_end,
            rebuild_metrics_window_start = %telemetry.metrics_window_start,
            rebuild_partial = telemetry.partial,
            rebuild_completed = telemetry.completed,
            rebuild_publishable_checkpoint_blocker =
                Self::persisted_stream_publishable_checkpoint_blocker(telemetry),
            rebuild_budget_exhausted_reason = telemetry
                .budget_exhausted_reason
                .map(PersistedStreamBudgetExhaustedReason::as_str),
            rebuild_phase_cursor_ts = ?telemetry
                .phase_cursor
                .as_ref()
                .map(|cursor| cursor.ts_utc),
            rebuild_phase_cursor_slot = telemetry
                .phase_cursor
                .as_ref()
                .map(|cursor| cursor.slot),
            rebuild_phase_cursor_signature = telemetry
                .phase_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            rebuild_replay_wallet_stats_wallet_cursor =
                telemetry.replay_wallet_stats_wallet_cursor.as_deref(),
            rebuild_collect_buy_mints_cursor_token =
                telemetry.collect_buy_mints_cursor_token.as_deref(),
            rebuild_collect_buy_mints_reconcile_source_window_start =
                ?telemetry.collect_buy_mints_reconcile_source_window_start,
            rebuild_collect_buy_mints_reconcile_source_horizon_end =
                ?telemetry.collect_buy_mints_reconcile_source_horizon_end,
            rebuild_collect_buy_mints_reconcile_expired_head_cursor_ts = ?telemetry
                .collect_buy_mints_reconcile_expired_head_cursor
                .as_ref()
                .map(|cursor| cursor.ts_utc),
            rebuild_collect_buy_mints_reconcile_expired_head_cursor_slot = ?telemetry
                .collect_buy_mints_reconcile_expired_head_cursor
                .as_ref()
                .map(|cursor| cursor.slot),
            rebuild_collect_buy_mints_reconcile_expired_head_cursor_signature = ?telemetry
                .collect_buy_mints_reconcile_expired_head_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            rebuild_collect_buy_mints_reconcile_expired_head_cursor_token = telemetry
                .collect_buy_mints_reconcile_expired_head_cursor_token
                .as_deref(),
            rebuild_collect_buy_mints_reconcile_expired_head_pending_mints = telemetry
                .collect_buy_mints_reconcile_expired_head_pending_mints,
            rebuild_collect_buy_mints_reconcile_new_tail_cursor_ts = ?telemetry
                .collect_buy_mints_reconcile_new_tail_cursor
                .as_ref()
                .map(|cursor| cursor.ts_utc),
            rebuild_collect_buy_mints_reconcile_new_tail_cursor_slot = ?telemetry
                .collect_buy_mints_reconcile_new_tail_cursor
                .as_ref()
                .map(|cursor| cursor.slot),
            rebuild_collect_buy_mints_reconcile_new_tail_cursor_signature = ?telemetry
                .collect_buy_mints_reconcile_new_tail_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            rebuild_collect_buy_mints_reconcile_new_tail_cursor_token = telemetry
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .as_deref(),
            rebuild_collect_buy_mints_reconcile_new_tail_slice_end_token = telemetry
                .collect_buy_mints_reconcile_new_tail_slice_end_token
                .as_deref(),
            rebuild_collect_buy_mints_reconcile_new_tail_pending_mints = telemetry
                .collect_buy_mints_reconcile_new_tail_pending_mints,
            rebuild_cycle_rows_processed = telemetry.cycle_rows_processed,
            rebuild_cycle_pages_processed = telemetry.cycle_pages_processed,
            rebuild_cycle_replay_wallet_stats_fast_path_pages_processed = telemetry
                .cycle_replay_wallet_stats_day_count_source_progress
                .fast_path_pages_processed,
            rebuild_cycle_replay_wallet_stats_fallback_pages_processed = telemetry
                .cycle_replay_wallet_stats_day_count_source_progress
                .fallback_pages_processed,
            rebuild_cycle_replay_wallet_stats_fast_path_wallets_processed = telemetry
                .cycle_replay_wallet_stats_day_count_source_progress
                .fast_path_wallets_processed,
            rebuild_cycle_replay_wallet_stats_fallback_wallets_processed = telemetry
                .cycle_replay_wallet_stats_day_count_source_progress
                .fallback_wallets_processed,
            rebuild_cycle_unique_buy_mints_discovered =
                telemetry.cycle_unique_buy_mints_discovered,
            rebuild_cycle_rows_per_second = if telemetry.cycle_elapsed_ms == 0 {
                telemetry.cycle_rows_processed as u64
            } else {
                (telemetry.cycle_rows_processed as u64).saturating_mul(1_000)
                    / telemetry.cycle_elapsed_ms.max(1)
            },
            rebuild_prepass_rows_processed = telemetry.prepass_rows_processed,
            rebuild_prepass_pages_processed = telemetry.prepass_pages_processed,
            rebuild_replay_wallet_stats_complete = telemetry.replay_wallet_stats_complete,
            rebuild_replay_wallet_stats_rows_processed =
                telemetry.replay_wallet_stats_rows_processed,
            rebuild_replay_wallet_stats_pages_processed =
                telemetry.replay_wallet_stats_pages_processed,
            rebuild_replay_wallet_stats_fast_path_pages_processed = telemetry
                .replay_wallet_stats_day_count_source_progress
                .fast_path_pages_processed,
            rebuild_replay_wallet_stats_fallback_pages_processed = telemetry
                .replay_wallet_stats_day_count_source_progress
                .fallback_pages_processed,
            rebuild_replay_wallet_stats_fast_path_wallets_processed = telemetry
                .replay_wallet_stats_day_count_source_progress
                .fast_path_wallets_processed,
            rebuild_replay_wallet_stats_fallback_wallets_processed = telemetry
                .replay_wallet_stats_day_count_source_progress
                .fallback_wallets_processed,
            rebuild_replay_wallet_stats_budget_floor_wallets =
                telemetry.replay_wallet_stats_budget_floor_wallets,
            rebuild_replay_wallet_stats_current_observed_wallet_floor =
                derived.replay_wallet_stats_current_observed_wallet_floor,
            rebuild_replay_wallet_stats_budget_floor_carried_forward =
                telemetry.replay_wallet_stats_budget_floor_wallets
                    > derived.replay_wallet_stats_current_observed_wallet_floor,
            rebuild_replay_wallet_stats_last_partial_cycle_pages_processed =
                telemetry.replay_wallet_stats_last_partial_cycle_pages_processed,
            rebuild_replay_wallet_stats_last_partial_cycle_wallets_processed =
                telemetry.replay_wallet_stats_last_partial_cycle_wallets_processed,
            rebuild_replay_wallet_stats_last_partial_cycle_elapsed_ms =
                telemetry.replay_wallet_stats_last_partial_cycle_elapsed_ms,
            rebuild_replay_wallet_stats_publishable_horizon_remaining_ms =
                telemetry.replay_wallet_stats_publishable_horizon_remaining_ms,
            rebuild_replay_wallet_stats_milestone_reached =
                telemetry.replay_wallet_stats_milestone_reached,
            rebuild_replay_sol_leg_reentry_pending =
                telemetry.replay_sol_leg_reentry_pending,
            rebuild_replay_candidate_activity_backfill_required =
                telemetry.replay_candidate_activity_backfill_required,
            rebuild_replay_candidate_activity_backfill_wallet_cursor =
                telemetry.replay_candidate_activity_backfill_wallet_cursor.as_deref(),
            rebuild_replay_sol_leg_last_partial_cycle_pages_processed =
                telemetry.replay_sol_leg_last_partial_cycle_pages_processed,
            rebuild_replay_sol_leg_last_partial_cycle_rows_processed =
                telemetry.replay_sol_leg_last_partial_cycle_rows_processed,
            rebuild_replay_sol_leg_last_partial_cycle_elapsed_ms =
                telemetry.replay_sol_leg_last_partial_cycle_elapsed_ms,
            rebuild_replay_sol_leg_budget_floor_pages =
                telemetry.replay_sol_leg_budget_floor_pages,
            rebuild_replay_sol_leg_target_ms_per_page =
                derived.replay_sol_leg_target_ms_per_page,
            rebuild_replay_sol_leg_publishable_horizon_remaining_ms =
                telemetry.replay_sol_leg_publishable_horizon_remaining_ms,
            rebuild_replay_sol_leg_retained_contract_floor_pages =
                telemetry.replay_sol_leg_retained_contract_floor_pages,
            rebuild_replay_sol_leg_open_frontier_floor_pages =
                derived.replay_sol_leg_open_frontier_floor_pages,
            rebuild_replay_sol_leg_remaining_frontier_min_pages =
                derived.replay_sol_leg_remaining_frontier_min_pages,
            rebuild_replay_sol_leg_remaining_frontier_min_rows =
                derived.replay_sol_leg_remaining_frontier_min_rows,
            rebuild_replay_sol_leg_frontier_saturated =
                derived.replay_sol_leg_last_partial_cycle_frontier_saturated,
            rebuild_replay_wallet_stats_target_ms_per_page =
                derived.replay_wallet_stats_target_ms_per_page,
            rebuild_replay_wallet_stats_wallet_batch_size =
                derived.replay_wallet_stats_wallet_batch_size,
            rebuild_replay_wallet_stats_progress_floor_pages =
                derived.replay_wallet_stats_progress_floor_pages,
            rebuild_replay_wallet_stats_open_frontier_floor_pages =
                derived.replay_wallet_stats_open_frontier_floor_pages,
            rebuild_replay_wallet_stats_remaining_frontier_min_pages =
                derived.replay_wallet_stats_remaining_frontier_min_pages,
            rebuild_replay_wallet_stats_remaining_frontier_min_wallets =
                derived.replay_wallet_stats_remaining_frontier_min_wallets,
            rebuild_replay_wallet_stats_persistently_open_frontier =
                derived.replay_wallet_stats_persistently_open_frontier,
            rebuild_replay_wallet_stats_frontier_saturated =
                derived.replay_wallet_stats_last_partial_cycle_frontier_saturated,
            rebuild_replay_wallet_stats_frontier_saturated_inferred =
                derived.replay_wallet_stats_last_partial_cycle_frontier_saturated
                    && telemetry.replay_wallet_stats_last_partial_cycle_wallets_processed == 0,
            rebuild_replay_wallet_stats_cursor_live =
                telemetry.replay_wallet_stats_wallet_cursor.is_some(),
            rebuild_replay_wallet_stats_cycle_wallet_cursor_before = telemetry
                .cycle_replay_wallet_stats_wallet_cursor_before
                .as_deref(),
            rebuild_replay_wallet_stats_cycle_wallet_cursor_after = telemetry
                .cycle_replay_wallet_stats_wallet_cursor_after
                .as_deref(),
            rebuild_replay_wallet_stats_cycle_wallet_cursor_advanced =
                telemetry.cycle_replay_wallet_stats_wallet_cursor_before
                    != telemetry.cycle_replay_wallet_stats_wallet_cursor_after,
            rebuild_replay_wallet_stats_completion_requirement = if telemetry
                .replay_subphase
                == Some("wallet_stats")
                && !telemetry.replay_wallet_stats_complete
            {
                Some("wallet_id_source_exhaustion")
            } else {
                None
            },
            rebuild_replay_sol_leg_completion_requirement = if telemetry.replay_subphase
                == Some("sol_leg")
            {
                Some("sol_leg_swap_source_exhaustion")
            } else {
                None
            },
            rebuild_replay_activity_backfill_completion_requirement = if telemetry
                .replay_subphase
                == Some("activity_backfill")
            {
                Some("candidate_wallet_swap_source_exhaustion")
            } else {
                None
            },
            rebuild_replay_rows_processed = telemetry.replay_rows_processed,
            rebuild_replay_pages_processed = telemetry.replay_pages_processed,
            rebuild_observed_swaps_loaded = telemetry.observed_swaps_loaded,
            rebuild_unique_buy_mints = telemetry.unique_buy_mints,
            rebuild_quality_next_mint_index = telemetry.quality_next_mint_index,
            rebuild_quality_rpc_attempted = telemetry.quality_rpc_attempted,
            rebuild_quality_rpc_spent_ms = telemetry.quality_rpc_spent_ms,
            rebuild_wallets_buffered = telemetry.wallets_buffered,
            rebuild_publish_pending_requested_wallet_count =
                telemetry.publish_pending_requested_wallet_count,
            rebuild_chunks_completed = telemetry.chunks_completed,
            rebuild_started_at = %telemetry.started_at,
            rebuild_cycle_elapsed_ms = telemetry.cycle_elapsed_ms,
            rebuild_total_elapsed_ms = telemetry.total_elapsed_ms,
            "{message}"
        );
    }
}
