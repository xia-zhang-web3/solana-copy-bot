impl DiscoveryService {
    fn persisted_stream_progress_telemetry_from_state(
        &self,
        state: &PersistedStreamRebuildState,
        now: DateTime<Utc>,
    ) -> PersistedStreamProgressTelemetry {
        PersistedStreamProgressTelemetry {
            phase: state.phase,
            collect_buy_mints_mode: state.payload.collect_buy_mints_mode,
            replay_mode: state.payload.replay_mode,
            replay_subphase: Self::replay_subphase(
                state.phase,
                state.payload.replay_wallet_stats_complete,
                state.payload.replay_candidate_activity_backfill_pending,
            ),
            window_start: state.window_start,
            horizon_end: state.horizon_end,
            metrics_window_start: state.metrics_window_start,
            phase_cursor: state.phase_cursor.clone(),
            replay_wallet_stats_wallet_cursor: state
                .payload
                .replay_wallet_stats_wallet_cursor
                .clone(),
            collect_buy_mints_cursor_token: state.payload.collect_buy_mints_cursor_token.clone(),
            collect_buy_mints_reconcile_source_window_start: state
                .payload
                .collect_buy_mints_reconcile_source_window_start,
            collect_buy_mints_reconcile_source_horizon_end: state
                .payload
                .collect_buy_mints_reconcile_source_horizon_end,
            collect_buy_mints_reconcile_expired_head_cursor: state
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor
                .clone(),
            collect_buy_mints_reconcile_new_tail_cursor: state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor
                .clone(),
            collect_buy_mints_reconcile_expired_head_cursor_token: state
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor_token
                .clone(),
            collect_buy_mints_reconcile_new_tail_cursor_token: state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .clone(),
            collect_buy_mints_reconcile_expired_head_pending_mints: state
                .payload
                .collect_buy_mints_reconcile_expired_head_pending_mints
                .len(),
            collect_buy_mints_reconcile_new_tail_slice_end_token: state
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token
                .clone(),
            collect_buy_mints_reconcile_new_tail_pending_mints: state
                .payload
                .collect_buy_mints_reconcile_new_tail_pending_mints
                .len(),
            prepass_rows_processed: state.prepass_rows_processed,
            prepass_pages_processed: state.prepass_pages_processed,
            replay_wallet_stats_complete: state.payload.replay_wallet_stats_complete,
            replay_wallet_stats_rows_processed: state.payload.replay_wallet_stats_rows_processed,
            replay_wallet_stats_pages_processed: state.payload.replay_wallet_stats_pages_processed,
            replay_wallet_stats_day_count_source_progress: state
                .payload
                .replay_wallet_stats_day_count_source_progress,
            replay_wallet_stats_budget_floor_wallets: state
                .payload
                .replay_wallet_stats_budget_floor_wallets,
            replay_wallet_stats_last_partial_cycle_pages_processed: state
                .payload
                .replay_wallet_stats_last_partial_cycle_pages_processed,
            replay_wallet_stats_last_partial_cycle_wallets_processed: state
                .payload
                .replay_wallet_stats_last_partial_cycle_wallets_processed,
            replay_wallet_stats_last_partial_cycle_elapsed_ms: state
                .payload
                .replay_wallet_stats_last_partial_cycle_elapsed_ms,
            replay_wallet_stats_publishable_horizon_remaining_ms: self
                .replay_wallet_stats_remaining_publishable_horizon_ms(state, now),
            replay_wallet_stats_milestone_reached: state
                .payload
                .replay_wallet_stats_milestone_reached,
            replay_sol_leg_reentry_pending: state.payload.replay_sol_leg_reentry_pending,
            replay_sol_leg_last_partial_cycle_pages_processed: state
                .payload
                .replay_sol_leg_last_partial_cycle_pages_processed,
            replay_sol_leg_last_partial_cycle_rows_processed: state
                .payload
                .replay_sol_leg_last_partial_cycle_rows_processed,
            replay_sol_leg_last_partial_cycle_elapsed_ms: state
                .payload
                .replay_sol_leg_last_partial_cycle_elapsed_ms,
            replay_sol_leg_budget_floor_pages: state.payload.replay_sol_leg_budget_floor_pages,
            replay_sol_leg_publishable_horizon_remaining_ms: self
                .replay_sol_leg_remaining_publishable_horizon_ms(state, now),
            replay_sol_leg_retained_contract_floor_pages: state
                .payload
                .replay_sol_leg_retained_contract_floor_pages,
            replay_candidate_activity_backfill_required: state
                .payload
                .replay_candidate_activity_backfill_required,
            replay_candidate_activity_backfill_wallet_cursor: state
                .payload
                .replay_candidate_activity_backfill_wallet_cursor
                .clone(),
            replay_sol_leg_access_path: None,
            replay_rows_processed: state.replay_rows_processed,
            replay_pages_processed: state.replay_pages_processed,
            chunks_completed: state.chunks_completed,
            cycle_rows_processed: 0,
            cycle_pages_processed: 0,
            cycle_replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress::default(),
            cycle_replay_wallet_stats_wallet_cursor_before: state
                .payload
                .replay_wallet_stats_wallet_cursor
                .clone(),
            cycle_replay_wallet_stats_wallet_cursor_after: state
                .payload
                .replay_wallet_stats_wallet_cursor
                .clone(),
            cycle_unique_buy_mints_discovered: 0,
            observed_swaps_loaded: Self::persisted_stream_observed_swaps_loaded(state),
            unique_buy_mints: state.payload.unique_buy_mints.len(),
            quality_next_mint_index: state.payload.token_quality_progress.next_mint_index,
            quality_rpc_attempted: state.payload.token_quality_progress.rpc_attempted,
            quality_rpc_spent_ms: state.payload.token_quality_progress.rpc_spent_ms,
            wallets_buffered: if state.phase == DiscoveryPersistedRebuildPhase::PublishPending {
                state.payload.completed_snapshots.len()
            } else {
                state.payload.by_wallet.len()
            },
            publish_pending_requested_wallet_count: if state.phase
                == DiscoveryPersistedRebuildPhase::PublishPending
            {
                self.publish_pending_requested_wallet_ids_from_state(state)
                    .len()
            } else {
                0
            },
            started_at: state.started_at,
            cycle_elapsed_ms: 0,
            total_elapsed_ms: (now
                .signed_duration_since(state.started_at)
                .num_milliseconds()
                .max(0) as u64),
            partial: state.phase != DiscoveryPersistedRebuildPhase::PublishPending,
            completed: state.phase == DiscoveryPersistedRebuildPhase::PublishPending,
            budget_exhausted_reason: None,
        }
    }

}
