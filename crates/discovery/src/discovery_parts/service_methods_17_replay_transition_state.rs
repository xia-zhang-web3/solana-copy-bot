impl DiscoveryService {
    fn transition_persisted_stream_from_wallet_stats_to_sol_leg_with_candidate_activity_backfill(
        state: &mut PersistedStreamRebuildState,
    ) {
        let buffered_wallets = state.payload.by_wallet.len();
        state.payload.replay_wallet_stats_complete = true;
        state.payload.replay_wallet_stats_milestone_reached = true;
        state.payload.replay_wallet_stats_wallet_cursor = None;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed = 0;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_rows_processed = 0;
        state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms = 0;
        state.payload.replay_sol_leg_budget_floor_pages = 0;
        state.payload.replay_sol_leg_retained_contract_floor_pages = 0;
        state.payload.replay_candidate_activity_backfill_required = true;
        state.payload.replay_candidate_activity_backfill_pending = false;
        state
            .payload
            .replay_candidate_activity_backfill_wallet_cursor = None;
        Self::clear_replay_exact_target_surface_resume_state(&mut state.payload);
        state.phase_cursor = None;
        state.payload.by_wallet.clear();
        info!(
            rebuild_window_start = %state.window_start,
            rebuild_horizon_end = %state.horizon_end,
            rebuild_wallets_buffered_before = buffered_wallets,
            rebuild_replay_wallet_stats_budget_floor_wallets =
                state.payload.replay_wallet_stats_budget_floor_wallets,
            rebuild_replay_wallet_stats_last_partial_cycle_pages_processed =
                state.payload.replay_wallet_stats_last_partial_cycle_pages_processed,
            rebuild_replay_wallet_stats_last_partial_cycle_wallets_processed =
                state.payload.replay_wallet_stats_last_partial_cycle_wallets_processed,
            rebuild_replay_wallet_stats_last_partial_cycle_elapsed_ms =
                state.payload.replay_wallet_stats_last_partial_cycle_elapsed_ms,
            "deferred exhaustive replay wallet-stats source exhaustion behind exact candidate-wallet activity backfill; clearing all-wallet activity buffer and switching to SOL-leg replay"
        );
    }

    fn prepare_persisted_stream_replay_candidate_activity_backfill(
        state: &mut PersistedStreamRebuildState,
    ) {
        state.phase_cursor = None;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed = 0;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_rows_processed = 0;
        state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms = 0;
        state.payload.replay_sol_leg_budget_floor_pages = 0;
        state.payload.replay_sol_leg_retained_contract_floor_pages = 0;
        state.payload.replay_candidate_activity_backfill_pending = true;
        state
            .payload
            .replay_candidate_activity_backfill_wallet_cursor = None;
        Self::clear_replay_exact_target_surface_resume_state(&mut state.payload);
        for acc in state.payload.by_wallet.values_mut() {
            acc.reset_activity_summary_for_exact_backfill();
        }
    }

    fn state_can_freeze_exact_target_buy_mint_surface_for_partial_sol_leg_checkpoint(
        state: &PersistedStreamRebuildState,
    ) -> bool {
        matches!(state.phase, DiscoveryPersistedRebuildPhase::Replay)
            && state.payload.replay_wallet_stats_complete
            && state.payload.replay_candidate_activity_backfill_required
            && !state.payload.replay_candidate_activity_backfill_pending
            && !state.payload.discovery_critical_target_buy_mints.is_empty()
    }

    fn state_owns_exact_target_buy_mint_surface_backfill_seam(
        state: &PersistedStreamRebuildState,
    ) -> bool {
        matches!(state.phase, DiscoveryPersistedRebuildPhase::Replay)
            && state.payload.collect_buy_mints_prepass_complete
            && matches!(state.payload.replay_mode, ReplayMode::WalletStatsThenSolLeg)
            && state.payload.replay_wallet_stats_complete
            && state.payload.replay_candidate_activity_backfill_required
            && !state.payload.replay_candidate_activity_backfill_pending
            && state.phase_cursor.is_some()
            && state.replay_rows_processed > 0
            && state.payload.replay_sol_leg_budget_floor_pages > 0
            && state.payload.discovery_critical_target_buy_mints.is_empty()
            && !state.payload.by_wallet.is_empty()
            && state.payload.by_wallet.values().any(|acc| {
                acc.buy_total > 0
                    || !acc.buy_mints.is_empty()
                    || !acc.positions.is_empty()
                    || !acc.buy_observations.is_empty()
            })
    }

    fn state_can_backfill_exact_target_buy_mint_surface_for_resume(
        state: &PersistedStreamRebuildState,
    ) -> bool {
        Self::state_owns_exact_target_buy_mint_surface_backfill_seam(state)
            && (!state.payload.replay_exact_target_surface_pre_row_blocked
                || !state
                    .payload
                    .replay_exact_target_surface_staged_wallet_ids
                    .is_empty())
    }

    fn persist_replay_exact_target_buy_mint_surface_budget_exhaustion_state(
        state: &mut PersistedStreamRebuildState,
        diagnostics: &mut ResumeExactTargetBuyMintSurfaceRepairDiagnostics,
        page: &copybot_storage::ObservedWalletActivityPage,
        wallet_cursor_after: Option<String>,
    ) {
        diagnostics.time_budget_exhausted = true;
        diagnostics.wallet_cursor_after = wallet_cursor_after;
        diagnostics.wallet_id_page_wallets_seen = diagnostics
            .wallet_id_page_wallets_seen
            .saturating_add(page.wallet_id_page_wallets_seen);
        if page.rows_seen == 0 && page.wallet_id_query_exhausted_before_first_page {
            state.payload.replay_exact_target_surface_pre_row_blocked = true;
            state
                .payload
                .replay_exact_target_surface_staged_wallet_ids
                .clear();
            state
                .payload
                .replay_exact_target_surface_staged_wallet_cursor_after = None;
            diagnostics.persisted_blocked_state = true;
            diagnostics.persisted_partial_progress = false;
            return;
        }
        if page.rows_seen == 0 && !page.wallet_id_page_wallet_ids.is_empty() {
            state.payload.replay_exact_target_surface_pre_row_blocked = false;
            state.payload.replay_exact_target_surface_staged_wallet_ids =
                page.wallet_id_page_wallet_ids.clone();
            state
                .payload
                .replay_exact_target_surface_staged_wallet_cursor_after =
                page.wallet_id_page_cursor_after.clone();
            diagnostics.persisted_staged_pre_row_state = true;
            diagnostics.persisted_partial_progress = false;
            return;
        }
        if page.rows_seen == 0 && page.wallet_id_page_wallets_seen > 0 {
            state.payload.replay_exact_target_surface_wallet_cursor = None;
            state.payload.replay_exact_target_surface_pre_row_blocked = true;
            state
                .payload
                .replay_exact_target_surface_staged_wallet_ids
                .clear();
            state
                .payload
                .replay_exact_target_surface_staged_wallet_cursor_after = None;
            diagnostics.persisted_blocked_state = true;
            diagnostics.persisted_partial_progress = false;
            return;
        }
        diagnostics.persisted_partial_progress = (diagnostics.wallet_rows > 0
            || diagnostics.wallet_id_page_wallets_seen > 0)
            && diagnostics.wallet_cursor_after != diagnostics.wallet_cursor_before;
        if diagnostics.persisted_partial_progress {
            state.payload.replay_exact_target_surface_wallet_cursor =
                diagnostics.wallet_cursor_after.clone();
        }
    }
}
