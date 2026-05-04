impl DiscoveryService {
    fn advance_persisted_stream_active_phase(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        active_phase: DiscoveryPersistedRebuildPhase,
        fetch_limit: usize,
        fetch_page_limit: usize,
        collect_buy_mints_phase_page_limit_override: Option<usize>,
        replay_wallet_stats_phase_page_limit_override: Option<usize>,
        replay_sol_leg_phase_page_limit_override: Option<usize>,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        match active_phase {
            DiscoveryPersistedRebuildPhase::CollectBuyMints => self.advance_persisted_stream_prepass(
                store,
                state,
                fetch_limit,
                fetch_page_limit,
                collect_buy_mints_phase_page_limit_override,
                deadline,
            ),
            DiscoveryPersistedRebuildPhase::ResolveTokenQuality => self
                .advance_persisted_stream_token_quality(
                    store,
                    state,
                    fetch_limit,
                    fetch_page_limit,
                    deadline,
                ),
            DiscoveryPersistedRebuildPhase::Replay => self
                .advance_persisted_stream_replay_with_phase_page_limits(
                    store,
                    state,
                    fetch_limit,
                    fetch_page_limit,
                    replay_wallet_stats_phase_page_limit_override,
                    replay_sol_leg_phase_page_limit_override,
                    deadline,
                ),
            DiscoveryPersistedRebuildPhase::PublishPending => unreachable!(
                "publish-pending checkpoints are returned before phase advancement"
            ),
        }
    }

    fn apply_persisted_stream_phase_advance_to_state(
        state: &mut PersistedStreamRebuildState,
        phase_advance: &PersistedStreamPhaseAdvance,
    ) {
        match state.phase {
            DiscoveryPersistedRebuildPhase::CollectBuyMints => {
                state.prepass_rows_processed = state
                    .prepass_rows_processed
                    .saturating_add(phase_advance.rows_processed);
                state.prepass_pages_processed = state
                    .prepass_pages_processed
                    .saturating_add(phase_advance.pages_processed);
            }
            DiscoveryPersistedRebuildPhase::ResolveTokenQuality => {}
            DiscoveryPersistedRebuildPhase::Replay => {
                state.payload.replay_wallet_stats_rows_processed = state
                    .payload
                    .replay_wallet_stats_rows_processed
                    .saturating_add(phase_advance.replay_wallet_stats_rows_processed);
                state.payload.replay_wallet_stats_pages_processed = state
                    .payload
                    .replay_wallet_stats_pages_processed
                    .saturating_add(phase_advance.replay_wallet_stats_pages_processed);
                state
                    .payload
                    .replay_wallet_stats_day_count_source_progress
                    .merge(phase_advance.replay_wallet_stats_day_count_source_progress);
                state.replay_rows_processed = state
                    .replay_rows_processed
                    .saturating_add(phase_advance.rows_processed);
                state.replay_pages_processed = state
                    .replay_pages_processed
                    .saturating_add(phase_advance.pages_processed);
            }
            DiscoveryPersistedRebuildPhase::PublishPending => {}
        }
        state.phase_cursor = phase_advance.phase_cursor.clone();
        if state.phase == DiscoveryPersistedRebuildPhase::CollectBuyMints {
            state.payload.collect_buy_mints_cursor_token =
                phase_advance.collect_buy_mints_cursor_token.clone();
        }
    }

    fn handle_persisted_stream_rebuild_source_exhausted(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        active_phase: DiscoveryPersistedRebuildPhase,
        now: DateTime<Utc>,
        cycle: &PersistedStreamAdvanceCycleState,
    ) -> Result<Option<PersistedStreamRebuildAdvanceOutcome>> {
        if active_phase == DiscoveryPersistedRebuildPhase::CollectBuyMints {
            Self::transition_persisted_stream_rebuild_collect_buy_mints_exhausted(
                state, now, cycle,
            );
            return Ok(None);
        }
        if active_phase == DiscoveryPersistedRebuildPhase::ResolveTokenQuality {
            Self::transition_persisted_stream_rebuild_token_quality_exhausted(
                state,
                self.config.min_buy_count > 0,
            );
            return Ok(None);
        }
        self.complete_persisted_stream_rebuild_from_replay(store, state, now, cycle)
            .map(Some)
    }

    fn transition_persisted_stream_rebuild_collect_buy_mints_exhausted(
        state: &mut PersistedStreamRebuildState,
        now: DateTime<Utc>,
        cycle: &PersistedStreamAdvanceCycleState,
    ) {
        if Self::payload_has_exact_buy_mint_membership(&state.payload) {
            Self::sync_unique_buy_mints_from_counts(&mut state.payload);
        }
        if Self::canonicalize_unique_buy_mints(&mut state.payload.unique_buy_mints) {
            warn!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                "normalized collect_buy_mints output to canonical sorted distinct order before token-quality resolution"
            );
        }
        Self::transition_persisted_stream_from_collect_buy_mints_to_token_quality(state, now);
        info!(
            rebuild_window_start = %state.window_start,
            rebuild_horizon_end = %state.horizon_end,
            rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
            rebuild_cycle_unique_buy_mints_discovered = cycle.unique_buy_mints_discovered,
            rebuild_prepass_rows_processed = state.prepass_rows_processed,
            rebuild_prepass_pages_processed = state.prepass_pages_processed,
            rebuild_quality_cached_mints = state.payload.token_quality_cache.len(),
            rebuild_quality_next_mint_index = state.payload.token_quality_progress.next_mint_index,
            "completed bounded discovery persisted observed_swaps prepass; switching to bounded token-quality resolution"
        );
    }

    fn transition_persisted_stream_rebuild_token_quality_exhausted(
        state: &mut PersistedStreamRebuildState,
        min_buy_count_enabled: bool,
    ) {
        let replay_sol_leg_reentry_pending = state.payload.replay_sol_leg_reentry_pending;
        let replay_wallet_stats_milestone_reached =
            state.payload.replay_wallet_stats_milestone_reached;
        Self::transition_persisted_stream_from_token_quality_to_replay(
            state,
            min_buy_count_enabled,
        );
        if replay_sol_leg_reentry_pending || replay_wallet_stats_milestone_reached {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                rebuild_quality_next_mint_index = state.payload.token_quality_progress.next_mint_index,
                rebuild_quality_rpc_attempted = state.payload.token_quality_progress.rpc_attempted,
                rebuild_quality_rpc_spent_ms = state.payload.token_quality_progress.rpc_spent_ms,
                rebuild_replay_wallet_stats_budget_floor_wallets =
                    state.payload.replay_wallet_stats_budget_floor_wallets,
                rebuild_replay_wallet_stats_last_partial_cycle_pages_processed =
                    state.payload.replay_wallet_stats_last_partial_cycle_pages_processed,
                rebuild_replay_wallet_stats_last_partial_cycle_wallets_processed =
                    state.payload.replay_wallet_stats_last_partial_cycle_wallets_processed,
                rebuild_replay_wallet_stats_last_partial_cycle_elapsed_ms =
                    state.payload.replay_wallet_stats_last_partial_cycle_elapsed_ms,
                rebuild_replay_wallet_stats_budget_floor_carried_forward_into_replay =
                    state.payload.replay_wallet_stats_budget_floor_wallets > 0,
                rebuild_replay_wallet_stats_complete_carried_forward_into_replay =
                    state.payload.replay_wallet_stats_complete,
                rebuild_replay_wallet_stats_milestone_reached =
                    state.payload.replay_wallet_stats_milestone_reached,
                rebuild_replay_candidate_activity_backfill_required =
                    state.payload.replay_candidate_activity_backfill_required,
                "completed bounded discovery token-quality resolution; re-entering replay directly at SOL-leg because the wallet-stats milestone was safely carried forward across rollover"
            );
        } else {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                rebuild_quality_next_mint_index = state.payload.token_quality_progress.next_mint_index,
                rebuild_quality_rpc_attempted = state.payload.token_quality_progress.rpc_attempted,
                rebuild_quality_rpc_spent_ms = state.payload.token_quality_progress.rpc_spent_ms,
                rebuild_replay_wallet_stats_budget_floor_wallets =
                    state.payload.replay_wallet_stats_budget_floor_wallets,
                rebuild_replay_wallet_stats_last_partial_cycle_pages_processed =
                    state.payload.replay_wallet_stats_last_partial_cycle_pages_processed,
                rebuild_replay_wallet_stats_last_partial_cycle_wallets_processed =
                    state.payload.replay_wallet_stats_last_partial_cycle_wallets_processed,
                rebuild_replay_wallet_stats_last_partial_cycle_elapsed_ms =
                    state.payload.replay_wallet_stats_last_partial_cycle_elapsed_ms,
                rebuild_replay_wallet_stats_budget_floor_carried_forward_into_replay =
                    state.payload.replay_wallet_stats_budget_floor_wallets > 0,
                rebuild_replay_wallet_stats_complete_carried_forward_into_replay = false,
                rebuild_replay_wallet_stats_milestone_reached =
                    state.payload.replay_wallet_stats_milestone_reached,
                rebuild_replay_candidate_activity_backfill_required =
                    state.payload.replay_candidate_activity_backfill_required,
                "completed bounded discovery token-quality resolution; switching to replay while preserving any carried wallet-stats budgeting memory for the new target window"
            );
        }
    }
}
