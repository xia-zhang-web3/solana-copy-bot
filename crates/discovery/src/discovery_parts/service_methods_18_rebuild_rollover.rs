use super::*;

impl DiscoveryService {
    pub(crate) fn reset_replay_progress_for_optimized_resume(state: &mut PersistedStreamRebuildState) {
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.phase_cursor = None;
        state.replay_rows_processed = 0;
        state.replay_pages_processed = 0;
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        Self::reset_replay_wallet_stats_progress(&mut state.payload);
        state.payload.by_wallet.clear();
        state.payload.token_states.clear();
        state.payload.token_recent_sol_trades.clear();
        state.payload.pending_rug_checks.clear();
        state.payload.token_pending_buy_starts.clear();
        state.payload.completed_snapshots.clear();
        state.payload.discovery_critical_target_buy_mints.clear();
        state.payload.publish_pending_requested_wallet_ids = None;
        state.payload.publish_pending_quality_retry_mints = None;
    }

    pub(crate) fn replay_checkpoint_has_local_progress(state: &PersistedStreamRebuildState) -> bool {
        state.replay_rows_processed > 0
            || state.replay_pages_processed > 0
            || state.phase_cursor.is_some()
            || !state.payload.by_wallet.is_empty()
            || !state.payload.token_states.is_empty()
            || !state.payload.token_recent_sol_trades.is_empty()
            || !state.payload.pending_rug_checks.is_empty()
            || !state.payload.token_pending_buy_starts.is_empty()
    }

    pub(crate) fn prepare_persisted_stream_rebuild_for_metrics_window_rollover(
        &self,
        state: &mut PersistedStreamRebuildState,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        if state.metrics_window_start == metrics_window_start {
            return Ok(false);
        }
        if !Self::state_can_carry_forward_metrics_rollover(state) {
            return Ok(false);
        }

        let source_window_start = state.window_start;
        let source_horizon_end = state.horizon_end;
        let source_metrics_window_start = state.metrics_window_start;
        let old_phase = state.phase;
        let old_replay_wallet_stats_complete = state.payload.replay_wallet_stats_complete;
        let old_replay_candidate_activity_backfill_pending =
            state.payload.replay_candidate_activity_backfill_pending;
        let old_collect_cursor = state.payload.collect_buy_mints_cursor_token.clone();
        let carried_replay_wallet_stats_budget_floor_wallets =
            Self::replay_wallet_stats_buffered_wallet_backlog_floor_wallets(state);
        let carried_replay_wallet_stats_last_partial_cycle_pages_processed = state
            .payload
            .replay_wallet_stats_last_partial_cycle_pages_processed;
        let carried_replay_wallet_stats_last_partial_cycle_wallets_processed = state
            .payload
            .replay_wallet_stats_last_partial_cycle_wallets_processed;
        let carried_replay_wallet_stats_last_partial_cycle_elapsed_ms = state
            .payload
            .replay_wallet_stats_last_partial_cycle_elapsed_ms;
        let carried_replay_wallet_stats_milestone_reached =
            self.config.min_buy_count > 0 && Self::state_has_replay_wallet_stats_milestone(state);
        let carried_replay_sol_leg_reentry_pending =
            self.state_can_carry_forward_replay_sol_leg_reentry(state);
        let carry_sol_leg_budget_hints = old_phase == DiscoveryPersistedRebuildPhase::Replay
            && old_replay_wallet_stats_complete
            && !old_replay_candidate_activity_backfill_pending;
        let carried_replay_sol_leg_last_partial_cycle_pages_processed =
            if carry_sol_leg_budget_hints {
                state
                    .payload
                    .replay_sol_leg_last_partial_cycle_pages_processed
            } else {
                0
            };
        let carried_replay_sol_leg_last_partial_cycle_rows_processed = if carry_sol_leg_budget_hints
        {
            state
                .payload
                .replay_sol_leg_last_partial_cycle_rows_processed
        } else {
            0
        };
        let carried_replay_sol_leg_last_partial_cycle_elapsed_ms = if carry_sol_leg_budget_hints {
            state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms
        } else {
            0
        };
        let carried_replay_sol_leg_budget_floor_pages = if carry_sol_leg_budget_hints {
            state.payload.replay_sol_leg_budget_floor_pages
        } else {
            0
        };
        let carried_replay_sol_leg_retained_contract_floor_pages = if carry_sol_leg_budget_hints {
            state.payload.replay_sol_leg_retained_contract_floor_pages
        } else {
            0
        };
        let carry_token_quality_cache = Self::payload_has_exact_buy_mint_membership(&state.payload)
            && (state.payload.collect_buy_mints_prepass_complete
                || state.phase != DiscoveryPersistedRebuildPhase::CollectBuyMints);
        let carried_token_quality_cache = if carry_token_quality_cache {
            let mut carried = state.payload.token_quality_cache.clone();
            let exact_buy_mints: HashSet<String> =
                state.payload.buy_mint_counts.keys().cloned().collect();
            carried.retain(|mint, resolution| {
                exact_buy_mints.contains(mint)
                    && Self::token_quality_resolution_is_reusable_for_resume(resolution, now)
            });
            carried
        } else {
            HashMap::new()
        };
        let prepass_complete = state.payload.collect_buy_mints_prepass_complete
            || state.phase != DiscoveryPersistedRebuildPhase::CollectBuyMints;

        state.window_start = window_start;
        state.horizon_end = now;
        state.metrics_window_start = metrics_window_start;
        state.payload.collect_buy_mints_prepass_complete = prepass_complete;
        state.payload.collect_buy_mints_cursor_token = if prepass_complete {
            None
        } else {
            old_collect_cursor
        };
        state
            .payload
            .collect_buy_mints_reconcile_source_window_start = Some(source_window_start);
        state.payload.collect_buy_mints_reconcile_source_horizon_end = Some(source_horizon_end);
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor = None;
        state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor_token = None;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token = None;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
        Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
        Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
        Self::sync_unique_buy_mints_from_counts(&mut state.payload);
        Self::reset_bucket_sensitive_rebuild_state_for_rollover(state);
        state.payload.replay_wallet_stats_budget_floor_wallets =
            carried_replay_wallet_stats_budget_floor_wallets;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_pages_processed =
            carried_replay_wallet_stats_last_partial_cycle_pages_processed;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_wallets_processed =
            carried_replay_wallet_stats_last_partial_cycle_wallets_processed;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_elapsed_ms =
            carried_replay_wallet_stats_last_partial_cycle_elapsed_ms;
        state.payload.replay_wallet_stats_milestone_reached =
            carried_replay_wallet_stats_milestone_reached;
        state.payload.replay_sol_leg_reentry_pending = carried_replay_sol_leg_reentry_pending;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed =
            carried_replay_sol_leg_last_partial_cycle_pages_processed;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_rows_processed =
            carried_replay_sol_leg_last_partial_cycle_rows_processed;
        state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms =
            carried_replay_sol_leg_last_partial_cycle_elapsed_ms;
        state.payload.replay_sol_leg_budget_floor_pages = carried_replay_sol_leg_budget_floor_pages;
        state.payload.replay_sol_leg_retained_contract_floor_pages =
            carried_replay_sol_leg_retained_contract_floor_pages;
        state.payload.token_quality_cache = carried_token_quality_cache;
        state.payload.token_quality_progress =
            quality_cache::TokenQualityResolutionProgress::default();
        info!(
            rebuild_previous_phase = old_phase.as_str(),
            rebuild_previous_replay_subphase = Self::replay_subphase(
                old_phase,
                old_replay_wallet_stats_complete,
                old_replay_candidate_activity_backfill_pending,
            ),
            rebuild_previous_window_start = %source_window_start,
            rebuild_previous_horizon_end = %source_horizon_end,
            rebuild_previous_metrics_window_start = %source_metrics_window_start,
            rebuild_target_window_start = %window_start,
            rebuild_target_horizon_end = %now,
            rebuild_target_metrics_window_start = %metrics_window_start,
            rebuild_collect_buy_mints_cursor_token =
                state.payload.collect_buy_mints_cursor_token.as_deref(),
            rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
            rebuild_replay_wallet_stats_budget_floor_wallets =
                state.payload.replay_wallet_stats_budget_floor_wallets,
            rebuild_replay_wallet_stats_last_partial_cycle_pages_processed =
                state.payload.replay_wallet_stats_last_partial_cycle_pages_processed,
            rebuild_replay_wallet_stats_last_partial_cycle_wallets_processed =
                state.payload.replay_wallet_stats_last_partial_cycle_wallets_processed,
            rebuild_replay_wallet_stats_last_partial_cycle_elapsed_ms =
                state.payload.replay_wallet_stats_last_partial_cycle_elapsed_ms,
            rebuild_replay_wallet_stats_milestone_reached =
                state.payload.replay_wallet_stats_milestone_reached,
            rebuild_replay_sol_leg_reentry_pending =
                state.payload.replay_sol_leg_reentry_pending,
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
            rebuild_quality_cached_mints_carried_forward =
                state.payload.token_quality_cache.len(),
            rebuild_quality_next_mint_index_carried_forward =
                state.payload.token_quality_progress.next_mint_index,
            "carrying forward exact canonical buy-mint membership progress across metrics bucket rollover; bucket-sensitive quality/replay state reset for fresh target-window rebuild"
        );
        Ok(true)
    }

    pub(crate) fn persisted_stream_rebuild_restart_reason(
        &self,
        state: &PersistedStreamRebuildState,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Option<&'static str> {
        if state.window_start > window_start {
            return Some("window_start_in_future");
        }
        if state.horizon_end > now {
            return Some("horizon_in_future");
        }
        None
    }

}
