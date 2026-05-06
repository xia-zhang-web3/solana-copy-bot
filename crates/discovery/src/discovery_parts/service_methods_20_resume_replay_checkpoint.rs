impl DiscoveryService {
    fn repair_restored_replay_publish_frozen_target_state_for_resume(
        &self,
        state: &mut PersistedStreamRebuildState,
        now: DateTime<Utc>,
    ) -> bool {
        let mut changed = false;
        if Self::state_can_freeze_exact_target_buy_mint_surface_for_partial_sol_leg_checkpoint(state) {
            let had_streaming_token_state_ballast = state.payload.token_states.values().any(
                |token_state| token_state.first_seen.is_some() || !token_state.wallets_seen.is_empty(),
            ) || state.payload.token_states.iter().any(|(_, token_state)| {
                token_state.sol_trades_5m.is_empty()
                    && token_state.sol_volume_5m <= 0.0
                    && token_state.sol_traders_5m.is_empty()
            });
            if had_streaming_token_state_ballast {
                let token_states_before = state.payload.token_states.len();
                Self::compact_streaming_token_state_for_frozen_exact_target_checkpoint(
                    &mut state.payload,
                );
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
                    rebuild_token_states_before = token_states_before,
                    rebuild_token_states_after = state.payload.token_states.len(),
                    "dropping redundant streaming token-state ballast from a frozen exact-target replay checkpoint before resume because partial SOL-leg replay derives tradability from rolling SOL-leg trades, not cumulative token wallet-membership"
                );
                changed = true;
            }
            let target_mints: HashSet<String> = state
                .payload
                .discovery_critical_target_buy_mints
                .iter()
                .cloned()
                .collect();
            let quality_cached_before = state.payload.token_quality_cache.len();
            let has_non_target_or_nonreusable_quality_cache = state
                .payload
                .token_quality_cache
                .iter()
                .any(|(mint, resolution)| {
                    !target_mints.contains(mint)
                        || !Self::token_quality_resolution_is_reusable_for_resume(
                            resolution,
                            now,
                        )
                });
            if has_non_target_or_nonreusable_quality_cache {
                Self::compact_token_quality_cache_for_frozen_exact_target_checkpoint(
                    &mut state.payload,
                    now,
                );
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
                    rebuild_quality_cached_mints_before = quality_cached_before,
                    rebuild_quality_cached_mints_after =
                        state.payload.token_quality_cache.len(),
                    "dropping redundant non-target token-quality cache entries from a frozen exact-target replay checkpoint before resume because partial SOL-leg replay only consumes reusable quality truth for the frozen target mint surface"
                );
                changed = true;
            }
        }
        changed
    }

    fn repair_restored_replay_publish_checkpoint_state_for_resume(
        &self,
        state: &mut PersistedStreamRebuildState,
    ) -> bool {
        let mut changed = false;
        if state.phase == DiscoveryPersistedRebuildPhase::Replay
            && state.payload.replay_mode == ReplayMode::LegacyCompleteReplay
        {
            if Self::replay_checkpoint_has_local_progress(state) {
                warn!(
                    rebuild_window_start = %state.window_start,
                    rebuild_horizon_end = %state.horizon_end,
                    rebuild_phase_cursor_ts = ?state.phase_cursor.as_ref().map(|cursor| cursor.ts_utc),
                    rebuild_phase_cursor_slot = state.phase_cursor.as_ref().map(|cursor| cursor.slot),
                    rebuild_phase_cursor_signature =
                        state.phase_cursor.as_ref().map(|cursor| cursor.signature.as_str()),
                    rebuild_replay_rows_processed = state.replay_rows_processed,
                    rebuild_replay_pages_processed = state.replay_pages_processed,
                    "rewinding legacy replay checkpoint onto optimized replay pipeline before resume"
                );
            } else {
                info!(
                    rebuild_window_start = %state.window_start,
                    rebuild_horizon_end = %state.horizon_end,
                    rebuild_phase = state.phase.as_str(),
                    "upgrading zero-progress legacy replay checkpoint onto optimized replay pipeline before resume"
                );
            }
            Self::reset_replay_progress_for_optimized_resume(state);
            changed = true;
        }
        if Self::canonicalize_unique_buy_mints(&mut state.payload.unique_buy_mints) {
            warn!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                "rewinding persisted replay/publish checkpoint onto canonical sorted buy-mint order before resume"
            );
            state.phase = DiscoveryPersistedRebuildPhase::ResolveTokenQuality;
            state.phase_cursor = None;
            state.replay_rows_processed = 0;
            state.replay_pages_processed = 0;
            state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
            Self::reset_replay_wallet_stats_progress(&mut state.payload);
            state.payload.token_quality_cache.clear();
            state.payload.token_quality_progress =
                quality_cache::TokenQualityResolutionProgress::default();
            state.payload.by_wallet.clear();
            state.payload.token_states.clear();
            state.payload.token_recent_sol_trades.clear();
            state.payload.pending_rug_checks.clear();
            state.payload.token_pending_buy_starts.clear();
            state.payload.completed_snapshots.clear();
            state.payload.discovery_critical_target_buy_mints.clear();
            state.payload.publish_pending_requested_wallet_ids = None;
            changed = true;
        }
        if self.config.min_buy_count > 0
            && state.phase == DiscoveryPersistedRebuildPhase::Replay
            && state.payload.replay_wallet_stats_milestone_reached
            && !state.payload.replay_wallet_stats_complete
        {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_replay_wallet_stats_rows_processed =
                    state.payload.replay_wallet_stats_rows_processed,
                rebuild_replay_wallet_stats_pages_processed =
                    state.payload.replay_wallet_stats_pages_processed,
                rebuild_wallets_buffered = state.payload.by_wallet.len(),
                "repairing degraded persisted replay checkpoint back to SOL-leg reentry because the durable post-wallet-stats milestone proves wallet_stats was already safely crossed earlier in the same lineage"
            );
            Self::transition_persisted_stream_from_wallet_stats_to_sol_leg_with_candidate_activity_backfill(state);
            changed = true;
        }
        changed |= self.repair_replay_wallet_stats_budget_hints_for_resume(state);
        changed |= self.repair_replay_sol_leg_budget_hints_for_resume(state);
        changed
    }
}
