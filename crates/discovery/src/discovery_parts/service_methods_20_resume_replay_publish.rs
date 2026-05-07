use super::*;

impl DiscoveryService {
    pub(crate) fn repair_restored_replay_or_publish_state_for_resume(
        &self,
        state: &mut PersistedStreamRebuildState,
        now: DateTime<Utc>,
    ) -> bool {
        let mut changed = false;
        if state.phase != DiscoveryPersistedRebuildPhase::PublishPending
            && (state.payload.publish_pending_requested_wallet_ids.is_some()
                || state.payload.publish_pending_quality_retry_mints.is_some())
        {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                "clearing stale persisted exact publish-set ownership from a non-publish-pending checkpoint before resume"
            );
            state.payload.publish_pending_requested_wallet_ids = None;
            state.payload.publish_pending_quality_retry_mints = None;
            changed = true;
        }
        if Self::state_has_replay_wallet_stats_milestone(state)
            && !state.payload.replay_wallet_stats_milestone_reached
        {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                "backfilling durable post-wallet-stats replay milestone on a persisted replay lineage so future rollover/restart cannot silently degrade it back to wallet_stats"
            );
            state.payload.replay_wallet_stats_milestone_reached = true;
            changed = true;
        }
        if state.payload.replay_sol_leg_reentry_pending {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                "clearing stale carried SOL-leg replay reentry marker from a persisted replay/publish checkpoint because it is only meaningful before token-quality hands the target window back into replay"
            );
            state.payload.replay_sol_leg_reentry_pending = false;
            changed = true;
        }
        if state.phase == DiscoveryPersistedRebuildPhase::PublishPending
            && state.payload.publish_pending_requested_wallet_ids.is_none()
        {
            let requested_wallet_ids = self
                .publish_pending_requested_wallet_ids_from_snapshots(
                    &state.payload.completed_snapshots,
                );
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_publish_pending_requested_wallet_count =
                    requested_wallet_ids.len(),
                "backfilling exact publish-set ownership onto a persisted publish-pending checkpoint before resume"
            );
            state.payload.publish_pending_requested_wallet_ids = Some(requested_wallet_ids);
            changed = true;
        }
        if state.phase == DiscoveryPersistedRebuildPhase::PublishPending
            && state
                .payload
                .publish_pending_requested_wallet_ids
                .as_ref()
                .is_some_and(|wallets| wallets.is_empty())
            && state.payload.publish_pending_quality_retry_mints.is_none()
        {
            let retry_mints =
                self.legacy_publish_pending_quality_retry_backfill_mints(state);
            if !retry_mints.is_empty() {
                info!(
                    rebuild_window_start = %state.window_start,
                    rebuild_horizon_end = %state.horizon_end,
                    rebuild_phase = state.phase.as_str(),
                    rebuild_publish_pending_requested_wallet_count = 0usize,
                    rebuild_publish_pending_quality_retry_mint_count = retry_mints.len(),
                    "backfilling exact publish-quality retry ownership onto a retained zero-wallet publish-pending checkpoint so later cycles can re-resolve missing quality truth instead of looping forever on an empty publish set"
                );
                state.payload.publish_pending_quality_retry_mints = Some(retry_mints);
                changed = true;
            }
        }
        if state.phase == DiscoveryPersistedRebuildPhase::PublishPending
            && state
                .payload
                .publish_pending_requested_wallet_ids
                .as_ref()
                .is_some_and(|wallets| !wallets.is_empty())
            && state.payload.publish_pending_quality_retry_mints.is_some()
        {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                "clearing stale exact publish-quality retry ownership from a publish-pending checkpoint that already carries a non-empty exact publish set"
            );
            state.payload.publish_pending_quality_retry_mints = None;
            changed = true;
        }
        if Self::payload_has_exact_buy_mint_membership(&state.payload) {
            Self::sync_unique_buy_mints_from_counts(&mut state.payload);
        }
        if !state
            .payload
            .replay_exact_target_surface_staged_wallet_ids
            .is_empty()
            && state.payload.replay_exact_target_surface_pre_row_blocked
        {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                "clearing stale pre-row blocked marker because a staged exact wallet-id page is already present and should be resumed on the next cycle"
            );
            state.payload.replay_exact_target_surface_pre_row_blocked = false;
            changed = true;
        }
        if !state
            .payload
            .replay_exact_target_surface_staged_wallet_ids
            .is_empty()
            && state
                .payload
                .replay_exact_target_surface_staged_wallet_cursor_after
                .is_none()
        {
            state
                .payload
                .replay_exact_target_surface_staged_wallet_cursor_after = state
                .payload
                .replay_exact_target_surface_staged_wallet_ids
                .last()
                .cloned();
            changed = true;
        }
        if !Self::state_owns_exact_target_buy_mint_surface_backfill_seam(state)
            && (state
                .payload
                .replay_exact_target_surface_wallet_cursor
                .is_some()
                || !state
                    .payload
                    .replay_exact_target_surface_staged_wallet_ids
                    .is_empty()
                || state.payload.replay_exact_target_surface_pre_row_blocked)
        {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                "clearing stale persisted exact target-surface repair resume state because the resumed checkpoint no longer owns that backfill seam"
            );
            Self::clear_replay_exact_target_surface_resume_state(&mut state.payload);
            changed = true;
        }
        changed |= self.repair_restored_replay_publish_frozen_target_state_for_resume(state, now);
        changed |= self.repair_restored_replay_publish_checkpoint_state_for_resume(state);
        changed
    }
}
