use super::*;

impl DiscoveryService {
    pub(crate) fn persist_persisted_stream_rebuild_state(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        if matches!(
            state.phase,
            DiscoveryPersistedRebuildPhase::Replay | DiscoveryPersistedRebuildPhase::PublishPending
        ) && !state.payload.by_wallet.is_empty()
        {
            if state.payload.replay_exact_target_surface_wallet_cursor.is_some()
                || !state
                    .payload
                    .replay_exact_target_surface_staged_wallet_ids
                    .is_empty()
                || state.payload.replay_exact_target_surface_pre_row_blocked
            {
                state.payload.discovery_critical_target_buy_mints.clear();
            } else if Self::state_can_freeze_exact_target_buy_mint_surface_for_partial_sol_leg_checkpoint(
                state,
            ) {
                Self::compact_wallet_activity_summary_for_frozen_exact_target_checkpoint(
                    &mut state.payload,
                );
                Self::compact_streaming_token_state_for_frozen_exact_target_checkpoint(
                    &mut state.payload,
                );
                Self::compact_token_quality_cache_for_frozen_exact_target_checkpoint(
                    &mut state.payload,
                    state.horizon_end,
                );
            } else {
                state.payload.discovery_critical_target_buy_mints = self
                    .discovery_critical_target_buy_mints_from_accumulators(
                        store,
                        &state.payload.by_wallet,
                        state.horizon_end,
                    )?;
            }
        }
        state.updated_at = updated_at;
        let row = Self::persisted_stream_rebuild_row(state, updated_at)?;
        store.upsert_discovery_persisted_rebuild_state(&row)
    }
}
