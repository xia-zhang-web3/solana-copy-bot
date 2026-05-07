use super::*;

impl DiscoveryService {
    pub(crate) fn persisted_stream_observed_swaps_loaded(state: &PersistedStreamRebuildState) -> usize {
        match state.payload.replay_mode {
            ReplayMode::LegacyCompleteReplay => state.replay_rows_processed,
            ReplayMode::WalletStatsThenSolLeg => state
                .payload
                .replay_wallet_stats_rows_processed
                .saturating_add(state.replay_rows_processed),
        }
    }

    pub(crate) fn canonicalize_unique_buy_mints(mints: &mut Vec<String>) -> bool {
        let original = mints.clone();
        mints.sort();
        mints.dedup();
        *mints != original
    }

    pub(crate) fn repair_collect_buy_mints_payload_for_cursor(
        &self,
        state: &mut PersistedStreamRebuildState,
    ) -> bool {
        let original_len = state.payload.unique_buy_mints.len();
        let changed = Self::canonicalize_unique_buy_mints(&mut state.payload.unique_buy_mints);
        if let Some(cursor_token) = state.payload.collect_buy_mints_cursor_token.as_deref() {
            state
                .payload
                .unique_buy_mints
                .retain(|mint| mint.as_str() <= cursor_token);
            state
                .payload
                .buy_mint_counts
                .retain(|mint, count| *count > 0 && mint.as_str() <= cursor_token);
        }
        let truncated = state.payload.unique_buy_mints.len() != original_len;
        if Self::payload_has_exact_buy_mint_membership(&state.payload) {
            Self::sync_unique_buy_mints_from_counts(&mut state.payload);
        }
        if changed || truncated {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_collect_buy_mints_cursor_token =
                    state.payload.collect_buy_mints_cursor_token.as_deref(),
                rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                rebuild_unique_buy_mints_dropped =
                    original_len.saturating_sub(state.payload.unique_buy_mints.len()),
                "repaired persisted collect_buy_mints checkpoint to canonical sorted prefix for the stored token cursor"
            );
        }
        changed || truncated
    }

}
