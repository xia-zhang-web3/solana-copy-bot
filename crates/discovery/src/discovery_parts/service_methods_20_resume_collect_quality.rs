use super::*;

impl DiscoveryService {
    pub(crate) fn repair_restored_collect_buy_mints_state_for_resume(
        &self,
        state: &mut PersistedStreamRebuildState,
        now: DateTime<Utc>,
    ) -> bool {
        let mut changed = false;
        if Self::payload_has_exact_buy_mint_membership(&state.payload) {
            Self::sync_unique_buy_mints_from_counts(&mut state.payload);
        }
        if state.payload.collect_buy_mints_cursor_token.is_some() {
            changed |= self.repair_collect_buy_mints_payload_for_cursor(state);
        }
        changed |= self.repair_collect_buy_mints_quality_progress_for_resume(state, now);
        if state.payload.collect_buy_mints_mode != CollectBuyMintsMode::FreshScan
            && (state
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor
                .is_some()
                || state
                    .payload
                    .collect_buy_mints_reconcile_new_tail_cursor
                    .is_some())
        {
            warn!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_collect_buy_mints_mode = state.payload.collect_buy_mints_mode.as_str(),
                "rewinding legacy raw-swap collect_buy_mints reconciliation cursor onto grouped buy-mint delta pagination before resume"
            );
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
            changed = true;
        }
        changed
    }

    pub(crate) fn repair_restored_token_quality_state_for_resume(
        &self,
        state: &mut PersistedStreamRebuildState,
        now: DateTime<Utc>,
    ) -> bool {
        let mut changed = false;
        if Self::payload_has_exact_buy_mint_membership(&state.payload) {
            Self::sync_unique_buy_mints_from_counts(&mut state.payload);
        }
        if Self::canonicalize_unique_buy_mints(&mut state.payload.unique_buy_mints) {
            warn!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                "rewinding persisted token-quality progress onto canonical sorted buy-mint order before resume"
            );
            changed = true;
        }
        if let Some(retry_mints) =
            state.payload.publish_pending_quality_retry_mints.as_mut()
        {
            let original_len = retry_mints.len();
            retry_mints.sort();
            retry_mints.dedup();
            retry_mints
                .retain(|mint| state.payload.unique_buy_mints.binary_search(mint).is_ok());
            if retry_mints.is_empty() {
                info!(
                    rebuild_window_start = %state.window_start,
                    rebuild_horizon_end = %state.horizon_end,
                    rebuild_phase = state.phase.as_str(),
                    "clearing stale exact publish-quality retry ownership from a token-quality checkpoint because its target mint set is now empty"
                );
                state.payload.publish_pending_quality_retry_mints = None;
                changed = true;
            } else if retry_mints.len() != original_len {
                info!(
                    rebuild_window_start = %state.window_start,
                    rebuild_horizon_end = %state.horizon_end,
                    rebuild_phase = state.phase.as_str(),
                    rebuild_quality_retry_mints = retry_mints.len(),
                    "repaired persisted exact publish-quality retry mint set onto canonical sorted exact buy-mint membership before resume"
                );
                changed = true;
            }
        }
        changed |= self.repair_token_quality_progress_for_resume(state, now);
        changed
    }
}
