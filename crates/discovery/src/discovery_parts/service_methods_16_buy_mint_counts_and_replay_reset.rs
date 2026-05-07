use super::*;

impl DiscoveryService {
    pub(crate) fn add_buy_mint_occurrences(
        payload: &mut PersistedStreamRebuildPayload,
        mint: &str,
        count: usize,
    ) -> bool {
        if count == 0 {
            return false;
        }
        let entry = payload.buy_mint_counts.entry(mint.to_string()).or_insert(0);
        let was_zero = *entry == 0;
        *entry = entry.saturating_add(count.min(u32::MAX as usize) as u32);
        if was_zero {
            Self::insert_unique_buy_mint(payload, mint);
        }
        was_zero
    }

    pub(crate) fn insert_unique_buy_mint(payload: &mut PersistedStreamRebuildPayload, mint: &str) {
        match payload
            .unique_buy_mints
            .binary_search_by(|existing| existing.as_str().cmp(mint))
        {
            Ok(_) => {}
            Err(index) => payload.unique_buy_mints.insert(index, mint.to_string()),
        }
    }

    pub(crate) fn remove_unique_buy_mint(payload: &mut PersistedStreamRebuildPayload, mint: &str) {
        if let Ok(index) = payload
            .unique_buy_mints
            .binary_search_by(|existing| existing.as_str().cmp(mint))
        {
            payload.unique_buy_mints.remove(index);
        }
    }

    pub(crate) fn set_buy_mint_occurrences(
        payload: &mut PersistedStreamRebuildPayload,
        mint: &str,
        count: usize,
    ) -> bool {
        let was_missing = payload.buy_mint_counts.get(mint).copied().unwrap_or(0) == 0;
        if count == 0 {
            payload.buy_mint_counts.remove(mint);
            if !was_missing {
                Self::remove_unique_buy_mint(payload, mint);
            }
            return was_missing;
        }
        payload
            .buy_mint_counts
            .insert(mint.to_string(), count.min(u32::MAX as usize) as u32);
        if was_missing {
            Self::insert_unique_buy_mint(payload, mint);
        }
        was_missing
    }

    pub(crate) fn subtract_buy_mint_occurrences(
        payload: &mut PersistedStreamRebuildPayload,
        mint: &str,
        count: usize,
    ) {
        if count == 0 {
            return;
        }
        let count = count.min(u32::MAX as usize) as u32;
        let remove = match payload.buy_mint_counts.get_mut(mint) {
            Some(existing) if *existing <= count => {
                *existing = 0;
                true
            }
            Some(existing) => {
                *existing -= count;
                false
            }
            None => false,
        };
        if remove {
            payload.buy_mint_counts.remove(mint);
            Self::remove_unique_buy_mint(payload, mint);
        }
    }

    pub(crate) fn reset_replay_wallet_stats_progress(payload: &mut PersistedStreamRebuildPayload) {
        payload.replay_wallet_stats_complete = false;
        payload.replay_wallet_stats_rows_processed = 0;
        payload.replay_wallet_stats_pages_processed = 0;
        payload.replay_wallet_stats_wallet_cursor = None;
        payload.replay_wallet_stats_day_count_source_progress =
            ReplayWalletStatsDayCountSourceProgress::default();
        payload.replay_wallet_stats_budget_floor_wallets = 0;
        payload.replay_wallet_stats_last_partial_cycle_pages_processed = 0;
        payload.replay_wallet_stats_last_partial_cycle_wallets_processed = 0;
        payload.replay_wallet_stats_last_partial_cycle_elapsed_ms = 0;
        payload.replay_wallet_stats_milestone_reached = false;
        payload.replay_sol_leg_reentry_pending = false;
        payload.replay_sol_leg_last_partial_cycle_pages_processed = 0;
        payload.replay_sol_leg_last_partial_cycle_rows_processed = 0;
        payload.replay_sol_leg_last_partial_cycle_elapsed_ms = 0;
        payload.replay_sol_leg_budget_floor_pages = 0;
        payload.replay_sol_leg_retained_contract_floor_pages = 0;
        payload.replay_candidate_activity_backfill_required = false;
        payload.replay_candidate_activity_backfill_pending = false;
        payload.replay_candidate_activity_backfill_wallet_cursor = None;
        Self::clear_replay_exact_target_surface_resume_state(payload);
    }

    pub(crate) fn clear_replay_exact_target_surface_resume_state(payload: &mut PersistedStreamRebuildPayload) {
        payload.replay_exact_target_surface_wallet_cursor = None;
        payload.replay_exact_target_surface_pre_row_blocked = false;
        payload
            .replay_exact_target_surface_staged_wallet_ids
            .clear();
        payload.replay_exact_target_surface_staged_wallet_cursor_after = None;
    }

    pub(crate) fn prepare_publish_pending_exact_quality_retry(
        state: &mut PersistedStreamRebuildState,
        quality_retry_mints: Vec<String>,
    ) {
        state.phase = DiscoveryPersistedRebuildPhase::ResolveTokenQuality;
        state.phase_cursor = None;
        state.replay_rows_processed = 0;
        state.replay_pages_processed = 0;
        state.payload.replay_sol_leg_reentry_pending = true;
        state.payload.replay_candidate_activity_backfill_required = true;
        state.payload.replay_candidate_activity_backfill_pending = false;
        state
            .payload
            .replay_candidate_activity_backfill_wallet_cursor = None;
        Self::clear_replay_exact_target_surface_resume_state(&mut state.payload);
        state.payload.completed_snapshots.clear();
        state.payload.discovery_critical_target_buy_mints.clear();
        state.payload.publish_pending_requested_wallet_ids = None;
        state.payload.publish_pending_quality_retry_mints = Some(quality_retry_mints);
        state.payload.token_quality_cache.clear();
        state.payload.token_quality_progress =
            quality_cache::TokenQualityResolutionProgress::default();
        state.payload.by_wallet.clear();
        state.payload.token_states.clear();
        state.payload.token_recent_sol_trades.clear();
        state.payload.pending_rug_checks.clear();
        state.payload.token_pending_buy_starts.clear();
    }

    pub(crate) fn reset_replay_wallet_stats_progress_preserving_budget_hints(
        payload: &mut PersistedStreamRebuildPayload,
    ) {
        let replay_wallet_stats_budget_floor_wallets =
            payload.replay_wallet_stats_budget_floor_wallets;
        let replay_wallet_stats_last_partial_cycle_pages_processed =
            payload.replay_wallet_stats_last_partial_cycle_pages_processed;
        let replay_wallet_stats_last_partial_cycle_wallets_processed =
            payload.replay_wallet_stats_last_partial_cycle_wallets_processed;
        let replay_wallet_stats_last_partial_cycle_elapsed_ms =
            payload.replay_wallet_stats_last_partial_cycle_elapsed_ms;
        let replay_sol_leg_last_partial_cycle_pages_processed =
            payload.replay_sol_leg_last_partial_cycle_pages_processed;
        let replay_sol_leg_last_partial_cycle_rows_processed =
            payload.replay_sol_leg_last_partial_cycle_rows_processed;
        let replay_sol_leg_last_partial_cycle_elapsed_ms =
            payload.replay_sol_leg_last_partial_cycle_elapsed_ms;
        let replay_sol_leg_budget_floor_pages = payload.replay_sol_leg_budget_floor_pages;
        let replay_sol_leg_retained_contract_floor_pages =
            payload.replay_sol_leg_retained_contract_floor_pages;
        Self::reset_replay_wallet_stats_progress(payload);
        payload.replay_wallet_stats_budget_floor_wallets = replay_wallet_stats_budget_floor_wallets;
        payload.replay_wallet_stats_last_partial_cycle_pages_processed =
            replay_wallet_stats_last_partial_cycle_pages_processed;
        payload.replay_wallet_stats_last_partial_cycle_wallets_processed =
            replay_wallet_stats_last_partial_cycle_wallets_processed;
        payload.replay_wallet_stats_last_partial_cycle_elapsed_ms =
            replay_wallet_stats_last_partial_cycle_elapsed_ms;
        payload.replay_sol_leg_last_partial_cycle_pages_processed =
            replay_sol_leg_last_partial_cycle_pages_processed;
        payload.replay_sol_leg_last_partial_cycle_rows_processed =
            replay_sol_leg_last_partial_cycle_rows_processed;
        payload.replay_sol_leg_last_partial_cycle_elapsed_ms =
            replay_sol_leg_last_partial_cycle_elapsed_ms;
        payload.replay_sol_leg_budget_floor_pages = replay_sol_leg_budget_floor_pages;
        payload.replay_sol_leg_retained_contract_floor_pages =
            replay_sol_leg_retained_contract_floor_pages;
    }
}
