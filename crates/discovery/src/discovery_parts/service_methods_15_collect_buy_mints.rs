use super::*;

impl DiscoveryService {
    pub(crate) fn maybe_warm_collect_buy_mints_token_quality_prefix(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        deadline: Instant,
    ) -> Result<usize> {
        if state.payload.collect_buy_mints_mode != CollectBuyMintsMode::FreshScan
            || !Self::payload_has_exact_buy_mint_membership(&state.payload)
            || state.payload.token_quality_progress.next_mint_index
                >= state.payload.unique_buy_mints.len()
            || Instant::now() >= deadline
        {
            return Ok(0);
        }

        let quality_before = state.payload.token_quality_progress.next_mint_index;
        let outcome = self.resolve_token_quality_for_mints_chunk(
            store,
            &state.payload.unique_buy_mints,
            state.horizon_end,
            &mut state.payload.token_quality_cache,
            &mut state.payload.token_quality_progress,
            Self::collect_buy_mints_fresh_scan_batch_size(fetch_limit),
            deadline,
        )?;
        let quality_after = state.payload.token_quality_progress.next_mint_index;
        if quality_after > quality_before {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_collect_buy_mints_cursor_token =
                    state.payload.collect_buy_mints_cursor_token.as_deref(),
                rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
                rebuild_quality_next_mint_index_before = quality_before,
                rebuild_quality_next_mint_index_after = quality_after,
                rebuild_quality_rpc_attempted = state.payload.token_quality_progress.rpc_attempted,
                rebuild_quality_rpc_spent_ms = state.payload.token_quality_progress.rpc_spent_ms,
                "advanced token-quality warmup over the exact collect_buy_mints prefix while fresh-scan continues toward source exhaustion"
            );
        }
        Ok(outcome.processed_mints)
    }

    pub(crate) fn replay_wallet_stats_catch_up_page_limit(
        fetch_limit: usize,
        fetch_page_limit: usize,
    ) -> usize {
        let wallet_batch_size = Self::replay_wallet_stats_wallet_batch_size(fetch_limit).max(1);
        let baseline_page_limit = fetch_page_limit
            .max(1)
            .saturating_mul(REPLAY_WALLET_STATS_CATCH_UP_PAGE_LIMIT_MULTIPLIER);
        let pages_for_fetch_width = fetch_limit
            .max(1)
            .saturating_add(wallet_batch_size.saturating_sub(1))
            / wallet_batch_size;
        baseline_page_limit.max(pages_for_fetch_width.max(1))
    }

    pub(crate) fn replay_wallet_stats_repair_phase_page_limit(
        &self,
        fetch_limit: usize,
        fetch_page_limit: usize,
        repair_time_budget: StdDuration,
    ) -> usize {
        let baseline_page_limit =
            Self::replay_wallet_stats_catch_up_page_limit(fetch_limit, fetch_page_limit);
        let normal_fetch_budget_ms = self.config.fetch_time_budget_ms.max(1) as u128;
        let repair_budget_ms = repair_time_budget.as_millis().max(1);
        let budget_multiplier = repair_budget_ms.div_ceil(normal_fetch_budget_ms);
        baseline_page_limit
            .saturating_mul(budget_multiplier.min(usize::MAX as u128).max(1) as usize)
    }

    pub(crate) fn replay_wallet_stats_wallet_batch_size(fetch_limit: usize) -> usize {
        fetch_limit.max(1).min(REPLAY_WALLET_STATS_WALLET_BATCH_CAP)
    }
}
