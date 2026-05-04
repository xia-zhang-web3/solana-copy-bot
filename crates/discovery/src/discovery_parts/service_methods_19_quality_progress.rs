impl DiscoveryService {
    fn repair_collect_buy_mints_quality_progress_for_resume(
        &self,
        state: &mut PersistedStreamRebuildState,
        now: DateTime<Utc>,
    ) -> bool {
        let original_next_mint_index = state.payload.token_quality_progress.next_mint_index;
        let original_cache_len = state.payload.token_quality_cache.len();

        if state.payload.collect_buy_mints_mode != CollectBuyMintsMode::FreshScan {
            if original_next_mint_index == 0 && original_cache_len == 0 {
                return false;
            }
            if state.payload.collect_buy_mints_prepass_complete {
                let cache_changed = Self::trim_token_quality_cache_to_reusable_exact_buy_mints(
                    &mut state.payload,
                    now,
                );
                let progress_changed = Self::reset_token_quality_progress(&mut state.payload);
                if cache_changed || progress_changed {
                    info!(
                        rebuild_window_start = %state.window_start,
                        rebuild_horizon_end = %state.horizon_end,
                        rebuild_phase = state.phase.as_str(),
                        rebuild_collect_buy_mints_mode = state.payload.collect_buy_mints_mode.as_str(),
                        rebuild_quality_next_mint_index_before = original_next_mint_index,
                        rebuild_quality_next_mint_index_after =
                            state.payload.token_quality_progress.next_mint_index,
                        rebuild_quality_cached_mints_before = original_cache_len,
                        rebuild_quality_cached_mints_after = state.payload.token_quality_cache.len(),
                        "retained carried token-quality cache while clearing in-flight progress because exact carry-forward reconcile can safely reuse cached mint quality once the new target membership settles"
                    );
                }
                return cache_changed || progress_changed;
            }
            state.payload.token_quality_cache.clear();
            state.payload.token_quality_progress =
                quality_cache::TokenQualityResolutionProgress::default();
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_collect_buy_mints_mode = state.payload.collect_buy_mints_mode.as_str(),
                "cleared collect_buy_mints token-quality warmup progress because reconciled mint membership can move before the stored prefix"
            );
            return true;
        }

        let cursor_token = state.payload.collect_buy_mints_cursor_token.as_deref();
        state
            .payload
            .token_quality_cache
            .retain(|mint, resolution| {
                state.payload.unique_buy_mints.binary_search(mint).is_ok()
                    && cursor_token.is_none_or(|cursor_token| mint.as_str() <= cursor_token)
                    && Self::token_quality_resolution_is_reusable_for_resume(resolution, now)
            });
        let safe_prefix_len =
            Self::reusable_token_quality_cached_exact_buy_mint_prefix_len(&state.payload, now);
        if state.payload.token_quality_progress.next_mint_index > safe_prefix_len {
            state.payload.token_quality_progress.next_mint_index = safe_prefix_len;
            state.payload.token_quality_progress.rpc_attempted = 0;
            state.payload.token_quality_progress.rpc_spent_ms = 0;
        }

        let cache_changed = state.payload.token_quality_cache.len() != original_cache_len;
        let next_index_changed =
            state.payload.token_quality_progress.next_mint_index != original_next_mint_index;
        if cache_changed || next_index_changed {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_collect_buy_mints_cursor_token = cursor_token,
                rebuild_quality_next_mint_index_before = original_next_mint_index,
                rebuild_quality_next_mint_index_after =
                    state.payload.token_quality_progress.next_mint_index,
                rebuild_quality_cached_mints_before = original_cache_len,
                rebuild_quality_cached_mints_after = state.payload.token_quality_cache.len(),
                "repaired collect_buy_mints token-quality warmup progress onto the stored exact mint prefix before resume"
            );
        }
        cache_changed || next_index_changed
    }

    fn repair_token_quality_progress_for_resume(
        &self,
        state: &mut PersistedStreamRebuildState,
        now: DateTime<Utc>,
    ) -> bool {
        let original_next_mint_index = state.payload.token_quality_progress.next_mint_index;
        let original_cache_len = state.payload.token_quality_cache.len();
        let changed =
            if let Some(target_mints) = state.payload.publish_pending_quality_retry_mints.clone() {
                Self::align_token_quality_progress_to_reusable_cached_mint_prefix(
                    &mut state.payload,
                    &target_mints,
                    now,
                )
            } else {
                Self::align_token_quality_progress_to_reusable_cached_exact_buy_mint_prefix(
                    &mut state.payload,
                    now,
                )
            };
        if changed {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_quality_next_mint_index_before = original_next_mint_index,
                rebuild_quality_next_mint_index_after =
                    state.payload.token_quality_progress.next_mint_index,
                rebuild_quality_cached_mints_before = original_cache_len,
                rebuild_quality_cached_mints_after = state.payload.token_quality_cache.len(),
                "repaired persisted token-quality progress onto the reusable exact cached mint prefix before resume"
            );
        }
        changed
    }
}
