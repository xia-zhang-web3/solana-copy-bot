impl DiscoveryService {
    fn token_quality_resolution_is_reusable_for_resume(
        resolution: &quality_cache::TokenQualityResolution,
        now: DateTime<Utc>,
    ) -> bool {
        match resolution {
            quality_cache::TokenQualityResolution::Fresh(row) => {
                row.fetched_at + Duration::seconds(QUALITY_CACHE_TTL_SECONDS) >= now
            }
            quality_cache::TokenQualityResolution::Stale(_)
            | quality_cache::TokenQualityResolution::Deferred
            | quality_cache::TokenQualityResolution::Missing => false,
        }
    }

    fn trim_token_quality_cache_to_reusable_mints(
        payload: &mut PersistedStreamRebuildPayload,
        target_mints: &HashSet<String>,
        now: DateTime<Utc>,
    ) -> bool {
        let original_cache_len = payload.token_quality_cache.len();
        payload.token_quality_cache.retain(|mint, resolution| {
            target_mints.contains(mint)
                && Self::token_quality_resolution_is_reusable_for_resume(resolution, now)
        });
        payload.token_quality_cache.len() != original_cache_len
    }

    fn trim_token_quality_cache_to_reusable_exact_buy_mints(
        payload: &mut PersistedStreamRebuildPayload,
        now: DateTime<Utc>,
    ) -> bool {
        let exact_buy_mints: HashSet<String> =
            if Self::payload_has_exact_buy_mint_membership(payload) {
                payload.buy_mint_counts.keys().cloned().collect()
            } else {
                payload.unique_buy_mints.iter().cloned().collect()
            };
        Self::trim_token_quality_cache_to_reusable_mints(payload, &exact_buy_mints, now)
    }

    fn reset_token_quality_progress(payload: &mut PersistedStreamRebuildPayload) -> bool {
        let changed = payload.token_quality_progress.next_mint_index != 0
            || payload.token_quality_progress.rpc_attempted != 0
            || payload.token_quality_progress.rpc_spent_ms != 0;
        if changed {
            payload.token_quality_progress =
                quality_cache::TokenQualityResolutionProgress::default();
        }
        changed
    }

    fn reusable_token_quality_cached_mint_prefix_len(
        mints: &[String],
        payload: &PersistedStreamRebuildPayload,
        now: DateTime<Utc>,
    ) -> usize {
        mints
            .iter()
            .take_while(|mint| {
                payload
                    .token_quality_cache
                    .get(*mint)
                    .is_some_and(|resolution| {
                        Self::token_quality_resolution_is_reusable_for_resume(resolution, now)
                    })
            })
            .count()
    }

    fn reusable_token_quality_cached_exact_buy_mint_prefix_len(
        payload: &PersistedStreamRebuildPayload,
        now: DateTime<Utc>,
    ) -> usize {
        Self::reusable_token_quality_cached_mint_prefix_len(&payload.unique_buy_mints, payload, now)
    }

    fn align_token_quality_progress_to_reusable_cached_mint_prefix(
        payload: &mut PersistedStreamRebuildPayload,
        target_mints: &[String],
        now: DateTime<Utc>,
    ) -> bool {
        let target_mints_set: HashSet<String> = target_mints.iter().cloned().collect();
        let cache_changed =
            Self::trim_token_quality_cache_to_reusable_mints(payload, &target_mints_set, now);
        let safe_prefix_len =
            Self::reusable_token_quality_cached_mint_prefix_len(target_mints, payload, now);
        let next_index_changed = payload.token_quality_progress.next_mint_index != safe_prefix_len;
        let rpc_changed = payload.token_quality_progress.rpc_attempted != 0
            || payload.token_quality_progress.rpc_spent_ms != 0;
        if next_index_changed || rpc_changed {
            payload.token_quality_progress.next_mint_index = safe_prefix_len;
            payload.token_quality_progress.rpc_attempted = 0;
            payload.token_quality_progress.rpc_spent_ms = 0;
        }
        cache_changed || next_index_changed || rpc_changed
    }

    fn align_token_quality_progress_to_reusable_cached_exact_buy_mint_prefix(
        payload: &mut PersistedStreamRebuildPayload,
        now: DateTime<Utc>,
    ) -> bool {
        Self::align_token_quality_progress_to_reusable_cached_mint_prefix(
            payload,
            &payload.unique_buy_mints.clone(),
            now,
        )
    }

    fn transition_persisted_stream_from_collect_buy_mints_to_token_quality(
        state: &mut PersistedStreamRebuildState,
        now: DateTime<Utc>,
    ) {
        Self::align_token_quality_progress_to_reusable_cached_exact_buy_mint_prefix(
            &mut state.payload,
            now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::ResolveTokenQuality;
        state.phase_cursor = None;
        state.payload.collect_buy_mints_cursor_token = None;
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        state.payload.collect_buy_mints_prepass_complete = true;
        state
            .payload
            .collect_buy_mints_reconcile_source_window_start = None;
        state.payload.collect_buy_mints_reconcile_source_horizon_end = None;
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
        state.payload.publish_pending_quality_retry_mints = None;
        Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
        Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
    }

    fn transition_persisted_stream_from_token_quality_to_replay(
        state: &mut PersistedStreamRebuildState,
        allow_post_wallet_stats_reentry: bool,
    ) {
        let replay_sol_leg_reentry_pending = state.payload.replay_sol_leg_reentry_pending;
        let replay_wallet_stats_milestone_reached =
            state.payload.replay_wallet_stats_milestone_reached;
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.phase_cursor = None;
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.publish_pending_quality_retry_mints = None;
        Self::reset_replay_wallet_stats_progress_preserving_budget_hints(&mut state.payload);
        if allow_post_wallet_stats_reentry
            && (replay_sol_leg_reentry_pending || replay_wallet_stats_milestone_reached)
        {
            state.payload.replay_wallet_stats_complete = true;
            state.payload.replay_wallet_stats_milestone_reached = true;
            state.payload.replay_candidate_activity_backfill_required = true;
            state
                .payload
                .replay_candidate_activity_backfill_wallet_cursor = None;
        }
    }
}
