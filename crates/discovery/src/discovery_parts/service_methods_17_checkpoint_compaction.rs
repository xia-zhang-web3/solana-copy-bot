impl DiscoveryService {
    fn compact_wallet_activity_summary_for_frozen_exact_target_checkpoint(
        payload: &mut PersistedStreamRebuildPayload,
    ) {
        for acc in payload.by_wallet.values_mut() {
            acc.reset_activity_summary_for_exact_backfill();
        }
    }

    fn compact_streaming_token_state_for_frozen_exact_target_checkpoint(
        payload: &mut PersistedStreamRebuildPayload,
    ) {
        for state in payload.token_states.values_mut() {
            state.first_seen = None;
            state.wallets_seen.clear();
        }
        payload.token_states.retain(|_, state| {
            !state.sol_trades_5m.is_empty()
                || state.sol_volume_5m > 0.0
                || !state.sol_traders_5m.is_empty()
        });
    }

    fn compact_token_quality_cache_for_frozen_exact_target_checkpoint(
        payload: &mut PersistedStreamRebuildPayload,
        now: DateTime<Utc>,
    ) {
        let exact_target_mints: HashSet<String> = payload
            .discovery_critical_target_buy_mints
            .iter()
            .cloned()
            .collect();
        Self::trim_token_quality_cache_to_reusable_mints(payload, &exact_target_mints, now);
    }

    fn reset_bucket_sensitive_rebuild_state_for_rollover(state: &mut PersistedStreamRebuildState) {
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.phase_cursor = None;
        state.replay_rows_processed = 0;
        state.replay_pages_processed = 0;
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        Self::reset_replay_wallet_stats_progress(&mut state.payload);
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileExpiredHead;
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
        state.payload.publish_pending_quality_retry_mints = None;
    }
}
