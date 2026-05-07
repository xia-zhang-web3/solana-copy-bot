use super::*;

impl DiscoveryService {
    pub(crate) fn log_persisted_stream_rebuild_restore_outcome(
        restore_outcome: PersistedStreamRebuildRestoreOutcome,
        state: &PersistedStreamRebuildState,
    ) {
        match restore_outcome {
            PersistedStreamRebuildRestoreOutcome::StartedFresh => {
                info!(
                    rebuild_window_start = %state.window_start,
                    rebuild_horizon_end = %state.horizon_end,
                    rebuild_metrics_window_start = %state.metrics_window_start,
                    "starting bounded discovery persisted observed_swaps rebuild"
                );
            }
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
            | PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow
            | PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow => {
                let message = if restore_outcome
                    == PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow
                {
                    "resuming carried-forward bounded discovery persisted observed_swaps rebuild after metrics bucket rollover"
                } else if restore_outcome
                    == PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
                {
                    if state.phase == DiscoveryPersistedRebuildPhase::CollectBuyMints {
                        "resuming stale metrics-window collect_buy_mints reconciliation until the next exact carry-forward checkpoint becomes available"
                    } else {
                        "resuming stale metrics-window replay/token-quality progress because the frozen target still remains publishable under the current freshness gate"
                    }
                } else {
                    "resuming bounded discovery persisted observed_swaps rebuild"
                };
                info!(
                    rebuild_phase = state.phase.as_str(),
                    rebuild_collect_buy_mints_mode = state.payload.collect_buy_mints_mode.as_str(),
                    rebuild_replay_mode = state.payload.replay_mode.as_str(),
                    rebuild_window_start = %state.window_start,
                    rebuild_horizon_end = %state.horizon_end,
                    rebuild_metrics_window_start = %state.metrics_window_start,
                    rebuild_phase_cursor_ts = ?state.phase_cursor.as_ref().map(|cursor| cursor.ts_utc),
                    rebuild_phase_cursor_slot = state.phase_cursor.as_ref().map(|cursor| cursor.slot),
                    rebuild_phase_cursor_signature =
                        state.phase_cursor.as_ref().map(|cursor| cursor.signature.as_str()),
                    rebuild_collect_buy_mints_cursor_token =
                        state.payload.collect_buy_mints_cursor_token.as_deref(),
                    rebuild_collect_buy_mints_reconcile_source_window_start = ?state
                        .payload
                        .collect_buy_mints_reconcile_source_window_start,
                    rebuild_collect_buy_mints_reconcile_source_horizon_end = ?state
                        .payload
                        .collect_buy_mints_reconcile_source_horizon_end,
                    rebuild_collect_buy_mints_reconcile_new_tail_slice_end_token = state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_slice_end_token
                        .as_deref(),
                    rebuild_prepass_rows_processed = state.prepass_rows_processed,
                    rebuild_replay_rows_processed = state.replay_rows_processed,
                    rebuild_replay_wallet_stats_complete =
                        state.payload.replay_wallet_stats_complete,
                    rebuild_replay_wallet_stats_rows_processed =
                        state.payload.replay_wallet_stats_rows_processed,
                    rebuild_replay_wallet_stats_pages_processed =
                        state.payload.replay_wallet_stats_pages_processed,
                    rebuild_replay_wallet_stats_milestone_reached =
                        state.payload.replay_wallet_stats_milestone_reached,
                    rebuild_replay_sol_leg_reentry_pending =
                        state.payload.replay_sol_leg_reentry_pending,
                    rebuild_replay_wallet_stats_fast_path_pages_processed = state
                        .payload
                        .replay_wallet_stats_day_count_source_progress
                        .fast_path_pages_processed,
                    rebuild_replay_wallet_stats_fallback_pages_processed = state
                        .payload
                        .replay_wallet_stats_day_count_source_progress
                        .fallback_pages_processed,
                    rebuild_replay_wallet_stats_fast_path_wallets_processed = state
                        .payload
                        .replay_wallet_stats_day_count_source_progress
                        .fast_path_wallets_processed,
                    rebuild_replay_wallet_stats_fallback_wallets_processed = state
                        .payload
                        .replay_wallet_stats_day_count_source_progress
                        .fallback_wallets_processed,
                    rebuild_quality_next_mint_index = state.payload.token_quality_progress.next_mint_index,
                    rebuild_quality_rpc_attempted = state.payload.token_quality_progress.rpc_attempted,
                    rebuild_quality_rpc_spent_ms = state.payload.token_quality_progress.rpc_spent_ms,
                    rebuild_chunks_completed = state.chunks_completed,
                    "{message}"
                );
            }
        }
    }
}
