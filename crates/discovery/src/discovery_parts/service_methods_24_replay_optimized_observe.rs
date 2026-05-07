use crate::*;

use super::PersistedStreamReplayOptimizedProgress;

impl DiscoveryService {
    pub(crate) fn observe_persisted_stream_replay_optimized_sol_leg_swap(
        &self,
        state: &mut PersistedStreamRebuildState,
        swap: SwapEvent,
        page_last_cursor: &mut Option<DiscoveryRuntimeCursor>,
        base_replay_rows_processed: usize,
        progress: &mut PersistedStreamReplayOptimizedProgress,
        lookahead: Duration,
    ) -> Result<()> {
        *page_last_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: swap.ts_utc,
            slot: swap.slot,
            signature: swap.signature.clone(),
        });
        let buy_quality = self.update_token_quality_state_streaming(
            &mut state.payload.token_states,
            &mut state.payload.token_recent_sol_trades,
            &state.payload.token_quality_cache,
            &swap,
        );
        let buy_quality_requires_retry = is_sol_buy(&swap)
            && Self::token_quality_resolution_requires_publish_pending_retry(
                state.payload.token_quality_cache.get(&swap.token_out),
            );
        let entry = state
            .payload
            .by_wallet
            .entry(swap.wallet.clone())
            .or_default();
        if is_sol_buy(&swap) {
            entry.observe_buy_streaming(
                swap.token_out.as_str(),
                swap.amount_out,
                swap.amount_in,
                swap.ts_utc,
                buy_quality.unwrap_or(BuyTradability::Rejected),
                buy_quality_requires_retry,
            );
        } else if is_sol_sell(&swap) {
            entry.observe_sell(
                swap.token_in.as_str(),
                swap.amount_in,
                swap.amount_out,
                swap.ts_utc,
            );
        }

        let Some(token) = sol_leg_token(&swap) else {
            return Ok(());
        };
        self.finalize_streaming_rug_metrics_up_to(
            &mut state.payload.by_wallet,
            token,
            &mut state.payload.token_recent_sol_trades,
            &mut state.payload.pending_rug_checks,
            &mut state.payload.token_pending_buy_starts,
            swap.ts_utc,
            lookahead,
            state.horizon_end,
        );
        if is_sol_buy(&swap) {
            state
                .payload
                .pending_rug_checks
                .push_back(PendingBuyRugCheck {
                    token: token.to_string(),
                    wallet_id: swap.wallet.clone(),
                    buy_ts: swap.ts_utc,
                });
            state
                .payload
                .token_pending_buy_starts
                .entry(token.to_string())
                .or_default()
                .push_back(swap.ts_utc);
        }
        let processed_total = base_replay_rows_processed
            .saturating_add(progress.replay_rows_processed)
            .saturating_add(1);
        if processed_total % STREAMING_RUG_TRADE_SWEEP_INTERVAL_SWAPS == 0 {
            self.evict_idle_streaming_rug_trade_history(
                &mut state.payload.token_recent_sol_trades,
                &state.payload.token_pending_buy_starts,
                swap.ts_utc - lookahead,
            );
        }
        progress.replay_rows_processed = progress.replay_rows_processed.saturating_add(1);
        progress.replay_sol_leg_rows_processed =
            progress.replay_sol_leg_rows_processed.saturating_add(1);
        Ok(())
    }
}
