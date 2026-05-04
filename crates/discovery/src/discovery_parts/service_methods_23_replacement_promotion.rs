impl DiscoveryService {
    fn advance_persisted_stream_replay_legacy(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        let mut rows_processed = 0usize;
        let mut pages_processed = 0usize;
        let mut cursor = state.phase_cursor.clone();
        let lookahead = Duration::seconds(self.config.rug_lookahead_seconds.max(1) as i64);

        let budget_exhausted_reason = loop {
            if pages_processed >= fetch_page_limit {
                break Some(PersistedStreamBudgetExhaustedReason::PageBudget);
            }
            if Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            let mut page_last_cursor = cursor.clone();
            let base_replay_rows_processed = state.replay_rows_processed;
            let page = store.for_each_observed_swap_in_window_after_cursor_with_budget(
                state.window_start,
                state.horizon_end,
                cursor.as_ref(),
                fetch_limit,
                deadline,
                |swap| {
                    page_last_cursor = Some(DiscoveryRuntimeCursor {
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
                    entry.observe_swap_streaming(
                        &swap,
                        self.config.max_tx_per_minute,
                        buy_quality,
                        buy_quality_requires_retry,
                    );

                    let Some(token) = sol_leg_token(&swap) else {
                        rows_processed = rows_processed.saturating_add(1);
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
                        .saturating_add(rows_processed)
                        .saturating_add(1);
                    if processed_total % STREAMING_RUG_TRADE_SWEEP_INTERVAL_SWAPS == 0 {
                        self.evict_idle_streaming_rug_trade_history(
                            &mut state.payload.token_recent_sol_trades,
                            &state.payload.token_pending_buy_starts,
                            swap.ts_utc - lookahead,
                        );
                    }
                    rows_processed = rows_processed.saturating_add(1);
                    Ok(())
                },
            )?;
            pages_processed = pages_processed.saturating_add(1);
            cursor = page_last_cursor;

            if page.time_budget_exhausted {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }
            if page.rows_seen < fetch_limit {
                return Ok(PersistedStreamPhaseAdvance {
                    rows_processed,
                    pages_processed,
                    replay_wallet_stats_rows_processed: 0,
                    replay_wallet_stats_pages_processed: 0,
                    replay_sol_leg_rows_processed: 0,
                    replay_sol_leg_pages_processed: 0,
                    replay_sol_leg_elapsed_ms: 0,
                    replay_wallet_stats_day_count_source_progress:
                        ReplayWalletStatsDayCountSourceProgress::default(),
                    replay_sol_leg_access_path: None,
                    source_exhausted: true,
                    phase_cursor: None,
                    collect_buy_mints_cursor_token: None,
                    unique_buy_mints_discovered: 0,
                    budget_exhausted_reason: None,
                });
            }
        };

        Ok(PersistedStreamPhaseAdvance {
            rows_processed,
            pages_processed,
            replay_wallet_stats_rows_processed: 0,
            replay_wallet_stats_pages_processed: 0,
            replay_sol_leg_rows_processed: 0,
            replay_sol_leg_pages_processed: 0,
            replay_sol_leg_elapsed_ms: 0,
            replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress::default(),
            replay_sol_leg_access_path: None,
            source_exhausted: false,
            phase_cursor: cursor,
            collect_buy_mints_cursor_token: None,
            unique_buy_mints_discovered: 0,
            budget_exhausted_reason,
        })
    }

    #[cfg(test)]
    fn advance_persisted_stream_replay_optimized(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        self.advance_persisted_stream_replay_optimized_with_wallet_stats_phase_page_limit(
            store,
            state,
            fetch_limit,
            fetch_page_limit,
            None,
            None,
            false,
            deadline,
        )
    }
}
