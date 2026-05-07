use super::*;

impl DiscoveryService {
    pub(crate) fn build_wallet_snapshots_from_persisted_stream_one_shot(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(Vec<WalletSnapshot>, usize)> {
        let state = self.build_wallet_snapshots_from_persisted_stream_one_shot_state(
            store,
            window_start,
            now,
        )?;
        Ok((state.snapshots, state.observed_swaps_loaded))
    }

    pub(crate) fn build_wallet_snapshots_from_persisted_stream_one_shot_state(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<PersistedStreamSnapshotState> {
        let unique_buy_mints = store.load_observed_buy_mints_in_window(window_start, now)?;
        let token_quality_cache =
            self.resolve_token_quality_for_mints(store, &unique_buy_mints, now)?;
        let lookahead = Duration::seconds(self.config.rug_lookahead_seconds.max(1) as i64);
        let mut by_wallet: HashMap<String, WalletAccumulator> = HashMap::new();
        let mut token_states: HashMap<String, TokenRollingState> = HashMap::new();
        let mut token_recent_sol_trades: HashMap<String, VecDeque<SolLegTrade>> = HashMap::new();
        let mut pending_rug_checks = VecDeque::new();
        let mut token_pending_buy_starts: HashMap<String, VecDeque<DateTime<Utc>>> = HashMap::new();
        let mut processed_swaps = 0usize;
        let observed_swaps_loaded = store.for_each_observed_swap_in_window_paged(
            window_start,
            now,
            OBSERVED_SWAP_WINDOW_PAGED_READ_LIMIT,
            |swap| {
                processed_swaps = processed_swaps.saturating_add(1);
                let buy_quality = self.update_token_quality_state_streaming(
                    &mut token_states,
                    &mut token_recent_sol_trades,
                    &token_quality_cache,
                    &swap,
                );
                let buy_quality_requires_retry = is_sol_buy(&swap)
                    && Self::token_quality_resolution_requires_publish_pending_retry(
                        token_quality_cache.get(&swap.token_out),
                    );
                let entry = by_wallet.entry(swap.wallet.clone()).or_default();
                entry.observe_swap_streaming(
                    &swap,
                    self.config.max_tx_per_minute,
                    buy_quality,
                    buy_quality_requires_retry,
                );

                let Some(token) = sol_leg_token(&swap) else {
                    return Ok(());
                };
                self.finalize_streaming_rug_metrics_up_to(
                    &mut by_wallet,
                    token,
                    &mut token_recent_sol_trades,
                    &mut pending_rug_checks,
                    &mut token_pending_buy_starts,
                    swap.ts_utc,
                    lookahead,
                    now,
                );
                if is_sol_buy(&swap) {
                    pending_rug_checks.push_back(PendingBuyRugCheck {
                        token: token.to_string(),
                        wallet_id: swap.wallet.clone(),
                        buy_ts: swap.ts_utc,
                    });
                    token_pending_buy_starts
                        .entry(token.to_string())
                        .or_default()
                        .push_back(swap.ts_utc);
                }
                if processed_swaps % STREAMING_RUG_TRADE_SWEEP_INTERVAL_SWAPS == 0 {
                    self.evict_idle_streaming_rug_trade_history(
                        &mut token_recent_sol_trades,
                        &token_pending_buy_starts,
                        swap.ts_utc - lookahead,
                    );
                }
                Ok(())
            },
        )?;
        self.finalize_all_streaming_rug_metrics(
            &mut by_wallet,
            &mut token_recent_sol_trades,
            &mut pending_rug_checks,
            &mut token_pending_buy_starts,
            now,
            lookahead,
        );
        let empty_token_sol_history = HashMap::new();
        let outcomes = self.wallet_snapshot_outcomes_from_accumulators(
            store,
            by_wallet.clone(),
            now,
            &empty_token_sol_history,
        )?;
        let snapshots = outcomes
            .iter()
            .map(|outcome| outcome.snapshot.clone())
            .collect();
        Ok(PersistedStreamSnapshotState {
            snapshots,
            observed_swaps_loaded,
        })
    }

}
