impl DiscoveryService {
    fn advance_persisted_stream_token_quality(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        let target_mints = state
            .payload
            .publish_pending_quality_retry_mints
            .clone()
            .unwrap_or_else(|| state.payload.unique_buy_mints.clone());
        let mut rows_processed = 0usize;
        let mut pages_processed = 0usize;
        let budget_exhausted_reason = loop {
            if state.payload.token_quality_progress.next_mint_index >= target_mints.len() {
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
            if pages_processed >= fetch_page_limit {
                break Some(PersistedStreamBudgetExhaustedReason::PageBudget);
            }
            if Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            let outcome = self.resolve_token_quality_for_mints_chunk(
                store,
                &target_mints,
                state.horizon_end,
                &mut state.payload.token_quality_cache,
                &mut state.payload.token_quality_progress,
                fetch_limit,
                deadline,
            )?;
            rows_processed = rows_processed.saturating_add(outcome.processed_mints);
            if outcome.processed_mints > 0 {
                pages_processed = pages_processed.saturating_add(1);
            }
            if outcome.source_exhausted {
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
            if outcome.processed_mints == 0 {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
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
            phase_cursor: None,
            collect_buy_mints_cursor_token: None,
            unique_buy_mints_discovered: 0,
            budget_exhausted_reason,
        })
    }

    fn advance_persisted_stream_replay_wallet_stats(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        let mut rows_processed = 0usize;
        let mut pages_processed = 0usize;
        let wallet_batch_size = Self::replay_wallet_stats_wallet_batch_size(fetch_limit);
        let mut wallet_cursor = state.payload.replay_wallet_stats_wallet_cursor.clone();
        let mut replay_wallet_stats_day_count_source_progress =
            ReplayWalletStatsDayCountSourceProgress::default();

        let budget_exhausted_reason = loop {
            if pages_processed >= fetch_page_limit {
                break Some(PersistedStreamBudgetExhaustedReason::PageBudget);
            }
            if Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            let page = store.observed_wallet_activity_page_in_window_with_budget(
                state.window_start,
                state.horizon_end,
                wallet_cursor.as_deref(),
                wallet_batch_size,
                self.config.max_tx_per_minute,
                deadline,
            )?;
            pages_processed = pages_processed.saturating_add(1);

            if page.time_budget_exhausted || Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }
            rows_processed = rows_processed.saturating_add(page.rows_seen);
            let page_wallet_count = page.rows.len();
            replay_wallet_stats_day_count_source_progress
                .observe_page(page.active_day_count_source, page_wallet_count);
            let last_wallet_id = page.rows.last().map(|row| row.wallet_id.clone());
            for row in page.rows {
                Self::observe_replay_wallet_activity_summary(&mut state.payload, row);
            }
            wallet_cursor = last_wallet_id;

            if page_wallet_count < wallet_batch_size {
                state.payload.replay_wallet_stats_wallet_cursor = None;
                return Ok(PersistedStreamPhaseAdvance {
                    rows_processed: 0,
                    pages_processed: 0,
                    replay_wallet_stats_rows_processed: rows_processed,
                    replay_wallet_stats_pages_processed: pages_processed,
                    replay_sol_leg_rows_processed: 0,
                    replay_sol_leg_pages_processed: 0,
                    replay_sol_leg_elapsed_ms: 0,
                    replay_wallet_stats_day_count_source_progress,
                    replay_sol_leg_access_path: None,
                    source_exhausted: true,
                    phase_cursor: None,
                    collect_buy_mints_cursor_token: None,
                    unique_buy_mints_discovered: 0,
                    budget_exhausted_reason: None,
                });
            }
        };

        state.payload.replay_wallet_stats_wallet_cursor = wallet_cursor;

        Ok(PersistedStreamPhaseAdvance {
            rows_processed: 0,
            pages_processed: 0,
            replay_wallet_stats_rows_processed: rows_processed,
            replay_wallet_stats_pages_processed: pages_processed,
            replay_sol_leg_rows_processed: 0,
            replay_sol_leg_pages_processed: 0,
            replay_sol_leg_elapsed_ms: 0,
            replay_wallet_stats_day_count_source_progress,
            replay_sol_leg_access_path: None,
            source_exhausted: false,
            phase_cursor: None,
            collect_buy_mints_cursor_token: None,
            unique_buy_mints_discovered: 0,
            budget_exhausted_reason,
        })
    }
}
