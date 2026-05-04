impl DiscoveryService {
    fn can_handoff_replay_wallet_stats_to_sol_leg_with_candidate_activity_backfill(
        &self,
        state: &PersistedStreamRebuildState,
        advance: &PersistedStreamPhaseAdvance,
        fetch_limit: usize,
        replay_wallet_stats_phase_page_limit_override: Option<usize>,
    ) -> bool {
        if replay_wallet_stats_phase_page_limit_override.is_none()
            || self.config.min_buy_count == 0
            || state.payload.replay_wallet_stats_complete
            || advance.source_exhausted
            || state.payload.replay_wallet_stats_wallet_cursor.is_none()
            || advance.replay_wallet_stats_pages_processed == 0
        {
            return false;
        }
        let wallets_processed = advance
            .replay_wallet_stats_day_count_source_progress
            .total_wallets_processed();
        if wallets_processed == 0 {
            return false;
        }
        let wallet_batch_size = Self::replay_wallet_stats_wallet_batch_size(fetch_limit).max(1);
        wallets_processed
            >= advance
                .replay_wallet_stats_pages_processed
                .saturating_mul(wallet_batch_size)
    }

    fn observe_replay_wallet_activity_summary(
        payload: &mut PersistedStreamRebuildPayload,
        row: ObservedWalletActivityRow,
    ) {
        let entry = payload.by_wallet.entry(row.wallet_id).or_default();
        entry.first_seen = Some(
            entry
                .first_seen
                .map(|current| current.min(row.first_seen))
                .unwrap_or(row.first_seen),
        );
        entry.last_seen = Some(
            entry
                .last_seen
                .map(|current| current.max(row.last_seen))
                .unwrap_or(row.last_seen),
        );
        entry.trades = entry
            .trades
            .saturating_add(row.trades.min(u32::MAX as usize) as u32);
        entry.exact_active_day_count = Some(
            entry
                .exact_active_day_count
                .unwrap_or(0)
                .saturating_add(row.active_day_count),
        );
        entry.suspicious |= row.suspicious;
    }

    fn advance_persisted_stream_replay_candidate_wallet_activity_backfill(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        if state.payload.by_wallet.is_empty() {
            state.payload.replay_candidate_activity_backfill_pending = false;
            state
                .payload
                .replay_candidate_activity_backfill_wallet_cursor = None;
            state.phase_cursor = None;
            return Ok(PersistedStreamPhaseAdvance {
                rows_processed: 0,
                pages_processed: 0,
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

        let mut rows_processed = 0usize;
        let mut pages_processed = 0usize;
        let mut wallet_cursor = state
            .payload
            .replay_candidate_activity_backfill_wallet_cursor
            .clone();
        let exact_wallet_ids: Vec<String> = state.payload.by_wallet.keys().cloned().collect();
        let wallet_limit = Self::replay_wallet_stats_wallet_batch_size(fetch_limit).max(1);

        let budget_exhausted_reason = loop {
            if pages_processed >= fetch_page_limit {
                break Some(PersistedStreamBudgetExhaustedReason::PageBudget);
            }
            if Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            let page = store
                .observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
                    &exact_wallet_ids,
                    state.window_start,
                    state.horizon_end,
                    wallet_cursor.as_deref(),
                    wallet_limit,
                    self.config.max_tx_per_minute,
                    deadline,
                )?;
            pages_processed = pages_processed.saturating_add(1);
            rows_processed = rows_processed.saturating_add(page.rows_seen);
            for row in page.rows.iter().cloned() {
                Self::observe_replay_wallet_activity_summary(&mut state.payload, row);
            }
            wallet_cursor = page.rows.last().map(|row| row.wallet_id.clone());

            if page.time_budget_exhausted {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }
            if page.rows.len() < wallet_limit {
                state.payload.replay_candidate_activity_backfill_pending = false;
                state
                    .payload
                    .replay_candidate_activity_backfill_wallet_cursor = None;
                state.phase_cursor = None;
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

        state
            .payload
            .replay_candidate_activity_backfill_wallet_cursor = wallet_cursor;
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
}
