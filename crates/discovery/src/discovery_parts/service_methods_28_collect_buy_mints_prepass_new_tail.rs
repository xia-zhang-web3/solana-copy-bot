use crate::discovery_service_methods_28_steps::CollectBuyMintsPrepassStep;
use crate::*;

impl DiscoveryService {
    pub(crate) fn advance_collect_buy_mints_reconcile_new_tail_prepass(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        deadline: Instant,
        rows_processed: &mut usize,
        pages_processed: &mut usize,
        unique_buy_mints_discovered: &mut usize,
    ) -> Result<CollectBuyMintsPrepassStep> {
        let Some(source_horizon_end) =
            state.payload.collect_buy_mints_reconcile_source_horizon_end
        else {
            state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
            return Ok(CollectBuyMintsPrepassStep::Continue);
        };
        if source_horizon_end >= state.horizon_end {
            Self::clear_collect_buy_mints_new_tail_reconcile(state);
            if state.payload.collect_buy_mints_prepass_complete {
                return Ok(CollectBuyMintsPrepassStep::Return(
                    Self::collect_buy_mints_prepass_source_exhausted_advance(
                        *rows_processed,
                        *pages_processed,
                        *unique_buy_mints_discovered,
                    ),
                ));
            }
            state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
            return Ok(CollectBuyMintsPrepassStep::Continue);
        }

        let reconcile_cursor_token = state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token
            .clone();
        let reconcile_batch_size = Self::stale_reconcile_token_batch_size(fetch_limit);
        let exact_count_batch_size = Self::stale_reconcile_exact_count_batch_size(fetch_limit);
        if state
            .payload
            .collect_buy_mints_reconcile_new_tail_pending_mints
            .is_empty()
        {
            let step = self.load_collect_buy_mints_new_tail_pending_slice(
                store,
                state,
                source_horizon_end,
                reconcile_cursor_token.as_deref(),
                reconcile_batch_size,
                deadline,
                *rows_processed,
                *pages_processed,
                *unique_buy_mints_discovered,
            )?;
            match step {
                CollectBuyMintsPrepassStep::Continue => {
                    if state
                        .payload
                        .collect_buy_mints_reconcile_new_tail_pending_mints
                        .is_empty()
                    {
                        return Ok(CollectBuyMintsPrepassStep::Continue);
                    }
                }
                terminal => return Ok(terminal),
            }
        }

        let active_pending_mints = state
            .payload
            .collect_buy_mints_reconcile_new_tail_pending_mints
            .iter()
            .take(exact_count_batch_size)
            .cloned()
            .collect::<Vec<_>>();

        if active_pending_mints.len() == 1 {
            return self.advance_collect_buy_mints_new_tail_single_exact_count(
                store,
                state,
                source_horizon_end,
                deadline,
                rows_processed,
                pages_processed,
                unique_buy_mints_discovered,
            );
        }

        let exact_batch = Self::advance_reconcile_new_tail_exact_batch(
            store,
            state,
            source_horizon_end,
            &active_pending_mints,
            reconcile_cursor_token.as_deref(),
            deadline,
        )?;
        *pages_processed = pages_processed.saturating_add(exact_batch.pages_processed);
        *rows_processed = rows_processed.saturating_add(exact_batch.rows_processed);
        *unique_buy_mints_discovered =
            unique_buy_mints_discovered.saturating_add(exact_batch.unique_buy_mints_discovered);
        if exact_batch.time_budget_exhausted {
            return Ok(CollectBuyMintsPrepassStep::Break(
                PersistedStreamBudgetExhaustedReason::TimeBudget,
            ));
        }
        Ok(CollectBuyMintsPrepassStep::Continue)
    }

    fn load_collect_buy_mints_new_tail_pending_slice(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        source_horizon_end: DateTime<Utc>,
        reconcile_cursor_token: Option<&str>,
        reconcile_batch_size: usize,
        deadline: Instant,
        rows_processed: usize,
        pages_processed: usize,
        unique_buy_mints_discovered: usize,
    ) -> Result<CollectBuyMintsPrepassStep> {
        let pending_slice_end_token = state
            .payload
            .collect_buy_mints_reconcile_new_tail_slice_end_token
            .clone();
        let candidate_page = store.load_observed_buy_mints_in_time_bounds_after_token_with_budget(
            source_horizon_end,
            false,
            state.horizon_end,
            true,
            reconcile_cursor_token,
            pending_slice_end_token.as_deref(),
            reconcile_batch_size,
            deadline,
        )?;
        let Some(_) = candidate_page.mints.last() else {
            return Self::handle_empty_collect_buy_mints_new_tail_page(
                state,
                pending_slice_end_token,
                candidate_page.time_budget_exhausted,
                rows_processed,
                pages_processed,
                unique_buy_mints_discovered,
            );
        };
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_pending_mints = candidate_page.mints;
        if candidate_page.time_budget_exhausted {
            return Ok(CollectBuyMintsPrepassStep::Break(
                PersistedStreamBudgetExhaustedReason::TimeBudget,
            ));
        }
        Ok(CollectBuyMintsPrepassStep::Continue)
    }

    fn handle_empty_collect_buy_mints_new_tail_page(
        state: &mut PersistedStreamRebuildState,
        pending_slice_end_token: Option<String>,
        time_budget_exhausted: bool,
        rows_processed: usize,
        pages_processed: usize,
        unique_buy_mints_discovered: usize,
    ) -> Result<CollectBuyMintsPrepassStep> {
        if time_budget_exhausted {
            return Ok(CollectBuyMintsPrepassStep::Break(
                PersistedStreamBudgetExhaustedReason::TimeBudget,
            ));
        }
        if pending_slice_end_token.is_some() {
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
            return Ok(CollectBuyMintsPrepassStep::Continue);
        }
        Self::clear_collect_buy_mints_new_tail_reconcile(state);
        if state.payload.collect_buy_mints_prepass_complete {
            return Ok(CollectBuyMintsPrepassStep::Return(
                Self::collect_buy_mints_prepass_source_exhausted_advance(
                    rows_processed,
                    pages_processed,
                    unique_buy_mints_discovered,
                ),
            ));
        }
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        info!(
            rebuild_window_start = %state.window_start,
            rebuild_horizon_end = %state.horizon_end,
            rebuild_collect_buy_mints_cursor_token =
                state.payload.collect_buy_mints_cursor_token.as_deref(),
            rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
            "completed new-tail reconciliation for carried-forward collect_buy_mints state; resuming canonical distinct mint scan from persisted cursor"
        );
        Ok(CollectBuyMintsPrepassStep::Continue)
    }

    fn advance_collect_buy_mints_new_tail_single_exact_count(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        source_horizon_end: DateTime<Utc>,
        deadline: Instant,
        rows_processed: &mut usize,
        pages_processed: &mut usize,
        unique_buy_mints_discovered: &mut usize,
    ) -> Result<CollectBuyMintsPrepassStep> {
        let exact_mint = state
            .payload
            .collect_buy_mints_reconcile_new_tail_pending_mints
            .first()
            .cloned()
            .expect("single pending mint");
        let exact_count = store.count_observed_buy_mint_occurrences_in_time_bounds_with_budget(
            source_horizon_end,
            false,
            state.horizon_end,
            true,
            &exact_mint,
            deadline,
        )?;
        *pages_processed = pages_processed.saturating_add(1);
        state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
        if exact_count.time_budget_exhausted {
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token = Some(exact_mint);
            return Ok(CollectBuyMintsPrepassStep::Break(
                PersistedStreamBudgetExhaustedReason::TimeBudget,
            ));
        }
        *rows_processed = rows_processed.saturating_add(1);
        if Self::add_buy_mint_occurrences(&mut state.payload, &exact_mint, exact_count.buy_count) {
            *unique_buy_mints_discovered = unique_buy_mints_discovered.saturating_add(1);
        }
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token = Some(exact_mint);
        if state
            .payload
            .collect_buy_mints_reconcile_new_tail_slice_end_token
            .as_deref()
            == state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .as_deref()
        {
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
        }
        Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
        Ok(CollectBuyMintsPrepassStep::Continue)
    }

    fn clear_collect_buy_mints_new_tail_reconcile(state: &mut PersistedStreamRebuildState) {
        state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token = None;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
        Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
        Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
        state
            .payload
            .collect_buy_mints_reconcile_source_window_start = None;
        state.payload.collect_buy_mints_reconcile_source_horizon_end = None;
    }

    fn collect_buy_mints_prepass_source_exhausted_advance(
        rows_processed: usize,
        pages_processed: usize,
        unique_buy_mints_discovered: usize,
    ) -> PersistedStreamPhaseAdvance {
        PersistedStreamPhaseAdvance {
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
            unique_buy_mints_discovered,
            budget_exhausted_reason: None,
        }
    }
}
