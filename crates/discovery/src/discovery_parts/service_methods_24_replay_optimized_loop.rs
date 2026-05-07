use crate::*;
use tracing::info;

use super::PersistedStreamReplayOptimizedProgress;

impl DiscoveryService {
    pub(crate) fn advance_persisted_stream_replay_optimized_with_wallet_stats_phase_page_limit(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        replay_wallet_stats_phase_page_limit_override: Option<usize>,
        replay_sol_leg_phase_page_limit_override: Option<usize>,
        allow_wallet_stats_candidate_activity_backfill_handoff: bool,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        let mut progress = PersistedStreamReplayOptimizedProgress::default();
        let mut cursor = state.phase_cursor.clone();
        let lookahead = Duration::seconds(self.config.rug_lookahead_seconds.max(1) as i64);

        let budget_exhausted_reason = loop {
            let phase_page_limit = if state.payload.replay_wallet_stats_complete {
                replay_sol_leg_phase_page_limit_override.unwrap_or(fetch_page_limit)
            } else {
                replay_wallet_stats_phase_page_limit_override.unwrap_or_else(|| {
                    Self::replay_wallet_stats_catch_up_page_limit(fetch_limit, fetch_page_limit)
                })
            };
            let total_pages_processed = progress
                .replay_pages_processed
                .saturating_add(progress.replay_wallet_stats_pages_processed);
            if total_pages_processed >= phase_page_limit {
                break Some(PersistedStreamBudgetExhaustedReason::PageBudget);
            }
            if Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            if !state.payload.replay_wallet_stats_complete {
                state.phase_cursor = cursor.clone();
                let advance = self.advance_persisted_stream_replay_wallet_stats(
                    store,
                    state,
                    fetch_limit,
                    phase_page_limit.saturating_sub(total_pages_processed),
                    deadline,
                )?;
                progress.replay_wallet_stats_rows_processed = progress
                    .replay_wallet_stats_rows_processed
                    .saturating_add(advance.replay_wallet_stats_rows_processed);
                progress.replay_wallet_stats_pages_processed = progress
                    .replay_wallet_stats_pages_processed
                    .saturating_add(advance.replay_wallet_stats_pages_processed);
                progress
                    .replay_wallet_stats_day_count_source_progress
                    .merge(advance.replay_wallet_stats_day_count_source_progress);
                cursor = advance.phase_cursor.clone();
                if advance.source_exhausted {
                    state.payload.replay_wallet_stats_complete = true;
                    state.payload.replay_wallet_stats_milestone_reached = true;
                    state.payload.replay_wallet_stats_wallet_cursor = None;
                    state
                        .payload
                        .replay_sol_leg_last_partial_cycle_pages_processed = 0;
                    state
                        .payload
                        .replay_sol_leg_last_partial_cycle_rows_processed = 0;
                    state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms = 0;
                    state.payload.replay_sol_leg_budget_floor_pages = 0;
                    state.payload.replay_sol_leg_retained_contract_floor_pages = 0;
                    state.payload.replay_candidate_activity_backfill_required = false;
                    state.payload.replay_candidate_activity_backfill_pending = false;
                    state
                        .payload
                        .replay_candidate_activity_backfill_wallet_cursor = None;
                    state.phase_cursor = None;
                    cursor = None;
                    let replay_wallet_stats_day_count_source_progress_total = state
                        .payload
                        .replay_wallet_stats_day_count_source_progress
                        .merged_with(progress.replay_wallet_stats_day_count_source_progress);
                    for acc in state.payload.by_wallet.values_mut() {
                        acc.tx_per_minute.clear();
                    }
                    info!(
                        rebuild_window_start = %state.window_start,
                        rebuild_horizon_end = %state.horizon_end,
                        rebuild_wallets_buffered = state.payload.by_wallet.len(),
                        rebuild_replay_wallet_stats_rows_processed = state
                            .payload
                            .replay_wallet_stats_rows_processed
                            .saturating_add(progress.replay_wallet_stats_rows_processed),
                        rebuild_replay_wallet_stats_pages_processed = state
                            .payload
                            .replay_wallet_stats_pages_processed
                            .saturating_add(progress.replay_wallet_stats_pages_processed),
                        rebuild_replay_wallet_stats_fast_path_pages_processed =
                            replay_wallet_stats_day_count_source_progress_total
                                .fast_path_pages_processed,
                        rebuild_replay_wallet_stats_fallback_pages_processed =
                            replay_wallet_stats_day_count_source_progress_total
                                .fallback_pages_processed,
                        rebuild_replay_wallet_stats_fast_path_wallets_processed =
                            replay_wallet_stats_day_count_source_progress_total
                                .fast_path_wallets_processed,
                        rebuild_replay_wallet_stats_fallback_wallets_processed =
                            replay_wallet_stats_day_count_source_progress_total
                                .fallback_wallets_processed,
                        "completed bounded replay wallet-stats prepass; switching to SOL-leg replay"
                    );
                    continue;
                }
                if allow_wallet_stats_candidate_activity_backfill_handoff
                    && self.can_handoff_replay_wallet_stats_to_sol_leg_with_candidate_activity_backfill(
                        state,
                        &advance,
                        fetch_limit,
                        replay_wallet_stats_phase_page_limit_override,
                    )
                {
                    Self::transition_persisted_stream_from_wallet_stats_to_sol_leg_with_candidate_activity_backfill(state);
                    cursor = None;
                    continue;
                }
                return Ok(progress.phase_advance(
                    false,
                    cursor,
                    advance.budget_exhausted_reason,
                ));
            }

            if state.payload.replay_candidate_activity_backfill_pending {
                state.phase_cursor = cursor.clone();
                let advance = self
                    .advance_persisted_stream_replay_candidate_wallet_activity_backfill(
                        store,
                        state,
                        fetch_limit,
                        phase_page_limit.saturating_sub(total_pages_processed),
                        deadline,
                    )?;
                progress.replay_rows_processed = progress
                    .replay_rows_processed
                    .saturating_add(advance.rows_processed);
                progress.replay_pages_processed = progress
                    .replay_pages_processed
                    .saturating_add(advance.pages_processed);
                cursor = advance.phase_cursor.clone();
                if advance.source_exhausted {
                    state.payload.replay_candidate_activity_backfill_required = false;
                    state.payload.replay_candidate_activity_backfill_pending = false;
                    state
                        .payload
                        .replay_candidate_activity_backfill_wallet_cursor = None;
                    state
                        .payload
                        .replay_sol_leg_last_partial_cycle_pages_processed = 0;
                    state
                        .payload
                        .replay_sol_leg_last_partial_cycle_rows_processed = 0;
                    state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms = 0;
                    state.payload.replay_sol_leg_budget_floor_pages = 0;
                    state.payload.replay_sol_leg_retained_contract_floor_pages = 0;
                    state.phase_cursor = None;
                    return Ok(progress.phase_advance(true, None, None));
                }
                return Ok(progress.phase_advance(
                    false,
                    cursor,
                    advance.budget_exhausted_reason,
                ));
            }

            let sol_leg_page_started = Instant::now();
            let mut page_last_cursor = cursor.clone();
            let base_replay_rows_processed = state.replay_rows_processed;
            let exact_target_buy_mints = (state.payload.replay_candidate_activity_backfill_required
                && !state.payload.discovery_critical_target_buy_mints.is_empty())
            .then(|| state.payload.discovery_critical_target_buy_mints.clone());
            let page = if let Some(exact_target_buy_mints) = exact_target_buy_mints.as_ref() {
                store.for_each_observed_sol_leg_swap_in_window_after_cursor_for_target_buy_mints_with_budget(
                    state.window_start,
                    state.horizon_end,
                    cursor.as_ref(),
                    exact_target_buy_mints,
                    fetch_limit,
                    deadline,
                    |swap| {
                        self.observe_persisted_stream_replay_optimized_sol_leg_swap(
                            state,
                            swap,
                            &mut page_last_cursor,
                            base_replay_rows_processed,
                            &mut progress,
                            lookahead,
                        )
                    },
                )?
            } else {
                store.for_each_observed_sol_leg_swap_in_window_after_cursor_with_budget(
                    state.window_start,
                    state.horizon_end,
                    cursor.as_ref(),
                    fetch_limit,
                    deadline,
                    |swap| {
                        self.observe_persisted_stream_replay_optimized_sol_leg_swap(
                            state,
                            swap,
                            &mut page_last_cursor,
                            base_replay_rows_processed,
                            &mut progress,
                            lookahead,
                        )
                    },
                )?
            };
            progress.replay_sol_leg_elapsed_ms = progress
                .replay_sol_leg_elapsed_ms
                .saturating_add(sol_leg_page_started.elapsed().as_millis() as u64);
            progress.replay_sol_leg_access_path = Some(page.access_path);
            progress.replay_pages_processed = progress.replay_pages_processed.saturating_add(1);
            progress.replay_sol_leg_pages_processed =
                progress.replay_sol_leg_pages_processed.saturating_add(1);
            cursor = page_last_cursor;

            if page.time_budget_exhausted {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }
            if page.rows_seen < fetch_limit {
                if state.payload.replay_candidate_activity_backfill_required
                    && !state.payload.by_wallet.is_empty()
                {
                    Self::prepare_persisted_stream_replay_candidate_activity_backfill(state);
                    info!(
                        rebuild_window_start = %state.window_start,
                        rebuild_horizon_end = %state.horizon_end,
                        rebuild_wallets_buffered = state.payload.by_wallet.len(),
                        rebuild_replay_rows_processed = state.replay_rows_processed
                            .saturating_add(progress.replay_rows_processed),
                        rebuild_replay_pages_processed = state.replay_pages_processed
                            .saturating_add(progress.replay_pages_processed),
                        "completed bounded SOL-leg replay for the current candidate set; switching to exact all-swap activity backfill for candidate wallets before publish"
                    );
                    cursor = None;
                    continue;
                }
                state.payload.replay_candidate_activity_backfill_required = false;
                state.payload.replay_candidate_activity_backfill_pending = false;
                state
                    .payload
                    .replay_candidate_activity_backfill_wallet_cursor = None;
                state
                    .payload
                    .replay_sol_leg_last_partial_cycle_pages_processed = 0;
                state
                    .payload
                    .replay_sol_leg_last_partial_cycle_rows_processed = 0;
                state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms = 0;
                state.payload.replay_sol_leg_budget_floor_pages = 0;
                state.payload.replay_sol_leg_retained_contract_floor_pages = 0;
                return Ok(progress.phase_advance(true, None, None));
            }
        };

        Ok(progress.phase_advance(false, cursor, budget_exhausted_reason))
    }
}
