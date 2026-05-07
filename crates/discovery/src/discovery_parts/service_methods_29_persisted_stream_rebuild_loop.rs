use super::*;

impl DiscoveryService {
    pub(crate) fn advance_persisted_stream_rebuild_cycle_loop(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        fetch_limit: usize,
        fetch_page_limit: usize,
        collect_buy_mints_phase_page_limit_override: Option<usize>,
        replay_wallet_stats_phase_page_limit_override: Option<usize>,
        replay_sol_leg_phase_page_limit_override: Option<usize>,
        cycle: &mut PersistedStreamAdvanceCycleState,
    ) -> Result<PersistedStreamRebuildCycleLoopOutcome> {
        let budget_exhausted_reason = loop {
            if self.prepare_persisted_stream_rebuild_cycle_metrics_window_rollover(
                state,
                window_start,
                metrics_window_start,
                now,
            )? {
                continue;
            }
            if Instant::now() >= cycle.deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            let active_phase = state.phase;
            let phase_advance = self.advance_persisted_stream_active_phase(
                store,
                state,
                active_phase,
                fetch_limit,
                fetch_page_limit,
                collect_buy_mints_phase_page_limit_override,
                replay_wallet_stats_phase_page_limit_override,
                replay_sol_leg_phase_page_limit_override,
                cycle.deadline,
            )?;
            cycle.absorb_phase_advance(&phase_advance);
            Self::apply_persisted_stream_phase_advance_to_state(state, &phase_advance);

            if phase_advance.source_exhausted {
                if let Some(outcome) = self.handle_persisted_stream_rebuild_source_exhausted(
                    store,
                    state,
                    active_phase,
                    now,
                    cycle,
                )? {
                    return Ok(PersistedStreamRebuildCycleLoopOutcome::Completed { outcome });
                }
                continue;
            }

            if phase_advance.budget_exhausted_reason.is_some() {
                break phase_advance.budget_exhausted_reason;
            }
        };
        Ok(PersistedStreamRebuildCycleLoopOutcome::InProgress {
            budget_exhausted_reason,
        })
    }

    pub(crate) fn prepare_persisted_stream_rebuild_cycle_metrics_window_rollover(
        &self,
        state: &mut PersistedStreamRebuildState,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        if state.metrics_window_start != metrics_window_start
            && self.state_can_resume_stale_metrics_window_until_publish_checkpoint(state, now)
        {
            // Keep advancing the still-publishable frozen replay/token-quality target until
            // it either reaches a publishable checkpoint or truly ages out of the gate.
            Ok(false)
        } else if state.metrics_window_start != metrics_window_start
            && Self::state_can_pin_stale_metrics_window_until_first_publishable_checkpoint(state)
        {
            // Once a carried replay lineage has already crossed wallet_stats and owns exact
            // buy-mint membership, rebasing it onto a newer moving target would discard the
            // exact downstream replay frontier and reopen replay_sol_leg_incomplete. Keep the
            // frozen target alive until it reaches its first exact publishable checkpoint.
            Ok(false)
        } else if state.metrics_window_start != metrics_window_start
            && self.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                state,
                window_start,
                metrics_window_start,
                now,
            )?
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
