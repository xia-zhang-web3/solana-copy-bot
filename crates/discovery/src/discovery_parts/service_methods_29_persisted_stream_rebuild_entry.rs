impl DiscoveryService {
    fn advance_persisted_stream_rebuild_with_phase_page_limits(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        fetch_limit: usize,
        fetch_page_limit: usize,
        rebuild_time_budget: StdDuration,
        collect_buy_mints_phase_page_limit_override: Option<usize>,
        replay_wallet_stats_phase_page_limit_override: Option<usize>,
        replay_sol_leg_phase_page_limit_override: Option<usize>,
    ) -> Result<PersistedStreamRebuildAdvanceOutcome> {
        let (mut state, restore_outcome) = self.load_or_start_persisted_stream_rebuild_state(
            store,
            window_start,
            metrics_window_start,
            now,
        )?;
        let recovery_inputs = self.persisted_stream_rebuild_recovery_inputs(
            &state,
            fetch_limit,
            fetch_page_limit,
            rebuild_time_budget,
            collect_buy_mints_phase_page_limit_override,
            replay_wallet_stats_phase_page_limit_override,
            replay_sol_leg_phase_page_limit_override,
        );
        let requested_contract = recovery_inputs.requested_contract;
        let adjusted_contract = self
            .deepen_persisted_stream_priority_recovery_contract_for_state_at(
                &state,
                fetch_limit,
                fetch_page_limit,
                requested_contract,
                Some(now),
            );
        let collect_buy_mints_phase_page_limit_override =
            adjusted_contract.collect_buy_mints_phase_page_limit_override;
        let replay_wallet_stats_phase_page_limit_override =
            adjusted_contract.replay_wallet_stats_phase_page_limit_override;
        let replay_sol_leg_phase_page_limit_override =
            adjusted_contract.replay_sol_leg_phase_page_limit_override;
        if adjusted_contract != requested_contract {
            self.log_adjusted_persisted_stream_rebuild_contract(
                &state,
                now,
                fetch_limit,
                adjusted_contract,
                &recovery_inputs,
            );
        }
        Self::log_persisted_stream_rebuild_restore_outcome(restore_outcome, &state);

        let mut cycle = PersistedStreamAdvanceCycleState::new(
            &state,
            adjusted_contract.time_budget,
            fetch_page_limit,
            replay_sol_leg_phase_page_limit_override,
        );
        if let Some(outcome) =
            self.complete_or_reenter_persisted_stream_publish_pending_checkpoint(
                &mut state,
                now,
                &cycle,
            )
        {
            return Ok(outcome);
        }
        match self.advance_persisted_stream_rebuild_cycle_loop(
            store,
            &mut state,
            window_start,
            metrics_window_start,
            now,
            fetch_limit,
            fetch_page_limit,
            collect_buy_mints_phase_page_limit_override,
            replay_wallet_stats_phase_page_limit_override,
            replay_sol_leg_phase_page_limit_override,
            &mut cycle,
        )? {
            PersistedStreamRebuildCycleLoopOutcome::Completed { outcome } => Ok(outcome),
            PersistedStreamRebuildCycleLoopOutcome::InProgress {
                budget_exhausted_reason,
            } => self.yield_persisted_stream_rebuild_cycle(
                store,
                &mut state,
                now,
                fetch_limit,
                &cycle,
                budget_exhausted_reason,
            ),
        }
    }
}
