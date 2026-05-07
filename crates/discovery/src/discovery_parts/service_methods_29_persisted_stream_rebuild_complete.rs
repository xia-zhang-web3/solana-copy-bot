use super::*;

impl DiscoveryService {
    pub(crate) fn complete_or_reenter_persisted_stream_publish_pending_checkpoint(
        &self,
        state: &mut PersistedStreamRebuildState,
        now: DateTime<Utc>,
        cycle: &PersistedStreamAdvanceCycleState,
    ) -> Option<PersistedStreamRebuildAdvanceOutcome> {
        if state.phase != DiscoveryPersistedRebuildPhase::PublishPending {
            return None;
        }
        let publish_pending_requested_wallet_count = self
            .publish_pending_requested_wallet_ids_from_state(state)
            .len();
        let publish_pending_quality_retry_mints =
            Self::publish_pending_quality_retry_mints_from_state(state);
        if publish_pending_requested_wallet_count == 0 && !publish_pending_quality_retry_mints.is_empty()
        {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_publish_pending_requested_wallet_count =
                    publish_pending_requested_wallet_count,
                rebuild_publish_pending_quality_retry_mint_count =
                    publish_pending_quality_retry_mints.len(),
                "re-entering exact token-quality resolution for the unresolved mint set that kept the completed rebuild's exact publish set empty"
            );
            Self::prepare_publish_pending_exact_quality_retry(
                state,
                publish_pending_quality_retry_mints,
            );
            return None;
        }
        let snapshots = Self::publish_pending_snapshots(state);
        let telemetry = self.persisted_stream_cycle_telemetry(
            state,
            cycle.telemetry_input(now, false, true, None),
        );
        self.log_persisted_stream_progress(
            &telemetry,
            "resuming bounded discovery persisted observed_swaps rebuild from publish-pending checkpoint",
        );
        Some(PersistedStreamRebuildAdvanceOutcome::Completed {
            snapshots,
            telemetry,
        })
    }

    pub(crate) fn complete_persisted_stream_rebuild_from_replay(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        now: DateTime<Utc>,
        cycle: &PersistedStreamAdvanceCycleState,
    ) -> Result<PersistedStreamRebuildAdvanceOutcome> {
        self.finalize_all_streaming_rug_metrics(
            &mut state.payload.by_wallet,
            &mut state.payload.token_recent_sol_trades,
            &mut state.payload.pending_rug_checks,
            &mut state.payload.token_pending_buy_starts,
            state.horizon_end,
            Duration::seconds(self.config.rug_lookahead_seconds.max(1) as i64),
        );
        let empty_token_sol_history = HashMap::new();
        let unresolved_publish_quality_mints =
            Self::unresolved_publish_quality_mints(&state.payload, state.horizon_end);
        state.payload.discovery_critical_target_buy_mints = self
            .discovery_critical_target_buy_mints_from_accumulators(
                store,
                &state.payload.by_wallet,
                state.horizon_end,
            )?;
        let by_wallet = std::mem::take(&mut state.payload.by_wallet);
        let snapshots = self.wallet_snapshots_from_accumulators(
            store,
            by_wallet.clone(),
            state.horizon_end,
            &empty_token_sol_history,
        )?;
        let publish_pending_requested_wallet_ids =
            self.publish_pending_requested_wallet_ids_from_snapshots(&snapshots);
        let publish_pending_quality_retry_mints = if publish_pending_requested_wallet_ids.is_empty() {
            self.replay_completion_publish_quality_retry_mints(
                store,
                &by_wallet,
                &unresolved_publish_quality_mints,
                &snapshots,
                state.horizon_end,
            )?
        } else {
            Vec::new()
        };
        state.phase = DiscoveryPersistedRebuildPhase::PublishPending;
        state.phase_cursor = None;
        state.payload.completed_snapshots = snapshots.clone();
        state.payload.publish_pending_requested_wallet_ids =
            Some(publish_pending_requested_wallet_ids.clone());
        state.payload.publish_pending_quality_retry_mints =
            (!publish_pending_quality_retry_mints.is_empty())
                .then_some(publish_pending_quality_retry_mints);
        state.payload.token_quality_cache.clear();
        state.payload.token_states.clear();
        state.payload.token_recent_sol_trades.clear();
        state.payload.pending_rug_checks.clear();
        state.payload.token_pending_buy_starts.clear();
        self.persist_persisted_stream_rebuild_state(store, state, now)?;
        let telemetry = self.persisted_stream_cycle_telemetry(
            state,
            cycle.telemetry_input(now, false, true, None),
        );
        self.log_persisted_stream_progress(
            &telemetry,
            "completed bounded discovery persisted observed_swaps rebuild",
        );
        Ok(PersistedStreamRebuildAdvanceOutcome::Completed {
            snapshots,
            telemetry,
        })
    }

    pub(crate) fn yield_persisted_stream_rebuild_cycle(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        now: DateTime<Utc>,
        fetch_limit: usize,
        cycle: &PersistedStreamAdvanceCycleState,
        budget_exhausted_reason: Option<PersistedStreamBudgetExhaustedReason>,
    ) -> Result<PersistedStreamRebuildAdvanceOutcome> {
        state.chunks_completed = state.chunks_completed.saturating_add(1);
        let cycle_elapsed_ms = cycle.elapsed_ms();
        Self::persist_partial_replay_frontier_hints_after_cycle(
            state,
            fetch_limit,
            cycle.started_in_replay_sol_leg,
            cycle.replay_wallet_stats_day_count_source_progress,
            cycle_elapsed_ms,
            budget_exhausted_reason,
            cycle.replay_sol_leg_phase_page_limit,
            cycle.replay_sol_leg_pages_processed,
            cycle.replay_sol_leg_rows_processed,
            cycle.replay_sol_leg_elapsed_ms,
        );
        self.persist_persisted_stream_rebuild_state(store, state, now)?;
        let telemetry = self.persisted_stream_cycle_telemetry(
            state,
            cycle.telemetry_input(now, true, false, budget_exhausted_reason),
        );
        self.log_persisted_stream_progress(
            &telemetry,
            "yielding bounded discovery persisted observed_swaps rebuild back to scheduler",
        );
        Ok(PersistedStreamRebuildAdvanceOutcome::InProgress { telemetry })
    }
}
