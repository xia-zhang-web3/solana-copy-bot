impl DiscoveryService {
    fn repair_loaded_persisted_stream_rebuild_state_for_resume_with_options(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        now: DateTime<Utc>,
        resume_exact_target_surface_repair_deadline: Option<Instant>,
        helper_trace_context: Option<&DiscoveryPublicationTruthRepairTraceContext>,
        defer_resume_exact_target_surface_repair_to_runtime_cycle: bool,
    ) -> Result<ResumeExactTargetBuyMintSurfaceRepairDiagnostics> {
        let repaired_for_resume = self.repair_restored_persisted_stream_state_for_resume(
            state,
            now,
            helper_trace_context,
        );
        let exact_target_surface_repair =
            if defer_resume_exact_target_surface_repair_to_runtime_cycle {
                ResumeExactTargetBuyMintSurfaceRepairDiagnostics::default()
            } else {
                let exact_target_surface_repair_deadline =
                    resume_exact_target_surface_repair_deadline
                        .unwrap_or_else(|| Instant::now() + StdDuration::from_secs(300));
                self.repair_replay_exact_target_buy_mint_surface_for_resume(
                    store,
                    state,
                    exact_target_surface_repair_deadline,
                    helper_trace_context,
                )?
            };
        if exact_target_surface_repair.persisted_partial_progress {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_replay_subphase =
                    Self::replay_subphase(
                        state.phase,
                        state.payload.replay_wallet_stats_complete,
                        state.payload.replay_candidate_activity_backfill_pending,
                    ),
                rebuild_resume_exact_target_surface_wallet_pages =
                    exact_target_surface_repair.wallet_pages,
                rebuild_resume_exact_target_surface_wallet_rows =
                    exact_target_surface_repair.wallet_rows,
                rebuild_resume_exact_target_surface_wallet_cursor_before =
                    exact_target_surface_repair.wallet_cursor_before.as_deref(),
                rebuild_resume_exact_target_surface_wallet_cursor_after =
                    exact_target_surface_repair.wallet_cursor_after.as_deref(),
                "persisting resumable exact target-mint surface repair progress onto the replay checkpoint after bounded deadline exhaustion"
            );
        }
        if exact_target_surface_repair.persisted_staged_pre_row_state {
            warn!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_replay_subphase =
                    Self::replay_subphase(
                        state.phase,
                        state.payload.replay_wallet_stats_complete,
                        state.payload.replay_candidate_activity_backfill_pending,
                    ),
                rebuild_resume_exact_target_surface_wallet_pages =
                    exact_target_surface_repair.wallet_pages,
                rebuild_resume_exact_target_surface_wallet_rows =
                    exact_target_surface_repair.wallet_rows,
                rebuild_resume_exact_target_surface_wallet_id_page_wallets_seen =
                    exact_target_surface_repair.wallet_id_page_wallets_seen,
                rebuild_resume_exact_target_surface_wallet_cursor_before =
                    exact_target_surface_repair.wallet_cursor_before.as_deref(),
                rebuild_resume_exact_target_surface_wallet_cursor_after =
                    exact_target_surface_repair.wallet_cursor_after.as_deref(),
                "persisting staged resumable pre-row exact target-surface repair state after bounded deadline exhaustion"
            );
        }
        if exact_target_surface_repair.resumed_from_staged_pre_row_state
            && !exact_target_surface_repair.persisted_staged_pre_row_state
        {
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_replay_subphase =
                    Self::replay_subphase(
                        state.phase,
                        state.payload.replay_wallet_stats_complete,
                        state.payload.replay_candidate_activity_backfill_pending,
                    ),
                rebuild_resume_exact_target_surface_wallet_pages =
                    exact_target_surface_repair.wallet_pages,
                rebuild_resume_exact_target_surface_wallet_rows =
                    exact_target_surface_repair.wallet_rows,
                "persisting replay checkpoint after staged pre-row exact target-surface state was consumed so the next cycle does not restart from the same staged page"
            );
        }
        if exact_target_surface_repair.persisted_blocked_state {
            warn!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_phase = state.phase.as_str(),
                rebuild_replay_subphase =
                    Self::replay_subphase(
                        state.phase,
                        state.payload.replay_wallet_stats_complete,
                        state.payload.replay_candidate_activity_backfill_pending,
                    ),
                rebuild_resume_exact_target_surface_wallet_pages =
                    exact_target_surface_repair.wallet_pages,
                rebuild_resume_exact_target_surface_wallet_rows =
                    exact_target_surface_repair.wallet_rows,
                rebuild_resume_exact_target_surface_wallet_id_page_wallets_seen =
                    exact_target_surface_repair.wallet_id_page_wallets_seen,
                rebuild_resume_exact_target_surface_wallet_cursor_before =
                    exact_target_surface_repair.wallet_cursor_before.as_deref(),
                rebuild_resume_exact_target_surface_wallet_cursor_after =
                    exact_target_surface_repair.wallet_cursor_after.as_deref(),
                "persisting explicit blocked replay state because bounded exact target-surface repair exhausted its deadline before the first materialized wallet-activity row"
            );
        }
        if repaired_for_resume
            || exact_target_surface_repair.completed
            || exact_target_surface_repair.persisted_partial_progress
            || exact_target_surface_repair.persisted_staged_pre_row_state
            || exact_target_surface_repair.resumed_from_staged_pre_row_state
            || exact_target_surface_repair.persisted_blocked_state
        {
            self.persist_persisted_stream_rebuild_state(store, state, now)?;
        }
        Ok(exact_target_surface_repair)
    }
}
