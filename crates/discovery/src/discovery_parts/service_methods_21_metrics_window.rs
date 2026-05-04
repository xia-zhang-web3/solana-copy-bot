impl DiscoveryService {
    fn finish_loaded_persisted_stream_rebuild_state_restore(
        &self,
        store: &SqliteStore,
        mut state: PersistedStreamRebuildState,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        exact_target_surface_repair: ResumeExactTargetBuyMintSurfaceRepairDiagnostics,
        region: &mut DiscoveryPublicationTruthRepairRegionScope<'_>,
    ) -> Result<(
        PersistedStreamRebuildState,
        PersistedStreamRebuildRestoreOutcome,
        ResumeExactTargetBuyMintSurfaceRepairDiagnostics,
    )> {
            if state.metrics_window_start != metrics_window_start {
                if self
                    .state_can_resume_stale_metrics_window_until_publish_checkpoint(&state, now)
                {
                    warn!(
                        persisted_window_start = %state.window_start,
                        persisted_metrics_window_start = %state.metrics_window_start,
                        persisted_horizon_end = %state.horizon_end,
                        current_window_start = %window_start,
                        current_metrics_window_start = %metrics_window_start,
                        current_now = %now,
                        rebuild_phase = state.phase.as_str(),
                        rebuild_replay_subphase =
                            Self::replay_subphase(
                                state.phase,
                                state.payload.replay_wallet_stats_complete,
                                state.payload.replay_candidate_activity_backfill_pending,
                            ),
                        restart_reason =
                            "metrics_window_start_changed_but_existing_replay_target_still_publishable_under_gate",
                        "resuming stale metrics-window replay/token-quality progress on its frozen target window until the next publishable checkpoint instead of rewinding back into collect_buy_mints"
                    );
                    region.progress_mut().persisted_rebuild_restore_outcome = Some(
                        PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
                            .as_str(),
                    );
                    region.progress_mut().rebuild_phase = Some(state.phase.as_str());
                    region.progress_mut().rebuild_replay_subphase = Self::replay_subphase(
                        state.phase,
                        state.payload.replay_wallet_stats_complete,
                        state.payload.replay_candidate_activity_backfill_pending,
                    );
                    return Ok((
                        state,
                        PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow,
                        exact_target_surface_repair,
                    ));
                }
                if Self::state_can_pin_stale_metrics_window_until_first_publishable_checkpoint(
                    &state,
                ) {
                    warn!(
                        persisted_window_start = %state.window_start,
                        persisted_metrics_window_start = %state.metrics_window_start,
                        persisted_horizon_end = %state.horizon_end,
                        current_window_start = %window_start,
                        current_metrics_window_start = %metrics_window_start,
                        current_now = %now,
                        rebuild_phase = state.phase.as_str(),
                        rebuild_replay_subphase =
                            Self::replay_subphase(
                                state.phase,
                                state.payload.replay_wallet_stats_complete,
                                state.payload.replay_candidate_activity_backfill_pending,
                            ),
                        restart_reason =
                            "metrics_window_start_changed_downstream_replay_target_pinned_until_first_publishable_checkpoint",
                        "resuming stale downstream replay/token-quality progress on its frozen target window until the first exact publishable checkpoint instead of rebasing the same carried lineage onto a newer moving target"
                    );
                    region.progress_mut().persisted_rebuild_restore_outcome = Some(
                        PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
                            .as_str(),
                    );
                    region.progress_mut().rebuild_phase = Some(state.phase.as_str());
                    region.progress_mut().rebuild_replay_subphase = Self::replay_subphase(
                        state.phase,
                        state.payload.replay_wallet_stats_complete,
                        state.payload.replay_candidate_activity_backfill_pending,
                    );
                    return Ok((
                        state,
                        PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow,
                        exact_target_surface_repair,
                    ));
                }
                if matches!(
                    state.phase,
                    DiscoveryPersistedRebuildPhase::ResolveTokenQuality
                        | DiscoveryPersistedRebuildPhase::Replay
                ) && Self::state_can_carry_forward_metrics_rollover(&state)
                {
                    warn!(
                        persisted_window_start = %state.window_start,
                        persisted_metrics_window_start = %state.metrics_window_start,
                        persisted_horizon_end = %state.horizon_end,
                        current_window_start = %window_start,
                        current_metrics_window_start = %metrics_window_start,
                        current_now = %now,
                        rebuild_phase = state.phase.as_str(),
                        rebuild_replay_subphase =
                            Self::replay_subphase(
                                state.phase,
                                state.payload.replay_wallet_stats_complete,
                                state.payload.replay_candidate_activity_backfill_pending,
                            ),
                        restart_reason =
                            "metrics_window_start_changed_replay_or_quality_target_aged_out_but_exact_buy_mint_membership_can_carry_forward",
                        "stale metrics-window replay/token-quality progress aged out of the publish gate; carrying forward the exact canonical buy-mint membership onto the current target window instead of restarting from collect_buy_mints fresh-scan"
                    );
                }
                if self.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                    &mut state,
                    window_start,
                    metrics_window_start,
                    now,
                )? {
                    region.progress_mut().persisted_rebuild_restore_outcome = Some(
                        PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow
                            .as_str(),
                    );
                    region.progress_mut().rebuild_phase = Some(state.phase.as_str());
                    region.progress_mut().rebuild_replay_subphase = Self::replay_subphase(
                        state.phase,
                        state.payload.replay_wallet_stats_complete,
                        state.payload.replay_candidate_activity_backfill_pending,
                    );
                    return Ok((
                        state,
                        PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow,
                        exact_target_surface_repair,
                    ));
                }
                if Self::state_can_resume_stale_metrics_window_until_exact_checkpoint(&state) {
                    warn!(
                        persisted_window_start = %state.window_start,
                        persisted_metrics_window_start = %state.metrics_window_start,
                        persisted_horizon_end = %state.horizon_end,
                        current_window_start = %window_start,
                        current_metrics_window_start = %metrics_window_start,
                        current_now = %now,
                        rebuild_phase = state.phase.as_str(),
                        rebuild_collect_buy_mints_mode =
                            state.payload.collect_buy_mints_mode.as_str(),
                        restart_reason =
                            "metrics_window_start_changed_awaiting_exact_carry_forward_checkpoint",
                        "resuming stale in-progress collect_buy_mints reconciliation on its frozen target window until the next exact carry-forward checkpoint is available"
                    );
                    region.progress_mut().persisted_rebuild_restore_outcome = Some(
                        PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
                            .as_str(),
                    );
                    region.progress_mut().rebuild_phase = Some(state.phase.as_str());
                    region.progress_mut().rebuild_replay_subphase = Self::replay_subphase(
                        state.phase,
                        state.payload.replay_wallet_stats_complete,
                        state.payload.replay_candidate_activity_backfill_pending,
                    );
                    return Ok((
                        state,
                        PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow,
                        exact_target_surface_repair,
                    ));
                }
                warn!(
                    persisted_window_start = %state.window_start,
                    persisted_metrics_window_start = %state.metrics_window_start,
                    persisted_horizon_end = %state.horizon_end,
                    current_window_start = %window_start,
                    current_metrics_window_start = %metrics_window_start,
                    current_now = %now,
                    restart_reason = "metrics_window_start_changed_without_exact_buy_mint_membership",
                    "discarding persisted discovery rebuild progress because the metrics bucket moved before exact canonical buy-mint membership state was available"
                );
                store.clear_discovery_persisted_rebuild_state()?;
                region.progress_mut().persisted_rebuild_restore_outcome =
                    Some(PersistedStreamRebuildRestoreOutcome::StartedFresh.as_str());
                return Ok((
                    self.start_persisted_stream_rebuild_state(
                        window_start,
                        metrics_window_start,
                        now,
                    ),
                    PersistedStreamRebuildRestoreOutcome::StartedFresh,
                    exact_target_surface_repair,
                ));
            }
            region.progress_mut().persisted_rebuild_restore_outcome =
                Some(PersistedStreamRebuildRestoreOutcome::ResumedExisting.as_str());
            region.progress_mut().rebuild_phase = Some(state.phase.as_str());
            region.progress_mut().rebuild_replay_subphase = Self::replay_subphase(
                state.phase,
                state.payload.replay_wallet_stats_complete,
                state.payload.replay_candidate_activity_backfill_pending,
            );
            Ok((
                state,
                PersistedStreamRebuildRestoreOutcome::ResumedExisting,
                exact_target_surface_repair,
            ))
    }
}
