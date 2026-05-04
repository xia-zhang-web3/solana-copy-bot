impl DiscoveryService {
    fn load_or_start_persisted_stream_rebuild_state_with_options(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        resume_exact_target_surface_repair_deadline: Option<Instant>,
        helper_trace_context: Option<&DiscoveryPublicationTruthRepairTraceContext>,
        defer_resume_exact_target_surface_repair_to_runtime_cycle: bool,
    ) -> Result<(
        PersistedStreamRebuildState,
        PersistedStreamRebuildRestoreOutcome,
        ResumeExactTargetBuyMintSurfaceRepairDiagnostics,
    )> {
        let mut region = DiscoveryPublicationTruthRepairRegionScope::new(
            helper_trace_context,
            "load_or_start_persisted_stream_rebuild_state_with_options",
            Some("repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed_with_options"),
        );
        let Some(row) = store.load_discovery_persisted_rebuild_state()? else {
            return Ok(self.started_fresh_persisted_stream_rebuild_restore(
                window_start,
                metrics_window_start,
                now,
                &mut region,
                ResumeExactTargetBuyMintSurfaceRepairDiagnostics::default(),
            ));
        };
        match Self::persisted_stream_rebuild_state_from_row(row) {
            Ok(mut state) => {
                if let Some(reason) =
                    self.persisted_stream_rebuild_restart_reason(&state, window_start, now)
                {
                    warn!(
                        persisted_window_start = %state.window_start,
                        persisted_metrics_window_start = %state.metrics_window_start,
                        persisted_horizon_end = %state.horizon_end,
                        current_window_start = %window_start,
                        current_metrics_window_start = %metrics_window_start,
                        current_now = %now,
                        restart_reason = reason,
                        "discarding stale persisted discovery rebuild progress and restarting from a fresh frozen horizon"
                    );
                    store.clear_discovery_persisted_rebuild_state()?;
                    return Ok(self.started_fresh_persisted_stream_rebuild_restore(
                        window_start,
                        metrics_window_start,
                        now,
                        &mut region,
                        ResumeExactTargetBuyMintSurfaceRepairDiagnostics::default(),
                    ));
                }
                let exact_target_surface_repair =
                    self.repair_loaded_persisted_stream_rebuild_state_for_resume_with_options(
                        store,
                        &mut state,
                        now,
                        resume_exact_target_surface_repair_deadline,
                        helper_trace_context,
                        defer_resume_exact_target_surface_repair_to_runtime_cycle,
                    )?;
                self.finish_loaded_persisted_stream_rebuild_state_restore(
                    store,
                    state,
                    window_start,
                    metrics_window_start,
                    now,
                    exact_target_surface_repair,
                    &mut region,
                )
            }
            Err(error) => {
                warn!(
                    error = %error,
                    "failed restoring persisted discovery rebuild progress; restarting from a fresh frozen horizon"
                );
                store.clear_discovery_persisted_rebuild_state()?;
                Ok(self.started_fresh_persisted_stream_rebuild_restore(
                    window_start,
                    metrics_window_start,
                    now,
                    &mut region,
                    ResumeExactTargetBuyMintSurfaceRepairDiagnostics::default(),
                ))
            }
        }
    }

    fn started_fresh_persisted_stream_rebuild_restore(
        &self,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        region: &mut DiscoveryPublicationTruthRepairRegionScope<'_>,
        exact_target_surface_repair: ResumeExactTargetBuyMintSurfaceRepairDiagnostics,
    ) -> (
        PersistedStreamRebuildState,
        PersistedStreamRebuildRestoreOutcome,
        ResumeExactTargetBuyMintSurfaceRepairDiagnostics,
    ) {
        region.progress_mut().persisted_rebuild_restore_outcome =
            Some(PersistedStreamRebuildRestoreOutcome::StartedFresh.as_str());
        (
            self.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now),
            PersistedStreamRebuildRestoreOutcome::StartedFresh,
            exact_target_surface_repair,
        )
    }
}
