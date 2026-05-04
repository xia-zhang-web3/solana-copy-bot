impl DiscoveryService {
    fn snapshot_run_cycle_publication_boundary_diagnostics(
        &self,
        store: &SqliteStore,
        prepared_cycle_state: &'static str,
        publish_due: bool,
        persist_publication_state_called: bool,
    ) -> Result<RunCyclePublicationBoundaryDiagnostics> {
        let persisted_rebuild_state = store
            .load_discovery_persisted_rebuild_state()?
            .map(Self::persisted_stream_rebuild_state_from_row)
            .transpose()?;
        let persisted_rebuild_checkpoint_exists = persisted_rebuild_state.is_some();
        let replay_incomplete = persisted_rebuild_state
            .as_ref()
            .is_some_and(|state| state.phase != DiscoveryPersistedRebuildPhase::PublishPending);
        let persisted_rebuild_phase = persisted_rebuild_state
            .as_ref()
            .map(|state| state.phase.as_str());
        let publishable_checkpoint_blocker = persisted_rebuild_state
            .as_ref()
            .map(Self::persisted_stream_publishable_checkpoint_blocker_from_state);
        Ok(RunCyclePublicationBoundaryDiagnostics {
            prepared_cycle_state,
            publish_due,
            persisted_rebuild_checkpoint_exists,
            replay_incomplete,
            persisted_rebuild_phase,
            publishable_checkpoint_blocker,
            persist_publication_state_called,
        })
    }

    fn log_run_cycle_publication_boundary(diagnostics: &RunCyclePublicationBoundaryDiagnostics) {
        info!(
            run_cycle_publication_boundary_reached = true,
            run_cycle_publication_prepared_cycle_state = diagnostics.prepared_cycle_state,
            run_cycle_publication_publish_due = diagnostics.publish_due,
            run_cycle_publication_persisted_rebuild_checkpoint_exists =
                diagnostics.persisted_rebuild_checkpoint_exists,
            run_cycle_publication_replay_incomplete = diagnostics.replay_incomplete,
            run_cycle_publication_persisted_rebuild_phase = diagnostics.persisted_rebuild_phase,
            run_cycle_publication_publishable_checkpoint_blocker =
                diagnostics.publishable_checkpoint_blocker,
            run_cycle_publication_persist_publication_state_called =
                diagnostics.persist_publication_state_called,
            "discovery run_cycle publication boundary"
        );
    }

    fn refresh_fail_closed_publication_runtime_surface_for_deferred_runtime_cycle(
        &self,
        store: &SqliteStore,
        publication_state: Option<&DiscoveryPublicationStateRow>,
        checkpoint_blocker: &'static str,
    ) -> Result<()> {
        let Some(publication_state) = publication_state else {
            return Ok(());
        };
        store.set_discovery_publication_state_with_options(
            &DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: format!("publication_truth_withheld_while_{checkpoint_blocker}"),
                last_published_at: None,
                last_published_window_start: None,
                published_scoring_source: publication_state.published_scoring_source.clone(),
                published_wallet_ids: None,
            },
            false,
            publication_state.publication_policy_fingerprint.as_deref(),
        )?;
        Ok(())
    }
}
