use super::*;

impl DiscoveryService {
    pub(crate) fn invalidate_incompatible_recent_publication_truth_if_needed(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let Some((publication_state, stored_policy_fingerprint)) =
            store.discovery_publication_state_with_policy_read_only()?
        else {
            return Ok(false);
        };
        if publication_state.runtime_mode == DiscoveryRuntimeMode::FailClosed {
            return Ok(false);
        }
        if Self::runtime_publication_truth_from_state(publication_state.clone()).is_none() {
            return Ok(false);
        }
        if publication_state.published_scoring_source.as_deref()
            == Some("discovery_v2_operational_window")
        {
            return Ok(false);
        }

        let current_policy_fingerprint = self.publication_selection_policy_fingerprint();
        if stored_policy_fingerprint.as_deref() == Some(current_policy_fingerprint.as_str()) {
            return Ok(false);
        }

        let reason = "publication_truth_invalidated_selection_policy_mismatch";
        warn!(
            publication_truth_invalidated_selection_policy_mismatch = true,
            publication_previous_runtime_mode = publication_state.runtime_mode.as_str(),
            publication_previous_reason = publication_state.reason.as_str(),
            publication_previous_last_published_at = ?publication_state.last_published_at,
            publication_previous_published_wallet_count = publication_state
                .published_wallet_ids
                .as_ref()
                .map_or(0, |wallets| wallets.len()),
            publication_previous_policy_fingerprint =
                stored_policy_fingerprint.as_deref(),
            publication_current_policy_fingerprint =
                current_policy_fingerprint.as_str(),
            "invalidating recent exact publication truth because the current discovery selection policy no longer matches the policy that produced the published universe"
        );
        store.persist_discovery_cycle(&[], &[], &[], false, false, now, reason)?;
        store.set_discovery_publication_state_with_options(
            &DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: reason.to_string(),
                last_published_at: None,
                last_published_window_start: None,
                published_scoring_source: publication_state.published_scoring_source.clone(),
                published_wallet_ids: None,
            },
            true,
            None,
        )?;
        Ok(true)
    }

    pub(crate) fn runtime_publication_truth_from_state(
        publication_state: DiscoveryPublicationStateRow,
    ) -> Option<RuntimePublishedUniverseTruth> {
        Some(RuntimePublishedUniverseTruth {
            runtime_mode: publication_state.runtime_mode,
            reason: publication_state.reason,
            last_published_at: publication_state.last_published_at?,
            last_published_window_start: publication_state.last_published_window_start?,
            published_scoring_source: publication_state.published_scoring_source,
            published_wallet_ids: publication_state
                .published_wallet_ids
                .filter(|wallet_ids| !wallet_ids.is_empty())?,
        })
    }

    pub fn runtime_publication_truth_resolution(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<Option<RuntimePublicationTruthResolution>> {
        if self.invalidate_incompatible_recent_publication_truth_if_needed(store, now)? {
            return Ok(None);
        }
        let Some(publication_state) = store.discovery_publication_state_read_only()? else {
            return Ok(None);
        };
        if publication_state.runtime_mode == DiscoveryRuntimeMode::FailClosed {
            return Ok(None);
        }
        let Some(runtime_truth) =
            Self::runtime_publication_truth_from_state(publication_state.clone())
        else {
            return Ok(None);
        };
        if publication_state.is_fresh_under_gate(&self.publication_freshness_gate(), now) {
            return Ok(Some(RuntimePublicationTruthResolution::Recent(
                runtime_truth,
            )));
        }
        if store.discovery_bootstrap_degraded_state_read_only()?.active {
            let mut bootstrap_runtime_truth = runtime_truth;
            bootstrap_runtime_truth.runtime_mode = DiscoveryRuntimeMode::BootstrapDegraded;
            return Ok(Some(RuntimePublicationTruthResolution::BootstrapDegraded(
                bootstrap_runtime_truth,
            )));
        }
        Ok(None)
    }

    pub fn recent_runtime_publication_truth(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<Option<RuntimePublishedUniverseTruth>> {
        Ok(
            match self.runtime_publication_truth_resolution(store, now)? {
                Some(RuntimePublicationTruthResolution::Recent(truth)) => Some(truth),
                Some(RuntimePublicationTruthResolution::BootstrapDegraded(_)) | None => None,
            },
        )
    }

    pub fn bootstrap_degraded_runtime_publication_truth(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<Option<RuntimePublishedUniverseTruth>> {
        Ok(
            match self.runtime_publication_truth_resolution(store, now)? {
                Some(RuntimePublicationTruthResolution::BootstrapDegraded(truth)) => Some(truth),
                Some(RuntimePublicationTruthResolution::Recent(_)) | None => None,
            },
        )
    }

    pub fn recent_published_follow_universe_wallets(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<Option<HashSet<String>>> {
        Ok(self
            .recent_runtime_publication_truth(store, now)?
            .map(|truth| truth.active_wallets()))
    }

    pub(crate) fn log_publication_truth_repair_helper_return(
        telemetry: &DiscoveryPublicationTruthRepairTelemetry,
        trace_context: Option<&DiscoveryPublicationTruthRepairTraceContext>,
    ) {
        let trace_id = trace_context.map(|context| context.trace_id);
        info!(
            publication_truth_repair_trace_id = ?trace_id,
            repair_state = telemetry.state,
            repair_reason = telemetry.reason.as_deref().unwrap_or("none"),
            publication_truth_refresh_delegated_to_runtime_cycle =
                telemetry.publication_truth_refresh_delegated_to_runtime_cycle,
            publication_truth_refresh_publishable_checkpoint_blocker =
                telemetry.publication_truth_refresh_publishable_checkpoint_blocker,
            publication_state_exists_before = telemetry.publication_state_exists_before,
            publication_truth_complete_before = telemetry.publication_truth_complete_before,
            publication_truth_fresh_before = telemetry.publication_truth_fresh_before,
            runtime_cursor_exists_before = telemetry.runtime_cursor_exists_before,
            journal_store_exists = telemetry.journal_store_exists,
            runtime_window_complete_before = telemetry.runtime_window_complete_before,
            runtime_window_complete_after = telemetry.runtime_window_complete_after,
            publication_state_write_attempted_from_helper =
                telemetry.publication_truth_refresh_helper_write_attempted,
            publication_state_write_succeeded_from_helper =
                telemetry.publication_truth_refresh_helper_write_succeeded,
            publication_state_write_resulting_reason = ?telemetry
                .publication_truth_refresh_helper_write_resulting_reason,
            publication_state_write_resulting_updated_at = ?telemetry
                .publication_truth_refresh_helper_write_resulting_updated_at
                .map(|ts| ts.to_rfc3339()),
            publication_truth_refresh_resume_exact_target_surface_repair_attempted =
                telemetry.publication_truth_refresh_resume_exact_target_surface_repair_attempted,
            publication_truth_refresh_resume_exact_target_surface_repair_completed =
                telemetry.publication_truth_refresh_resume_exact_target_surface_repair_completed,
            publication_truth_refresh_resume_exact_target_surface_repair_time_budget_exhausted =
                telemetry
                    .publication_truth_refresh_resume_exact_target_surface_repair_time_budget_exhausted,
            publication_truth_refresh_resume_exact_target_surface_repair_wallet_pages =
                telemetry.publication_truth_refresh_resume_exact_target_surface_repair_wallet_pages,
            publication_truth_refresh_resume_exact_target_surface_repair_wallet_rows =
                telemetry.publication_truth_refresh_resume_exact_target_surface_repair_wallet_rows,
            publication_truth_refresh_resume_exact_target_surface_repair_target_buy_mints_restored =
                telemetry
                    .publication_truth_refresh_resume_exact_target_surface_repair_target_buy_mints_restored,
            "discovery publication truth repair helper returned"
        );
    }
}
