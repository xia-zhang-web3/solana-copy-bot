impl DiscoveryService {
    fn persist_publication_state(
        &self,
        store: &SqliteStore,
        runtime_mode: DiscoveryRuntimeMode,
        publish_due: bool,
        published_window_start: DateTime<Utc>,
        published_wallet_ids: Option<&[String]>,
        scoring_source: &'static str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<PublicationStatePersistOutcome> {
        let existing_publication_state = store.discovery_publication_state_read_only()?;
        let mut write_attempted = false;
        let mut healthy_publish_refused = false;
        let mut carry_forward_happened = false;
        if !publish_due {
            if runtime_mode != DiscoveryRuntimeMode::Healthy {
                if let Some(existing_publication_state) = existing_publication_state.as_ref() {
                    if existing_publication_state.runtime_mode == DiscoveryRuntimeMode::Healthy {
                        warn!(
                            publication_runtime_surface_refreshed_without_publish = true,
                            publication_existing_runtime_mode =
                                existing_publication_state.runtime_mode.as_str(),
                            publication_existing_reason = existing_publication_state.reason.as_str(),
                        publication_new_runtime_mode = runtime_mode.as_str(),
                        publication_new_reason = reason,
                        publication_scoring_source = scoring_source,
                        "downgrading a stale healthy publication row during a non-publish-due discovery cycle so incomplete replay recovery cannot keep surfacing healthy publication state without a fresh exact universe flush"
                    );
                        write_attempted = true;
                        carry_forward_happened =
                            existing_publication_state.last_published_at.is_some()
                                || existing_publication_state
                                    .published_wallet_ids
                                    .as_ref()
                                    .is_some_and(|wallets| !wallets.is_empty());
                        store.set_discovery_publication_state_with_options(
                            &DiscoveryPublicationStateUpdate {
                                runtime_mode,
                                reason: reason.to_string(),
                                last_published_at: None,
                                last_published_window_start: None,
                                published_scoring_source: Some(scoring_source.to_string()),
                                published_wallet_ids: None,
                            },
                            reason == RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON,
                            None,
                        )?;
                    }
                }
            }
            let outcome = PublicationStatePersistOutcome {
                runtime_mode,
                published_universe_persisted: false,
                write_attempted,
                healthy_publish_refused: false,
                carry_forward_happened,
                effective_reason: reason.to_string(),
            };
            info!(
                publication_persist_publish_due = publish_due,
                publication_persist_write_attempted = outcome.write_attempted,
                publication_persist_runtime_mode = outcome.runtime_mode.as_str(),
                publication_persist_published_universe_persisted =
                    outcome.published_universe_persisted,
                publication_persist_reason = outcome.effective_reason.as_str(),
                publication_persist_healthy_publish_refused = outcome.healthy_publish_refused,
                publication_persist_carry_forward_happened = outcome.carry_forward_happened,
                "discovery publication persist returned"
            );
            return Ok(outcome);
        }
        let mut effective_runtime_mode = runtime_mode;
        let mut effective_reason = reason.to_string();
        let requested_published_wallet_count =
            published_wallet_ids.map_or(0, |wallets| wallets.len());
        if runtime_mode == DiscoveryRuntimeMode::Healthy {
            let incomplete_persisted_checkpoint = store
                .load_discovery_persisted_rebuild_state()?
                .map(Self::persisted_stream_rebuild_state_from_row)
                .transpose()?
                .filter(|state| state.phase != DiscoveryPersistedRebuildPhase::PublishPending);
            if let Some(state) = incomplete_persisted_checkpoint.as_ref() {
                let checkpoint_blocker =
                    Self::persisted_stream_publishable_checkpoint_blocker_from_state(state);
                warn!(
                    publication_healthy_write_withheld = true,
                    publication_write_reason = reason,
                    publication_scoring_source = scoring_source,
                    publication_requested_wallet_count = requested_published_wallet_count,
                    rebuild_phase = state.phase.as_str(),
                    rebuild_publishable_checkpoint_blocker = checkpoint_blocker,
                    "refusing to persist healthy publication state while an incomplete persisted discovery rebuild checkpoint still exists"
                );
                healthy_publish_refused = true;
                effective_runtime_mode = DiscoveryRuntimeMode::FailClosed;
                effective_reason = format!("publication_truth_withheld_while_{checkpoint_blocker}");
            } else if requested_published_wallet_count == 0 {
                warn!(
                    publication_healthy_write_withheld = true,
                    publication_write_reason = reason,
                    publication_scoring_source = scoring_source,
                    publication_requested_wallet_count = requested_published_wallet_count,
                    "refusing to persist healthy publication state without an exact non-empty published wallet universe"
                );
                healthy_publish_refused = true;
                effective_runtime_mode = DiscoveryRuntimeMode::FailClosed;
                effective_reason =
                    "publication_truth_withheld_missing_exact_published_wallet_ids".to_string();
            }
        }
        let published_universe = effective_runtime_mode == DiscoveryRuntimeMode::Healthy;
        let clear_published_truth =
            !published_universe && effective_reason == RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON;
        write_attempted = true;
        carry_forward_happened = !published_universe
            && !clear_published_truth
            && existing_publication_state.as_ref().is_some_and(|state| {
                state.last_published_at.is_some()
                    || state
                        .published_wallet_ids
                        .as_ref()
                        .is_some_and(|wallets| !wallets.is_empty())
            });
        store.set_discovery_publication_state_with_options(
            &DiscoveryPublicationStateUpdate {
                runtime_mode: effective_runtime_mode,
                reason: effective_reason.clone(),
                last_published_at: published_universe.then_some(now),
                last_published_window_start: published_universe.then_some(published_window_start),
                published_scoring_source: Some(scoring_source.to_string()),
                published_wallet_ids: if published_universe {
                    published_wallet_ids.map(|wallet_ids| wallet_ids.to_vec())
                } else {
                    None
                },
            },
            clear_published_truth,
            published_universe
                .then(|| self.publication_selection_policy_fingerprint())
                .as_deref(),
        )?;
        let outcome = PublicationStatePersistOutcome {
            runtime_mode: effective_runtime_mode,
            published_universe_persisted: published_universe,
            write_attempted,
            healthy_publish_refused,
            carry_forward_happened,
            effective_reason,
        };
        info!(
            publication_persist_publish_due = publish_due,
            publication_persist_write_attempted = outcome.write_attempted,
            publication_persist_runtime_mode = outcome.runtime_mode.as_str(),
            publication_persist_published_universe_persisted = outcome.published_universe_persisted,
            publication_persist_reason = outcome.effective_reason.as_str(),
            publication_persist_healthy_publish_refused = outcome.healthy_publish_refused,
            publication_persist_carry_forward_happened = outcome.carry_forward_happened,
            "discovery publication persist returned"
        );
        Ok(outcome)
    }

    fn in_band_wallet_freshness_shadow_evidence_lookback_seconds(&self) -> u64 {
        self.config.refresh_seconds.max(1).saturating_mul(2)
    }

    fn cached_cycle_exact_published_wallet_ids<'a>(
        runtime_mode: DiscoveryRuntimeMode,
        current_raw: Option<&'a CachedCurrentRawTruthSample>,
    ) -> Option<&'a [String]> {
        (runtime_mode == DiscoveryRuntimeMode::Healthy)
            .then_some(current_raw?.top_wallet_ids.as_slice())
    }
}
