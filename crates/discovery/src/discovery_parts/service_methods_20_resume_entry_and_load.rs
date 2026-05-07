use super::*;

impl DiscoveryService {
    pub(crate) fn repair_restored_persisted_stream_state_for_resume(
        &self,
        state: &mut PersistedStreamRebuildState,
        now: DateTime<Utc>,
        helper_trace_context: Option<&DiscoveryPublicationTruthRepairTraceContext>,
    ) -> bool {
        let mut region = DiscoveryPublicationTruthRepairRegionScope::new(
            helper_trace_context,
            "repair_restored_persisted_stream_state_for_resume",
            Some("load_or_start_persisted_stream_rebuild_state_with_options"),
        );
        region.progress_mut().rebuild_phase = Some(state.phase.as_str());
        region.progress_mut().rebuild_replay_subphase = Self::replay_subphase(
            state.phase,
            state.payload.replay_wallet_stats_complete,
            state.payload.replay_candidate_activity_backfill_pending,
        );
        let changed = match state.phase {
            DiscoveryPersistedRebuildPhase::CollectBuyMints => {
                self.repair_restored_collect_buy_mints_state_for_resume(state, now)
            }
            DiscoveryPersistedRebuildPhase::ResolveTokenQuality => {
                self.repair_restored_token_quality_state_for_resume(state, now)
            }
            DiscoveryPersistedRebuildPhase::Replay
            | DiscoveryPersistedRebuildPhase::PublishPending => {
                self.repair_restored_replay_or_publish_state_for_resume(state, now)
            }
        };
        region.progress_mut().rebuild_phase = Some(state.phase.as_str());
        region.progress_mut().rebuild_replay_subphase = Self::replay_subphase(
            state.phase,
            state.payload.replay_wallet_stats_complete,
            state.payload.replay_candidate_activity_backfill_pending,
        );
        region.progress_mut().state_repaired_for_resume = Some(changed);
        changed
    }

    pub(crate) fn derive_legacy_collect_buy_mints_safe_prefix_len(
        &self,
        store: &SqliteStore,
        state: &PersistedStreamRebuildState,
        canonical_mints: &[String],
        deadline: Instant,
    ) -> Result<(usize, bool)> {
        if canonical_mints.is_empty() {
            return Ok((0, false));
        }

        let mut lo = 0usize;
        let mut hi = canonical_mints.len();
        let mut safe_prefix_len = 0usize;
        while lo < hi {
            if Instant::now() >= deadline {
                return Ok((safe_prefix_len, true));
            }
            let mid = lo + (hi - lo) / 2;
            let observed_prefix = store
                .count_observed_buy_mints_in_window_up_to_token_with_budget(
                    state.window_start,
                    state.horizon_end,
                    &canonical_mints[mid],
                    deadline,
                )?;
            if observed_prefix.time_budget_exhausted {
                return Ok((safe_prefix_len, true));
            }
            if observed_prefix.count == mid.saturating_add(1) {
                safe_prefix_len = mid.saturating_add(1);
                lo = mid.saturating_add(1);
            } else {
                hi = mid;
            }
        }
        Ok((safe_prefix_len, false))
    }

    pub(crate) fn load_or_start_persisted_stream_rebuild_state(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(
        PersistedStreamRebuildState,
        PersistedStreamRebuildRestoreOutcome,
    )> {
        let (state, outcome, _) = self.load_or_start_persisted_stream_rebuild_state_with_options(
            store,
            window_start,
            metrics_window_start,
            now,
            None,
            None,
            false,
        )?;
        Ok((state, outcome))
    }
}
