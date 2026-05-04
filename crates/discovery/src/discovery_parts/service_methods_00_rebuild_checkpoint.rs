impl DiscoveryService {
    fn persisted_stream_publishable_checkpoint_blocker_for_phase(
        phase: DiscoveryPersistedRebuildPhase,
        collect_buy_mints_mode: CollectBuyMintsMode,
        quality_next_mint_index: usize,
        unique_buy_mints: usize,
        replay_subphase: Option<&'static str>,
        publish_pending_requested_wallet_count: usize,
    ) -> &'static str {
        match phase {
            DiscoveryPersistedRebuildPhase::CollectBuyMints => match collect_buy_mints_mode {
                CollectBuyMintsMode::FreshScan => "collect_buy_mints_fresh_scan_incomplete",
                CollectBuyMintsMode::ReconcileExpiredHead => {
                    "collect_buy_mints_reconcile_expired_head_incomplete"
                }
                CollectBuyMintsMode::ReconcileNewTail => {
                    "collect_buy_mints_reconcile_new_tail_incomplete"
                }
            },
            DiscoveryPersistedRebuildPhase::ResolveTokenQuality => {
                if quality_next_mint_index < unique_buy_mints {
                    "token_quality_incomplete"
                } else {
                    "token_quality_handoff_pending"
                }
            }
            DiscoveryPersistedRebuildPhase::Replay => match replay_subphase {
                Some("wallet_stats") => "replay_wallet_stats_incomplete",
                Some("activity_backfill") => "replay_candidate_activity_backfill_incomplete",
                Some("sol_leg") => "replay_sol_leg_incomplete",
                _ => "replay_post_wallet_stats_handoff_pending",
            },
            DiscoveryPersistedRebuildPhase::PublishPending => {
                if publish_pending_requested_wallet_count == 0 {
                    "publish_pending_exact_publish_set_empty"
                } else {
                    "publish_pending_flush"
                }
            }
        }
    }

    fn persisted_stream_publishable_checkpoint_blocker_from_state(
        state: &PersistedStreamRebuildState,
    ) -> &'static str {
        if state.phase == DiscoveryPersistedRebuildPhase::PublishPending
            && state
                .payload
                .publish_pending_requested_wallet_ids
                .as_ref()
                .is_some_and(|wallets| wallets.is_empty())
            && state
                .payload
                .publish_pending_quality_retry_mints
                .as_ref()
                .is_some_and(|mints| !mints.is_empty())
        {
            return "publish_pending_exact_publish_set_quality_unresolved";
        }
        Self::persisted_stream_publishable_checkpoint_blocker_for_phase(
            state.phase,
            state.payload.collect_buy_mints_mode,
            state.payload.token_quality_progress.next_mint_index,
            state.payload.unique_buy_mints.len(),
            Self::replay_subphase(
                state.phase,
                state.payload.replay_wallet_stats_complete,
                state.payload.replay_candidate_activity_backfill_pending,
            ),
            state
                .payload
                .publish_pending_requested_wallet_ids
                .as_ref()
                .map_or(usize::MAX, Vec::len),
        )
    }

    fn persisted_stream_publishable_checkpoint_blocker(
        telemetry: &PersistedStreamProgressTelemetry,
    ) -> &'static str {
        Self::persisted_stream_publishable_checkpoint_blocker_for_phase(
            telemetry.phase,
            telemetry.collect_buy_mints_mode,
            telemetry.quality_next_mint_index,
            telemetry.unique_buy_mints,
            telemetry.replay_subphase,
            telemetry.publish_pending_requested_wallet_count,
        )
    }

    fn replay_subphase(
        phase: DiscoveryPersistedRebuildPhase,
        replay_wallet_stats_complete: bool,
        replay_candidate_activity_backfill_pending: bool,
    ) -> Option<&'static str> {
        if phase != DiscoveryPersistedRebuildPhase::Replay {
            return None;
        }
        Some(if !replay_wallet_stats_complete {
            "wallet_stats"
        } else if replay_candidate_activity_backfill_pending {
            "activity_backfill"
        } else {
            "sol_leg"
        })
    }

    fn state_can_carry_forward_replay_sol_leg_reentry(
        &self,
        state: &PersistedStreamRebuildState,
    ) -> bool {
        if self.config.min_buy_count == 0 {
            return false;
        }
        Self::state_has_replay_wallet_stats_milestone(state)
    }

    fn state_has_replay_wallet_stats_milestone(state: &PersistedStreamRebuildState) -> bool {
        state.payload.replay_wallet_stats_milestone_reached
            || match state.phase {
                DiscoveryPersistedRebuildPhase::Replay => {
                    state.payload.replay_wallet_stats_complete
                        || state.payload.replay_candidate_activity_backfill_required
                        || state.payload.replay_candidate_activity_backfill_pending
                }
                DiscoveryPersistedRebuildPhase::PublishPending => true,
                _ => false,
            }
    }
}
