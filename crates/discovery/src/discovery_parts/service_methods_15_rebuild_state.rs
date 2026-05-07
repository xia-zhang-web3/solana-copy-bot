use super::*;

impl DiscoveryService {
    pub(crate) fn persisted_stream_rebuild_state_from_row(
        row: DiscoveryPersistedRebuildStateRow,
    ) -> Result<PersistedStreamRebuildState> {
        let payload: PersistedStreamRebuildPayload = serde_json::from_str(&row.state_json)
            .context("failed deserializing discovery persisted rebuild state payload")?;
        Ok(PersistedStreamRebuildState {
            phase: row.phase,
            window_start: row.window_start,
            horizon_end: row.horizon_end,
            metrics_window_start: row.metrics_window_start,
            phase_cursor: row.phase_cursor,
            prepass_rows_processed: row.prepass_rows_processed,
            prepass_pages_processed: row.prepass_pages_processed,
            replay_rows_processed: row.replay_rows_processed,
            replay_pages_processed: row.replay_pages_processed,
            chunks_completed: row.chunks_completed,
            started_at: row.started_at,
            updated_at: row.updated_at,
            payload,
        })
    }

    pub(crate) fn persisted_stream_rebuild_row(
        state: &PersistedStreamRebuildState,
        updated_at: DateTime<Utc>,
    ) -> Result<DiscoveryPersistedRebuildStateRow> {
        Ok(DiscoveryPersistedRebuildStateRow {
            phase: state.phase,
            window_start: state.window_start,
            horizon_end: state.horizon_end,
            metrics_window_start: state.metrics_window_start,
            phase_cursor: state.phase_cursor.clone(),
            prepass_rows_processed: state.prepass_rows_processed,
            prepass_pages_processed: state.prepass_pages_processed,
            replay_rows_processed: state.replay_rows_processed,
            replay_pages_processed: state.replay_pages_processed,
            chunks_completed: state.chunks_completed,
            state_json: serde_json::to_string(&state.payload)
                .context("failed serializing discovery persisted rebuild state payload")?,
            started_at: state.started_at,
            updated_at,
        })
    }

    pub(crate) fn start_persisted_stream_rebuild_state(
        &self,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> PersistedStreamRebuildState {
        PersistedStreamRebuildState {
            phase: DiscoveryPersistedRebuildPhase::CollectBuyMints,
            window_start,
            horizon_end: now,
            metrics_window_start,
            phase_cursor: None,
            prepass_rows_processed: 0,
            prepass_pages_processed: 0,
            replay_rows_processed: 0,
            replay_pages_processed: 0,
            chunks_completed: 0,
            started_at: now,
            updated_at: now,
            payload: PersistedStreamRebuildPayload {
                replay_mode: ReplayMode::WalletStatsThenSolLeg,
                ..PersistedStreamRebuildPayload::default()
            },
        }
    }

    pub(crate) fn sync_unique_buy_mints_from_counts(payload: &mut PersistedStreamRebuildPayload) {
        payload.unique_buy_mints = payload
            .buy_mint_counts
            .iter()
            .filter_map(|(mint, count)| (*count > 0).then_some(mint.clone()))
            .collect();
    }

    pub(crate) fn payload_has_exact_buy_mint_membership(payload: &PersistedStreamRebuildPayload) -> bool {
        payload.unique_buy_mints.is_empty()
            || payload.buy_mint_counts.len() == payload.unique_buy_mints.len()
    }

    pub(crate) fn state_can_carry_forward_metrics_rollover(state: &PersistedStreamRebuildState) -> bool {
        let has_exact_buy_mint_membership =
            Self::payload_has_exact_buy_mint_membership(&state.payload);
        match state.phase {
            DiscoveryPersistedRebuildPhase::CollectBuyMints
            | DiscoveryPersistedRebuildPhase::PublishPending => {
                state.payload.collect_buy_mints_mode == CollectBuyMintsMode::FreshScan
                    && has_exact_buy_mint_membership
            }
            DiscoveryPersistedRebuildPhase::ResolveTokenQuality
            | DiscoveryPersistedRebuildPhase::Replay => has_exact_buy_mint_membership,
        }
    }

    pub(crate) fn state_can_resume_stale_metrics_window_until_exact_checkpoint(
        state: &PersistedStreamRebuildState,
    ) -> bool {
        state.phase == DiscoveryPersistedRebuildPhase::CollectBuyMints
            && matches!(
                state.payload.collect_buy_mints_mode,
                CollectBuyMintsMode::ReconcileExpiredHead | CollectBuyMintsMode::ReconcileNewTail
            )
            && Self::payload_has_exact_buy_mint_membership(&state.payload)
    }

    pub(crate) fn metrics_window_start_remains_publishable_under_gate(
        &self,
        metrics_window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> bool {
        let expected_metrics_window_start = self.metrics_window_start(now);
        let max_lag =
            Duration::seconds(self.runtime_metric_snapshot_interval_seconds().max(1) as i64);
        metrics_window_start + max_lag >= expected_metrics_window_start
    }

    pub(crate) fn horizon_end_remains_publishable_under_gate(
        &self,
        horizon_end: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> bool {
        let max_lag =
            Duration::seconds(self.runtime_metric_snapshot_interval_seconds().max(1) as i64);
        horizon_end + max_lag >= now
    }

    pub(crate) fn state_can_resume_stale_metrics_window_until_publish_checkpoint(
        &self,
        state: &PersistedStreamRebuildState,
        now: DateTime<Utc>,
    ) -> bool {
        matches!(
            state.phase,
            DiscoveryPersistedRebuildPhase::ResolveTokenQuality
                | DiscoveryPersistedRebuildPhase::Replay
        ) && Self::payload_has_exact_buy_mint_membership(&state.payload)
            && self.metrics_window_start_remains_publishable_under_gate(
                state.metrics_window_start,
                now,
            )
            && self.horizon_end_remains_publishable_under_gate(state.horizon_end, now)
    }

    pub(crate) fn state_can_pin_stale_metrics_window_until_first_publishable_checkpoint(
        state: &PersistedStreamRebuildState,
    ) -> bool {
        state.phase == DiscoveryPersistedRebuildPhase::Replay
            && Self::payload_has_exact_buy_mint_membership(&state.payload)
            && state.payload.replay_wallet_stats_milestone_reached
            && state.payload.replay_wallet_stats_complete
            && !state.payload.replay_candidate_activity_backfill_pending
            && state.phase_cursor.is_some()
            && (state
                .payload
                .replay_sol_leg_last_partial_cycle_pages_processed
                > 0
                || state.payload.replay_sol_leg_budget_floor_pages > 0
                || state.payload.replay_sol_leg_retained_contract_floor_pages > 0)
    }

    pub(crate) fn stale_reconcile_token_batch_size(fetch_limit: usize) -> usize {
        fetch_limit.max(1).min(STALE_RECONCILE_TOKEN_BATCH_CAP)
    }

    pub(crate) fn narrowed_stale_reconcile_slice_end(sorted_candidate_mints: &[String]) -> Option<String> {
        if sorted_candidate_mints.len() <= 1 {
            return sorted_candidate_mints.last().cloned();
        }
        let narrowed_end_index = sorted_candidate_mints.len().saturating_sub(1) / 2;
        sorted_candidate_mints.get(narrowed_end_index).cloned()
    }

    pub(crate) fn clear_reconcile_new_tail_pending_batch(payload: &mut PersistedStreamRebuildPayload) {
        payload
            .collect_buy_mints_reconcile_new_tail_pending_mints
            .clear();
    }

    pub(crate) fn clear_reconcile_expired_head_pending_batch(payload: &mut PersistedStreamRebuildPayload) {
        payload
            .collect_buy_mints_reconcile_expired_head_pending_mints
            .clear();
    }

    pub(crate) fn stale_reconcile_exact_count_batch_size(fetch_limit: usize) -> usize {
        fetch_limit
            .max(1)
            .min(STALE_RECONCILE_TOKEN_BATCH_CAP)
            .min(STALE_RECONCILE_EXACT_COUNT_BATCH_CAP)
    }

    pub(crate) fn collect_buy_mints_fresh_scan_batch_size(fetch_limit: usize) -> usize {
        fetch_limit
            .max(1)
            .min(COLLECT_BUY_MINTS_FRESH_SCAN_BATCH_CAP)
    }

    pub(crate) fn collect_buy_mints_fresh_scan_work_deadline(
        state: &PersistedStreamRebuildState,
        deadline: Instant,
    ) -> Instant {
        if state.payload.collect_buy_mints_mode != CollectBuyMintsMode::FreshScan
            || !Self::payload_has_exact_buy_mint_membership(&state.payload)
            || state.payload.token_quality_progress.next_mint_index
                >= state.payload.unique_buy_mints.len()
        {
            return deadline;
        }

        let now = Instant::now();
        let remaining = deadline.saturating_duration_since(now);
        if remaining.is_zero() {
            return now;
        }

        let reserve = StdDuration::from_millis(COLLECT_BUY_MINTS_QUALITY_WARMUP_RESERVE_MS)
            .min(remaining / 5);
        if reserve.is_zero() || reserve >= remaining {
            deadline
        } else {
            deadline - reserve
        }
    }

    pub(crate) fn collect_buy_mints_catch_up_page_limit(fetch_limit: usize, fetch_page_limit: usize) -> usize {
        let fresh_scan_batch_size = Self::collect_buy_mints_fresh_scan_batch_size(fetch_limit);
        let baseline_page_limit = fetch_page_limit
            .max(1)
            .saturating_mul(COLLECT_BUY_MINTS_CATCH_UP_PAGE_LIMIT_MULTIPLIER);
        let pages_for_fetch_width = fetch_limit
            .max(1)
            .saturating_add(fresh_scan_batch_size.saturating_sub(1))
            / fresh_scan_batch_size.max(1);
        baseline_page_limit.max(pages_for_fetch_width.max(1))
    }

    pub(crate) fn collect_buy_mints_repair_phase_page_limit(
        &self,
        fetch_limit: usize,
        fetch_page_limit: usize,
        repair_time_budget: StdDuration,
    ) -> usize {
        let baseline_page_limit =
            Self::collect_buy_mints_catch_up_page_limit(fetch_limit, fetch_page_limit);
        let normal_fetch_budget_ms = self.config.fetch_time_budget_ms.max(1) as u128;
        let repair_budget_ms = repair_time_budget.as_millis().max(1);
        let budget_multiplier = repair_budget_ms.div_ceil(normal_fetch_budget_ms);
        baseline_page_limit
            .saturating_mul(budget_multiplier.min(usize::MAX as u128).max(1) as usize)
    }
}
