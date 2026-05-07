use super::*;

impl DiscoveryService {
    pub(crate) fn repair_replay_exact_target_buy_mint_surface_for_resume(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        deadline: Instant,
        helper_trace_context: Option<&DiscoveryPublicationTruthRepairTraceContext>,
    ) -> Result<ResumeExactTargetBuyMintSurfaceRepairDiagnostics> {
        let mut region = DiscoveryPublicationTruthRepairRegionScope::new(
            helper_trace_context,
            "repair_replay_exact_target_buy_mint_surface_for_resume",
            Some("load_or_start_persisted_stream_rebuild_state_with_options"),
        );
        region.progress_mut().rebuild_phase = Some(state.phase.as_str());
        region.progress_mut().rebuild_replay_subphase = Self::replay_subphase(
            state.phase,
            state.payload.replay_wallet_stats_complete,
            state.payload.replay_candidate_activity_backfill_pending,
        );
        if !Self::state_can_backfill_exact_target_buy_mint_surface_for_resume(state) {
            return Ok(ResumeExactTargetBuyMintSurfaceRepairDiagnostics::default());
        }

        let exact_wallet_ids: Vec<String> = state.payload.by_wallet.keys().cloned().collect();
        let wallet_limit =
            Self::replay_wallet_stats_wallet_batch_size(self.config.max_fetch_swaps_per_cycle)
                .max(1);
        let mut diagnostics = ResumeExactTargetBuyMintSurfaceRepairDiagnostics {
            attempted: true,
            wallet_cursor_before: state
                .payload
                .replay_exact_target_surface_wallet_cursor
                .clone(),
            ..ResumeExactTargetBuyMintSurfaceRepairDiagnostics::default()
        };
        region.progress_mut().wallets_scanned = exact_wallet_ids.len();
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
            rebuild_wallets_buffered = state.payload.by_wallet.len(),
            rebuild_target_buy_mints = state.payload.discovery_critical_target_buy_mints.len(),
            rebuild_resume_exact_target_surface_deadline_remaining_ms =
                deadline.saturating_duration_since(Instant::now()).as_millis().min(u64::MAX as u128)
                    as u64,
            "repairing resumed replay checkpoint exact target-mint surface before helper/run_cycle publication decisions"
        );
        let mut wallet_cursor = state
            .payload
            .replay_exact_target_surface_wallet_cursor
            .clone();
        let mut staged_wallet_ids =
            std::mem::take(&mut state.payload.replay_exact_target_surface_staged_wallet_ids);
        let mut staged_wallet_cursor_after = state
            .payload
            .replay_exact_target_surface_staged_wallet_cursor_after
            .take();
        {
            let mut scan_region = DiscoveryPublicationTruthRepairRegionScope::new(
                helper_trace_context,
                "repair_replay_exact_target_buy_mint_surface_for_resume.wallet_activity_scan",
                Some("repair_replay_exact_target_buy_mint_surface_for_resume"),
            );
            scan_region.progress_mut().rebuild_phase = Some(state.phase.as_str());
            scan_region.progress_mut().rebuild_replay_subphase = Self::replay_subphase(
                state.phase,
                state.payload.replay_wallet_stats_complete,
                state.payload.replay_candidate_activity_backfill_pending,
            );
            scan_region.progress_mut().wallets_scanned = exact_wallet_ids.len();
            loop {
                let consuming_staged_pre_row_state = !staged_wallet_ids.is_empty();
                let staged_page_len = staged_wallet_ids.len();
                let page = if consuming_staged_pre_row_state {
                    diagnostics.resumed_from_staged_pre_row_state = true;
                    store
                        .observed_wallet_activity_page_for_staged_wallet_ids_in_window_with_budget(
                            &staged_wallet_ids,
                            state.window_start,
                            state.horizon_end,
                            self.config.max_tx_per_minute,
                            deadline,
                        )?
                } else {
                    store.observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
                        &exact_wallet_ids,
                        state.window_start,
                        state.horizon_end,
                        wallet_cursor.as_deref(),
                        wallet_limit,
                        self.config.max_tx_per_minute,
                        deadline,
                    )?
                };
                diagnostics.wallet_pages = diagnostics.wallet_pages.saturating_add(1);
                diagnostics.wallet_rows = diagnostics.wallet_rows.saturating_add(page.rows_seen);
                scan_region.progress_mut().pages_scanned = diagnostics.wallet_pages;
                scan_region.progress_mut().rows_scanned = diagnostics.wallet_rows;
                for row in page.rows.iter().cloned() {
                    Self::observe_replay_wallet_activity_summary(&mut state.payload, row);
                }
                #[cfg(test)]
                let forced_wallet_page_budget_exhausted = {
                    let page_limit = REPLAY_RESUME_EXACT_TARGET_SURFACE_TEST_WALLET_PAGE_LIMIT
                        .load(AtomicOrdering::Relaxed);
                    page_limit > 0 && diagnostics.wallet_pages >= page_limit
                };
                #[cfg(not(test))]
                let forced_wallet_page_budget_exhausted = false;
                if page.time_budget_exhausted || forced_wallet_page_budget_exhausted {
                    if consuming_staged_pre_row_state && page.wallet_id_page_wallet_ids.is_empty() {
                        let mut staged_page = page.clone();
                        staged_page.wallet_id_page_wallets_seen = staged_page_len;
                        staged_page.wallet_id_page_cursor_after =
                            staged_wallet_cursor_after.clone();
                        staged_page.wallet_id_page_wallet_ids = staged_wallet_ids.clone();
                        Self::persist_replay_exact_target_buy_mint_surface_budget_exhaustion_state(
                            state,
                            &mut diagnostics,
                            &staged_page,
                            staged_wallet_cursor_after.clone(),
                        );
                    } else {
                        let wallet_cursor_after = if consuming_staged_pre_row_state {
                            staged_wallet_cursor_after.clone()
                        } else {
                            page.rows
                                .last()
                                .map(|row| row.wallet_id.clone())
                                .or_else(|| page.wallet_id_page_cursor_after.clone())
                        };
                        Self::persist_replay_exact_target_buy_mint_surface_budget_exhaustion_state(
                            state,
                            &mut diagnostics,
                            &page,
                            wallet_cursor_after,
                        );
                    }
                    scan_region.progress_mut().time_budget_exhausted = Some(true);
                    region.progress_mut().pages_scanned = diagnostics.wallet_pages;
                    region.progress_mut().rows_scanned = diagnostics.wallet_rows;
                    region.progress_mut().wallets_scanned = exact_wallet_ids.len();
                    region.progress_mut().time_budget_exhausted = Some(true);
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
                        rebuild_wallets_buffered = state.payload.by_wallet.len(),
                        rebuild_resume_exact_target_surface_wallet_pages = diagnostics.wallet_pages,
                        rebuild_resume_exact_target_surface_wallet_rows = diagnostics.wallet_rows,
                        rebuild_resume_exact_target_surface_wallet_id_page_wallets_seen =
                            diagnostics.wallet_id_page_wallets_seen,
                        rebuild_resume_exact_target_surface_wallet_cursor_before =
                            diagnostics.wallet_cursor_before.as_deref(),
                        rebuild_resume_exact_target_surface_wallet_cursor_after =
                            diagnostics.wallet_cursor_after.as_deref(),
                        rebuild_resume_exact_target_surface_partial_progress_persisted =
                            diagnostics.persisted_partial_progress,
                        rebuild_resume_exact_target_surface_staged_pre_row_state_persisted =
                            diagnostics.persisted_staged_pre_row_state,
                        rebuild_resume_exact_target_surface_blocked_state_persisted =
                            diagnostics.persisted_blocked_state,
                        rebuild_resume_exact_target_surface_time_budget_exhausted =
                            diagnostics.time_budget_exhausted,
                        "bounded helper/resume repair exhausted its deadline before reconstructing the missing exact target-mint surface"
                    );
                    return Ok(diagnostics);
                }
                if consuming_staged_pre_row_state {
                    staged_wallet_ids.clear();
                    state
                        .payload
                        .replay_exact_target_surface_staged_wallet_ids
                        .clear();
                    state
                        .payload
                        .replay_exact_target_surface_staged_wallet_cursor_after = None;
                    state.payload.replay_exact_target_surface_pre_row_blocked = false;
                    wallet_cursor = staged_wallet_cursor_after.take();
                    if staged_page_len < wallet_limit {
                        wallet_cursor = None;
                        break;
                    }
                    continue;
                }
                wallet_cursor = page
                    .rows
                    .last()
                    .map(|row| row.wallet_id.clone())
                    .or_else(|| page.wallet_id_page_cursor_after.clone());
                if page.rows.len() < wallet_limit {
                    wallet_cursor = None;
                    break;
                }
            }
            scan_region.progress_mut().time_budget_exhausted = Some(false);
        }
        diagnostics.wallet_cursor_after = wallet_cursor.clone();
        Self::clear_replay_exact_target_surface_resume_state(&mut state.payload);

        let exact_target_buy_mints = {
            let mut rebuild_region = DiscoveryPublicationTruthRepairRegionScope::new(
                helper_trace_context,
                "repair_replay_exact_target_buy_mint_surface_for_resume.rebuild_target_buy_mints",
                Some("repair_replay_exact_target_buy_mint_surface_for_resume"),
            );
            rebuild_region.progress_mut().rebuild_phase = Some(state.phase.as_str());
            rebuild_region.progress_mut().rebuild_replay_subphase = Self::replay_subphase(
                state.phase,
                state.payload.replay_wallet_stats_complete,
                state.payload.replay_candidate_activity_backfill_pending,
            );
            rebuild_region.progress_mut().wallets_scanned = exact_wallet_ids.len();
            rebuild_region.progress_mut().pages_scanned = diagnostics.wallet_pages;
            rebuild_region.progress_mut().rows_scanned = diagnostics.wallet_rows;
            let exact_target_buy_mints = self
                .discovery_critical_target_buy_mints_from_accumulators(
                    store,
                    &state.payload.by_wallet,
                    state.horizon_end,
                )?;
            rebuild_region.progress_mut().rebuilt_target_mint_count = exact_target_buy_mints.len();
            exact_target_buy_mints
        };
        if exact_target_buy_mints.is_empty() {
            region.progress_mut().pages_scanned = diagnostics.wallet_pages;
            region.progress_mut().rows_scanned = diagnostics.wallet_rows;
            region.progress_mut().wallets_scanned = exact_wallet_ids.len();
            region.progress_mut().time_budget_exhausted = Some(false);
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
                rebuild_wallets_buffered = state.payload.by_wallet.len(),
                rebuild_resume_exact_target_surface_wallet_pages = diagnostics.wallet_pages,
                rebuild_resume_exact_target_surface_wallet_rows = diagnostics.wallet_rows,
                rebuild_resume_exact_target_surface_wallet_cursor_before =
                    diagnostics.wallet_cursor_before.as_deref(),
                rebuild_resume_exact_target_surface_wallet_cursor_after =
                    diagnostics.wallet_cursor_after.as_deref(),
                rebuild_target_buy_mints = 0usize,
                "resumed replay exact target-mint surface repair finished without reconstructing any target buy mints"
            );
            return Ok(diagnostics);
        }

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
            rebuild_wallets_buffered = state.payload.by_wallet.len(),
            rebuild_repair_wallet_activity_pages = diagnostics.wallet_pages,
            rebuild_repair_wallet_activity_rows = diagnostics.wallet_rows,
            rebuild_target_buy_mints = exact_target_buy_mints.len(),
            "backfilling missing exact candidate target-mint surface onto a resumed frozen partial SOL-leg checkpoint so persisted replay can re-enter the exact-target replay contract instead of broad-source fallback"
        );
        state.payload.discovery_critical_target_buy_mints = exact_target_buy_mints;
        Self::clear_replay_exact_target_surface_resume_state(&mut state.payload);
        diagnostics.completed = true;
        diagnostics.target_buy_mints_restored =
            state.payload.discovery_critical_target_buy_mints.len();
        region.progress_mut().pages_scanned = diagnostics.wallet_pages;
        region.progress_mut().rows_scanned = diagnostics.wallet_rows;
        region.progress_mut().wallets_scanned = exact_wallet_ids.len();
        region.progress_mut().time_budget_exhausted = Some(false);
        region.progress_mut().rebuilt_target_mint_count = diagnostics.target_buy_mints_restored;
        Ok(diagnostics)
    }
}
