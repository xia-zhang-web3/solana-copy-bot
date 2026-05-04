impl DiscoveryService {
    fn advance_persisted_stream_prepass(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        collect_buy_mints_phase_page_limit_override: Option<usize>,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        let mut rows_processed = 0usize;
        let mut pages_processed = 0usize;
        let mut unique_buy_mints_discovered = 0usize;
        let mut cursor_token = state.payload.collect_buy_mints_cursor_token.clone();
        let phase_page_limit = collect_buy_mints_phase_page_limit_override
            .unwrap_or(fetch_page_limit)
            .max(1);
        if state.payload.collect_buy_mints_mode == CollectBuyMintsMode::FreshScan
            && cursor_token.is_none()
            && state.phase_cursor.is_some()
        {
            include!("service_methods_28_collect_buy_mints_prepass_legacy_migration.rs");
        }
        let budget_exhausted_reason = loop {
            if pages_processed >= phase_page_limit {
                break Some(PersistedStreamBudgetExhaustedReason::PageBudget);
            }
            if Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            match state.payload.collect_buy_mints_mode {
                CollectBuyMintsMode::FreshScan => {
                    include!("service_methods_28_collect_buy_mints_prepass_fresh_scan.rs");
                }
                CollectBuyMintsMode::ReconcileExpiredHead => {
                    include!(
                        "service_methods_28_collect_buy_mints_prepass_reconcile_expired_head.rs"
                    );
                }
                CollectBuyMintsMode::ReconcileNewTail => {
                    include!(
                        "service_methods_28_collect_buy_mints_prepass_reconcile_new_tail.rs"
                    );
                }
            }
        };

        let _ = self.maybe_warm_collect_buy_mints_token_quality_prefix(
            store,
            state,
            fetch_limit,
            deadline,
        )?;

        Ok(PersistedStreamPhaseAdvance {
            rows_processed,
            pages_processed,
            replay_wallet_stats_rows_processed: 0,
            replay_wallet_stats_pages_processed: 0,
            replay_sol_leg_rows_processed: 0,
            replay_sol_leg_pages_processed: 0,
            replay_sol_leg_elapsed_ms: 0,
            replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress::default(),
            replay_sol_leg_access_path: None,
            source_exhausted: false,
            phase_cursor: None,
            collect_buy_mints_cursor_token: cursor_token,
            unique_buy_mints_discovered,
            budget_exhausted_reason,
        })
    }
}
