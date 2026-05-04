    #[test]
    fn run_cycle_returns_error_on_fatal_runtime_cursor_write_failure() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-discovery-runtime-cursor-fatal.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        assert_eq!(store.load_discovery_runtime_cursor()?, None);

        let now = DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        store.insert_observed_swap(&swap(
            "wallet_cursor_fatal",
            "cursor-fatal-sig-001",
            now - Duration::minutes(5),
            SOL_MINT,
            "TokenCursorFatal111111111111111111111111",
            1.0,
            100.0,
        ))?;

        let conn = Connection::open(Path::new(&db_path))?;
        conn.execute_batch(
            "CREATE TRIGGER fail_discovery_runtime_cursor_insert
             BEFORE INSERT ON discovery_runtime_state
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.max_window_swaps_in_memory = 100;
        config.max_fetch_swaps_per_cycle = 10;
        config.max_fetch_pages_per_cycle = 1;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());

        let error = discovery
            .run_cycle(&store, now)
            .expect_err("fatal discovery cursor persist must propagate");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains("failed persisting discovery runtime cursor with fatal sqlite I/O"),
            "expected fatal discovery cursor context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed updating discovery runtime cursor"),
            "expected sqlite cursor upsert context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert_eq!(
            store.load_discovery_runtime_cursor()?,
            None,
            "fatal cursor persist failure must leave runtime cursor unset"
        );
        Ok(())
    }

    #[test]
    fn discovery_runtime_cursor_load_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(discovery_runtime_cursor_load_error_requires_abort(&error));
    }

    #[test]
    fn discovery_runtime_cursor_load_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!discovery_runtime_cursor_load_error_requires_abort(&error));
    }

    #[test]
    fn discovery_recent_window_load_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(discovery_recent_window_load_error_requires_abort(&error));
    }

    #[test]
    fn discovery_recent_window_load_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!discovery_recent_window_load_error_requires_abort(&error));
    }

    fn catch_up_test_telemetry(
        phase: DiscoveryPersistedRebuildPhase,
        mode: CollectBuyMintsMode,
        replay_wallet_stats_complete: bool,
        budget_exhausted_reason: Option<PersistedStreamBudgetExhaustedReason>,
    ) -> PersistedStreamProgressTelemetry {
        let now = DateTime::parse_from_rfc3339("2026-03-19T18:30:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        PersistedStreamProgressTelemetry {
            phase,
            collect_buy_mints_mode: mode,
            replay_mode: ReplayMode::WalletStatsThenSolLeg,
            replay_subphase: DiscoveryService::replay_subphase(
                phase,
                replay_wallet_stats_complete,
                false,
            ),
            window_start: now - Duration::days(2),
            horizon_end: now,
            metrics_window_start: now,
            phase_cursor: None,
            replay_wallet_stats_wallet_cursor: None,
            collect_buy_mints_cursor_token: None,
            collect_buy_mints_reconcile_source_window_start: None,
            collect_buy_mints_reconcile_source_horizon_end: None,
            collect_buy_mints_reconcile_expired_head_cursor: None,
            collect_buy_mints_reconcile_new_tail_cursor: None,
            collect_buy_mints_reconcile_expired_head_cursor_token: None,
            collect_buy_mints_reconcile_new_tail_cursor_token: None,
            collect_buy_mints_reconcile_expired_head_pending_mints: 0,
            collect_buy_mints_reconcile_new_tail_slice_end_token: None,
            collect_buy_mints_reconcile_new_tail_pending_mints: 0,
            prepass_rows_processed: 0,
            prepass_pages_processed: 0,
            replay_wallet_stats_complete,
            replay_wallet_stats_rows_processed: 0,
            replay_wallet_stats_pages_processed: 0,
            replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress::default(),
            replay_wallet_stats_budget_floor_wallets: 0,
            replay_wallet_stats_last_partial_cycle_pages_processed: 0,
            replay_wallet_stats_last_partial_cycle_wallets_processed: 0,
            replay_wallet_stats_last_partial_cycle_elapsed_ms: 0,
            replay_wallet_stats_publishable_horizon_remaining_ms: None,
            replay_wallet_stats_milestone_reached: false,
            replay_sol_leg_reentry_pending: false,
            replay_sol_leg_last_partial_cycle_pages_processed: 0,
            replay_sol_leg_last_partial_cycle_rows_processed: 0,
            replay_sol_leg_last_partial_cycle_elapsed_ms: 0,
            replay_sol_leg_budget_floor_pages: 0,
            replay_sol_leg_publishable_horizon_remaining_ms: None,
            replay_sol_leg_retained_contract_floor_pages: 0,
            replay_candidate_activity_backfill_required: false,
            replay_candidate_activity_backfill_wallet_cursor: None,
            replay_sol_leg_access_path: None,
            replay_rows_processed: 0,
            replay_pages_processed: 0,
            chunks_completed: 0,
            cycle_rows_processed: 0,
            cycle_pages_processed: 0,
            cycle_replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress::default(),
            cycle_replay_wallet_stats_wallet_cursor_before: None,
            cycle_replay_wallet_stats_wallet_cursor_after: None,
            cycle_unique_buy_mints_discovered: 0,
            observed_swaps_loaded: 0,
            unique_buy_mints: 0,
            quality_next_mint_index: 0,
            quality_rpc_attempted: 0,
            quality_rpc_spent_ms: 0,
            wallets_buffered: 0,
            publish_pending_requested_wallet_count: 0,
            started_at: now,
            cycle_elapsed_ms: 0,
            total_elapsed_ms: 0,
            partial: true,
            completed: false,
            budget_exhausted_reason,
        }
    }

    #[test]
    fn replay_wallet_stats_catch_up_page_limit_scales_to_live_fetch_width() {
        assert_eq!(
            DiscoveryService::replay_wallet_stats_catch_up_page_limit(1, 1),
            2
        );
        assert_eq!(
            DiscoveryService::replay_wallet_stats_catch_up_page_limit(20_000, 5),
            23,
            "live fetch width should widen the replay wallet-stats page budget beyond the old fixed 2x page multiplier"
        );
    }

    #[test]
    fn replay_wallet_stats_repair_phase_page_limit_scales_to_live_repair_budget() {
        let mut config = stage1_runtime_config();
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        assert_eq!(
            discovery.replay_wallet_stats_repair_phase_page_limit(
                20_000,
                5,
                StdDuration::from_secs(60),
            ),
            92,
            "runtime-window-complete stale publication-truth repair should widen the live wallet-stats replay page budget in proportion to the longer 60s repair contract"
        );
    }

    #[test]
    fn collect_buy_mints_catch_up_page_limit_scales_to_live_fetch_width() {
        assert_eq!(
            DiscoveryService::collect_buy_mints_catch_up_page_limit(1, 1),
            2
        );
        assert_eq!(
            DiscoveryService::collect_buy_mints_catch_up_page_limit(20_000, 5),
            40,
            "live fetch width should widen the collect_buy_mints page budget beyond the fixed 2x page multiplier once grouped fresh-scan pages are capped"
        );
    }

    #[test]
    fn collect_buy_mints_repair_phase_page_limit_scales_to_live_repair_budget() {
        let mut config = stage1_runtime_config();
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        assert_eq!(
            discovery.collect_buy_mints_repair_phase_page_limit(
                20_000,
                5,
                StdDuration::from_secs(60),
            ),
            160,
            "runtime-window-complete stale publication-truth repair should widen the collect_buy_mints page budget in proportion to the longer 60s repair contract"
        );
    }

    #[test]
    fn persisted_stream_catch_up_request_targets_collect_buy_mints_and_replay_phases() {
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::ReconcileExpiredHead,
                false,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget),
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::ReconcileNewTail,
                false,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget),
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::FreshScan,
                false,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            )
        ));
        assert!(!should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::ReconcileExpiredHead,
                false,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            )
        ));
        assert!(!should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::ReconcileNewTail,
                false,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                false,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                false,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget),
            )
        ));
        assert!(!should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                true,
                None,
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                true,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget),
            )
        ));
        assert!(!should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                false,
                None,
            )
        ));
    }

    #[test]
    fn persisted_stream_catch_up_pressure_override_targets_priority_recovery_phases() {
        let mut replay_sol_leg = catch_up_test_telemetry(
            DiscoveryPersistedRebuildPhase::Replay,
            CollectBuyMintsMode::FreshScan,
            true,
            Some(PersistedStreamBudgetExhaustedReason::PageBudget),
        );
        replay_sol_leg.replay_subphase = Some("sol_leg");
        assert!(should_request_persisted_stream_catch_up_pressure_override(
            &replay_sol_leg
        ));

        let mut replay_wallet_stats = catch_up_test_telemetry(
            DiscoveryPersistedRebuildPhase::Replay,
            CollectBuyMintsMode::FreshScan,
            false,
            Some(PersistedStreamBudgetExhaustedReason::PageBudget),
        );
        replay_wallet_stats.replay_subphase = Some("wallet_stats");
        assert!(!should_request_persisted_stream_catch_up_pressure_override(
            &replay_wallet_stats
        ));

        replay_wallet_stats.budget_exhausted_reason =
            Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
        replay_wallet_stats.replay_wallet_stats_wallet_cursor =
            Some("wallet-stats-cursor".to_string());
        replay_wallet_stats.cycle_pages_processed = 54;
        replay_wallet_stats.cycle_rows_processed = 1_094_540;
        replay_wallet_stats.wallets_buffered = 500_400;
        assert!(should_request_persisted_stream_catch_up_pressure_override(
            &replay_wallet_stats
        ));

        replay_wallet_stats.replay_wallet_stats_complete = true;
        assert!(!should_request_persisted_stream_catch_up_pressure_override(
            &replay_wallet_stats
        ));

        let mut collect_buy_mints = catch_up_test_telemetry(
            DiscoveryPersistedRebuildPhase::CollectBuyMints,
            CollectBuyMintsMode::FreshScan,
            false,
            Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
        );
        collect_buy_mints.collect_buy_mints_cursor_token = Some("fresh-scan-cursor".to_string());
        collect_buy_mints.cycle_pages_processed = 2;
        collect_buy_mints.cycle_rows_processed = 518;
        collect_buy_mints.cycle_unique_buy_mints_discovered = 518;
        assert!(should_request_persisted_stream_catch_up_pressure_override(
            &collect_buy_mints
        ));

        collect_buy_mints.cycle_unique_buy_mints_discovered = 0;
        assert!(!should_request_persisted_stream_catch_up_pressure_override(
            &collect_buy_mints
        ));

        let collect_buy_mints_page_budget = catch_up_test_telemetry(
            DiscoveryPersistedRebuildPhase::CollectBuyMints,
            CollectBuyMintsMode::FreshScan,
            false,
            Some(PersistedStreamBudgetExhaustedReason::PageBudget),
        );
        assert!(!should_request_persisted_stream_catch_up_pressure_override(
            &collect_buy_mints_page_budget
        ));

        let collect_buy_mints_reconcile = catch_up_test_telemetry(
            DiscoveryPersistedRebuildPhase::CollectBuyMints,
            CollectBuyMintsMode::ReconcileExpiredHead,
            false,
            Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
        );
        assert!(!should_request_persisted_stream_catch_up_pressure_override(
            &collect_buy_mints_reconcile
        ));
    }

    fn aggregate_write_config(config: &DiscoveryConfig) -> DiscoveryAggregateWriteConfig {
        DiscoveryAggregateWriteConfig {
            max_tx_per_minute: config.max_tx_per_minute,
            rug_lookahead_seconds: config.rug_lookahead_seconds as u32,
            helius_http_url: None,
            min_token_age_hint_seconds: None,
        }
    }

    fn insert_shadow_recorded_signal(
        store: &SqliteStore,
        wallet_id: &str,
        token: &str,
        ts: DateTime<Utc>,
        signal_id: &str,
    ) -> Result<()> {
        store.insert_copy_signal(&CopySignalRow {
            signal_id: signal_id.to_string(),
            wallet_id: wallet_id.to_string(),
            side: "buy".to_string(),
            token: token.to_string(),
            notional_sol: 0.2,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts,
            status: "shadow_recorded".to_string(),
        })?;
        Ok(())
    }

    fn insert_recent_tail_noise_swaps(
        store: &SqliteStore,
        start: DateTime<Utc>,
        count: usize,
        signature_prefix: &str,
    ) -> Result<()> {
        for idx in 0..count {
            let ts = start + Duration::minutes(idx as i64);
            store.insert_observed_swap(&swap(
                "wallet_tail_noise",
                &format!("{signature_prefix}-{idx}"),
                ts,
                SOL_MINT,
                "TokenStage3TailNoise1111111111111111111111",
                0.2,
                20.0,
            ))?;
        }
        Ok(())
    }

    #[test]
    fn recent_raw_promotion_ready_case_reports_ready_now_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-promotion-ready",
            SourceStateSeed::StagedCurrent,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface(
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_promotion_blocker_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_promotion_reason_class,
            RecentRawPromotionBlockerReasonClass::RecentRawPromotionReadyNow
        );
        assert!(diagnostic.recent_raw_promotion_ready_now);
        assert_eq!(diagnostic.recent_raw_staged_newer_than_promoted, Some(true));
        assert_eq!(diagnostic.recent_raw_source_outruns_staged, Some(false));
        assert!(diagnostic.recent_raw_stage3_truth_blocked_by_promotion);
        assert!(!diagnostic.recent_raw_stage3_current_fresh_healthy_evidence_possible);
        Ok(())
    }

    #[test]
    fn recent_raw_promotion_missing_staged_snapshot_reports_blocker_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-promotion-missing-staged",
            SourceStateSeed::StagedCurrent,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_promotion_blocker_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_promotion_reason_class,
            RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByMissingStagedSnapshot
        );
        assert!(diagnostic.recent_raw_promotion_blocker_observed);
        assert!(!diagnostic.recent_raw_staged_exists);
        assert_eq!(diagnostic.recent_raw_source_outruns_promoted, Some(true));
        Ok(())
    }

    #[test]
    fn recent_raw_promotion_incomplete_staged_coverage_reports_blocker_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-promotion-incomplete-staged",
            SourceStateSeed::StagedCurrent,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface(
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:01:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_promotion_blocker_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_promotion_reason_class,
            RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByIncompleteStagedCoverage
        );
        assert_eq!(diagnostic.recent_raw_source_outruns_staged, Some(true));
        assert!(!diagnostic.recent_raw_promotion_ready_now);
        Ok(())
    }
