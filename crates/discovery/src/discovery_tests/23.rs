    #[test]
    fn helper_live_like_missing_exact_target_surface_region_trace_preserves_strict_parent_child_order_stage1(
    ) -> Result<()> {
        let (_temp, runtime_store, journal_store, discovery, now, _, _) =
            seed_stage1_deferred_runtime_publication_refresh_missing_exact_target_surface_fixture(
            )?;

        let (_repair, events) = traced_runtime_publication_truth_repair_helper(
            &discovery,
            &runtime_store,
            Some(&journal_store),
            now,
            Instant::now() + StdDuration::from_secs(60),
            Some(Instant::now()),
            true,
        )?;

        let helper_enter = events
            .iter()
            .position(|event| {
                event.region
                    == "repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed_with_options"
                    && event.state == "entered"
            })
            .expect("helper region enter should be traced");
        let load_enter = events
            .iter()
            .position(|event| {
                event.region == "load_or_start_persisted_stream_rebuild_state_with_options"
                    && event.state == "entered"
            })
            .expect("load/start region enter should be traced");
        let restore_enter = events
            .iter()
            .position(|event| {
                event.region == "repair_restored_persisted_stream_state_for_resume"
                    && event.state == "entered"
            })
            .expect("resume-state repair region enter should be traced");
        let restore_exit = events
            .iter()
            .position(|event| {
                event.region == "repair_restored_persisted_stream_state_for_resume"
                    && event.state == "exited"
            })
            .expect("resume-state repair region exit should be traced");
        let load_exit = events
            .iter()
            .position(|event| {
                event.region == "load_or_start_persisted_stream_rebuild_state_with_options"
                    && event.state == "exited"
            })
            .expect("load/start region exit should be traced");

        assert!(helper_enter < load_enter);
        assert!(load_enter < restore_enter);
        assert!(restore_enter < restore_exit);
        assert!(restore_exit < load_exit);
        assert_eq!(
            events[load_enter].parent_region,
            Some("repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed_with_options")
        );
        assert_eq!(
            events[restore_enter].parent_region,
            Some("load_or_start_persisted_stream_rebuild_state_with_options")
        );
        assert!(
            !events.iter().any(|event| {
                event.region == "repair_replay_exact_target_buy_mint_surface_for_resume"
            }),
            "the fixed helper path should now exit load/start without descending into the wallet_activity_scan child region"
        );
        Ok(())
    }

    #[test]
    fn old_like_helper_live_like_missing_exact_target_surface_region_trace_surfaces_wallet_scan_progress_and_deadline_accounting_stage1(
    ) -> Result<()> {
        let (_temp, runtime_store, journal_store, discovery, now, _, _) =
            seed_stage1_deferred_runtime_publication_refresh_missing_exact_target_surface_fixture(
            )?;

        let (repair, events) = traced_runtime_publication_truth_repair_helper(
            &discovery,
            &runtime_store,
            Some(&journal_store),
            now,
            Instant::now() + StdDuration::from_secs(60),
            Some(Instant::now()),
            false,
        )?;

        let wallet_scan_enter = events
            .iter()
            .find(|event| {
                event.region
                    == "repair_replay_exact_target_buy_mint_surface_for_resume.wallet_activity_scan"
                    && event.state == "entered"
            })
            .expect("wallet scan region enter should be traced");
        let wallet_scan_exit = events
            .iter()
            .find(|event| {
                event.region
                    == "repair_replay_exact_target_buy_mint_surface_for_resume.wallet_activity_scan"
                    && event.state == "exited"
            })
            .expect("wallet scan region exit should be traced");
        let exact_surface_exit = events
            .iter()
            .find(|event| {
                event.region == "repair_replay_exact_target_buy_mint_surface_for_resume"
                    && event.state == "exited"
            })
            .expect("exact-target surface exit should be traced");

        assert!(repair.publication_truth_refresh_helper_write_attempted);
        assert!(wallet_scan_exit.pages_scanned > 0);
        assert_eq!(wallet_scan_exit.time_budget_exhausted, Some(true));
        assert!(wallet_scan_exit.deadline_remaining_ms <= wallet_scan_enter.deadline_remaining_ms);
        assert_eq!(exact_surface_exit.time_budget_exhausted, Some(true));
        assert_eq!(
            exact_surface_exit.wallets_scanned,
            wallet_scan_exit.wallets_scanned
        );
        assert_eq!(
            exact_surface_exit.rows_scanned,
            wallet_scan_exit.rows_scanned
        );
        Ok(())
    }

    #[test]
    fn helper_live_like_missing_exact_target_surface_fixed_path_skips_wallet_activity_scan_stage1(
    ) -> Result<()> {
        let (_temp, runtime_store, journal_store, discovery, now, _, _) =
            seed_stage1_deferred_runtime_publication_refresh_missing_exact_target_surface_fixture(
            )?;

        let (repair, events) = traced_runtime_publication_truth_repair_helper(
            &discovery,
            &runtime_store,
            Some(&journal_store),
            now,
            Instant::now() + StdDuration::from_secs(60),
            Some(Instant::now()),
            true,
        )?;

        assert!(
            !events.iter().any(|event| {
                event.region
                    == "repair_replay_exact_target_buy_mint_surface_for_resume.wallet_activity_scan"
            }),
            "the fixed helper path should now skip the exact-wallet scan entirely on the proven live seam"
        );
        assert!(repair.publication_truth_refresh_helper_write_attempted);
        assert!(repair.publication_truth_refresh_helper_write_succeeded);
        assert!(!repair.publication_truth_refresh_resume_exact_target_surface_repair_attempted);
        Ok(())
    }

    #[test]
    fn deferred_helper_exact_target_surface_scan_is_later_owned_by_normal_replay_resume_path_stage1(
    ) -> Result<()> {
        let (
            _temp,
            runtime_store,
            journal_store,
            discovery,
            now,
            _stale_last_published_at,
            _stale_wallet_ids,
        ) = seed_stage1_deferred_runtime_publication_refresh_missing_exact_target_surface_fixture(
        )?;
        let required_window_start = now - Duration::days(discovery.runtime_scoring_window_days());
        let metrics_window_start = discovery.metrics_window_start(now);

        let repair = discovery
            .repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed_with_options(
                &runtime_store,
                Some(&journal_store),
                now,
                64,
                Instant::now() + StdDuration::from_secs(60),
                Some(Instant::now()),
                None,
                true,
            )?;
        assert!(repair.publication_truth_refresh_helper_write_attempted);
        assert!(!repair.publication_truth_refresh_resume_exact_target_surface_repair_attempted);
        let after_helper = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert!(
            after_helper.payload.discovery_critical_target_buy_mints.is_empty(),
            "helper deferral must leave exact-target ownership unresolved for the later replay path"
        );

        let (resumed, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &runtime_store,
            required_window_start,
            metrics_window_start,
            now,
        )?;
        assert!(matches!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
                | PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
        ));
        assert!(
            !resumed.payload.discovery_critical_target_buy_mints.is_empty(),
            "normal replay resume must still own and complete exact-target surface repair after the helper defers it"
        );
        Ok(())
    }

    #[test]
    fn repair_runtime_window_complete_stale_publication_truth_refreshes_before_publish_due_cycle(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-publication-repair-runtime-window-complete.db");
        let journal_db_path = temp
            .path()
            .join("stage1-publication-repair-runtime-window-complete-journal.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let mut journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;
        journal_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-01T16:18:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (_window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&runtime_store, &config, now, 6, 9)?;
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_incomplete_no_recent_published_universe".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        let repair = discovery
            .repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed(
                &runtime_store,
                None,
                now,
                64,
                Instant::now() + StdDuration::from_secs(1),
            )?;
        assert_ne!(
            repair.state, "skipped_runtime_window_complete",
            "runtime-window-complete repair must not skip stale or incomplete publication truth"
        );
        assert_eq!(
            repair.state,
            "deferred_runtime_window_truth_refresh_to_run_cycle"
        );
        assert!(
            !repair.publication_truth_refresh_attempted,
            "runtime-window-complete repair should now delegate the persisted rebuild to the owning run_cycle instead of duplicating that work before the cycle starts"
        );
        assert!(repair.publication_truth_refresh_delegated_to_runtime_cycle);
        assert!(
            repair.publication_truth_refresh_phase.is_some(),
            "repair logs must expose which persisted-truth rebuild phase will be resumed by run_cycle"
        );

        let summary = discovery.run_cycle(&runtime_store, now)?;
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert!(
            summary.published,
            "the first publish-due cycle after repair should refresh publication truth instead of preserving the stale fail-closed surface"
        );
        let publication_state = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist after refreshed cycle");
        assert_eq!(
            publication_state.runtime_mode,
            DiscoveryRuntimeMode::Healthy
        );
        assert_eq!(publication_state.last_published_at, Some(now));
        assert_eq!(
            publication_state.last_published_window_start,
            Some(metrics_window_start)
        );
        assert!(
            publication_state
                .published_wallet_ids
                .as_ref()
                .is_some_and(|wallets| !wallets.is_empty()),
            "refreshed publication truth must repopulate published wallet ids before runtime export can pass"
        );
        Ok(())
    }

    #[test]
    fn recommended_publication_truth_repair_time_budget_extends_runtime_window_complete_stale_truth_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-publication-repair-budget-runtime-window-complete.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-01T19:52:20Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (_window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&runtime_store, &config, now, 6, 9)?;
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_incomplete_no_recent_published_universe".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        assert_eq!(
            discovery.recommended_publication_truth_repair_time_budget(&runtime_store, now)?,
            StdDuration::from_millis(
                DISCOVERY_PUBLICATION_TRUTH_REPAIR_RUNTIME_WINDOW_REFRESH_MIN_TIME_BUDGET_MS
            ),
            "runtime-window-complete but stale publication truth should get the longer repair budget instead of the old 10s micro-burst that kept wallet-stats replay in partial refresh"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_priority_recovery_contract_extends_live_fetch_contract_for_stale_publication_truth_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-priority-recovery-contract-runtime-window-complete.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-02T13:23:26Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (_window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&runtime_store, &config, now, 6, 9)?;
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_incomplete_no_recent_published_universe".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        let contract = discovery.persisted_stream_priority_recovery_contract(
            &runtime_store,
            now,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            StdDuration::from_millis(config.fetch_time_budget_ms),
        )?;
        assert_eq!(contract.time_budget, StdDuration::from_secs(60));
        assert_eq!(
            contract.collect_buy_mints_phase_page_limit_override,
            Some(160)
        );
        assert_eq!(
            contract.replay_wallet_stats_phase_page_limit_override,
            Some(92)
        );
        assert_eq!(
            contract.reason,
            Some("runtime_window_complete_stale_publication_truth")
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_priority_recovery_contract_deepens_resumed_replay_wallet_stats_checkpoint_stage1(
    ) -> Result<()> {
        let now = DateTime::parse_from_rfc3339("2026-04-02T14:41:24Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let mut state = discovery.start_persisted_stream_rebuild_state(
            now - Duration::days(config.scoring_window_days.max(1) as i64),
            metrics_window_start_for_test(&config, now),
            now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.horizon_end = now;
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.unique_buy_mints =
            vec!["TokenReplayDeep11111111111111111111111111".to_string()];
        state.payload.buy_mint_counts = state
            .payload
            .unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1u32))
            .collect();
        state.payload.token_quality_progress.next_mint_index = state.payload.unique_buy_mints.len();
        state.payload.replay_wallet_stats_rows_processed = 2_426_547;
        state.payload.replay_wallet_stats_pages_processed = 121;
        state.payload.replay_wallet_stats_wallet_cursor =
            Some("4PLFdk7JXLFUxuFkJjms4PbVtUTgVhc1FaYwk1pHzu6d".to_string());
        state.payload.by_wallet.insert(
            "wallet_live_like_partial".to_string(),
            WalletAccumulator {
                trades: 4,
                ..WalletAccumulator::default()
            },
        );

        let base_contract = PersistedStreamPriorityRecoveryContract {
            time_budget: StdDuration::from_secs(60),
            collect_buy_mints_phase_page_limit_override: Some(160),
            replay_wallet_stats_phase_page_limit_override: Some(92),
            replay_sol_leg_phase_page_limit_override: None,
            reason: Some("runtime_window_complete_stale_publication_truth"),
        };
        let deep_contract = discovery.deepen_persisted_stream_priority_recovery_contract_for_state(
            &state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            base_contract,
        );

        assert_eq!(deep_contract.time_budget, StdDuration::from_millis(409_500));
        assert_eq!(
            deep_contract.collect_buy_mints_phase_page_limit_override,
            Some(1_120)
        );
        assert_eq!(
            deep_contract.replay_wallet_stats_phase_page_limit_override,
            Some(644)
        );
        assert_eq!(
            deep_contract.reason,
            Some("deep_replay_wallet_stats_large_processed_backlog")
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_priority_recovery_contract_scales_for_large_resumed_replay_wallet_stats_backlog_stage1(
    ) {
        let now = DateTime::parse_from_rfc3339("2026-04-02T17:01:12Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let mut state = discovery.start_persisted_stream_rebuild_state(
            now - Duration::days(config.scoring_window_days.max(1) as i64),
            metrics_window_start_for_test(&config, now),
            now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.horizon_end = now;
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.unique_buy_mints =
            vec!["TokenReplayDeep11111111111111111111111111".to_string()];
        state.payload.token_quality_progress.next_mint_index = state.payload.unique_buy_mints.len();
        state.payload.replay_wallet_stats_rows_processed = 7_932_777;
        state.payload.replay_wallet_stats_pages_processed = 276;
        state.payload.replay_wallet_stats_wallet_cursor =
            Some("AQkMMyk5ZjkALFF3pvYXKnD37vXPS8QZ2D8UwRxpqZJL".to_string());
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .fast_path_wallets_processed = 367_200;

        let base_contract = PersistedStreamPriorityRecoveryContract {
            time_budget: StdDuration::from_secs(60),
            collect_buy_mints_phase_page_limit_override: Some(160),
            replay_wallet_stats_phase_page_limit_override: Some(92),
            replay_sol_leg_phase_page_limit_override: None,
            reason: Some("runtime_window_complete_stale_publication_truth"),
        };
        let deep_contract = discovery.deepen_persisted_stream_priority_recovery_contract_for_state(
            &state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            base_contract,
        );

        assert_eq!(
            deep_contract.time_budget,
            StdDuration::from_millis(1_377_000)
        );
        assert!(
            deep_contract
                .replay_wallet_stats_phase_page_limit_override
                .is_some_and(|limit| limit > 276),
            "large live-like replay backlogs should widen the wallet-stats phase ceiling beyond the fixed 180s contract"
        );
        assert!(
            deep_contract
                .collect_buy_mints_phase_page_limit_override
                .is_some_and(|limit| limit > 480),
            "once the deep replay contract is stretched for a large buffered wallet backlog, the shared recovery contract should carry the same widened time budget through its other phase ceilings"
        );
        assert_eq!(
            deep_contract.reason,
            Some("deep_replay_wallet_stats_large_buffered_backlog")
        );
    }
