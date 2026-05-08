#[test]
    fn wallet_freshness_history_report_reads_in_band_captures_from_run_cycle() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage3-in-band-capture-history.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let first_now = DateTime::parse_from_rfc3339("2026-03-25T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let second_now = first_now + Duration::minutes(10);
        let third_now = first_now + Duration::minutes(20);
        let config = stage1_runtime_config();
        seed_stage1_persisted_stream_runtime_fixture(&store, &config, first_now, 6, 9)?;
        insert_recent_profitable_pair(
            &store,
            "wallet_top",
            first_now,
            "stage3-in-band-history-current-1",
        )?;
        insert_shadow_recorded_signal(
            &store,
            "wallet_top",
            "TokenStage3ShadowHistory1111111111111111111",
            first_now - Duration::seconds(30),
            "shadow:stage3:wallet-top-history-1",
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_summary = discovery.run_cycle(&store, first_now)?;
        assert_eq!(
            first_summary.wallet_freshness_capture_state,
            Some("persisted")
        );

        insert_recent_tail_noise_swaps(
            &store,
            first_now + Duration::minutes(1),
            10,
            "stage3-in-band-history-2",
        )?;
        insert_recent_profitable_pair(
            &store,
            "wallet_top",
            second_now,
            "stage3-in-band-history-current-2",
        )?;
        insert_shadow_recorded_signal(
            &store,
            "wallet_top",
            "TokenStage3ShadowHistory1111111111111111111",
            second_now - Duration::seconds(30),
            "shadow:stage3:wallet-top-history-2",
        )?;
        let second_summary = discovery.run_cycle(&store, second_now)?;
        assert_eq!(
            second_summary.wallet_freshness_capture_state,
            Some("persisted")
        );

        insert_recent_tail_noise_swaps(
            &store,
            second_now + Duration::minutes(1),
            10,
            "stage3-in-band-history-3",
        )?;
        insert_recent_profitable_pair(
            &store,
            "wallet_top",
            third_now,
            "stage3-in-band-history-current-3",
        )?;
        insert_shadow_recorded_signal(
            &store,
            "wallet_top",
            "TokenStage3ShadowHistory1111111111111111111",
            third_now - Duration::seconds(30),
            "shadow:stage3:wallet-top-history-3",
        )?;
        let third_summary = discovery.run_cycle(&store, third_now)?;
        assert_eq!(
            third_summary.wallet_freshness_capture_state,
            Some("persisted")
        );

        let report = discovery.wallet_freshness_history_report(&store, third_now, 5)?;

        assert_eq!(report.captures_loaded, 3);
        assert_eq!(report.captures_considered, 3);
        assert_eq!(report.captures_within_recent_horizon, 3);
        assert_eq!(
            report.verdict,
            WalletFreshnessHistoryVerdict::PartiallyValidatedButLowRotation
        );
        assert_eq!(report.shadow_signal_present_capture_count, 3);
        assert_eq!(report.current_raw_change_count, 0);
        assert_eq!(report.active_follow_change_count, 0);
        Ok(())
    }

    #[test]
    fn aggregate_ready_state_does_not_override_raw_window_truth_on_restart() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-ready-raw-truth-restart.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.scoring_aggregates_enabled = true;
        config.scoring_aggregates_write_enabled = true;

        seed_runtime_aggregate_ready_wallet(
            &store,
            &config,
            "wallet_raw_truth",
            "TokenAggregateReadyRawTruth111111111111111",
            now,
        )?;

        let first = DiscoveryService::new(config.clone(), permissive_shadow_quality())
            .run_cycle(&store, now)?;
        let second =
            DiscoveryService::new(config, permissive_shadow_quality()).run_cycle(&store, now)?;

        for summary in [&first, &second] {
            assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
            assert_eq!(summary.scoring_source, "raw_window");
            assert_eq!(summary.active_follow_wallets, 1);
            assert!(
                summary
                    .top_wallets
                    .iter()
                    .any(|label| label.starts_with("wallet_raw_truth:")),
                "restart must keep using raw-window truth even when aggregate state is ready"
            );
        }
        assert!(
            store
                .list_active_follow_wallets()?
                .contains("wallet_raw_truth"),
            "raw-window winner should stay active after restart"
        );
        Ok(())
    }

    #[test]
    fn aggregate_ready_state_degrades_to_recent_publication_truth_on_restart() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-ready-degraded-restart.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.follow_top_n = 1;
        config.scoring_aggregates_enabled = true;
        config.scoring_aggregates_write_enabled = true;

        seed_runtime_aggregate_ready_wallet(
            &store,
            &config,
            "wallet_aggregate_only",
            "TokenAggregateOnlyDegraded111111111111111",
            now,
        )?;
        store.delete_observed_swaps_before(now + Duration::seconds(1))?;
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let expected_active_wallets =
            seed_published_wallet_metrics_snapshot(&store, metrics_window_start, 4, 1)?;
        let discovery_for_seed = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        seed_recent_published_universe(
            &discovery_for_seed,
            &store,
            now - Duration::seconds(30),
            metrics_window_start,
            "raw_window",
            &expected_active_wallets,
        )?;

        let first = DiscoveryService::new(config.clone(), permissive_shadow_quality())
            .run_cycle(&store, now)?;
        let second =
            DiscoveryService::new(config, permissive_shadow_quality()).run_cycle(&store, now)?;

        for summary in [&first, &second] {
            assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
            assert_eq!(
                summary.scoring_source,
                "published_universe_raw_window_unavailable"
            );
            assert_eq!(summary.active_follow_wallets, expected_active_wallets.len());
        }
        assert_eq!(store.list_active_follow_wallets()?, expected_active_wallets);
        Ok(())
    }

    #[test]
    fn aggregate_ready_state_does_not_override_fail_closed_truth_on_restart() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("aggregate-ready-fail-closed-restart.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.scoring_aggregates_enabled = true;
        config.scoring_aggregates_write_enabled = true;

        seed_runtime_aggregate_ready_wallet(
            &store,
            &config,
            "wallet_aggregate_only_fail_closed",
            "TokenAggregateOnlyFailClosed1111111111111",
            now,
        )?;
        store.delete_observed_swaps_before(now + Duration::seconds(1))?;
        store.activate_follow_wallet(
            "wallet_stale_follow",
            now - Duration::minutes(1),
            "seed-follow",
        )?;

        let first = DiscoveryService::new(config.clone(), permissive_shadow_quality())
            .run_cycle(&store, now)?;
        let second =
            DiscoveryService::new(config, permissive_shadow_quality()).run_cycle(&store, now)?;

        for summary in [&first, &second] {
            assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
            assert_eq!(
                summary.scoring_source,
                "raw_window_unusable_no_recent_published_universe"
            );
            assert_eq!(summary.active_follow_wallets, 0);
        }
        assert!(store.list_active_follow_wallets()?.is_empty());
        Ok(())
    }

    #[test]
    fn discovery_runtime_cursor_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(discovery_runtime_cursor_error_requires_abort(&error));
    }

    #[test]
    fn discovery_runtime_cursor_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!discovery_runtime_cursor_error_requires_abort(&error));
    }
