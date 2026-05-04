    #[test]
    fn build_wallet_snapshots_uses_persisted_activity_days_for_eligibility() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("persisted-activity-days.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let wallet_id = "wallet_active_days";
        store.upsert_wallet_activity_days(&[
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: (now - Duration::days(3)).date_naive(),
                last_seen: now - Duration::days(3),
            },
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: (now - Duration::days(2)).date_naive(),
                last_seen: now - Duration::days(2),
            },
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: (now - Duration::days(1)).date_naive(),
                last_seen: now - Duration::days(1),
            },
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: now.date_naive(),
                last_seen: now,
            },
        ])?;

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.min_trades = 1;
        config.min_active_days = 4;
        config.min_leader_notional_sol = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.max_rug_ratio = 1.0;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());

        let swaps = VecDeque::from([swap(
            wallet_id,
            "sig-active-days-1",
            now,
            SOL_MINT,
            "TokenActiveDays1111111111111111111111111111",
            1.0,
            100.0,
        )]);
        let snapshots = discovery.build_wallet_snapshots_from_cached(&store, &swaps, now)?;
        let snapshot = snapshots.into_iter().next().context("expected snapshot")?;

        assert!(
            snapshot.eligible,
            "persisted day-level activity should satisfy min_active_days even when the capped tail only contains one day"
        );
        Ok(())
    }

    #[test]
    fn discovery_wallet_activity_day_count_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(discovery_wallet_activity_day_count_error_requires_abort(
            &error
        ));
    }

    #[test]
    fn discovery_wallet_activity_day_count_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!discovery_wallet_activity_day_count_error_requires_abort(
            &error
        ));
    }

    #[test]
    fn run_cycle_uses_existing_persisted_activity_days_for_eligibility() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-eligibility.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let wallet_id = "wallet_backfill";
        store.upsert_wallet_activity_days(&[
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: (now - Duration::days(6)).date_naive(),
                last_seen: now - Duration::days(6),
            },
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: (now - Duration::days(4)).date_naive(),
                last_seen: now - Duration::days(4),
            },
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: (now - Duration::days(2)).date_naive(),
                last_seen: now - Duration::days(2),
            },
            WalletActivityDayRow {
                wallet_id: wallet_id.to_string(),
                activity_day: now.date_naive(),
                last_seen: now,
            },
        ])?;

        store.insert_observed_swap(&swap(
            wallet_id,
            "backfill-eligibility-0",
            now,
            SOL_MINT,
            "TokenBackfillElig11111111111111111111111111",
            1.0,
            100.0,
        ))?;

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.min_trades = 1;
        config.min_active_days = 4;
        config.min_leader_notional_sol = 0.1;
        config.min_buy_count = 1;
        config.min_score = 0.0;
        config.min_tradable_ratio = 0.0;
        config.max_rug_ratio = 1.0;
        config.max_fetch_swaps_per_cycle = 1;
        config.max_fetch_pages_per_cycle = 1;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());

        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.eligible_wallets, 1);

        let counts = store
            .wallet_active_day_counts_since(&[wallet_id.to_string()], now - Duration::days(7))?;
        assert_eq!(
            counts.get(wallet_id),
            Some(&4),
            "persisted wallet_activity_days should satisfy eligibility even when the in-memory tail remains short"
        );
        Ok(())
    }

    #[test]
    fn run_cycle_persists_in_band_wallet_freshness_capture_without_standalone_raw_rebuild(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage3-in-band-capture.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-25T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.scoring_aggregates_enabled = true;
        config.scoring_aggregates_write_enabled = true;
        seed_runtime_aggregate_ready_wallet(
            &store,
            &config,
            "wallet_aggregate_only",
            "TokenAggregateOnlyStage31111111111111111",
            now,
        )?;
        seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;
        insert_recent_profitable_pair(&store, "wallet_top", now, "stage3-in-band-capture-current")?;
        insert_shadow_recorded_signal(
            &store,
            "wallet_top",
            "TokenStage3Shadow1111111111111111111111111",
            now - Duration::seconds(30),
            "shadow:stage3:wallet-top",
        )?;

        reset_current_raw_truth_sample_call_count_for_tests();
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(summary.wallet_freshness_capture_state, Some("persisted"));
        assert_eq!(summary.wallet_freshness_capture_reason, None);
        assert_eq!(current_raw_truth_sample_call_count_for_tests(), 0);

        let captures = store.list_discovery_wallet_freshness_captures(5)?;
        assert_eq!(captures.len(), 1);
        assert_eq!(
            summary.wallet_freshness_capture_id,
            Some(captures[0].capture_id)
        );
        let capture = wallet_freshness_capture_from_row(captures[0].clone())?;
        assert_eq!(capture.audit.verdict.as_str(), "fresh_current");
        assert_eq!(
            capture.audit.published_wallet_ids,
            vec!["wallet_top".to_string()]
        );
        assert_eq!(
            capture.audit.active_follow_wallet_ids,
            vec!["wallet_top".to_string()]
        );
        assert_eq!(
            capture.audit.current_raw_top_wallet_ids,
            vec!["wallet_top".to_string()]
        );
        assert!(
            !capture
                .audit
                .current_raw_top_wallet_ids
                .contains(&"wallet_aggregate_only".to_string()),
            "in-band capture must stay anchored to raw-window truth even when aggregate state exists"
        );
        assert_eq!(
            capture
                .shadow_signal
                .selected_wallets_with_recent_shadow_signal,
            1
        );
        Ok(())
    }

    #[test]
    fn run_cycle_capture_persistence_failure_is_fail_open_for_publication() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage3-in-band-capture-fail-open.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-25T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;
        insert_recent_profitable_pair(
            &store,
            "wallet_top",
            now,
            "stage3-in-band-capture-fail-open-current",
        )?;
        insert_shadow_recorded_signal(
            &store,
            "wallet_top",
            "TokenStage3ShadowFailOpen1111111111111111111",
            now - Duration::seconds(30),
            "shadow:stage3:wallet-top-fail-open",
        )?;

        assert!(store
            .list_discovery_wallet_freshness_captures(1)?
            .is_empty());
        let conn = Connection::open(Path::new(&db_path))?;
        conn.execute_batch(
            "CREATE TRIGGER fail_discovery_wallet_freshness_capture_insert
             BEFORE INSERT ON discovery_wallet_freshness_history
             BEGIN
                 SELECT RAISE(FAIL, 'simulated wallet freshness capture persistence failure');
             END;",
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(
            summary.wallet_freshness_capture_state,
            Some("persistence_failed")
        );
        assert!(summary
            .wallet_freshness_capture_reason
            .as_deref()
            .is_some_and(
                |reason| reason.contains("simulated wallet freshness capture persistence failure")
            ));
        assert_eq!(store.list_discovery_wallet_freshness_captures(5)?.len(), 0);
        assert!(
            discovery
                .recent_published_follow_universe_wallets(&store, now)?
                .is_some_and(|wallets| wallets.contains("wallet_top")),
            "capture persistence failure must not block exact publication truth"
        );
        assert!(
            store.list_active_follow_wallets()?.contains("wallet_top"),
            "capture persistence failure must not block active follow updates"
        );
        Ok(())
    }

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
