    #[test]
    fn repair_recent_raw_journal_head_gap_refuses_foreign_runtime_cursor_lineage() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-publication-repair-foreign-runtime.db");
        let journal_db_path = temp
            .path()
            .join("stage1-publication-repair-foreign-journal.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let mut journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;
        journal_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-01T14:52:24Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = discovery.metrics_window_start(now);

        let journal_swaps = vec![
            swap(
                "wallet_top",
                "foreign-repair-window-floor",
                window_start,
                SOL_MINT,
                "TokenStage1ForeignRepair111111111111111111",
                0.8,
                80.0,
            ),
            swap(
                "wallet_top",
                "foreign-repair-top-buy",
                window_start + Duration::minutes(1),
                SOL_MINT,
                "TokenStage1ForeignRepair111111111111111111",
                1.0,
                100.0,
            ),
            swap(
                "wallet_top",
                "foreign-repair-top-sell",
                window_start + Duration::minutes(6),
                "TokenStage1ForeignRepair111111111111111111",
                SOL_MINT,
                100.0,
                1.3,
            ),
        ];
        journal_store.insert_recent_raw_journal_batch(&journal_swaps, now)?;

        let runtime_swap = swap(
            "wallet_tail_noise",
            "foreign-runtime-tail",
            now,
            SOL_MINT,
            "TokenStage1ForeignRuntimeTail11111111111111",
            0.2,
            20.0,
        );
        runtime_store.insert_observed_swap(&runtime_swap)?;
        runtime_store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: runtime_swap.ts_utc + Duration::minutes(1),
            slot: runtime_swap.slot.saturating_add(1),
            signature: "foreign-runtime-cursor-after-journal".to_string(),
        })?;
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
                Some(&journal_store),
                now,
                64,
                Instant::now() + StdDuration::from_secs(1),
            )?;
        assert_eq!(repair.state, "skipped_journal_cursor_lineage_mismatch");
        assert_eq!(
            repair.reason.as_deref(),
            Some("recent_raw_journal_does_not_cover_runtime_cursor")
        );
        assert!(
            !discovery.persisted_observed_swaps_cover_window(&runtime_store, window_start)?,
            "foreign journal lineage must not be used to backfill the missing runtime raw window"
        );
        let publication_state = runtime_store
            .discovery_publication_state()?
            .expect("publication state should still exist");
        assert_eq!(
            publication_state.runtime_mode,
            DiscoveryRuntimeMode::FailClosed
        );
        assert!(
            !publication_state.has_complete_publication_truth(),
            "foreign journal lineage must not fake complete publication truth"
        );
        Ok(())
    }

    #[test]
    fn cold_start_incomplete_raw_window_with_recent_published_universe_enters_degraded_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-cold-start-degraded.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:05:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..9 {
            let ts = now - Duration::minutes(9) + Duration::minutes(idx as i64);
            let swap = swap(
                "wallet_noise",
                &format!("stage1-degraded-noise-{idx}"),
                ts,
                SOL_MINT,
                "TokenStage1DegradedNoise1111111111111111",
                0.2,
                20.0,
            );
            latest_cursor = Some(DiscoveryRuntimeCursor {
                ts_utc: swap.ts_utc,
                slot: swap.slot,
                signature: swap.signature.clone(),
            });
            store.insert_observed_swap(&swap)?;
        }
        let mut config = stage1_runtime_config();
        config.follow_top_n = 15;
        let metrics_window_start = {
            let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
            let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
            let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
            bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
        };
        let expected_active_wallets =
            seed_published_wallet_metrics_snapshot(&store, metrics_window_start, 80, 15)?;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        seed_recent_published_universe(
            &discovery,
            &store,
            now - Duration::seconds(30),
            metrics_window_start,
            "raw_window",
            &expected_active_wallets,
        )?;
        store.upsert_discovery_runtime_cursor(
            &latest_cursor.expect("latest cursor should be present"),
        )?;
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
        assert!(!summary.trusted_selection_fail_closed);
        assert!(summary.raw_window_cap_truncated);
        assert_eq!(
            summary.scoring_source,
            "published_universe_raw_window_degraded"
        );
        assert_eq!(summary.eligible_wallets, 80);
        assert_eq!(summary.active_follow_wallets, 15);
        assert_eq!(summary.follow_promoted, 0);
        assert_eq!(summary.follow_demoted, 0);
        let active_wallets = store.list_active_follow_wallets()?;
        assert_eq!(active_wallets, expected_active_wallets);
        Ok(())
    }

    #[test]
    fn cold_start_truncated_in_memory_with_incomplete_persisted_observed_swaps_and_no_recent_published_universe_fail_closes_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-cold-start-truncated-incomplete-fail-close.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:08:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..9 {
            let ts = now - Duration::minutes(9) + Duration::minutes(idx as i64);
            let swap = swap(
                "wallet_noise",
                &format!("stage1-fail-close-noise-{idx}"),
                ts,
                SOL_MINT,
                "TokenStage1FailCloseNoise111111111111111",
                0.2,
                20.0,
            );
            latest_cursor = Some(DiscoveryRuntimeCursor {
                ts_utc: swap.ts_utc,
                slot: swap.slot,
                signature: swap.signature.clone(),
            });
            store.insert_observed_swap(&swap)?;
        }
        store.activate_follow_wallet("wallet_stale", now - Duration::minutes(1), "seed-follow")?;
        store.upsert_discovery_runtime_cursor(
            &latest_cursor.expect("latest cursor should be present"),
        )?;

        let discovery = DiscoveryService::new(stage1_runtime_config(), permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert!(summary.trusted_selection_fail_closed);
        assert!(summary.raw_window_cap_truncated);
        assert_eq!(
            summary.scoring_source,
            "raw_window_incomplete_no_recent_published_universe"
        );
        assert_eq!(summary.eligible_wallets, 0);
        assert_eq!(summary.follow_demoted, 1);
        assert!(store.list_active_follow_wallets()?.is_empty());
        Ok(())
    }

    #[test]
    fn cold_start_stale_persisted_history_with_recent_published_universe_enters_degraded_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-cold-start-stale-persisted-degraded.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:10:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.follow_top_n = 15;
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let stale_ts = window_start - Duration::minutes(5);
        let stale_swap = swap(
            "wallet_stale_history",
            "stage1-stale-persisted-old-swap",
            stale_ts,
            SOL_MINT,
            "TokenStage1StalePersisted111111111111111",
            0.5,
            50.0,
        );
        store.insert_observed_swap(&stale_swap)?;
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: stale_swap.ts_utc,
            slot: stale_swap.slot,
            signature: stale_swap.signature.clone(),
        })?;

        let metrics_window_start = {
            let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
            let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
            let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
            bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
        };
        let expected_active_wallets =
            seed_published_wallet_metrics_snapshot(&store, metrics_window_start, 80, 15)?;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        seed_recent_published_universe(
            &discovery,
            &store,
            now - Duration::seconds(30),
            metrics_window_start,
            "raw_window",
            &expected_active_wallets,
        )?;
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
        assert!(!summary.trusted_selection_fail_closed);
        assert_eq!(
            summary.scoring_source,
            "published_universe_raw_window_unavailable"
        );
        assert!(!summary.raw_window_cap_truncated);
        assert_eq!(summary.eligible_wallets, 80);
        assert_eq!(summary.active_follow_wallets, 15);
        assert_eq!(summary.follow_promoted, 0);
        assert_eq!(summary.follow_demoted, 0);
        assert_eq!(store.list_active_follow_wallets()?, expected_active_wallets);
        Ok(())
    }

    #[test]
    fn restart_with_recent_published_universe_replaces_stale_followlist_residue_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-restart-published-universe-replaces-stale-followlist.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:11:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.follow_top_n = 3;
        let metrics_window_start = {
            let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
            let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
            let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
            bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
        };
        let published_active_wallets =
            seed_published_wallet_metrics_snapshot(&store, metrics_window_start, 6, 3)?;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        seed_recent_published_universe(
            &discovery,
            &store,
            now - Duration::seconds(30),
            metrics_window_start,
            "raw_window",
            &published_active_wallets,
        )?;
        store.activate_follow_wallet(
            "wallet_legacy_residue",
            now - Duration::minutes(10),
            "legacy_bootstrap_residue",
        )?;
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
        assert_eq!(
            summary.scoring_source,
            "published_universe_raw_window_unavailable"
        );
        assert!(
            summary.follow_demoted >= 1,
            "degraded restart should clear stale followlist residue that does not belong to the published universe"
        );
        assert_eq!(
            summary.active_follow_wallets,
            published_active_wallets.len()
        );
        assert_eq!(
            store.list_active_follow_wallets()?,
            published_active_wallets,
            "runtime degraded restart must restore the last published universe itself, not whatever active followlist residue happened to survive restart"
        );
        Ok(())
    }

    #[test]
    fn restart_with_recent_published_universe_uses_exact_wallet_set_when_current_ranking_drifted_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-restart-published-universe-exact-wallet-set.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:11:30Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.follow_top_n = 1;
        config.min_score = 0.95;
        let metrics_window_start = {
            let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
            let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
            let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
            bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
        };

        store.upsert_wallet(
            "wallet_ranked_now",
            now - Duration::days(2),
            now - Duration::minutes(1),
            "candidate",
        )?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_ranked_now".to_string(),
            window_start: metrics_window_start,
            pnl: 3.2,
            win_rate: 0.9,
            trades: 8,
            closed_trades: 8,
            hold_median_seconds: 90,
            score: 1.2,
            buy_total: 8,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
        let expected_published_wallets = HashSet::from([
            "wallet_published_exact".to_string(),
            "wallet_published_second".to_string(),
        ]);
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        seed_recent_published_universe(
            &discovery,
            &store,
            now - Duration::seconds(30),
            metrics_window_start,
            "raw_window",
            &expected_published_wallets,
        )?;
        store.activate_follow_wallet(
            "wallet_legacy_residue",
            now - Duration::minutes(10),
            "legacy_bootstrap_residue",
        )?;
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
        assert_eq!(
            summary.scoring_source,
            "published_universe_raw_window_unavailable"
        );
        assert_eq!(
            summary.active_follow_wallets,
            expected_published_wallets.len()
        );
        assert_eq!(
            store.list_active_follow_wallets()?,
            expected_published_wallets
        );
        assert!(
            !store.list_active_follow_wallets()?.contains("wallet_ranked_now"),
            "degraded restart must read the exact published universe control plane, not reconstruct it from current wallet_metrics ranking"
        );
        Ok(())
    }
