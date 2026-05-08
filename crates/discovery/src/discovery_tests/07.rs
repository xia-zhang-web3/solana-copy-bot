    #[test]
    fn live_like_retention_position_reconstruction_restores_legit_carry_leaders_without_readmitting_refill_drain_junk_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("retention-position-reconstruction-restores-legit-carry-leaders.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-06T22:59:22Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = live_restored_rug_policy_discovery_config_for_tests();
        config.require_open_positions_for_publication = true;
        let legit_wallet_ids: Vec<String> = (0..3)
            .map(|idx| format!("wallet_legit_prewindow_carry_{idx:02}"))
            .collect();
        seed_prewindow_carry_leader_position_history(&store, &config, now, &legit_wallet_ids)?;
        let (junk_entries, stale_cluster_wallet_ids, independent_wallet_ids) =
            refill_drain_open_lot_fixture_wallets(now, false);
        assert!(independent_wallet_ids.is_empty());

        let mut by_wallet: HashMap<String, WalletAccumulator> = junk_entries.into_iter().collect();
        for wallet_id in &legit_wallet_ids {
            by_wallet.insert(
                wallet_id.clone(),
                prewindow_carry_leader_scoring_window_accumulator(now),
            );
        }

        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let snapshots = discovery.wallet_snapshots_from_accumulators(
            &store,
            by_wallet,
            now,
            &HashMap::new(),
        )?;
        let ranked = rank_follow_candidates(&snapshots, config.min_score);
        let desired = desired_wallets(&ranked, config.follow_top_n);
        assert_eq!(
            desired, legit_wallet_ids,
            "the retention-window position reconstruction should restore only the legit carry leaders whose actionable lot predates the scoring window"
        );
        for wallet_id in &legit_wallet_ids {
            let snapshot = snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("restored legit carry snapshot should exist");
            assert!(
                snapshot.eligible && snapshot.score >= config.min_score,
                "the reconstructed pre-window carry leader {wallet_id} should remain publishable once its retained carry lot is visible to the actionable gate"
            );
        }
        for wallet_id in &stale_cluster_wallet_ids {
            let snapshot = snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("junk snapshot should exist");
            assert!(
                !snapshot.eligible && snapshot.score.abs() < 1e-9,
                "retention-window position reconstruction must not re-admit the stale refill/drain wallet {wallet_id}"
            );
        }

        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let publication_outcome = discovery.persist_publication_state(
            &store,
            DiscoveryRuntimeMode::Healthy,
            true,
            metrics_window_start,
            Some(&desired),
            "raw_window_persisted_stream",
            "retention_window_actionable_open_position_reconstruction",
            now,
        )?;
        assert!(
            publication_outcome.published_universe_persisted,
            "once the legit carry leaders are restored for a deterministic retained-position reason, publication truth must materialize as exact wallet ids"
        );
        let publication_state = store
            .discovery_publication_state_read_only()?
            .expect("publication state should exist");
        assert_eq!(
            publication_state.published_wallet_ids.unwrap_or_default(),
            legit_wallet_ids,
            "publication truth must persist only the restored legit carry leaders after retained-position reconstruction"
        );
        Ok(())
    }

    #[test]
    fn live_like_fourteen_day_retention_without_nonfollowed_market_context_still_collapses_store_backed_raw_window_to_zero_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("live-like-fourteen-day-retention-missing-market-context-zero-wallets.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-07T13:02:39Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = live_restored_rug_policy_discovery_config_for_tests();
        config.require_open_positions_for_publication = true;
        config.observed_swaps_retention_days = 14;
        config.max_window_swaps_in_memory = 64;
        let (full_swaps, legit_wallet_ids, junk_wallet_ids) =
            long_horizon_carry_vs_refill_drain_fixture_swaps(&config, now);
        let partial_swaps = omit_nonfollowed_legit_market_context_swaps(&full_swaps);
        assert!(
            partial_swaps.len() < full_swaps.len(),
            "reduced repro must remove only the not-followed SOL-leg context swaps while keeping the same candidate wallets"
        );
        insert_observed_swaps_and_seed_runtime_cursor(&store, &partial_swaps)?;

        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let reconstructed_positions = discovery
            .reconstruct_retention_window_positions_for_wallets(&store, &legit_wallet_ids, now)?;
        for wallet_id in &legit_wallet_ids {
            let positions = reconstructed_positions
                .get(wallet_id)
                .cloned()
                .unwrap_or_default();
            assert!(
                !positions.is_empty(),
                "fourteen-day retention should still reconstruct the legit carry for {wallet_id}; the zero-wallet collapse must happen after retained-position reconstruction"
            );
        }

        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.scoring_source, "raw_window_persisted_stream");
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert!(summary.metrics_written > 0);
        assert_eq!(summary.eligible_wallets, 0);
        assert!(summary.top_wallets.is_empty());
        assert!(!summary.published);

        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let (snapshots, observed_swaps_loaded) = discovery
            .build_wallet_snapshots_from_persisted_stream_one_shot(&store, window_start, now)?;
        assert!(
            observed_swaps_loaded > legit_wallet_ids.len() + junk_wallet_ids.len(),
            "the reduced live-like repro must still operate on a non-trivial persisted field even though the non-followed market-context swaps are missing"
        );
        let ranked = rank_follow_candidates(&snapshots, config.min_score);
        assert!(ranked.is_empty());
        for wallet_id in &legit_wallet_ids {
            let snapshot = snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("legit carry snapshot should exist");
            assert!(
                !snapshot.eligible && snapshot.score.abs() < 1e-9,
                "legit carry leader {wallet_id} should still be eliminated on the fourteen-day path once the supporting non-followed SOL-leg market context is absent"
            );
            assert!(
                snapshot.rug_ratio > config.max_rug_ratio,
                "the missing non-followed context for {wallet_id} must manifest as a rug/thin-market failure rather than another open-position failure"
            );
        }
        for wallet_id in &junk_wallet_ids {
            let snapshot = snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("junk snapshot should exist");
            assert!(
                !snapshot.eligible && snapshot.score.abs() < 1e-9,
                "junk refill/drain wallet {wallet_id} must stay excluded even when the legit market-context swaps are missing"
            );
        }
        Ok(())
    }

    #[test]
    fn run_cycle_recovers_from_poisoned_window_mutex() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-poison.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());

        let state = discovery.window_state.clone();
        let _ = std::panic::catch_unwind(move || {
            let _guard = state.lock().expect("lock must succeed");
            panic!("poison discovery window state");
        });

        let now = Utc::now();
        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.wallets_seen, 0);
        assert_eq!(summary.metrics_written, 0);
        Ok(())
    }

    #[test]
    fn run_cycle_enforces_max_window_swaps_in_memory_cap() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-cap.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-04T11:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::minutes(30);
        for idx in 0..20 {
            let ts = start + Duration::seconds((idx * 5) as i64);
            store.insert_observed_swap(&swap(
                "wallet_cap",
                &format!("cap-sig-{idx:03}"),
                ts,
                SOL_MINT,
                "TokenCap1111111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.max_window_swaps_in_memory = 5;
        config.max_fetch_swaps_per_cycle = 100;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let _ = discovery.run_cycle(&store, now)?;

        let guard = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(
            guard.swaps.len() <= 5,
            "window swap cache must stay within configured cap"
        );
        assert!(
            guard.signatures.len() <= 5,
            "window signature cache must stay within configured cap"
        );
        Ok(())
    }

    #[test]
    fn run_cycle_uses_persisted_cursor_for_incremental_fetch_after_restart() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-cursor.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::minutes(20);
        for idx in 0..12 {
            let ts = start + Duration::seconds((idx * 10) as i64);
            store.insert_observed_swap(&swap(
                "wallet_cursor",
                &format!("cursor-sig-{idx:03}"),
                ts,
                SOL_MINT,
                "TokenCursor1111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.max_window_swaps_in_memory = 100;
        config.max_fetch_swaps_per_cycle = 4;
        config.max_fetch_pages_per_cycle = 1;

        let discovery_first = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let _ = discovery_first.run_cycle(&store, now)?;
        let cursor_after_first = store
            .load_discovery_runtime_cursor()?
            .expect("cursor must be persisted after first cycle");
        assert_eq!(cursor_after_first.signature, "cursor-sig-003");

        // Simulate process restart: new DiscoveryService should continue from persisted cursor.
        let discovery_second = DiscoveryService::new(config, permissive_shadow_quality());
        let _ = discovery_second.run_cycle(&store, now + Duration::minutes(1))?;
        let cursor_after_second = store
            .load_discovery_runtime_cursor()?
            .expect("cursor must stay persisted after second cycle");
        assert_eq!(cursor_after_second.signature, "cursor-sig-007");
        Ok(())
    }
