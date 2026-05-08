#[test]
    fn run_cycle_fetches_multiple_cursor_pages_within_single_cycle() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-multi-page-fetch.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::minutes(20);
        for idx in 0..10 {
            let ts = start + Duration::seconds((idx * 10) as i64);
            store.insert_observed_swap(&swap(
                "wallet_multi_page",
                &format!("multi-page-sig-{idx:03}"),
                ts,
                SOL_MINT,
                "TokenMultiPage111111111111111111111111111",
                1.0,
                100.0,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.max_window_swaps_in_memory = 100;
        config.max_fetch_swaps_per_cycle = 4;
        config.max_fetch_pages_per_cycle = 3;
        config.fetch_time_budget_ms = 60_000;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let _ = discovery.run_cycle(&store, now)?;
        let cursor = store
            .load_discovery_runtime_cursor()?
            .expect("cursor must be persisted after multi-page fetch");
        assert_eq!(
            cursor.signature, "multi-page-sig-009",
            "single cycle should page through all cursor rows until the short final page"
        );
        Ok(())
    }

    #[test]
    fn run_cycle_respects_fetch_page_budget_and_continues_next_cycle() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-fetch-page-budget.db");
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
                "wallet_page_budget",
                &format!("page-budget-sig-{idx:03}"),
                ts,
                SOL_MINT,
                "TokenPageBudget1111111111111111111111111",
                1.0,
                100.0,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.max_window_swaps_in_memory = 100;
        config.max_fetch_swaps_per_cycle = 4;
        config.max_fetch_pages_per_cycle = 2;
        config.fetch_time_budget_ms = 60_000;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let _ = discovery.run_cycle(&store, now)?;
        let cursor_after_first = store
            .load_discovery_runtime_cursor()?
            .expect("cursor must be persisted after first bounded cycle");
        assert_eq!(cursor_after_first.signature, "page-budget-sig-007");

        let _ = discovery.run_cycle(&store, now + Duration::minutes(1))?;
        let cursor_after_second = store
            .load_discovery_runtime_cursor()?
            .expect("cursor must advance on next cycle");
        assert_eq!(cursor_after_second.signature, "page-budget-sig-011");
        Ok(())
    }

    #[test]
    fn restart_with_persisted_cursor_warm_load_does_not_false_demote_followlist() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-followlist-warm.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-04T13:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::hours(8);
        for idx in 0..12 {
            let buy_ts = start + Duration::minutes((idx * 20) as i64);
            let sell_ts = buy_ts + Duration::minutes(5);
            store.insert_observed_swap(&swap(
                "wallet_a",
                &format!("warm-a-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenWarmA11111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_a",
                &format!("warm-a-sell-{idx}"),
                sell_ts,
                "TokenWarmA11111111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.35,
            ))?;

            store.insert_observed_swap(&swap(
                "wallet_b",
                &format!("warm-b-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenWarmB11111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_b",
                &format!("warm-b-sell-{idx}"),
                sell_ts,
                "TokenWarmB11111111111111111111111111111111",
                SOL_MINT,
                100.0,
                0.70,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.55;
        config.max_tx_per_minute = 50;
        config.min_buy_count = 10;
        config.thin_market_min_unique_traders = 1;
        config.max_window_swaps_in_memory = 200;
        config.max_fetch_swaps_per_cycle = 200;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let _ = discovery.run_cycle(&store, now)?;
        let active_before = store.list_active_follow_wallets()?;
        assert!(active_before.contains("wallet_a"));

        // One fresh low-signal swap arrives after cursor checkpoint.
        store.insert_observed_swap(&swap(
            "wallet_noise",
            "warm-noise-buy-0",
            now + Duration::minutes(1),
            SOL_MINT,
            "TokenNoise111111111111111111111111111111111",
            0.2,
            20.0,
        ))?;

        // Simulate restart with narrow per-cycle fetch budget.
        let mut restart_config = config.clone();
        restart_config.max_fetch_swaps_per_cycle = 1;
        let discovery_after_restart =
            DiscoveryService::new(restart_config, permissive_shadow_quality());
        let _ = discovery_after_restart.run_cycle(&store, now + Duration::minutes(2))?;
        let active_after = store.list_active_follow_wallets()?;
        assert!(
            active_after.contains("wallet_a"),
            "wallet_a should not be false-demoted on restart cold state"
        );
        Ok(())
    }

    #[test]
    fn cold_start_full_raw_window_without_trusted_snapshot_publishes_raw_top_n_stage1() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-cold-start-full-raw.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        for idx in 0..6 {
            let offset = Duration::minutes((idx * 20) as i64);
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("stage1-top-buy-{idx}"),
                now - Duration::days(2) + offset,
                SOL_MINT,
                "TokenStage1Top1111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("stage1-top-sell-{idx}"),
                now - Duration::days(2) + offset + Duration::minutes(5),
                "TokenStage1Top1111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.3,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("stage1-noise-buy-{idx}"),
                now - Duration::days(2) + offset,
                SOL_MINT,
                "TokenStage1Noise11111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("stage1-noise-sell-{idx}"),
                now - Duration::days(2) + offset + Duration::minutes(5),
                "TokenStage1Noise11111111111111111111111111",
                SOL_MINT,
                100.0,
                0.7,
            ))?;
        }

        let mut config = stage1_runtime_config();
        config.max_window_swaps_in_memory = 64;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(summary.scoring_source, "raw_window");
        assert!(!summary.trusted_selection_fail_closed);
        assert_eq!(summary.active_follow_wallets, 1);
        assert!(
            summary
                .top_wallets
                .iter()
                .any(|label| label.starts_with("wallet_top:")),
            "raw-window publish should surface the profitable wallet as leader"
        );
        let active_wallets = store.list_active_follow_wallets()?;
        assert_eq!(active_wallets.len(), 1);
        assert!(active_wallets.contains("wallet_top"));
        Ok(())
    }
