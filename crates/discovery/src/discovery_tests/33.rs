    #[test]
    fn tradable_ratio_blocks_wallet_when_most_buys_are_deferred() {
        let now = DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let mut config = DiscoveryConfig::default();
        config.min_trades = 10;
        config.min_active_days = 1;
        config.min_leader_notional_sol = 0.1;
        config.min_buy_count = 10;
        config.min_tradable_ratio = 0.5;
        config.max_rug_ratio = 1.0;
        let discovery = DiscoveryService::new(config, copybot_config::ShadowConfig::default());

        let mut acc = WalletAccumulator::default();
        acc.first_seen = Some(now - Duration::minutes(20));
        acc.last_seen = Some(now);
        acc.trades = 10;
        acc.max_buy_notional_sol = 1.0;
        acc.active_days.insert(now.date_naive());
        acc.buy_observations.push(BuyObservation {
            token: "TokenResolved11111111111111111111111111111".to_string(),
            ts: now - Duration::minutes(20),
            tradable: true,
            quality_resolved: true,
        });
        for idx in 0..9 {
            acc.buy_observations.push(BuyObservation {
                token: format!("TokenDeferred{idx:02}111111111111111111111111111"),
                ts: now - Duration::minutes(19 - idx as i64),
                tradable: false,
                quality_resolved: false,
            });
        }

        let snapshot = discovery.snapshot_from_accumulator(
            "wallet_mostly_deferred".to_string(),
            acc,
            now,
            &HashMap::new(),
        );

        assert!(
            (snapshot.tradable_ratio - 0.1_f64.sqrt()).abs() < 1e-9,
            "tradable_ratio should be penalized when most buys remain unresolved"
        );
        assert!(
            !snapshot.eligible,
            "wallet should not pass tradability gating when only a small minority of buys are resolved"
        );
    }

    #[test]
    fn max_rug_ratio_one_disables_rug_penalty_and_gate() {
        let now = DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let mut config = DiscoveryConfig::default();
        config.min_trades = 10;
        config.min_active_days = 3;
        config.min_leader_notional_sol = 0.5;
        config.min_buy_count = 10;
        config.min_tradable_ratio = 0.25;
        config.max_rug_ratio = 1.0;
        let discovery = DiscoveryService::new(config, copybot_config::ShadowConfig::default());

        let snapshot = discovery.snapshot_from_components(
            "wallet_rug_disabled".to_string(),
            now - Duration::days(3),
            now,
            16,
            3,
            10.0,
            3.5,
            1.2,
            5,
            5,
            &[114, 120, 98, 130, 117],
            &HashMap::new(),
            false,
            12,
            12,
            12,
            RugMetrics {
                evaluated: 12,
                rugged: 12,
                unevaluated: 0,
            },
            now,
        );

        assert!(
            snapshot.eligible,
            "rug gate must be bypassed at max_rug_ratio=1.0"
        );
        assert!(
            snapshot.score > 0.4,
            "rug penalty must no longer zero the score when emergency rug disable is active"
        );
        assert!(
            (snapshot.rug_ratio - 1.0).abs() < 1e-9,
            "the raw rug_ratio can stay 1.0 while the emergency override bypasses it"
        );
    }

    #[test]
    fn run_cycle_persists_wallet_metrics_only_once_per_snapshot_bucket() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-metric-bucket.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let buy_ts = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let sell_ts = buy_ts + Duration::minutes(6);
        for idx in 0..6 {
            let offset = Duration::minutes((idx * 20) as i64);
            store.insert_observed_swap(&swap(
                "wallet_bucket",
                &format!("bucket-buy-{idx}"),
                buy_ts + offset,
                SOL_MINT,
                "TokenBucket1111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_bucket",
                &format!("bucket-sell-{idx}"),
                sell_ts + offset,
                "TokenBucket1111111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.2,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.metric_snapshot_interval_seconds = 3600;
        config.observed_swaps_retention_days = 45;
        config.thin_market_min_unique_traders = 1;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_now = DateTime::parse_from_rfc3339("2026-03-04T12:40:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let second_now = first_now + Duration::minutes(10);
        let third_now = first_now + Duration::hours(1);

        let summary_first = discovery.run_cycle(&store, first_now)?;
        assert_eq!(summary_first.metrics_written, 1);
        let first_window = store
            .latest_wallet_metrics_window_start()?
            .expect("expected first metrics window to persist");

        let summary_second = discovery.run_cycle(&store, second_now)?;
        assert_eq!(
            summary_second.metrics_written, 0,
            "same snapshot bucket must not rewrite wallet_metrics"
        );
        let second_window = store
            .latest_wallet_metrics_window_start()?
            .expect("metrics window should remain available");
        assert_eq!(second_window, first_window);

        let summary_third = discovery.run_cycle(&store, third_now)?;
        assert_eq!(summary_third.metrics_written, 1);
        let third_window = store
            .latest_wallet_metrics_window_start()?
            .expect("next snapshot bucket must persist a new wallet_metrics window");
        assert!(third_window > second_window);
        Ok(())
    }

    #[test]
    fn run_cycle_persists_wallet_metrics_after_scoring_window_change_moves_window_backward(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-metric-window-config-change.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let buy_ts = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let sell_ts = buy_ts + Duration::minutes(6);
        for idx in 0..6 {
            let offset = Duration::minutes((idx * 20) as i64);
            store.insert_observed_swap(&swap(
                "wallet_config_shift",
                &format!("shift-buy-{idx}"),
                buy_ts + offset,
                SOL_MINT,
                "TokenShift11111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_config_shift",
                &format!("shift-sell-{idx}"),
                sell_ts + offset,
                "TokenShift11111111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.2,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.observed_swaps_retention_days = 30;
        config.metric_snapshot_interval_seconds = 3600;
        config.thin_market_min_unique_traders = 1;

        let now = DateTime::parse_from_rfc3339("2026-03-04T12:40:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let discovery_initial = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let summary_initial = discovery_initial.run_cycle(&store, now)?;
        assert_eq!(summary_initial.metrics_written, 1);

        let first_window = store
            .latest_wallet_metrics_window_start()?
            .expect("expected first metrics window to persist");

        config.scoring_window_days = 30;
        config.decay_window_days = 30;
        let discovery_shifted = DiscoveryService::new(config, permissive_shadow_quality());
        let summary_shifted = discovery_shifted.run_cycle(&store, now)?;
        assert_eq!(
            summary_shifted.metrics_written, 1,
            "a backward-shifted metrics window caused by config change must still persist"
        );

        let second_window = store
            .latest_wallet_metrics_window_start()?
            .expect("expected second metrics window to persist");
        assert_eq!(
            second_window, first_window,
            "an older config-shifted bucket should not advance the global MAX(window_start)"
        );
        assert!(
            store.wallet_metrics_window_exists(discovery_shifted.metrics_window_start(now))?,
            "the backward-shifted metrics bucket must still be inserted"
        );
        Ok(())
    }

    #[test]
    fn run_cycle_defers_full_snapshot_recompute_until_next_snapshot_bucket() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-snapshot-recompute-cadence.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let base_ts = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        for idx in 0..6 {
            let offset = Duration::minutes((idx * 20) as i64);
            let buy_ts = base_ts + offset;
            let sell_ts = buy_ts + Duration::minutes(6);
            store.insert_observed_swap(&swap(
                "wallet_recompute_a",
                &format!("recompute-a-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenRecomputeA111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_recompute_a",
                &format!("recompute-a-sell-{idx}"),
                sell_ts,
                "TokenRecomputeA111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.2,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.follow_top_n = 2;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.metric_snapshot_interval_seconds = 3600;
        config.thin_market_min_unique_traders = 1;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_now = DateTime::parse_from_rfc3339("2026-03-04T15:40:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let second_now = first_now + Duration::minutes(10);
        let third_now = first_now + Duration::hours(1);

        let summary_first = discovery.run_cycle(&store, first_now)?;
        assert_eq!(summary_first.wallets_seen, 1);
        assert_eq!(summary_first.metrics_written, 1);

        for idx in 0..6 {
            let offset = Duration::minutes((idx * 20) as i64);
            let buy_ts = base_ts + Duration::minutes(5) + offset;
            let sell_ts = buy_ts + Duration::minutes(6);
            store.insert_observed_swap(&swap(
                "wallet_recompute_b",
                &format!("recompute-b-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenRecomputeB111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_recompute_b",
                &format!("recompute-b-sell-{idx}"),
                sell_ts,
                "TokenRecomputeB111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.2,
            ))?;
        }

        let summary_second = discovery.run_cycle(&store, second_now)?;
        assert_eq!(
            summary_second.wallets_seen, 1,
            "same snapshot bucket should reuse cached discovery summary instead of full recompute"
        );
        assert_eq!(summary_second.metrics_written, 0);

        let summary_third = discovery.run_cycle(&store, third_now)?;
        assert_eq!(
            summary_third.wallets_seen, 2,
            "next snapshot bucket must recompute and include swaps accumulated while cached"
        );
        assert_eq!(summary_third.metrics_written, 2);
        Ok(())
    }

    #[test]
    fn cap_truncation_temporarily_suppresses_false_followlist_demotions_before_guard_expires(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-cap-truncation-followlist-suppression.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let base_ts = DateTime::parse_from_rfc3339("2026-03-04T10:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        for idx in 0..4 {
            let buy_ts = base_ts + Duration::minutes((idx * 20) as i64);
            let sell_ts = buy_ts + Duration::minutes(6);
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("leader-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenLeader111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("leader-sell-{idx}"),
                sell_ts,
                "TokenLeader111111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.3,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.follow_top_n = 1;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 4;
        config.min_active_days = 1;
        config.min_score = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.metric_snapshot_interval_seconds = 60;
        config.max_window_swaps_in_memory = 8;
        config.max_fetch_swaps_per_cycle = 100;
        config.thin_market_min_unique_traders = 1;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_now = DateTime::parse_from_rfc3339("2026-03-04T15:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let second_now = first_now + Duration::minutes(1);

        let first_summary = discovery.run_cycle(&store, first_now)?;
        assert!(
            first_summary.follow_promoted >= 1,
            "seed cycle should promote the profitable leader"
        );
        let active_before = store.list_active_follow_wallets()?;
        assert!(active_before.contains("wallet_leader"));

        for idx in 0..8 {
            let ts = first_now + Duration::seconds((idx + 1) as i64);
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("noise-buy-{idx}"),
                ts,
                SOL_MINT,
                "TokenNoise11111111111111111111111111111111",
                0.2,
                20.0,
            ))?;
        }

        let second_summary = discovery.run_cycle(&store, second_now)?;
        assert_eq!(
            second_summary.follow_demoted, 0,
            "first cap-truncated recompute must still suppress followlist demotions while the bounded guard is active"
        );
        assert!(
            second_summary.raw_window_cap_truncated,
            "cap-truncated raw recompute summary must report that the raw window is still truncated"
        );
        assert!(
            second_summary.cap_truncation_deactivation_guard_active,
            "cap-truncated raw recompute summary must report active temporary deactivation suppression"
        );
        assert_eq!(
            second_summary.cap_truncation_deactivation_guard_reason,
            Some("live_cap_eviction"),
            "summary must expose why cap-truncation suppression is active"
        );
        assert!(
            second_summary.cap_truncation_floor_signature.is_some(),
            "summary must expose the retained truncation floor signature for diagnostics"
        );
        assert_eq!(
            second_summary.scoring_source, "published_universe_raw_window_degraded",
            "cap-truncated recompute now degrades onto the last published universe instead of rotating from partial raw data"
        );
        assert_eq!(second_summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
        let active_after = store.list_active_follow_wallets()?;
        assert!(
            active_after.contains("wallet_leader"),
            "leader must remain active while discovery window is known truncated by the cap"
        );
        let state = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(
            state.cap_truncation_floor.is_some(),
            "cap eviction should leave a truncation marker while raw history remains incomplete"
        );
        Ok(())
    }
