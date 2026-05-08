#[test]
    fn warm_restore_keeps_followlist_demotions_suppressed_while_raw_window_remains_truncated_after_guard_countdown(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-warm-restore-capped-tail-demotion-guard.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-13T08:21:30Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        for idx in 0..4 {
            let buy_ts = now - Duration::minutes(40) + Duration::minutes((idx * 4) as i64);
            let sell_ts = buy_ts + Duration::minutes(2);
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("restart-leader-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenRestartLeader111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("restart-leader-sell-{idx}"),
                sell_ts,
                "TokenRestartLeader111111111111111111111111",
                SOL_MINT,
                100.0,
                1.3,
            ))?;
        }

        let mut latest_noise_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..9 {
            let ts = now - Duration::minutes(9) + Duration::minutes(idx as i64);
            let signature = format!("restart-noise-buy-{idx}");
            let swap = swap(
                "wallet_noise",
                signature.as_str(),
                ts,
                SOL_MINT,
                "TokenRestartNoise1111111111111111111111111",
                0.2,
                20.0,
            );
            latest_noise_cursor = Some(DiscoveryRuntimeCursor {
                ts_utc: swap.ts_utc,
                slot: swap.slot,
                signature: swap.signature.clone(),
            });
            store.insert_observed_swap(&swap)?;
        }

        store.activate_follow_wallet("wallet_leader", now - Duration::minutes(1), "seed-follow")?;
        store.upsert_discovery_runtime_cursor(
            &latest_noise_cursor.expect("latest noise cursor should be present"),
        )?;

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

        let metrics_window_start = {
            let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
            let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
            let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
            bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
        };
        store.upsert_wallet(
            "wallet_leader",
            now - Duration::days(2),
            now - Duration::minutes(1),
            "candidate",
        )?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_leader".to_string(),
            window_start: metrics_window_start,
            pnl: 2.4,
            win_rate: 0.85,
            trades: 8,
            closed_trades: 4,
            hold_median_seconds: 360,
            score: 0.81,
            buy_total: 4,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
        let published_wallets = HashSet::from(["wallet_leader".to_string()]);
        let discovery_after_restart = DiscoveryService::new(config, permissive_shadow_quality());
        seed_recent_published_universe(
            &discovery_after_restart,
            &store,
            now - Duration::seconds(30),
            metrics_window_start,
            "raw_window",
            &published_wallets,
        )?;

        let summary = discovery_after_restart.run_cycle(&store, now)?;
        assert_eq!(
            summary.metrics_written, 0,
            "warm-restored capped tail should not persist a fresh wallet_metrics bucket from partial raw data"
        );
        assert_eq!(
            summary.follow_promoted, 0,
            "warm-restored capped tail should not promote from partial raw data"
        );
        assert_eq!(
            summary.follow_demoted, 0,
            "warm-restore on an already capped recent tail must suppress false followlist demotions"
        );
        assert!(
            summary.raw_window_cap_truncated,
            "warm-restored capped-tail bootstrap summary must report that raw history remains truncated"
        );
        assert!(
            summary.cap_truncation_deactivation_guard_active,
            "warm-restored capped-tail bootstrap summary must report that the temporary deactivation guard is active"
        );
        assert_eq!(
            summary.cap_truncation_deactivation_guard_reason,
            Some("warm_load_truncated"),
            "warm-restored capped-tail bootstrap summary must expose the warm-load truncation reason"
        );
        assert_eq!(
            summary.scoring_source,
            "published_universe_raw_window_degraded",
            "warm-restored capped-tail restart must degrade to the last published universe while raw history remains truncated"
        );
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
        assert_eq!(summary.eligible_wallets, 1);
        let active_after = store.list_active_follow_wallets()?;
        assert!(
            active_after.contains("wallet_leader"),
            "existing followed wallet must remain active on first post-restart recompute when warm slice is already truncated"
        );
        assert_eq!(
            store.latest_wallet_metrics_window_start()?,
            Some(metrics_window_start),
            "bootstrap on truncated warm restore must not write a newer wallet_metrics bucket from partial raw data"
        );
        let state = discovery_after_restart
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert_eq!(state.swaps.len(), 8);
        assert!(
            state.cap_truncation_floor.is_some(),
            "warm-restore capped tail must immediately latch truncation marker"
        );
        assert!(
            !state.truncated_warm_restore_bootstrap,
            "warm-restore degraded mode should not leave the legacy persisted-bootstrap bridge armed"
        );
        assert_eq!(
            state
                .cap_truncation_floor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some("restart-noise-buy-1"),
            "warm-restore truncation floor should point at the oldest retained row"
        );
        drop(state);

        let summary_follow_up =
            discovery_after_restart.run_cycle(&store, now + Duration::minutes(1))?;
        assert_eq!(
            summary_follow_up.follow_demoted, 0,
            "immediate follow-up raw recompute should still honor the bounded cap-truncation deactivation guard"
        );
        let summary_guard_expired =
            discovery_after_restart.run_cycle(&store, now + Duration::minutes(2))?;
        assert_eq!(
            summary_guard_expired.follow_demoted, 0,
            "warm-restored truncated raw discovery must keep deactivations suppressed while the retained raw window is still incomplete"
        );
        let active_after_guard_expiry = store.list_active_follow_wallets()?;
        assert!(
            active_after_guard_expiry.contains("wallet_leader"),
            "warm-restored followlist entries must not collapse while the raw window still represents only the capped tail"
        );
        assert!(
            store.latest_wallet_metrics_window_start()? == Some(metrics_window_start),
            "while raw history remains incomplete after warm restore, discovery must not persist a newer wallet_metrics snapshot from the capped tail"
        );
        let state_after_guard_expiry = discovery_after_restart
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert_eq!(
            state_after_guard_expiry.cap_truncation_deactivation_guard_cycles_remaining,
            0,
            "warm-restore countdown should still reach zero even when incomplete raw history keeps deactivations suppressed"
        );
        assert!(
            summary_guard_expired.cap_truncation_deactivation_guard_active,
            "summary must continue exposing active deactivation suppression while the warm-restored raw window remains truncated"
        );
        Ok(())
    }

    #[test]
    fn build_wallet_snapshots_normalizes_out_of_order_swaps_before_rug_partition_point(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-out-of-order-rug-history.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let buy_ts = DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut swaps = VecDeque::new();
        swaps.push_back(swap(
            "wallet_target",
            "target-buy",
            buy_ts,
            SOL_MINT,
            "TokenOrder11111111111111111111111111111111",
            1.0,
            100.0,
        ));
        swaps.push_back(swap(
            "wallet_post",
            "post-sell",
            buy_ts + Duration::minutes(1),
            "TokenOrder11111111111111111111111111111111",
            SOL_MINT,
            100.0,
            0.01,
        ));
        swaps.push_back(swap(
            "wallet_pre",
            "pre-sell",
            buy_ts - Duration::minutes(1),
            "TokenOrder11111111111111111111111111111111",
            SOL_MINT,
            100.0,
            10.0,
        ));

        let mut config = DiscoveryConfig::default();
        config.rug_lookahead_seconds = 300;
        config.thin_market_min_volume_sol = 2.0;
        config.thin_market_min_unique_traders = 1;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());

        let snapshots = discovery.build_wallet_snapshots_from_cached(
            &store,
            &swaps,
            buy_ts + Duration::minutes(10),
        )?;
        let target_snapshot = snapshots
            .into_iter()
            .find(|snapshot| snapshot.wallet_id == "wallet_target")
            .expect("target wallet snapshot must exist");

        assert!(
            (target_snapshot.rug_ratio - 1.0).abs() < 1e-9,
            "pre-buy trades that appear later in an unsorted swap window must not leak into rug lookahead volume"
        );
        Ok(())
    }
