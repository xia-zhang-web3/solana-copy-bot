    #[test]
    fn cap_truncation_keeps_followlist_demotions_suppressed_while_raw_window_remains_incomplete(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-cap-truncation-followlist-guard-expiry.db");
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
                &format!("leader-guard-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                "TokenLeaderGuard1111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("leader-guard-sell-{idx}"),
                sell_ts,
                "TokenLeaderGuard1111111111111111111111111",
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

        let first_summary = discovery.run_cycle(&store, first_now)?;
        assert!(
            first_summary.follow_promoted >= 1,
            "seed cycle should promote the profitable leader"
        );
        assert!(store
            .list_active_follow_wallets()?
            .contains("wallet_leader"));

        for idx in 0..8 {
            let ts = first_now + Duration::seconds((idx + 1) as i64);
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("noise-guard-buy-{idx}"),
                ts,
                SOL_MINT,
                "TokenNoiseGuard11111111111111111111111111",
                0.2,
                20.0,
            ))?;
        }

        let second_summary = discovery.run_cycle(&store, first_now + Duration::minutes(1))?;
        assert_eq!(
            second_summary.follow_demoted, 0,
            "first cap-truncated cycle should still honor the temporary deactivation guard"
        );

        let third_summary = discovery.run_cycle(&store, first_now + Duration::minutes(2))?;
        assert_eq!(
            third_summary.follow_demoted, 0,
            "second cap-truncated cycle should consume the remaining temporary deactivation guard"
        );

        let fourth_summary = discovery.run_cycle(&store, first_now + Duration::minutes(3))?;
        assert_eq!(
            fourth_summary.follow_demoted, 0,
            "cap-truncated raw discovery must not demote followlist entries while the retained raw window still covers only a tiny fraction of the intended scoring horizon"
        );
        let active_after = store.list_active_follow_wallets()?;
        assert!(
            active_after.contains("wallet_leader"),
            "leader must stay active while cap-truncated raw discovery still represents incomplete history"
        );
        let state = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(
            state.cap_truncation_floor.is_some(),
            "raw-window truncation floor should still describe the incomplete history gap after the guard expires"
        );
        assert_eq!(
            state.cap_truncation_deactivation_guard_cycles_remaining, 0,
            "bounded countdown should still reach zero even when safety suppression remains active for an incomplete raw window"
        );
        assert!(
            fourth_summary.cap_truncation_deactivation_guard_active,
            "summary must continue advertising deactivation suppression while the raw window remains incomplete after the bounded countdown expires"
        );
        Ok(())
    }

    #[test]
    fn cap_truncated_partial_raw_window_suppresses_followlist_promotions_and_metrics_even_after_most_of_horizon_is_retained(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-cap-truncation-activation-boundary.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let window_start = now - Duration::days(1);

        for (idx, offset_minutes) in [5, 45].into_iter().enumerate() {
            let buy_ts = window_start + Duration::minutes(offset_minutes);
            let sell_ts = buy_ts + Duration::minutes(10);
            let token = format!("TokenLeaderBoundary{idx:02}111111111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("leader-boundary-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                token.as_str(),
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_leader",
                &format!("leader-boundary-sell-{idx}"),
                sell_ts,
                token.as_str(),
                SOL_MINT,
                100.0,
                1.3,
            ))?;
        }

        for (idx, offset_minutes) in [125, 485, 845, 1380].into_iter().enumerate() {
            let buy_ts = window_start + Duration::minutes(offset_minutes);
            let sell_ts = buy_ts + Duration::minutes(10);
            let token = format!("TokenCandidateBoundary{idx:02}11111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_candidate",
                &format!("candidate-boundary-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                token.as_str(),
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_candidate",
                &format!("candidate-boundary-sell-{idx}"),
                sell_ts,
                token.as_str(),
                SOL_MINT,
                100.0,
                1.35,
            ))?;
        }

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 1;
        config.decay_window_days = 1;
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
        let summary = discovery.run_cycle(&store, now)?;
        assert!(
            summary.raw_window_cap_truncated,
            "the retained raw window must still advertise cap truncation while the earliest leader slice is missing"
        );
        assert_eq!(
            summary.follow_promoted, 0,
            "cap-truncated raw recompute must not promote from a partial tail even when the retained tail spans most of the scoring horizon"
        );
        assert_eq!(
            summary.metrics_written, 0,
            "cap-truncated raw recompute must not persist wallet_metrics from a partial tail even when the retained span exceeds the old coverage heuristic"
        );

        let active_follow_wallets = store.list_active_follow_wallets()?;
        assert!(
            !active_follow_wallets.contains("wallet_candidate"),
            "partial-tail candidate must not activate while raw discovery is still source-invalid"
        );
        assert!(
            active_follow_wallets.is_empty(),
            "cap-truncated raw recompute must not publish a new active follow universe from an incomplete tail"
        );
        assert_eq!(
            store.latest_wallet_metrics_window_start()?,
            None,
            "partial raw discovery should not publish a fresh wallet_metrics bucket while cap truncation remains active"
        );
        let state = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        assert!(
            state.cap_truncation_floor.is_some(),
            "activation suppression should remain tied to the actual truncation marker rather than a coverage heuristic"
        );
        Ok(())
    }
