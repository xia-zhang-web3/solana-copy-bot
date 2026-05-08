    #[test]
    fn promotes_profitable_wallets_to_followlist() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::days(1);

        for idx in 0..12 {
            let buy_ts = start + Duration::minutes((idx * 20) as i64);
            let sell_ts = buy_ts + Duration::minutes(6);
            let signature_buy = format!("a-buy-{idx}");
            let signature_sell = format!("a-sell-{idx}");
            store.insert_observed_swap(&swap(
                "wallet_a",
                &signature_buy,
                buy_ts,
                SOL_MINT,
                "TokenA11111111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_a",
                &signature_sell,
                sell_ts,
                "TokenA11111111111111111111111111111111111",
                SOL_MINT,
                100.0,
                1.35,
            ))?;

            let signature_b_buy = format!("b-buy-{idx}");
            let signature_b_sell = format!("b-sell-{idx}");
            store.insert_observed_swap(&swap(
                "wallet_b",
                &signature_b_buy,
                buy_ts,
                SOL_MINT,
                "TokenB11111111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_b",
                &signature_b_sell,
                sell_ts,
                "TokenB11111111111111111111111111111111111",
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
        config.thin_market_min_unique_traders = 1;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.wallets_seen, 2);
        assert_eq!(summary.metrics_written, 2);
        assert!(summary.follow_promoted >= 1);

        let active = store.list_active_follow_wallets()?;
        assert!(active.contains("wallet_a"));
        assert!(!active.contains("wallet_b"));
        Ok(())
    }

    #[test]
    fn live_like_complete_publication_truth_persists_only_ten_wallets_when_pre_rank_selection_gates_leave_ten_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("live-like-small-published-universe.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-06T06:37:24Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = live_shadow_blocker_discovery_config_for_tests();
        assert_eq!(config.follow_top_n, 15);
        let entries = live_publish_gate_fixture_wallets(now);
        let (snapshots, desired) =
            desired_wallets_from_live_publish_gate_fixture(config.clone(), &entries, now);

        assert_eq!(
            rank_follow_candidates(&snapshots, config.min_score).len(),
            10,
            "the live-like field must already collapse to ten wallets before top-N truncation"
        );
        assert_eq!(
            desired.len(),
            10,
            "published universe must stay at ten because only ten wallets survive exact eligibility and score gates"
        );
        assert!(
            !desired.contains(&"wallet_fail_min_active_days".to_string()),
            "wallet below min_active_days must not reach publication truth"
        );
        assert!(
            !desired.contains(&"wallet_fail_min_buy_count".to_string()),
            "wallet below min_buy_count must not reach publication truth"
        );
        assert!(
            !desired.contains(&"wallet_fail_tradable_ratio".to_string()),
            "wallet below min_tradable_ratio must not reach publication truth"
        );
        assert!(
            !desired.contains(&"wallet_fail_score".to_string()),
            "wallet below min_score must not reach publication truth"
        );
        assert!(
            !desired.contains(&"wallet_fail_min_leader_notional".to_string()),
            "wallet below min_leader_notional_sol must not reach publication truth"
        );
        assert!(
            !desired.contains(&"wallet_fail_decay".to_string()),
            "wallet outside decay_window_days must not reach publication truth"
        );
        assert!(
            !desired.contains(&"wallet_fail_suspicious".to_string()),
            "suspicious wallet must not reach publication truth"
        );

        let fail_tradable_snapshot = snapshots
            .iter()
            .find(|snapshot| snapshot.wallet_id == "wallet_fail_tradable_ratio")
            .expect("tradable-ratio failure snapshot should exist");
        assert!(
            fail_tradable_snapshot.tradable_ratio < config.min_tradable_ratio,
            "tradable-ratio gate must be the causal filter for wallet_fail_tradable_ratio"
        );
        assert!(
            !fail_tradable_snapshot.eligible,
            "wallet below min_tradable_ratio must already be ineligible before ranking"
        );

        let fail_score_snapshot = snapshots
            .iter()
            .find(|snapshot| snapshot.wallet_id == "wallet_fail_score")
            .expect("score failure snapshot should exist");
        assert!(
            fail_score_snapshot.eligible,
            "wallet_fail_score must pass hard eligibility so the repro isolates the min_score ranking filter"
        );
        assert!(
            fail_score_snapshot.score < config.min_score,
            "wallet_fail_score must specifically fail the score floor"
        );

        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let publication_outcome = discovery.persist_publication_state(
            &store,
            DiscoveryRuntimeMode::Healthy,
            true,
            metrics_window_start,
            Some(&desired),
            "raw_window_persisted_stream",
            "discovery_score_refresh",
            now,
        )?;
        assert!(
            publication_outcome.published_universe_persisted,
            "once the exact desired wallet set is non-empty, publication truth should persist it as-is"
        );
        let publication_state = store
            .discovery_publication_state_read_only()?
            .expect("publication state should be present");
        assert_eq!(
            publication_state.published_wallet_ids.unwrap_or_default(),
            desired,
            "the persisted publication truth must exactly mirror the pre-ranked wallet set"
        );

        Ok(())
    }

    #[test]
    fn live_like_small_published_universe_gate_attribution_counts_stage1() {
        let now = DateTime::parse_from_rfc3339("2026-04-06T06:37:24Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = live_shadow_blocker_discovery_config_for_tests();
        let entries = live_publish_gate_fixture_wallets(now);
        let (snapshots, desired) =
            desired_wallets_from_live_publish_gate_fixture(config.clone(), &entries, now);
        let counts = live_publish_gate_attribution_counts(&config, &entries, &snapshots, now);

        assert_eq!(
            counts,
            LivePublishGateAttributionCounts {
                total_snapshots: 17,
                removed_by_min_trades: 0,
                removed_by_min_active_days: 1,
                removed_by_suspicious: 1,
                removed_by_min_leader_notional_sol: 1,
                removed_by_decay_window_days: 1,
                removed_by_min_buy_count: 1,
                removed_by_min_tradable_ratio: 1,
                removed_by_min_score: 1,
                published_wallets: 10,
            },
            "the live-like reduced field must lose seven wallets before top-N: one each to min_active_days, suspicious, min_leader_notional_sol, decay_window_days, min_buy_count, min_tradable_ratio, and min_score"
        );
        assert_eq!(
            desired.len(),
            counts.published_wallets,
            "published wallet count should exactly match the post-gate count"
        );
    }
