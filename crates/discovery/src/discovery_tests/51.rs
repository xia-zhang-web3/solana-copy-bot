#[test]
    fn live_like_small_published_universe_is_caused_by_exact_pre_rank_gates_not_top_n_stage1() {
        let now = DateTime::parse_from_rfc3339("2026-04-06T06:37:24Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = live_shadow_blocker_discovery_config_for_tests();
        let entries = live_publish_gate_fixture_wallets(now);

        let (_, base_desired) =
            desired_wallets_from_live_publish_gate_fixture(config.clone(), &entries, now);
        assert_eq!(base_desired.len(), 10);
        assert_eq!(config.follow_top_n, 15);

        let mut relaxed_active_days = config.clone();
        relaxed_active_days.min_active_days = 2;
        let (_, active_days_desired) =
            desired_wallets_from_live_publish_gate_fixture(relaxed_active_days, &entries, now);
        assert_eq!(active_days_desired.len(), 11);
        assert!(
            active_days_desired.contains(&"wallet_fail_min_active_days".to_string()),
            "lowering only min_active_days should recover exactly the wallet gated by recency breadth"
        );

        let mut relaxed_buy_count = config.clone();
        relaxed_buy_count.min_buy_count = 9;
        let (_, buy_count_desired) =
            desired_wallets_from_live_publish_gate_fixture(relaxed_buy_count, &entries, now);
        assert_eq!(buy_count_desired.len(), 11);
        assert!(
            buy_count_desired.contains(&"wallet_fail_min_buy_count".to_string()),
            "lowering only min_buy_count should recover exactly the wallet gated by insufficient SOL buys"
        );

        let mut relaxed_tradable_ratio = config.clone();
        relaxed_tradable_ratio.min_tradable_ratio = 0.0;
        let (_, tradable_ratio_desired) =
            desired_wallets_from_live_publish_gate_fixture(relaxed_tradable_ratio, &entries, now);
        assert_eq!(
            tradable_ratio_desired.len(),
            10,
            "relaxing only min_tradable_ratio is still insufficient because the same tradability debt also drags score below min_score"
        );
        assert!(
            !tradable_ratio_desired.contains(&"wallet_fail_tradable_ratio".to_string()),
            "quality/tradability debt should still block publication when the score floor remains intact"
        );

        let mut relaxed_tradable_ratio_and_score = config.clone();
        relaxed_tradable_ratio_and_score.min_tradable_ratio = 0.0;
        relaxed_tradable_ratio_and_score.min_score = 0.0;
        let (_, tradable_ratio_and_score_desired) = desired_wallets_from_live_publish_gate_fixture(
            relaxed_tradable_ratio_and_score,
            &entries,
            now,
        );
        assert_eq!(tradable_ratio_and_score_desired.len(), 12);
        assert!(
            tradable_ratio_and_score_desired.contains(&"wallet_fail_tradable_ratio".to_string()),
            "lowering the exact tradability floor and the coupled score floor should recover the wallet blocked by exact quality/tradability debt"
        );
        assert!(
            tradable_ratio_and_score_desired.contains(&"wallet_fail_score".to_string()),
            "once the score floor is also relaxed, the separately score-blocked wallet should recover too"
        );

        let mut relaxed_score = config.clone();
        relaxed_score.min_score = 0.0;
        let (_, score_desired) =
            desired_wallets_from_live_publish_gate_fixture(relaxed_score, &entries, now);
        assert_eq!(score_desired.len(), 11);
        assert!(
            score_desired.contains(&"wallet_fail_score".to_string()),
            "lowering only min_score should recover exactly the wallet gated by ranking quality"
        );

        let mut relaxed_notional = config.clone();
        relaxed_notional.min_leader_notional_sol = 0.4;
        let (_, notional_desired) =
            desired_wallets_from_live_publish_gate_fixture(relaxed_notional, &entries, now);
        assert_eq!(notional_desired.len(), 11);
        assert!(
            notional_desired.contains(&"wallet_fail_min_leader_notional".to_string()),
            "lowering only min_leader_notional_sol should recover exactly the wallet gated by leader trade size"
        );

        let mut relaxed_decay = config.clone();
        relaxed_decay.decay_window_days = 7;
        let (_, decay_desired) =
            desired_wallets_from_live_publish_gate_fixture(relaxed_decay, &entries, now);
        assert_eq!(decay_desired.len(), 11);
        assert!(
            decay_desired.contains(&"wallet_fail_decay".to_string()),
            "widening only decay_window_days should recover exactly the wallet gated by last_seen recency"
        );

        let mut unsuspicious_entries = entries.clone();
        let (_, suspicious_acc) = unsuspicious_entries
            .iter_mut()
            .find(|(wallet_id, _)| wallet_id == "wallet_fail_suspicious")
            .expect("suspicious fixture wallet should exist");
        suspicious_acc.suspicious = false;
        let (_, suspicious_desired) =
            desired_wallets_from_live_publish_gate_fixture(config, &unsuspicious_entries, now);
        assert_eq!(suspicious_desired.len(), 11);
        assert!(
            suspicious_desired.contains(&"wallet_fail_suspicious".to_string()),
            "clearing only the suspicious marker should recover exactly the wallet blocked by the spam gate"
        );
    }

    #[test]
    fn live_like_window_shift_can_reduce_exact_publication_truth_from_ten_to_nine_via_decay_gate_stage1(
    ) {
        let baseline_now = DateTime::parse_from_rfc3339("2026-04-06T06:37:24Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let later_now = baseline_now + Duration::hours(2);
        let config = live_shadow_blocker_discovery_config_for_tests();
        let entries = live_publish_gate_fixture_wallets_with_decay_boundary(baseline_now);

        let (baseline_snapshots, baseline_desired) =
            desired_wallets_from_live_publish_gate_fixture(config.clone(), &entries, baseline_now);
        let baseline_counts = live_publish_gate_attribution_counts(
            &config,
            &entries,
            &baseline_snapshots,
            baseline_now,
        );
        assert_eq!(baseline_desired.len(), 10);
        assert!(
            baseline_desired.contains(&"wallet_pass_09".to_string()),
            "the borderline wallet must still survive the exact full-publication gate set before the decay cutoff advances"
        );
        assert_eq!(
            baseline_counts.removed_by_decay_window_days, 1,
            "baseline field should only lose the dedicated recency-fail wallet at the decay gate"
        );

        let (later_snapshots, later_desired) =
            desired_wallets_from_live_publish_gate_fixture(config.clone(), &entries, later_now);
        let later_counts =
            live_publish_gate_attribution_counts(&config, &entries, &later_snapshots, later_now);
        assert_eq!(later_desired.len(), 9);
        assert!(
            !later_desired.contains(&"wallet_pass_09".to_string()),
            "once the exact decay cutoff advances past the borderline wallet, the later exact publication truth must shrink to nine"
        );
        assert_eq!(
            later_counts,
            LivePublishGateAttributionCounts {
                total_snapshots: 17,
                removed_by_min_trades: 0,
                removed_by_min_active_days: 1,
                removed_by_suspicious: 1,
                removed_by_min_leader_notional_sol: 1,
                removed_by_decay_window_days: 2,
                removed_by_min_buy_count: 1,
                removed_by_min_tradable_ratio: 1,
                removed_by_min_score: 1,
                published_wallets: 9,
            },
            "the exact 10 -> 9 delta on the same live-like policy surface should be explained by one additional wallet crossing only the decay_window_days recency gate"
        );
    }

    #[test]
    fn degraded_runtime_preserves_latest_exact_nine_wallet_publication_truth_after_boundary_decay_drop_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("live-like-degraded-published-universe-preserves-latest-nine.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let published_ten_at = DateTime::parse_from_rfc3339("2026-04-06T06:37:24Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let published_nine_at = published_ten_at + Duration::hours(2);
        let degraded_now = published_nine_at + Duration::minutes(5);
        let mut config = live_shadow_blocker_discovery_config_for_tests();
        config.max_window_swaps_in_memory = 8;
        let entries = live_publish_gate_fixture_wallets_with_decay_boundary(published_ten_at);

        let (_, desired_ten) = desired_wallets_from_live_publish_gate_fixture(
            config.clone(),
            &entries,
            published_ten_at,
        );
        let (_, desired_nine) = desired_wallets_from_live_publish_gate_fixture(
            config.clone(),
            &entries,
            published_nine_at,
        );
        assert_eq!(desired_ten.len(), 10);
        assert_eq!(desired_nine.len(), 9);
        assert!(
            desired_ten.contains(&"wallet_pass_09".to_string())
                && !desired_nine.contains(&"wallet_pass_09".to_string()),
            "the later exact publish must drop only the borderline decay wallet"
        );

        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let published_ten_window_start = metrics_window_start_for_test(&config, published_ten_at);
        let published_nine_window_start = metrics_window_start_for_test(&config, published_nine_at);
        let first_publication = discovery.persist_publication_state(
            &store,
            DiscoveryRuntimeMode::Healthy,
            true,
            published_ten_window_start,
            Some(&desired_ten),
            "raw_window_persisted_stream",
            "test_exact_ten_wallet_publish",
            published_ten_at,
        )?;
        assert!(first_publication.published_universe_persisted);
        let second_publication = discovery.persist_publication_state(
            &store,
            DiscoveryRuntimeMode::Healthy,
            true,
            published_nine_window_start,
            Some(&desired_nine),
            "raw_window_persisted_stream",
            "test_exact_nine_wallet_publish_after_decay_shift",
            published_nine_at,
        )?;
        assert!(second_publication.published_universe_persisted);

        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..9 {
            let ts = degraded_now - Duration::minutes(9) + Duration::minutes(idx as i64);
            let swap = swap(
                "wallet_noise",
                &format!("live-like-degraded-noise-{idx}"),
                ts,
                SOL_MINT,
                "TokenLiveLikeDegradedNoise111111111111111",
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
        store.upsert_discovery_runtime_cursor(
            &latest_cursor.expect("latest cursor should be present"),
        )?;

        let summary = discovery.run_cycle(&store, degraded_now)?;
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Degraded);
        assert_eq!(
            summary.scoring_source, "published_universe_raw_window_degraded",
            "when the raw window is incomplete, degraded runtime must surface the most recent exact published universe rather than recomputing a new one"
        );
        assert_eq!(summary.active_follow_wallets, 9);
        assert_eq!(
            store.list_active_follow_wallets()?,
            desired_nine.iter().cloned().collect(),
            "degraded followlist surface must preserve the latest exact nine-wallet publication universe, not the earlier ten-wallet publish and not any current partial raw ranking"
        );

        let publication_state = store
            .discovery_publication_state_read_only()?
            .expect("publication state should exist");
        assert_eq!(
            publication_state.runtime_mode,
            DiscoveryRuntimeMode::Degraded
        );
        assert_eq!(
            publication_state.reason, "published_universe_raw_window_degraded",
            "degraded cycle should surface the raw-window-degraded reason while preserving exact publication truth ownership"
        );
        assert_eq!(
            publication_state.last_published_at,
            Some(published_nine_at),
            "degraded publication state should keep the latest exact publish timestamp rather than reverting to the older ten-wallet publish"
        );
        assert_eq!(
            publication_state.last_published_window_start,
            Some(published_nine_window_start),
            "degraded publication state should keep the latest exact publish window"
        );
        assert_eq!(
            publication_state.published_wallet_ids.unwrap_or_default(),
            desired_nine,
            "degraded publication state should preserve the exact nine-wallet control-plane universe from the latest exact publish"
        );

        Ok(())
    }
