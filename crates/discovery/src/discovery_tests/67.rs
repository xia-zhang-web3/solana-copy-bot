#[test]
    fn short_retention_bootstrap_does_not_republish_every_tick_or_repersist_metrics() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("test-short-retention-bootstrap-cadence.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let base_now = DateTime::parse_from_rfc3339("2026-03-08T13:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.observed_swaps_retention_days = 1;
        config.follow_top_n = 1;

        let persisted_bucket = base_now - Duration::days(config.scoring_window_days.max(1) as i64);
        store.upsert_wallet(
            "wallet_a",
            base_now - Duration::days(4),
            base_now - Duration::days(2),
            "candidate",
        )?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_a".to_string(),
            window_start: persisted_bucket,
            pnl: 2.4,
            win_rate: 0.85,
            trades: 16,
            closed_trades: 8,
            hold_median_seconds: 360,
            score: 0.81,
            buy_total: 8,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
        store.activate_follow_wallet("wallet_a", base_now - Duration::minutes(5), "test-seed")?;
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: base_now - Duration::days(2),
            slot: 42,
            signature: "cursor-short-retention-cadence".to_string(),
        })?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_now = base_now + Duration::minutes(31);
        let first_summary = discovery.run_cycle(&store, first_now)?;
        assert!(
            first_summary.published,
            "first bootstrap tick should publish"
        );
        assert!(
            !store.wallet_metrics_window_exists(discovery.metrics_window_start(first_now))?,
            "bootstrap-only cycle must not write a new wallet_metrics bucket from carried persisted snapshots"
        );

        let second_summary = discovery.run_cycle(&store, first_now + Duration::minutes(1))?;
        assert!(
            !second_summary.published,
            "bootstrap path must still respect refresh_seconds publish cadence"
        );
        assert!(
            !store.wallet_metrics_window_exists(
                discovery.metrics_window_start(first_now + Duration::minutes(1))
            )?,
            "bootstrap follow-up tick must not materialize synthetic wallet_metrics buckets"
        );
        Ok(())
    }

    #[test]
    fn warm_restore_and_cursor_delta_keep_cache_ordered_before_cap_eviction() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("test-ordering-cap.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-04T14:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let start = now - Duration::minutes(40);
        for idx in 0..20 {
            let ts = start + Duration::minutes(idx as i64);
            store.insert_observed_swap(&swap(
                "wallet_mix",
                &format!("mix-sig-{idx:03}"),
                ts,
                SOL_MINT,
                "TokenMix1111111111111111111111111111111111",
                1.0,
                100.0,
            ))?;
        }

        // Simulate persisted cursor far behind recent tail.
        let cursor = DiscoveryRuntimeCursor {
            ts_utc: start + Duration::minutes(5),
            slot: (start + Duration::minutes(5)).timestamp().max(0) as u64,
            signature: "mix-sig-005".to_string(),
        };
        store.upsert_discovery_runtime_cursor(&cursor)?;

        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.max_window_swaps_in_memory = 5;
        config.max_fetch_swaps_per_cycle = 3;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let _ = discovery.run_cycle(&store, now)?;

        let guard = discovery
            .window_state
            .lock()
            .expect("window_state lock should succeed");
        let signatures: Vec<String> = guard
            .swaps
            .iter()
            .map(|swap| swap.signature.clone())
            .collect();
        assert_eq!(signatures.len(), 5);
        assert_eq!(
            signatures,
            vec![
                "mix-sig-015".to_string(),
                "mix-sig-016".to_string(),
                "mix-sig-017".to_string(),
                "mix-sig-018".to_string(),
                "mix-sig-019".to_string(),
            ],
            "cache must keep latest swaps after ordering normalization + cap eviction"
        );
        Ok(())
    }

    #[test]
    fn rug_ratio_treats_unevaluated_buys_as_risky_until_they_mature() {
        let now = DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let buy_ts = now - Duration::seconds(30);

        let mut config = DiscoveryConfig::default();
        config.min_trades = 1;
        config.min_active_days = 1;
        config.min_leader_notional_sol = 0.1;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.max_rug_ratio = 0.60;
        config.rug_lookahead_seconds = 300;
        let discovery = DiscoveryService::new(config, copybot_config::ShadowConfig::default());

        let mut acc = WalletAccumulator::default();
        acc.first_seen = Some(buy_ts);
        acc.last_seen = Some(buy_ts);
        acc.trades = 1;
        acc.max_buy_notional_sol = 1.0;
        acc.active_days.insert(buy_ts.date_naive());
        acc.buy_observations.push(BuyObservation {
            token: "TokenRecent11111111111111111111111111111111".to_string(),
            ts: buy_ts,
            tradable: true,
            quality_resolved: true,
        });

        let snapshot = discovery.snapshot_from_accumulator(
            "wallet_recent".to_string(),
            acc,
            now,
            &HashMap::new(),
        );

        assert!(
            (snapshot.rug_ratio - 1.0).abs() < 1e-9,
            "fresh unevaluated buys must count as risky until lookahead matures"
        );
        assert!(
            !snapshot.eligible,
            "wallet with only unevaluated buys must not pass rug gating as safe"
        );
    }

    #[test]
    fn rug_ratio_uses_total_buy_count_when_some_buys_are_still_unevaluated() {
        let now = DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let mut config = DiscoveryConfig::default();
        config.min_trades = 5;
        config.min_active_days = 1;
        config.min_leader_notional_sol = 0.1;
        config.min_buy_count = 5;
        config.min_tradable_ratio = 0.0;
        config.max_rug_ratio = 0.60;
        config.rug_lookahead_seconds = 300;
        config.thin_market_min_volume_sol = 1.0;
        config.thin_market_min_unique_traders = 1;
        let discovery = DiscoveryService::new(config, copybot_config::ShadowConfig::default());

        let mut acc = WalletAccumulator::default();
        acc.trades = 5;
        acc.max_buy_notional_sol = 1.0;

        let mut token_sol_history = HashMap::new();
        for idx in 0..4 {
            let buy_ts = now - Duration::minutes(20 + idx as i64);
            let token = format!("TokenMature{idx:02}");
            if acc.first_seen.is_none() {
                acc.first_seen = Some(buy_ts);
            }
            acc.last_seen = Some(
                acc.last_seen
                    .map(|current| current.max(buy_ts))
                    .unwrap_or(buy_ts),
            );
            acc.active_days.insert(buy_ts.date_naive());
            acc.buy_observations.push(BuyObservation {
                token: token.clone(),
                ts: buy_ts,
                tradable: true,
                quality_resolved: true,
            });
            token_sol_history.insert(
                token,
                vec![SolLegTrade {
                    ts: buy_ts + Duration::seconds(30),
                    wallet_id: format!("wallet-{idx}"),
                    sol_notional: 2.0,
                }],
            );
        }

        let fresh_buy_ts = now - Duration::seconds(60);
        acc.last_seen = Some(
            acc.last_seen
                .map(|current| current.max(fresh_buy_ts))
                .unwrap_or(fresh_buy_ts),
        );
        acc.active_days.insert(fresh_buy_ts.date_naive());
        acc.buy_observations.push(BuyObservation {
            token: "TokenFresh999999999999999999999999999999999".to_string(),
            ts: fresh_buy_ts,
            tradable: true,
            quality_resolved: true,
        });

        let snapshot = discovery.snapshot_from_accumulator(
            "wallet_mixed".to_string(),
            acc,
            now,
            &token_sol_history,
        );

        assert!(
            (snapshot.rug_ratio - 0.2).abs() < 1e-9,
            "one fresh buy out of the total buys must contribute to rug_ratio denominator"
        );
        assert!(
            snapshot.eligible,
            "a mostly healthy wallet should remain eligible when unevaluated buys stay below max_rug_ratio"
        );
    }

    #[test]
    fn tradable_ratio_soft_penalizes_deferred_quality_buys() {
        let now = DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let mut config = DiscoveryConfig::default();
        config.min_trades = 3;
        config.min_active_days = 1;
        config.min_leader_notional_sol = 0.1;
        config.min_buy_count = 3;
        config.min_tradable_ratio = 0.5;
        config.max_rug_ratio = 1.0;
        let discovery = DiscoveryService::new(config, copybot_config::ShadowConfig::default());

        let mut acc = WalletAccumulator::default();
        acc.first_seen = Some(now - Duration::minutes(10));
        acc.last_seen = Some(now);
        acc.trades = 3;
        acc.max_buy_notional_sol = 1.0;
        acc.active_days.insert(now.date_naive());
        acc.buy_observations.push(BuyObservation {
            token: "TokenTradable111111111111111111111111111111".to_string(),
            ts: now - Duration::minutes(10),
            tradable: true,
            quality_resolved: true,
        });
        acc.buy_observations.push(BuyObservation {
            token: "TokenRejected111111111111111111111111111111".to_string(),
            ts: now - Duration::minutes(9),
            tradable: false,
            quality_resolved: true,
        });
        acc.buy_observations.push(BuyObservation {
            token: "TokenDeferred11111111111111111111111111111".to_string(),
            ts: now - Duration::minutes(8),
            tradable: false,
            quality_resolved: false,
        });

        let snapshot = discovery.snapshot_from_accumulator(
            "wallet_tradability".to_string(),
            acc,
            now,
            &HashMap::new(),
        );

        let expected = 0.5 * (2.0_f64 / 3.0).sqrt();
        assert!(
            (snapshot.tradable_ratio - expected).abs() < 1e-9,
            "deferred buys must apply a soft penalty to tradable_ratio"
        );
        assert!(
            !snapshot.eligible,
            "deferred buys should no longer be neutral for min_tradable_ratio eligibility"
        );
    }
