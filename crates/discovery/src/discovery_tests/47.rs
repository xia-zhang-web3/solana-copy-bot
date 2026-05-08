fn seed_published_wallet_metrics_snapshot(
        store: &SqliteStore,
        metrics_window_start: DateTime<Utc>,
        eligible_wallets: usize,
        active_wallets: usize,
    ) -> Result<HashSet<String>> {
        let mut published_active_wallets = HashSet::new();
        for idx in 0..eligible_wallets {
            let wallet_id = format!("wallet_published_{idx:03}");
            store.upsert_wallet(
                &wallet_id,
                metrics_window_start - Duration::days(1),
                metrics_window_start + Duration::minutes(idx as i64),
                "candidate",
            )?;
            store.insert_wallet_metric(&WalletMetricRow {
                wallet_id: wallet_id.clone(),
                window_start: metrics_window_start,
                pnl: 2.0 + idx as f64 * 0.001,
                win_rate: 0.8,
                trades: 6,
                closed_trades: 6,
                hold_median_seconds: 120,
                score: 1.0 - idx as f64 * 0.001,
                buy_total: 6,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            })?;
            if idx < active_wallets {
                store.activate_follow_wallet(
                    &wallet_id,
                    metrics_window_start + Duration::minutes(idx as i64),
                    "seed-follow",
                )?;
                published_active_wallets.insert(wallet_id);
            }
        }
        Ok(published_active_wallets)
    }

    fn stage1_runtime_config() -> DiscoveryConfig {
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.decay_window_days = 7;
        config.observed_swaps_retention_days = 14;
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
        config.max_fetch_pages_per_cycle = 10;
        config.fetch_time_budget_ms = 1_000;
        config.thin_market_min_unique_traders = 1;
        config
    }

    fn live_shadow_blocker_discovery_config_for_tests() -> DiscoveryConfig {
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 2;
        config.decay_window_days = 2;
        config.observed_swaps_retention_days = 7;
        config.follow_top_n = 15;
        config.min_leader_notional_sol = 0.5;
        config.min_trades = 10;
        config.min_active_days = 3;
        config.min_score = 0.4;
        config.max_tx_per_minute = 50;
        config.min_buy_count = 10;
        config.min_tradable_ratio = 0.25;
        config.max_rug_ratio = 1.0;
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_window_swaps_in_memory = 100_000;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        config.thin_market_min_volume_sol = 0.0;
        config.thin_market_min_unique_traders = 0;
        config
    }

    fn bounded_stage1_runtime_config() -> DiscoveryConfig {
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 5;
        config.max_fetch_pages_per_cycle = 1;
        config.fetch_time_budget_ms = 60_000;
        config
    }

    fn metrics_window_start_for_test(
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
    ) -> DateTime<Utc> {
        let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
    }

    fn assert_sorted_strings(values: &[String]) {
        assert!(
            values
                .windows(2)
                .all(|pair| pair[0].as_str() <= pair[1].as_str()),
            "expected canonical sorted strings, got {values:?}"
        );
    }

    fn buy_observations_for_quality_mix(
        now: DateTime<Utc>,
        buy_total: u32,
        quality_resolved_buys: u32,
        tradable_buys: u32,
    ) -> Vec<BuyObservation> {
        (0..buy_total)
            .map(|idx| BuyObservation {
                token: format!("TokenLivePublishGate{idx:02}11111111111111111111111"),
                ts: now - Duration::minutes(i64::from(buy_total.saturating_sub(idx))),
                tradable: idx < tradable_buys,
                quality_resolved: idx < quality_resolved_buys,
            })
            .collect()
    }

    fn live_publish_gate_wallet_accumulator(
        now: DateTime<Utc>,
        trades: u32,
        active_days: u32,
        buy_total: u32,
        quality_resolved_buys: u32,
        tradable_buys: u32,
        realized_pnl_sol: f64,
        wins: u32,
        closed_trades: u32,
        hold_median_seconds: i64,
        max_buy_notional_sol: f64,
    ) -> WalletAccumulator {
        let mut acc = WalletAccumulator::default();
        acc.first_seen = Some(now - Duration::days(4));
        acc.last_seen = Some(now - Duration::minutes(5));
        acc.trades = trades;
        acc.exact_active_day_count = Some(active_days);
        acc.spent_sol = 10.0;
        acc.realized_pnl_sol = realized_pnl_sol;
        acc.max_buy_notional_sol = max_buy_notional_sol;
        acc.wins = wins;
        acc.closed_trades = closed_trades;
        acc.hold_samples_sec = vec![hold_median_seconds; closed_trades.max(1) as usize];
        for idx in 0..active_days {
            acc.realized_pnl_by_day.insert(
                (now - Duration::days(idx as i64)).date_naive(),
                if realized_pnl_sol >= 0.0 { 1.0 } else { -0.5 },
            );
        }
        acc.buy_observations =
            buy_observations_for_quality_mix(now, buy_total, quality_resolved_buys, tradable_buys);
        acc
    }

    fn live_publish_gate_fixture_wallets(now: DateTime<Utc>) -> Vec<(String, WalletAccumulator)> {
        let mut wallets = Vec::new();
        for idx in 0..10 {
            wallets.push((
                format!("wallet_pass_{idx:02}"),
                live_publish_gate_wallet_accumulator(now, 12, 3, 10, 10, 10, 2.5, 7, 10, 120, 1.0),
            ));
        }
        wallets.push((
            "wallet_fail_min_active_days".to_string(),
            live_publish_gate_wallet_accumulator(now, 12, 2, 10, 10, 10, 2.5, 7, 10, 120, 1.0),
        ));
        wallets.push((
            "wallet_fail_min_buy_count".to_string(),
            live_publish_gate_wallet_accumulator(now, 12, 3, 9, 9, 9, 2.5, 7, 10, 120, 1.0),
        ));
        wallets.push((
            "wallet_fail_tradable_ratio".to_string(),
            live_publish_gate_wallet_accumulator(now, 12, 3, 10, 4, 1, 2.5, 7, 10, 120, 1.0),
        ));
        wallets.push((
            "wallet_fail_score".to_string(),
            live_publish_gate_wallet_accumulator(now, 12, 3, 10, 10, 10, -2.0, 0, 10, 10, 1.0),
        ));
        wallets.push((
            "wallet_fail_min_leader_notional".to_string(),
            live_publish_gate_wallet_accumulator(now, 12, 3, 10, 10, 10, 2.5, 7, 10, 120, 0.4),
        ));
        let mut fail_decay =
            live_publish_gate_wallet_accumulator(now, 12, 3, 10, 10, 10, 2.5, 7, 10, 120, 1.0);
        fail_decay.last_seen = Some(now - Duration::days(6));
        wallets.push(("wallet_fail_decay".to_string(), fail_decay));
        let mut fail_suspicious =
            live_publish_gate_wallet_accumulator(now, 12, 3, 10, 10, 10, 2.5, 7, 10, 120, 1.0);
        fail_suspicious.suspicious = true;
        wallets.push(("wallet_fail_suspicious".to_string(), fail_suspicious));
        wallets
    }

    fn live_publish_gate_fixture_wallets_with_decay_boundary(
        now: DateTime<Utc>,
    ) -> Vec<(String, WalletAccumulator)> {
        let mut wallets = live_publish_gate_fixture_wallets(now);
        let (_, acc) = wallets
            .iter_mut()
            .find(|(wallet_id, _)| wallet_id == "wallet_pass_09")
            .expect("borderline decay fixture wallet should exist");
        acc.first_seen = Some(now - Duration::days(2) + Duration::minutes(10));
        acc.last_seen = Some(now - Duration::days(2) + Duration::minutes(50));
        wallets
    }

    fn refill_drain_wallet_accumulator(
        now: DateTime<Utc>,
        open_lot_age: Duration,
        hold_median_seconds: i64,
        token: &str,
    ) -> WalletAccumulator {
        let mut acc = WalletAccumulator::default();
        acc.first_seen = Some(now - Duration::days(4));
        acc.last_seen = Some(now - open_lot_age);
        acc.trades = 25;
        acc.exact_active_day_count = Some(4);
        acc.spent_sol = 13.0;
        acc.realized_pnl_sol = 4.5;
        acc.max_buy_notional_sol = 1.1;
        acc.wins = 10;
        acc.closed_trades = 12;
        acc.hold_samples_sec = vec![hold_median_seconds; acc.closed_trades as usize];
        for idx in 0..4 {
            acc.realized_pnl_by_day
                .insert((now - Duration::days(idx)).date_naive(), 1.0);
        }
        acc.buy_total = 13;
        acc.quality_resolved_buys = 13;
        acc.tradable_buys = 13;
        acc.rug_metrics = RugMetrics {
            evaluated: 13,
            rugged: 0,
            unevaluated: 0,
        };
        acc.positions
            .entry(token.to_string())
            .or_default()
            .push_back(Lot {
                qty: 100.0,
                cost_sol: 1.0,
                opened_at: now - open_lot_age,
            });
        acc
    }

    fn refill_drain_open_lot_fixture_wallets(
        now: DateTime<Utc>,
        include_independent_wallets: bool,
    ) -> (Vec<(String, WalletAccumulator)>, Vec<String>, Vec<String>) {
        let stale_cluster_wallet_ids: Vec<String> = (0..7)
            .map(|idx| format!("wallet_refill_drain_cluster_{idx:02}"))
            .collect();
        let independent_wallet_ids: Vec<String> = if include_independent_wallets {
            (0..3)
                .map(|idx| format!("wallet_independent_recent_open_{idx:02}"))
                .collect()
        } else {
            Vec::new()
        };
        let mut entries = Vec::new();
        for wallet_id in &stale_cluster_wallet_ids {
            entries.push((
                wallet_id.clone(),
                refill_drain_wallet_accumulator(
                    now,
                    Duration::hours(2),
                    5 * 60,
                    "TokenRefillDrainCluster1111111111111111111",
                ),
            ));
        }
        for wallet_id in &independent_wallet_ids {
            entries.push((
                wallet_id.clone(),
                refill_drain_wallet_accumulator(
                    now,
                    Duration::minutes(20),
                    20 * 60,
                    "TokenIndependentCarry111111111111111111111",
                ),
            ));
        }
        (entries, stale_cluster_wallet_ids, independent_wallet_ids)
    }
