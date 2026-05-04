    fn desired_wallets_from_live_publish_gate_fixture(
        config: DiscoveryConfig,
        entries: &[(String, WalletAccumulator)],
        now: DateTime<Utc>,
    ) -> (Vec<WalletSnapshot>, Vec<String>) {
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let snapshots = entries
            .iter()
            .map(|(wallet_id, acc)| {
                discovery.snapshot_from_accumulator(
                    wallet_id.clone(),
                    acc.clone(),
                    now,
                    &HashMap::new(),
                )
            })
            .collect::<Vec<_>>();
        let ranked = rank_follow_candidates(&snapshots, config.min_score);
        let desired = desired_wallets(&ranked, config.follow_top_n);
        (snapshots, desired)
    }

    fn desired_wallets_from_refill_drain_fixture_with_open_position_semantics(
        config: DiscoveryConfig,
        entries: &[(String, WalletAccumulator)],
        now: DateTime<Utc>,
        require_actionable_open_positions: bool,
    ) -> (Vec<WalletSnapshot>, Vec<String>) {
        let mut ungated_config = config.clone();
        ungated_config.require_open_positions_for_publication = false;
        let discovery = DiscoveryService::new(ungated_config, permissive_shadow_quality());
        let snapshots = entries
            .iter()
            .map(|(wallet_id, acc)| {
                let mut snapshot = discovery.snapshot_from_accumulator(
                    wallet_id.clone(),
                    acc.clone(),
                    now,
                    &HashMap::new(),
                );
                if config.require_open_positions_for_publication {
                    let gate_passed = if require_actionable_open_positions {
                        acc.has_actionable_open_positions(
                            now,
                            config.metric_snapshot_interval_seconds,
                        )
                    } else {
                        acc.has_open_positions()
                    };
                    if !gate_passed {
                        snapshot.eligible = false;
                        snapshot.score = 0.0;
                    }
                }
                snapshot
            })
            .collect::<Vec<_>>();
        let ranked = rank_follow_candidates(&snapshots, config.min_score);
        let desired = desired_wallets(&ranked, config.follow_top_n);
        (snapshots, desired)
    }

    fn old_actionable_open_position_gate_passes(
        acc: &WalletAccumulator,
        now: DateTime<Utc>,
        metric_snapshot_interval_seconds: u64,
    ) -> bool {
        let cadence_floor_seconds =
            i64::try_from(metric_snapshot_interval_seconds.max(1)).unwrap_or(i64::MAX);
        let historical_hold_allowance_seconds = acc
            .hold_samples_sec
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
            .saturating_mul(ACTIONABLE_OPEN_POSITION_HOLD_MULTIPLIER);
        let max_open_age_seconds = cadence_floor_seconds.max(historical_hold_allowance_seconds);
        acc.positions.values().flatten().any(|lot| {
            lot.qty > 1e-12
                && lot.cost_sol > 1e-12
                && (now - lot.opened_at).num_seconds().max(0) <= max_open_age_seconds
        })
    }

    fn leader_with_open_position_and_hold_history(
        now: DateTime<Utc>,
        open_lot_age: Duration,
        closed_trades: u32,
        hold_samples: &[i64],
    ) -> WalletAccumulator {
        let mut acc = WalletAccumulator::default();
        acc.first_seen = Some(now - Duration::days(4));
        acc.last_seen = Some(now - Duration::minutes(5));
        acc.trades = 18;
        acc.exact_active_day_count = Some(4);
        acc.spent_sol = 9.0;
        acc.realized_pnl_sol = 2.5;
        acc.max_buy_notional_sol = 1.0;
        acc.wins = closed_trades.min(6);
        acc.closed_trades = closed_trades;
        acc.hold_samples_sec = hold_samples.to_vec();
        for idx in 0..4 {
            acc.realized_pnl_by_day
                .insert((now - Duration::days(idx)).date_naive(), 0.8);
        }
        acc.buy_total = 12;
        acc.quality_resolved_buys = 12;
        acc.tradable_buys = 12;
        acc.rug_metrics = RugMetrics {
            evaluated: 12,
            rugged: 0,
            unevaluated: 0,
        };
        acc.positions
            .entry("TokenLegitCarry1111111111111111111111111".to_string())
            .or_default()
            .push_back(Lot {
                qty: 100.0,
                cost_sol: 1.0,
                opened_at: now - open_lot_age,
            });
        acc
    }

    fn prewindow_carry_leader_scoring_window_accumulator(now: DateTime<Utc>) -> WalletAccumulator {
        let mut acc = live_publish_gate_wallet_accumulator(
            now,
            20,
            4,
            10,
            10,
            10,
            4.5,
            8,
            10,
            48 * 60 * 60,
            1.0,
        );
        acc.first_seen = Some(now - Duration::days(6));
        acc.last_seen = Some(now - Duration::hours(20));
        acc.buy_total = 10;
        acc.quality_resolved_buys = 10;
        acc.tradable_buys = 10;
        acc.buy_observations.clear();
        acc.rug_metrics = RugMetrics {
            evaluated: 10,
            rugged: 0,
            unevaluated: 0,
        };
        acc.positions.clear();
        acc
    }

    fn seed_prewindow_carry_leader_position_history(
        store: &SqliteStore,
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
        wallet_ids: &[String],
    ) -> Result<()> {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        for (wallet_idx, wallet_id) in wallet_ids.iter().enumerate() {
            let carry_token = format!("TokenCarryAnchor{wallet_idx:02}111111111111111111111");
            let cycle_token = format!("TokenCarryCycle{wallet_idx:02}1111111111111111111111");
            store.insert_observed_swap(&swap(
                wallet_id,
                &format!("stage1-prewindow-carry-buy-{wallet_idx}"),
                window_start - Duration::hours(18) + Duration::minutes(wallet_idx as i64),
                SOL_MINT,
                &carry_token,
                1.2,
                120.0,
            ))?;
            for round in 0..10 {
                let buy_ts = window_start
                    + Duration::hours((round * 4) as i64 + 1)
                    + Duration::minutes(wallet_idx as i64);
                let sell_ts = buy_ts + Duration::hours(48);
                store.insert_observed_swap(&swap(
                    wallet_id,
                    &format!("stage1-carry-cycle-buy-{wallet_idx}-{round}"),
                    buy_ts,
                    SOL_MINT,
                    &cycle_token,
                    1.0,
                    100.0,
                ))?;
                store.insert_observed_swap(&swap(
                    wallet_id,
                    &format!("stage1-carry-cycle-sell-{wallet_idx}-{round}"),
                    sell_ts,
                    &cycle_token,
                    SOL_MINT,
                    100.0,
                    1.25,
                ))?;
            }
        }
        Ok(())
    }

    fn long_horizon_carry_vs_refill_drain_fixture_swaps(
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
    ) -> (Vec<SwapEvent>, Vec<String>, Vec<String>) {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let legit_wallet_ids: Vec<String> = (0..3)
            .map(|idx| format!("wallet_legit_long_horizon_carry_{idx:02}"))
            .collect();
        let junk_wallet_ids: Vec<String> = (0..7)
            .map(|idx| format!("wallet_refill_drain_stale_{idx:02}"))
            .collect();
        let mut swaps = Vec::new();

        for (wallet_idx, wallet_id) in legit_wallet_ids.iter().enumerate() {
            let carry_token =
                format!("TokenLongHorizonCarry{wallet_idx:02}11111111111111111111111");
            let cycle_token =
                format!("TokenLongHorizonCycle{wallet_idx:02}11111111111111111111111");
            swaps.push(swap(
                wallet_id,
                &format!("legit-long-carry-open-{wallet_idx}"),
                now - Duration::days(9) + Duration::minutes(wallet_idx as i64),
                SOL_MINT,
                carry_token.as_str(),
                1.2,
                120.0,
            ));
            for round in 0..10 {
                let buy_ts = window_start
                    + Duration::hours((round * 6) as i64 + 1)
                    + Duration::minutes(wallet_idx as i64);
                let sell_ts = buy_ts + Duration::hours(60);
                swaps.push(swap(
                    wallet_id,
                    &format!("legit-long-cycle-buy-{wallet_idx}-{round}"),
                    buy_ts,
                    SOL_MINT,
                    cycle_token.as_str(),
                    1.0,
                    100.0,
                ));
                for noise_idx in 0..10 {
                    swaps.push(swap(
                        &format!(
                            "wallet_legit_long_cycle_noise_{wallet_idx:02}_{round:02}_{noise_idx:02}"
                        ),
                        &format!("legit-long-cycle-noise-{wallet_idx}-{round}-{noise_idx}"),
                        buy_ts + Duration::seconds(30 + noise_idx as i64),
                        SOL_MINT,
                        cycle_token.as_str(),
                        0.4,
                        40.0,
                    ));
                }
                swaps.push(swap(
                    wallet_id,
                    &format!("legit-long-cycle-sell-{wallet_idx}-{round}"),
                    sell_ts,
                    cycle_token.as_str(),
                    SOL_MINT,
                    100.0,
                    1.3,
                ));
            }
        }

        for (wallet_idx, wallet_id) in junk_wallet_ids.iter().enumerate() {
            let junk_token =
                format!("TokenRefillDrainStale{wallet_idx:02}111111111111111111111111");
            for round in 0..10 {
                let buy_ts = window_start
                    + Duration::hours((round * 8) as i64 + 2)
                    + Duration::minutes(wallet_idx as i64);
                let sell_ts = buy_ts + Duration::minutes(5);
                swaps.push(swap(
                    wallet_id,
                    &format!("junk-refill-drain-buy-{wallet_idx}-{round}"),
                    buy_ts,
                    SOL_MINT,
                    junk_token.as_str(),
                    1.0,
                    100.0,
                ));
                for noise_idx in 0..10 {
                    swaps.push(swap(
                        &format!(
                            "wallet_junk_refill_drain_noise_{wallet_idx:02}_{round:02}_{noise_idx:02}"
                        ),
                        &format!("junk-refill-drain-noise-{wallet_idx}-{round}-{noise_idx}"),
                        buy_ts + Duration::seconds(30 + noise_idx as i64),
                        SOL_MINT,
                        junk_token.as_str(),
                        0.4,
                        40.0,
                    ));
                }
                swaps.push(swap(
                    wallet_id,
                    &format!("junk-refill-drain-sell-{wallet_idx}-{round}"),
                    sell_ts,
                    junk_token.as_str(),
                    SOL_MINT,
                    100.0,
                    1.25,
                ));
            }
            let stale_open_buy_ts = now - Duration::hours(2) + Duration::minutes(wallet_idx as i64);
            swaps.push(swap(
                wallet_id,
                &format!("junk-refill-drain-stale-open-{wallet_idx}"),
                stale_open_buy_ts,
                SOL_MINT,
                junk_token.as_str(),
                1.0,
                100.0,
            ));
            for noise_idx in 0..10 {
                swaps.push(swap(
                    &format!("wallet_junk_refill_drain_stale_noise_{wallet_idx:02}_{noise_idx:02}"),
                    &format!("junk-refill-drain-stale-noise-{wallet_idx}-{noise_idx}"),
                    stale_open_buy_ts + Duration::seconds(30 + noise_idx as i64),
                    SOL_MINT,
                    junk_token.as_str(),
                    0.4,
                    40.0,
                ));
            }
        }

        swaps.sort_by(|a, b| {
            a.ts_utc
                .cmp(&b.ts_utc)
                .then_with(|| a.signature.cmp(&b.signature))
        });
        (swaps, legit_wallet_ids, junk_wallet_ids)
    }

    fn omit_nonfollowed_legit_market_context_swaps(swaps: &[SwapEvent]) -> Vec<SwapEvent> {
        swaps
            .iter()
            .filter(|swap| !swap.wallet.starts_with("wallet_legit_long_cycle_noise_"))
            .cloned()
            .collect()
    }

    fn insert_observed_swaps_and_seed_runtime_cursor(
        store: &SqliteStore,
        swaps: &[SwapEvent],
    ) -> Result<()> {
        let mut latest_cursor = None;
        for swap in swaps {
            latest_cursor = Some(DiscoveryRuntimeCursor {
                ts_utc: swap.ts_utc,
                slot: swap.slot,
                signature: swap.signature.clone(),
            });
            store.insert_observed_swap(swap)?;
        }
        store.upsert_discovery_runtime_cursor(
            &latest_cursor.expect("fixture swaps should include a latest cursor"),
        )?;
        Ok(())
    }

    #[derive(Debug, PartialEq, Eq)]
    struct LivePublishGateAttributionCounts {
        total_snapshots: usize,
        removed_by_min_trades: usize,
        removed_by_min_active_days: usize,
        removed_by_suspicious: usize,
        removed_by_min_leader_notional_sol: usize,
        removed_by_decay_window_days: usize,
        removed_by_min_buy_count: usize,
        removed_by_min_tradable_ratio: usize,
        removed_by_min_score: usize,
        published_wallets: usize,
    }

    fn live_publish_gate_attribution_counts(
        config: &DiscoveryConfig,
        entries: &[(String, WalletAccumulator)],
        snapshots: &[WalletSnapshot],
        now: DateTime<Utc>,
    ) -> LivePublishGateAttributionCounts {
        let snapshot_by_wallet: HashMap<&str, &WalletSnapshot> = snapshots
            .iter()
            .map(|snapshot| (snapshot.wallet_id.as_str(), snapshot))
            .collect();
        let total_snapshots = entries.len();
        let mut remaining: Vec<(&String, &WalletAccumulator)> = entries
            .iter()
            .map(|(wallet_id, acc)| (wallet_id, acc))
            .collect();

        let before = remaining.len();
        remaining.retain(|(_, acc)| acc.trades >= config.min_trades);
        let removed_by_min_trades = before - remaining.len();

        let before = remaining.len();
        remaining.retain(|(_, acc)| {
            let active_days = acc
                .exact_active_day_count
                .unwrap_or(acc.active_days.len() as u32);
            active_days >= config.min_active_days
        });
        let removed_by_min_active_days = before - remaining.len();

        let before = remaining.len();
        remaining.retain(|(_, acc)| !acc.suspicious);
        let removed_by_suspicious = before - remaining.len();

        let before = remaining.len();
        remaining.retain(|(_, acc)| acc.max_buy_notional_sol >= config.min_leader_notional_sol);
        let removed_by_min_leader_notional_sol = before - remaining.len();

        let decay_cutoff = now - Duration::days(config.decay_window_days.max(1) as i64);
        let before = remaining.len();
        remaining.retain(|(_, acc)| acc.last_seen.unwrap_or(now) >= decay_cutoff);
        let removed_by_decay_window_days = before - remaining.len();

        let before = remaining.len();
        remaining.retain(|(wallet_id, _)| {
            snapshot_by_wallet
                .get(wallet_id.as_str())
                .is_some_and(|snapshot| snapshot.buy_total >= config.min_buy_count)
        });
        let removed_by_min_buy_count = before - remaining.len();

        let before = remaining.len();
        remaining.retain(|(wallet_id, _)| {
            snapshot_by_wallet
                .get(wallet_id.as_str())
                .is_some_and(|snapshot| snapshot.tradable_ratio >= config.min_tradable_ratio)
        });
        let removed_by_min_tradable_ratio = before - remaining.len();

        let before = remaining.len();
        remaining.retain(|(wallet_id, _)| {
            snapshot_by_wallet
                .get(wallet_id.as_str())
                .is_some_and(|snapshot| snapshot.score >= config.min_score)
        });
        let removed_by_min_score = before - remaining.len();

        LivePublishGateAttributionCounts {
            total_snapshots,
            removed_by_min_trades,
            removed_by_min_active_days,
            removed_by_suspicious,
            removed_by_min_leader_notional_sol,
            removed_by_decay_window_days,
            removed_by_min_buy_count,
            removed_by_min_tradable_ratio,
            removed_by_min_score,
            published_wallets: remaining.len(),
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
    enum LivePublishGateRelaxation {
        MinActiveDays,
        MinLeaderNotionalSol,
        DecayWindowDays,
        MinBuyCount,
        MinTradableRatio,
        MinScore,
        Suspicious,
    }

    impl LivePublishGateRelaxation {
        fn label(self) -> &'static str {
            match self {
                Self::MinActiveDays => "min_active_days",
                Self::MinLeaderNotionalSol => "min_leader_notional_sol",
                Self::DecayWindowDays => "decay_window_days",
                Self::MinBuyCount => "min_buy_count",
                Self::MinTradableRatio => "min_tradable_ratio",
                Self::MinScore => "min_score",
                Self::Suspicious => "suspicious",
            }
        }
    }

    fn relaxed_live_shadow_blocker_config_for_tests(
        base: &DiscoveryConfig,
        relaxations: &[LivePublishGateRelaxation],
    ) -> DiscoveryConfig {
        let mut config = base.clone();
        for relaxation in relaxations {
            match relaxation {
                LivePublishGateRelaxation::MinActiveDays => config.min_active_days = 2,
                LivePublishGateRelaxation::MinLeaderNotionalSol => {
                    config.min_leader_notional_sol = 0.4;
                }
                LivePublishGateRelaxation::DecayWindowDays => config.decay_window_days = 7,
                LivePublishGateRelaxation::MinBuyCount => config.min_buy_count = 9,
                LivePublishGateRelaxation::MinTradableRatio => config.min_tradable_ratio = 0.0,
                LivePublishGateRelaxation::MinScore => config.min_score = 0.0,
                LivePublishGateRelaxation::Suspicious => {}
            }
        }
        config
    }

    fn live_publish_gate_fixture_with_relaxations(
        base_entries: &[(String, WalletAccumulator)],
        relaxations: &[LivePublishGateRelaxation],
    ) -> Vec<(String, WalletAccumulator)> {
        let mut entries = base_entries.to_vec();
        if relaxations.contains(&LivePublishGateRelaxation::Suspicious) {
            let (_, suspicious_acc) = entries
                .iter_mut()
                .find(|(wallet_id, _)| wallet_id == "wallet_fail_suspicious")
                .expect("suspicious fixture wallet should exist");
            suspicious_acc.suspicious = false;
        }
        entries
    }

    fn live_publish_gate_desired_wallets_for_relaxations(
        base_config: &DiscoveryConfig,
        base_entries: &[(String, WalletAccumulator)],
        relaxations: &[LivePublishGateRelaxation],
        now: DateTime<Utc>,
    ) -> Vec<String> {
        let config = relaxed_live_shadow_blocker_config_for_tests(base_config, relaxations);
        let entries = live_publish_gate_fixture_with_relaxations(base_entries, relaxations);
        let (_, desired) = desired_wallets_from_live_publish_gate_fixture(config, &entries, now);
        desired
    }

    fn live_restored_rug_policy_discovery_config_for_tests() -> DiscoveryConfig {
        let mut config = live_shadow_blocker_discovery_config_for_tests();
        let defaults = DiscoveryConfig::default();
        config.max_rug_ratio = defaults.max_rug_ratio;
        config.thin_market_min_volume_sol = defaults.thin_market_min_volume_sol;
        config.thin_market_min_unique_traders = defaults.thin_market_min_unique_traders;
        config
    }
