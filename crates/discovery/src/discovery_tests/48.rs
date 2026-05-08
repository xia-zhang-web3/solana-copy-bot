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
