    #[test]
    fn live_like_small_published_universe_requires_five_policy_surfaces_to_reach_fifteen_stage1() {
        let now = DateTime::parse_from_rfc3339("2026-04-06T06:37:24Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = live_shadow_blocker_discovery_config_for_tests();
        let entries = live_publish_gate_fixture_wallets(now);
        let relaxable_surfaces = [
            LivePublishGateRelaxation::MinActiveDays,
            LivePublishGateRelaxation::MinLeaderNotionalSol,
            LivePublishGateRelaxation::DecayWindowDays,
            LivePublishGateRelaxation::MinBuyCount,
            LivePublishGateRelaxation::MinTradableRatio,
            LivePublishGateRelaxation::MinScore,
            LivePublishGateRelaxation::Suspicious,
        ];

        let mut smallest_surface_count_reaching_fifteen: Option<usize> = None;
        let mut five_surface_combos_reaching_fifteen = Vec::new();
        for mask in 1usize..(1usize << relaxable_surfaces.len()) {
            let relaxations = relaxable_surfaces
                .iter()
                .enumerate()
                .filter_map(|(idx, relaxation)| {
                    ((mask & (1usize << idx)) != 0).then_some(*relaxation)
                })
                .collect::<Vec<_>>();
            let desired = live_publish_gate_desired_wallets_for_relaxations(
                &config,
                &entries,
                &relaxations,
                now,
            );
            if desired.len() >= 15 {
                smallest_surface_count_reaching_fifteen = Some(
                    smallest_surface_count_reaching_fifteen
                        .map_or(relaxations.len(), |best| best.min(relaxations.len())),
                );
                if relaxations.len() == 5 {
                    five_surface_combos_reaching_fifteen.push(
                        relaxations
                            .iter()
                            .map(|relaxation| relaxation.label())
                            .collect::<Vec<_>>(),
                    );
                }
            }
        }

        assert_eq!(
            smallest_surface_count_reaching_fifteen,
            Some(5),
            "there is no one/two/three/four-surface discovery policy tweak that reaches 15+ on the live-like reduced field; the minimal path is already multiple surfaces wide"
        );
        assert!(
            five_surface_combos_reaching_fifteen.iter().any(|combo| combo
                == &vec![
                    "min_active_days",
                    "min_leader_notional_sol",
                    "decay_window_days",
                    "min_buy_count",
                    "min_score",
                ]),
            "at least one minimal path to 15+ must avoid touching the suspicious/spam gate entirely"
        );
    }

    #[test]
    fn live_like_small_published_universe_can_reach_fifteen_without_relaxing_spam_or_tradability_stage1(
    ) {
        let now = DateTime::parse_from_rfc3339("2026-04-06T06:37:24Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = live_shadow_blocker_discovery_config_for_tests();
        let entries = live_publish_gate_fixture_wallets(now);
        let desired = live_publish_gate_desired_wallets_for_relaxations(
            &config,
            &entries,
            &[
                LivePublishGateRelaxation::MinActiveDays,
                LivePublishGateRelaxation::MinLeaderNotionalSol,
                LivePublishGateRelaxation::DecayWindowDays,
                LivePublishGateRelaxation::MinBuyCount,
                LivePublishGateRelaxation::MinScore,
            ],
            now,
        );

        assert_eq!(
            desired.len(),
            15,
            "a minimal live-like path to 15 should exist without relaxing the suspicious gate or the tradability hard floor"
        );
        assert!(
            desired.contains(&"wallet_fail_min_active_days".to_string()),
            "lowering min_active_days should recover the active-days miss"
        );
        assert!(
            desired.contains(&"wallet_fail_min_leader_notional".to_string()),
            "lowering min_leader_notional_sol should recover the notional miss"
        );
        assert!(
            desired.contains(&"wallet_fail_decay".to_string()),
            "widening decay_window_days should recover the recency miss"
        );
        assert!(
            desired.contains(&"wallet_fail_min_buy_count".to_string()),
            "lowering min_buy_count should recover the buy-count miss"
        );
        assert!(
            desired.contains(&"wallet_fail_score".to_string()),
            "lowering min_score should recover the score-only miss"
        );
        assert!(
            !desired.contains(&"wallet_fail_suspicious".to_string()),
            "the spam/suspicious gate can remain intact on this minimal 15-wallet path"
        );
        assert!(
            !desired.contains(&"wallet_fail_tradable_ratio".to_string()),
            "the tradability floor can remain intact on this minimal 15-wallet path"
        );
    }

    #[test]
    fn live_like_clustered_fully_liquidated_wallets_publish_under_current_rug_disabled_policy_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("clustered-fully-liquidated-wallets-publish-under-live-policy.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-06T10:26:22Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = live_shadow_blocker_discovery_config_for_tests();
        let (swaps, clustered_wallet_ids, independent_wallet_ids) =
            clustered_thin_market_fixture_swaps(now, false);
        assert!(
            independent_wallet_ids.is_empty(),
            "this exact failure-class repro should isolate only the clustered drained cohort"
        );

        let (snapshots, desired, entries, _) =
            desired_wallets_from_clustered_thin_market_fixture(config.clone(), &swaps, now);
        assert_eq!(
            desired.len(),
            clustered_wallet_ids.len(),
            "with the current live policy surfaces, the fully liquidated clustered cohort should still become the entire published universe"
        );
        assert_eq!(
            desired, clustered_wallet_ids,
            "current live policy should publish the exact clustered cohort because top-N is not binding and rug/thin-market gating is disabled"
        );

        for wallet_id in &clustered_wallet_ids {
            let snapshot = snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("clustered wallet snapshot should exist");
            assert!(
                snapshot.eligible,
                "the clustered wallet should still satisfy current historical eligibility when live rug gating is disabled"
            );
            assert!(
                snapshot.score >= config.min_score,
                "the clustered wallet should still clear the score floor under the current live policy"
            );
            assert!(
                snapshot.rug_ratio.abs() < 1e-9,
                "with thin-market thresholds disabled at 0/0, the clustered wallet should look fully healthy to the current live policy despite sharing the exact clustered token path"
            );
            let (_, acc) = entries
                .iter()
                .find(|(entry_wallet_id, _)| entry_wallet_id == wallet_id)
                .expect("clustered wallet accumulator should exist");
            assert!(
                acc.positions.values().all(|lots| lots.is_empty()),
                "the repro must specifically cover the live-observed class where the wallets already fully drained all tracked lots before publication"
            );
        }

        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let publication_outcome = discovery.persist_publication_state(
            &store,
            DiscoveryRuntimeMode::Healthy,
            true,
            metrics_window_start,
            Some(&desired),
            "raw_window_persisted_stream",
            "clustered_thin_market_live_policy_publish",
            now,
        )?;
        assert!(
            publication_outcome.published_universe_persisted,
            "current live policy should currently materialize the clustered cohort into exact publication truth once it becomes the desired set"
        );
        let publication_state = store
            .discovery_publication_state_read_only()?
            .expect("publication state should exist");
        assert_eq!(
            publication_state.published_wallet_ids.unwrap_or_default(),
            clustered_wallet_ids,
            "published wallet ids must exactly mirror the clustered cohort under the current live policy"
        );

        Ok(())
    }

    #[test]
    fn live_like_clustered_wallets_require_both_rug_policy_surfaces_to_be_excluded_stage1() {
        let now = DateTime::parse_from_rfc3339("2026-04-06T10:26:22Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let live_config = live_shadow_blocker_discovery_config_for_tests();
        let default_config = DiscoveryConfig::default();
        let restored_config = live_restored_rug_policy_discovery_config_for_tests();
        let (swaps, clustered_wallet_ids, _) = clustered_thin_market_fixture_swaps(now, false);

        let (_, live_desired, _, _) =
            desired_wallets_from_clustered_thin_market_fixture(live_config.clone(), &swaps, now);
        assert_eq!(live_desired, clustered_wallet_ids);

        let mut rug_only_config = live_config.clone();
        rug_only_config.max_rug_ratio = default_config.max_rug_ratio;
        let (rug_only_snapshots, rug_only_desired, _, _) =
            desired_wallets_from_clustered_thin_market_fixture(rug_only_config, &swaps, now);
        assert_eq!(
            rug_only_desired, clustered_wallet_ids,
            "lowering only max_rug_ratio cannot help while thin-market thresholds stay disabled at 0/0"
        );
        assert!(
            rug_only_snapshots
                .iter()
                .filter(|snapshot| snapshot.wallet_id.starts_with("wallet_clustered_"))
                .all(|snapshot| snapshot.rug_ratio.abs() < 1e-9),
            "with thin-market thresholds still disabled, the clustered cohort should no longer look rugged at all"
        );

        let mut thin_market_only_config = live_config.clone();
        thin_market_only_config.thin_market_min_volume_sol =
            default_config.thin_market_min_volume_sol;
        thin_market_only_config.thin_market_min_unique_traders =
            default_config.thin_market_min_unique_traders;
        let (thin_market_only_snapshots, thin_market_only_desired, _, _) =
            desired_wallets_from_clustered_thin_market_fixture(
                thin_market_only_config,
                &swaps,
                now,
            );
        assert_eq!(
            thin_market_only_desired, clustered_wallet_ids,
            "restoring only thin-market thresholds still cannot exclude the cohort while live policy keeps rug gating bypassed at max_rug_ratio=1.0"
        );
        assert!(
            thin_market_only_snapshots
                .iter()
                .filter(|snapshot| snapshot.wallet_id.starts_with("wallet_clustered_"))
                .all(|snapshot| (snapshot.rug_ratio - 1.0).abs() < 1e-9),
            "with thin-market thresholds restored, the clustered cohort should immediately look fully rugged"
        );

        let (_, restored_desired, _, _) =
            desired_wallets_from_clustered_thin_market_fixture(restored_config, &swaps, now);
        assert!(
            restored_desired.is_empty(),
            "only the combined restore of the existing rug gate and thin-market thresholds should exclude the clustered cohort"
        );
    }

    #[test]
    fn live_like_restored_rug_policy_excludes_clustered_wallets_while_preserving_independent_wallets_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("restored-rug-policy-preserves-independent-wallets.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-06T10:26:22Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let live_config = live_shadow_blocker_discovery_config_for_tests();
        let restored_config = live_restored_rug_policy_discovery_config_for_tests();
        let (swaps, clustered_wallet_ids, independent_wallet_ids) =
            clustered_thin_market_fixture_swaps(now, true);
        assert_eq!(clustered_wallet_ids.len(), 9);
        assert_eq!(independent_wallet_ids.len(), 3);

        let (_, live_desired, _, _) =
            desired_wallets_from_clustered_thin_market_fixture(live_config.clone(), &swaps, now);
        assert_eq!(
            live_desired.len(),
            clustered_wallet_ids.len() + independent_wallet_ids.len(),
            "with live rug gating disabled, both the clustered cohort and the independent profitable wallets should publish together"
        );
        for wallet_id in &clustered_wallet_ids {
            assert!(
                live_desired.contains(wallet_id),
                "current live policy should still publish the clustered wallet {wallet_id}"
            );
        }
        for wallet_id in &independent_wallet_ids {
            assert!(
                live_desired.contains(wallet_id),
                "current live policy should also keep the independent profitable wallet {wallet_id}"
            );
        }

        let (_, restored_desired, _, _) = desired_wallets_from_clustered_thin_market_fixture(
            restored_config.clone(),
            &swaps,
            now,
        );
        assert_eq!(
            restored_desired, independent_wallet_ids,
            "restoring the existing rug/thin-market policy should exclude only the clustered thin-market cohort while preserving the independent profitable wallets"
        );

        let discovery = DiscoveryService::new(restored_config.clone(), permissive_shadow_quality());
        let metrics_window_start = metrics_window_start_for_test(&restored_config, now);
        let publication_outcome = discovery.persist_publication_state(
            &store,
            DiscoveryRuntimeMode::Healthy,
            true,
            metrics_window_start,
            Some(&restored_desired),
            "raw_window_persisted_stream",
            "clustered_thin_market_policy_restore_publish",
            now,
        )?;
        assert!(
            publication_outcome.published_universe_persisted,
            "once the clustered cohort is excluded for an exact thin-market ruggedness reason, the remaining independent universe should still materialize as exact publication truth"
        );
        let publication_state = store
            .discovery_publication_state_read_only()?
            .expect("publication state should exist");
        assert_eq!(
            publication_state.published_wallet_ids.unwrap_or_default(),
            independent_wallet_ids,
            "publication truth should persist only the surviving independent wallets after the policy restore"
        );

        Ok(())
    }

    #[test]
    fn live_like_restored_rug_policy_removes_only_three_of_nine_drained_clustered_wallets_stage1() {
        let now = DateTime::parse_from_rfc3339("2026-04-06T16:55:36Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = live_restored_rug_policy_discovery_config_for_tests();
        let (swaps, removed_wallet_ids, surviving_wallet_ids, independent_wallet_ids) =
            clustered_partial_survival_fixture_swaps(now, false);
        assert!(
            independent_wallet_ids.is_empty(),
            "the exact partial-survival repro should isolate only the clustered drained cohort"
        );

        let removed_token = "TokenClusteredThinEdge111111111111111111111";
        let surviving_token = "TokenClusteredThickEdge11111111111111111111";
        let (snapshots, desired, entries, token_sol_history) =
            desired_wallets_from_clustered_thin_market_fixture(config.clone(), &swaps, now);
        assert_eq!(
            desired, surviving_wallet_ids,
            "restored rug/thin-market policy should remove only the thin three while the thicker six still survive on historical score"
        );
        assert_eq!(removed_wallet_ids.len(), 3);
        assert_eq!(surviving_wallet_ids.len(), 6);

        let removed_first_buy_ts = swaps
            .iter()
            .find(|swap| {
                swap.wallet == removed_wallet_ids[0]
                    && swap.token_out == removed_token
                    && swap.token_in == SOL_MINT
            })
            .expect("removed-cluster buy should exist")
            .ts_utc;
        let surviving_first_buy_ts = swaps
            .iter()
            .find(|swap| {
                swap.wallet == surviving_wallet_ids[0]
                    && swap.token_out == surviving_token
                    && swap.token_in == SOL_MINT
            })
            .expect("surviving-cluster buy should exist")
            .ts_utc;
        let lookahead = Duration::seconds(config.rug_lookahead_seconds as i64);
        let (removed_volume, removed_unique_traders) = sol_leg_window_volume_and_unique_traders(
            &token_sol_history,
            removed_token,
            removed_first_buy_ts,
            removed_first_buy_ts + lookahead,
        );
        let (surviving_volume, surviving_unique_traders) = sol_leg_window_volume_and_unique_traders(
            &token_sol_history,
            surviving_token,
            surviving_first_buy_ts,
            surviving_first_buy_ts + lookahead,
        );
        assert!(
            removed_volume + 1e-12 < config.thin_market_min_volume_sol
                || removed_unique_traders < config.thin_market_min_unique_traders as usize,
            "the removed thin subgroup must fail the restored thin-market floor in the exact replay window"
        );
        assert!(
            surviving_volume + 1e-12 >= config.thin_market_min_volume_sol,
            "the surviving subgroup must clear the restored thin-market volume floor"
        );
        assert!(
            surviving_unique_traders >= config.thin_market_min_unique_traders as usize,
            "the surviving subgroup must clear the restored unique-trader floor"
        );

        for wallet_id in &removed_wallet_ids {
            let snapshot = snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("removed wallet snapshot should exist");
            assert!(
                snapshot.rug_ratio > config.max_rug_ratio,
                "the removed subgroup must be excluded by the restored rug/thin-market gate itself"
            );
            assert!(
                !snapshot.eligible,
                "the removed subgroup must already be ineligible before ranking"
            );
            let (_, acc) = entries
                .iter()
                .find(|(entry_wallet_id, _)| entry_wallet_id == wallet_id)
                .expect("removed wallet accumulator should exist");
            assert!(
                !acc.has_open_positions(),
                "the removed subgroup should share the same fully drained state as the surviving junk cohort"
            );
        }

        for wallet_id in &surviving_wallet_ids {
            let snapshot = snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("surviving wallet snapshot should exist");
            assert!(
                snapshot.rug_ratio <= config.max_rug_ratio,
                "the surviving subgroup should currently clear the restored rug gate"
            );
            assert!(
                snapshot.eligible && snapshot.score >= config.min_score,
                "the surviving subgroup should still publish today because current selection is historical-score driven"
            );
            let (_, acc) = entries
                .iter()
                .find(|(entry_wallet_id, _)| entry_wallet_id == wallet_id)
                .expect("surviving wallet accumulator should exist");
            assert!(
                !acc.has_open_positions(),
                "the surviving subgroup must still be fully drained, which is the exact stale-actionable-state gap this batch is addressing"
            );
        }
    }
