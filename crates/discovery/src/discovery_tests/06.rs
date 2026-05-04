    #[test]
    fn live_like_open_position_publication_gate_excludes_drained_cluster_survivors_while_preserving_independent_wallets_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("open-position-publication-gate-excludes-drained-cluster-survivors.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-06T16:55:36Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let base_config = live_restored_rug_policy_discovery_config_for_tests();
        let mut gated_config = base_config.clone();
        gated_config.require_open_positions_for_publication = true;
        let (swaps, removed_wallet_ids, surviving_wallet_ids, independent_wallet_ids) =
            clustered_partial_survival_fixture_swaps(now, true);
        assert_eq!(removed_wallet_ids.len(), 3);
        assert_eq!(surviving_wallet_ids.len(), 6);
        assert_eq!(independent_wallet_ids.len(), 3);

        let (_, base_desired, base_entries, _) =
            desired_wallets_from_clustered_thin_market_fixture(base_config.clone(), &swaps, now);
        assert_eq!(
            base_desired.len(),
            surviving_wallet_ids.len() + independent_wallet_ids.len(),
            "before the new publication gate, restored rug policy should still publish the six drained cluster survivors plus the valid independent wallets"
        );
        for wallet_id in &surviving_wallet_ids {
            assert!(
                base_desired.contains(wallet_id),
                "the exact live blocker requires the drained clustered survivor {wallet_id} to still pass current restored policy"
            );
            let (_, acc) = base_entries
                .iter()
                .find(|(entry_wallet_id, _)| entry_wallet_id == wallet_id)
                .expect("surviving wallet accumulator should exist");
            assert!(
                !acc.has_open_positions(),
                "the drained clustered survivor {wallet_id} must have no open tracked lots before the new gate"
            );
        }
        for wallet_id in &independent_wallet_ids {
            assert!(
                base_desired.contains(wallet_id),
                "valid independent wallet {wallet_id} should already be publishable before the new gate"
            );
            let (_, acc) = base_entries
                .iter()
                .find(|(entry_wallet_id, _)| entry_wallet_id == wallet_id)
                .expect("independent wallet accumulator should exist");
            assert!(
                acc.has_open_positions(),
                "the positive-control independent wallet {wallet_id} must keep an actionable open lot"
            );
        }

        let (gated_snapshots, gated_desired, _, _) =
            desired_wallets_from_clustered_thin_market_fixture(gated_config.clone(), &swaps, now);
        assert_eq!(
            gated_desired, independent_wallet_ids,
            "requiring an open tracked position for publication should exclude only the drained clustered survivors while preserving independent wallets with live actionable state"
        );
        for wallet_id in &surviving_wallet_ids {
            let snapshot = gated_snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("gated surviving wallet snapshot should exist");
            assert!(
                !snapshot.eligible && snapshot.score.abs() < 1e-9,
                "the new gate must demote the drained clustered survivor {wallet_id} before ranking rather than relying on top-N"
            );
        }
        for wallet_id in &independent_wallet_ids {
            let snapshot = gated_snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("gated independent wallet snapshot should exist");
            assert!(
                snapshot.eligible && snapshot.score >= gated_config.min_score,
                "the new gate must preserve the legitimate independent wallet {wallet_id}"
            );
        }

        let discovery = DiscoveryService::new(gated_config.clone(), permissive_shadow_quality());
        let metrics_window_start = metrics_window_start_for_test(&gated_config, now);
        let publication_outcome = discovery.persist_publication_state(
            &store,
            DiscoveryRuntimeMode::Healthy,
            true,
            metrics_window_start,
            Some(&gated_desired),
            "raw_window_persisted_stream",
            "open_position_publication_gate_cluster_filter",
            now,
        )?;
        assert!(
            publication_outcome.published_universe_persisted,
            "once the drained clustered survivors are excluded for a tracked actionable-state reason, the remaining independent universe must still materialize as exact publication truth"
        );
        let publication_state = store
            .discovery_publication_state_read_only()?
            .expect("publication state should exist");
        assert_eq!(
            publication_state.published_wallet_ids.unwrap_or_default(),
            independent_wallet_ids,
            "publication truth must persist only the surviving independent wallets after the new gate"
        );

        Ok(())
    }

    #[test]
    fn live_like_refill_drain_wallets_with_stale_phantom_open_lots_pass_old_open_position_gate_stage1(
    ) {
        let now = DateTime::parse_from_rfc3339("2026-04-06T17:59:22Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = live_restored_rug_policy_discovery_config_for_tests();
        config.require_open_positions_for_publication = true;
        let (entries, stale_cluster_wallet_ids, independent_wallet_ids) =
            refill_drain_open_lot_fixture_wallets(now, false);
        assert!(independent_wallet_ids.is_empty());

        let (old_snapshots, old_desired) =
            desired_wallets_from_refill_drain_fixture_with_open_position_semantics(
                config.clone(),
                &entries,
                now,
                false,
            );
        assert_eq!(
            old_desired, stale_cluster_wallet_ids,
            "under the old gate semantics, any unmatched buy lot keeps the refill/drain wallet publishable even when that lot already looks stale relative to the wallet's own realized hold behavior"
        );

        for wallet_id in &stale_cluster_wallet_ids {
            let (_, acc) = entries
                .iter()
                .find(|(entry_wallet_id, _)| entry_wallet_id == wallet_id)
                .expect("stale refill/drain wallet accumulator should exist");
            assert!(
                acc.has_open_positions(),
                "the exact failure class requires the refill/drain wallet {wallet_id} to still carry a phantom open lot in swap-derived state"
            );
            assert!(
                !acc.has_actionable_open_positions(now, config.metric_snapshot_interval_seconds),
                "the same phantom open lot for {wallet_id} should already be stale relative to the wallet's realized hold envelope and current metric bucket cadence"
            );
            let snapshot = old_snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("old-gate snapshot should exist");
            assert!(
                snapshot.eligible && snapshot.score >= config.min_score,
                "historical scoring should still keep the refill/drain wallet {wallet_id} publishable before the tighter actionable-open-position semantics run"
            );
        }
    }

    #[test]
    fn actionable_open_position_gate_preserves_legitimate_open_leader_without_realized_hold_history_stage1(
    ) {
        let now = DateTime::parse_from_rfc3339("2026-04-06T17:59:22Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = live_restored_rug_policy_discovery_config_for_tests();
        config.require_open_positions_for_publication = true;
        let wallet_id = "wallet_legit_open_no_history".to_string();
        let acc = leader_with_open_position_and_hold_history(now, Duration::hours(2), 0, &[]);

        assert!(
            !old_actionable_open_position_gate_passes(
                &acc,
                now,
                config.metric_snapshot_interval_seconds,
            ),
            "the old gate would wrongly kill a leader with a genuine long-lived open position but no realized sell history because it collapsed the allowed age to a single metric bucket"
        );
        assert!(
            acc.has_actionable_open_positions(now, config.metric_snapshot_interval_seconds),
            "without an established realized hold profile, discovery must treat the open lot as still actionable instead of inferring staleness from missing sell history"
        );

        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let snapshot = discovery.snapshot_from_accumulator(
            wallet_id.clone(),
            acc.clone(),
            now,
            &HashMap::new(),
        );
        assert!(
            snapshot.eligible && snapshot.score >= config.min_score,
            "the tightened gate must preserve legitimate open-position leaders when there is no realized hold history yet"
        );
        let snapshots = [snapshot];
        let ranked = rank_follow_candidates(&snapshots, config.min_score);
        let desired = desired_wallets(&ranked, config.follow_top_n);
        assert_eq!(desired, vec![wallet_id]);
    }

    #[test]
    fn actionable_open_position_gate_preserves_legitimate_open_leader_with_insufficient_hold_samples_stage1(
    ) {
        let now = DateTime::parse_from_rfc3339("2026-04-06T17:59:22Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = live_restored_rug_policy_discovery_config_for_tests();
        config.require_open_positions_for_publication = true;
        let wallet_id = "wallet_legit_open_low_history".to_string();
        let acc = leader_with_open_position_and_hold_history(
            now,
            Duration::hours(2),
            2,
            &[5 * 60, 7 * 60],
        );

        assert!(
            !old_actionable_open_position_gate_passes(
                &acc,
                now,
                config.metric_snapshot_interval_seconds,
            ),
            "the old gate would also wrongly kill a leader with only a couple of realized exits because two short samples do not establish a trustworthy hold envelope"
        );
        assert!(
            acc.has_actionable_open_positions(now, config.metric_snapshot_interval_seconds),
            "with fewer than the minimum hold samples, discovery must not infer that the remaining open lot is stale"
        );

        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let snapshot = discovery.snapshot_from_accumulator(
            wallet_id.clone(),
            acc.clone(),
            now,
            &HashMap::new(),
        );
        assert!(
            snapshot.eligible && snapshot.score >= config.min_score,
            "the actionable-open-position gate must preserve leaders with insufficient realized hold history until a real hold profile exists"
        );
        let snapshots = [snapshot];
        let ranked = rank_follow_candidates(&snapshots, config.min_score);
        let desired = desired_wallets(&ranked, config.follow_top_n);
        assert_eq!(desired, vec![wallet_id]);
    }

    #[test]
    fn live_like_actionable_open_position_gate_excludes_refill_drain_wallets_while_preserving_recent_independent_leaders_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("actionable-open-position-gate-excludes-refill-drain-wallets.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-06T17:59:22Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = live_restored_rug_policy_discovery_config_for_tests();
        config.require_open_positions_for_publication = true;
        let (entries, stale_cluster_wallet_ids, independent_wallet_ids) =
            refill_drain_open_lot_fixture_wallets(now, true);
        assert_eq!(stale_cluster_wallet_ids.len(), 7);
        assert_eq!(independent_wallet_ids.len(), 3);

        let (_, old_desired) =
            desired_wallets_from_refill_drain_fixture_with_open_position_semantics(
                config.clone(),
                &entries,
                now,
                false,
            );
        for wallet_id in &stale_cluster_wallet_ids {
            assert!(
                old_desired.contains(wallet_id),
                "old open-position semantics should still publish the refill/drain wallet {wallet_id}"
            );
        }
        for wallet_id in &independent_wallet_ids {
            assert!(
                old_desired.contains(wallet_id),
                "the positive-control independent leader {wallet_id} should also publish under the old semantics"
            );
        }

        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let new_snapshots = entries
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
        let new_ranked = rank_follow_candidates(&new_snapshots, config.min_score);
        let new_desired = desired_wallets(&new_ranked, config.follow_top_n);
        assert_eq!(
            new_desired, independent_wallet_ids,
            "the actionable-open-position gate should exclude only the refill/drain wallets whose phantom lots already outlived their realized hold envelope, while preserving recent independent leaders"
        );
        for wallet_id in &stale_cluster_wallet_ids {
            let snapshot = new_snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("new-gate stale wallet snapshot should exist");
            assert!(
                !snapshot.eligible && snapshot.score.abs() < 1e-9,
                "the stale refill/drain wallet {wallet_id} must be demoted before ranking once its only open lot is no longer actionable"
            );
        }
        for wallet_id in &independent_wallet_ids {
            let snapshot = new_snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("new-gate independent snapshot should exist");
            assert!(
                snapshot.eligible && snapshot.score >= config.min_score,
                "the actionable-open-position gate must preserve the recent independent leader {wallet_id}"
            );
        }

        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let publication_outcome = discovery.persist_publication_state(
            &store,
            DiscoveryRuntimeMode::Healthy,
            true,
            metrics_window_start,
            Some(&new_desired),
            "raw_window_persisted_stream",
            "actionable_open_position_gate_refill_drain_filter",
            now,
        )?;
        assert!(
            publication_outcome.published_universe_persisted,
            "once the refill/drain wallets are excluded for a stale-actionable-state reason, the remaining independent leaders must still materialize as exact publication truth"
        );
        let publication_state = store
            .discovery_publication_state_read_only()?
            .expect("publication state should exist");
        assert_eq!(
            publication_state.published_wallet_ids.unwrap_or_default(),
            independent_wallet_ids,
            "publication truth must persist only the recent independent leaders after the actionable-open-position gate"
        );
        Ok(())
    }

    #[test]
    fn live_like_scoring_window_only_actionable_gate_collapses_prewindow_carry_leaders_to_zero_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("actionable-open-position-gate-collapses-prewindow-carry-leaders.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-06T22:59:22Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = live_restored_rug_policy_discovery_config_for_tests();
        config.require_open_positions_for_publication = true;
        let legit_wallet_ids: Vec<String> = (0..3)
            .map(|idx| format!("wallet_legit_prewindow_carry_{idx:02}"))
            .collect();
        seed_prewindow_carry_leader_position_history(&store, &config, now, &legit_wallet_ids)?;
        let (junk_entries, stale_cluster_wallet_ids, independent_wallet_ids) =
            refill_drain_open_lot_fixture_wallets(now, false);
        assert!(independent_wallet_ids.is_empty());

        let mut entries = junk_entries;
        for wallet_id in &legit_wallet_ids {
            entries.push((
                wallet_id.clone(),
                prewindow_carry_leader_scoring_window_accumulator(now),
            ));
        }
        let by_wallet: HashMap<String, WalletAccumulator> = entries.iter().cloned().collect();

        let mut ungated_config = config.clone();
        ungated_config.require_open_positions_for_publication = false;
        let ungated_discovery =
            DiscoveryService::new(ungated_config.clone(), permissive_shadow_quality());
        let ungated_snapshots = entries
            .iter()
            .map(|(wallet_id, acc)| {
                ungated_discovery.snapshot_from_accumulator(
                    wallet_id.clone(),
                    acc.clone(),
                    now,
                    &HashMap::new(),
                )
            })
            .collect::<Vec<_>>();
        let ungated_ranked = rank_follow_candidates(&ungated_snapshots, ungated_config.min_score);
        let ungated_desired = desired_wallets(&ungated_ranked, ungated_config.follow_top_n);
        for wallet_id in &legit_wallet_ids {
            assert!(
                ungated_desired.contains(wallet_id),
                "before the actionable-open-position gate runs, the same pre-window carry leader {wallet_id} should still rank into the historical top set"
            );
        }
        for wallet_id in &stale_cluster_wallet_ids {
            assert!(
                ungated_desired.contains(wallet_id),
                "before the tighter gate, the same refill/drain wallet {wallet_id} should still ride historical score into the candidate set"
            );
        }

        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let current_snapshots = entries
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
        let current_ranked = rank_follow_candidates(&current_snapshots, config.min_score);
        let current_desired = desired_wallets(&current_ranked, config.follow_top_n);
        assert!(
            current_desired.is_empty(),
            "the current c284388 semantics should collapse this live-shaped mixed field to zero because legit carry leaders lose their only actionable state on the scoring-window-only accumulator path while the refill/drain cluster is demoted by stale-lot inference"
        );
        for wallet_id in &legit_wallet_ids {
            let snapshot = current_snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("current legit carry snapshot should exist");
            assert!(
                !snapshot.eligible && snapshot.score.abs() < 1e-9,
                "without retention-window position reconstruction, the legit pre-window carry leader {wallet_id} must be zeroed only because the scoring-window accumulator cannot represent its still-open carry lot"
            );
        }
        for wallet_id in &stale_cluster_wallet_ids {
            let snapshot = current_snapshots
                .iter()
                .find(|snapshot| snapshot.wallet_id == *wallet_id)
                .expect("current junk snapshot should exist");
            assert!(
                !snapshot.eligible && snapshot.score.abs() < 1e-9,
                "the same junk refill/drain wallet {wallet_id} must still be demoted under the current actionable-open-position rule"
            );
        }

        let reconstructed_snapshots = discovery.wallet_snapshots_from_accumulators(
            &store,
            by_wallet,
            now,
            &HashMap::new(),
        )?;
        let reconstructed_ranked =
            rank_follow_candidates(&reconstructed_snapshots, config.min_score);
        let reconstructed_desired = desired_wallets(&reconstructed_ranked, config.follow_top_n);
        assert_eq!(
            reconstructed_desired, legit_wallet_ids,
            "once the same store-backed raw path reconstructs positions across the retention-only prefix, only the legit pre-window carry leaders should remain publishable while the junk refill/drain cluster stays out"
        );
        Ok(())
    }
