    #[test]
    fn policy_tightening_invalidates_recent_exact_publication_truth_before_degraded_reuse_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-policy-tightening-invalidates-recent-publication-truth.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-06T14:49:22Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let old_publish_at = now - Duration::minutes(9);
        let mut old_config = live_restored_rug_policy_discovery_config_for_tests();
        old_config.max_window_swaps_in_memory = 8;
        let mut tightened_config = old_config.clone();
        tightened_config.require_open_positions_for_publication = true;

        let (swaps, _removed_wallet_ids, surviving_wallet_ids, _independent_wallet_ids) =
            clustered_partial_survival_fixture_swaps(now, false);
        let (_, old_desired, _, _) =
            desired_wallets_from_clustered_thin_market_fixture(old_config.clone(), &swaps, now);
        assert_eq!(
            old_desired, surviving_wallet_ids,
            "the old exact publication truth for this repro must be the same six drained cluster survivors that remained after the rug/thin-market restore"
        );

        store.persist_discovery_cycle(
            &[],
            &[],
            &old_desired,
            false,
            false,
            old_publish_at,
            "seed_old_clustered_publication_truth",
        )?;
        let metrics_window_start = metrics_window_start_for_test(&old_config, old_publish_at);
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "seed_old_clustered_publication_truth".to_string(),
            last_published_at: Some(old_publish_at),
            last_published_window_start: Some(metrics_window_start),
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: Some(old_desired.clone()),
        })?;

        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..9 {
            let ts = now - Duration::minutes(9) + Duration::minutes(idx as i64);
            let swap = swap(
                "wallet_noise",
                &format!("stage1-policy-tightening-noise-{idx}"),
                ts,
                SOL_MINT,
                "TokenStage1PolicyTighteningNoise11111111111",
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

        let publication_state_before = store
            .discovery_publication_state_read_only()?
            .expect("old publication state should exist");
        let tightened_discovery =
            DiscoveryService::new(tightened_config.clone(), permissive_shadow_quality());
        let tightened_gate = tightened_discovery.publication_freshness_gate();
        assert!(publication_state_before.has_complete_publication_truth());
        assert!(
            publication_state_before.is_fresh_under_gate(&tightened_gate, now),
            "under the old pre-fix contract, the old six-wallet exact publish would still have been considered recent enough to reuse after the policy change"
        );
        assert!(
            !publication_state_before.is_fresh_under_gate(
                &tightened_gate,
                old_publish_at + tightened_discovery.runtime_published_universe_max_age()
                    + Duration::seconds(1),
            ),
            "the old truth would only have stopped being reusable once freshness expired, which is the exact live failure class this batch is addressing"
        );
        assert!(
            publication_state_before
                .publication_policy_fingerprint
                .is_none(),
            "the exact live failure class is a legacy exact publish that predates the new policy-fingerprint contract entirely"
        );

        assert!(
            tightened_discovery
                .runtime_publication_truth_resolution(&store, now)?
                .is_none(),
            "tightened selection policy must invalidate the old six-wallet exact truth before degraded runtime reuse"
        );
        let invalidated_publication_state = store
            .discovery_publication_state_read_only()?
            .expect("publication state should still exist after invalidation");
        assert_eq!(
            invalidated_publication_state.runtime_mode,
            DiscoveryRuntimeMode::FailClosed
        );
        assert_eq!(
            invalidated_publication_state.reason,
            "publication_truth_invalidated_selection_policy_mismatch"
        );
        assert!(
            !invalidated_publication_state.has_complete_publication_truth(),
            "policy mismatch invalidation must clear the stale exact publish instead of leaving the old truth complete until expiry"
        );
        assert!(
            store.list_active_follow_wallets()?.is_empty(),
            "policy mismatch invalidation must also clear the old active followlist surface before degraded reuse can leak it back into runtime"
        );

        let summary = tightened_discovery.run_cycle(&store, now)?;
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert!(summary.trusted_selection_fail_closed);
        assert_eq!(
            summary.scoring_source,
            "raw_window_incomplete_no_recent_published_universe"
        );
        assert_eq!(summary.eligible_wallets, 0);
        assert_eq!(summary.active_follow_wallets, 0);
        assert!(
            !store
                .discovery_publication_state_read_only()?
                .expect("publication state should remain readable after fail-closed cycle")
                .has_complete_publication_truth(),
            "with no recent exact truth left after policy mismatch invalidation, the tightened runtime must not end the cycle with a reusable published universe"
        );
        Ok(())
    }

    #[test]
    fn legacy_policy_mismatch_does_not_invalidate_v2_publication_truth() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-v2-publication-truth-owned-by-v2.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-06T14:49:22Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = live_restored_rug_policy_discovery_config_for_tests();
        let published_at = now - Duration::minutes(1);
        let metrics_window_start = metrics_window_start_for_test(&config, published_at);
        store.set_discovery_publication_state_with_options(
            &DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "discovery_v2_ready".to_string(),
                last_published_at: Some(published_at),
                last_published_window_start: Some(metrics_window_start),
                published_scoring_source: Some("discovery_v2_operational_window".to_string()),
                published_wallet_ids: Some(vec!["wallet-v2".to_string()]),
            },
            false,
            Some("v2-policy-fingerprint"),
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let truth = discovery
            .recent_runtime_publication_truth(&store, now)?
            .expect("V2 publication truth must stay readable");
        let state = store
            .discovery_publication_state_read_only()?
            .expect("publication state");

        assert_eq!(truth.published_wallet_ids, vec!["wallet-v2".to_string()]);
        assert_eq!(state.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(state.reason, "discovery_v2_ready");
        assert_eq!(
            state.publication_policy_fingerprint.as_deref(),
            Some("v2-policy-fingerprint")
        );
        Ok(())
    }

    #[test]
    fn cold_start_stale_persisted_history_without_recent_published_universe_fail_closes_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-cold-start-stale-persisted-fail-close.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:12:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let stale_ts = window_start - Duration::minutes(5);
        let stale_swap = swap(
            "wallet_stale_history",
            "stage1-stale-persisted-old-swap-fail-close",
            stale_ts,
            SOL_MINT,
            "TokenStage1StalePersistedFailClose11111",
            0.5,
            50.0,
        );
        store.insert_observed_swap(&stale_swap)?;
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: stale_swap.ts_utc,
            slot: stale_swap.slot,
            signature: stale_swap.signature.clone(),
        })?;
        store.activate_follow_wallet("wallet_stale", now - Duration::minutes(1), "seed-follow")?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert!(summary.trusted_selection_fail_closed);
        assert_eq!(
            summary.scoring_source,
            "raw_window_unusable_no_recent_published_universe"
        );
        assert!(!summary.raw_window_cap_truncated);
        assert_eq!(summary.eligible_wallets, 0);
        assert_eq!(summary.follow_demoted, 1);
        assert!(store.list_active_follow_wallets()?.is_empty());
        Ok(())
    }

    #[test]
    fn cold_start_unusable_raw_window_without_recent_published_universe_fail_closes_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-cold-start-fail-close.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:10:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        store.activate_follow_wallet("wallet_stale", now - Duration::minutes(5), "seed-follow")?;

        let mut config = stage1_runtime_config();
        config.max_window_swaps_in_memory = 128;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert!(summary.trusted_selection_fail_closed);
        assert_eq!(
            summary.scoring_source,
            "raw_window_unusable_no_recent_published_universe"
        );
        assert_eq!(summary.follow_demoted, 1);
        assert!(store.list_active_follow_wallets()?.is_empty());
        Ok(())
    }
