#[test]
    fn persisted_stream_collect_buy_mints_reconcile_legacy_raw_cursor_repairs_to_grouped_delta_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-collect-buy-mints-reconcile-upgrade-repair.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let now = DateTime::parse_from_rfc3339("2026-03-18T18:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);

        let mut legacy_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        legacy_state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        legacy_state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileNewTail;
        legacy_state.payload.collect_buy_mints_prepass_complete = false;
        legacy_state.payload.collect_buy_mints_cursor_token =
            Some("TokenStage1FreshResumeCursor111111111111".to_string());
        legacy_state
            .payload
            .collect_buy_mints_reconcile_source_window_start =
            Some(window_start - Duration::seconds(20));
        legacy_state
            .payload
            .collect_buy_mints_reconcile_source_horizon_end = Some(now - Duration::seconds(20));
        legacy_state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: now - Duration::seconds(10),
            slot: 42,
            signature: "legacy-reconcile-raw-cursor".to_string(),
        });
        legacy_state.payload.unique_buy_mints =
            vec!["TokenStage1CarryResumeMint1111111111111".to_string()];
        legacy_state.payload.buy_mint_counts =
            BTreeMap::from([("TokenStage1CarryResumeMint1111111111111".to_string(), 1)]);
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&legacy_state, now)?,
        )?;

        let (repaired, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            window_start,
            metrics_window_start,
            now,
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(
            repaired.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileNewTail
        );
        assert_eq!(
            repaired.payload.collect_buy_mints_cursor_token.as_deref(),
            legacy_state.payload.collect_buy_mints_cursor_token.as_deref(),
            "repair must preserve the fresh-scan resume cursor instead of resetting the whole collect_buy_mints attempt"
        );
        assert!(
            repaired
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor
                .is_none(),
            "legacy raw-swap reconcile cursor must be cleared before grouped delta pagination resumes"
        );
        assert!(
            repaired
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .is_none(),
            "grouped delta reconciliation should restart only the current delta subphase from token cursor None"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_eventually_completes_healthy_after_metrics_bucket_roll_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-carry-forward-eventual-healthy.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:59Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.max_fetch_swaps_per_cycle = 1;
        config.max_fetch_pages_per_cycle = 1;
        config.fetch_time_budget_ms = 1_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (window_start, _) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 2, 3)?;
        let _ = seed_fresh_token_quality_cache_rows_for_window_for_test(&store, window_start, now)?;

        let first_summary = discovery.run_cycle(&store, now)?;
        assert_eq!(first_summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        let first_progress = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert!(
            matches!(
                first_progress.phase,
                DiscoveryPersistedRebuildPhase::CollectBuyMints
                    | DiscoveryPersistedRebuildPhase::ResolveTokenQuality
            ),
            "the first bounded cycle should establish an upstream carried-rebuild lineage, but newer exact token-quality reuse is allowed to push that lineage as far as ResolveTokenQuality before the next cycle"
        );

        let rollover_now = now + Duration::seconds(2);
        let second_summary = discovery.run_cycle(&store, rollover_now)?;
        assert_eq!(
            second_summary.runtime_mode,
            DiscoveryRuntimeMode::FailClosed
        );
        let second_progress = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert!(
            second_progress.prepass_rows_processed >= first_progress.prepass_rows_processed,
            "carry-forward after bucket rollover must preserve bounded collect_buy_mints progress instead of restarting from zero"
        );
        assert!(
            matches!(
                second_progress.phase,
                DiscoveryPersistedRebuildPhase::CollectBuyMints
                    | DiscoveryPersistedRebuildPhase::ResolveTokenQuality
                    | DiscoveryPersistedRebuildPhase::Replay
            ),
            "carry-forward after bucket rollover may now reuse exact token-quality cache and advance farther downstream, but it must stay on the persisted rebuild path rather than resetting away from it"
        );

        for step in 1..=75 {
            let cycle_now = rollover_now + Duration::seconds(step);
            let summary = discovery.run_cycle(&store, cycle_now)?;
            if summary.runtime_mode == DiscoveryRuntimeMode::Healthy {
                assert_eq!(summary.scoring_source, "raw_window_persisted_stream");
                assert!(
                    store.load_discovery_persisted_rebuild_state()?.is_none(),
                    "healthy completion after carried-forward rebuild must clear the persisted rebuild checkpoint"
                );
                return Ok(());
            }
        }

        anyhow::bail!(
            "carried-forward persisted rebuild did not reach healthy completion before the next test cutoff"
        );
    }

    #[test]
    fn persisted_stream_rebuild_carried_forward_collect_buy_mints_resumes_after_restart_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-carry-forward-restart.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:59Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.max_fetch_swaps_per_cycle = 1;
        config.max_fetch_pages_per_cycle = 1;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (window_start, _) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 2, 3)?;
        let _ = seed_fresh_token_quality_cache_rows_for_window_for_test(&store, window_start, now)?;

        let _ = discovery.run_cycle(&store, now)?;
        let rollover_now = now + Duration::seconds(2);
        let _ = discovery.run_cycle(&store, rollover_now)?;
        let carried_before_restart = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert!(
            carried_before_restart.metrics_window_start
                == metrics_window_start_for_test(&config, rollover_now)
                || carried_before_restart.metrics_window_start
                    == metrics_window_start_for_test(&config, now),
            "carry-forward before restart may now either sit on the current bucket or keep advancing a still-publishable frozen bucket, but it must stay on one of those persisted rebuild targets instead of resetting away from them"
        );

        let discovery_after_restart =
            DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let _ = discovery_after_restart.run_cycle(&store, rollover_now + Duration::seconds(1))?;
        let carried_after_restart = load_persisted_stream_rebuild_state_for_test(&store)?;

        assert!(
            carried_after_restart.prepass_rows_processed
                > carried_before_restart.prepass_rows_processed
                || carried_after_restart.prepass_pages_processed
                    > carried_before_restart.prepass_pages_processed
                || carried_after_restart.phase != carried_before_restart.phase
                || carried_after_restart.payload.collect_buy_mints_mode
                    != carried_before_restart.payload.collect_buy_mints_mode
                || carried_after_restart.metrics_window_start
                    != carried_before_restart.metrics_window_start,
            "restart must continue the carried-forward rebuild lineage instead of resetting it; newer exact token-quality reuse is allowed to resume a different still-valid frozen/current target as long as progress is not discarded"
        );
        Ok(())
    }
