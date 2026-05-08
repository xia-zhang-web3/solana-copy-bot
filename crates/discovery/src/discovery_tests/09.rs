    #[test]
    fn persisted_stream_rebuild_resumes_after_restart_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-bounded-persisted-stream-restart.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;

        let discovery_first = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let _ = discovery_first.run_cycle(&store, now)?;
        let first_progress = store
            .load_discovery_persisted_rebuild_state()?
            .expect("first cycle should persist rebuild progress");

        let discovery_after_restart = DiscoveryService::new(config, permissive_shadow_quality());
        let second_summary =
            discovery_after_restart.run_cycle(&store, now + Duration::minutes(1))?;
        assert_eq!(
            second_summary.runtime_mode,
            DiscoveryRuntimeMode::FailClosed
        );
        let second_progress = store
            .load_discovery_persisted_rebuild_state()?
            .expect("restart cycle should restore and advance rebuild progress");

        assert_eq!(second_progress.window_start, first_progress.window_start);
        assert_eq!(second_progress.horizon_end, first_progress.horizon_end);
        assert!(
            second_progress.prepass_rows_processed > first_progress.prepass_rows_processed,
            "restart must continue from the persisted checkpoint instead of replaying from zero"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_collect_buy_mints_migrates_legacy_raw_cursor_to_safe_prefix_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-collect-buy-mints-legacy-cursor-migration.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (window_start, ordered_mints) =
            seed_stage1_collect_buy_mints_legacy_migration_fixture(&store, &config, now)?;
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let mut legacy_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        legacy_state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        legacy_state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: window_start + Duration::minutes(1),
            slot: 3,
            signature: "legacy-raw-swap-cursor".to_string(),
        });
        legacy_state.prepass_rows_processed = 1_700_000;
        legacy_state.prepass_pages_processed = 17;
        legacy_state.chunks_completed = 17;
        legacy_state.payload.unique_buy_mints = vec![
            ordered_mints[3].clone(),
            ordered_mints[0].clone(),
            ordered_mints[1].clone(),
        ];
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&legacy_state, now)?,
        )?;

        let outcome = discovery.advance_persisted_stream_rebuild(
            &store,
            window_start,
            metrics_window_start,
            now + Duration::minutes(1),
            1,
            1,
            StdDuration::from_secs(1),
        )?;
        assert!(matches!(
            outcome,
            PersistedStreamRebuildAdvanceOutcome::InProgress { .. }
        ));
        let migrated = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            migrated.phase,
            DiscoveryPersistedRebuildPhase::CollectBuyMints
        );
        assert_eq!(migrated.chunks_completed, 18);
        assert!(
            migrated.prepass_rows_processed > legacy_state.prepass_rows_processed,
            "legacy collect_buy_mints checkpoints must keep advancing instead of restarting from zero"
        );
        assert!(
            migrated.phase_cursor.is_none(),
            "legacy raw-swap cursor should be replaced by the direct distinct-mint cursor"
        );
        assert_eq!(
            migrated.payload.collect_buy_mints_cursor_token.as_deref(),
            Some(ordered_mints[2].as_str()),
            "legacy migration must recover the safe sorted prefix and persist the next direct distinct-mint cursor"
        );
        assert_eq!(
            migrated.payload.unique_buy_mints,
            ordered_mints[..3].to_vec(),
            "legacy migration must normalize to the canonical sorted prefix and drop unsafe tail mints until they are rediscovered in order"
        );

        let next = discovery.advance_persisted_stream_rebuild(
            &store,
            window_start,
            metrics_window_start,
            now + Duration::minutes(2),
            1,
            1,
            StdDuration::from_secs(1),
        )?;
        assert!(matches!(
            next,
            PersistedStreamRebuildAdvanceOutcome::InProgress { .. }
        ));
        let resumed = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            resumed.payload.unique_buy_mints,
            ordered_mints[..4].to_vec(),
            "the next cycle must continue from the persisted direct distinct cursor instead of starting the sorted pagination from zero again"
        );
        assert_eq!(
            resumed.payload.collect_buy_mints_cursor_token.as_deref(),
            Some(ordered_mints[3].as_str()),
            "the resumed collect_buy_mints cursor must stay on the rediscovered sorted tail instead of rewinding to the start"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_collect_buy_mints_legacy_migration_preserves_canonical_order_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-collect-buy-mints-legacy-order-parity.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (window_start, ordered_mints) =
            seed_stage1_collect_buy_mints_legacy_migration_fixture(&store, &config, now)?;
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let mut legacy_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        legacy_state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        legacy_state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: window_start + Duration::minutes(1),
            slot: 3,
            signature: "legacy-raw-swap-cursor".to_string(),
        });
        legacy_state.payload.unique_buy_mints = vec![
            ordered_mints[3].clone(),
            ordered_mints[0].clone(),
            ordered_mints[1].clone(),
        ];
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&legacy_state, now)?,
        )?;

        let rebuild_time_budget = StdDuration::from_secs(1);
        for idx in 0..4 {
            let cycle_now = now + Duration::minutes(idx as i64 + 1);
            let _ = discovery.advance_persisted_stream_rebuild(
                &store,
                window_start,
                metrics_window_start,
                cycle_now,
                1,
                1,
                rebuild_time_budget,
            )?;
        }

        let repaired = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            repaired.phase,
            DiscoveryPersistedRebuildPhase::ResolveTokenQuality
        );
        assert_eq!(
            repaired.payload.unique_buy_mints,
            store.load_observed_buy_mints_in_window(window_start, now)?,
            "legacy collect_buy_mints migration must feed token-quality resolution with the exact canonical one-shot mint order"
        );
        assert_eq!(repaired.payload.unique_buy_mints, ordered_mints);
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_repairs_noncanonical_quality_checkpoint_before_resume_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-noncanonical-quality-checkpoint-repair.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (window_start, ordered_mints) =
            seed_stage1_collect_buy_mints_legacy_migration_fixture(&store, &config, now)?;
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let mut state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        state.phase = DiscoveryPersistedRebuildPhase::ResolveTokenQuality;
        state.payload.unique_buy_mints = vec![
            ordered_mints[3].clone(),
            ordered_mints[0].clone(),
            ordered_mints[1].clone(),
        ];
        state.payload.token_quality_progress.next_mint_index = 2;
        state.payload.token_quality_cache.insert(
            ordered_mints[3].clone(),
            quality_cache::TokenQualityResolution::Missing,
        );
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, now)?,
        )?;

        let outcome = discovery.advance_persisted_stream_rebuild(
            &store,
            window_start,
            metrics_window_start,
            now + Duration::minutes(1),
            1,
            1,
            StdDuration::from_secs(1),
        )?;
        assert!(matches!(
            outcome,
            PersistedStreamRebuildAdvanceOutcome::InProgress { .. }
        ));
        let repaired = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            repaired.phase,
            DiscoveryPersistedRebuildPhase::ResolveTokenQuality
        );
        assert_eq!(
            repaired.payload.unique_buy_mints,
            vec![
                ordered_mints[0].clone(),
                ordered_mints[1].clone(),
                ordered_mints[3].clone(),
            ],
            "resume repair must canonicalize non-collect checkpoints onto sorted buy-mint order"
        );
        assert_eq!(
            repaired.payload.token_quality_progress.next_mint_index,
            1,
            "resume repair must rewind token-quality positional progress before consuming the canonical order"
        );
        assert_eq!(repaired.payload.token_quality_cache.len(), 1);
        assert!(repaired
            .payload
            .token_quality_cache
            .contains_key(&ordered_mints[0]));
        Ok(())
    }
