#[test]
    fn persisted_stream_rebuild_repairs_collect_buy_mints_quality_prefix_before_resume_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-collect-buy-mints-quality-prefix-repair.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-02T13:23:26Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (window_start, ordered_mints) =
            seed_stage1_collect_buy_mints_legacy_migration_fixture(&store, &config, now)?;
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let mut state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        state.payload.collect_buy_mints_cursor_token = Some(ordered_mints[1].clone());
        state.payload.unique_buy_mints = vec![
            ordered_mints[2].clone(),
            ordered_mints[0].clone(),
            ordered_mints[1].clone(),
        ];
        state.payload.buy_mint_counts = BTreeMap::from([
            (ordered_mints[2].clone(), 1),
            (ordered_mints[0].clone(), 1),
            (ordered_mints[1].clone(), 1),
        ]);
        state.payload.token_quality_progress.next_mint_index = 2;
        state.payload.token_quality_progress.rpc_attempted = 3;
        state.payload.token_quality_progress.rpc_spent_ms = 900;
        state.payload.token_quality_cache.insert(
            ordered_mints[0].clone(),
            quality_cache::TokenQualityResolution::Missing,
        );
        state.payload.token_quality_cache.insert(
            ordered_mints[1].clone(),
            quality_cache::TokenQualityResolution::Fresh(
                copybot_core_types::TokenQualityCacheRow {
                    mint: ordered_mints[1].clone(),
                    holders: Some(42),
                    liquidity_sol: Some(3.0),
                    token_age_seconds: Some(3_600),
                    fetched_at: now,
                },
            ),
        );
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, now)?,
        )?;

        let (repaired, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            window_start,
            metrics_window_start,
            now + Duration::minutes(1),
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(
            repaired.payload.unique_buy_mints,
            vec![ordered_mints[0].clone(), ordered_mints[1].clone()],
            "resume repair must trim collect_buy_mints onto the exact stored cursor prefix before reusing warmed quality progress"
        );
        assert_eq!(
            repaired.payload.token_quality_progress.next_mint_index,
            0,
            "resume repair must not treat a leading Missing/Deferred/Stale entry as reusable cached truth just because the mint key exists in payload.token_quality_cache"
        );
        assert_eq!(repaired.payload.token_quality_cache.len(), 1);
        assert!(repaired
            .payload
            .token_quality_cache
            .contains_key(&ordered_mints[1]));
        assert_eq!(repaired.payload.token_quality_progress.rpc_attempted, 0);
        assert_eq!(repaired.payload.token_quality_progress.rpc_spent_ms, 0);
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_optimized_reduces_heavy_rows_on_large_noise_fixture_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-replay-optimized-noise-throughput.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-18T13:54:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (window_start, metrics_window_start, total_rows, sol_leg_rows) =
            seed_stage1_replay_noise_fixture(&store, &config, now, 24, 720)?;
        assert!(sol_leg_rows < total_rows);
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let mut legacy_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        legacy_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        legacy_state.horizon_end = now;
        legacy_state.payload.unique_buy_mints =
            store.load_observed_buy_mints_in_window(window_start, now)?;
        legacy_state.payload.replay_mode = ReplayMode::LegacyCompleteReplay;

        let mut optimized_state = legacy_state.clone();
        optimized_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        optimized_state.payload.replay_wallet_stats_complete = false;

        let legacy_advance = discovery.advance_persisted_stream_replay_legacy(
            &store,
            &mut legacy_state,
            total_rows.saturating_add(16),
            total_rows.saturating_add(16),
            Instant::now() + StdDuration::from_secs(5),
        )?;
        let optimized_advance = discovery.advance_persisted_stream_replay_optimized(
            &store,
            &mut optimized_state,
            total_rows.saturating_add(16),
            total_rows.saturating_add(16),
            Instant::now() + StdDuration::from_secs(5),
        )?;

        assert!(legacy_advance.source_exhausted);
        assert!(optimized_advance.source_exhausted);
        assert_eq!(legacy_advance.rows_processed, total_rows);
        assert_eq!(
            optimized_advance.replay_wallet_stats_rows_processed,
            total_rows
        );
        assert_eq!(optimized_advance.rows_processed, sol_leg_rows);
        assert!(
            optimized_advance.rows_processed < legacy_advance.rows_processed,
            "optimized replay should only apply later-phase streaming state to SOL-leg swaps after exact wallet stats were buffered separately"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_legacy_checkpoint_rewinds_to_optimized_mode_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-replay-legacy-upgrade-repair.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-18T13:54:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (window_start, metrics_window_start, _, _) =
            seed_stage1_replay_noise_fixture(&store, &config, now, 6, 120)?;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

        let mut legacy_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        legacy_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        legacy_state.horizon_end = now;
        legacy_state.prepass_rows_processed = 6;
        legacy_state.prepass_pages_processed = 2;
        legacy_state.payload.unique_buy_mints =
            store.load_observed_buy_mints_in_window(window_start, now)?;
        legacy_state.payload.token_quality_cache.insert(
            legacy_state.payload.unique_buy_mints[0].clone(),
            quality_cache::TokenQualityResolution::Deferred,
        );
        legacy_state.payload.replay_mode = ReplayMode::LegacyCompleteReplay;
        legacy_state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: window_start + Duration::minutes(10),
            slot: 42,
            signature: "legacy-replay-cursor".to_string(),
        });
        legacy_state.replay_rows_processed = 111;
        legacy_state.replay_pages_processed = 7;
        legacy_state.payload.by_wallet.insert(
            "wallet_partial".to_string(),
            WalletAccumulator {
                trades: 11,
                ..WalletAccumulator::default()
            },
        );
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
        assert_eq!(repaired.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert_eq!(
            repaired.payload.replay_mode,
            ReplayMode::WalletStatsThenSolLeg
        );
        assert!(!repaired.payload.replay_wallet_stats_complete);
        assert_eq!(repaired.replay_rows_processed, 0);
        assert_eq!(repaired.replay_pages_processed, 0);
        assert_eq!(repaired.phase_cursor, None);
        assert!(repaired.payload.by_wallet.is_empty());
        assert_eq!(
            repaired.prepass_rows_processed,
            legacy_state.prepass_rows_processed
        );
        assert!(
            repaired
                .payload
                .token_quality_cache
                .contains_key(&legacy_state.payload.unique_buy_mints[0]),
            "rewinding legacy replay progress must preserve the resolved token-quality cache"
        );
        Ok(())
    }
