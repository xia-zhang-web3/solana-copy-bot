    #[test]
    fn persisted_stream_replay_zero_progress_legacy_checkpoint_upgrades_to_optimized_mode_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-replay-zero-progress-legacy-upgrade.db");
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
        legacy_state.payload.replay_mode = ReplayMode::LegacyFullWindow;
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
        assert_eq!(repaired.payload.replay_wallet_stats_rows_processed, 0);
        assert_eq!(repaired.payload.replay_wallet_stats_pages_processed, 0);
        assert_eq!(repaired.replay_rows_processed, 0);
        assert_eq!(repaired.replay_pages_processed, 0);
        assert_eq!(repaired.phase_cursor, None);
        assert!(repaired.payload.by_wallet.is_empty());
        assert!(repaired.payload.token_states.is_empty());
        assert!(repaired.payload.token_recent_sol_trades.is_empty());
        assert!(repaired.payload.pending_rug_checks.is_empty());
        assert!(repaired.payload.token_pending_buy_starts.is_empty());
        assert_eq!(
            repaired.payload.unique_buy_mints,
            legacy_state.payload.unique_buy_mints
        );
        assert_eq!(
            repaired.prepass_rows_processed,
            legacy_state.prepass_rows_processed
        );
        assert!(
            repaired
                .payload
                .token_quality_cache
                .contains_key(&legacy_state.payload.unique_buy_mints[0]),
            "upgrading a zero-progress legacy replay checkpoint must preserve the resolved token-quality cache"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_optimized_resumes_after_restart_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-replay-optimized-restart.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-18T13:54:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (window_start, metrics_window_start, total_rows, _) =
            seed_stage1_replay_noise_fixture(&store, &config, now, 6, 120)?;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

        let mut state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.horizon_end = now;
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.unique_buy_mints =
            store.load_observed_buy_mints_in_window(window_start, now)?;

        let stats_advance = discovery.advance_persisted_stream_replay_optimized(
            &store,
            &mut state,
            total_rows.saturating_add(16),
            1,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        state.payload.replay_wallet_stats_rows_processed = state
            .payload
            .replay_wallet_stats_rows_processed
            .saturating_add(stats_advance.replay_wallet_stats_rows_processed);
        state.payload.replay_wallet_stats_pages_processed = state
            .payload
            .replay_wallet_stats_pages_processed
            .saturating_add(stats_advance.replay_wallet_stats_pages_processed);
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .merge(stats_advance.replay_wallet_stats_day_count_source_progress);
        state.replay_rows_processed = state
            .replay_rows_processed
            .saturating_add(stats_advance.rows_processed);
        state.replay_pages_processed = state
            .replay_pages_processed
            .saturating_add(stats_advance.pages_processed);
        state.phase_cursor = stats_advance.phase_cursor;
        assert!(state.payload.replay_wallet_stats_complete);

        let replay_advance = discovery.advance_persisted_stream_replay_optimized(
            &store,
            &mut state,
            1,
            1,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        state.payload.replay_wallet_stats_rows_processed = state
            .payload
            .replay_wallet_stats_rows_processed
            .saturating_add(replay_advance.replay_wallet_stats_rows_processed);
        state.payload.replay_wallet_stats_pages_processed = state
            .payload
            .replay_wallet_stats_pages_processed
            .saturating_add(replay_advance.replay_wallet_stats_pages_processed);
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .merge(replay_advance.replay_wallet_stats_day_count_source_progress);
        state.replay_rows_processed = state
            .replay_rows_processed
            .saturating_add(replay_advance.rows_processed);
        state.replay_pages_processed = state
            .replay_pages_processed
            .saturating_add(replay_advance.pages_processed);
        state.phase_cursor = replay_advance.phase_cursor.clone();
        assert!(state.replay_rows_processed > 0);
        assert!(state.phase_cursor.is_some());

        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, now)?,
        )?;

        let discovery_after_restart =
            DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (resumed, restore_outcome) = discovery_after_restart
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                window_start,
                metrics_window_start,
                now,
            )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(resumed.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert_eq!(
            resumed.payload.replay_mode,
            ReplayMode::WalletStatsThenSolLeg
        );
        assert!(resumed.payload.replay_wallet_stats_complete);
        assert_eq!(
            resumed.payload.replay_wallet_stats_rows_processed,
            state.payload.replay_wallet_stats_rows_processed
        );
        assert_eq!(
            resumed
                .payload
                .replay_wallet_stats_day_count_source_progress,
            state.payload.replay_wallet_stats_day_count_source_progress
        );
        assert_eq!(resumed.replay_rows_processed, state.replay_rows_processed);
        assert_eq!(resumed.phase_cursor, state.phase_cursor);
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_wallet_stats_sql_summary_matches_activity_scan_stage1() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-replay-wallet-stats-sql-summary.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-20T06:45:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = bounded_stage1_runtime_config();
        config.max_tx_per_minute = 3;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);

        store.insert_observed_swap(&swap(
            "wallet-replay-a",
            "wallet-replay-a-1",
            window_start + Duration::hours(1),
            SOL_MINT,
            "TokenReplayA1111111111111111111111111111111",
            1.0,
            10.0,
        ))?;
        store.insert_observed_swap(&swap(
            "wallet-replay-a",
            "wallet-replay-a-2",
            window_start + Duration::days(1) + Duration::hours(2),
            "TokenReplayA1111111111111111111111111111111",
            SOL_MINT,
            10.0,
            1.2,
        ))?;
        store.insert_observed_swap(&swap(
            "wallet-replay-b",
            "wallet-replay-b-1",
            window_start + Duration::hours(3),
            SOL_MINT,
            "TokenReplayB1111111111111111111111111111111",
            1.0,
            10.0,
        ))?;
        for idx in 0..4 {
            store.insert_observed_swap(&swap(
                "wallet-replay-b",
                &format!("wallet-replay-b-burst-{idx}"),
                window_start + Duration::hours(4),
                if idx % 2 == 0 {
                    SOL_MINT
                } else {
                    "TokenReplayB1111111111111111111111111111111"
                },
                if idx % 2 == 0 {
                    "TokenReplayBurst1111111111111111111111111"
                } else {
                    SOL_MINT
                },
                1.0,
                1.0,
            ))?;
        }
        store.insert_observed_swap(&swap(
            "wallet-replay-c",
            "wallet-replay-c-1",
            window_start + Duration::days(2) + Duration::hours(1),
            "TokenReplayCIn11111111111111111111111111111",
            "TokenReplayCOut111111111111111111111111111",
            2.0,
            3.0,
        ))?;
        store.upsert_wallet_activity_days(&[
            WalletActivityDayRow {
                wallet_id: "wallet-replay-a".to_string(),
                activity_day: (window_start + Duration::hours(1)).date_naive(),
                last_seen: window_start + Duration::hours(1),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-replay-a".to_string(),
                activity_day: (window_start + Duration::days(1) + Duration::hours(2)).date_naive(),
                last_seen: window_start + Duration::days(1) + Duration::hours(2),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-replay-b".to_string(),
                activity_day: (window_start + Duration::hours(4)).date_naive(),
                last_seen: window_start + Duration::hours(4),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-replay-c".to_string(),
                activity_day: (window_start + Duration::days(2) + Duration::hours(1)).date_naive(),
                last_seen: window_start + Duration::days(2) + Duration::hours(1),
            },
        ])?;

        let mut replay_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        replay_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        replay_state.horizon_end = now;
        replay_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        replay_state.payload.unique_buy_mints =
            store.load_observed_buy_mints_in_window(window_start, now)?;

        loop {
            let advance = discovery.advance_persisted_stream_replay_wallet_stats(
                &store,
                &mut replay_state,
                2,
                2,
                Instant::now() + StdDuration::from_secs(5),
            )?;
            replay_state.payload.replay_wallet_stats_rows_processed = replay_state
                .payload
                .replay_wallet_stats_rows_processed
                .saturating_add(advance.replay_wallet_stats_rows_processed);
            replay_state.payload.replay_wallet_stats_pages_processed = replay_state
                .payload
                .replay_wallet_stats_pages_processed
                .saturating_add(advance.replay_wallet_stats_pages_processed);
            replay_state
                .payload
                .replay_wallet_stats_day_count_source_progress
                .merge(advance.replay_wallet_stats_day_count_source_progress);
            if advance.source_exhausted {
                break;
            }
            assert!(
                advance.budget_exhausted_reason.is_some(),
                "paged wallet-stats replay should remain bounded until the final summary page"
            );
        }

        let mut reference_by_wallet: HashMap<String, WalletAccumulator> = HashMap::new();
        let reference_page = store.for_each_observed_swap_in_window_after_cursor_with_budget(
            window_start,
            now,
            None,
            10_000,
            Instant::now() + StdDuration::from_secs(5),
            |swap| {
                reference_by_wallet
                    .entry(swap.wallet.clone())
                    .or_default()
                    .observe_activity_only(&swap, config.max_tx_per_minute);
                Ok(())
            },
        )?;
        assert!(!reference_page.time_budget_exhausted);

        assert_eq!(
            replay_state.payload.replay_wallet_stats_wallet_cursor, None,
            "wallet cursor must clear after exact wallet-stats summary completion"
        );
        assert_eq!(
            replay_state.payload.by_wallet.len(),
            reference_by_wallet.len()
        );
        assert!(
            replay_state
                .payload
                .replay_wallet_stats_day_count_source_progress
                .fast_path_pages_processed
                > 0,
            "wallet-stats replay should expose wallet_activity_days fast-path usage on the nominal path"
        );
        assert_eq!(
            replay_state
                .payload
                .replay_wallet_stats_day_count_source_progress
                .fallback_pages_processed,
            0,
            "wallet-stats replay should not report raw fallback usage when auxiliary day counts are complete"
        );
        for (wallet_id, reference) in reference_by_wallet {
            let actual = replay_state
                .payload
                .by_wallet
                .get(&wallet_id)
                .with_context(|| {
                    format!("missing replay wallet activity summary for {wallet_id}")
                })?;
            assert_eq!(actual.first_seen, reference.first_seen);
            assert_eq!(actual.last_seen, reference.last_seen);
            assert_eq!(actual.trades, reference.trades);
            assert_eq!(actual.suspicious, reference.suspicious);
            assert_eq!(
                actual
                    .exact_active_day_count
                    .unwrap_or(actual.active_days.len() as u32),
                reference.active_days.len() as u32
            );
        }
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_wallet_stats_width_is_wider_than_sol_leg_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-replay-wallet-stats-width.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-20T06:45:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = bounded_stage1_runtime_config();
        config.max_fetch_swaps_per_cycle = 1;
        config.max_fetch_pages_per_cycle = 1;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (window_start, metrics_window_start, _, _) =
            seed_stage1_replay_noise_fixture(&store, &config, now, 3, 6)?;
        let unique_buy_mints = store.load_observed_buy_mints_in_window(window_start, now)?;

        let mut wallet_stats_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        wallet_stats_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        wallet_stats_state.horizon_end = now;
        wallet_stats_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        wallet_stats_state.payload.unique_buy_mints = unique_buy_mints.clone();

        let wallet_stats_advance = discovery.advance_persisted_stream_replay_optimized(
            &store,
            &mut wallet_stats_state,
            1,
            1,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert_eq!(
            wallet_stats_advance.replay_wallet_stats_pages_processed, 2,
            "wallet-stats prepass should get a replay-only widened page budget"
        );
        assert!(wallet_stats_advance.replay_wallet_stats_rows_processed > 0);
        assert!(
            wallet_stats_advance
                .replay_wallet_stats_day_count_source_progress
                .fast_path_pages_processed
                + wallet_stats_advance
                    .replay_wallet_stats_day_count_source_progress
                    .fallback_pages_processed
                > 0
        );
        assert_eq!(wallet_stats_advance.rows_processed, 0);
        assert_eq!(
            wallet_stats_advance.budget_exhausted_reason,
            Some(PersistedStreamBudgetExhaustedReason::PageBudget)
        );

        let mut sol_leg_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        sol_leg_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        sol_leg_state.horizon_end = now;
        sol_leg_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        sol_leg_state.payload.replay_wallet_stats_complete = true;
        sol_leg_state.payload.unique_buy_mints = unique_buy_mints;

        let sol_leg_advance = discovery.advance_persisted_stream_replay_optimized(
            &store,
            &mut sol_leg_state,
            1,
            1,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert_eq!(
            sol_leg_advance.pages_processed, 1,
            "SOL-leg replay should still respect the base page budget"
        );
        assert_eq!(sol_leg_advance.replay_wallet_stats_pages_processed, 0);
        assert_eq!(
            sol_leg_advance.budget_exhausted_reason,
            Some(PersistedStreamBudgetExhaustedReason::PageBudget)
        );
        Ok(())
    }
