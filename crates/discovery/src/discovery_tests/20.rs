    #[test]
    fn persisted_stream_replay_bucket_roll_carried_budget_floor_survives_target_window_replay_handoff_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-replay-aged-out-carry-floor-survives-replay-handoff.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-04-03T13:24:17Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let aged_out_now =
            source_now + Duration::seconds(config.metric_snapshot_interval_seconds as i64 + 61);
        let (window_start, metrics_window_start, _, _) =
            seed_stage1_replay_noise_fixture(&store, &config, source_now, 3, 12)?;
        let target_window_start =
            aged_out_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_metrics_window_start = metrics_window_start_for_test(&config, aged_out_now);
        let unique_buy_mints = store.load_observed_buy_mints_in_window(window_start, source_now)?;

        let mut state = discovery.start_persisted_stream_rebuild_state(
            window_start,
            metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.horizon_end = source_now;
        state.prepass_rows_processed = 51_667;
        state.prepass_pages_processed = 17;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        state.payload.unique_buy_mints = unique_buy_mints.clone();
        state.payload.buy_mint_counts = unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1u32))
            .collect();
        state.payload.token_quality_progress.next_mint_index = unique_buy_mints.len();
        state.payload.token_quality_cache =
            fresh_token_quality_cache_for_mints_for_test(&unique_buy_mints, source_now);
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.replay_wallet_stats_complete = false;
        state.payload.replay_wallet_stats_rows_processed = 12_419_939;
        state.payload.replay_wallet_stats_pages_processed = 733;
        state.payload.replay_wallet_stats_wallet_cursor =
            Some("rollover-carried-wallet-stats-cursor".to_string());
        state.payload.replay_wallet_stats_budget_floor_wallets = 682_639;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_pages_processed = 273;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_wallets_processed = 245_700;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_elapsed_ms = 492_750;
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .fast_path_wallets_processed = 656_100;
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, source_now)?,
        )?;

        let (mut carried, restore_outcome) = discovery
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                target_window_start,
                target_metrics_window_start,
                aged_out_now,
            )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow
        );
        assert_eq!(
            carried.payload.replay_wallet_stats_budget_floor_wallets,
            682_639
        );
        assert_eq!(
            carried
                .payload
                .replay_wallet_stats_last_partial_cycle_pages_processed,
            273
        );
        assert_eq!(
            carried
                .payload
                .replay_wallet_stats_last_partial_cycle_wallets_processed,
            245_700
        );
        assert_eq!(
            carried
                .payload
                .replay_wallet_stats_last_partial_cycle_elapsed_ms,
            492_750
        );

        carried.phase = DiscoveryPersistedRebuildPhase::ResolveTokenQuality;
        carried.payload.token_quality_cache = fresh_token_quality_cache_for_mints_for_test(
            &carried.payload.unique_buy_mints,
            aged_out_now,
        );
        carried.payload.token_quality_progress.next_mint_index =
            carried.payload.unique_buy_mints.len();
        carried.payload.by_wallet.clear();
        carried
            .payload
            .replay_wallet_stats_day_count_source_progress =
            ReplayWalletStatsDayCountSourceProgress::default();

        DiscoveryService::transition_persisted_stream_from_token_quality_to_replay(
            &mut carried,
            true,
        );

        assert_eq!(carried.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert_eq!(
            carried.payload.replay_wallet_stats_budget_floor_wallets,
            682_639,
            "carry-forwarded wallet-stats budgeting memory must survive the target-window token-quality -> replay handoff instead of silently restarting from the new observed frontier"
        );
        assert_eq!(
            carried
                .payload
                .replay_wallet_stats_last_partial_cycle_pages_processed,
            273
        );
        assert_eq!(
            carried
                .payload
                .replay_wallet_stats_last_partial_cycle_wallets_processed,
            245_700
        );
        assert_eq!(
            carried
                .payload
                .replay_wallet_stats_last_partial_cycle_elapsed_ms,
            492_750
        );
        assert_eq!(carried.payload.by_wallet.len(), 0);
        assert_eq!(
            DiscoveryService::replay_wallet_stats_current_observed_wallet_floor_wallets(&carried),
            0,
            "after the bucket-sensitive replay truth reset, the next target-window replay should widen from carried budgeting memory rather than from whatever current by_wallet frontier happened to be rebuilt first"
        );

        carried
            .payload
            .replay_wallet_stats_day_count_source_progress
            .fast_path_wallets_processed = 9_000;
        carried.payload.replay_wallet_stats_budget_floor_wallets = carried
            .payload
            .replay_wallet_stats_budget_floor_wallets
            .max(
                DiscoveryService::replay_wallet_stats_current_observed_wallet_floor_wallets(
                    &carried,
                ),
            );
        assert!(
            carried.payload.replay_wallet_stats_budget_floor_wallets > 9_000,
            "once the carried replay checkpoint reaches the new target-window replay handoff, the first partial replay update must not collapse the carried wallet frontier floor back down to the tiny newly observed frontier"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_discards_checkpoint_when_horizon_is_in_future_stage1() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-future-horizon.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let now = DateTime::parse_from_rfc3339("2026-03-17T12:20:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);

        let mut invalid_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        invalid_state.phase = DiscoveryPersistedRebuildPhase::PublishPending;
        invalid_state.horizon_end = now + Duration::seconds(1);
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(
                &invalid_state,
                invalid_state.updated_at,
            )?,
        )?;

        let (restarted, resumed_existing) = discovery
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                window_start,
                metrics_window_start,
                now,
            )?;
        assert_eq!(
            resumed_existing,
            PersistedStreamRebuildRestoreOutcome::StartedFresh
        );
        assert_eq!(
            restarted.phase,
            DiscoveryPersistedRebuildPhase::CollectBuyMints
        );
        assert_eq!(restarted.horizon_end, now);
        assert!(
            store.load_discovery_persisted_rebuild_state()?.is_none(),
            "future frozen horizon must be cleared before restart"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_partial_fail_closed_clears_active_followlist_stage1() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-fail-closed-clears-followlist.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let stale_metrics_window_start =
            metrics_window_start_for_test(&config, now) - Duration::hours(1);
        let stale_published_wallets =
            seed_published_wallet_metrics_snapshot(&store, stale_metrics_window_start, 1, 1)?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "test_stale_publication".to_string(),
            last_published_at: Some(
                now - discovery.published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(stale_metrics_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(stale_published_wallets.into_iter().collect()),
        })?;
        assert_eq!(store.list_active_follow_wallets()?.len(), 1);

        seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;
        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(summary.active_follow_wallets, 0);
        assert!(
            store.list_active_follow_wallets()?.is_empty(),
            "partial no-fallback fail-closed must clear stale active follow wallets immediately"
        );
        Ok(())
    }

    #[test]
    fn recent_runtime_publication_truth_rejects_stale_exact_published_universe() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("runtime-publication-truth-stale.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "test_stale_exact_published_universe".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(vec!["wallet_published_exact".to_string()]),
        })?;

        assert!(
            discovery
                .recent_runtime_publication_truth(&store, now)?
                .is_none(),
            "stale publication truth must be rejected even when an exact published wallet set is stored"
        );
        Ok(())
    }

    fn seed_stage1_recent_raw_journal_head_gap_repair_fixture() -> Result<(
        tempfile::TempDir,
        SqliteStore,
        SqliteStore,
        DiscoveryService,
        DateTime<Utc>,
    )> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp.path().join("stage1-publication-repair-runtime.db");
        let journal_db_path = temp.path().join("stage1-publication-repair-journal.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let mut journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;
        journal_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-01T14:52:24Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = discovery.metrics_window_start(now);

        let mut journal_swaps = Vec::new();
        for idx in 0..4 {
            let offset = Duration::minutes((idx * 20) as i64);
            journal_swaps.push(swap(
                "wallet_top",
                &format!("repair-top-buy-{idx}"),
                window_start + offset,
                SOL_MINT,
                "TokenStage1RepairTop1111111111111111111111",
                1.0,
                100.0,
            ));
            journal_swaps.push(swap(
                "wallet_top",
                &format!("repair-top-sell-{idx}"),
                window_start + offset + Duration::minutes(5),
                "TokenStage1RepairTop1111111111111111111111",
                SOL_MINT,
                100.0,
                1.3,
            ));
            journal_swaps.push(swap(
                "wallet_noise",
                &format!("repair-noise-buy-{idx}"),
                window_start + offset,
                SOL_MINT,
                "TokenStage1RepairNoise11111111111111111111",
                1.0,
                100.0,
            ));
            journal_swaps.push(swap(
                "wallet_noise",
                &format!("repair-noise-sell-{idx}"),
                window_start + offset + Duration::minutes(5),
                "TokenStage1RepairNoise11111111111111111111",
                SOL_MINT,
                100.0,
                0.7,
            ));
        }

        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..6 {
            let ts = now - Duration::minutes(6) + Duration::minutes(idx as i64);
            let tail_swap = swap(
                "wallet_tail_noise",
                &format!("repair-tail-noise-{idx}"),
                ts,
                SOL_MINT,
                "TokenStage1RepairTail111111111111111111111",
                0.2,
                20.0,
            );
            latest_cursor = Some(DiscoveryRuntimeCursor {
                ts_utc: tail_swap.ts_utc,
                slot: tail_swap.slot,
                signature: tail_swap.signature.clone(),
            });
            journal_swaps.push(tail_swap);
        }
        journal_store.insert_recent_raw_journal_batch(&journal_swaps, now)?;

        for swap in journal_swaps.iter().skip(4) {
            runtime_store.insert_observed_swap(swap)?;
        }
        runtime_store.upsert_discovery_runtime_cursor(
            &latest_cursor.clone().expect("latest cursor must exist"),
        )?;
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_incomplete_no_recent_published_universe".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        Ok((temp, runtime_store, journal_store, discovery, now))
    }

    #[test]
    fn repair_recent_raw_journal_head_gap_restores_fresh_complete_publication_truth() -> Result<()>
    {
        let (_temp, runtime_store, journal_store, discovery, now) =
            seed_stage1_recent_raw_journal_head_gap_repair_fixture()?;
        let metrics_window_start = discovery.metrics_window_start(now);

        let repair = discovery
            .repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed(
                &runtime_store,
                Some(&journal_store),
                now,
                64,
                Instant::now() + StdDuration::from_secs(1),
            )?;
        assert_eq!(repair.state, "replayed_recent_raw_journal_head_gap");
        assert!(repair.replay_rows_inserted > 0);
        assert!(repair.runtime_window_complete_after);

        let summary = discovery.run_cycle(&runtime_store, now)?;
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert!(
            summary.published,
            "repaired runtime window should allow the publish-due cycle to persist fresh publication truth"
        );
        let publication_state = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist after repaired cycle");
        assert_eq!(
            publication_state.runtime_mode,
            DiscoveryRuntimeMode::Healthy
        );
        assert_eq!(publication_state.last_published_at, Some(now));
        assert_eq!(
            publication_state.last_published_window_start,
            Some(metrics_window_start)
        );
        assert!(
            publication_state
                .published_wallet_ids
                .as_ref()
                .is_some_and(|wallets| !wallets.is_empty()),
            "repaired healthy publish must repopulate the exact published wallet ids"
        );
        Ok(())
    }

    #[test]
    fn repair_recent_raw_journal_head_gap_zero_work_budget_exhausted_returns_explicit_state_stage1(
    ) -> Result<()> {
        let (_temp, runtime_store, journal_store, discovery, now) =
            seed_stage1_recent_raw_journal_head_gap_repair_fixture()?;
        let required_window_start = now - Duration::days(discovery.runtime_scoring_window_days());

        assert!(
            !discovery.persisted_observed_swaps_cover_window(
                &runtime_store,
                required_window_start
            )?,
            "fixture must start from an incomplete runtime window so the head-gap repair branch is actually needed"
        );

        let repair = discovery
            .repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed(
                &runtime_store,
                Some(&journal_store),
                now,
                64,
                Instant::now(),
            )?;
        assert_eq!(
            repair.state,
            "skipped_recent_raw_journal_head_gap_zero_effective_replay_work"
        );
        assert_eq!(
            repair.reason.as_deref(),
            Some("recent_raw_journal_head_gap_replay_budget_exhausted_before_first_batch")
        );
        assert!(repair.journal_covers_runtime_cursor);
        assert!(!repair.runtime_window_complete_after);
        assert_eq!(repair.replay_batches_completed, 0);
        assert_eq!(repair.replay_rows_loaded, 0);
        assert_eq!(repair.replay_rows_inserted, 0);
        assert!(repair.replay_time_budget_exhausted);
        assert!(
            !discovery.persisted_observed_swaps_cover_window(
                &runtime_store,
                required_window_start
            )?,
            "the zero-work seam must remain fail-closed incomplete instead of pretending the runtime window repaired itself"
        );
        Ok(())
    }
