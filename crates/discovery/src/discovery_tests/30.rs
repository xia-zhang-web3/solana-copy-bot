    #[test]
    fn run_cycle_runtime_window_complete_resumed_replay_wallet_stats_uses_deeper_priority_contract_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-run-cycle-deep-replay-priority-contract.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-02T14:41:24Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 10_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let noise_wallets = 50_000usize;
        let (window_start, metrics_window_start) =
            seed_stage1_live_like_wallet_stats_backlog_fixture(
                &runtime_store,
                &config,
                now,
                noise_wallets,
                "stage1-run-cycle-deep-replay",
            )?;

        let mut replay_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        replay_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        replay_state.horizon_end = now;
        replay_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        replay_state.payload.unique_buy_mints =
            runtime_store.load_observed_buy_mints_in_window(window_start, now)?;
        replay_state.payload.token_quality_progress.next_mint_index =
            replay_state.payload.unique_buy_mints.len();

        let partial = discovery.advance_persisted_stream_replay_wallet_stats(
            &runtime_store,
            &mut replay_state,
            config.max_fetch_swaps_per_cycle,
            8,
            Instant::now() + StdDuration::from_secs(30),
        )?;
        replay_state.payload.replay_wallet_stats_rows_processed = replay_state
            .payload
            .replay_wallet_stats_rows_processed
            .saturating_add(partial.replay_wallet_stats_rows_processed);
        replay_state.payload.replay_wallet_stats_pages_processed = replay_state
            .payload
            .replay_wallet_stats_pages_processed
            .saturating_add(partial.replay_wallet_stats_pages_processed);
        replay_state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .merge(partial.replay_wallet_stats_day_count_source_progress);
        runtime_store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&replay_state, now)?,
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

        let summary = discovery.run_cycle(&runtime_store, now)?;
        if runtime_store
            .load_discovery_persisted_rebuild_state()?
            .is_none()
        {
            assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
            let publication_state = runtime_store
                .discovery_publication_state()?
                .expect("publication state should exist after publish");
            assert!(
                publication_state
                    .published_wallet_ids
                    .as_ref()
                    .is_some_and(|wallets| !wallets.is_empty()),
                "if the deeper recovery contract completes the whole rebuild in one cycle, it must still publish a real non-empty wallet universe"
            );
            return Ok(());
        }

        let rebuild = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert_eq!(rebuild.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert!(
            rebuild.payload.replay_wallet_stats_complete,
            "the live run_cycle priority path should move the persisted checkpoint beyond wallet_stats instead of leaving it on the same incomplete subphase"
        );
        assert_eq!(rebuild.payload.replay_wallet_stats_wallet_cursor, None);
        Ok(())
    }

    #[test]
    fn run_cycle_runtime_window_complete_live_like_collect_buy_mints_uses_priority_recovery_contract_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let baseline_db_path = temp
            .path()
            .join("stage1-runtime-priority-collect-buy-mints-baseline.db");
        let runtime_db_path = temp
            .path()
            .join("stage1-runtime-priority-collect-buy-mints-runtime.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut baseline_store = SqliteStore::open(Path::new(&baseline_db_path))?;
        baseline_store.run_migrations(&migration_dir)?;
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-02T13:23:26Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

        let baseline_window_start =
            seed_stage1_collect_buy_mints_noise_fixture(&baseline_store, &config, now, 6_000, 0)?;
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let PersistedStreamRebuildAdvanceOutcome::InProgress { telemetry } = discovery
            .advance_persisted_stream_rebuild(
                &baseline_store,
                baseline_window_start,
                metrics_window_start,
                now,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                StdDuration::from_millis(config.fetch_time_budget_ms),
            )?
        else {
            panic!("baseline live-like collect_buy_mints rebuild should stay partial under the ordinary 15s contract");
        };
        assert_eq!(
            telemetry.phase,
            DiscoveryPersistedRebuildPhase::CollectBuyMints
        );
        assert!(
            telemetry.prepass_rows_processed
                <= COLLECT_BUY_MINTS_FRESH_SCAN_BATCH_CAP * config.max_fetch_pages_per_cycle,
            "baseline live contract should still stop near the old bounded-page fresh-scan ceiling before token quality starts"
        );
        assert!(
            telemetry.quality_next_mint_index <= telemetry.unique_buy_mints,
            "baseline direct rebuild may warm token-quality over the exact discovered mint prefix, but it must still remain in collect_buy_mints until the grouped source exhausts"
        );
        assert_eq!(telemetry.wallets_buffered, 0);

        let runtime_window_start =
            seed_stage1_collect_buy_mints_noise_fixture(&runtime_store, &config, now, 6_000, 0)?;
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

        let summary = discovery.run_cycle(&runtime_store, now)?;
        if runtime_store
            .load_discovery_persisted_rebuild_state()?
            .is_none()
        {
            assert_eq!(summary.scoring_source, "raw_window_persisted_stream");
            assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
            return Ok(());
        }

        let rebuild = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert_eq!(rebuild.window_start, runtime_window_start);
        assert_eq!(rebuild.metrics_window_start, metrics_window_start);
        assert_ne!(
            rebuild.phase,
            DiscoveryPersistedRebuildPhase::CollectBuyMints,
            "run_cycle should reuse the widened fail-closed publication recovery contract instead of leaving the live-like rebuild stuck in fresh_scan after the old bounded-page prepass"
        );
        assert!(
            rebuild.prepass_rows_processed >= 6_000,
            "priority runtime recovery should drain the large live-like fresh-scan backlog before yielding back to the scheduler"
        );
        assert!(
            rebuild.payload.collect_buy_mints_cursor_token.is_none(),
            "once the widened runtime recovery contract drains fresh_scan, the persisted mint cursor should clear instead of preserving another partial collect_buy_mints prefix"
        );
        assert!(
            rebuild.payload.token_quality_progress.next_mint_index > 0
                || rebuild.payload.replay_wallet_stats_rows_processed > 0
                || rebuild.replay_rows_processed > 0,
            "after the widened runtime recovery contract drains fresh_scan, later token-quality or replay progress must already be visible"
        );
        Ok(())
    }

    #[test]
    fn repair_runtime_window_complete_live_like_replay_wallet_stats_catch_up_restores_publication_truth(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-publication-repair-live-like-replay.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-01T18:20:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let noise_wallets = 60_000usize;
        let (window_start, metrics_window_start) =
            seed_stage1_live_like_wallet_stats_backlog_fixture(
                &runtime_store,
                &config,
                now,
                noise_wallets,
                "stage1-live-like-repair-delegated",
            )?;
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_incomplete_no_recent_published_universe".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start - Duration::hours(2)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        let mut replay_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        replay_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        replay_state.horizon_end = now;
        replay_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        replay_state.payload.unique_buy_mints =
            runtime_store.load_observed_buy_mints_in_window(window_start, now)?;
        replay_state.payload.token_quality_progress.next_mint_index =
            replay_state.payload.unique_buy_mints.len();

        let partial = discovery.advance_persisted_stream_replay_wallet_stats(
            &runtime_store,
            &mut replay_state,
            config.max_fetch_swaps_per_cycle,
            8,
            Instant::now() + StdDuration::from_secs(30),
        )?;
        replay_state.payload.replay_wallet_stats_rows_processed = replay_state
            .payload
            .replay_wallet_stats_rows_processed
            .saturating_add(partial.replay_wallet_stats_rows_processed);
        replay_state.payload.replay_wallet_stats_pages_processed = replay_state
            .payload
            .replay_wallet_stats_pages_processed
            .saturating_add(partial.replay_wallet_stats_pages_processed);
        replay_state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .merge(partial.replay_wallet_stats_day_count_source_progress);
        runtime_store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&replay_state, now)?,
        )?;

        let repair = discovery
            .repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed(
                &runtime_store,
                None,
                now,
                64,
                Instant::now() + StdDuration::from_secs(60),
            )?;
        assert_eq!(
            repair.state,
            "deferred_runtime_window_truth_refresh_to_run_cycle"
        );
        assert!(!repair.publication_truth_refresh_attempted);
        assert!(repair.publication_truth_refresh_delegated_to_runtime_cycle);
        assert_eq!(repair.publication_truth_refresh_phase, Some("replay"));
        assert_eq!(
            repair.publication_truth_refresh_collect_buy_mints_phase_page_limit,
            Some(480),
            "repair telemetry should expose the same deep-replay collect_buy_mints ceiling that run_cycle will actually use, not the stale 60s value"
        );
        assert_eq!(
            repair.publication_truth_refresh_replay_wallet_stats_phase_page_limit,
            Some(276),
            "repair telemetry should expose the same deep-replay wallet-stats ceiling that the owning run_cycle will actually use"
        );
        assert_eq!(
            repair.publication_truth_refresh_effective_time_budget_ms,
            Some(180_000)
        );
        assert_eq!(
            repair.publication_truth_refresh_priority_recovery_contract_reason,
            Some("deep_replay_wallet_stats_incomplete")
        );
        assert_eq!(
            repair.publication_truth_refresh_replay_subphase,
            Some("wallet_stats"),
            "before run_cycle resumes the checkpoint, repair telemetry should report the exact buffered replay subphase that still blocks publication"
        );
        assert_eq!(
            repair.publication_truth_refresh_publishable_checkpoint_blocker,
            Some("replay_wallet_stats_incomplete")
        );
        assert!(
            !repair.publication_truth_refresh_replay_wallet_stats_complete,
            "repair telemetry should no longer pretend the pre-run_cycle helper already drained wallet_stats"
        );
        assert!(
            repair
                .publication_truth_refresh_replay_wallet_stats_wallet_cursor
                .is_some(),
            "the delegated repair summary should preserve the exact replay cursor that run_cycle will resume instead of clearing it prematurely"
        );
        assert!(
            repair.publication_truth_refresh_wallets_buffered > 0,
            "repair telemetry should still expose that the live replay checkpoint already buffered wallets even before publication truth is refreshed"
        );
        let rebuild_after_repair = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert_eq!(
            rebuild_after_repair
                .payload
                .replay_wallet_stats_rows_processed,
            replay_state.payload.replay_wallet_stats_rows_processed,
            "delegated repair must not burn another deep replay pass before run_cycle starts"
        );
        assert_eq!(
            rebuild_after_repair.payload.replay_wallet_stats_wallet_cursor,
            replay_state.payload.replay_wallet_stats_wallet_cursor,
            "delegated repair must leave the persisted replay cursor untouched for the owning run_cycle"
        );

        let mut published_summary = None;
        for cycle_offset_minutes in 0..=6 {
            let cycle_now = now + Duration::minutes(cycle_offset_minutes);
            let summary = discovery.run_cycle(&runtime_store, cycle_now)?;
            if summary.published {
                published_summary = Some((cycle_now, summary));
                break;
            }
        }
        let (published_at, summary) = published_summary.expect(
            "after the widened live-like wallet-stats catch-up phase, the next bounded discovery cycles should reach a publishable universe instead of staying fail-closed in replay",
        );
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        let publication_state = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist after refreshed cycle");
        assert_eq!(
            publication_state.runtime_mode,
            DiscoveryRuntimeMode::Healthy
        );
        assert_eq!(publication_state.last_published_at, Some(published_at));
        assert_eq!(
            publication_state.last_published_window_start,
            Some(metrics_window_start)
        );
        assert!(
            publication_state
                .published_wallet_ids
                .as_ref()
                .is_some_and(|wallets| !wallets.is_empty()),
            "the real rebuild-to-publish path must repopulate the exact published wallet universe instead of leaving runtime export stuck on empty published_wallet_ids"
        );
        Ok(())
    }

    #[test]
    fn run_cycle_partial_replay_wallet_stats_fail_closes_with_zero_wallets_seen_while_rows_are_present_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-run-cycle-partial-replay-activity-backfill.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-08T16:44:37Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let noise_wallets = 60_000usize;
        let (window_start, metrics_window_start) =
            seed_stage1_live_like_wallet_stats_backlog_fixture(
                &runtime_store,
                &config,
                now,
                noise_wallets,
                "stage1-post-recovery-fail-closed",
            )?;

        let mut replay_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        replay_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        replay_state.horizon_end = now;
        replay_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        replay_state.payload.unique_buy_mints =
            runtime_store.load_observed_buy_mints_in_window(window_start, now)?;
        replay_state.payload.token_quality_progress.next_mint_index =
            replay_state.payload.unique_buy_mints.len();

        let partial = discovery.advance_persisted_stream_replay_wallet_stats(
            &runtime_store,
            &mut replay_state,
            config.max_fetch_swaps_per_cycle,
            8,
            Instant::now() + StdDuration::from_secs(30),
        )?;
        replay_state.payload.replay_wallet_stats_rows_processed = replay_state
            .payload
            .replay_wallet_stats_rows_processed
            .saturating_add(partial.replay_wallet_stats_rows_processed);
        replay_state.payload.replay_wallet_stats_pages_processed = replay_state
            .payload
            .replay_wallet_stats_pages_processed
            .saturating_add(partial.replay_wallet_stats_pages_processed);
        replay_state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .merge(partial.replay_wallet_stats_day_count_source_progress);

        runtime_store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&replay_state, now)?,
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

        let summary = discovery.run_cycle(&runtime_store, now)?;
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(
            summary.scoring_source,
            "raw_window_incomplete_no_recent_published_universe"
        );
        assert_eq!(summary.wallets_seen, 0);
        assert_eq!(summary.eligible_wallets, 0);
        assert_eq!(summary.metrics_written, 0);
        assert!(summary.persisted_stream_catch_up_requested);

        let rebuild = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert_eq!(rebuild.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert!(
            rebuild.payload.replay_wallet_stats_rows_processed
                > replay_state.payload.replay_wallet_stats_rows_processed,
            "the reduced live-like replay checkpoint must record additional wallet-stats progress even while the fail-closed summary reports wallets_seen=0"
        );
        assert!(
            rebuild.payload.replay_wallet_stats_pages_processed
                > replay_state.payload.replay_wallet_stats_pages_processed,
            "the replay checkpoint must advance pages so the zero-summary class cannot be misread as a no-work/no-row path"
        );
        assert!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(&rebuild)
                .starts_with("replay_"),
            "the exact fail-closed class should still be blocked inside replay after the bounded cycle yields"
        );
        Ok(())
    }
