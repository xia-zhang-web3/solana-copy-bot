    #[test]
    fn repair_runtime_window_complete_deferred_branch_reports_helper_entry_and_write_attempt_diagnostics_stage1(
    ) -> Result<()> {
        let (_temp, runtime_store, discovery, now, _stale_last_published_at, _stale_wallet_ids) =
            seed_stage1_deferred_runtime_publication_refresh_fixture()?;

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
        assert!(repair.publication_state_exists_before);
        assert!(repair.publication_truth_complete_before);
        assert!(!repair.publication_truth_fresh_before);
        assert!(repair.runtime_cursor_exists_before);
        assert!(!repair.journal_store_exists);
        assert!(repair.runtime_window_complete_before);
        assert!(repair.publication_truth_refresh_delegated_to_runtime_cycle);
        assert!(repair.publication_truth_refresh_helper_write_attempted);
        assert!(repair.publication_truth_refresh_helper_write_succeeded);
        assert_eq!(
            repair
                .publication_truth_refresh_helper_write_resulting_reason
                .as_deref(),
            Some("publication_truth_withheld_while_replay_sol_leg_incomplete")
        );
        assert!(repair
            .publication_truth_refresh_helper_write_resulting_updated_at
            .is_some());
        Ok(())
    }

    #[test]
    fn repair_runtime_window_complete_without_publication_state_reports_no_helper_write_attempt_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-deferred-runtime-publication-refresh-without-state.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-12T21:03:21Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 100;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (replay_state, _metrics_window_start, _) =
            seed_stage1_partial_sol_leg_replay_checkpoint_fixture(
                &runtime_store,
                &discovery,
                &config,
                now,
                401,
                3,
            )?;
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
        assert!(!repair.publication_state_exists_before);
        assert!(repair.runtime_cursor_exists_before);
        assert!(repair.runtime_window_complete_before);
        assert!(!repair.journal_store_exists);
        assert!(!repair.publication_truth_refresh_helper_write_attempted);
        assert!(!repair.publication_truth_refresh_helper_write_succeeded);
        assert!(repair
            .publication_truth_refresh_helper_write_resulting_reason
            .is_none());
        assert!(repair
            .publication_truth_refresh_helper_write_resulting_updated_at
            .is_none());
        Ok(())
    }

    #[test]
    fn repair_runtime_window_incomplete_reports_missing_runtime_cursor_diagnostics_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-publication-repair-missing-runtime-cursor-diagnostics.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-13T07:20:24Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let discovery = DiscoveryService::new(stage1_runtime_config(), permissive_shadow_quality());

        let repair = discovery
            .repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed(
                &runtime_store,
                None,
                now,
                64,
                Instant::now() + StdDuration::from_secs(1),
            )?;

        assert_eq!(repair.state, "skipped_missing_runtime_cursor");
        assert!(!repair.publication_state_exists_before);
        assert!(!repair.publication_truth_complete_before);
        assert!(!repair.publication_truth_fresh_before);
        assert!(!repair.runtime_cursor_exists_before);
        assert!(!repair.journal_store_exists);
        assert!(!repair.runtime_window_complete_before);
        assert!(!repair.publication_truth_refresh_helper_write_attempted);
        Ok(())
    }

    #[test]
    fn run_cycle_partial_replay_downgrades_stale_healthy_publication_row_without_publish_due_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-partial-replay-downgrades-stale-healthy-publication-row.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-04T20:28:08Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 10;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (replay_state, metrics_window_start, _) =
            seed_stage1_partial_sol_leg_replay_checkpoint_fixture(
                &runtime_store,
                &discovery,
                &config,
                now,
                401,
                54,
            )?;
        runtime_store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&replay_state, now)?,
        )?;

        let stale_published_at = now - Duration::hours(1);
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "discovery_score_refresh".to_string(),
            last_published_at: Some(stale_published_at),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;
        {
            let mut state = discovery
                .window_state
                .lock()
                .expect("window_state lock should succeed");
            state.last_publish_at = Some(now);
        }

        let summary = discovery.run_cycle(&runtime_store, now)?;
        assert!(
            !summary.published,
            "the reduced live-like replay cycle must stay non-publish-due so the repro exercises stale healthy row retention instead of the exact publish path"
        );
        assert!(
            matches!(
                summary.runtime_mode,
                DiscoveryRuntimeMode::FailClosed | DiscoveryRuntimeMode::Degraded
            ),
            "the owning cycle should already know it is not currently publishing a fresh healthy universe while replay is incomplete"
        );

        let publication_state = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist after the partial replay cycle");
        assert_ne!(
            publication_state.runtime_mode,
            DiscoveryRuntimeMode::Healthy,
            "a partial replay cycle must not leave behind a stale healthy publication row when the persisted rebuild checkpoint is still incomplete"
        );
        assert_eq!(
            publication_state.last_published_at,
            Some(stale_published_at),
            "non-publish-due downgrade must preserve the previous exact published timestamp instead of forging a fresh publish"
        );
        assert!(
            publication_state
                .published_wallet_ids
                .as_ref()
                .is_some_and(|wallets| wallets.is_empty()),
            "the repro should keep the exact empty-wallet-universe shape while only downgrading the stale healthy runtime surface"
        );
        assert!(
            runtime_store.load_discovery_persisted_rebuild_state()?.is_some(),
            "the reduced live-like repro must still have an incomplete persisted replay checkpoint after the cycle"
        );
        assert!(
            discovery
                .runtime_publication_truth_resolution(&runtime_store, now)?
                .is_none(),
            "export-equivalent runtime truth must remain unavailable until replay really reaches an exact published universe flush"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_bucket_roll_resumes_stale_publishable_checkpoint_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-replay-stale-publishable-resume.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-17T12:00:59Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_now = source_now + Duration::seconds(1);
        let (window_start, metrics_window_start, _, _) =
            seed_stage1_replay_noise_fixture(&store, &config, source_now, 3, 12)?;
        let target_window_start =
            target_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_metrics_window_start = metrics_window_start_for_test(&config, target_now);
        let unique_buy_mints = store.load_observed_buy_mints_in_window(window_start, source_now)?;

        let mut state = discovery.start_persisted_stream_rebuild_state(
            window_start,
            metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.horizon_end = source_now;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        state.payload.unique_buy_mints = unique_buy_mints.clone();
        state.payload.buy_mint_counts = unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1u32))
            .collect();
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.replay_wallet_stats_complete = true;
        state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: window_start + Duration::minutes(10),
            slot: 42,
            signature: "stage1-stale-replay-sol-leg-cursor".to_string(),
        });
        state.replay_rows_processed = 111;
        state.replay_pages_processed = 7;
        state.payload.by_wallet.insert(
            "wallet_partial".to_string(),
            WalletAccumulator {
                trades: 11,
                ..WalletAccumulator::default()
            },
        );
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, source_now)?,
        )?;

        let (resumed, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            target_window_start,
            target_metrics_window_start,
            target_now,
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
        );
        assert_eq!(resumed.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert_eq!(resumed.metrics_window_start, metrics_window_start);
        assert_eq!(resumed.horizon_end, source_now);
        assert_eq!(resumed.phase_cursor, state.phase_cursor);
        assert_eq!(resumed.replay_rows_processed, state.replay_rows_processed);
        assert_eq!(resumed.replay_pages_processed, state.replay_pages_processed);
        assert_eq!(resumed.payload.by_wallet.len(), 1);
        assert!(
            resumed.payload.replay_wallet_stats_complete,
            "bucket rollover should preserve the completed wallet-stats prepass for a stale-but-still-publishable replay checkpoint instead of rewinding back into collect_buy_mints"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_bucket_roll_aged_out_post_wallet_stats_target_pins_frozen_lineage_until_first_publishable_checkpoint_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-aged-out-post-wallet-stats-frozen-lineage.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-10T15:45:35Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 100;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 25_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let target_one_now =
            now + Duration::seconds(config.metric_snapshot_interval_seconds as i64 + 61);
        let target_one_window_start =
            target_one_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_one_metrics_window_start =
            metrics_window_start_for_test(&config, target_one_now);
        let target_two_now =
            target_one_now + Duration::seconds(config.metric_snapshot_interval_seconds as i64 + 61);
        let target_two_window_start =
            target_two_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_two_metrics_window_start =
            metrics_window_start_for_test(&config, target_two_now);

        let (mut source_state, _source_metrics_window_start) =
            seed_stage1_dense_partial_sol_leg_replay_checkpoint_fixture(
                &store, &discovery, &config, now, 8_000, 20,
            )?;
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut source_state,
                target_one_window_start,
                target_one_metrics_window_start,
                target_one_now,
            )?
        );
        advance_carried_target_until_replay_or_upstream_block_for_test(
            &discovery,
            &store,
            &mut source_state,
            64,
            64,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
        )?;
        assert_eq!(source_state.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert!(source_state.payload.replay_wallet_stats_complete);
        assert_eq!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(
                &source_state
            ),
            "replay_sol_leg_incomplete"
        );
        let advance_partial_sol_leg_once = |state: &mut PersistedStreamRebuildState,
                                            cycle_now: DateTime<Utc>|
         -> Result<PersistedStreamPhaseAdvance> {
            let contract = discovery
                .deepen_persisted_stream_priority_recovery_contract_for_state_at(
                    state,
                    config.max_fetch_swaps_per_cycle,
                    config.max_fetch_pages_per_cycle,
                    PersistedStreamPriorityRecoveryContract {
                        time_budget: StdDuration::from_secs(60),
                        collect_buy_mints_phase_page_limit_override: Some(160),
                        replay_wallet_stats_phase_page_limit_override: Some(92),
                        replay_sol_leg_phase_page_limit_override: Some(300),
                        reason: Some("runtime_window_complete_stale_publication_truth"),
                    },
                    Some(cycle_now),
                );
            let advance = discovery
                .advance_persisted_stream_replay_optimized_with_wallet_stats_phase_page_limit(
                    &store,
                    state,
                    config.max_fetch_swaps_per_cycle,
                    config.max_fetch_pages_per_cycle,
                    contract.replay_wallet_stats_phase_page_limit_override,
                    contract.replay_sol_leg_phase_page_limit_override,
                    false,
                    Instant::now() + StdDuration::from_secs(30),
                )?;
            state.replay_rows_processed = state
                .replay_rows_processed
                .saturating_add(advance.rows_processed);
            state.replay_pages_processed = state
                .replay_pages_processed
                .saturating_add(advance.pages_processed);
            state.phase_cursor = advance.phase_cursor.clone();
            DiscoveryService::persist_partial_replay_frontier_hints_after_cycle(
                state,
                config.max_fetch_swaps_per_cycle,
                true,
                ReplayWalletStatsDayCountSourceProgress::default(),
                advance.replay_sol_leg_elapsed_ms.max(1),
                advance.budget_exhausted_reason,
                contract.replay_sol_leg_phase_page_limit_override,
                advance.replay_sol_leg_pages_processed,
                advance.replay_sol_leg_rows_processed,
                advance.replay_sol_leg_elapsed_ms,
            );
            Ok(advance)
        };
        let first_partial_sol_leg_advance =
            advance_partial_sol_leg_once(&mut source_state, target_one_now)?;
        assert!(
            !first_partial_sol_leg_advance.source_exhausted,
            "the first frozen-target cycle must stay mid-sol-leg so the next bucket can reproduce the moving-goalpost seam instead of trivially exhausting the source"
        );
        let second_partial_sol_leg_advance =
            advance_partial_sol_leg_once(&mut source_state, target_one_now)?;
        assert!(
            !second_partial_sol_leg_advance.source_exhausted,
            "the frozen-target lineage must still own a materially advanced downstream sol-leg frontier after the second bounded cycle; otherwise the next bucket would not reproduce the exact moving-goalpost blocker"
        );
        assert!(
            !discovery.state_can_resume_stale_metrics_window_until_publish_checkpoint(
                &source_state,
                target_two_now,
            ),
            "this reduced live-like checkpoint must already be aged out of the ordinary publishable-gate resume path before the new frozen-lineage pinning branch is exercised"
        );
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&source_state, target_one_now)?,
        )?;

        let (resumed, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            target_two_window_start,
            target_two_metrics_window_start,
            target_two_now,
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow,
            "once the post-wallet-stats downstream target ages out before its first exact checkpoint, the new behavior must keep the frozen lineage alive instead of rebasing it onto another moving target"
        );
        assert_eq!(resumed.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert_eq!(resumed.window_start, source_state.window_start);
        assert_eq!(
            resumed.metrics_window_start,
            source_state.metrics_window_start
        );
        assert_eq!(resumed.horizon_end, source_state.horizon_end);
        assert_eq!(resumed.phase_cursor, source_state.phase_cursor);
        assert_eq!(
            resumed.replay_rows_processed,
            source_state.replay_rows_processed
        );
        assert_eq!(
            resumed.replay_pages_processed,
            source_state.replay_pages_processed
        );
        assert_eq!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(&resumed),
            "replay_sol_leg_incomplete"
        );
        Ok(())
    }
