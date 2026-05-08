    #[test]
    fn persisted_stream_priority_recovery_contract_retains_proven_sol_leg_contract_floor_stage1() {
        let now = DateTime::parse_from_rfc3339("2026-04-04T12:39:36Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.scoring_window_days = 30;
        config.decay_window_days = 30;
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 10;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let mut state = discovery.start_persisted_stream_rebuild_state(
            now - Duration::days(config.scoring_window_days.max(1) as i64),
            metrics_window_start_for_test(&config, now),
            now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.horizon_end = now;
        state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(1),
            slot: 409_909_030,
            signature: "stage1-sol-leg-retained-floor".to_string(),
        });
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.replay_wallet_stats_complete = true;
        state.payload.replay_wallet_stats_milestone_reached = true;
        state.payload.replay_candidate_activity_backfill_required = true;
        state.payload.by_wallet.insert(
            "wallet_sol_leg_retained_floor".to_string(),
            WalletAccumulator::default(),
        );
        state.replay_rows_processed = 650;
        state.replay_pages_processed = 65;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed = 60;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_rows_processed = 600;
        state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms = 139_747;

        let base_contract = PersistedStreamPriorityRecoveryContract {
            time_budget: StdDuration::from_secs(60),
            collect_buy_mints_phase_page_limit_override: Some(160),
            replay_wallet_stats_phase_page_limit_override: Some(92),
            replay_sol_leg_phase_page_limit_override: Some(300),
            reason: Some("runtime_window_complete_stale_publication_truth"),
        };

        let open_frontier_only_contract = discovery
            .deepen_persisted_stream_priority_recovery_contract_for_state(
                &state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                base_contract,
            );
        assert_eq!(
            open_frontier_only_contract.reason,
            Some("deep_replay_sol_leg_open_frontier_backlog")
        );
        assert_eq!(
            open_frontier_only_contract.replay_sol_leg_phase_page_limit_override,
            Some(190),
            "without retained contract memory, the second sol_leg checkpoint-specific lane shrinks to the 65 + 60 page heuristic even though the prior 300-page lane already proved insufficient on the same blocker"
        );

        state.payload.replay_sol_leg_retained_contract_floor_pages = 300;
        let retained_floor_contract = discovery
            .deepen_persisted_stream_priority_recovery_contract_for_state(
                &state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                base_contract,
            );
        assert_eq!(
            retained_floor_contract.reason,
            Some("deep_replay_sol_leg_retained_contract_floor")
        );
        assert_eq!(
            retained_floor_contract.replay_sol_leg_phase_page_limit_override,
            Some(300),
            "once a pure sol_leg checkpoint has already spent a 300-page bounded lane and still stayed blocked on sol_leg source exhaustion, the next contract must not collapse back to 190 pages on the same lineage"
        );
        assert_eq!(
            retained_floor_contract.time_budget,
            StdDuration::from_millis(900_000),
            "the retained sol_leg contract floor should preserve the already-proven insufficient 900s lane instead of relearning a smaller open-frontier budget from scratch"
        );
    }

    #[test]
    fn run_cycle_live_like_resumed_replay_sol_leg_open_frontier_progress_publishes_after_second_cycle_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-run-cycle-live-like-sol-leg-open-frontier.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-04T10:08:48Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 10;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (replay_state, metrics_window_start, _sol_leg_rows) =
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

        let first_summary = discovery.run_cycle(&runtime_store, now)?;
        assert!(!first_summary.published);
        let after_first = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert_eq!(after_first.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert_eq!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(
                &after_first
            ),
            "replay_sol_leg_incomplete",
            "the first cycle should deterministically reproduce the exact live-shaped downstream blocker: stable replay_sol_leg_incomplete with empty publication truth"
        );
        assert!(after_first.payload.replay_wallet_stats_complete);
        assert!(
            after_first
                .payload
                .replay_sol_leg_last_partial_cycle_pages_processed
                > 0
        );
        assert!(
            after_first
                .payload
                .replay_sol_leg_last_partial_cycle_rows_processed
                > 0
        );
        assert!(
            after_first
                .payload
                .replay_sol_leg_retained_contract_floor_pages
                > 0,
            "once the first pure sol_leg cycle proves the blocker remains, the checkpoint must retain that exact sol_leg contract floor so the next local cycle does not shrink below the already-proven insufficient lane"
        );
        let publication_after_first = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist after the first partial sol_leg cycle");
        assert!(
            publication_after_first
                .published_wallet_ids
                .as_ref()
                .is_some_and(|wallets| wallets.is_empty()),
            "the first live-shaped sol_leg cycle should still leave export-visible publication truth empty, matching the measured fail-closed blocker"
        );

        let second_summary = discovery.run_cycle(&runtime_store, now + Duration::seconds(1))?;
        assert_eq!(second_summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert!(second_summary.published);
        let publication_after_second = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist after the second cycle publishes");
        assert_eq!(
            publication_after_second.runtime_mode,
            DiscoveryRuntimeMode::Healthy
        );
        assert_eq!(
            publication_after_second.last_published_window_start,
            Some(metrics_window_start)
        );
        assert!(
            publication_after_second
                .published_wallet_ids
                .as_ref()
                .is_some_and(|wallets| !wallets.is_empty()),
            "once the same reduced live-shaped sol_leg checkpoint persists its saturated partial frontier, the next local cycle should move through candidate backfill into a real non-empty published universe"
        );
        assert!(
            runtime_store
                .load_discovery_persisted_rebuild_state()?
                .is_none(),
            "after the downstream sol_leg blocker clears and publish succeeds, the persisted replay checkpoint should clear instead of leaving recovery pinned on stale partial state"
        );
        Ok(())
    }

    #[test]
    fn run_cycle_live_like_resumed_replay_sol_leg_publishable_horizon_progress_moves_beyond_sol_leg_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-run-cycle-live-like-sol-leg-publishable-horizon.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-04T12:40:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 10;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (mut replay_state, metrics_window_start) =
            seed_stage1_dense_partial_sol_leg_replay_checkpoint_fixture(
                &runtime_store,
                &discovery,
                &config,
                now,
                1_500,
                298,
            )?;
        replay_state.payload.replay_wallet_stats_complete = true;
        replay_state.payload.replay_wallet_stats_milestone_reached = true;
        replay_state
            .payload
            .replay_candidate_activity_backfill_required = true;
        replay_state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed = 186;
        replay_state
            .payload
            .replay_sol_leg_last_partial_cycle_rows_processed = 1_860;
        replay_state
            .payload
            .replay_sol_leg_last_partial_cycle_elapsed_ms = 837_001;
        replay_state
            .payload
            .replay_sol_leg_retained_contract_floor_pages = 0;

        let base_contract = PersistedStreamPriorityRecoveryContract {
            time_budget: StdDuration::from_secs(60),
            collect_buy_mints_phase_page_limit_override: Some(160),
            replay_wallet_stats_phase_page_limit_override: Some(92),
            replay_sol_leg_phase_page_limit_override: Some(300),
            reason: Some("runtime_window_complete_stale_publication_truth"),
        };
        let adjusted_contract = discovery
            .deepen_persisted_stream_priority_recovery_contract_for_state_at(
                &replay_state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                base_contract,
                Some(now),
            );
        assert_eq!(
            adjusted_contract.reason,
            Some("deep_replay_sol_leg_publishable_horizon_backlog")
        );
        assert_eq!(
            adjusted_contract.replay_sol_leg_phase_page_limit_override,
            Some(1_090),
            "the reduced live-like resumed sol_leg checkpoint should now widen past the static 300-page cap using its observed sol-leg replay cost, not just nominal fetch-time page math, once the remaining open frontier still fits safely inside the publishable horizon"
        );

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
        let publication_state = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist after the downstream sol_leg cycle");
        if let Some(_) = runtime_store.load_discovery_persisted_rebuild_state()? {
            let rebuild = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
            assert_ne!(
                DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(
                    &rebuild
                ),
                "replay_sol_leg_incomplete",
                "once the same reduced live-like sol_leg checkpoint can spend the full frontier-derived lane instead of the static 300-page cap, it should move beyond the sol_leg blocker even if candidate backfill still needs another bounded cycle"
            );
            assert!(
                rebuild.payload.replay_candidate_activity_backfill_pending
                    || rebuild.phase == DiscoveryPersistedRebuildPhase::PublishPending,
                "after exhausting the remaining sol_leg source on the widened live-like lane, the persisted checkpoint should either be inside candidate activity backfill or already waiting to publish"
            );
            return Ok(());
        }

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert!(summary.published);
        assert!(
            publication_state
                .published_wallet_ids
                .as_ref()
                .is_some_and(|wallets| !wallets.is_empty()),
            "if the widened live-like sol_leg lane can finish candidate backfill in the same local cycle, it must publish a real non-empty wallet universe rather than leaving publication truth empty"
        );
        Ok(())
    }
