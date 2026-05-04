    #[test]
    fn persisted_stream_priority_recovery_contract_scales_for_live_rollover_wallet_stats_backlog_stage1(
    ) {
        let now = DateTime::parse_from_rfc3339("2026-04-03T05:34:03Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 20_000;
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
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.unique_buy_mints =
            vec!["TokenReplayDeep11111111111111111111111111".to_string()];
        state.payload.token_quality_progress.next_mint_index = 28_307;
        state.payload.replay_wallet_stats_rows_processed = 12_419_939;
        state.payload.replay_wallet_stats_pages_processed = 733;
        state.payload.replay_wallet_stats_wallet_cursor =
            Some("rollover-live-wallet-stats-cursor".to_string());
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .fast_path_wallets_processed = 656_100;

        let base_contract = PersistedStreamPriorityRecoveryContract {
            time_budget: StdDuration::from_secs(60),
            collect_buy_mints_phase_page_limit_override: Some(160),
            replay_wallet_stats_phase_page_limit_override: Some(92),
            replay_sol_leg_phase_page_limit_override: Some(300),
            reason: Some("runtime_window_complete_stale_publication_truth"),
        };
        let deep_contract = discovery.deepen_persisted_stream_priority_recovery_contract_for_state(
            &state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            base_contract,
        );

        assert_eq!(
            deep_contract.time_budget,
            StdDuration::from_millis(2_475_000)
        );
        assert_eq!(
            deep_contract.reason,
            Some("deep_replay_wallet_stats_large_processed_backlog")
        );
        assert!(
            deep_contract
                .replay_wallet_stats_phase_page_limit_override
                .is_some_and(|limit| limit > 1_380),
            "the exact live rollover wallet-stats checkpoint should now scale beyond the older 900s/1380-page lane instead of stalling under the same capped contract"
        );
    }

    #[test]
    fn persisted_stream_priority_recovery_contract_uses_carried_wallet_stats_budget_floor_stage1() {
        let now = DateTime::parse_from_rfc3339("2026-04-03T05:09:30Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 20_000;
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
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.unique_buy_mints =
            vec!["TokenReplayDeep11111111111111111111111111".to_string()];
        state.payload.token_quality_progress.next_mint_index = 28_307;
        state.payload.replay_wallet_stats_rows_processed = 417_187;
        state.payload.replay_wallet_stats_pages_processed = 23;
        state.payload.replay_wallet_stats_wallet_cursor =
            Some("carry-forward-replay-wallet-stats-cursor".to_string());
        state.payload.by_wallet.insert(
            "wallet_live_like_partial".to_string(),
            WalletAccumulator {
                trades: 4,
                ..WalletAccumulator::default()
            },
        );
        state.payload.replay_wallet_stats_budget_floor_wallets = 703_826;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_pages_processed = 406;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_elapsed_ms = 817_313;

        let base_contract = PersistedStreamPriorityRecoveryContract {
            time_budget: StdDuration::from_secs(60),
            collect_buy_mints_phase_page_limit_override: Some(160),
            replay_wallet_stats_phase_page_limit_override: Some(92),
            replay_sol_leg_phase_page_limit_override: Some(300),
            reason: Some("runtime_window_complete_stale_publication_truth"),
        };
        let deep_contract = discovery.deepen_persisted_stream_priority_recovery_contract_for_state(
            &state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            base_contract,
        );

        assert_eq!(
            deep_contract.time_budget,
            StdDuration::from_millis(2_643_750)
        );
        assert_eq!(
            deep_contract.reason,
            Some("deep_replay_wallet_stats_large_buffered_backlog")
        );
        assert!(
            deep_contract
                .replay_wallet_stats_phase_page_limit_override
                .is_some_and(|limit| limit > 2_475),
            "post-rollover replay should immediately reuse the carried wallet frontier as a budgeting floor instead of relearning backlog from near-zero buffered wallets before the deep contract can widen"
        );
    }

    #[test]
    fn persisted_stream_priority_recovery_contract_scales_for_open_live_wallet_stats_frontier_stage1(
    ) {
        let now = DateTime::parse_from_rfc3339("2026-04-03T11:52:52Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 20_000;
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
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.unique_buy_mints =
            vec!["TokenReplayDeep11111111111111111111111111".to_string()];
        state.payload.token_quality_progress.next_mint_index = 27_017;
        state.payload.replay_wallet_stats_rows_processed = 6_681_119;
        state.payload.replay_wallet_stats_pages_processed = 420;
        state.payload.replay_wallet_stats_wallet_cursor =
            Some("deep-open-frontier-wallet-stats-cursor".to_string());
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .fast_path_wallets_processed = 375_300;
        state.payload.replay_wallet_stats_budget_floor_wallets = 375_300;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_pages_processed = 273;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_elapsed_ms = 492_750;

        let base_contract = PersistedStreamPriorityRecoveryContract {
            time_budget: StdDuration::from_secs(60),
            collect_buy_mints_phase_page_limit_override: Some(160),
            replay_wallet_stats_phase_page_limit_override: Some(92),
            replay_sol_leg_phase_page_limit_override: Some(300),
            reason: Some("runtime_window_complete_stale_publication_truth"),
        };
        let deep_contract = discovery.deepen_persisted_stream_priority_recovery_contract_for_state(
            &state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            base_contract,
        );

        assert_eq!(
            deep_contract.time_budget,
            StdDuration::from_millis(2_340_000)
        );
        assert_eq!(
            deep_contract.reason,
            Some("deep_replay_wallet_stats_open_frontier_backlog")
        );
        assert!(
            deep_contract
                .replay_wallet_stats_phase_page_limit_override
                .is_some_and(|limit| limit > 2_185),
            "when the resumed wallet-stats frontier is still saturated across the last bounded chunk, the contract should widen beyond the older processed-only floor instead of waiting for another equally full cycle to prove the same fact again"
        );
    }

    #[test]
    fn persisted_stream_priority_recovery_contract_caps_open_live_wallet_stats_frontier_to_publishable_horizon_stage1(
    ) {
        let now = DateTime::parse_from_rfc3339("2026-04-03T17:24:26Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let current_metrics_window_start = metrics_window_start_for_test(&config, now);
        let stale_metrics_window_start = current_metrics_window_start
            - Duration::seconds(config.metric_snapshot_interval_seconds as i64);
        let mut state = discovery.start_persisted_stream_rebuild_state(
            now - Duration::days(config.scoring_window_days.max(1) as i64),
            stale_metrics_window_start,
            now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.horizon_end = now - Duration::milliseconds(1_951_712);
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.unique_buy_mints =
            vec!["TokenReplayDeep11111111111111111111111111".to_string()];
        state.payload.buy_mint_counts = state
            .payload
            .unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1u32))
            .collect();
        state.payload.token_quality_progress.next_mint_index = state.payload.unique_buy_mints.len();
        state.payload.replay_wallet_stats_rows_processed = 485_944;
        state.payload.replay_wallet_stats_pages_processed = 31;
        state.payload.replay_wallet_stats_wallet_cursor =
            Some("stage1-live-wallet-stats-open-frontier-cursor".to_string());
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .fast_path_wallets_processed = 27_000;
        state.payload.replay_wallet_stats_budget_floor_wallets = 662_481;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_pages_processed = 30;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_wallets_processed = 27_000;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_elapsed_ms = 60_003;
        assert_ne!(state.metrics_window_start, current_metrics_window_start);
        assert!(
            discovery.state_can_resume_stale_metrics_window_until_publish_checkpoint(&state, now),
            "the exact fresh rebuilt d622a0e live checkpoint should still be on a stale-but-publishable replay target when this cap logic runs"
        );

        let base_contract = PersistedStreamPriorityRecoveryContract {
            time_budget: StdDuration::from_secs(60),
            collect_buy_mints_phase_page_limit_override: Some(160),
            replay_wallet_stats_phase_page_limit_override: Some(92),
            replay_sol_leg_phase_page_limit_override: Some(300),
            reason: Some("runtime_window_complete_stale_publication_truth"),
        };
        let backlog_only_contract = discovery
            .deepen_persisted_stream_priority_recovery_contract_for_state(
                &state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                base_contract,
            );
        assert_eq!(
            DiscoveryService::replay_wallet_stats_open_frontier_floor_pages(
                config.max_fetch_swaps_per_cycle,
                &state,
            ),
            30
        );
        assert_eq!(
            DiscoveryService::replay_wallet_stats_progress_floor_pages(
                config.max_fetch_swaps_per_cycle,
                &state,
            ),
            737
        );
        assert_eq!(
            DiscoveryService::replay_wallet_stats_catch_up_page_limit(
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
            ),
            23
        );
        let snapshot_interval =
            Duration::seconds(config.metric_snapshot_interval_seconds.max(1) as i64);
        let horizon_gate_remaining_ms = (state.horizon_end + snapshot_interval)
            .signed_duration_since(now)
            .num_milliseconds()
            .max(0) as u64;
        let metrics_window_gate_remaining_ms = (state.metrics_window_start
            + Duration::days(config.scoring_window_days.max(1) as i64)
            + snapshot_interval
            + snapshot_interval)
            .signed_duration_since(now)
            .num_milliseconds()
            .max(0) as u64;
        assert_eq!(horizon_gate_remaining_ms, 1_648_288);
        assert_eq!(metrics_window_gate_remaining_ms, 2_134_000);
        assert!(
            metrics_window_gate_remaining_ms > horizon_gate_remaining_ms,
            "for the exact fresh rebuilt d622a0e live checkpoint, the stale target still ages out on the horizon gate before the metrics-window gate, so any bounded wallet-stats lane that spends longer than horizon_remaining is guaranteed to outrun the publishable window"
        );
        assert_eq!(
            discovery.replay_wallet_stats_remaining_publishable_horizon_ms(&state, now),
            Some(1_648_288)
        );
        let publishable_horizon_contract = discovery
            .deepen_persisted_stream_priority_recovery_contract_for_state_at(
                &state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                base_contract,
                Some(now),
            );

        assert_eq!(
            backlog_only_contract.time_budget,
            StdDuration::from_millis(2_589_750),
            "the raw backlog/open-frontier heuristic still reproduces the exact fresh rebuilt d622a0e checkpoint budget from the measured logs"
        );
        assert_eq!(
            backlog_only_contract.reason,
            Some("deep_replay_wallet_stats_open_frontier_backlog")
        );
        assert_eq!(
            publishable_horizon_contract.time_budget,
            StdDuration::from_millis(1_648_288),
            "once the exact stale target only has 1_648_288ms of publishable life left, the replay contract must cap itself there instead of burning a longer bounded cycle on a checkpoint that cannot remain publishable for that long"
        );
        assert_eq!(
            publishable_horizon_contract.reason,
            Some("deep_replay_wallet_stats_publishable_horizon_cap")
        );
        assert!(
            publishable_horizon_contract
                .replay_wallet_stats_phase_page_limit_override
                .zip(backlog_only_contract.replay_wallet_stats_phase_page_limit_override)
                .is_some_and(|(publishable_horizon_limit, backlog_only_limit)| {
                    publishable_horizon_limit < backlog_only_limit
                }),
            "the publishable-horizon cap should materially narrow the stale-target replay ceiling down to the remaining publishable lifetime instead of letting a longer bounded cycle overrun the stale-target gate"
        );
    }

    #[test]
    fn persisted_stream_replay_resume_backfills_missing_wallet_stats_budget_hint_from_existing_progress_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-replay-resume-backfills-budget-floor.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-03T07:24:59Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let restart_now = now + Duration::seconds(24);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 20_000;
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
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        state.payload.unique_buy_mints =
            vec!["TokenReplayDeep11111111111111111111111111".to_string()];
        state.payload.buy_mint_counts = state
            .payload
            .unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1u32))
            .collect();
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.replay_wallet_stats_complete = false;
        state.payload.replay_wallet_stats_rows_processed = 417_187;
        state.payload.replay_wallet_stats_pages_processed = 31;
        state.payload.replay_wallet_stats_wallet_cursor =
            Some("resume-backfill-wallet-stats-cursor".to_string());
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .fast_path_wallets_processed = 27_000;
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, now)?,
        )?;

        let (resumed, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            state.window_start,
            state.metrics_window_start,
            restart_now,
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(
            resumed.payload.replay_wallet_stats_budget_floor_wallets,
            27_000,
            "resume should backfill the missing wallet frontier hint from already-persisted wallet-stats progress instead of waiting for another full bounded cycle"
        );
        assert_eq!(
            resumed
                .payload
                .replay_wallet_stats_last_partial_cycle_pages_processed,
            0
        );
        assert_eq!(
            resumed
                .payload
                .replay_wallet_stats_last_partial_cycle_elapsed_ms,
            0
        );
        let persisted = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            persisted.payload.replay_wallet_stats_budget_floor_wallets,
            27_000,
            "resume-time backfill should be durably flushed so the next restart or bounded cycle sees the repaired budgeting floor immediately"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_resume_backfills_missing_sol_leg_budget_floor_from_existing_progress_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-replay-resume-backfills-sol-leg-budget-floor.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-10T12:26:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let restart_now = now + Duration::seconds(24);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let current_metrics_window_start = metrics_window_start_for_test(&config, now);
        let stale_metrics_window_start = current_metrics_window_start
            - Duration::seconds(config.metric_snapshot_interval_seconds as i64);
        let mut state = discovery.start_persisted_stream_rebuild_state(
            now - Duration::days(config.scoring_window_days.max(1) as i64),
            stale_metrics_window_start,
            now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.horizon_end = now;
        state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(3),
            slot: 42,
            signature: "resume-backfill-sol-leg-cursor".to_string(),
        });
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        state.payload.unique_buy_mints =
            vec!["TokenReplayDeep11111111111111111111111111".to_string()];
        state.payload.buy_mint_counts = state
            .payload
            .unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1u32))
            .collect();
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.replay_wallet_stats_complete = true;
        state.payload.replay_wallet_stats_milestone_reached = true;
        state.payload.replay_candidate_activity_backfill_required = true;
        state.replay_rows_processed = 121_259;
        state.replay_pages_processed = 7;
        state.payload.by_wallet.insert(
            "wallet_resume_sol_leg".to_string(),
            WalletAccumulator::default(),
        );
        state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed = 136;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_rows_processed = 2_714_250;
        state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms = 959_933;
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, now)?,
        )?;

        let (resumed, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            state.window_start,
            state.metrics_window_start,
            restart_now,
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(
            resumed.payload.replay_sol_leg_budget_floor_pages,
            7,
            "resume should backfill the missing carried SOL-leg budgeting floor from the already-persisted replay prefix instead of waiting for another stale-target partial cycle to relearn it"
        );
        assert_eq!(
            resumed
                .payload
                .replay_sol_leg_last_partial_cycle_pages_processed,
            136
        );
        assert_eq!(
            resumed.payload.replay_sol_leg_last_partial_cycle_elapsed_ms,
            959_933
        );
        let persisted = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            persisted.payload.replay_sol_leg_budget_floor_pages,
            7,
            "resume-time SOL-leg budget-floor backfill should be durably flushed so the next bounded cycle sees the repaired carried replay prefix immediately"
        );
        Ok(())
    }
