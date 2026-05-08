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
