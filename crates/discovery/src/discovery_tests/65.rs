#[test]
    fn persisted_stream_priority_recovery_contract_scales_open_live_sol_leg_frontier_to_publishable_horizon_stage1(
    ) {
        let now = DateTime::parse_from_rfc3339("2026-04-04T14:00:00Z")
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
        state.horizon_end = now;
        state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(1),
            slot: 409_909_030,
            signature: "stage1-sol-leg-publishable-horizon".to_string(),
        });
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.replay_wallet_stats_complete = true;
        state.payload.replay_wallet_stats_milestone_reached = true;
        state.payload.replay_candidate_activity_backfill_required = true;
        state.payload.unique_buy_mints =
            vec!["TokenReplaySolLegPublishable111111111111111111".to_string()];
        state.payload.buy_mint_counts = state
            .payload
            .unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1u32))
            .collect();
        state.payload.by_wallet.insert(
            "wallet_replay_sol_leg_publishable_horizon".to_string(),
            WalletAccumulator::default(),
        );
        state.replay_rows_processed = 5_921_305;
        state.replay_pages_processed = 298;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed = 186;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_rows_processed = 3_706_494;
        state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms = 837_001;
        assert!(
            discovery.state_can_resume_stale_metrics_window_until_publish_checkpoint(&state, now),
            "the reduced live-shaped replay_sol_leg checkpoint should still be on a stale-but-publishable target when the contract is widened"
        );
        assert_eq!(
            discovery.replay_sol_leg_remaining_publishable_horizon_ms(&state, now),
            Some(3_600_000)
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
            backlog_only_contract.reason,
            Some("deep_replay_sol_leg_open_frontier_backlog")
        );
        assert_eq!(
            backlog_only_contract.time_budget,
            StdDuration::from_millis(900_000),
            "without the publishable-horizon extension, the live-shaped sol_leg checkpoint stays pinned to the static 900s cap even though the same stale target can still be safely resumed much longer"
        );
        assert_eq!(
            backlog_only_contract.replay_sol_leg_phase_page_limit_override,
            Some(300)
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
            publishable_horizon_contract.reason,
            Some("deep_replay_sol_leg_publishable_horizon_backlog")
        );
        assert_eq!(
            publishable_horizon_contract.time_budget,
            StdDuration::from_millis(3_267_726),
            "the same reduced live-shaped sol_leg checkpoint must budget against its observed 4_501ms/page replay cost, not the nominal 3_000ms/page fetch contract, once the stale target still has enough publishable lifetime left"
        );
        assert_eq!(
            publishable_horizon_contract.replay_sol_leg_phase_page_limit_override,
            Some(1_090)
        );
    }

    #[test]
    fn persisted_stream_priority_recovery_contract_live_like_slow_replay_sol_leg_requires_observed_page_cost_stage1(
    ) {
        let now = DateTime::parse_from_rfc3339("2026-04-04T14:00:00Z")
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
        state.horizon_end = now;
        state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(1),
            slot: 409_909_030,
            signature: "stage1-sol-leg-observed-ms-per-page".to_string(),
        });
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.replay_wallet_stats_complete = true;
        state.payload.replay_wallet_stats_milestone_reached = true;
        state.payload.replay_candidate_activity_backfill_required = true;
        state.payload.unique_buy_mints =
            vec!["TokenReplaySolLegObserved1111111111111111111".to_string()];
        state.payload.buy_mint_counts = state
            .payload
            .unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1u32))
            .collect();
        state.payload.by_wallet.insert(
            "wallet_replay_sol_leg_observed_cost".to_string(),
            WalletAccumulator::default(),
        );
        state.replay_rows_processed = 5_921_305;
        state.replay_pages_processed = 298;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed = 186;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_rows_processed = 3_706_494;
        state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms = 837_001;

        let target_floor_pages = DiscoveryService::replay_sol_leg_processed_floor_pages(
            config.max_fetch_swaps_per_cycle,
            &state,
        )
        .saturating_add(DiscoveryService::replay_sol_leg_open_frontier_floor_pages(
            config.max_fetch_swaps_per_cycle,
            &state,
        ))
        .max(DiscoveryService::replay_sol_leg_processed_floor_pages(
            config.max_fetch_swaps_per_cycle,
            &state,
        ));
        let floor_pages_with_headroom = target_floor_pages
            .saturating_mul(
                DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_PAGE_HEADROOM_NUMERATOR,
            )
            .div_ceil(
                DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_PAGE_HEADROOM_DENOMINATOR,
            );
        let legacy_target_ms_per_page = config
            .fetch_time_budget_ms
            .div_ceil(config.max_fetch_pages_per_cycle as u64);
        let observed_target_ms_per_page =
            discovery.replay_sol_leg_target_ms_per_page(config.max_fetch_pages_per_cycle, &state);
        assert_eq!(legacy_target_ms_per_page, 3_000);
        assert_eq!(observed_target_ms_per_page, 4_501);

        let legacy_floor_budget_ms = floor_pages_with_headroom as u64 * legacy_target_ms_per_page;
        let legacy_coverable_pages_under_observed_cost =
            legacy_floor_budget_ms / observed_target_ms_per_page.max(1);
        assert!(
            legacy_coverable_pages_under_observed_cost < floor_pages_with_headroom as u64,
            "the old static sol-leg contract underbudgets this live-like replay checkpoint: it only covers {legacy_coverable_pages_under_observed_cost} of {floor_pages_with_headroom} required headroom pages once the stored 4_501ms/page replay cost is honored"
        );

        let publishable_horizon_contract = discovery
            .deepen_persisted_stream_priority_recovery_contract_for_state_at(
                &state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                PersistedStreamPriorityRecoveryContract {
                    time_budget: StdDuration::from_secs(60),
                    collect_buy_mints_phase_page_limit_override: Some(160),
                    replay_wallet_stats_phase_page_limit_override: Some(92),
                    replay_sol_leg_phase_page_limit_override: Some(300),
                    reason: Some("runtime_window_complete_stale_publication_truth"),
                },
                Some(now),
            );
        let new_coverable_pages_under_observed_cost =
            publishable_horizon_contract.time_budget.as_millis() as u64
                / observed_target_ms_per_page.max(1);
        assert!(
            new_coverable_pages_under_observed_cost >= floor_pages_with_headroom as u64,
            "the observed-cost-aware contract must budget enough time for the same live-like frontier instead of leaving it mathematically underprovisioned on the stored replay cost"
        );
    }
