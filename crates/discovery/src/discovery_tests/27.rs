    #[test]
    fn run_cycle_runtime_window_complete_resumed_replay_sol_leg_uses_deeper_priority_contract_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-run-cycle-deep-replay-sol-leg-priority-contract.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-02T18:15:01Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 100;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (replay_state, metrics_window_start, sol_leg_rows) =
            seed_stage1_partial_sol_leg_replay_checkpoint_fixture(
                &runtime_store,
                &discovery,
                &config,
                now,
                401,
                3,
            )?;
        assert!(
            sol_leg_rows
                > replay_state
                    .replay_rows_processed
                    .saturating_add(
                        config.max_fetch_swaps_per_cycle * config.max_fetch_pages_per_cycle
                ),
            "fixture must leave more than one ordinary bounded-page sol-leg suffix so the resumed live checkpoint reproduces the exact page-budget blocker"
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
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert!(summary.published);
        let publication_state = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist after the resumed sol-leg cycle publishes");
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
            "once the resumed sol-leg checkpoint gets its deeper bounded contract, the owning run_cycle should reach PublishPending and write a real non-empty published universe"
        );
        assert!(
            runtime_store
                .load_discovery_persisted_rebuild_state()?
                .is_none(),
            "after the deeper sol-leg contract reaches publish, the persisted replay checkpoint should clear"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_sol_leg_retained_contract_floor_requires_page_budgeted_full_lane_stage1(
    ) {
        let now = DateTime::parse_from_rfc3339("2026-04-04T12:39:29Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let window_start = metrics_window_start;
        let horizon_end = window_start + Duration::days(config.scoring_window_days as i64);

        let base_state = PersistedStreamRebuildState {
            phase: DiscoveryPersistedRebuildPhase::Replay,
            window_start,
            horizon_end,
            metrics_window_start,
            phase_cursor: Some(DiscoveryRuntimeCursor {
                ts_utc: now - Duration::minutes(1),
                slot: 456,
                signature: "stage1-sol-leg-retained-floor-threshold".to_string(),
            }),
            payload: PersistedStreamRebuildPayload {
                replay_mode: ReplayMode::WalletStatsThenSolLeg,
                replay_wallet_stats_complete: true,
                replay_wallet_stats_milestone_reached: true,
                replay_candidate_activity_backfill_required: true,
                by_wallet: HashMap::from_iter([(
                    "wallet_sol_leg_threshold".to_string(),
                    WalletAccumulator::default(),
                )]),
                ..PersistedStreamRebuildPayload::default()
            },
            prepass_rows_processed: 0,
            prepass_pages_processed: 0,
            replay_rows_processed: 650,
            replay_pages_processed: 65,
            chunks_completed: 0,
            started_at: now - Duration::minutes(5),
            updated_at: now - Duration::seconds(1),
        };

        let mut time_budget_state = base_state.clone();
        DiscoveryService::persist_partial_replay_frontier_hints_after_cycle(
            &mut time_budget_state,
            config.max_fetch_swaps_per_cycle,
            true,
            ReplayWalletStatsDayCountSourceProgress::default(),
            139_747,
            Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            Some(300),
            60,
            1_200_000,
            139_747,
        );
        assert_eq!(
            time_budget_state.payload.replay_sol_leg_retained_contract_floor_pages,
            0,
            "a time-budgeted partial sol_leg cycle must not retain the full 300-page lane because it did not prove the whole lane was exhausted"
        );

        let mut page_budget_state = base_state;
        DiscoveryService::persist_partial_replay_frontier_hints_after_cycle(
            &mut page_budget_state,
            config.max_fetch_swaps_per_cycle,
            true,
            ReplayWalletStatsDayCountSourceProgress::default(),
            139_747,
            Some(PersistedStreamBudgetExhaustedReason::PageBudget),
            Some(300),
            300,
            3_000_000,
            139_747,
        );
        assert_eq!(
            page_budget_state.payload.replay_sol_leg_retained_contract_floor_pages,
            300,
            "once the pure sol_leg cycle actually spends the full 300-page lane and still yields partial, the retained floor should record that exact proven-insufficient lane for the next checkpoint-specific contract"
        );
    }

    #[test]
    fn persisted_stream_priority_recovery_contract_scales_for_open_frontier_resumed_replay_sol_leg_checkpoint_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-sol-leg-open-frontier-priority-contract.db");
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
        let (mut replay_state, _metrics_window_start, sol_leg_rows) =
            seed_stage1_partial_sol_leg_replay_checkpoint_fixture(
                &runtime_store,
                &discovery,
                &config,
                now,
                401,
                54,
            )?;
        assert_eq!(replay_state.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert!(replay_state.payload.replay_wallet_stats_complete);
        assert_eq!(replay_state.replay_pages_processed, 54);
        assert_eq!(replay_state.replay_rows_processed, 540);

        let base_contract = PersistedStreamPriorityRecoveryContract {
            time_budget: StdDuration::from_secs(60),
            collect_buy_mints_phase_page_limit_override: Some(160),
            replay_wallet_stats_phase_page_limit_override: Some(92),
            replay_sol_leg_phase_page_limit_override: None,
            reason: Some("runtime_window_complete_stale_publication_truth"),
        };
        let first_contract = discovery
            .deepen_persisted_stream_priority_recovery_contract_for_state(
                &replay_state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                base_contract,
            );
        assert_eq!(
            first_contract.reason,
            Some("deep_replay_sol_leg_large_processed_backlog")
        );
        assert!(
            first_contract
                .replay_sol_leg_phase_page_limit_override
                .is_some_and(|limit| limit > replay_state.replay_pages_processed),
            "the first reduced live-shaped sol-leg contract should already widen beyond the processed prefix, but still remain tied to historical processed pages rather than the unresolved suffix"
        );
        assert!(
            sol_leg_rows
                > replay_state.replay_rows_processed.saturating_add(
                    config.max_fetch_swaps_per_cycle
                        * first_contract
                            .replay_sol_leg_phase_page_limit_override
                            .expect("sol_leg limit")
                ),
            "the reduced live-shaped sol-leg fixture must still leave more than one processed-prefix worth of source after the first deep contract so the downstream blocker is deterministic locally"
        );

        let first_advance = discovery
            .advance_persisted_stream_replay_optimized_with_wallet_stats_phase_page_limit(
                &runtime_store,
                &mut replay_state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                first_contract.replay_wallet_stats_phase_page_limit_override,
                first_contract.replay_sol_leg_phase_page_limit_override,
                false,
                Instant::now() + StdDuration::from_secs(30),
            )?;
        assert!(!first_advance.source_exhausted);
        assert!(
            matches!(
                first_advance.budget_exhausted_reason,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget)
            ) || matches!(
                first_advance.budget_exhausted_reason,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget)
            ),
            "old behavior must stay on the sol_leg blocker once the processed-prefix contract runs out before source exhaustion"
        );
        replay_state.replay_rows_processed = replay_state
            .replay_rows_processed
            .saturating_add(first_advance.rows_processed);
        replay_state.replay_pages_processed = replay_state
            .replay_pages_processed
            .saturating_add(first_advance.pages_processed);
        replay_state.phase_cursor = first_advance.phase_cursor;
        replay_state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed =
            first_advance.replay_sol_leg_pages_processed;
        replay_state
            .payload
            .replay_sol_leg_last_partial_cycle_rows_processed =
            first_advance.replay_sol_leg_rows_processed;
        replay_state
            .payload
            .replay_sol_leg_last_partial_cycle_elapsed_ms =
            first_advance.replay_sol_leg_elapsed_ms.max(1);

        let second_contract = discovery
            .deepen_persisted_stream_priority_recovery_contract_for_state(
                &replay_state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                base_contract,
            );
        assert_eq!(
            second_contract.reason,
            Some("deep_replay_sol_leg_open_frontier_backlog")
        );
        assert!(
            second_contract
                .replay_sol_leg_phase_page_limit_override
                .zip(first_contract.replay_sol_leg_phase_page_limit_override)
                .is_some_and(|(second_limit, first_limit)| second_limit > first_limit),
            "once a real saturated partial sol-leg cycle is persisted, the next checkpoint-specific contract must widen from the remaining frontier instead of pretending the processed prefix is the whole problem"
        );
        Ok(())
    }

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
