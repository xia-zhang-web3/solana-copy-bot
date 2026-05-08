    #[test]
    fn persisted_stream_priority_recovery_contract_deepens_resumed_replay_sol_leg_checkpoint_stage1(
    ) {
        let now = DateTime::parse_from_rfc3339("2026-04-02T18:15:01Z")
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
        state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(3),
            slot: 42,
            signature: "stage1-sol-leg-cursor".to_string(),
        });
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.replay_wallet_stats_complete = true;
        state.payload.replay_wallet_stats_rows_processed = 15_740_016;
        state.replay_rows_processed = 900_000;
        state.replay_pages_processed = 45;
        state.payload.unique_buy_mints =
            vec!["TokenReplayDeep11111111111111111111111111".to_string()];
        state.payload.token_quality_progress.next_mint_index = state.payload.unique_buy_mints.len();

        let base_contract = PersistedStreamPriorityRecoveryContract {
            time_budget: StdDuration::from_secs(60),
            collect_buy_mints_phase_page_limit_override: Some(160),
            replay_wallet_stats_phase_page_limit_override: Some(92),
            replay_sol_leg_phase_page_limit_override: None,
            reason: Some("runtime_window_complete_stale_publication_truth"),
        };
        let deep_contract = discovery.deepen_persisted_stream_priority_recovery_contract_for_state(
            &state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            base_contract,
        );

        assert_eq!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(&state),
            "replay_sol_leg_incomplete"
        );
        assert_eq!(deep_contract.time_budget, StdDuration::from_secs(204));
        assert_eq!(
            deep_contract.replay_sol_leg_phase_page_limit_override,
            Some(70)
        );
        assert!(
            deep_contract
                .replay_wallet_stats_phase_page_limit_override
                .is_some_and(|limit| limit > 92),
            "once the same replay checkpoint is still stuck in sol_leg, the widened contract should carry the larger bounded budget through the replay lane instead of leaving sol_leg on the old bounded-page ceiling"
        );
        assert_eq!(
            deep_contract.reason,
            Some("deep_replay_sol_leg_large_processed_backlog")
        );
    }

    #[test]
    fn persisted_stream_priority_recovery_contract_uses_carried_sol_leg_budget_floor_stage1() {
        let now = DateTime::parse_from_rfc3339("2026-04-10T12:48:09Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 100;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 25_000;
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
            signature: "stage1-carried-sol-leg-budget-floor".to_string(),
        });
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.replay_wallet_stats_complete = true;
        state.payload.replay_wallet_stats_milestone_reached = true;
        state.payload.replay_candidate_activity_backfill_required = true;
        state.payload.unique_buy_mints =
            vec!["TokenReplaySolLegBudgetFloor11111111111111111".to_string()];
        state.payload.buy_mint_counts = state
            .payload
            .unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1u32))
            .collect();
        state.payload.by_wallet.insert(
            "wallet_replay_sol_leg_budget_floor".to_string(),
            WalletAccumulator::default(),
        );
        state.replay_rows_processed = 2_000;
        state.replay_pages_processed = 20;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed = 20;
        state
            .payload
            .replay_sol_leg_last_partial_cycle_rows_processed = 2_000;
        state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms = 100_000;

        let base_contract = PersistedStreamPriorityRecoveryContract {
            time_budget: StdDuration::from_secs(60),
            collect_buy_mints_phase_page_limit_override: Some(160),
            replay_wallet_stats_phase_page_limit_override: Some(92),
            replay_sol_leg_phase_page_limit_override: Some(300),
            reason: Some("runtime_window_complete_stale_publication_truth"),
        };

        let old_like_contract = discovery
            .deepen_persisted_stream_priority_recovery_contract_for_state_at(
                &state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                base_contract,
                Some(now),
            );
        assert_eq!(
            old_like_contract.replay_sol_leg_phase_page_limit_override,
            Some(60),
            "without a carried sol_leg budgeting floor, the resumed stale-target replay contract relearns only from its current 20-page processed prefix plus 20-page open suffix"
        );

        state.payload.replay_sol_leg_budget_floor_pages = 80;
        let carried_floor_contract = discovery
            .deepen_persisted_stream_priority_recovery_contract_for_state_at(
                &state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                base_contract,
                Some(now),
            );

        assert!(
            carried_floor_contract
                .replay_sol_leg_phase_page_limit_override
                .zip(old_like_contract.replay_sol_leg_phase_page_limit_override)
                .is_some_and(|(new_limit, old_limit)| new_limit >= 145 && new_limit > old_limit),
            "once the same resumed stale-target replay carries an 80-page sol_leg budgeting floor from the prior target lineage, the widened contract should materially exceed the suffix-only 60-page lane instead of relearning from the much smaller local prefix"
        );
        assert!(
            carried_floor_contract.time_budget > old_like_contract.time_budget,
            "the carried sol_leg budgeting floor must widen the exact stale-target replay lane, not just rename the same contract"
        );
        assert!(
            matches!(
                carried_floor_contract.reason,
                Some("deep_replay_sol_leg_open_frontier_backlog")
                    | Some("deep_replay_sol_leg_publishable_horizon_cap")
                    | Some("deep_replay_sol_leg_publishable_horizon_backlog")
            ),
            "the carried-floor lane should still be recognized as a deep sol_leg recovery contract even if the stale-target publishable horizon trims its final time budget"
        );
    }

    #[test]
    fn runtime_window_complete_live_like_wallet_stats_replay_hits_baseline_page_budget_before_truth_refresh_can_publish(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-publication-repair-live-like-replay-baseline-page-budget.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-01T19:52:20Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let token = "TokenStage1RepairReplayLive11111111111111111";

        for idx in 0..2 {
            let buy_ts = window_start + Duration::minutes((idx * 10) as i64);
            runtime_store.insert_observed_swap(&swap(
                "wallet_live_like_top",
                &format!("stage1-live-like-top-buy-short-{idx}"),
                buy_ts,
                SOL_MINT,
                token,
                1.0,
                100.0,
            ))?;
            runtime_store.insert_observed_swap(&swap(
                "wallet_live_like_top",
                &format!("stage1-live-like-top-sell-short-{idx}"),
                buy_ts + Duration::minutes(5),
                token,
                SOL_MINT,
                100.0,
                1.3,
            ))?;
        }

        let noise_wallets = 60_000usize;
        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..noise_wallets {
            let ts = now - Duration::seconds((noise_wallets.saturating_sub(idx)) as i64);
            let swap = swap(
                &format!("wallet_live_like_noise_short_{idx:05}"),
                &format!("stage1-live-like-noise-short-{idx:05}"),
                ts,
                SOL_MINT,
                token,
                0.2,
                20.0,
            );
            latest_cursor = Some(DiscoveryRuntimeCursor {
                ts_utc: swap.ts_utc,
                slot: swap.slot,
                signature: swap.signature.clone(),
            });
            runtime_store.insert_observed_swap(&swap)?;
        }
        runtime_store.upsert_discovery_runtime_cursor(
            &latest_cursor.expect("latest cursor should be present"),
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

        let PersistedStreamRebuildAdvanceOutcome::InProgress { telemetry } = discovery
            .advance_persisted_stream_rebuild(
                &runtime_store,
                window_start,
                metrics_window_start,
                now,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                StdDuration::from_secs(60),
            )?
        else {
            panic!("baseline live-like rebuild should stay in progress under the old replay wallet-stats page ceiling");
        };
        assert_eq!(telemetry.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert_eq!(
            telemetry.replay_subphase,
            Some("wallet_stats"),
            "the baseline live fetch contract should reproduce the exact wallet_stats replay bottleneck before the repair-specific page-budget widening"
        );
        assert!(
            !telemetry.replay_wallet_stats_complete,
            "the baseline rebuild should stay in wallet_stats when the old fetch-width page ceiling runs out before publication truth becomes publishable"
        );
        assert!(
            matches!(
                telemetry.budget_exhausted_reason,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget)
            ),
            "the remaining live blocker is specifically page_budget, not time_budget"
        );
        assert!(
            telemetry.wallets_buffered < noise_wallets,
            "the baseline rebuild should leave the publishable-universe buffer materially behind the live-like wallet frontier under the old 23-page ceiling"
        );
        Ok(())
    }
