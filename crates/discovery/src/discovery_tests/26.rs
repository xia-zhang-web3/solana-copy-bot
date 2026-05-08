    #[test]
    fn persisted_stream_replay_sol_leg_partial_hint_persists_exact_suffix_not_mixed_cycle_volume_stage1(
    ) -> Result<()> {
        let now = DateTime::parse_from_rfc3339("2026-04-04T10:08:48Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let window_start = metrics_window_start;
        let horizon_end = window_start + Duration::days(config.scoring_window_days as i64);
        let mut replay_state = PersistedStreamRebuildState {
            phase: DiscoveryPersistedRebuildPhase::Replay,
            window_start,
            horizon_end,
            metrics_window_start,
            phase_cursor: Some(DiscoveryRuntimeCursor {
                ts_utc: now - Duration::minutes(1),
                slot: 123,
                signature: "stage1-mixed-sol-leg-suffix".to_string(),
            }),
            payload: PersistedStreamRebuildPayload {
                replay_mode: ReplayMode::WalletStatsThenSolLeg,
                replay_wallet_stats_complete: true,
                replay_wallet_stats_milestone_reached: true,
                replay_wallet_stats_budget_floor_wallets: 662_481,
                replay_wallet_stats_last_partial_cycle_pages_processed: 27,
                replay_wallet_stats_last_partial_cycle_wallets_processed: 24_300,
                replay_wallet_stats_last_partial_cycle_elapsed_ms: 60_002,
                by_wallet: HashMap::from_iter([(
                    "wallet_sol_leg".to_string(),
                    WalletAccumulator::default(),
                )]),
                ..PersistedStreamRebuildPayload::default()
            },
            prepass_rows_processed: 0,
            prepass_pages_processed: 0,
            replay_rows_processed: 5,
            replay_pages_processed: 1,
            chunks_completed: 0,
            started_at: now - Duration::minutes(5),
            updated_at: now - Duration::seconds(1),
        };
        let mut mixed_wallet_stats_progress = ReplayWalletStatsDayCountSourceProgress::default();
        mixed_wallet_stats_progress.observe_page(
            Some(ObservedWalletActivityDayCountSource::WalletActivityDays),
            24_300,
        );
        mixed_wallet_stats_progress.observe_page(
            Some(ObservedWalletActivityDayCountSource::WalletActivityDays),
            24_300,
        );
        mixed_wallet_stats_progress.observe_page(
            Some(ObservedWalletActivityDayCountSource::WalletActivityDays),
            24_300,
        );
        mixed_wallet_stats_progress.observe_page(
            Some(ObservedWalletActivityDayCountSource::ObservedSwapsFallback),
            5_400,
        );
        let whole_cycle_pages_processed = mixed_wallet_stats_progress
            .total_pages_processed()
            .saturating_add(5);
        let whole_cycle_rows_processed = mixed_wallet_stats_progress
            .total_wallets_processed()
            .saturating_add(500);

        DiscoveryService::persist_partial_replay_frontier_hints_after_cycle(
            &mut replay_state,
            config.max_fetch_swaps_per_cycle,
            true,
            mixed_wallet_stats_progress,
            60_002,
            Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            Some(25),
            5,
            500,
            7_250,
        );

        assert!(
            whole_cycle_pages_processed > replay_state.payload.replay_sol_leg_last_partial_cycle_pages_processed,
            "the deterministic repro must stay mixed: whole-cycle pages include wallet_stats plus the later sol_leg suffix"
        );
        assert!(
            whole_cycle_rows_processed > replay_state.payload.replay_sol_leg_last_partial_cycle_rows_processed,
            "the deterministic repro must stay mixed: whole-cycle rows include wallet_stats plus the later sol_leg suffix"
        );
        assert_eq!(
            replay_state.payload.replay_sol_leg_last_partial_cycle_pages_processed,
            5,
            "whole-cycle pages must now be strictly larger than persisted sol_leg suffix pages on a mixed wallet_stats -> sol_leg cycle"
        );
        assert!(
            replay_state.payload.replay_sol_leg_last_partial_cycle_rows_processed == 500,
            "persisted sol_leg rows must come only from the exact sol_leg suffix of the mixed cycle"
        );
        assert_eq!(
            replay_state.payload.replay_sol_leg_last_partial_cycle_elapsed_ms,
            7_250,
            "persisted sol_leg elapsed time must come from the exact sol_leg suffix, not the whole mixed cycle"
        );
        assert_eq!(
            replay_state.payload.replay_wallet_stats_last_partial_cycle_pages_processed,
            27,
            "once the mixed cycle has already crossed wallet_stats and is blocking in sol_leg, the downstream sol_leg hint persistence must not overwrite wallet_stats-specific hint memory"
        );
        assert_eq!(
            replay_state.payload.replay_sol_leg_retained_contract_floor_pages,
            0,
            "a partial sol_leg suffix that ended on time budget must not retain the whole lane as already-proven insufficient, because the cycle did not demonstrate that the full 25-page contract was exhausted"
        );
        assert_eq!(
            replay_state.replay_pages_processed,
            1,
            "persisted sol_leg page hints must equal exact sol_leg replay suffix work, not the full mixed cycle volume"
        );
        assert_eq!(
            replay_state.replay_rows_processed,
            5,
            "persisting exact sol_leg suffix memory must not mutate cumulative replay progress counters"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_priority_recovery_contract_does_not_reuse_sol_leg_open_frontier_for_candidate_activity_backfill_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-candidate-backfill-does-not-reuse-sol-leg-frontier.db");
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
        let (mut replay_state, _metrics_window_start, _sol_leg_rows) =
            seed_stage1_partial_sol_leg_replay_checkpoint_fixture(
                &runtime_store,
                &discovery,
                &config,
                now,
                401,
                54,
            )?;
        DiscoveryService::prepare_persisted_stream_replay_candidate_activity_backfill(
            &mut replay_state,
        );
        replay_state
            .payload
            .replay_sol_leg_last_partial_cycle_pages_processed = 54;
        replay_state
            .payload
            .replay_sol_leg_last_partial_cycle_rows_processed = 540;
        replay_state
            .payload
            .replay_sol_leg_last_partial_cycle_elapsed_ms = 243_000;

        let contract = discovery.deepen_persisted_stream_priority_recovery_contract_for_state(
            &replay_state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            PersistedStreamPriorityRecoveryContract {
                time_budget: StdDuration::from_secs(60),
                collect_buy_mints_phase_page_limit_override: Some(160),
                replay_wallet_stats_phase_page_limit_override: Some(92),
                replay_sol_leg_phase_page_limit_override: None,
                reason: Some("runtime_window_complete_stale_publication_truth"),
            },
        );
        assert_ne!(
            contract.reason,
            Some("deep_replay_candidate_activity_backfill_open_frontier_backlog"),
            "candidate activity backfill must not reuse persisted sol_leg frontier hints as if they were exact candidate-backfill suffix memory"
        );
        Ok(())
    }

    #[test]
    fn replay_candidate_activity_backfill_restores_exact_activity_summary_for_candidate_wallets_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-replay-candidate-activity-backfill.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-03T19:03:26Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 100;
        config.max_fetch_pages_per_cycle = 10;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let token = "TokenStage1ReplayBackfill11111111111111111111111";

        let candidate_swaps = [
            swap(
                "wallet_candidate_backfill",
                "candidate-backfill-non-sol-early",
                window_start + Duration::hours(1),
                "TokenOtherA11111111111111111111111111111111",
                "TokenOtherB11111111111111111111111111111111",
                10.0,
                20.0,
            ),
            swap(
                "wallet_candidate_backfill",
                "candidate-backfill-buy",
                window_start + Duration::days(1) + Duration::hours(2),
                SOL_MINT,
                token,
                1.0,
                100.0,
            ),
            swap(
                "wallet_candidate_backfill",
                "candidate-backfill-sell",
                window_start + Duration::days(1) + Duration::hours(3),
                token,
                SOL_MINT,
                100.0,
                1.3,
            ),
            swap(
                "wallet_candidate_backfill",
                "candidate-backfill-non-sol-late",
                window_start + Duration::days(3) + Duration::hours(4),
                "TokenOtherB11111111111111111111111111111111",
                "TokenOtherC11111111111111111111111111111111",
                30.0,
                40.0,
            ),
        ];
        for swap in candidate_swaps {
            store.insert_observed_swap(&swap)?;
        }
        store.insert_observed_swap(&swap(
            "wallet_irrelevant_backfill",
            "irrelevant-backfill-buy",
            window_start + Duration::days(2),
            SOL_MINT,
            "TokenIrrelevant1111111111111111111111111111",
            0.2,
            20.0,
        ))?;

        let mut replay_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        replay_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        replay_state.horizon_end = now;
        replay_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        replay_state.payload.replay_wallet_stats_complete = true;
        replay_state
            .payload
            .replay_candidate_activity_backfill_required = true;
        replay_state
            .payload
            .replay_candidate_activity_backfill_pending = true;
        let mut acc = WalletAccumulator::default();
        acc.observe_buy_streaming(
            token,
            100.0,
            1.0,
            window_start + Duration::days(1) + Duration::hours(2),
            BuyTradability::Tradable,
            false,
        );
        acc.observe_sell(
            token,
            100.0,
            1.3,
            window_start + Duration::days(1) + Duration::hours(3),
        );
        replay_state
            .payload
            .by_wallet
            .insert("wallet_candidate_backfill".to_string(), acc);

        let advance = discovery
            .advance_persisted_stream_replay_candidate_wallet_activity_backfill(
                &store,
                &mut replay_state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                Instant::now() + StdDuration::from_secs(30),
            )?;
        assert!(advance.source_exhausted);
        assert!(
            !replay_state
                .payload
                .replay_candidate_activity_backfill_pending
        );
        let acc = replay_state
            .payload
            .by_wallet
            .get("wallet_candidate_backfill")
            .expect("candidate wallet must remain buffered");
        assert_eq!(
            acc.first_seen,
            Some(window_start + Duration::hours(1)),
            "candidate activity backfill must restore the earliest all-swap first_seen, not just the SOL-leg replay prefix"
        );
        assert_eq!(
            acc.last_seen,
            Some(window_start + Duration::days(3) + Duration::hours(4)),
            "candidate activity backfill must restore the latest all-swap last_seen before publish"
        );
        assert_eq!(
            acc.trades, 4,
            "candidate activity backfill must restore the exact all-swap trade count for the publishable wallet set"
        );
        assert_eq!(
            acc.exact_active_day_count,
            Some(3),
            "candidate activity backfill must restore the exact all-swap active-day count even when the backfill source no longer replays raw swap order into the incidental active_days set"
        );
        assert!(!acc.suspicious);
        Ok(())
    }
#[test]
    fn replay_candidate_activity_backfill_preserves_pending_on_wallet_id_page_interrupt_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-replay-candidate-activity-backfill-wallet-id-interrupt.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-11T12:15:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 900;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);

        let candidate_wallet_count = 20_000usize;
        let mut candidate_swaps = Vec::with_capacity(candidate_wallet_count);
        let mut by_wallet = HashMap::with_capacity(candidate_wallet_count);
        for idx in 0..candidate_wallet_count {
            let wallet_id = format!("wallet_candidate_interrupt_{idx:05}");
            let token = format!("TokenCandidateInterrupt{idx:05}11111111111111111111");
            let ts = window_start + Duration::seconds((idx % 86_400) as i64);
            candidate_swaps.push(swap(
                &wallet_id,
                &format!("candidate-wallet-id-interrupt-{idx:05}"),
                ts,
                SOL_MINT,
                &token,
                1.0,
                100.0,
            ));
            let mut acc = WalletAccumulator::default();
            acc.observe_buy_streaming(&token, 100.0, 1.0, ts, BuyTradability::Tradable, false);
            by_wallet.insert(wallet_id, acc);
        }
        store.insert_observed_swaps_batch_with_activity_days(&candidate_swaps)?;

        let mut replay_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        replay_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        replay_state.horizon_end = now;
        replay_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        replay_state.payload.replay_wallet_stats_complete = true;
        replay_state
            .payload
            .replay_candidate_activity_backfill_required = true;
        replay_state.payload.by_wallet = by_wallet;
        DiscoveryService::prepare_persisted_stream_replay_candidate_activity_backfill(
            &mut replay_state,
        );

        let advance = discovery
            .advance_persisted_stream_replay_candidate_wallet_activity_backfill(
                &store,
                &mut replay_state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                Instant::now() + StdDuration::from_millis(10),
            )?;

        assert_eq!(
            advance.budget_exhausted_reason,
            Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            "an interrupted wallet-id page must propagate as time-budget exhaustion instead of silently looking source-exhausted"
        );
        assert!(
            !advance.source_exhausted,
            "candidate backfill must not mark the exact-wallet source exhausted when the wallet-id page was interrupted"
        );
        assert!(
            replay_state
                .payload
                .replay_candidate_activity_backfill_pending,
            "candidate backfill pending must stay armed on wallet-id interrupt so the next cycle resumes instead of falsely completing"
        );
        assert_eq!(
            replay_state
                .payload
                .replay_candidate_activity_backfill_wallet_cursor,
            None,
            "an interrupted wallet-id page must not advance the exact candidate-wallet cursor"
        );
        assert_eq!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(
                &replay_state
            ),
            "replay_candidate_activity_backfill_incomplete",
            "wallet-id interrupt must keep the lineage on the real backfill blocker instead of clearing it as if source exhaustion had completed"
        );
        Ok(())
    }

    #[test]
    fn repair_runtime_window_complete_delegated_replay_sol_leg_checkpoint_uses_deeper_priority_contract_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-publication-repair-live-like-sol-leg.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-02T18:15:04Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 100;
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
                3,
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
        assert!(repair.publication_truth_refresh_delegated_to_runtime_cycle);
        assert_eq!(repair.publication_truth_refresh_phase, Some("replay"));
        assert_eq!(
            repair.publication_truth_refresh_replay_subphase,
            Some("sol_leg")
        );
        assert!(repair.publication_truth_refresh_replay_wallet_stats_complete);
        assert_eq!(
            repair.publication_truth_refresh_publishable_checkpoint_blocker,
            Some("replay_sol_leg_incomplete")
        );
        assert_eq!(
            repair.publication_truth_refresh_priority_recovery_contract_reason,
            Some("deep_replay_sol_leg_incomplete")
        );
        assert_eq!(
            repair.publication_truth_refresh_effective_time_budget_ms,
            Some(180_000)
        );
        assert_eq!(
            repair.publication_truth_refresh_replay_sol_leg_phase_page_limit,
            Some(60)
        );
        assert_eq!(repair.publication_truth_refresh_replay_rows_processed, 300);
        assert_eq!(repair.publication_truth_refresh_replay_pages_processed, 3);
        assert!(
            repair.publication_truth_refresh_observed_swaps_loaded
                > replay_state.payload.replay_wallet_stats_rows_processed,
            "delegated repair telemetry should now expose total optimized replay progress instead of freezing observed_swaps_loaded on the completed wallet-stats count"
        );
        let rebuild_after_repair = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert_eq!(
            rebuild_after_repair.replay_rows_processed,
            replay_state.replay_rows_processed,
            "delegated repair must still leave the partial sol-leg checkpoint untouched for the owning run_cycle"
        );
        Ok(())
    }
