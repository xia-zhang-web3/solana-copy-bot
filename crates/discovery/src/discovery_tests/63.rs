#[test]
    fn deep_replay_wallet_stats_priority_recovery_contract_moves_resumed_checkpoint_to_sol_leg_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-priority-recovery-deep-replay-wallet-stats.db");
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
        let noise_wallets = 60_000usize;
        let (window_start, metrics_window_start) =
            seed_stage1_live_like_wallet_stats_backlog_fixture(
                &runtime_store,
                &config,
                now,
                noise_wallets,
                "stage1-deep-replay-priority",
            )?;

        let mut partial_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        partial_state.phase = DiscoveryPersistedRebuildPhase::Replay;
        partial_state.horizon_end = now;
        partial_state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        partial_state.payload.unique_buy_mints =
            runtime_store.load_observed_buy_mints_in_window(window_start, now)?;
        partial_state.payload.token_quality_progress.next_mint_index =
            partial_state.payload.unique_buy_mints.len();

        let seeded_partial = discovery.advance_persisted_stream_replay_wallet_stats(
            &runtime_store,
            &mut partial_state,
            config.max_fetch_swaps_per_cycle,
            8,
            Instant::now() + StdDuration::from_secs(30),
        )?;
        partial_state.payload.replay_wallet_stats_rows_processed = partial_state
            .payload
            .replay_wallet_stats_rows_processed
            .saturating_add(seeded_partial.replay_wallet_stats_rows_processed);
        partial_state.payload.replay_wallet_stats_pages_processed = partial_state
            .payload
            .replay_wallet_stats_pages_processed
            .saturating_add(seeded_partial.replay_wallet_stats_pages_processed);
        partial_state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .merge(seeded_partial.replay_wallet_stats_day_count_source_progress);
        assert!(!seeded_partial.source_exhausted);
        assert!(partial_state
            .payload
            .replay_wallet_stats_wallet_cursor
            .is_some());
        assert!(!partial_state.payload.by_wallet.is_empty());

        let mut baseline_state = partial_state.clone();
        let baseline_phase_page_limit = discovery.replay_wallet_stats_repair_phase_page_limit(
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            StdDuration::from_secs(60),
        );
        let baseline_advance = discovery
            .advance_persisted_stream_replay_optimized_with_wallet_stats_phase_page_limit(
                &runtime_store,
                &mut baseline_state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                Some(baseline_phase_page_limit),
                None,
                false,
                Instant::now() + StdDuration::from_secs(30),
            )?;
        assert!(
            !baseline_state.payload.replay_wallet_stats_complete,
            "the existing 60s priority contract should still leave this deep replay checkpoint inside wallet_stats"
        );
        assert!(
            baseline_state.payload.replay_wallet_stats_wallet_cursor.is_some(),
            "old behavior should keep the persisted wallet cursor live because wallet_stats was not fully drained"
        );

        let mut priority_state = partial_state.clone();
        let priority_phase_page_limit = discovery.replay_wallet_stats_repair_phase_page_limit(
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            StdDuration::from_secs(180),
        );
        let priority_advance = discovery
            .advance_persisted_stream_replay_optimized_with_wallet_stats_phase_page_limit(
                &runtime_store,
                &mut priority_state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                Some(priority_phase_page_limit),
                None,
                true,
                Instant::now() + StdDuration::from_secs(30),
            )?;
        assert!(
            priority_state.payload.replay_wallet_stats_complete,
            "the deeper recovery contract should drain wallet_stats and hand the checkpoint forward to the next replay milestone"
        );
        assert_eq!(
            priority_state.payload.replay_wallet_stats_wallet_cursor,
            None
        );
        assert!(
            priority_advance.replay_wallet_stats_pages_processed
                > baseline_advance.replay_wallet_stats_pages_processed,
            "the deeper priority contract should materially advance wallet_stats beyond the old 60s replay ceiling"
        );
        assert!(
            matches!(
                priority_advance.budget_exhausted_reason,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget)
            ) || priority_advance.source_exhausted,
            "after draining wallet_stats, the deeper checkpoint should either hand off into the next replay step or fully exhaust the replay source"
        );
        Ok(())
    }

    #[test]
    fn priority_recovery_open_frontier_wallet_stats_hands_off_to_sol_leg_candidate_backfill_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-priority-recovery-wallet-stats-handoff-candidate-backfill.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-03T19:03:26Z")
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
                "stage1-wallet-stats-handoff",
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

        let seeded_partial = discovery.advance_persisted_stream_replay_wallet_stats(
            &runtime_store,
            &mut replay_state,
            config.max_fetch_swaps_per_cycle,
            8,
            Instant::now() + StdDuration::from_secs(30),
        )?;
        replay_state.payload.replay_wallet_stats_rows_processed = replay_state
            .payload
            .replay_wallet_stats_rows_processed
            .saturating_add(seeded_partial.replay_wallet_stats_rows_processed);
        replay_state.payload.replay_wallet_stats_pages_processed = replay_state
            .payload
            .replay_wallet_stats_pages_processed
            .saturating_add(seeded_partial.replay_wallet_stats_pages_processed);
        replay_state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .merge(seeded_partial.replay_wallet_stats_day_count_source_progress);
        assert!(!seeded_partial.source_exhausted);
        assert!(replay_state
            .payload
            .replay_wallet_stats_wallet_cursor
            .is_some());

        let baseline_phase_page_limit = discovery.replay_wallet_stats_repair_phase_page_limit(
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            StdDuration::from_secs(60),
        );
        let advance = discovery
            .advance_persisted_stream_replay_optimized_with_wallet_stats_phase_page_limit(
                &runtime_store,
                &mut replay_state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                Some(baseline_phase_page_limit),
                None,
                true,
                Instant::now() + StdDuration::from_secs(30),
            )?;
        assert!(
            replay_state.payload.replay_wallet_stats_complete,
            "priority recovery should move the replay checkpoint beyond the wallet_stats blocker once the live-shaped open frontier proves that exhaustive all-wallet source draining is the bottleneck"
        );
        assert_eq!(
            replay_state.payload.replay_wallet_stats_wallet_cursor,
            None,
            "after the handoff, the all-wallet wallet_stats cursor must clear instead of keeping the checkpoint pinned on the same blocker"
        );
        assert_ne!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(
                &replay_state
            ),
            "replay_wallet_stats_incomplete",
            "the checkpoint-specific blocker should advance beyond wallet_stats once the candidate-backfill handoff is armed"
        );
        assert!(
            advance.replay_wallet_stats_pages_processed > 0,
            "the handoff should be based on real wallet-stats frontier work, not on a zero-progress shortcut"
        );
        Ok(())
    }
