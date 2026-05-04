    #[test]
    fn persisted_stream_replay_bucket_roll_aged_out_checkpoint_carries_forward_exact_membership_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-replay-aged-out-carry-forward.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-17T12:00:59Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let aged_out_now =
            source_now + Duration::seconds(config.metric_snapshot_interval_seconds as i64 + 61);
        let (window_start, metrics_window_start, _, _) =
            seed_stage1_replay_noise_fixture(&store, &config, source_now, 3, 12)?;
        let target_window_start =
            aged_out_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_metrics_window_start = metrics_window_start_for_test(&config, aged_out_now);
        let unique_buy_mints = store.load_observed_buy_mints_in_window(window_start, source_now)?;

        let mut state = discovery.start_persisted_stream_rebuild_state(
            window_start,
            metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.horizon_end = source_now;
        state.prepass_rows_processed = 32_645;
        state.prepass_pages_processed = 12;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        state.payload.unique_buy_mints = unique_buy_mints.clone();
        state.payload.buy_mint_counts = unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1u32))
            .collect();
        state.payload.token_quality_progress.next_mint_index = unique_buy_mints.len();
        state.payload.token_quality_cache =
            fresh_token_quality_cache_for_mints_for_test(&unique_buy_mints, source_now);
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.replay_wallet_stats_complete = true;
        state.payload.replay_wallet_stats_rows_processed = 15_740_016;
        state.payload.replay_wallet_stats_pages_processed = 733;
        state.payload.replay_wallet_stats_budget_floor_wallets = 703_826;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_pages_processed = 406;
        state
            .payload
            .replay_wallet_stats_last_partial_cycle_elapsed_ms = 817_313;
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .fast_path_wallets_processed = 703_826;
        state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: window_start + Duration::minutes(10),
            slot: 42,
            signature: "stage1-aged-out-replay-sol-leg-cursor".to_string(),
        });
        state.replay_rows_processed = 900_000;
        state.replay_pages_processed = 45;
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
            aged_out_now,
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
        );
        assert_eq!(resumed.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert_eq!(resumed.window_start, window_start);
        assert_eq!(resumed.metrics_window_start, metrics_window_start);
        assert_eq!(resumed.horizon_end, source_now);
        assert_eq!(resumed.prepass_rows_processed, state.prepass_rows_processed);
        assert_eq!(
            resumed.prepass_pages_processed,
            state.prepass_pages_processed
        );
        assert_eq!(resumed.phase_cursor, state.phase_cursor);
        assert_eq!(resumed.payload.unique_buy_mints, unique_buy_mints);
        assert_eq!(
            resumed.payload.buy_mint_counts,
            state.payload.buy_mint_counts
        );
        assert_eq!(
            resumed.payload.token_quality_cache.len(),
            unique_buy_mints.len(),
            "once an aged-out replay checkpoint already carries an exact downstream SOL-leg frontier, restart should keep that frozen lineage alive together with the exact mint-quality cache instead of rebasing the same lineage onto a newer target"
        );
        assert_eq!(
            resumed.payload.token_quality_progress.next_mint_index,
            state.payload.token_quality_progress.next_mint_index
        );
        assert!(resumed.payload.replay_wallet_stats_complete);
        assert!(
            resumed.payload.replay_wallet_stats_milestone_reached,
            "when an aged-out replay checkpoint has already crossed wallet_stats and built a downstream SOL-leg frontier, restart must keep the durable milestone on the frozen lineage instead of rebasing it onto a new target"
        );
        assert!(
            !resumed.payload.replay_sol_leg_reentry_pending,
            "the frozen downstream replay lineage should stay inside active replay; the pre-token-quality reentry marker is only for newer target carry-forward, not for an already active frozen SOL-leg frontier"
        );
        assert_eq!(resumed.replay_rows_processed, state.replay_rows_processed);
        assert_eq!(resumed.replay_pages_processed, state.replay_pages_processed);
        assert_eq!(
            resumed.payload.replay_wallet_stats_budget_floor_wallets,
            703_826,
            "pinning the frozen downstream replay target must still preserve the carried wallet frontier budgeting proof"
        );
        assert_eq!(
            resumed
                .payload
                .replay_wallet_stats_last_partial_cycle_pages_processed,
            406
        );
        assert_eq!(
            resumed
                .payload
                .replay_wallet_stats_last_partial_cycle_elapsed_ms,
            817_313
        );
        assert!(
            !resumed.payload.by_wallet.is_empty(),
            "pinning the frozen downstream replay target must keep the exact SOL-leg accumulators alive; clearing them would reopen the same moving-goalpost blocker"
        );
        Ok(())
    }

    #[test]
    fn extracted_prod_morning_wallet_stats_checkpoint_reproduces_persisted_fallback_shape_stage1(
    ) -> Result<()> {
        let (state, runtime_mode, publication_reason, published_wallet_ids, publication_updated_at) =
            load_extracted_prod_morning_fallback_state()?;

        assert_eq!(state.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert_eq!(
            state.window_start,
            DateTime::parse_from_rfc3339("2026-03-30T07:37:54.180093001+00:00")
                .expect("valid timestamp")
                .with_timezone(&Utc)
        );
        assert_eq!(
            state.horizon_end,
            DateTime::parse_from_rfc3339("2026-04-04T07:37:54.180093001+00:00")
                .expect("valid timestamp")
                .with_timezone(&Utc)
        );
        assert_eq!(
            state.metrics_window_start,
            DateTime::parse_from_rfc3339("2026-03-30T07:00:00+00:00")
                .expect("valid timestamp")
                .with_timezone(&Utc)
        );
        assert_eq!(state.prepass_rows_processed, 77_011);
        assert_eq!(state.prepass_pages_processed, 1_538);
        assert_eq!(state.replay_rows_processed, 0);
        assert_eq!(state.replay_pages_processed, 0);
        assert_eq!(state.chunks_completed, 166);
        assert_eq!(
            state.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::FreshScan
        );
        assert!(state.payload.collect_buy_mints_prepass_complete);
        assert_eq!(state.payload.replay_mode, ReplayMode::WalletStatsThenSolLeg);
        assert!(!state.payload.replay_wallet_stats_complete);
        assert!(!state.payload.replay_candidate_activity_backfill_pending);
        assert!(!state.payload.replay_candidate_activity_backfill_required);
        assert!(!state.payload.replay_wallet_stats_milestone_reached);
        assert_eq!(state.payload.replay_wallet_stats_rows_processed, 378_138);
        assert_eq!(state.payload.replay_wallet_stats_pages_processed, 28);
        assert_eq!(
            state.payload.replay_wallet_stats_wallet_cursor.as_deref(),
            Some("2eviCimfYrYCeAFueuswzN61TJyT2HsbYWVx6pcCLhsy")
        );
        assert_eq!(
            state.payload.replay_wallet_stats_budget_floor_wallets,
            662_481
        );
        assert_eq!(
            state
                .payload
                .replay_wallet_stats_last_partial_cycle_pages_processed,
            27
        );
        assert_eq!(
            state
                .payload
                .replay_wallet_stats_last_partial_cycle_wallets_processed,
            24_300
        );
        assert_eq!(
            state
                .payload
                .replay_wallet_stats_last_partial_cycle_elapsed_ms,
            60_002
        );
        assert_eq!(
            state
                .payload
                .replay_wallet_stats_day_count_source_progress
                .fast_path_pages_processed,
            27
        );
        assert_eq!(
            state
                .payload
                .replay_wallet_stats_day_count_source_progress
                .fast_path_wallets_processed,
            24_300
        );
        assert_eq!(state.payload.unique_buy_mints.len(), 22_089);
        assert_eq!(state.payload.by_wallet.len(), 24_300);
        assert_eq!(state.payload.token_quality_cache.len(), 22_089);
        assert!(state.payload.token_states.is_empty());
        assert_eq!(runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(
            publication_reason,
            "raw_window_incomplete_no_recent_published_universe"
        );
        assert!(published_wallet_ids.is_empty());
        assert_eq!(
            publication_updated_at,
            DateTime::parse_from_rfc3339("2026-04-04T07:40:56.665448258+00:00")
                .expect("valid timestamp")
                .with_timezone(&Utc)
        );
        assert_eq!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(&state),
            "replay_wallet_stats_incomplete",
            "the exact persisted morning prod checkpoint is already the degraded wallet_stats fallback shape on disk; the deeper post-wallet-stats milestone is not present in the payload anymore"
        );
        Ok(())
    }

    #[test]
    fn extracted_prod_morning_wallet_stats_checkpoint_repairs_back_to_sol_leg_when_durable_milestone_is_present_stage1(
    ) -> Result<()> {
        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let (mut state, _, _, _, _) = load_extracted_prod_morning_fallback_state()?;
        state.payload.replay_wallet_stats_milestone_reached = true;

        let resume_now = state.horizon_end;
        let changed = discovery
            .repair_restored_persisted_stream_state_for_resume(&mut state, resume_now, None);

        assert!(
            changed,
            "once the exact extracted morning fallback row carries a durable proof that wallet_stats was already crossed earlier in the same lineage, resume repair must stop treating it as a fresh all-wallet wallet_stats checkpoint"
        );
        assert_eq!(state.phase, DiscoveryPersistedRebuildPhase::Replay);
        assert!(state.payload.replay_wallet_stats_complete);
        assert!(state.payload.replay_wallet_stats_milestone_reached);
        assert!(state.payload.replay_candidate_activity_backfill_required);
        assert!(!state.payload.replay_candidate_activity_backfill_pending);
        assert_eq!(state.phase_cursor, None);
        assert_eq!(state.payload.replay_wallet_stats_wallet_cursor, None);
        assert!(
            state.payload.by_wallet.is_empty(),
            "repair should clear the degraded all-wallet activity buffer before re-entering SOL-leg, matching the normal wallet_stats -> sol_leg transition semantics"
        );
        assert_eq!(
            DiscoveryService::replay_subphase(
                state.phase,
                state.payload.replay_wallet_stats_complete,
                state.payload.replay_candidate_activity_backfill_pending,
            ),
            Some("sol_leg")
        );
        assert_eq!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(&state),
            "replay_sol_leg_incomplete"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_bucket_roll_legacy_checkpoint_without_budget_hint_still_carries_wallet_frontier_floor_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-replay-aged-out-legacy-hintless-carry-forward.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-17T12:00:59Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let aged_out_now =
            source_now + Duration::seconds(config.metric_snapshot_interval_seconds as i64 + 61);
        let (window_start, metrics_window_start, _, _) =
            seed_stage1_replay_noise_fixture(&store, &config, source_now, 3, 12)?;
        let target_window_start =
            aged_out_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_metrics_window_start = metrics_window_start_for_test(&config, aged_out_now);
        let unique_buy_mints = store.load_observed_buy_mints_in_window(window_start, source_now)?;

        let mut state = discovery.start_persisted_stream_rebuild_state(
            window_start,
            metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.horizon_end = source_now;
        state.prepass_rows_processed = 32_645;
        state.prepass_pages_processed = 12;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        state.payload.unique_buy_mints = unique_buy_mints.clone();
        state.payload.buy_mint_counts = unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1u32))
            .collect();
        state.payload.token_quality_progress.next_mint_index = unique_buy_mints.len();
        state.payload.token_quality_cache =
            fresh_token_quality_cache_for_mints_for_test(&unique_buy_mints, source_now);
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.replay_wallet_stats_complete = false;
        state.payload.replay_wallet_stats_rows_processed = 12_419_939;
        state.payload.replay_wallet_stats_pages_processed = 733;
        state.payload.replay_wallet_stats_wallet_cursor =
            Some("legacy-hintless-aged-out-wallet-stats-cursor".to_string());
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .fast_path_wallets_processed = 656_100;
        state.phase_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: window_start + Duration::minutes(10),
            slot: 42,
            signature: "stage1-aged-out-replay-wallet-stats-cursor".to_string(),
        });
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, source_now)?,
        )?;

        let (carried, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            target_window_start,
            target_metrics_window_start,
            aged_out_now,
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow
        );
        assert_eq!(
            carried.phase,
            DiscoveryPersistedRebuildPhase::CollectBuyMints
        );
        assert_eq!(
            carried.payload.replay_wallet_stats_budget_floor_wallets,
            656_100,
            "aged-out replay carry-forward must recover the wallet frontier floor from legacy checkpoints that predate explicit replay budgeting hints"
        );
        assert_eq!(
            carried
                .payload
                .replay_wallet_stats_last_partial_cycle_pages_processed,
            0
        );
        assert_eq!(
            carried
                .payload
                .replay_wallet_stats_last_partial_cycle_elapsed_ms,
            0
        );
        Ok(())
    }
