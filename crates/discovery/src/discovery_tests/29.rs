    #[test]
    fn persisted_stream_replay_resume_pre_row_budget_exhaustion_persists_staged_state_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-resume-exact-target-surface-pre-row-blocked.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-13T08:33:16Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 100;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

        let (mut replay_state, _metrics_window_start) =
            seed_stage1_dense_partial_sol_leg_replay_checkpoint_fixture(
                &runtime_store,
                &discovery,
                &config,
                now,
                4_000,
                20,
            )?;
        replay_state.payload.replay_wallet_stats_complete = true;
        replay_state.payload.replay_wallet_stats_milestone_reached = true;
        replay_state
            .payload
            .replay_candidate_activity_backfill_required = true;
        replay_state
            .payload
            .replay_candidate_activity_backfill_pending = false;
        replay_state.payload.replay_sol_leg_budget_floor_pages = replay_state
            .payload
            .replay_sol_leg_budget_floor_pages
            .max(20);
        DiscoveryService::compact_wallet_activity_summary_for_frozen_exact_target_checkpoint(
            &mut replay_state.payload,
        );
        replay_state
            .payload
            .discovery_critical_target_buy_mints
            .clear();
        assert!(
            DiscoveryService::state_can_backfill_exact_target_buy_mint_surface_for_resume(
                &replay_state
            ),
            "the deterministic pre-row repro must begin from the same frozen exact-target replay seam as the live checkpoint"
        );
        runtime_store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&replay_state, now)?,
        )?;
        let before = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;

        let page = copybot_storage::ObservedWalletActivityPage {
            rows: Vec::new(),
            rows_seen: 0,
            time_budget_exhausted: true,
            active_day_count_source: None,
            wallet_id_query_exhausted_before_first_page: false,
            wallet_id_page_wallets_seen: 3,
            wallet_id_page_cursor_after: Some("wallet-pre-row-cursor".to_string()),
            wallet_id_page_wallet_ids: vec![
                "wallet-pre-row-a".to_string(),
                "wallet-pre-row-b".to_string(),
                "wallet-pre-row-c".to_string(),
            ],
        };
        let mut diagnostics = ResumeExactTargetBuyMintSurfaceRepairDiagnostics {
            attempted: true,
            wallet_pages: 1,
            wallet_cursor_before: replay_state
                .payload
                .replay_exact_target_surface_wallet_cursor
                .clone(),
            ..ResumeExactTargetBuyMintSurfaceRepairDiagnostics::default()
        };
        DiscoveryService::persist_replay_exact_target_buy_mint_surface_budget_exhaustion_state(
            &mut replay_state,
            &mut diagnostics,
            &page,
            page.wallet_id_page_cursor_after.clone(),
        );
        assert!(diagnostics.time_budget_exhausted);
        assert!(!diagnostics.persisted_partial_progress);
        assert!(diagnostics.persisted_staged_pre_row_state);
        assert!(!diagnostics.persisted_blocked_state);
        assert_eq!(diagnostics.wallet_rows, 0);
        assert_eq!(diagnostics.wallet_id_page_wallets_seen, 3);
        assert!(
            replay_state
                .payload
                .replay_exact_target_surface_staged_wallet_ids
                == page.wallet_id_page_wallet_ids,
            "when bounded exact-target repair exhausts its budget after wallet-id paging but before the first activity row, the checkpoint must persist that exact staged page for the next cycle"
        );
        assert_eq!(
            replay_state
                .payload
                .replay_exact_target_surface_staged_wallet_cursor_after,
            page.wallet_id_page_cursor_after
        );
        assert!(
            !replay_state
                .payload
                .replay_exact_target_surface_pre_row_blocked
        );

        discovery.persist_persisted_stream_rebuild_state(
            &runtime_store,
            &mut replay_state,
            now + Duration::seconds(1),
        )?;
        let after = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert!(
            after.updated_at > before.updated_at,
            "the pre-row exhausted repair seam must now durably change the replay checkpoint instead of leaving the same stale row forever"
        );
        assert_eq!(
            after.payload.replay_exact_target_surface_staged_wallet_ids,
            page.wallet_id_page_wallet_ids
        );
        assert_eq!(
            after
                .payload
                .replay_exact_target_surface_staged_wallet_cursor_after,
            page.wallet_id_page_cursor_after
        );
        assert!(!after.payload.replay_exact_target_surface_pre_row_blocked);
        assert_eq!(after.payload.discovery_critical_target_buy_mints.len(), 0);
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_resume_pre_row_zero_progress_budget_exhaustion_persists_blocked_state_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-resume-exact-target-surface-zero-progress-pre-page.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-13T08:32:11Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 100;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

        let (mut replay_state, _metrics_window_start) =
            seed_stage1_dense_partial_sol_leg_replay_checkpoint_fixture(
                &runtime_store,
                &discovery,
                &config,
                now,
                4_000,
                20,
            )?;
        replay_state.payload.replay_wallet_stats_complete = true;
        replay_state.payload.replay_wallet_stats_milestone_reached = true;
        replay_state
            .payload
            .replay_candidate_activity_backfill_required = true;
        replay_state
            .payload
            .replay_candidate_activity_backfill_pending = false;
        replay_state.payload.replay_sol_leg_budget_floor_pages = replay_state
            .payload
            .replay_sol_leg_budget_floor_pages
            .max(20);
        DiscoveryService::compact_wallet_activity_summary_for_frozen_exact_target_checkpoint(
            &mut replay_state.payload,
        );
        replay_state
            .payload
            .discovery_critical_target_buy_mints
            .clear();
        assert!(
            DiscoveryService::state_can_backfill_exact_target_buy_mint_surface_for_resume(
                &replay_state
            ),
            "the deterministic zero-progress repro must begin from the same frozen exact-target replay seam as the live checkpoint"
        );
        runtime_store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&replay_state, now)?,
        )?;
        let before = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;

        let page = copybot_storage::ObservedWalletActivityPage {
            rows: Vec::new(),
            rows_seen: 0,
            time_budget_exhausted: true,
            active_day_count_source: None,
            wallet_id_query_exhausted_before_first_page: true,
            wallet_id_page_wallets_seen: 0,
            wallet_id_page_cursor_after: None,
            wallet_id_page_wallet_ids: Vec::new(),
        };
        let wallet_cursor_before = replay_state
            .payload
            .replay_exact_target_surface_wallet_cursor
            .clone();
        let mut diagnostics = ResumeExactTargetBuyMintSurfaceRepairDiagnostics {
            attempted: true,
            wallet_pages: 1,
            wallet_cursor_before: wallet_cursor_before.clone(),
            ..ResumeExactTargetBuyMintSurfaceRepairDiagnostics::default()
        };
        DiscoveryService::persist_replay_exact_target_buy_mint_surface_budget_exhaustion_state(
            &mut replay_state,
            &mut diagnostics,
            &page,
            None,
        );
        assert!(diagnostics.time_budget_exhausted);
        assert!(!diagnostics.persisted_partial_progress);
        assert!(!diagnostics.persisted_staged_pre_row_state);
        assert!(diagnostics.persisted_blocked_state);
        assert_eq!(diagnostics.wallet_rows, 0);
        assert_eq!(diagnostics.wallet_id_page_wallets_seen, 0);
        assert!(
            replay_state
                .payload
                .replay_exact_target_surface_staged_wallet_ids
                .is_empty(),
            "the earliest zero-progress seam has no truthful staged page to persist"
        );
        assert!(
            replay_state.payload.replay_exact_target_surface_pre_row_blocked,
            "when exact-target recovery exhausts inside the wallet-id page query before any truthful page boundary exists, the checkpoint must persist explicit blockedness instead of silently returning all-false progress"
        );
        assert_eq!(
            replay_state.payload.replay_exact_target_surface_wallet_cursor,
            wallet_cursor_before,
            "the zero-progress seam must not invent cursor advancement or erase the last truthful preexisting cursor"
        );

        discovery.persist_persisted_stream_rebuild_state(
            &runtime_store,
            &mut replay_state,
            now + Duration::seconds(1),
        )?;
        let after = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert!(
            after.updated_at > before.updated_at,
            "the zero-progress seam must now durably change the persisted checkpoint instead of leaving staged=false, blocked=false, partial=false on the same stale row"
        );
        assert!(after.payload.replay_exact_target_surface_pre_row_blocked);
        assert!(
            after
                .payload
                .replay_exact_target_surface_staged_wallet_ids
                .is_empty(),
            "zero-progress exact wallet-id query exhaustion must not fake a staged page"
        );
        assert_eq!(
            after.payload.replay_exact_target_surface_wallet_cursor,
            wallet_cursor_before
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_resume_pre_row_zero_progress_blocked_state_prevents_invisible_reentry_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-resume-exact-target-surface-zero-progress-blocked-next-cycle.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-13T08:34:19Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 100;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

        let (mut replay_state, metrics_window_start) =
            seed_stage1_dense_partial_sol_leg_replay_checkpoint_fixture(
                &runtime_store,
                &discovery,
                &config,
                now,
                4_000,
                20,
            )?;
        replay_state.payload.replay_wallet_stats_complete = true;
        replay_state.payload.replay_wallet_stats_milestone_reached = true;
        replay_state
            .payload
            .replay_candidate_activity_backfill_required = true;
        replay_state
            .payload
            .replay_candidate_activity_backfill_pending = false;
        replay_state.payload.replay_sol_leg_budget_floor_pages = replay_state
            .payload
            .replay_sol_leg_budget_floor_pages
            .max(20);
        DiscoveryService::compact_wallet_activity_summary_for_frozen_exact_target_checkpoint(
            &mut replay_state.payload,
        );
        replay_state
            .payload
            .discovery_critical_target_buy_mints
            .clear();
        replay_state
            .payload
            .replay_exact_target_surface_pre_row_blocked = true;
        runtime_store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&replay_state, now)?,
        )?;

        let (_resumed, restore_outcome, diagnostics) = discovery
            .load_or_start_persisted_stream_rebuild_state_with_options(
                &runtime_store,
                replay_state.window_start,
                metrics_window_start,
                now + Duration::seconds(1),
                Some(Instant::now() + StdDuration::from_secs(30)),
                None,
                false,
            )?;
        assert!(
            matches!(
                restore_outcome,
                PersistedStreamRebuildRestoreOutcome::ResumedExisting
                    | PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
            ),
            "the explicit zero-progress blocked seam must stay on the same persisted lineage rather than silently restarting from scratch"
        );
        assert!(
            !diagnostics.attempted,
            "once the earliest zero-progress seam is truthfully blocked, the next cycle must not silently re-enter the same exact-target recovery path as if nothing had been persisted"
        );
        let after = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert!(after.payload.replay_exact_target_surface_pre_row_blocked);
        assert!(
            after.payload.replay_exact_target_surface_staged_wallet_ids.is_empty(),
            "the zero-progress blocked seam must remain explicit blockedness, not magically transform into staged progress on the next cycle"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_resume_staged_pre_row_state_is_consumed_before_fresh_page_query_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-resume-exact-target-surface-pre-row-blocked-next-cycle.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-13T08:33:16Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 100;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

        let (mut replay_state, metrics_window_start) =
            seed_stage1_dense_partial_sol_leg_replay_checkpoint_fixture(
                &runtime_store,
                &discovery,
                &config,
                now,
                4_000,
                20,
            )?;
        replay_state.payload.replay_wallet_stats_complete = true;
        replay_state.payload.replay_wallet_stats_milestone_reached = true;
        replay_state
            .payload
            .replay_candidate_activity_backfill_required = true;
        replay_state
            .payload
            .replay_candidate_activity_backfill_pending = false;
        replay_state.payload.replay_sol_leg_budget_floor_pages = replay_state
            .payload
            .replay_sol_leg_budget_floor_pages
            .max(20);
        DiscoveryService::compact_wallet_activity_summary_for_frozen_exact_target_checkpoint(
            &mut replay_state.payload,
        );
        replay_state
            .payload
            .discovery_critical_target_buy_mints
            .clear();
        let mut staged_wallet_ids = replay_state
            .payload
            .by_wallet
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        staged_wallet_ids.sort();
        staged_wallet_ids.truncate(3);
        let staged_wallet_cursor_after = staged_wallet_ids.last().cloned();
        replay_state
            .payload
            .replay_exact_target_surface_staged_wallet_ids = staged_wallet_ids.clone();
        replay_state
            .payload
            .replay_exact_target_surface_staged_wallet_cursor_after =
            staged_wallet_cursor_after.clone();
        runtime_store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&replay_state, now)?,
        )?;

        let (_resumed, restore_outcome, diagnostics) = discovery
            .load_or_start_persisted_stream_rebuild_state_with_options(
                &runtime_store,
                replay_state.window_start,
                metrics_window_start,
                now + Duration::seconds(1),
                Some(Instant::now() + StdDuration::from_secs(30)),
                None,
                false,
            )?;
        assert!(
            matches!(
                restore_outcome,
                PersistedStreamRebuildRestoreOutcome::ResumedExisting
                    | PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
            ),
            "the explicit pre-row blocked replay seam must remain on the same persisted lineage instead of silently restarting from scratch"
        );
        assert!(diagnostics.attempted);
        assert!(diagnostics.resumed_from_staged_pre_row_state);
        assert!(
            diagnostics.wallet_rows > 0,
            "the next cycle must materialize wallet-activity rows from the staged exact wallet-id page before it is allowed to continue the replay seam"
        );
        let after = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert!(
            after.payload.replay_exact_target_surface_staged_wallet_ids.is_empty(),
            "once the staged exact wallet-id page has been safely materialized into activity rows, staged state must be cleared"
        );
        assert_eq!(
            after
                .payload
                .replay_exact_target_surface_staged_wallet_cursor_after,
            None
        );
        assert!(!after.payload.replay_exact_target_surface_pre_row_blocked);
        assert!(
            after.updated_at > now,
            "consuming staged pre-row state must durably advance the persisted checkpoint instead of leaving the same staged page behind"
        );
        Ok(())
    }
