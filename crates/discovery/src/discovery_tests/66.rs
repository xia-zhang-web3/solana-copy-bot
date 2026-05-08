#[test]
    fn persisted_stream_replay_resume_budget_exhaustion_persists_exact_target_surface_cursor_and_resumes_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-resume-exact-target-surface-budget-exhaustion.db");
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
        assert!(
            DiscoveryService::state_can_backfill_exact_target_buy_mint_surface_for_resume(
                &replay_state
            ),
            "the deterministic repro must start from the exact frozen replay seam that owns the missing exact target-mint surface repair"
        );
        runtime_store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&replay_state, now)?,
        )?;
        let before = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert_eq!(
            before.payload.replay_exact_target_surface_wallet_cursor, None,
            "the repro must begin from the current live-like frozen shape: no persisted resume cursor for the missing exact target-surface scan"
        );
        let partial_now = now + Duration::seconds(1);

        let wallet_page_limit_override =
            force_replay_resume_exact_target_surface_wallet_page_limit_for_test(1);
        let (_partially_repaired, restore_outcome, partial_diagnostics) = discovery
            .load_or_start_persisted_stream_rebuild_state_with_options(
                &runtime_store,
                replay_state.window_start,
                metrics_window_start,
                partial_now,
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
            "resume repair must stay on the existing replay lineage instead of restarting from scratch"
        );
        assert!(partial_diagnostics.attempted);
        assert!(partial_diagnostics.time_budget_exhausted);
        assert!(
            partial_diagnostics.wallet_rows > 0,
            "the forced one-page budget exhaust must still account for at least one exact-wallet page before persisting a resume cursor"
        );
        assert!(!partial_diagnostics.completed);
        assert!(partial_diagnostics.persisted_partial_progress);
        assert!(
            partial_diagnostics.wallet_cursor_after.is_some(),
            "the partial repair must persist a real exact-wallet resume cursor so the next cycle starts later than the frozen checkpoint"
        );
        let after_partial = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert!(
            after_partial.updated_at > before.updated_at,
            "the budget-exhausted repair path must now durably advance the checkpoint instead of returning to the same stale row forever"
        );
        assert_eq!(
            after_partial.payload.replay_exact_target_surface_wallet_cursor,
            partial_diagnostics.wallet_cursor_after,
            "the persisted checkpoint must record the exact-wallet resume cursor reached before deadline exhaustion"
        );
        drop(wallet_page_limit_override);
        Ok(())
    }

    #[test]
    fn persisted_stream_replay_resume_advances_from_persisted_exact_target_surface_cursor_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-resume-exact-target-surface-from-persisted-cursor.db");
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
        let mut exact_wallet_ids = replay_state
            .payload
            .by_wallet
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        exact_wallet_ids.sort();
        let persisted_resume_cursor = exact_wallet_ids
            .first()
            .cloned()
            .context("expected at least one exact wallet in the frozen replay checkpoint")?;
        replay_state
            .payload
            .replay_exact_target_surface_wallet_cursor = Some(persisted_resume_cursor.clone());
        runtime_store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&replay_state, now)?,
        )?;
        let wallet_page_limit_override =
            force_replay_resume_exact_target_surface_wallet_page_limit_for_test(1);

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
            "the resumed exact-target repair must stay on the persisted replay lineage"
        );
        assert_eq!(
            diagnostics.wallet_cursor_before,
            Some(persisted_resume_cursor),
            "the next-cycle repair must honor the persisted exact-wallet resume cursor instead of restarting from the frozen checkpoint head"
        );
        assert!(diagnostics.persisted_partial_progress);
        assert!(
            diagnostics.wallet_cursor_after.is_some()
                && diagnostics.wallet_cursor_after != diagnostics.wallet_cursor_before,
            "once a persisted exact-wallet resume cursor exists, the next bounded cycle must advance that cursor again instead of replaying the same frozen prefix"
        );
        drop(wallet_page_limit_override);

        let after_complete = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert_eq!(
            after_complete.payload.replay_exact_target_surface_wallet_cursor,
            diagnostics.wallet_cursor_after,
            "the checkpoint written after a resumed partial repair must durably advance the persisted exact-wallet cursor"
        );
        Ok(())
    }
