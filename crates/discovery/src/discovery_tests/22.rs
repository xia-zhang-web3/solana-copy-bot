    fn old_like_deferred_runtime_publication_refresh_without_surface_write(
        discovery: &DiscoveryService,
        runtime_store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<DiscoveryPublicationTruthRepairTelemetry> {
        let gate = discovery.publication_freshness_gate();
        let required_window_start = now - Duration::days(discovery.runtime_scoring_window_days());
        let publication_state = runtime_store.discovery_publication_state_read_only()?;
        let publication_truth_complete_before = publication_state
            .as_ref()
            .is_some_and(DiscoveryPublicationStateRow::has_complete_publication_truth);
        let publication_truth_fresh_before = publication_state
            .as_ref()
            .is_some_and(|state| state.is_fresh_under_gate(&gate, now));
        let runtime_window_complete_before = discovery
            .persisted_observed_swaps_cover_window(runtime_store, required_window_start)?;
        assert!(runtime_window_complete_before);
        assert!(!publication_truth_fresh_before);
        let refresh_budget = StdDuration::from_secs(60);
        let metrics_window_start = discovery.metrics_window_start(now);
        let fetch_limit = discovery.config.max_fetch_swaps_per_cycle.max(1);
        let fetch_page_limit = discovery.config.max_fetch_pages_per_cycle.max(1);
        let base_recovery_contract = discovery.persisted_stream_priority_recovery_contract(
            runtime_store,
            now,
            fetch_limit,
            fetch_page_limit,
            refresh_budget,
        )?;
        let (state, _) = discovery.load_or_start_persisted_stream_rebuild_state(
            runtime_store,
            required_window_start,
            metrics_window_start,
            now,
        )?;
        let recovery_contract = discovery
            .deepen_persisted_stream_priority_recovery_contract_for_state_at(
                &state,
                fetch_limit,
                fetch_page_limit,
                base_recovery_contract,
                Some(now),
            );
        let telemetry = discovery.persisted_stream_progress_telemetry_from_state(&state, now);
        Ok(
            DiscoveryPublicationTruthRepairTelemetry::deferred_to_runtime_cycle(
                required_window_start,
                publication_truth_complete_before,
                publication_truth_fresh_before,
                runtime_window_complete_before,
                recovery_contract,
                &telemetry,
            ),
        )
    }

    fn old_like_runtime_window_complete_helper_missing_exact_target_surface_without_bounded_resume_deadline(
        discovery: &DiscoveryService,
        runtime_store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<(
        DiscoveryPublicationTruthRepairTelemetry,
        ResumeExactTargetBuyMintSurfaceRepairDiagnostics,
    )> {
        let gate = discovery.publication_freshness_gate();
        let required_window_start = now - Duration::days(discovery.runtime_scoring_window_days());
        let publication_state = runtime_store.discovery_publication_state_read_only()?;
        let publication_truth_complete_before = publication_state
            .as_ref()
            .is_some_and(DiscoveryPublicationStateRow::has_complete_publication_truth);
        let publication_truth_fresh_before = publication_state
            .as_ref()
            .is_some_and(|state| state.is_fresh_under_gate(&gate, now));
        let runtime_window_complete_before = discovery
            .persisted_observed_swaps_cover_window(runtime_store, required_window_start)?;
        assert!(runtime_window_complete_before);
        assert!(publication_truth_complete_before);
        assert!(!publication_truth_fresh_before);
        let refresh_budget = StdDuration::from_secs(60);
        let metrics_window_start = discovery.metrics_window_start(now);
        let fetch_limit = discovery.config.max_fetch_swaps_per_cycle.max(1);
        let fetch_page_limit = discovery.config.max_fetch_pages_per_cycle.max(1);
        let base_recovery_contract = discovery.persisted_stream_priority_recovery_contract(
            runtime_store,
            now,
            fetch_limit,
            fetch_page_limit,
            refresh_budget,
        )?;
        let (state, _, resume_exact_target_surface_repair) = discovery
            .load_or_start_persisted_stream_rebuild_state_with_options(
                runtime_store,
                required_window_start,
                metrics_window_start,
                now,
                None,
                None,
                false,
            )?;
        let recovery_contract = discovery
            .deepen_persisted_stream_priority_recovery_contract_for_state_at(
                &state,
                fetch_limit,
                fetch_page_limit,
                base_recovery_contract,
                Some(now),
            );
        let telemetry = discovery.persisted_stream_progress_telemetry_from_state(&state, now);
        Ok((
            DiscoveryPublicationTruthRepairTelemetry::deferred_to_runtime_cycle(
                required_window_start,
                publication_truth_complete_before,
                publication_truth_fresh_before,
                runtime_window_complete_before,
                recovery_contract,
                &telemetry,
            )
            .with_resume_exact_target_surface_repair(&resume_exact_target_surface_repair),
            resume_exact_target_surface_repair,
        ))
    }

    fn traced_runtime_publication_truth_repair_helper(
        discovery: &DiscoveryService,
        runtime_store: &SqliteStore,
        journal_store: Option<&SqliteStore>,
        now: DateTime<Utc>,
        deadline: Instant,
        resume_exact_target_surface_repair_deadline_override: Option<Instant>,
        defer_resume_exact_target_surface_repair_to_runtime_cycle: bool,
    ) -> Result<(
        DiscoveryPublicationTruthRepairTelemetry,
        Vec<DiscoveryPublicationTruthRepairRegionTraceEvent>,
    )> {
        let collector = DiscoveryPublicationTruthRepairTraceCollector::default();
        let telemetry = discovery
            .repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed_with_options(
                runtime_store,
                journal_store,
                now,
                64,
                deadline,
                resume_exact_target_surface_repair_deadline_override,
                Some(collector.clone()),
                defer_resume_exact_target_surface_repair_to_runtime_cycle,
            )?;
        Ok((telemetry, collector.snapshot()))
    }

    fn region_event_sequence(
        events: &[DiscoveryPublicationTruthRepairRegionTraceEvent],
    ) -> Vec<(&'static str, &'static str)> {
        events
            .iter()
            .map(|event| (event.region, event.state))
            .collect()
    }

    fn assert_region_trace_contains_subsequence(
        events: &[DiscoveryPublicationTruthRepairRegionTraceEvent],
        expected: &[(&'static str, &'static str)],
    ) {
        let mut cursor = 0usize;
        for expected_event in expected.iter().copied() {
            while cursor < events.len()
                && (events[cursor].region, events[cursor].state) != expected_event
            {
                cursor = cursor.saturating_add(1);
            }
            assert!(
                cursor < events.len(),
                "missing expected region event {:?} in {:?}",
                expected_event,
                region_event_sequence(events)
            );
            cursor = cursor.saturating_add(1);
        }
    }

    #[test]
    fn repair_runtime_window_complete_deferred_replay_sol_leg_old_like_performs_zero_publication_writes_stage1(
    ) -> Result<()> {
        let (_temp, runtime_store, discovery, now, stale_last_published_at, stale_wallet_ids) =
            seed_stage1_deferred_runtime_publication_refresh_fixture()?;
        let before = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist before the delegated repair");

        let repair = old_like_deferred_runtime_publication_refresh_without_surface_write(
            &discovery,
            &runtime_store,
            now,
        )?;
        assert_eq!(
            repair.state,
            "deferred_runtime_window_truth_refresh_to_run_cycle"
        );
        assert!(repair.publication_truth_refresh_delegated_to_runtime_cycle);

        let after = runtime_store
            .discovery_publication_state()?
            .expect("publication state should still exist after the old-like deferred repair");
        assert_eq!(
            after.updated_at, before.updated_at,
            "old/current-like delegated repair performs zero publication writes before the long-running run_cycle begins"
        );
        assert_eq!(after.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(
            after.reason,
            "publication_truth_withheld_missing_exact_published_wallet_ids"
        );
        assert_eq!(after.last_published_at, Some(stale_last_published_at));
        assert_eq!(
            after.published_wallet_ids.unwrap_or_default(),
            stale_wallet_ids,
            "the carried complete publication row should stay untouched under the old zero-write seam"
        );
        Ok(())
    }

    #[test]
    fn repair_runtime_window_complete_deferred_replay_sol_leg_refreshes_fail_closed_runtime_surface_before_run_cycle_stage1(
    ) -> Result<()> {
        let (_temp, runtime_store, discovery, now, stale_last_published_at, stale_wallet_ids) =
            seed_stage1_deferred_runtime_publication_refresh_fixture()?;
        let before = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist before repair");

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
        assert_eq!(
            repair.publication_truth_refresh_publishable_checkpoint_blocker,
            Some("replay_sol_leg_incomplete")
        );

        let after = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist after deferred repair");
        assert!(
            after.updated_at > before.updated_at,
            "the fixed deferred repair must refresh publication state before the long-running run_cycle starts"
        );
        assert_eq!(after.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(
            after.reason,
            "publication_truth_withheld_while_replay_sol_leg_incomplete"
        );
        assert_eq!(
            after.last_published_at,
            Some(stale_last_published_at),
            "the deferred repair must carry forward the old publish timestamp instead of faking freshness"
        );
        assert_eq!(
            after.published_wallet_ids.clone().unwrap_or_default(),
            stale_wallet_ids,
            "the deferred repair must preserve the carried exact wallet ids instead of inventing a new publish"
        );
        assert!(after.has_complete_publication_truth());
        assert!(
            !after.is_fresh_under_gate(&discovery.publication_freshness_gate(), now),
            "refreshing the fail-closed runtime surface must not satisfy the export freshness gate"
        );
        Ok(())
    }
#[test]
    fn repair_runtime_window_complete_deferred_refresh_with_missing_exact_wallet_ids_does_not_fake_complete_publication_truth_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-deferred-runtime-publication-refresh-missing-wallet-ids.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-12T21:03:21Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 100;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (replay_state, metrics_window_start, _) =
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
        let stale_last_published_at =
            now - discovery.runtime_published_universe_max_age() - Duration::seconds(1);
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_missing_exact_published_wallet_ids".to_string(),
            last_published_at: Some(stale_last_published_at),
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
        let after = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist after deferred repair");
        assert_eq!(after.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(
            after.reason,
            "publication_truth_withheld_while_replay_sol_leg_incomplete"
        );
        assert_eq!(after.last_published_at, Some(stale_last_published_at));
        assert!(
            !after.has_complete_publication_truth(),
            "the deferred repair must not fake complete publication truth when exact published wallet ids are still missing"
        );
        assert!(
            !after.is_fresh_under_gate(&discovery.publication_freshness_gate(), now),
            "an honest fail-closed deferred refresh must still fail the freshness gate"
        );
        Ok(())
    }

    #[test]
    fn runtime_window_complete_helper_live_like_missing_exact_target_surface_routes_next_into_resume_repair_stage1(
    ) -> Result<()> {
        let (_temp, runtime_store, _journal_store, discovery, now, _, _) =
            seed_stage1_deferred_runtime_publication_refresh_missing_exact_target_surface_fixture(
            )?;
        let required_window_start = now - Duration::days(discovery.runtime_scoring_window_days());
        let metrics_window_start = discovery.metrics_window_start(now);

        assert!(discovery
            .persisted_observed_swaps_cover_window(&runtime_store, required_window_start)?);
        let (state, restore_outcome, resume_exact_target_surface_repair) = discovery
            .load_or_start_persisted_stream_rebuild_state_with_options(
                &runtime_store,
                required_window_start,
                metrics_window_start,
                now,
                Some(Instant::now()),
                None,
                false,
            )?;

        assert!(
            matches!(
                restore_outcome,
                PersistedStreamRebuildRestoreOutcome::ResumedExisting
                    | PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
            ),
            "the live-like helper repro must stay on the existing persisted replay lineage instead of restarting from scratch"
        );
        assert!(
            resume_exact_target_surface_repair.attempted,
            "once the helper entry shape sees a complete-but-stale publication row, complete runtime window, runtime cursor, and existing replay checkpoint, the next internal region must be the exact target-surface resume repair"
        );
        assert!(resume_exact_target_surface_repair.time_budget_exhausted);
        assert!(!resume_exact_target_surface_repair.completed);
        assert!(
            state.payload.discovery_critical_target_buy_mints.is_empty(),
            "when the helper-level repair deadline is already exhausted, the exact target surface must remain missing so the helper can surface the real replay blocker instead of spending more time inside resume repair"
        );
        Ok(())
    }

    #[test]
    fn runtime_window_complete_helper_live_like_missing_exact_target_surface_old_like_resume_repair_ignores_helper_budget_stage1(
    ) -> Result<()> {
        let (_temp, runtime_store, _journal_store, discovery, now, _, _) =
            seed_stage1_deferred_runtime_publication_refresh_missing_exact_target_surface_fixture(
            )?;

        let (repair, resume_exact_target_surface_repair) =
            old_like_runtime_window_complete_helper_missing_exact_target_surface_without_bounded_resume_deadline(
                &discovery,
                &runtime_store,
                now,
            )?;

        assert_eq!(
            repair.state,
            "deferred_runtime_window_truth_refresh_to_run_cycle"
        );
        assert!(
            resume_exact_target_surface_repair.attempted,
            "old/current-like helper behavior must spend control inside the exact target-surface resume repair before it can ever reach the deferred return path"
        );
        assert!(
            resume_exact_target_surface_repair.completed,
            "without the helper deadline threaded into resume repair, the old/current-like path ignores the bounded helper contract and continues reconstructing the exact target surface"
        );
        assert!(!resume_exact_target_surface_repair.time_budget_exhausted);
        assert!(
            resume_exact_target_surface_repair.target_buy_mints_restored > 0,
            "the old/current-like path must do real exact-target reconstruction work here; otherwise it would not explain why live stayed inside the helper after entry"
        );
        let repaired = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert!(
            !repaired.payload.discovery_critical_target_buy_mints.is_empty(),
            "the old/current-like path must durably restore exact target mints during resume repair because it is not honoring the helper deadline on this seam"
        );
        Ok(())
    }

    #[test]
    fn repair_runtime_window_complete_live_like_missing_exact_target_surface_defers_wallet_activity_scan_and_reaches_deferred_write_stage1(
    ) -> Result<()> {
        let (
            _temp,
            runtime_store,
            journal_store,
            discovery,
            now,
            stale_last_published_at,
            stale_wallet_ids,
        ) = seed_stage1_deferred_runtime_publication_refresh_missing_exact_target_surface_fixture(
        )?;
        let before = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist before helper repair");

        let repair = discovery
            .repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed_with_options(
                &runtime_store,
                Some(&journal_store),
                now,
                64,
                Instant::now() + StdDuration::from_secs(60),
                Some(Instant::now()),
                None,
                true,
            )?;

        assert_eq!(
            repair.state,
            "deferred_runtime_window_truth_refresh_to_run_cycle"
        );
        assert!(repair.publication_truth_refresh_delegated_to_runtime_cycle);
        assert!(repair.publication_truth_refresh_helper_write_attempted);
        assert!(repair.publication_truth_refresh_helper_write_succeeded);
        assert!(
            !repair.publication_truth_refresh_resume_exact_target_surface_repair_attempted,
            "the fixed helper path must not enter the DB-heavy exact-wallet scan region before surfacing its deferred fail-closed write/return"
        );
        assert_eq!(
            repair.publication_truth_refresh_publishable_checkpoint_blocker,
            Some("replay_sol_leg_incomplete")
        );

        let after = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist after helper repair");
        assert!(after.updated_at > before.updated_at);
        assert_eq!(after.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(
            after.reason,
            "publication_truth_withheld_while_replay_sol_leg_incomplete"
        );
        assert_eq!(after.last_published_at, Some(stale_last_published_at));
        assert_eq!(
            after.published_wallet_ids.clone().unwrap_or_default(),
            stale_wallet_ids
        );
        assert!(
            !after.is_fresh_under_gate(&discovery.publication_freshness_gate(), now),
            "respecting the helper deadline must not fake freshness under the export gate"
        );
        assert!(
            discovery
                .runtime_publication_truth_resolution(&runtime_store, now)?
                .is_none(),
            "the bounded helper fix must still leave runtime publication truth unavailable until a real exact publish happens later"
        );
        let rebuilt = load_persisted_stream_rebuild_state_for_test(&runtime_store)?;
        assert!(
            rebuilt.payload.discovery_critical_target_buy_mints.is_empty(),
            "deferring helper-side exact-target repair must leave ownership with the later replay path instead of half-repairing it before helper return"
        );
        Ok(())
    }

    #[test]
    fn helper_live_like_missing_exact_target_surface_region_trace_enters_expected_next_regions_stage1(
    ) -> Result<()> {
        let (_temp, runtime_store, journal_store, discovery, now, _, _) =
            seed_stage1_deferred_runtime_publication_refresh_missing_exact_target_surface_fixture(
            )?;

        let (_repair, events) = traced_runtime_publication_truth_repair_helper(
            &discovery,
            &runtime_store,
            Some(&journal_store),
            now,
            Instant::now() + StdDuration::from_secs(60),
            Some(Instant::now()),
            true,
        )?;

        assert_region_trace_contains_subsequence(
            &events,
            &[
                (
                    "repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed_with_options",
                    "entered",
                ),
                ("persisted_stream_priority_recovery_contract", "entered"),
                ("persisted_stream_priority_recovery_contract", "exited"),
                ("load_or_start_persisted_stream_rebuild_state_with_options", "entered"),
                ("repair_restored_persisted_stream_state_for_resume", "entered"),
                ("repair_restored_persisted_stream_state_for_resume", "exited"),
                ("load_or_start_persisted_stream_rebuild_state_with_options", "exited"),
            ],
        );
        assert!(
            !events.iter().any(|event| {
                event.region == "repair_replay_exact_target_buy_mint_surface_for_resume"
            }),
            "the fixed helper path must not enter exact-target surface repair before surfacing the deferred publication write/return"
        );
        Ok(())
    }
