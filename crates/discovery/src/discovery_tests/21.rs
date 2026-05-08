    fn seed_stage1_recent_raw_journal_head_gap_no_repairable_rows_fixture() -> Result<(
        tempfile::TempDir,
        SqliteStore,
        SqliteStore,
        DiscoveryService,
        DateTime<Utc>,
        DiscoveryRuntimeCursor,
    )> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-publication-repair-head-gap-no-repairable-rows-runtime.db");
        let journal_db_path = temp
            .path()
            .join("stage1-publication-repair-head-gap-no-repairable-rows-journal.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let mut journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;
        journal_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-02T11:15:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = discovery.metrics_window_start(now);
        let runtime_cursor_ts = now - Duration::minutes(1);
        let runtime_cursor = DiscoveryRuntimeCursor {
            ts_utc: runtime_cursor_ts,
            slot: runtime_cursor_ts.timestamp().max(0) as u64,
            signature: "head-gap-no-repairable-rows-runtime-cursor".to_string(),
        };

        journal_store.insert_recent_raw_journal_batch(
            &[
                swap(
                    "wallet_old",
                    "head-gap-no-repairable-rows-before-window",
                    window_start - Duration::minutes(30),
                    SOL_MINT,
                    "TokenHeadGapOld111111111111111111111111111",
                    0.9,
                    90.0,
                ),
                swap(
                    "wallet_future",
                    "head-gap-no-repairable-rows-after-runtime-cursor",
                    runtime_cursor_ts + Duration::minutes(2),
                    SOL_MINT,
                    "TokenHeadGapFuture11111111111111111111111",
                    1.1,
                    110.0,
                ),
            ],
            now,
        )?;
        runtime_store.upsert_discovery_runtime_cursor(&runtime_cursor)?;
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_unusable_no_recent_published_universe".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        Ok((
            temp,
            runtime_store,
            journal_store,
            discovery,
            now,
            runtime_cursor,
        ))
    }

    #[test]
    fn repair_recent_raw_journal_head_gap_without_repairable_rows_rejects_cursor_metadata_as_insufficient_stage1(
    ) -> Result<()> {
        let (_temp, runtime_store, journal_store, discovery, now, runtime_cursor) =
            seed_stage1_recent_raw_journal_head_gap_no_repairable_rows_fixture()?;
        let required_window_start = now - Duration::days(discovery.runtime_scoring_window_days());

        assert!(
            !discovery.persisted_observed_swaps_cover_window(
                &runtime_store,
                required_window_start
            )?,
            "fixture must start from an incomplete runtime window so the head-gap branch is actually considered"
        );

        let repair = discovery
            .repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed(
                &runtime_store,
                Some(&journal_store),
                now,
                64,
                Instant::now() + StdDuration::from_secs(1),
            )?;
        assert_eq!(
            repair.state,
            "skipped_recent_raw_journal_repair_interval_unavailable"
        );
        assert_eq!(
            repair.reason.as_deref(),
            Some("recent_raw_journal_does_not_cover_repairable_interval_before_runtime_cursor")
        );
        assert!(
            !repair.journal_covers_runtime_cursor,
            "effective cursor coverage must now require a repairable journal interval, not only outer-bounds metadata"
        );
        assert!(!repair.runtime_window_complete_after);
        assert!(repair.runtime_window_first_cursor.is_none());
        assert_eq!(repair.replay_batches_completed, 0);
        assert_eq!(repair.replay_rows_loaded, 0);
        assert_eq!(repair.replay_rows_inserted, 0);
        assert!(!repair.replay_time_budget_exhausted);
        assert_ne!(
            repair.state,
            "skipped_recent_raw_journal_head_gap_no_repairable_rows",
            "the empty-interval seam should now fail the stronger coverage predicate before claiming head-gap replay eligibility"
        );
        let replay_until_cursor = repair.replay_until_cursor.as_ref().expect(
            "the explicit non-replay state should still expose the derived replay boundary",
        );
        assert_eq!(replay_until_cursor.ts_utc, runtime_cursor.ts_utc);
        assert_eq!(replay_until_cursor.slot, runtime_cursor.slot);
        assert_eq!(replay_until_cursor.signature, runtime_cursor.signature);
        Ok(())
    }

    fn seed_stage1_recent_raw_journal_required_window_ahead_of_runtime_cursor_fixture() -> Result<(
        tempfile::TempDir,
        SqliteStore,
        SqliteStore,
        DiscoveryService,
        DateTime<Utc>,
        DiscoveryRuntimeCursor,
    )> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-publication-repair-window-ahead-runtime-cursor-runtime.db");
        let journal_db_path = temp
            .path()
            .join("stage1-publication-repair-window-ahead-runtime-cursor-journal.db");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let mut journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;
        journal_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-23T08:59:49Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = discovery.metrics_window_start(now);
        let runtime_cursor_ts = window_start - Duration::hours(16);
        let runtime_cursor = DiscoveryRuntimeCursor {
            ts_utc: runtime_cursor_ts,
            slot: runtime_cursor_ts.timestamp().max(0) as u64,
            signature: "head-gap-required-window-ahead-runtime-cursor".to_string(),
        };

        journal_store.insert_recent_raw_journal_batch(
            &[swap(
                "wallet_old",
                "head-gap-required-window-ahead-runtime-cursor",
                runtime_cursor_ts,
                SOL_MINT,
                "TokenHeadGapCursor111111111111111111111111",
                1.0,
                100.0,
            )],
            now,
        )?;
        runtime_store.upsert_discovery_runtime_cursor(&runtime_cursor)?;
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_unusable_no_recent_published_universe".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        Ok((
            temp,
            runtime_store,
            journal_store,
            discovery,
            now,
            runtime_cursor,
        ))
    }

    #[test]
    fn repair_recent_raw_journal_required_window_ahead_of_runtime_cursor_exits_before_head_gap_replay_stage1(
    ) -> Result<()> {
        let (_temp, runtime_store, journal_store, discovery, now, runtime_cursor) =
            seed_stage1_recent_raw_journal_required_window_ahead_of_runtime_cursor_fixture()?;
        let required_window_start = now - Duration::days(discovery.runtime_scoring_window_days());
        assert!(
            required_window_start > runtime_cursor.ts_utc,
            "fixture must reproduce the live shape where the freshness horizon is ahead of the runtime cursor"
        );
        assert!(
            !discovery
                .persisted_observed_swaps_cover_window(&runtime_store, required_window_start)?,
            "runtime window must start incomplete"
        );

        let before = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist before repair");
        let repair = discovery
            .repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed(
                &runtime_store,
                Some(&journal_store),
                now,
                64,
                Instant::now() + StdDuration::from_secs(1),
            )?;

        assert_eq!(
            repair.state,
            "skipped_recent_raw_journal_required_window_ahead_of_runtime_cursor"
        );
        assert_eq!(
            repair.reason.as_deref(),
            Some("required_window_start_is_after_runtime_cursor")
        );
        assert!(
            !repair.journal_covers_runtime_cursor,
            "effective recent_raw repairability must be false when the required window starts after the runtime cursor"
        );
        assert!(!repair.runtime_window_complete_before);
        assert!(!repair.runtime_window_complete_after);
        assert!(repair.runtime_window_first_cursor.is_none());
        assert_eq!(repair.replay_batches_completed, 0);
        assert_eq!(repair.replay_rows_loaded, 0);
        assert_eq!(repair.replay_rows_inserted, 0);
        assert!(!repair.replay_time_budget_exhausted);
        let replay_until_cursor = repair.replay_until_cursor.as_ref().expect(
            "the explicit horizon-ahead state should still expose the impossible replay boundary",
        );
        assert_eq!(replay_until_cursor.ts_utc, runtime_cursor.ts_utc);
        assert_eq!(replay_until_cursor.slot, runtime_cursor.slot);
        assert_eq!(replay_until_cursor.signature, runtime_cursor.signature);

        let after = runtime_store
            .discovery_publication_state()?
            .expect("publication state should still exist after repair");
        assert_eq!(after.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(
            after.reason,
            "raw_window_unusable_no_recent_published_universe"
        );
        assert_eq!(after.updated_at, before.updated_at);
        Ok(())
    }
fn seed_stage1_recent_raw_journal_required_window_ahead_minimal_pre_predicate_fixture(
    ) -> Result<(
        tempfile::TempDir,
        SqliteStore,
        SqliteStore,
        DiscoveryService,
        DateTime<Utc>,
        DiscoveryRuntimeCursor,
    )> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-publication-repair-window-ahead-minimal-runtime.db");
        let journal_db_path = temp
            .path()
            .join("stage1-publication-repair-window-ahead-minimal-journal.db");
        let runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;

        let now = DateTime::parse_from_rfc3339("2026-04-23T08:59:49Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let required_window_start = now - Duration::days(discovery.runtime_scoring_window_days());
        let metrics_window_start = discovery.metrics_window_start(now);
        let runtime_cursor_ts = required_window_start - Duration::hours(16);
        let runtime_cursor = DiscoveryRuntimeCursor {
            ts_utc: runtime_cursor_ts,
            slot: runtime_cursor_ts.timestamp().max(0) as u64,
            signature: "head-gap-window-ahead-pre-predicate".to_string(),
        };

        runtime_store.upsert_discovery_runtime_cursor(&runtime_cursor)?;
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_unusable_no_recent_published_universe".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        Ok((
            temp,
            runtime_store,
            journal_store,
            discovery,
            now,
            runtime_cursor,
        ))
    }

    #[test]
    fn repair_recent_raw_journal_required_window_ahead_exits_before_window_or_journal_reads_stage1(
    ) -> Result<()> {
        let (_temp, runtime_store, journal_store, discovery, now, runtime_cursor) =
            seed_stage1_recent_raw_journal_required_window_ahead_minimal_pre_predicate_fixture()?;
        let required_window_start = now - Duration::days(discovery.runtime_scoring_window_days());
        assert!(
            required_window_start > runtime_cursor.ts_utc,
            "fixture must start with the live horizon-ahead shape"
        );

        let repair = discovery
            .repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed(
                &runtime_store,
                Some(&journal_store),
                now,
                64,
                Instant::now() + StdDuration::from_secs(1),
            )?;

        assert_eq!(
            repair.state,
            "skipped_recent_raw_journal_required_window_ahead_of_runtime_cursor"
        );
        assert_eq!(
            repair.reason.as_deref(),
            Some("required_window_start_is_after_runtime_cursor")
        );
        assert!(repair.runtime_cursor_exists_before);
        assert!(repair.journal_store_exists);
        assert!(!repair.runtime_window_complete_before);
        assert!(!repair.runtime_window_complete_after);
        assert!(
            repair.journal_covered_since.is_none(),
            "the horizon-ahead exit must not read recent_raw journal state before returning"
        );
        assert!(repair.runtime_window_first_cursor.is_none());
        assert_eq!(repair.replay_batches_completed, 0);
        assert_eq!(repair.replay_rows_loaded, 0);
        assert_eq!(repair.replay_rows_inserted, 0);
        let replay_until_cursor = repair
            .replay_until_cursor
            .as_ref()
            .expect("explicit horizon-ahead telemetry should retain the runtime cursor boundary");
        assert_eq!(replay_until_cursor.ts_utc, runtime_cursor.ts_utc);
        assert_eq!(replay_until_cursor.slot, runtime_cursor.slot);
        assert_eq!(replay_until_cursor.signature, runtime_cursor.signature);
        Ok(())
    }

    fn seed_stage1_deferred_runtime_publication_refresh_fixture() -> Result<(
        tempfile::TempDir,
        SqliteStore,
        DiscoveryService,
        DateTime<Utc>,
        DateTime<Utc>,
        Vec<String>,
    )> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-deferred-runtime-publication-refresh.db");
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
        let stale_published_wallets = (0..7usize)
            .map(|idx| format!("wallet-stale-{idx}"))
            .collect::<Vec<_>>();
        let stale_last_published_at =
            now - discovery.runtime_published_universe_max_age() - Duration::seconds(1);
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_missing_exact_published_wallet_ids".to_string(),
            last_published_at: Some(stale_last_published_at),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(stale_published_wallets.clone()),
        })?;
        Ok((
            temp,
            runtime_store,
            discovery,
            now,
            stale_last_published_at,
            stale_published_wallets,
        ))
    }

    fn seed_stage1_deferred_runtime_publication_refresh_missing_exact_target_surface_fixture(
    ) -> Result<(
        tempfile::TempDir,
        SqliteStore,
        SqliteStore,
        DiscoveryService,
        DateTime<Utc>,
        DateTime<Utc>,
        Vec<String>,
    )> {
        let temp = tempdir().context("failed to create tempdir")?;
        let runtime_db_path = temp
            .path()
            .join("stage1-deferred-runtime-publication-refresh-missing-exact-target-surface.db");
        let journal_db_path = temp.path().join(
            "stage1-deferred-runtime-publication-refresh-missing-exact-target-surface-journal.db",
        );
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        let mut journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;
        journal_store.run_migrations(&migration_dir)?;

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
                1_500,
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
            "the live-like helper repro must start from the exact missing target-surface seam that can spend control in resume repair before the first helper return/write log"
        );
        runtime_store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&replay_state, now)?,
        )?;
        let stale_published_wallets = (0..7usize)
            .map(|idx| format!("wallet-live-like-stale-{idx}"))
            .collect::<Vec<_>>();
        let stale_last_published_at =
            now - discovery.runtime_published_universe_max_age() - Duration::seconds(1);
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_missing_exact_published_wallet_ids".to_string(),
            last_published_at: Some(stale_last_published_at),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(stale_published_wallets.clone()),
        })?;

        Ok((
            temp,
            runtime_store,
            journal_store,
            discovery,
            now,
            stale_last_published_at,
            stale_published_wallets,
        ))
    }
