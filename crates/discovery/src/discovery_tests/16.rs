    #[test]
    fn persisted_stream_rebuild_resumes_publish_pending_after_long_restart_within_same_bucket_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-persisted-stream-stale-horizon.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let checkpoint_now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let restart_now = checkpoint_now + Duration::minutes(15);
        let window_start = restart_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, restart_now);

        let mut stale_state = discovery.start_persisted_stream_rebuild_state(
            window_start,
            metrics_window_start,
            checkpoint_now,
        );
        stale_state.phase = DiscoveryPersistedRebuildPhase::PublishPending;
        stale_state
            .payload
            .completed_snapshots
            .push(WalletSnapshot {
                wallet_id: "wallet_stale".to_string(),
                first_seen: stale_state.window_start,
                last_seen: stale_state.horizon_end,
                pnl_sol: 1.0,
                win_rate: 1.0,
                trades: 4,
                closed_trades: 4,
                hold_median_seconds: 60,
                score: 1.0,
                buy_total: 4,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
                eligible: true,
            });
        stale_state.updated_at =
            checkpoint_now - Duration::seconds(config.refresh_seconds.max(1) as i64 + 1);
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&stale_state, stale_state.updated_at)?,
        )?;

        let (resumed, resumed_existing) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            window_start,
            metrics_window_start,
            restart_now,
        )?;

        assert_eq!(
            resumed_existing,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(
            resumed.phase,
            DiscoveryPersistedRebuildPhase::PublishPending
        );
        assert_eq!(resumed.horizon_end, checkpoint_now);
        assert_eq!(resumed.metrics_window_start, metrics_window_start);
        assert_eq!(resumed.payload.completed_snapshots.len(), 1);
        assert_eq!(
            resumed.payload.publish_pending_requested_wallet_ids,
            Some(vec!["wallet_stale".to_string()]),
            "resume repair must backfill exact publish-set ownership onto legacy publish-pending checkpoints so later flush attempts keep using the same frozen requested publish set"
        );
        assert!(
            store.load_discovery_persisted_rebuild_state()?.is_some(),
            "long same-bucket restart must keep the persisted checkpoint"
        );
        Ok(())
    }

    #[test]
    fn cached_raw_window_persisted_stream_cycle_flushes_exact_cached_wallet_ids_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-cached-persisted-stream-publication-flush.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let now = DateTime::parse_from_rfc3339("2026-04-04T16:05:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_incomplete_no_recent_published_universe".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;
        {
            let mut state = discovery
                .window_state
                .lock()
                .expect("window_state lock should succeed");
            state.last_publish_at =
                Some(now - Duration::seconds(config.refresh_seconds as i64 + 1));
            state.last_snapshot_bucket = Some(metrics_window_start);
            state.last_summary = Some(
                DiscoverySummary {
                    window_start,
                    wallets_seen: 1,
                    eligible_wallets: 1,
                    metrics_written: 1,
                    follow_promoted: 0,
                    follow_demoted: 0,
                    active_follow_wallets: 0,
                    top_wallets: vec!["wallet_publish_pending_flush".to_string()],
                    published: false,
                    ..DiscoverySummary::default()
                }
                .with_runtime_mode(DiscoveryRuntimeMode::Healthy)
                .with_scoring_source("raw_window_persisted_stream"),
            );
            state.last_exact_current_raw_truth = Some(CachedCurrentRawTruthSample {
                window_start,
                observed_swaps_loaded: 100,
                eligible_wallet_count: 1,
                top_wallet_ids: vec!["wallet_publish_pending_flush".to_string()],
            });
        }

        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert!(summary.published);
        let publication_state = store
            .discovery_publication_state()?
            .expect("publication state should exist after the cached persisted-stream flush");
        assert_eq!(
            publication_state.runtime_mode,
            DiscoveryRuntimeMode::Healthy
        );
        assert_eq!(publication_state.last_published_at, Some(now));
        assert_eq!(
            publication_state.last_published_window_start,
            Some(metrics_window_start)
        );
        assert_eq!(
            publication_state.published_wallet_ids.unwrap_or_default(),
            vec!["wallet_publish_pending_flush".to_string()],
            "cached healthy publication refresh for a completed persisted rebuild must flush the exact cached wallet ids instead of issuing a partial publication-state update that leaves runtime export on empty published_wallet_ids"
        );
        Ok(())
    }

    #[test]
    fn publish_pending_checkpoint_same_bucket_takes_precedence_over_cached_healthy_refresh_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-publish-pending-checkpoint-beats-cached-refresh.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let now = DateTime::parse_from_rfc3339("2026-04-04T16:05:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_incomplete_no_recent_published_universe".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        let mut publish_pending =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        publish_pending.phase = DiscoveryPersistedRebuildPhase::PublishPending;
        publish_pending.replay_rows_processed = 123;
        publish_pending.replay_pages_processed = 4;
        publish_pending.payload.replay_wallet_stats_complete = true;
        publish_pending
            .payload
            .replay_wallet_stats_milestone_reached = true;
        publish_pending
            .payload
            .completed_snapshots
            .push(WalletSnapshot {
                wallet_id: "wallet_from_publish_pending".to_string(),
                first_seen: window_start,
                last_seen: now,
                pnl_sol: 4.0,
                win_rate: 1.0,
                trades: 8,
                closed_trades: 8,
                hold_median_seconds: 60,
                score: 4.0,
                buy_total: 8,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
                eligible: true,
            });
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&publish_pending, now)?,
        )?;

        {
            let mut state = discovery
                .window_state
                .lock()
                .expect("window_state lock should succeed");
            state.last_publish_at =
                Some(now - Duration::seconds(config.refresh_seconds as i64 + 1));
            state.last_snapshot_bucket = Some(metrics_window_start);
            state.last_summary = Some(
                DiscoverySummary {
                    window_start,
                    wallets_seen: 1,
                    eligible_wallets: 1,
                    metrics_written: 1,
                    follow_promoted: 0,
                    follow_demoted: 0,
                    active_follow_wallets: 0,
                    top_wallets: vec!["wallet_from_cached_summary".to_string()],
                    published: false,
                    ..DiscoverySummary::default()
                }
                .with_runtime_mode(DiscoveryRuntimeMode::Healthy)
                .with_scoring_source("raw_window"),
            );
            state.last_exact_current_raw_truth = Some(CachedCurrentRawTruthSample {
                window_start,
                observed_swaps_loaded: 100,
                eligible_wallet_count: 1,
                top_wallet_ids: vec!["wallet_from_cached_summary".to_string()],
            });
        }

        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert!(summary.published);
        assert_eq!(summary.scoring_source, "raw_window_persisted_stream");
        let publication_state = store
            .discovery_publication_state()?
            .expect("publication state should exist after publish-pending flush");
        assert_eq!(publication_state.last_published_at, Some(now));
        assert_eq!(
            publication_state.last_published_window_start,
            Some(metrics_window_start)
        );
        assert_eq!(
            publication_state.published_wallet_ids.unwrap_or_default(),
            vec!["wallet_from_publish_pending".to_string()],
            "same-bucket cached healthy summaries must not shadow a persisted publish-pending checkpoint; the exact published wallet universe must flush from the checkpoint snapshots"
        );
        assert!(
            store.load_discovery_persisted_rebuild_state()?.is_none(),
            "successful publish-pending flush should clear the persisted checkpoint"
        );
        Ok(())
    }

    #[test]
    fn publish_pending_checkpoint_missing_exact_wallet_ids_stays_pending_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-publish-pending-missing-wallet-ids-stays-pending.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let now = DateTime::parse_from_rfc3339("2026-04-05T10:03:14Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;

        let previous_published_at = now - Duration::hours(4);
        let previous_published_window_start = metrics_window_start - Duration::hours(1);
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_incomplete_no_recent_published_universe".to_string(),
            last_published_at: Some(previous_published_at),
            last_published_window_start: Some(previous_published_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        let mut publish_pending =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        publish_pending.phase = DiscoveryPersistedRebuildPhase::PublishPending;
        publish_pending.replay_rows_processed = 473;
        publish_pending.replay_pages_processed = 21;
        publish_pending.payload.replay_wallet_stats_complete = true;
        publish_pending
            .payload
            .replay_wallet_stats_milestone_reached = true;
        publish_pending
            .payload
            .completed_snapshots
            .push(WalletSnapshot {
                wallet_id: "wallet_ineligible_publish_pending".to_string(),
                first_seen: window_start,
                last_seen: now,
                pnl_sol: 0.5,
                win_rate: 1.0,
                trades: 1,
                closed_trades: 1,
                hold_median_seconds: 60,
                score: 0.0,
                buy_total: 1,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
                eligible: false,
            });
        let ranked = rank_follow_candidates(
            &publish_pending.payload.completed_snapshots,
            config.min_score,
        );
        let desired_wallets = desired_wallets(&ranked, config.follow_top_n);
        assert!(
            desired_wallets.is_empty(),
            "this repro must exercise the exact publish-pending failure class where completed rebuild snapshots still do not materialize an exact non-empty published universe"
        );
        assert_eq!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(
                &publish_pending
            ),
            "publish_pending_flush",
            "old-style publish-pending checkpoints that do not yet persist exact publish-set ownership can only surface the generic flush blocker even when the derived publish set is already empty"
        );
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&publish_pending, now)?,
        )?;

        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.scoring_source, "raw_window_persisted_stream");
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert!(
            !summary.published,
            "completed persisted-stream rebuild must not report a successful publication flush when exact published wallet ids are still missing"
        );

        let retained_publish_pending = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            retained_publish_pending.phase,
            DiscoveryPersistedRebuildPhase::PublishPending
        );
        assert_eq!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(
                &retained_publish_pending
            ),
            "publish_pending_exact_publish_set_empty",
            "once exact publish-set ownership is backfilled onto the retained publish-pending checkpoint, the blocker must stop masquerading as a generic flush and instead surface that the frozen exact publish set itself is empty"
        );
        assert!(
            retained_publish_pending
                .payload
                .publish_pending_requested_wallet_ids
                .as_ref()
                .is_some_and(|wallets| wallets.is_empty()),
            "the retained publish-pending checkpoint must preserve the exact empty requested publish set instead of recomputing a generic flush attempt every cycle"
        );
        assert_eq!(
            retained_publish_pending.payload.completed_snapshots.len(),
            1,
            "publish-pending ownership must stay with the persisted rebuild checkpoint until exact publication truth materializes"
        );

        let publication_state = store
            .discovery_publication_state()?
            .expect("publication state should exist after withheld publish-pending flush");
        assert_eq!(
            publication_state.runtime_mode,
            DiscoveryRuntimeMode::FailClosed
        );
        assert_eq!(
            publication_state.reason,
            "publication_truth_withheld_missing_exact_published_wallet_ids"
        );
        assert_eq!(
            publication_state.last_published_at,
            Some(previous_published_at),
            "withheld publish-pending flush must not stamp a fresh published timestamp without an exact universe"
        );
        assert_eq!(
            publication_state.last_published_window_start,
            Some(previous_published_window_start)
        );
        assert!(
            publication_state
                .published_wallet_ids
                .as_ref()
                .is_some_and(|wallets| wallets.is_empty()),
            "withheld publish-pending flush must not inject fake published wallet ids"
        );
        assert!(
            discovery
                .runtime_publication_truth_resolution(&store, now)?
                .is_none(),
            "export-equivalent runtime truth must remain unavailable until exact publish-pending truth is really persisted"
        );
        Ok(())
    }

    #[test]
    fn cached_healthy_cycle_flushes_exact_cached_wallet_ids_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-cached-healthy-publication-flush.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_window_swaps_in_memory = 512;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let now = DateTime::parse_from_rfc3339("2026-04-04T18:05:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let (_window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 1)?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_incomplete_no_recent_published_universe".to_string(),
            last_published_at: Some(
                now - discovery.runtime_published_universe_max_age() - Duration::seconds(1),
            ),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;
        {
            let mut state = discovery
                .window_state
                .lock()
                .expect("window_state lock should succeed");
            state.last_publish_at = Some(now - Duration::seconds(60));
        }

        let first_summary = discovery.run_cycle(&store, now)?;
        assert_eq!(first_summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert!(!first_summary.published);
        assert!(
            store.load_discovery_persisted_rebuild_state()?.is_none(),
            "this repro must stay on the cached healthy path, not the persisted rebuild path"
        );

        let second_now = now + Duration::seconds(config.refresh_seconds as i64 + 1);
        let second_summary = discovery.run_cycle(&store, second_now)?;
        assert_eq!(second_summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert!(second_summary.published);
        let publication_state = store
            .discovery_publication_state()?
            .expect("publication state should exist after cached healthy publish");
        assert_eq!(publication_state.last_published_at, Some(second_now));
        assert_eq!(
            publication_state.last_published_window_start,
            Some(metrics_window_start)
        );
        assert!(
            publication_state
                .published_wallet_ids
                .as_ref()
                .is_some_and(|wallets| wallets == &vec!["wallet_top".to_string()]),
            "cached healthy publish must flush the exact cached top-wallet ids instead of issuing a partial publication-state update with published_wallet_ids=None"
        );
        Ok(())
    }
