    #[test]
    fn persisted_stream_rebuild_completion_persists_exact_zero_publish_set_before_flush_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-bounded-persisted-stream-zero-publish-set-before-flush.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-05T14:46:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = bounded_stage1_runtime_config();
        config.min_trades = 5;
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 2, 1)?;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

        let (reference_snapshots, _) = discovery
            .build_wallet_snapshots_from_persisted_stream_one_shot(&store, window_start, now)?;
        let expected_requested_wallet_ids =
            discovery.publish_pending_requested_wallet_ids_from_snapshots(&reference_snapshots);
        assert!(
            expected_requested_wallet_ids.is_empty(),
            "this reduced repro must start from a real completed rebuild shape whose exact publish set is already empty before any publish_pending flush attempt"
        );

        let rebuild_time_budget = StdDuration::from_millis(config.fetch_time_budget_ms.max(1));
        let mut completed = false;
        for idx in 0..30 {
            let cycle_now = now + Duration::minutes(idx as i64);
            match discovery.advance_persisted_stream_rebuild(
                &store,
                window_start,
                metrics_window_start,
                cycle_now,
                config.max_fetch_swaps_per_cycle.max(1),
                config.max_fetch_pages_per_cycle.max(1),
                rebuild_time_budget,
            )? {
                PersistedStreamRebuildAdvanceOutcome::Completed {
                    telemetry,
                    snapshots,
                } => {
                    assert_eq!(
                        telemetry.phase,
                        DiscoveryPersistedRebuildPhase::PublishPending
                    );
                    assert_eq!(
                        telemetry.publish_pending_requested_wallet_count, 0,
                        "completed rebuild telemetry must expose that the exact requested publish set is already empty at the PublishPending handoff"
                    );
                    assert!(
                        discovery
                            .publish_pending_requested_wallet_ids_from_snapshots(&snapshots)
                            .is_empty(),
                        "the replay-to-publish transition must not drop a non-empty publish set later; this repro requires the exact requested publish set to be empty already at completion time"
                    );
                    completed = true;
                    break;
                }
                PersistedStreamRebuildAdvanceOutcome::InProgress { .. } => {}
            }
        }
        assert!(
            completed,
            "reduced bounded rebuild should reach publish_pending within the test cutoff"
        );

        let publish_pending = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            publish_pending.phase,
            DiscoveryPersistedRebuildPhase::PublishPending
        );
        assert_eq!(
            publish_pending.payload.publish_pending_requested_wallet_ids,
            Some(Vec::new()),
            "once replay completes, the persisted publish-pending checkpoint must retain the exact empty requested publish set instead of recomputing ownership later at flush time"
        );
        assert_eq!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(
                &publish_pending
            ),
            "publish_pending_exact_publish_set_empty"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_completion_persists_exact_quality_retry_ownership_when_zero_publish_set_is_quality_blocked_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-bounded-persisted-stream-zero-publish-set-quality-blocked.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let reference_temp = tempdir().context("failed to create reference tempdir")?;
        let reference_db_path = reference_temp
            .path()
            .join("stage1-bounded-persisted-stream-zero-publish-set-quality-reference.db");
        let mut reference_store = SqliteStore::open(Path::new(&reference_db_path))?;
        reference_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-05T21:55:11Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 4, 1)?;
        let (reference_window_start, _) =
            seed_stage1_persisted_stream_runtime_fixture(&reference_store, &config, now, 4, 1)?;
        assert_eq!(reference_window_start, window_start);

        let reference_retry_mints = seed_fresh_token_quality_cache_rows_for_window_for_test(
            &reference_store,
            window_start,
            now,
        )?;
        let shadow_quality = copybot_config::ShadowConfig::default();
        let reference_discovery = DiscoveryService::new(config.clone(), shadow_quality.clone());
        let (reference_snapshots, _) = reference_discovery
            .build_wallet_snapshots_from_persisted_stream_one_shot(
                &reference_store,
                window_start,
                now,
            )?;
        let expected_wallet_ids = reference_discovery
            .publish_pending_requested_wallet_ids_from_snapshots(&reference_snapshots);
        assert_eq!(
            expected_wallet_ids,
            vec!["wallet_top".to_string()],
            "with exact fresh token-quality truth available, this reduced fixture must publish the profitable top wallet"
        );

        let discovery = DiscoveryService::new(config.clone(), shadow_quality);
        let rebuild_time_budget = StdDuration::from_millis(config.fetch_time_budget_ms.max(1));
        let mut completed = false;
        for idx in 0..30 {
            let cycle_now = now + Duration::minutes(idx as i64);
            match discovery.advance_persisted_stream_rebuild(
                &store,
                window_start,
                metrics_window_start,
                cycle_now,
                config.max_fetch_swaps_per_cycle.max(1),
                config.max_fetch_pages_per_cycle.max(1),
                rebuild_time_budget,
            )? {
                PersistedStreamRebuildAdvanceOutcome::Completed { telemetry, .. } => {
                    assert_eq!(
                        telemetry.phase,
                        DiscoveryPersistedRebuildPhase::PublishPending
                    );
                    assert_eq!(telemetry.publish_pending_requested_wallet_count, 0);
                    completed = true;
                    break;
                }
                PersistedStreamRebuildAdvanceOutcome::InProgress { .. } => {}
            }
        }
        assert!(
            completed,
            "quality-blocked reduced rebuild must still reach a retained publish-pending checkpoint within the test cutoff"
        );

        let publish_pending = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            publish_pending.phase,
            DiscoveryPersistedRebuildPhase::PublishPending
        );
        assert_eq!(
            publish_pending.payload.publish_pending_requested_wallet_ids,
            Some(Vec::new()),
            "the exact requested publish set must still be empty before the later exact quality retry runs"
        );
        assert_eq!(
            publish_pending.payload.publish_pending_quality_retry_mints,
            Some(reference_retry_mints),
            "when replay would otherwise enter PublishPending with an exact zero publish set that is causally explained by unresolved token-quality debt, the checkpoint must now retain the exact mint set that has to be re-resolved before a real publish can exist"
        );
        assert_eq!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(
                &publish_pending
            ),
            "publish_pending_exact_publish_set_quality_unresolved"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_completion_persists_exact_quality_retry_ownership_when_zero_publish_set_is_tradable_ratio_blocked_by_unresolved_quality_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join(
            "stage1-bounded-persisted-stream-zero-publish-set-tradable-ratio-quality-blocked.db",
        );
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let reference_temp = tempdir().context("failed to create reference tempdir")?;
        let reference_db_path = reference_temp.path().join(
            "stage1-bounded-persisted-stream-zero-publish-set-tradable-ratio-quality-reference.db",
        );
        let mut reference_store = SqliteStore::open(Path::new(&reference_db_path))?;
        reference_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-05T22:25:14Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = bounded_stage1_runtime_config();
        config.min_tradable_ratio = 0.5;
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 4, 1)?;
        let (reference_window_start, _) =
            seed_stage1_persisted_stream_runtime_fixture(&reference_store, &config, now, 4, 1)?;
        assert_eq!(reference_window_start, window_start);

        let reference_retry_mints = seed_fresh_token_quality_cache_rows_for_window_for_test(
            &reference_store,
            window_start,
            now,
        )?;
        let partially_resolved_mints: Vec<String> = reference_retry_mints
            .iter()
            .filter(|mint| mint.contains("Top00"))
            .cloned()
            .collect();
        assert_eq!(
            partially_resolved_mints.len(),
            1,
            "the reduced fixture should expose exactly one profitable mint to seed as already fresh quality truth"
        );
        seed_fresh_token_quality_cache_rows_for_mints_for_test(
            &store,
            &partially_resolved_mints,
            now,
        )?;
        let expected_retry_mints: Vec<String> = reference_retry_mints
            .iter()
            .filter(|mint| !partially_resolved_mints.contains(mint))
            .cloned()
            .collect();

        let shadow_quality = copybot_config::ShadowConfig::default();
        let reference_discovery = DiscoveryService::new(config.clone(), shadow_quality.clone());
        let (reference_snapshots, _) = reference_discovery
            .build_wallet_snapshots_from_persisted_stream_one_shot(
                &reference_store,
                window_start,
                now,
            )?;
        let expected_wallet_ids = reference_discovery
            .publish_pending_requested_wallet_ids_from_snapshots(&reference_snapshots);
        assert_eq!(
            expected_wallet_ids,
            vec!["wallet_top".to_string()],
            "with exact fresh token-quality truth available, the tradable-ratio-gated reduced fixture must still publish the profitable top wallet"
        );

        let discovery = DiscoveryService::new(config.clone(), shadow_quality);
        let rebuild_time_budget = StdDuration::from_millis(config.fetch_time_budget_ms.max(1));
        let mut completed = false;
        for idx in 0..30 {
            let cycle_now = now + Duration::minutes(idx as i64);
            match discovery.advance_persisted_stream_rebuild(
                &store,
                window_start,
                metrics_window_start,
                cycle_now,
                config.max_fetch_swaps_per_cycle.max(1),
                config.max_fetch_pages_per_cycle.max(1),
                rebuild_time_budget,
            )? {
                PersistedStreamRebuildAdvanceOutcome::Completed { telemetry, .. } => {
                    assert_eq!(
                        telemetry.phase,
                        DiscoveryPersistedRebuildPhase::PublishPending
                    );
                    assert_eq!(telemetry.publish_pending_requested_wallet_count, 0);
                    completed = true;
                    break;
                }
                PersistedStreamRebuildAdvanceOutcome::InProgress { .. } => {}
            }
        }
        assert!(
            completed,
            "tradable-ratio-blocked reduced rebuild must still reach a retained publish-pending checkpoint within the test cutoff"
        );

        let publish_pending = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            publish_pending.phase,
            DiscoveryPersistedRebuildPhase::PublishPending
        );
        let wallet_top_snapshot = publish_pending
            .payload
            .completed_snapshots
            .iter()
            .find(|snapshot| snapshot.wallet_id == "wallet_top")
            .expect("completed snapshots should include wallet_top in the tradable-ratio repro");
        assert!(
            wallet_top_snapshot.tradable_ratio > 0.0
                && wallet_top_snapshot.tradable_ratio < config.min_tradable_ratio,
            "this repro must specifically cover the review-found shape where unresolved quality keeps the publish set empty through a non-zero tradable_ratio below min_tradable_ratio, not through a zero tradable_ratio shortcut"
        );
        assert_eq!(
            publish_pending.payload.publish_pending_requested_wallet_ids,
            Some(Vec::new()),
            "the exact requested publish set must still be empty before the later exact quality retry runs"
        );
        assert_eq!(
            publish_pending.payload.publish_pending_quality_retry_mints,
            Some(expected_retry_mints),
            "when replay would otherwise enter PublishPending with an exact zero publish set because unresolved quality pushes tradable_ratio below min_tradable_ratio, the checkpoint must retain the unresolved mint set instead of treating that zero publish set as exact final truth"
        );
        assert_eq!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(
                &publish_pending
            ),
            "publish_pending_exact_publish_set_quality_unresolved"
        );
        Ok(())
    }

    #[test]
    fn legacy_zero_publish_set_publish_pending_checkpoint_retries_exact_quality_and_publishes_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-publish-pending-quality-retry-publishes.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-05T21:55:11Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 4, 1)?;
        let shadow_quality = copybot_config::ShadowConfig::default();
        let discovery = DiscoveryService::new(config.clone(), shadow_quality.clone());
        let rebuild_time_budget = StdDuration::from_millis(config.fetch_time_budget_ms.max(1));

        let mut completed = false;
        for idx in 0..30 {
            let cycle_now = now + Duration::minutes(idx as i64);
            match discovery.advance_persisted_stream_rebuild(
                &store,
                window_start,
                metrics_window_start,
                cycle_now,
                config.max_fetch_swaps_per_cycle.max(1),
                config.max_fetch_pages_per_cycle.max(1),
                rebuild_time_budget,
            )? {
                PersistedStreamRebuildAdvanceOutcome::Completed { .. } => {
                    completed = true;
                    break;
                }
                PersistedStreamRebuildAdvanceOutcome::InProgress { .. } => {}
            }
        }
        assert!(completed);

        let mut legacy_publish_pending = load_persisted_stream_rebuild_state_for_test(&store)?;
        legacy_publish_pending
            .payload
            .publish_pending_quality_retry_mints = None;
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&legacy_publish_pending, now)?,
        )?;

        let (repaired, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            window_start,
            metrics_window_start,
            now + Duration::minutes(1),
        )?;
        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(
            repaired.phase,
            DiscoveryPersistedRebuildPhase::PublishPending
        );
        assert!(
            repaired
                .payload
                .publish_pending_quality_retry_mints
                .as_ref()
                .is_some_and(|mints| !mints.is_empty()),
            "resume repair must backfill exact quality-retry ownership onto the retained zero-wallet publish-pending checkpoint so the next cycle can retry exact quality truth instead of looping forever on an empty requested publish set"
        );
        assert_eq!(
            DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state(&repaired),
            "publish_pending_exact_publish_set_quality_unresolved"
        );

        let expected_wallet_ids = {
            let reference_temp = tempdir().context("failed to create publish reference tempdir")?;
            let reference_db_path = reference_temp
                .path()
                .join("stage1-publish-pending-quality-retry-reference.db");
            let mut reference_store = SqliteStore::open(Path::new(&reference_db_path))?;
            reference_store.run_migrations(&migration_dir)?;
            let (reference_window_start, _) =
                seed_stage1_persisted_stream_runtime_fixture(&reference_store, &config, now, 4, 1)?;
            assert_eq!(reference_window_start, window_start);
            seed_fresh_token_quality_cache_rows_for_window_for_test(
                &reference_store,
                window_start,
                now + Duration::minutes(1),
            )?;
            let reference_discovery = DiscoveryService::new(config.clone(), shadow_quality.clone());
            let (reference_snapshots, _) = reference_discovery
                .build_wallet_snapshots_from_persisted_stream_one_shot(
                    &reference_store,
                    window_start,
                    now + Duration::minutes(1),
                )?;
            reference_discovery
                .publish_pending_requested_wallet_ids_from_snapshots(&reference_snapshots)
        };
        assert_eq!(expected_wallet_ids, vec!["wallet_top".to_string()]);

        seed_fresh_token_quality_cache_rows_for_window_for_test(
            &store,
            window_start,
            now + Duration::minutes(1),
        )?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_missing_exact_published_wallet_ids".to_string(),
            last_published_at: Some(now - Duration::hours(2)),
            last_published_window_start: Some(metrics_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        let mut published_summary = None;
        for idx in 1..=12 {
            let cycle_now = now + Duration::minutes(idx as i64);
            let summary = discovery.run_cycle(&store, cycle_now)?;
            if summary.published {
                published_summary = Some(summary);
                break;
            }
        }
        let summary = published_summary.expect(
            "once exact fresh token-quality truth arrives for the retained zero-wallet publish-pending checkpoint, the retry path should eventually rebuild a real non-empty publish set and publish it",
        );
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert!(summary.published);

        let publication_state = store
            .discovery_publication_state()?
            .expect("publication state should exist after exact publish truth materializes");
        assert_eq!(
            publication_state.runtime_mode,
            DiscoveryRuntimeMode::Healthy
        );
        assert_eq!(
            publication_state.published_wallet_ids.unwrap_or_default(),
            expected_wallet_ids,
            "after exact quality retry replays the completed lineage with real fresh truth, publication must persist the same exact wallet universe that one-shot semantics would select"
        );
        assert!(
            store.load_discovery_persisted_rebuild_state()?.is_none(),
            "once exact publication truth materializes after the publish-pending quality retry, the retained checkpoint must finally clear"
        );
        Ok(())
    }
