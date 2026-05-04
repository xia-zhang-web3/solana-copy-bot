    #[test]
    fn cached_cycle_exact_empty_publication_clears_stale_published_truth_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-cached-exact-empty-publication-clears-stale-truth.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_window_swaps_in_memory = 512;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let now = DateTime::parse_from_rfc3339("2026-04-24T18:05:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 1)?;

        let stale_published_at = DateTime::parse_from_rfc3339("2026-04-06T17:55:23Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let stale_wallet_ids = (0..7)
            .map(|idx| format!("wallet_stale_published_{idx}"))
            .collect::<Vec<_>>();
        let stale_publish = discovery.persist_publication_state(
            &store,
            DiscoveryRuntimeMode::Healthy,
            true,
            metrics_window_start - Duration::hours(1),
            Some(&stale_wallet_ids),
            "raw_window",
            "seed_stale_publication_truth",
            stale_published_at,
        )?;
        assert!(stale_publish.published_universe_persisted);
        let stale_freshness_captured_at = stale_published_at + Duration::seconds(1);
        let stale_freshness_write = discovery
            .wallet_freshness_capture_snapshot_from_precomputed_current_raw(
                &store,
                stale_freshness_captured_at,
                PrecomputedWalletFreshnessCurrentRawTruth {
                    window_start,
                    observed_swaps_loaded: 45_000,
                    eligible_wallet_count: stale_wallet_ids.len(),
                    top_wallet_ids: stale_wallet_ids.clone(),
                },
                Some(60),
            )?
            .snapshot
            .to_storage_write()?;
        let stale_freshness_capture =
            store.append_discovery_wallet_freshness_capture(&stale_freshness_write)?;
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
                    wallets_seen: 7_715,
                    eligible_wallets: 0,
                    metrics_written: 0,
                    follow_promoted: 0,
                    follow_demoted: 0,
                    active_follow_wallets: 0,
                    top_wallets: stale_wallet_ids.clone(),
                    published: false,
                    ..DiscoverySummary::default()
                }
                .with_runtime_mode(DiscoveryRuntimeMode::FailClosed)
                .with_scoring_source("raw_window"),
            );
            state.last_exact_current_raw_truth = Some(CachedCurrentRawTruthSample {
                window_start,
                observed_swaps_loaded: 45_771_784,
                eligible_wallet_count: 0,
                top_wallet_ids: Vec::new(),
            });
        }

        let summary = discovery.run_cycle(&store, now)?;
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(summary.scoring_source, "raw_window");
        assert_eq!(
            summary.wallet_freshness_capture_state,
            Some("persisted_zero_universe_evidence"),
            "cached exact-empty zero-universe evidence must use the dedicated cheap writer, not the generic wallet freshness audit path"
        );
        assert_eq!(summary.wallet_freshness_capture_reason, None);
        assert!(
            !summary.published,
            "exact empty current_raw truth must not report a healthy publish"
        );
        assert!(
            summary.top_wallets.is_empty(),
            "cached exact empty raw-window truth must not keep stale summary top wallets"
        );
        assert_eq!(summary.eligible_wallets, 0);
        assert_eq!(
            store.list_active_follow_wallets()?,
            HashSet::<String>::new(),
            "exact empty raw-window truth must not activate the stale published followlist"
        );

        let captures = store.list_discovery_wallet_freshness_captures(5)?;
        assert_eq!(captures.len(), 2);
        let latest_capture_row = captures
            .first()
            .expect("fresh zero-universe capture should be first");
        assert_eq!(
            summary.wallet_freshness_capture_id,
            Some(latest_capture_row.capture_id)
        );
        assert_ne!(
            latest_capture_row.capture_id, stale_freshness_capture.capture_id,
            "fresh zero-universe evidence must supersede the stale freshness capture"
        );
        assert!(
            latest_capture_row.captured_at > stale_freshness_capture.captured_at,
            "zero-universe evidence must be a current freshness capture"
        );
        assert!(!latest_capture_row.raw_truth_sufficient);
        assert_eq!(latest_capture_row.verdict, "insufficient_raw_truth");
        assert_eq!(
            latest_capture_row.reason,
            RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON
        );
        assert_eq!(
            latest_capture_row.raw_truth_reason,
            RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON
        );
        assert_eq!(
            latest_capture_row.shadow_signal_verdict,
            "no_selected_wallets"
        );
        assert_eq!(
            latest_capture_row.shadow_signal_reason,
            "no_active_follow_wallets_selected"
        );
        assert_eq!(
            latest_capture_row.current_raw_top_wallet_ids,
            Vec::<String>::new()
        );
        assert_eq!(
            latest_capture_row.active_follow_wallet_ids,
            Vec::<String>::new()
        );
        assert_eq!(
            latest_capture_row.published_wallet_ids,
            Vec::<String>::new()
        );
        let latest_capture = wallet_freshness_capture_from_row(latest_capture_row.clone())?;
        assert_eq!(
            latest_capture.audit.verdict.as_str(),
            "insufficient_raw_truth"
        );
        assert_eq!(
            latest_capture.audit.reason,
            RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON
        );
        assert!(!latest_capture.audit.raw_truth.sufficient);
        assert_eq!(
            latest_capture.audit.raw_truth.reason,
            RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON
        );
        assert_eq!(
            latest_capture.audit.raw_truth.observed_swaps_loaded,
            45_771_784
        );
        assert_eq!(latest_capture.audit.raw_truth.eligible_wallet_count, 0);
        assert_eq!(latest_capture.audit.raw_truth.top_wallet_count, 0);
        assert_eq!(
            latest_capture.audit.current_raw_top_wallet_ids,
            Vec::<String>::new()
        );
        assert_eq!(
            latest_capture.audit.active_follow_wallet_ids,
            Vec::<String>::new()
        );
        assert_eq!(
            latest_capture.audit.published_wallet_ids,
            Vec::<String>::new()
        );
        assert_eq!(
            latest_capture.shadow_signal.verdict.as_str(),
            "no_selected_wallets"
        );
        assert_eq!(
            latest_capture.shadow_signal.reason,
            "no_active_follow_wallets_selected"
        );
        assert_eq!(latest_capture.shadow_signal.selected_wallet_count, 0);
        let latest_audit_json: serde_json::Value =
            serde_json::from_str(&latest_capture_row.audit_json)?;
        assert_eq!(
            latest_audit_json
                .pointer("/raw_truth/wallets_seen")
                .and_then(serde_json::Value::as_u64),
            Some(7_715)
        );

        let publication_state = store
            .discovery_publication_state_read_only()?
            .expect("publication state should exist after exact empty cached cycle");
        assert_eq!(
            publication_state.runtime_mode,
            DiscoveryRuntimeMode::FailClosed
        );
        assert_eq!(
            publication_state.reason,
            RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON
        );
        assert_eq!(
            publication_state.published_scoring_source.as_deref(),
            Some("raw_window")
        );
        assert_eq!(
            publication_state.last_published_at, None,
            "exact empty current_raw truth must clear the stale last_published_at"
        );
        assert_eq!(
            publication_state.last_published_window_start, None,
            "exact empty current_raw truth must clear the stale publication window"
        );
        assert!(
            publication_state
                .published_wallet_ids
                .as_ref()
                .map_or(true, Vec::is_empty),
            "exact empty current_raw truth must clear stale published wallet ids"
        );
        assert_ne!(
            publication_state.published_wallet_ids.unwrap_or_default(),
            stale_wallet_ids,
            "cached exact empty raw-window truth must not carry forward stale wallet ids"
        );
        assert!(
            discovery
                .runtime_publication_truth_resolution(&store, now)?
                .is_none(),
            "fail-closed exact empty publication state must not resolve as production-ready truth"
        );
        Ok(())
    }

    #[test]
    fn persist_publication_state_refuses_healthy_write_while_incomplete_replay_checkpoint_exists_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-healthy-publication-withheld-incomplete-replay.db");
        let mut runtime_store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        runtime_store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-04T19:06:37Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        config.max_fetch_swaps_per_cycle = 10;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let (replay_state, metrics_window_start, _) =
            seed_stage1_partial_sol_leg_replay_checkpoint_fixture(
                &runtime_store,
                &discovery,
                &config,
                now,
                64,
                12,
            )?;
        runtime_store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&replay_state, now)?,
        )?;

        let previous_published_at = now - Duration::hours(2);
        let previous_published_window_start = metrics_window_start - Duration::hours(1);
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_incomplete_no_recent_published_universe".to_string(),
            last_published_at: Some(previous_published_at),
            last_published_window_start: Some(previous_published_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        let requested_wallets = vec!["wallet_should_not_publish".to_string()];
        let boundary = discovery.snapshot_run_cycle_publication_boundary_diagnostics(
            &runtime_store,
            "persisted_recompute",
            true,
            false,
        )?;
        assert!(boundary.persisted_rebuild_checkpoint_exists);
        assert!(boundary.replay_incomplete);
        assert_eq!(boundary.persisted_rebuild_phase, Some("replay"));
        assert_eq!(
            boundary.publishable_checkpoint_blocker,
            Some("replay_sol_leg_incomplete")
        );
        assert!(!boundary.persist_publication_state_called);

        let outcome = discovery.persist_publication_state(
            &runtime_store,
            DiscoveryRuntimeMode::Healthy,
            true,
            metrics_window_start,
            Some(&requested_wallets),
            "raw_window_persisted_stream",
            "discovery_score_refresh",
            now,
        )?;
        assert_eq!(outcome.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert!(!outcome.published_universe_persisted);
        assert!(outcome.write_attempted);
        assert!(outcome.healthy_publish_refused);
        assert!(outcome.carry_forward_happened);
        assert_eq!(
            outcome.effective_reason,
            "publication_truth_withheld_while_replay_sol_leg_incomplete"
        );

        let publication_state = runtime_store
            .discovery_publication_state()?
            .expect("publication state should exist after withheld healthy write");
        assert_eq!(
            publication_state.runtime_mode,
            DiscoveryRuntimeMode::FailClosed
        );
        assert_eq!(
            publication_state.reason,
            "publication_truth_withheld_while_replay_sol_leg_incomplete"
        );
        assert_eq!(
            publication_state.last_published_at,
            Some(previous_published_at),
            "withheld healthy publication writes must not stamp a fresh published timestamp while replay is still incomplete"
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
            "withheld healthy publication writes must not inject a fake published universe while replay is still incomplete"
        );
        assert!(
            discovery
                .runtime_publication_truth_resolution(&runtime_store, now)?
                .is_none(),
            "export-equivalent runtime truth must remain unavailable while the persisted replay checkpoint is still incomplete"
        );
        Ok(())
    }

    #[test]
    fn persist_publication_state_refuses_healthy_write_without_exact_wallet_ids_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-healthy-publication-withheld-missing-wallet-ids.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let now = DateTime::parse_from_rfc3339("2026-04-04T19:06:37Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let (_window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;

        let previous_published_at = now - Duration::hours(3);
        let previous_published_window_start = metrics_window_start - Duration::hours(1);
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_incomplete_no_recent_published_universe".to_string(),
            last_published_at: Some(previous_published_at),
            last_published_window_start: Some(previous_published_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        let empty_wallets: Vec<String> = Vec::new();
        let outcome = discovery.persist_publication_state(
            &store,
            DiscoveryRuntimeMode::Healthy,
            true,
            metrics_window_start,
            Some(&empty_wallets),
            "raw_window",
            "discovery_score_refresh",
            now,
        )?;
        assert_eq!(outcome.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert!(!outcome.published_universe_persisted);
        assert!(outcome.write_attempted);
        assert!(outcome.healthy_publish_refused);
        assert!(outcome.carry_forward_happened);
        assert_eq!(
            outcome.effective_reason,
            "publication_truth_withheld_missing_exact_published_wallet_ids"
        );

        let publication_state = store
            .discovery_publication_state()?
            .expect("publication state should exist after withheld empty-wallet healthy write");
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
            Some(previous_published_at)
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
            "withheld healthy publication writes must preserve the empty fail-closed universe instead of persisting a bogus healthy row"
        );
        assert!(
            discovery
                .runtime_publication_truth_resolution(&store, now)?
                .is_none(),
            "healthy publication truth must stay unavailable until exact non-empty wallet ids exist"
        );
        Ok(())
    }

    #[test]
    fn persist_publication_state_missing_exact_wallet_ids_carries_forward_stale_complete_row_and_refreshes_updated_at_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-withheld-missing-wallet-ids-carries-forward-stale-complete-row.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 3_600;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let stale_publish_at = DateTime::parse_from_rfc3339("2026-04-06T17:55:23Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let now = DateTime::parse_from_rfc3339("2026-04-12T15:53:32Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let metrics_window_start = metrics_window_start_for_test(&config, stale_publish_at);
        let stale_published_wallets =
            seed_published_wallet_metrics_snapshot(&store, metrics_window_start, 7, 7)?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "seed_stale_complete_publication_truth".to_string(),
            last_published_at: Some(stale_publish_at),
            last_published_window_start: Some(metrics_window_start),
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: Some(stale_published_wallets.iter().cloned().collect()),
        })?;
        let before = store
            .discovery_publication_state_read_only()?
            .expect("publication state should exist before withheld write");

        let empty_wallets: Vec<String> = Vec::new();
        let outcome = discovery.persist_publication_state(
            &store,
            DiscoveryRuntimeMode::Healthy,
            true,
            metrics_window_start,
            Some(&empty_wallets),
            "raw_window_persisted_stream",
            "discovery_score_refresh",
            now,
        )?;
        assert_eq!(outcome.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert!(!outcome.published_universe_persisted);
        assert!(outcome.write_attempted);
        assert!(outcome.healthy_publish_refused);
        assert!(outcome.carry_forward_happened);
        assert_eq!(
            outcome.effective_reason,
            "publication_truth_withheld_missing_exact_published_wallet_ids"
        );

        let after = store
            .discovery_publication_state_read_only()?
            .expect("publication state should exist after withheld write");
        assert_eq!(after.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(
            after.reason,
            "publication_truth_withheld_missing_exact_published_wallet_ids"
        );
        assert!(
            after.updated_at > before.updated_at,
            "the live-like fail-closed row must be stale-but-updating rather than completely frozen"
        );
        assert_eq!(after.last_published_at, Some(stale_publish_at));
        assert_eq!(
            after.last_published_window_start,
            Some(metrics_window_start)
        );
        assert_eq!(
            after.published_wallet_ids.clone().unwrap_or_default().len(),
            7,
            "the live-like published_wallet_count=7 comes from the prior persisted row being carried forward, not from a fresh exact universe write on the fail-closed path"
        );
        assert!(after.has_complete_publication_truth());
        assert!(
            !after.is_fresh_under_gate(discovery.publication_freshness_gate(), now),
            "the carried-forward row must still fail the freshness gate at the live timestamp"
        );
        assert!(
            discovery.runtime_publication_truth_resolution(&store, now)?.is_none(),
            "runtime truth must still stay unavailable even though the carried-forward stale row remains structurally complete"
        );
        Ok(())
    }
