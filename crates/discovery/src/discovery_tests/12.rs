    #[test]
    fn persisted_stream_rebuild_publish_failure_keeps_publish_pending_checkpoint_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-persisted-stream-publish-pending.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let mut publish_pending =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        publish_pending.phase = DiscoveryPersistedRebuildPhase::PublishPending;
        publish_pending.replay_rows_processed = 100;
        publish_pending.replay_pages_processed = 5;
        publish_pending.payload.replay_wallet_stats_complete = true;
        publish_pending
            .payload
            .replay_wallet_stats_milestone_reached = true;
        publish_pending
            .payload
            .completed_snapshots
            .push(WalletSnapshot {
                wallet_id: "wallet_publish_failure".to_string(),
                first_seen: window_start,
                last_seen: now,
                pnl_sol: 2.0,
                win_rate: 1.0,
                trades: 6,
                closed_trades: 6,
                hold_median_seconds: 60,
                score: 2.0,
                buy_total: 6,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
                eligible: true,
            });
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&publish_pending, now)?,
        )?;

        let conn = Connection::open(Path::new(&db_path))?;
        conn.execute_batch(
            "CREATE TRIGGER fail_wallet_metrics_insert
             BEFORE INSERT ON wallet_metrics
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = discovery
            .run_cycle(&store, now + Duration::minutes(1))
            .expect_err("publish failure should bubble up");
        assert!(
            format!("{error:#}").contains("disk I/O error"),
            "unexpected publish failure: {error:#}"
        );

        let publish_pending_after_failure = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            publish_pending_after_failure.phase,
            DiscoveryPersistedRebuildPhase::PublishPending
        );
        assert_eq!(
            publish_pending_after_failure.replay_rows_processed,
            publish_pending.replay_rows_processed
        );
        assert_eq!(
            publish_pending_after_failure.window_start,
            publish_pending.window_start
        );
        assert_eq!(
            publish_pending_after_failure.metrics_window_start,
            publish_pending.metrics_window_start
        );

        conn.execute_batch("DROP TRIGGER fail_wallet_metrics_insert;")?;
        let summary = discovery.run_cycle(&store, now + Duration::minutes(2))?;
        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(summary.scoring_source, "raw_window_persisted_stream");
        assert!(
            store.load_discovery_persisted_rebuild_state()?.is_none(),
            "successful publish must clear the publish-pending checkpoint"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_carries_forward_partial_collect_buy_mints_across_metrics_bucket_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("stage1-persisted-stream-stale-bucket.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let checkpoint_now = DateTime::parse_from_rfc3339("2026-03-17T12:00:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let rollover_now = checkpoint_now + Duration::seconds(20);
        let window_start =
            checkpoint_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, checkpoint_now);
        let next_window_start =
            rollover_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let next_metrics_window_start = metrics_window_start_for_test(&config, rollover_now);

        let token_old_only = "TokenStage1CarryForwardAOldOnly111111111111".to_string();
        let token_survives = "TokenStage1CarryForwardBSurvives1111111111".to_string();
        let token_future_c = "TokenStage1CarryForwardCFuture11111111111".to_string();
        let token_future_d = "TokenStage1CarryForwardDFuture11111111111".to_string();
        let token_new_tail_before_cursor =
            "TokenStage1CarryForwardAANewTail1111111111111".to_string();

        for (idx, (token, ts)) in [
            (token_old_only.as_str(), window_start + Duration::seconds(5)),
            (
                token_survives.as_str(),
                window_start + Duration::seconds(70),
            ),
            (
                token_future_c.as_str(),
                window_start + Duration::seconds(80),
            ),
            (
                token_future_d.as_str(),
                window_start + Duration::seconds(90),
            ),
            (
                token_new_tail_before_cursor.as_str(),
                checkpoint_now + Duration::seconds(5),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            store.insert_observed_swap(&swap(
                "wallet_carry_forward",
                &format!("stage1-carry-forward-buy-{idx}"),
                ts,
                SOL_MINT,
                token,
                0.5,
                50.0,
            ))?;
        }

        let mut stale_state = discovery.start_persisted_stream_rebuild_state(
            window_start,
            metrics_window_start,
            checkpoint_now,
        );
        stale_state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        stale_state.prepass_rows_processed = 2;
        stale_state.prepass_pages_processed = 1;
        stale_state.payload.unique_buy_mints = vec![token_old_only.clone(), token_survives.clone()];
        stale_state.payload.buy_mint_counts =
            BTreeMap::from([(token_old_only.clone(), 1), (token_survives.clone(), 1)]);
        stale_state.payload.collect_buy_mints_cursor_token = Some(token_survives.clone());
        stale_state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&stale_state, stale_state.horizon_end)?,
        )?;

        let (mut carried, resumed_existing) = discovery
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                next_window_start,
                next_metrics_window_start,
                rollover_now,
            )?;
        assert_eq!(
            resumed_existing,
            PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow
        );
        assert_eq!(
            carried.phase,
            DiscoveryPersistedRebuildPhase::CollectBuyMints
        );
        assert_eq!(carried.window_start, next_window_start);
        assert_eq!(carried.metrics_window_start, next_metrics_window_start);
        assert_eq!(carried.horizon_end, rollover_now);
        assert_eq!(
            carried.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileExpiredHead
        );
        assert_eq!(
            carried.payload.collect_buy_mints_cursor_token.as_deref(),
            Some(token_survives.as_str())
        );

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut carried,
            1,
            10,
            None,
            Instant::now() + StdDuration::from_secs(1),
        )?;
        assert!(phase_advance.source_exhausted);
        assert_eq!(
            carried.payload.unique_buy_mints,
            store.load_observed_buy_mints_in_window(next_window_start, rollover_now)?,
            "carry-forward reconciliation must produce the exact canonical target-window mint set before token-quality resolution"
        );
        assert_eq!(
            carried.payload.unique_buy_mints,
            vec![
                token_new_tail_before_cursor,
                token_survives,
                token_future_c,
                token_future_d,
            ]
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_expired_head_stale_bucket_resumes_until_exact_checkpoint_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-expired-head-stale-bucket-resume.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-18T18:00:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_one_now = source_now + Duration::seconds(60);
        let target_two_now = target_one_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_one_window_start =
            target_one_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_one_metrics_window_start =
            metrics_window_start_for_test(&config, target_one_now);
        let target_two_window_start =
            target_two_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_two_metrics_window_start =
            metrics_window_start_for_test(&config, target_two_now);

        let token_expired_a = "TokenStage1StaleExpiredHeadA111111111111".to_string();
        let token_expired_b = "TokenStage1StaleExpiredHeadB111111111111".to_string();
        let token_survives = "TokenStage1StaleExpiredHeadSurvive11111".to_string();
        let token_new_tail = "TokenStage1StaleExpiredHeadNewTail11111".to_string();
        for (idx, (token, ts)) in [
            (
                token_expired_a.as_str(),
                source_window_start + Duration::seconds(5),
            ),
            (
                token_expired_b.as_str(),
                source_window_start + Duration::seconds(6),
            ),
            (
                token_survives.as_str(),
                target_one_window_start + Duration::seconds(5),
            ),
            (token_new_tail.as_str(), source_now + Duration::seconds(5)),
        ]
        .into_iter()
        .enumerate()
        {
            store.insert_observed_swap(&swap(
                "wallet_stage1_reconcile_expired",
                &format!("stage1-stale-expired-head-buy-{idx}"),
                ts,
                SOL_MINT,
                token,
                1.0,
                10.0,
            ))?;
        }

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.unique_buy_mints = vec![
            token_expired_a.clone(),
            token_expired_b.clone(),
            token_survives.clone(),
        ];
        state.payload.buy_mint_counts = BTreeMap::from([
            (token_expired_a, 1),
            (token_expired_b.clone(), 1),
            (token_survives.clone(), 1),
        ]);
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_one_window_start,
                target_one_metrics_window_start,
                target_one_now,
            )?
        );

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            1,
            1,
            None,
            Instant::now() + StdDuration::from_secs(1),
        )?;
        state.prepass_rows_processed = state
            .prepass_rows_processed
            .saturating_add(phase_advance.rows_processed);
        state.prepass_pages_processed = state
            .prepass_pages_processed
            .saturating_add(phase_advance.pages_processed);
        state.payload.collect_buy_mints_cursor_token =
            phase_advance.collect_buy_mints_cursor_token.clone();
        assert_eq!(
            state.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileExpiredHead
        );
        assert_eq!(
            state.payload.unique_buy_mints,
            vec![token_expired_b.clone(), token_survives.clone()],
            "partial stale expired-head reconcile should keep exact canonical membership incrementally without a full counts->vector rebuild"
        );
        assert_eq!(
            state.payload.buy_mint_counts.keys().cloned().collect::<Vec<_>>(),
            state.payload.unique_buy_mints,
            "authoritative buy-mint counts and resumable unique mint prefix must stay aligned after a partial stale reconcile page"
        );
        assert_sorted_strings(&state.payload.unique_buy_mints);
        assert!(
            DiscoveryService::state_can_resume_stale_metrics_window_until_exact_checkpoint(&state),
            "partial stale expired-head reconcile must remain eligible for stale-resume on the next bucket rollover"
        );
        assert!(state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor_token
            .is_some());
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, target_one_now)?,
        )?;

        let (resumed, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            target_two_window_start,
            target_two_metrics_window_start,
            target_two_now,
        )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
        );
        assert_eq!(
            resumed.metrics_window_start,
            target_one_metrics_window_start
        );
        assert_eq!(
            resumed.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileExpiredHead
        );
        assert_eq!(
            resumed
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor_token,
            state
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor_token
        );
        assert_eq!(resumed.prepass_rows_processed, state.prepass_rows_processed);
        Ok(())
    }

    #[test]
    fn persisted_stream_stale_reconcile_membership_stays_sorted_and_exact_without_full_resync_stage1(
    ) -> Result<()> {
        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let now = DateTime::parse_from_rfc3339("2026-03-18T22:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);

        let mut state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileExpiredHead;
        state.payload.collect_buy_mints_prepass_complete = true;
        for mint in [
            "TokenStage1IncrementalExactC1111111111111",
            "TokenStage1IncrementalExactA1111111111111",
            "TokenStage1IncrementalExactB1111111111111",
        ] {
            assert!(DiscoveryService::add_buy_mint_occurrences(
                &mut state.payload,
                mint,
                1,
            ));
        }
        assert_eq!(
            state.payload.unique_buy_mints,
            vec![
                "TokenStage1IncrementalExactA1111111111111".to_string(),
                "TokenStage1IncrementalExactB1111111111111".to_string(),
                "TokenStage1IncrementalExactC1111111111111".to_string(),
            ]
        );

        DiscoveryService::subtract_buy_mint_occurrences(
            &mut state.payload,
            "TokenStage1IncrementalExactB1111111111111",
            1,
        );
        assert_eq!(
            state.payload.unique_buy_mints,
            vec![
                "TokenStage1IncrementalExactA1111111111111".to_string(),
                "TokenStage1IncrementalExactC1111111111111".to_string(),
            ]
        );

        assert!(DiscoveryService::add_buy_mint_occurrences(
            &mut state.payload,
            "TokenStage1IncrementalExactB1111111111111",
            2,
        ));
        assert_eq!(
            state.payload.unique_buy_mints,
            vec![
                "TokenStage1IncrementalExactA1111111111111".to_string(),
                "TokenStage1IncrementalExactB1111111111111".to_string(),
                "TokenStage1IncrementalExactC1111111111111".to_string(),
            ]
        );
        assert_eq!(
            state
                .payload
                .buy_mint_counts
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            state.payload.unique_buy_mints
        );
        assert_sorted_strings(&state.payload.unique_buy_mints);
        assert!(
            DiscoveryService::state_can_resume_stale_metrics_window_until_exact_checkpoint(&state)
        );
        Ok(())
    }
