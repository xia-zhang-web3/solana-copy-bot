    #[test]
    fn app_consumer_irrelevant_writer_backpressure_stays_follow_rejected_stage1() -> Result<()> {
        let (_store, db_path) = make_test_store("follow-rejected-backpressure-stays-irrelevant")?;
        let blocker_conn = rusqlite::Connection::open(&db_path)?;
        blocker_conn.busy_timeout(StdDuration::from_millis(1))?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_for_test_with_normal_try_enqueue_soft_limit(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
                TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
                false,
                DiscoveryAggregateWriteConfig::default(),
                1,
            )?;
            let follow_snapshot = FollowSnapshot::default();
            let open_shadow_lots = HashSet::new();
            let mut recent_signatures = HashSet::new();
            let mut recent_signature_order = VecDeque::new();
            let first = test_swap("sig-follow-rejected-backpressure-first");
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &first.signature,
            ));
            assert_eq!(
                persist_irrelevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &first,
                    false,
                )
                .await?,
                IrrelevantObservedSwapEnqueueOutcome::Enqueued
            );

            let second = test_swap("sig-follow-rejected-backpressure-second");
            let relevance = classify_observed_swap_shadow_relevance(
                &second,
                &follow_snapshot,
                &ShadowScheduler::new(),
                &open_shadow_lots,
            );
            assert!(
                matches!(
                    relevance,
                    ObservedSwapShadowRelevance::IrrelevantNotFollowed(_)
                ),
                "backpressure must not convert a follow-rejected swap into a followed/relevant swap"
            );
            let mut telemetry = AppConsumerLoopTelemetry::default();
            telemetry.note_swap_seen();
            telemetry.note_follow_rejected();
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &second.signature,
            ));
            assert_eq!(
                persist_irrelevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &second,
                    false,
                )
                .await?,
                IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure,
                "normal non-critical writer backpressure remains conservative"
            );
            let snapshot = telemetry.snapshot_and_reset();
            assert_eq!(snapshot.follow_rejected, 1);
            assert_eq!(snapshot.follow_rejected_ratio, 1.0);

            blocker_conn.execute_batch("ROLLBACK")?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
        Ok(())
    }

    #[test]
    fn observed_swap_shadow_relevance_keeps_followed_buy_relevant_stage1() {
        let swap = test_swap("sig-followed-buy-stage1");
        let mut follow_snapshot = FollowSnapshot::default();
        follow_snapshot.active.insert(swap.wallet.clone());
        assert!(
            matches!(
                classify_observed_swap_shadow_relevance(
                    &swap,
                    &follow_snapshot,
                    &ShadowScheduler::new(),
                    &HashSet::new(),
                ),
                ObservedSwapShadowRelevance::Relevant(ShadowSwapSide::Buy)
            ),
            "followed wallets stay on the relevant branch, so the new irrelevant_not_followed drop path is unreachable"
        );
    }

    #[test]
    fn observed_swap_shadow_relevance_keeps_sell_with_open_lot_relevant_stage1() {
        let mut swap = test_swap("sig-sell-open-lot-stage1");
        swap.token_in = "token-a".to_string();
        swap.token_out = "So11111111111111111111111111111111111111112".to_string();
        let sell_key = shadow_task_key_for_swap(&swap, ShadowSwapSide::Sell);
        let open_shadow_lots = HashSet::from([(sell_key.wallet.clone(), sell_key.token.clone())]);
        assert!(
            matches!(
                classify_observed_swap_shadow_relevance(
                    &swap,
                    &FollowSnapshot::default(),
                    &ShadowScheduler::new(),
                    &open_shadow_lots,
                ),
                ObservedSwapShadowRelevance::Relevant(ShadowSwapSide::Sell)
            ),
            "open shadow lots keep the swap on the relevant branch, so the new irrelevant_not_followed drop path is unreachable"
        );
    }

    #[test]
    fn irrelevant_not_followed_discovery_critical_reserved_path_still_enqueues_stage1() -> Result<()>
    {
        let (_store, db_path) =
            make_test_store("irrelevant-not-followed-discovery-critical-preserved")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_for_test(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
                TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
                false,
                DiscoveryAggregateWriteConfig::default(),
            )?;
            let swap = test_swap("sig-not-followed-discovery-critical-preserved");
            let mut recent_signatures = HashSet::new();
            let mut recent_signature_order = VecDeque::new();
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            assert_eq!(
                persist_irrelevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                    true,
                )
                .await?,
                IrrelevantObservedSwapEnqueueOutcome::Enqueued,
                "the reserved discovery-critical irrelevant writer path must remain available"
            );
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
        Ok(())
    }

    #[test]
    fn live_like_writer_plateau_and_stale_export_truth_can_coexist_without_output_pressure_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("live-like-writer-plateau-and-stale-export-truth")?;
        let stale_publish_at = DateTime::parse_from_rfc3339("2026-04-06T17:55:23Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let export_now = DateTime::parse_from_rfc3339("2026-04-12T15:53:32Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let mut config = copybot_config::DiscoveryConfig::default();
        config.scoring_window_days = 7;
        config.metric_snapshot_interval_seconds = 60;
        config.refresh_seconds = 600;
        config.follow_top_n = 1;
        config.min_score = 0.1;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

        let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts =
            stale_publish_at.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now =
            DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(stale_publish_at);
        let metrics_window_start =
            bucketed_now - chrono::Duration::days(config.scoring_window_days.max(1) as i64);
        let published_wallet_ids = (0..7usize)
            .map(|idx| format!("wallet_stale_published_{idx:02}"))
            .collect::<Vec<_>>();
        for (idx, wallet_id) in published_wallet_ids.iter().enumerate() {
            let ts = stale_publish_at - chrono::Duration::minutes(idx as i64 + 1);
            store.upsert_wallet(wallet_id, ts, ts, "candidate")?;
            store.insert_wallet_metric(&WalletMetricRow {
                wallet_id: wallet_id.clone(),
                window_start: metrics_window_start,
                pnl: 1.0,
                win_rate: 1.0,
                trades: 4,
                closed_trades: 4,
                hold_median_seconds: 60,
                score: 1.0,
                buy_total: 4,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            })?;
        }
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: export_now - chrono::Duration::seconds(1),
            slot: 42,
            signature: "sig-runtime-export-live-like".to_string(),
        })?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_missing_exact_published_wallet_ids".to_string(),
            last_published_at: Some(stale_publish_at),
            last_published_window_start: Some(metrics_window_start),
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: Some(published_wallet_ids.clone()),
        })?;

        let export_error = store
            .export_discovery_runtime_artifact(export_now, discovery.publication_freshness_gate())
            .expect_err("stale fail-closed publication truth must still refuse export");
        let export_error_text = format!("{export_error:#}");
        assert!(export_error_text.contains("requires fresh publication truth under export gate"));
        assert!(export_error_text.contains("runtime_mode=fail_closed"));
        assert!(export_error_text.contains("fresh_under_export_gate=false"));
        assert!(export_error_text.contains("published_wallet_count=7"));

        let blocker_conn = rusqlite::Connection::open(&db_path)?;
        blocker_conn.busy_timeout(StdDuration::from_millis(1))?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let writer = ObservedSwapWriter::start_for_test(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            false,
            DiscoveryAggregateWriteConfig::default(),
        )?;
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();
        for idx in 0..TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-live-combined-{idx:03}"),
                idx,
                export_now,
            );
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            let outcome = runtime.block_on(async {
                enqueue_irrelevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                    false,
                )
                .await
            })?;
            assert_eq!(outcome, IrrelevantObservedSwapEnqueueOutcome::Enqueued);
        }
        let plateau_swap = irrelevant_backpressure_swap(
            "sig-live-combined-backpressure",
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            export_now,
        );
        assert!(note_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            &plateau_swap.signature,
        ));
        let plateau_outcome = runtime.block_on(async {
            enqueue_irrelevant_observed_swap(
                &writer,
                &mut recent_signatures,
                &mut recent_signature_order,
                &plateau_swap,
                false,
            )
            .await
        })?;
        assert_eq!(
            plateau_outcome,
            IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure
        );
        let plateau_snapshot = writer.snapshot();
        assert_eq!(
            plateau_snapshot.pending_requests,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE
        );
        assert_eq!(plateau_snapshot.aggregate_queue_depth_batches, 0);

        let publication_state_after = store
            .discovery_publication_state_read_only()?
            .expect("publication state should remain readable");
        assert_eq!(
            publication_state_after.last_published_at,
            Some(stale_publish_at)
        );
        assert_eq!(
            publication_state_after
                .published_wallet_ids
                .as_ref()
                .map(Vec::len)
                .unwrap_or(0),
            7
        );

        blocker_conn.execute_batch("COMMIT")?;
        let drain_started = StdInstant::now();
        while writer.snapshot().pending_requests > 0 {
            if drain_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("writer failed to drain after combined plateau/export repro");
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
        writer.shutdown()?;

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
        Ok(())
    }

    #[test]
    fn noncritical_irrelevant_output_pressure_drop_targets_only_zero_universe_fail_closed_stage1() {
        let followed_snapshot = {
            let mut snapshot = FollowSnapshot::default();
            snapshot.active.insert("wallet-followed".to_string());
            snapshot
        };
        let pressured_snapshot = Some(infra_snapshot_with_yellowstone_queue(
            Utc::now(),
            2_048,
            0,
            2_048,
            2_048,
            20_000,
        ));

        assert!(
            should_preemptively_drop_noncritical_irrelevant_observed_swap_under_output_pressure(
                false,
                &FollowSnapshot::default(),
                &HashSet::new(),
                true,
                pressured_snapshot,
            ),
            "zero-universe fail-closed Yellowstone output pressure should preemptively drop non-critical irrelevant swaps"
        );
        assert!(
            !should_preemptively_drop_noncritical_irrelevant_observed_swap_under_output_pressure(
                true,
                &FollowSnapshot::default(),
                &HashSet::new(),
                true,
                pressured_snapshot,
            ),
            "discovery-critical irrelevant swaps must never be preemptively dropped by the non-critical pressure path"
        );
        assert!(
            !should_preemptively_drop_noncritical_irrelevant_observed_swap_under_output_pressure(
                false,
                &followed_snapshot,
                &HashSet::new(),
                true,
                pressured_snapshot,
            ),
            "followed universes must not take the zero-universe fail-closed fast drop path"
        );
        assert!(
            !should_preemptively_drop_noncritical_irrelevant_observed_swap_under_output_pressure(
                false,
                &FollowSnapshot::default(),
                &HashSet::new(),
                true,
                Some(infra_snapshot_with_yellowstone_queue(
                    Utc::now(),
                    1,
                    0,
                    512,
                    2_048,
                    10,
                )),
            ),
            "light Yellowstone queue pressure must not activate the fast drop path"
        );
    }

    #[test]
    fn noncritical_irrelevant_output_pressure_waves_recreate_post_recovery_2048_repin_stage1(
    ) -> Result<()> {
        let summary = run_noncritical_irrelevant_output_pressure_wave_scenario(false)?;

        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should still write normally before the non-critical output-pressure wave scenario begins: {summary:?}"
        );
        assert_eq!(
            summary.writer_pending_requests_at_wave_peak,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the exact live class must hit the one-batch non-critical irrelevant soft-limit peak of 128 before each wave drains back down: {summary:?}"
        );
        assert_eq!(
            summary.aggregate_queue_depth_at_wave_peak, 0,
            "aggregate queue must stay zero in this reduced live-like repro so aggregate backlog remains ruled out: {summary:?}"
        );
        assert!(
            summary.journal_queue_depth_at_wave_peak <= 1,
            "journal queue must stay in the low 0..1 class while the output queue remains pinned: {summary:?}"
        );
        assert_eq!(
            summary.upstream_queue_depth_before_loop, 2_048,
            "the reduced live-like repro should begin from the same 2048 upstream queue saturation as live: {summary:?}"
        );
        assert_eq!(
            summary.completed_waves, 4,
            "the current path should keep re-entering bounded 128-request non-critical waves instead of clearing the saturated upstream queue in one pass: {summary:?}"
        );
        assert!(
            summary.accepted_noncritical_irrelevant_swaps
                >= TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE * summary.completed_waves,
            "current logic must keep burning real raw-writer work on non-critical irrelevant swaps in each wave: {summary:?}"
        );
        assert!(
            summary.upstream_queue_depth_after_loop > 0,
            "after the same bounded number of waves the upstream queue should still remain pinned, matching the live oscillation class: {summary:?}"
        );
        Ok(())
    }

    #[test]
    fn preemptive_noncritical_irrelevant_output_pressure_drop_eliminates_post_recovery_repin_stage1(
    ) -> Result<()> {
        let old = run_noncritical_irrelevant_output_pressure_wave_scenario(false)?;
        let new = run_noncritical_irrelevant_output_pressure_wave_scenario(true)?;

        assert_eq!(
            old.writer_pending_requests_at_wave_peak, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "old side must reproduce the exact 128 non-critical wave peak first: old={old:?}"
        );
        assert_eq!(
            old.aggregate_queue_depth_at_wave_peak, 0,
            "aggregate queue must remain zero on the old side: old={old:?}"
        );
        assert!(
            old.journal_queue_depth_at_wave_peak <= 1,
            "journal queue must remain in the same low 0..1 class on the old side: old={old:?}"
        );
        assert_eq!(
            old.upstream_queue_depth_before_loop, 2_048,
            "old side must begin from the same saturated upstream queue: old={old:?}"
        );
        assert_eq!(
            new.accepted_noncritical_irrelevant_swaps, 0,
            "with the production fix, non-critical irrelevant swaps should stop consuming raw-writer budget once Yellowstone output pressure is already severe: new={new:?}"
        );
        assert_eq!(
            new.writer_pending_requests_at_wave_peak, 0,
            "the fix should eliminate the recurring one-batch non-critical wave entirely rather than merely shrinking it: new={new:?}"
        );
        assert_eq!(
            new.upstream_queue_depth_after_loop, 0,
            "the same saturated upstream queue should drain completely once non-critical irrelevant swaps are dropped immediately under severe Yellowstone output pressure: old={old:?} new={new:?}"
        );
        assert!(
            new.dropped_noncritical_irrelevant_swaps >= old.upstream_queue_depth_before_loop,
            "the fix should make the tradeoff explicit by dropping the non-critical irrelevant class instead of burning repeated raw-writer waves on it: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.aggregate_queue_depth_at_wave_peak, 0,
            "the fix must stay off the aggregate path: new={new:?}"
        );
        assert_eq!(
            new.journal_queue_depth_at_wave_peak, 0,
            "the fix must stay off the recent_raw journal path: new={new:?}"
        );
        Ok(())
    }

    #[test]
    fn app_consumer_loop_telemetry_reports_follow_rejected_ratio_and_resets() {
        let mut telemetry = AppConsumerLoopTelemetry::default();

        telemetry.note_swap_seen();
        telemetry.note_processing_duration(5);
        telemetry.note_swap_seen();
        telemetry.note_follow_rejected();
        telemetry.note_processing_duration(15);
        telemetry.note_swap_seen();
        telemetry.note_follow_rejected();
        telemetry.note_processing_duration(25);

        let snapshot = telemetry.snapshot_and_reset();
        assert_eq!(
            snapshot,
            AppConsumerLoopTelemetrySnapshot {
                swaps_seen: 3,
                follow_rejected: 2,
                follow_rejected_ratio: 2.0 / 3.0,
                processing_ms_p95: 25,
            }
        );

        let empty_snapshot = telemetry.snapshot_and_reset();
        assert_eq!(
            empty_snapshot,
            AppConsumerLoopTelemetrySnapshot {
                swaps_seen: 0,
                follow_rejected: 0,
                follow_rejected_ratio: 0.0,
                processing_ms_p95: 0,
            }
        );
    }

    #[test]
    fn parse_app_log_env_filter_uses_default_when_missing() {
        with_app_log_filter_env(None, None, || {
            parse_app_log_env_filter("info").expect("missing env must use default app log filter");
        });
    }
