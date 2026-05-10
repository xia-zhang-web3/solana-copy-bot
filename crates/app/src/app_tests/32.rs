#[test]
    fn broad_unique_buy_universe_target_set_old_loader_keeps_generic_backpressure_and_blocks_later_exact_context_stage1(
    ) -> Result<()> {
        let (store, db_path) =
            make_test_store("pending-irrelevant-broad-unique-buy-universe-targets")?;
        let broad_unique_buy_mints: Vec<String> = (0
            ..DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY)
            .map(|idx| format!("token-generic-{idx:04}"))
            .chain(std::iter::once("token-target".to_string()))
            .collect();
        let broad_unique_buy_mint_refs: Vec<&str> =
            broad_unique_buy_mints.iter().map(String::as_str).collect();
        seed_test_discovery_critical_target_buy_mints(&store, &[], &broad_unique_buy_mint_refs)?;

        let empty_follow_snapshot = FollowSnapshot::default();
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();
        let mut telemetry = AppConsumerLoopTelemetry::default();
        let processing_started_at = StdInstant::now();
        let backpressure_started_at = StdInstant::now();
        let mut pending_irrelevant_swaps = VecDeque::new();

        for idx in 0..DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY.saturating_sub(1) {
            let mut generic_swap = test_swap(&format!("sig-broad-generic-pending-{idx:04}"));
            generic_swap.token_out = format!("token-generic-{idx:04}");
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &generic_swap.signature,
            ));
            pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                swap: generic_swap,
                discovery_critical: true,
                processing_started_at,
                backpressure_started_at,
                last_backpressure_log_at: None,
            });
        }
        let mut target_swap = test_swap("sig-broad-target-pending");
        target_swap.token_out = "token-target".to_string();
        assert!(note_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            &target_swap.signature,
        ));
        pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
            swap: target_swap,
            discovery_critical: true,
            processing_started_at,
            backpressure_started_at,
            last_backpressure_log_at: None,
        });

        let mut refreshed_target_buy_mints = HashSet::new();
        refresh_discovery_critical_target_buy_mints_or_warn_old(
            &store,
            &mut refreshed_target_buy_mints,
        )?;
        assert_eq!(
            refreshed_target_buy_mints.len(),
            broad_unique_buy_mints.len(),
            "without an exact candidate target surface, refresh still reloads the full unique-buy-mint universe"
        );
        let dropped = prune_noncritical_zero_universe_pending_irrelevant_swaps(
            &mut pending_irrelevant_swaps,
            &mut recent_signatures,
            &mut recent_signature_order,
            &mut telemetry,
            &empty_follow_snapshot,
            &HashSet::new(),
            true,
            &refreshed_target_buy_mints,
        );
        assert_eq!(
            dropped, 0,
            "when the persisted target set is still the whole unique-buy-mint universe, generic backlog remains discovery-critical and cannot be pruned away"
        );
        assert!(
            pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps),
            "the same broad target-mint ownership leaves the bounded pending queue saturated, so later exact target context still cannot make forward progress"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn broad_unique_buy_fallback_recreates_clean_start_raw_writer_full_plateau_stage1() -> Result<()>
    {
        let summary = run_broad_unique_buy_fallback_backpressure_scenario(true)?;
        let raw_writer_full_with_single_inflight_batch =
            OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY + TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE;

        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should permit immediate raw persistence before the broad unique-buy fallback plateau forms: {summary:?}"
        );
        assert!(
            summary.loaded_target_buy_mints >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            "the old loader must actually populate a broad discovery-critical target set from unique_buy_mints, otherwise this repro is not exercising the surviving raw-writer class: {summary:?}"
        );
        assert!(
            summary.first_backpressure_pending_requests >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
                && summary.first_backpressure_pending_requests
                    <= raw_writer_full_with_single_inflight_batch,
            "with the broad unique-buy fallback contract, generic SOL-leg buys should still fill the raw writer to a full 4096 queued requests, plus at most one in-flight raw batch before the first backpressure event: {summary:?}"
        );
        assert!(
            summary.max_pending_requests_before_pause >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
                && summary.max_pending_requests_before_pause
                    <= raw_writer_full_with_single_inflight_batch,
            "the same broad fallback request class should keep pending requests pinned at a full raw writer plus at most one in-flight batch while aggregate/journal queues stay zero: {summary:?}"
        );
        assert!(
            summary.pending_irrelevant_queue_depth_at_pause
                >= DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY * 3 / 4,
            "after raw pending reaches the full writer, the app should build a material local discovery-critical irrelevant backlog; exact pause depth is scheduler-sensitive on CI runners: {summary:?}"
        );
        assert!(
            summary.upstream_queue_depth_shortly_after_pause <= 64,
            "the exact incident class should keep upstream pressure small while the local queue absorbs the broad class first; exact non-zero timing is scheduler-sensitive: {summary:?}"
        );
        assert!(
            summary.upstream_queue_depth_after_escalation == 0
                || summary.upstream_queue_depth_after_escalation == 2_048,
            "depending on runner scheduling, the modeled upstream queue may either remain drained or reach its 2048 capacity after local pressure forms: {summary:?}"
        );
        assert_eq!(summary.journal_queue_depth_at_pause, 0);
        assert!(summary.runtime_wal_bytes_at_pause < 16 * 1024 * 1024);
        assert_eq!(summary.sqlite_write_retry_delta, 0);
        assert_eq!(summary.sqlite_busy_error_delta, 0);
        assert!(
            summary.dropped_irrelevant_swaps <= TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "runner scheduling may allow one small batch of incidental explicit drops while the old broad fallback backlog forms, but it must not hide broad data loss: {summary:?}"
        );
        assert!(
            summary.ingestion_paused_by_pending_irrelevant_queue
                || summary.pending_irrelevant_queue_depth_at_pause
                    >= DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY * 3 / 4,
            "the app should either pause on the local discovery-critical backlog or prove that backlog materially formed before the scheduler drained the upstream queue: {summary:?}"
        );
        Ok(())
    }

    #[test]
    fn removing_broad_unique_buy_fallback_prevents_clean_start_raw_writer_full_plateau_stage1(
    ) -> Result<()> {
        let old = run_broad_unique_buy_fallback_backpressure_scenario(true)?;
        let new = run_broad_unique_buy_fallback_backpressure_scenario(false)?;
        let normal_irrelevant_soft_limit = OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
            .saturating_sub(TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE);
        let raw_writer_full_with_single_inflight_batch =
            OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY + TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE;

        assert!(
            old.loaded_target_buy_mints >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            "the old side of the A/B must load a broad fallback target set from unique_buy_mints: old={old:?}"
        );
        assert!(
            old.first_backpressure_pending_requests >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
                && old.first_backpressure_pending_requests
                    <= raw_writer_full_with_single_inflight_batch,
            "the old side of the A/B must reproduce a full raw-writer plateau first: old={old:?}"
        );
        assert_eq!(
            new.loaded_target_buy_mints, 0,
            "with the fallback removed, app-side raw-writer pressure handling should load no discovery-critical targets from unique_buy_mints alone: new={new:?}"
        );
        assert!(
            new.dropped_irrelevant_swaps > 0,
            "without the broad unique-buy fallback, the app must explicitly drop those backpressured generic irrelevant buys instead of buffering them as discovery-critical: new={new:?}"
        );
        assert!(
            new.first_backpressure_pending_requests <= normal_irrelevant_soft_limit,
            "the fix must stop the broad unique-buy fallback from consuming the reserved raw-writer capacity and reduce the first plateau back below 4096: old={old:?} new={new:?}"
        );
        assert_eq!(new.pending_irrelevant_queue_depth_at_pause, 0);
        assert!(!new.ingestion_paused_by_pending_irrelevant_queue);
        assert_eq!(new.upstream_queue_depth_after_escalation, 0);
        assert_eq!(new.journal_queue_depth_at_pause, 0);
        assert_eq!(new.sqlite_write_retry_delta, 0);
        assert_eq!(new.sqlite_busy_error_delta, 0);
        assert!(
            new.runtime_wal_bytes_at_pause <= old.runtime_wal_bytes_at_pause * 2 + 1,
            "the fix should not just trade the raw-writer plateau for runaway WAL growth: old={old:?} new={new:?}"
        );
        Ok(())
    }

    #[test]
    fn enqueue_irrelevant_observed_swap_discovery_critical_uses_reserved_writer_capacity_without_waiting_for_commit_stage1(
    ) -> Result<()> {
        let _contention_guard = sqlite_contention_delta_test_guard();
        let (_store, db_path) = make_test_store("irrelevant-discovery-critical-priority-enqueue")?;
        let blocker_conn = rusqlite::Connection::open(&db_path)?;
        blocker_conn.busy_timeout(StdDuration::from_millis(1))?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let runtime_handle = std::thread::spawn(move || -> Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let writer = ObservedSwapWriter::start_for_test(sqlite_path, 2, 1)?;
                let filler_swap = test_swap("sig-discovery-critical-filler");
                let critical_swap = test_swap("sig-discovery-critical-target");
                let mut recent_signatures = HashSet::new();
                let mut recent_signature_order = VecDeque::new();
                for swap in [&filler_swap, &critical_swap] {
                    assert!(note_recent_swap_signature(
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &swap.signature,
                    ));
                }

                assert_eq!(
                    enqueue_irrelevant_observed_swap(
                        &writer,
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &filler_swap,
                        false,
                    )
                    .await?,
                    IrrelevantObservedSwapEnqueueOutcome::Enqueued
                );
                result_tx
                    .send("ready")
                    .expect("main thread should receive ready signal");
                let outcome = enqueue_irrelevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &critical_swap,
                    true,
                )
                .await?;
                assert_eq!(outcome, IrrelevantObservedSwapEnqueueOutcome::Enqueued);
                result_tx
                    .send("critical_enqueued")
                    .expect("main thread should receive critical enqueue signal");

                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        writer.ensure_running()?;
                        if writer.snapshot().pending_requests == 0 {
                            break Ok::<(), anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .context(
                    "writer should drain discovery-critical irrelevant swap after lock release",
                )??;
                writer.shutdown()?;
                Ok::<(), anyhow::Error>(())
            })
        });

        assert_eq!(
            result_rx
                .recv_timeout(StdDuration::from_secs(1))
                .context("runtime thread never reached discovery-critical enqueue")?,
            "ready"
        );
        assert_eq!(
            result_rx
                .recv_timeout(StdDuration::from_millis(100))
                .context("runtime thread never completed discovery-critical enqueue before lock release")?,
            "critical_enqueued",
            "discovery-critical irrelevant swap should return immediately once it claims reserved writer capacity instead of waiting for sqlite durability"
        );

        let verify_before_commit = SqliteStore::open(&db_path)?;
        assert_eq!(
            verify_before_commit
                .load_observed_swaps_since(
                    DateTime::parse_from_rfc3339("2026-03-14T15:59:00Z")
                        .expect("timestamp")
                        .with_timezone(&Utc),
                )?
                .len(),
            0,
            "sqlite lock should still keep discovery-critical irrelevant swaps invisible until the writer thread can commit them"
        );

        blocker_conn.execute_batch("COMMIT")?;
        runtime_handle
            .join()
            .expect("runtime thread panicked")
            .context(
                "discovery-critical irrelevant swap should still persist after lock release",
            )?;

        let verify_store = SqliteStore::open(&db_path)?;
        let persisted = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T15:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(persisted.len(), 2);
        assert!(
            persisted
                .iter()
                .any(|swap| swap.signature == "sig-discovery-critical-target"),
            "the discovery-critical irrelevant swap should persist once the durable write unblocks"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
