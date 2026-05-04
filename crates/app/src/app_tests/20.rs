    #[test]
    fn pending_irrelevant_swap_queue_is_full_only_at_bounded_capacity_stage1() {
        let now = StdInstant::now();
        let mut pending = VecDeque::new();
        assert!(!pending_irrelevant_swap_queue_is_full(&pending));

        pending.push_back(PendingIrrelevantObservedSwap {
            swap: test_swap("sig-pending-bounded-one"),
            discovery_critical: true,
            processing_started_at: now,
            backpressure_started_at: now,
            last_backpressure_log_at: None,
        });
        assert!(
            !pending_irrelevant_swap_queue_is_full(&pending),
            "one pending discovery-critical swap should not mark the bounded in-memory backlog as full"
        );

        while pending.len() < DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY {
            pending.push_back(PendingIrrelevantObservedSwap {
                swap: test_swap(&format!("sig-pending-bounded-{}", pending.len())),
                discovery_critical: true,
                processing_started_at: now,
                backpressure_started_at: now,
                last_backpressure_log_at: None,
            });
        }
        assert!(
            pending_irrelevant_swap_queue_is_full(&pending),
            "the bounded discovery-critical backlog should only report full once its configured capacity is actually exhausted"
        );
    }

    #[test]
    fn should_drop_backpressured_discovery_critical_irrelevant_observed_swap_only_when_pending_queue_is_full_stage1(
    ) {
        let now = StdInstant::now();
        let mut pending = VecDeque::new();
        assert!(
            !should_drop_backpressured_discovery_critical_irrelevant_observed_swap(&pending),
            "an empty bounded pending backlog must not drop discovery-critical irrelevant swaps yet"
        );

        while pending.len() < DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY {
            pending.push_back(PendingIrrelevantObservedSwap {
                swap: test_swap(&format!("sig-pending-discovery-critical-{}", pending.len())),
                discovery_critical: true,
                processing_started_at: now,
                backpressure_started_at: now,
                last_backpressure_log_at: None,
            });
        }

        assert!(
            should_drop_backpressured_discovery_critical_irrelevant_observed_swap(&pending),
            "once the bounded discovery-critical backlog is full, additional backpressured discovery-critical irrelevant swaps must be dropped instead of pausing ingestion polling"
        );
    }

    #[test]
    fn sustained_discovery_critical_irrelevant_backpressure_buffers_multiple_swaps_before_ingestion_pause_stage1(
    ) -> Result<()> {
        let (_store, db_path) = make_test_store("irrelevant-discovery-critical-sustained-buffer")?;
        let blocker_conn = rusqlite::Connection::open(&db_path)?;
        blocker_conn.busy_timeout(StdDuration::from_millis(1))?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let db_path_for_runtime = db_path.clone();
        let runtime_handle = std::thread::spawn(move || -> Result<(usize, usize)> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let writer = ObservedSwapWriter::start_for_test(
                    db_path_for_runtime
                        .to_str()
                        .context("sqlite path must be valid utf-8")?
                        .to_string(),
                    2,
                    1,
                    false,
                    DiscoveryAggregateWriteConfig::default(),
                )?;
                let filler_swap = test_swap("sig-discovery-critical-sustained-filler");
                let critical_swaps = (0..4)
                    .map(|idx| test_swap(&format!("sig-discovery-critical-sustained-{idx}")))
                    .collect::<Vec<_>>();
                let mut recent_signatures = HashSet::new();
                let mut recent_signature_order = VecDeque::new();
                assert!(note_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &filler_swap.signature,
                ));
                for swap in &critical_swaps {
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

                let processing_started_at = StdInstant::now();
                let mut pending = VecDeque::new();
                let mut directly_enqueued = 0usize;
                for swap in &critical_swaps {
                    match enqueue_irrelevant_observed_swap(
                        &writer,
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        swap,
                        true,
                    )
                    .await? {
                        IrrelevantObservedSwapEnqueueOutcome::Enqueued => {
                            directly_enqueued = directly_enqueued.saturating_add(1);
                        }
                        IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure => {
                            pending.push_back(PendingIrrelevantObservedSwap {
                                swap: swap.clone(),
                                discovery_critical: true,
                                processing_started_at,
                                backpressure_started_at: StdInstant::now(),
                                last_backpressure_log_at: None,
                            });
                            assert!(
                                !pending_irrelevant_swap_backpressure_blocks_ingestion(&pending),
                                "the bounded pending queue should allow the consumer to keep polling after the first discovery-critical backpressure event"
                            );
                        }
                    }
                }

                assert!(
                    directly_enqueued >= 1,
                    "the reserved writer slot should accept at least one discovery-critical swap before the writer is fully saturated"
                );
                assert!(
                    pending.len() >= 2,
                    "sustained discovery-critical pressure should now buffer multiple swaps instead of freezing after the first pending one"
                );

                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        if pending.is_empty() {
                            break Ok::<(), anyhow::Error>(());
                        }
                        let mut blocked_front = None;
                        while let Some(pending_swap) = pending.pop_front() {
                            match enqueue_irrelevant_observed_swap(
                                &writer,
                                &mut recent_signatures,
                                &mut recent_signature_order,
                                &pending_swap.swap,
                                pending_swap.discovery_critical,
                            )
                            .await? {
                                IrrelevantObservedSwapEnqueueOutcome::Enqueued => {}
                                IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure => {
                                    blocked_front = Some(pending_swap);
                                    break;
                                }
                            }
                        }
                        if let Some(blocked_front) = blocked_front {
                            pending.push_front(blocked_front);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    }
                })
                .await
                .context("pending discovery-critical irrelevant swaps should eventually drain")??;

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
                .context("writer should drain sustained discovery-critical irrelevant swaps")??;

                writer.shutdown()?;
                Ok::<(usize, usize), anyhow::Error>((directly_enqueued, critical_swaps.len()))
            })
        });

        std::thread::sleep(StdDuration::from_millis(150));
        blocker_conn.execute_batch("COMMIT")?;

        let (directly_enqueued, critical_total) = runtime_handle
            .join()
            .expect("runtime thread panicked")
            .context("sustained discovery-critical backpressure path should stay healthy")?;

        let verify_store = SqliteStore::open(&db_path)?;
        let persisted = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-06T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(persisted.len(), 1 + critical_total);
        assert!(
            directly_enqueued < critical_total,
            "the repro must actually exercise buffered discovery-critical backpressure rather than only direct enqueue success"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn enqueue_irrelevant_observed_swap_reports_pending_backpressure_without_forgetting_signature(
    ) -> Result<()> {
        let (_store, db_path) = make_test_store("irrelevant-observed-swap-backpressure")?;
        let blocker_conn = rusqlite::Connection::open(&db_path)?;
        blocker_conn.busy_timeout(StdDuration::from_millis(1))?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let db_path_for_runtime = db_path.clone();
        let runtime_handle = std::thread::spawn(move || -> Result<usize> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let writer = ObservedSwapWriter::start_for_test(
                    db_path_for_runtime
                        .to_str()
                        .context("sqlite path must be valid utf-8")?
                        .to_string(),
                    1,
                    1,
                    false,
                    DiscoveryAggregateWriteConfig::default(),
                )?;
                let first_swap = test_swap("sig-irrelevant-backpressure-a");
                let second_swap = test_swap("sig-irrelevant-backpressure-b");
                let third_swap = test_swap("sig-irrelevant-backpressure-c");
                let fourth_swap = test_swap("sig-irrelevant-backpressure-d");
                let mut recent_signatures = HashSet::new();
                let mut recent_signature_order = VecDeque::new();

                for swap in [&first_swap, &second_swap, &third_swap, &fourth_swap] {
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
                        &first_swap,
                        false,
                    )
                    .await?,
                    IrrelevantObservedSwapEnqueueOutcome::Enqueued
                );
                let mut pending_swap = None;
                let mut expected_persisted = 1usize;
                for swap in [&second_swap, &third_swap, &fourth_swap] {
                    let outcome = tokio::time::timeout(
                        Duration::from_millis(50),
                        enqueue_irrelevant_observed_swap(
                            &writer,
                            &mut recent_signatures,
                            &mut recent_signature_order,
                            swap,
                            false,
                        ),
                    )
                    .await
                    .context("irrelevant observed swap backpressure check stalled runtime")??;
                    match outcome {
                        IrrelevantObservedSwapEnqueueOutcome::Enqueued => {
                            expected_persisted = expected_persisted.saturating_add(1);
                        }
                        IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure => {
                            pending_swap = Some(swap.clone());
                            expected_persisted = expected_persisted.saturating_add(1);
                            break;
                        }
                    }
                }
                let pending_swap = pending_swap
                    .context("expected bounded irrelevant observed swap queue to report backpressure")?;
                assert!(
                    !note_recent_swap_signature(
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &pending_swap.signature,
                    ),
                    "backpressured irrelevant observed swap must remain deduped until retried"
                );

                tokio::time::timeout(
                    Duration::from_millis(50),
                    tokio::time::sleep(Duration::from_millis(10)),
                )
                    .await
                    .context("current-thread runtime stalled while irrelevant observed swap was backpressured")?;

                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        match enqueue_irrelevant_observed_swap(
                            &writer,
                            &mut recent_signatures,
                            &mut recent_signature_order,
                            &pending_swap,
                            false,
                        )
                        .await? {
                            IrrelevantObservedSwapEnqueueOutcome::Enqueued => {
                                break Ok::<(), anyhow::Error>(())
                            }
                            IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure => {
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                        }
                    }
                })
                .await
                .context("backpressured irrelevant observed swap should eventually enqueue")??;

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
                .context("observed swap writer should drain pending irrelevant swaps")??;

                writer.shutdown()?;
                Ok::<usize, anyhow::Error>(expected_persisted)
            })
        });

        std::thread::sleep(StdDuration::from_millis(150));
        blocker_conn.execute_batch("COMMIT")?;

        let expected_persisted = runtime_handle
            .join()
            .expect("runtime thread panicked")
            .context("irrelevant observed swap backpressure path should stay healthy")?;

        let verify_store = SqliteStore::open(&db_path)?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-06T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), expected_persisted);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn should_buffer_backpressured_irrelevant_observed_swap_only_for_discovery_critical_stage1() {
        let mut follow_snapshot = FollowSnapshot::default();
        follow_snapshot.active.insert("wallet-followed".to_string());

        assert!(
            should_buffer_backpressured_irrelevant_observed_swap(
                true,
                &follow_snapshot,
                &HashSet::new(),
                false,
            ),
            "discovery-critical irrelevant swaps must still stay bufferable under writer backpressure"
        );
        assert!(
            !should_buffer_backpressured_irrelevant_observed_swap(
                false,
                &follow_snapshot,
                &HashSet::new(),
                false,
            ),
            "non-critical irrelevant swaps should no longer consume the bounded pending queue under writer backpressure"
        );
    }

    #[test]
    fn full_discovery_critical_pending_backlog_blocks_output_queue_drain_on_old_path_stage1(
    ) -> Result<()> {
        let summary = run_discovery_critical_pending_backlog_output_saturation_scenario(true)?;

        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should still allow immediate raw persistence before the remaining output-queue saturation class begins: {summary:?}"
        );
        assert_eq!(
            summary.writer_pending_requests_at_plateau,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the remaining live class starts only after the earlier raw-writer fix has already reduced pending requests to a single non-critical batch plateau of 128: {summary:?}"
        );
        assert_eq!(
            summary.aggregate_queue_depth_at_plateau, 0,
            "aggregate queue must stay zero in this repro so aggregate theory is ruled out for the remaining class: {summary:?}"
        );
        assert_eq!(
            summary.journal_queue_depth_at_plateau, 0,
            "recent_raw journal queue must stay zero in this repro so journal theory is ruled out for the remaining class: {summary:?}"
        );
        assert_eq!(
            summary.upstream_queue_depth_before_loop, 2_048,
            "the reduced live-like repro should start from a fully saturated upstream output queue just like live: {summary:?}"
        );
        assert_eq!(
            summary.ingestion_polls_attempted, 0,
            "old/current app ownership should stop polling ingestion entirely once the bounded discovery-critical backlog is full, even though raw writer is only at the 128 plateau and downstream writer queues are zero: {summary:?}"
        );
        assert_eq!(
            summary.upstream_queue_depth_after_loop, 2_048,
            "because old/current ownership never re-enters ingestion polling here, the upstream output queue cannot drain at all: {summary:?}"
        );
        assert_eq!(
            summary.dropped_over_capacity_discovery_critical_irrelevant_swaps, 0,
            "old/current ownership does not trade this choke for explicit drop accounting; it simply stops polling ingestion: {summary:?}"
        );
        assert_eq!(
            summary.pending_irrelevant_queue_depth,
            DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY,
            "the exact blocker is the already-full local discovery-critical irrelevant backlog, not any downstream writer queue: {summary:?}"
        );
        Ok(())
    }

    #[test]
    fn continuing_ingestion_polling_while_dropping_over_capacity_discovery_critical_irrelevant_swaps_prevents_output_queue_pin_stage1(
    ) -> Result<()> {
        let old = run_discovery_critical_pending_backlog_output_saturation_scenario(true)?;
        let new = run_discovery_critical_pending_backlog_output_saturation_scenario(false)?;

        assert_eq!(
            old.writer_pending_requests_at_plateau,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the old side of the A/B must reproduce the post-fix raw-writer plateau first: old={old:?}"
        );
        assert_eq!(
            new.writer_pending_requests_at_plateau,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the new side of the A/B must preserve the same raw-writer antecedent so the only changed factor is the local pending-backlog ownership rule: new={new:?}"
        );
        assert_eq!(
            old.aggregate_queue_depth_at_plateau, 0,
            "aggregate queue must remain zero on the old side as well: old={old:?}"
        );
        assert_eq!(
            new.aggregate_queue_depth_at_plateau, 0,
            "aggregate queue must remain zero on the new side as well: new={new:?}"
        );
        assert_eq!(
            old.journal_queue_depth_at_plateau, 0,
            "journal queue must remain zero on the old side as well: old={old:?}"
        );
        assert_eq!(
            new.journal_queue_depth_at_plateau, 0,
            "journal queue must remain zero on the new side as well: new={new:?}"
        );
        assert!(
            new.ingestion_polls_attempted > 0,
            "with the production fix, the app must keep polling ingestion even while the bounded discovery-critical backlog is already full: new={new:?}"
        );
        assert_eq!(
            new.upstream_queue_depth_after_loop, 0,
            "continuing ingestion polling while explicitly dropping over-capacity discovery-critical irrelevant swaps should let the same upstream output queue drain completely instead of staying pinned at 2048: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.dropped_over_capacity_discovery_critical_irrelevant_swaps,
            old.upstream_queue_depth_before_loop,
            "the new ownership rule should make the overflow explicit by dropping exactly the over-capacity discovery-critical irrelevant swaps that old/current logic would have left stranded upstream: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.pending_irrelevant_queue_depth,
            DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY,
            "the fix should keep the bounded local backlog capped instead of letting it grow or pretending the pressure disappeared: new={new:?}"
        );
        Ok(())
    }

    #[test]
    fn per_swap_discovery_critical_backpressure_refresh_repins_output_queue_even_after_pause_removal_stage1(
    ) -> Result<()> {
        let summary =
            run_discovery_critical_backpressure_refresh_output_saturation_scenario(false)?;

        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should still write normally before the remaining output-queue class begins: {summary:?}"
        );
        assert_eq!(
            summary.writer_pending_requests_at_plateau,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the remaining class must start only after the earlier raw-writer fix has already reduced pending requests to 128: {summary:?}"
        );
        assert_eq!(
            summary.aggregate_queue_depth_at_plateau, 0,
            "aggregate queue must stay zero in this repro so aggregate theory is ruled out again: {summary:?}"
        );
        assert_eq!(
            summary.journal_queue_depth_at_plateau, 0,
            "journal queue must stay zero in this repro so recent_raw theory is ruled out again: {summary:?}"
        );
        assert_eq!(
            summary.upstream_queue_depth_before_loop, 2_048,
            "the reduced live-like repro should begin from a fully saturated Yellowstone output queue: {summary:?}"
        );
        assert!(
            summary.ingestion_polls_attempted > 0,
            "the old pause-on-full backlog gate is already removed here; the app is still polling ingestion: {summary:?}"
        );
        assert!(
            summary.upstream_queue_depth_after_loop > 0,
            "old/current path should still leave the upstream queue pinned after burning the whole consumer budget on repeated live_runtime refreshes for the same backpressured target-mint class: {summary:?}"
        );
        assert_eq!(
            summary.backpressure_refresh_attempts,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "old/current path should spend one live_runtime refresh per backpressured target-mint swap until the modeled consumer budget is exhausted: {summary:?}"
        );
        assert_eq!(
            summary.backpressure_refresh_budget_units_spent,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the exact old-path stall is that repeated refresh work itself consumes the whole modeled post-plateau consumer budget before the output queue can drain: {summary:?}"
        );
        assert_eq!(
            summary.pending_irrelevant_queue_depth,
            DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY,
            "the bounded local backlog should remain full but stable; the remaining blocker is the repeated refresh work on top of that, not a renewed pause-on-full ownership rule: {summary:?}"
        );
        Ok(())
    }
