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
                let writer = ObservedSwapWriter::start_for_test(db_path_for_runtime
                        .to_str()
                        .context("sqlite path must be valid utf-8")?
                        .to_string(), 2, 1)?;
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
