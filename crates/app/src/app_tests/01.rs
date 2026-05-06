    #[derive(Debug, Clone, Copy)]
    struct EmptyTargetDiscoveryCriticalBackpressureSummary {
        baseline_rows_persisted: usize,
        first_backpressure_pending_requests: usize,
        max_pending_requests_before_pause: usize,
        pending_irrelevant_queue_depth_at_pause: usize,
        upstream_queue_depth_shortly_after_pause: usize,
        upstream_queue_depth_after_escalation: usize,
        runtime_wal_bytes_at_pause: u64,
        sqlite_write_retry_delta: u64,
        sqlite_busy_error_delta: u64,
        journal_queue_depth_at_pause: usize,
        dropped_irrelevant_swaps: usize,
        ingestion_paused_by_pending_irrelevant_queue: bool,
    }

    #[derive(Debug, Clone, Copy)]
    struct BroadUniqueBuyFallbackBackpressureSummary {
        baseline_rows_persisted: usize,
        first_backpressure_pending_requests: usize,
        max_pending_requests_before_pause: usize,
        pending_irrelevant_queue_depth_at_pause: usize,
        upstream_queue_depth_shortly_after_pause: usize,
        upstream_queue_depth_after_escalation: usize,
        runtime_wal_bytes_at_pause: u64,
        sqlite_write_retry_delta: u64,
        sqlite_busy_error_delta: u64,
        journal_queue_depth_at_pause: usize,
        loaded_target_buy_mints: usize,
        dropped_irrelevant_swaps: usize,
        ingestion_paused_by_pending_irrelevant_queue: bool,
    }

    #[derive(Debug, Clone, Copy)]
    struct DiscoveryCriticalPendingBacklogOutputSaturationSummary {
        baseline_rows_persisted: usize,
        writer_pending_requests_at_plateau: usize,
        journal_queue_depth_at_plateau: usize,
        upstream_queue_depth_before_loop: usize,
        upstream_queue_depth_after_loop: usize,
        pending_irrelevant_queue_depth: usize,
        dropped_over_capacity_discovery_critical_irrelevant_swaps: usize,
        ingestion_polls_attempted: usize,
    }

    #[derive(Debug, Clone, Copy)]
    struct DiscoveryCriticalBackpressureRefreshOutputSaturationSummary {
        baseline_rows_persisted: usize,
        writer_pending_requests_at_plateau: usize,
        journal_queue_depth_at_plateau: usize,
        upstream_queue_depth_before_loop: usize,
        upstream_queue_depth_after_loop: usize,
        pending_irrelevant_queue_depth: usize,
        dropped_over_capacity_discovery_critical_irrelevant_swaps: usize,
        ingestion_polls_attempted: usize,
        backpressure_refresh_attempts: usize,
        backpressure_refresh_budget_units_spent: usize,
    }

    fn run_noncritical_irrelevant_backpressure_plateau_scenario(
        buffer_noncritical_on_backpressure: bool,
        normal_try_enqueue_soft_limit_override: Option<usize>,
    ) -> Result<NoncriticalIrrelevantBackpressureSummary> {
        let (_store, db_path) = make_test_store("noncritical-irrelevant-backpressure-plateau")?;
        seed_runtime_raw_insert_backpressure(&db_path)?;
        let runtime_store = SqliteStore::open(Path::new(&db_path))?;
        runtime_store.checkpoint_wal_truncate()?;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let writer = if let Some(limit) = normal_try_enqueue_soft_limit_override {
            ObservedSwapWriter::start_for_test_with_normal_try_enqueue_soft_limit(sqlite_path, OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE, limit,
            )?
        } else {
            ObservedSwapWriter::start_for_test(sqlite_path, OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE)?
        };
        runtime_store.checkpoint_wal_truncate()?;

        let scenario_now = DateTime::parse_from_rfc3339("2026-04-08T11:47:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let follow_snapshot = {
            let mut snapshot = FollowSnapshot::default();
            snapshot.active.insert("wallet-followed".to_string());
            snapshot
        };
        let open_shadow_lots = HashSet::new();
        let shadow_strategy_fail_closed = false;
        let contention_before = sqlite_contention_snapshot();
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();
        let mut pending_irrelevant_swaps = VecDeque::new();
        let processing_started_at = StdInstant::now();

        for idx in 0..64usize {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-irrelevant-baseline-{idx:04}"),
                idx,
                scenario_now,
            );
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            let outcome = runtime.block_on(async {
                persist_irrelevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                    false,
                )
                .await
            })?;
            assert_eq!(outcome, IrrelevantObservedSwapEnqueueOutcome::Enqueued);
            std::thread::sleep(StdDuration::from_millis(1));
        }

        let baseline_started = StdInstant::now();
        let baseline_rows_persisted = loop {
            let rows = runtime_store
                .load_observed_swaps_since(scenario_now - chrono::Duration::minutes(1))?
                .len();
            if rows >= 32 {
                break rows;
            }
            if baseline_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "writer failed to establish clean post-checkpoint throughput before non-critical irrelevant backpressure scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        };

        let baseline_queue_drain_started = StdInstant::now();
        while writer.snapshot().pending_requests > 0 {
            if baseline_queue_drain_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "writer failed to drain its clean-start baseline backlog before the non-critical irrelevant plateau scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }

        let normal_irrelevant_soft_limit = normal_try_enqueue_soft_limit_override
            .unwrap_or(TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE);
        let mut first_backpressure_pending_requests = 0usize;
        let mut first_backpressure_discovery_critical = false;
        let mut max_pending_requests_before_pause = 0usize;
        let mut dropped_noncritical_irrelevant_swaps = 0usize;
        let mut next_idx = 64usize;
        let max_consumer_swaps = 64
            + normal_irrelevant_soft_limit
            + DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY
            + 4_096;

        while next_idx < max_consumer_swaps
            && !pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps)
        {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-irrelevant-burst-{next_idx:05}"),
                next_idx,
                scenario_now,
            );
            next_idx = next_idx.saturating_add(1);
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            let outcome = runtime.block_on(async {
                persist_irrelevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                    false,
                )
                .await
            })?;
            let writer_snapshot = writer.snapshot();
            max_pending_requests_before_pause =
                max_pending_requests_before_pause.max(writer_snapshot.pending_requests);
            match outcome {
                IrrelevantObservedSwapEnqueueOutcome::Enqueued => {}
                IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure => {
                    if first_backpressure_pending_requests == 0 {
                        first_backpressure_pending_requests = writer_snapshot.pending_requests;
                        first_backpressure_discovery_critical = false;
                    }
                    let should_buffer = if buffer_noncritical_on_backpressure {
                        should_buffer_backpressured_irrelevant_observed_swap_old(
                            false,
                            &follow_snapshot,
                            &open_shadow_lots,
                            shadow_strategy_fail_closed,
                        )
                    } else {
                        should_buffer_backpressured_irrelevant_observed_swap(
                            false,
                            &follow_snapshot,
                            &open_shadow_lots,
                            shadow_strategy_fail_closed,
                        )
                    };
                    if should_buffer {
                        pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                            swap,
                            discovery_critical: false,
                            processing_started_at,
                            backpressure_started_at: StdInstant::now(),
                            last_backpressure_log_at: None,
                        });
                    } else {
                        dropped_noncritical_irrelevant_swaps =
                            dropped_noncritical_irrelevant_swaps.saturating_add(1);
                        forget_recent_swap_signature(
                            &mut recent_signatures,
                            &mut recent_signature_order,
                            &swap.signature,
                        );
                    }
                }
            }
        }

        let snapshot_at_pause = writer.snapshot();
        let runtime_wal_bytes_at_pause = std::fs::metadata(format!("{}-wal", db_path.display()))
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let ingestion_paused_by_pending_irrelevant_queue =
            pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps);
        let mut upstream = VecDeque::new();
        let mut upstream_queue_depth_shortly_after_pause = 0usize;
        if ingestion_paused_by_pending_irrelevant_queue {
            while upstream.len() < 49 {
                let idx = next_idx;
                next_idx = next_idx.saturating_add(1);
                upstream.push_back(irrelevant_backpressure_swap(
                    &format!("sig-irrelevant-upstream-short-{idx:05}"),
                    idx,
                    scenario_now,
                ));
            }
            upstream_queue_depth_shortly_after_pause = upstream.len();
            while upstream.len() < 2_048 {
                let idx = next_idx;
                next_idx = next_idx.saturating_add(1);
                upstream.push_back(irrelevant_backpressure_swap(
                    &format!("sig-irrelevant-upstream-long-{idx:05}"),
                    idx,
                    scenario_now,
                ));
            }
        }
        writer.shutdown()?;
        let contention_after = sqlite_contention_snapshot();

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));

        Ok(NoncriticalIrrelevantBackpressureSummary {
            baseline_rows_persisted,
            first_backpressure_pending_requests,
            first_backpressure_discovery_critical,
            upstream_queue_depth_at_first_backpressure: 0,
            max_pending_requests_before_pause,
            pending_irrelevant_queue_depth_at_pause: pending_irrelevant_swaps.len(),
            upstream_queue_depth_shortly_after_pause,
            upstream_queue_depth_after_escalation: upstream.len(),
            runtime_wal_bytes_at_pause,
            sqlite_write_retry_delta: contention_after
                .write_retry_total
                .saturating_sub(contention_before.write_retry_total),
            sqlite_busy_error_delta: contention_after
                .busy_error_total
                .saturating_sub(contention_before.busy_error_total),
            journal_queue_depth_at_pause: snapshot_at_pause.journal_queue_depth_batches,
            dropped_noncritical_irrelevant_swaps,
            ingestion_paused_by_pending_irrelevant_queue,
        })
    }

    fn run_noncritical_irrelevant_output_pressure_wave_scenario(
        preemptive_drop_under_output_pressure: bool,
    ) -> Result<NoncriticalIrrelevantOutputPressureWaveSummary> {
        let (_store, db_path) = make_test_store("noncritical-irrelevant-output-pressure-wave")?;
        seed_runtime_raw_insert_backpressure(&db_path)?;
        let runtime_store = SqliteStore::open(Path::new(&db_path))?;
        runtime_store.checkpoint_wal_truncate()?;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let writer = ObservedSwapWriter::start_for_test_with_normal_try_enqueue_soft_limit(db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(), OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
        )?;
        runtime_store.checkpoint_wal_truncate()?;

        let scenario_now = DateTime::parse_from_rfc3339("2026-04-09T10:42:44Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();

        for idx in 0..64usize {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-noncritical-output-pressure-baseline-{idx:04}"),
                idx,
                scenario_now,
            );
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            let outcome = runtime.block_on(async {
                persist_irrelevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                    false,
                )
                .await
            })?;
            assert_eq!(outcome, IrrelevantObservedSwapEnqueueOutcome::Enqueued);
            std::thread::sleep(StdDuration::from_millis(1));
        }

        let baseline_started = StdInstant::now();
        let baseline_rows_persisted = loop {
            let rows = runtime_store
                .load_observed_swaps_since(scenario_now - chrono::Duration::minutes(1))?
                .len();
            if rows >= 32 {
                break rows;
            }
            if baseline_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "writer failed to establish clean post-checkpoint throughput before the non-critical output-pressure wave scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        };

        let baseline_queue_drain_started = StdInstant::now();
        while writer.snapshot().pending_requests > 0 {
            if baseline_queue_drain_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "writer failed to drain its clean-start baseline backlog before the non-critical output-pressure wave scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }

        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let shadow_strategy_fail_closed = true;
        let ingestion_snapshot = Some(infra_snapshot_with_yellowstone_queue(
            scenario_now,
            2_048,
            0,
            2_048,
            2_048,
            20_000,
        ));
        let mut upstream = VecDeque::new();
        while upstream.len() < 2_048 {
            let idx = upstream.len();
            upstream.push_back(irrelevant_backpressure_swap(
                &format!("sig-noncritical-output-pressure-upstream-{idx:05}"),
                idx,
                scenario_now,
            ));
        }
        let upstream_queue_depth_before_loop = upstream.len();
        let mut writer_pending_requests_at_wave_peak = 0usize;
        let mut journal_queue_depth_at_wave_peak = 0usize;
        let mut accepted_noncritical_irrelevant_swaps = 0usize;
        let mut dropped_noncritical_irrelevant_swaps = 0usize;
        let mut completed_waves = 0usize;

        while let Some(swap) = upstream.pop_front() {
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));

            if preemptive_drop_under_output_pressure
                && should_preemptively_drop_noncritical_irrelevant_observed_swap_under_output_pressure(
                    false,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    ingestion_snapshot,
                )
            {
                forget_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                );
                dropped_noncritical_irrelevant_swaps =
                    dropped_noncritical_irrelevant_swaps.saturating_add(1);
                continue;
            }

            let outcome = runtime.block_on(async {
                persist_irrelevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                    false,
                )
                .await
            })?;

            match outcome {
                IrrelevantObservedSwapEnqueueOutcome::Enqueued => {
                    accepted_noncritical_irrelevant_swaps =
                        accepted_noncritical_irrelevant_swaps.saturating_add(1);
                    let snapshot = writer.snapshot();
                    if snapshot.pending_requests > writer_pending_requests_at_wave_peak {
                        writer_pending_requests_at_wave_peak = snapshot.pending_requests;
                        journal_queue_depth_at_wave_peak = snapshot.journal_queue_depth_batches;
                    }
                    if snapshot.pending_requests >= TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE {
                        completed_waves = completed_waves.saturating_add(1);
                        let drain_started = StdInstant::now();
                        while writer.snapshot().pending_requests > 0 {
                            if drain_started.elapsed() > StdDuration::from_secs(10) {
                                anyhow::bail!(
                                    "writer failed to drain a bounded non-critical output-pressure wave"
                                );
                            }
                            std::thread::sleep(StdDuration::from_millis(20));
                        }
                        if completed_waves >= 4 {
                            break;
                        }
                    }
                }
                IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure => {
                    forget_recent_swap_signature(
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &swap.signature,
                    );
                    dropped_noncritical_irrelevant_swaps =
                        dropped_noncritical_irrelevant_swaps.saturating_add(1);
                }
            }
        }

        writer.shutdown()?;

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));

        Ok(NoncriticalIrrelevantOutputPressureWaveSummary {
            baseline_rows_persisted,
            writer_pending_requests_at_wave_peak,
            journal_queue_depth_at_wave_peak,
            upstream_queue_depth_before_loop,
            upstream_queue_depth_after_loop: upstream.len(),
            accepted_noncritical_irrelevant_swaps,
            dropped_noncritical_irrelevant_swaps,
            completed_waves,
        })
    }

    fn irrelevant_observed_swap_for_scenario_branch(
        branch: IrrelevantObservedSwapScenarioBranch,
        signature: &str,
        idx: usize,
        base_ts: DateTime<Utc>,
    ) -> SwapEvent {
        let mut swap = irrelevant_backpressure_swap(signature, idx, base_ts);
        if matches!(branch, IrrelevantObservedSwapScenarioBranch::Unclassified) {
            swap.token_in = format!("token-unclassified-in-{idx:05}");
            swap.token_out = format!("token-unclassified-out-{idx:05}");
        }
        swap
    }
