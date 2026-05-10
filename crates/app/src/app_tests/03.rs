    fn run_empty_target_discovery_critical_backpressure_scenario(
        broad_empty_target_bootstrap_enabled: bool,
    ) -> Result<EmptyTargetDiscoveryCriticalBackpressureSummary> {
        let _contention_guard = sqlite_contention_delta_test_guard();
        let (_store, db_path) =
            make_test_store("empty-target-discovery-critical-backpressure-plateau")?;
        seed_runtime_raw_insert_backpressure(&db_path)?;
        let runtime_store = SqliteStore::open(Path::new(&db_path))?;
        runtime_store.checkpoint_wal_truncate()?;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let writer = ObservedSwapWriter::start_for_test(db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(), OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE)?;
        runtime_store.checkpoint_wal_truncate()?;

        let scenario_now = DateTime::parse_from_rfc3339("2026-04-08T11:47:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let shadow_strategy_fail_closed = true;
        let discovery_critical_target_buy_mints = HashSet::new();
        let contention_before = sqlite_contention_snapshot();
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();
        let mut pending_irrelevant_swaps = VecDeque::new();
        let processing_started_at = StdInstant::now();

        for idx in 0..64usize {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-empty-target-baseline-{idx:04}"),
                idx,
                scenario_now,
            );
            let discovery_critical = if broad_empty_target_bootstrap_enabled {
                irrelevant_observed_swap_requires_discovery_critical_persistence_old(
                    &swap,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    &discovery_critical_target_buy_mints,
                )
            } else {
                irrelevant_observed_swap_requires_discovery_critical_persistence(
                    &swap,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    &discovery_critical_target_buy_mints,
                )
            };
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
                    discovery_critical,
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
                    "writer failed to establish clean post-checkpoint throughput before empty-target discovery-critical backpressure scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        };

        let mut first_backpressure_pending_requests = 0usize;
        let mut max_pending_requests_before_pause = 0usize;
        let mut dropped_irrelevant_swaps = 0usize;
        let mut next_idx = 64usize;
        let max_consumer_swaps = 64
            + OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
            + DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY
            + 4_096;

        while next_idx < max_consumer_swaps
            && !pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps)
        {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-empty-target-burst-{next_idx:05}"),
                next_idx,
                scenario_now,
            );
            next_idx = next_idx.saturating_add(1);
            let discovery_critical = if broad_empty_target_bootstrap_enabled {
                irrelevant_observed_swap_requires_discovery_critical_persistence_old(
                    &swap,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    &discovery_critical_target_buy_mints,
                )
            } else {
                irrelevant_observed_swap_requires_discovery_critical_persistence(
                    &swap,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    &discovery_critical_target_buy_mints,
                )
            };
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
                    discovery_critical,
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
                    }
                    if should_buffer_backpressured_irrelevant_observed_swap(
                        discovery_critical,
                        &follow_snapshot,
                        &open_shadow_lots,
                        shadow_strategy_fail_closed,
                    ) {
                        pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                            swap,
                            discovery_critical,
                            processing_started_at,
                            backpressure_started_at: StdInstant::now(),
                            last_backpressure_log_at: None,
                        });
                    } else {
                        dropped_irrelevant_swaps = dropped_irrelevant_swaps.saturating_add(1);
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
                    &format!("sig-empty-target-upstream-short-{idx:05}"),
                    idx,
                    scenario_now,
                ));
            }
            upstream_queue_depth_shortly_after_pause = upstream.len();
            while upstream.len() < 2_048 {
                let idx = next_idx;
                next_idx = next_idx.saturating_add(1);
                upstream.push_back(irrelevant_backpressure_swap(
                    &format!("sig-empty-target-upstream-long-{idx:05}"),
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

        Ok(EmptyTargetDiscoveryCriticalBackpressureSummary {
            baseline_rows_persisted,
            first_backpressure_pending_requests,
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
            dropped_irrelevant_swaps,
            ingestion_paused_by_pending_irrelevant_queue,
        })
    }

    fn run_broad_unique_buy_fallback_backpressure_scenario(
        broad_unique_buy_fallback_enabled: bool,
    ) -> Result<BroadUniqueBuyFallbackBackpressureSummary> {
        let _contention_guard = sqlite_contention_delta_test_guard();
        let (store, db_path) = make_test_store("broad-unique-buy-fallback-backpressure-plateau")?;
        seed_runtime_raw_insert_backpressure(&db_path)?;
        let broad_unique_buy_mints: Vec<String> = (0..(OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
            + DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY
            + 4_096))
            .map(|idx| format!("token-irrelevant-{idx:05}"))
            .collect();
        let broad_unique_buy_mint_refs: Vec<&str> =
            broad_unique_buy_mints.iter().map(String::as_str).collect();
        seed_test_discovery_critical_target_buy_mints(&store, &[], &broad_unique_buy_mint_refs)?;
        let runtime_store = SqliteStore::open(Path::new(&db_path))?;
        runtime_store.checkpoint_wal_truncate()?;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let writer = ObservedSwapWriter::start_for_test(db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(), OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE)?;
        runtime_store.checkpoint_wal_truncate()?;

        let scenario_now = DateTime::parse_from_rfc3339("2026-04-08T11:47:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let shadow_strategy_fail_closed = true;
        let contention_before = sqlite_contention_snapshot();
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();
        let mut pending_irrelevant_swaps = VecDeque::new();
        let processing_started_at = StdInstant::now();
        let mut discovery_critical_target_buy_mints = HashSet::new();
        if broad_unique_buy_fallback_enabled {
            refresh_discovery_critical_target_buy_mints_or_warn_old(
                &store,
                &mut discovery_critical_target_buy_mints,
            )?;
        } else {
            refresh_discovery_critical_target_buy_mints_or_warn(
                &store,
                &mut discovery_critical_target_buy_mints,
            )?;
        }

        for idx in 0..64usize {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-broad-fallback-baseline-{idx:04}"),
                idx,
                scenario_now,
            );
            let discovery_critical =
                irrelevant_observed_swap_requires_discovery_critical_persistence(
                    &swap,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    &discovery_critical_target_buy_mints,
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
                    discovery_critical,
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
                    "writer failed to establish clean post-checkpoint throughput before broad unique-buy fallback backpressure scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        };

        let mut first_backpressure_pending_requests = 0usize;
        let mut max_pending_requests_before_pause = 0usize;
        let mut dropped_irrelevant_swaps = 0usize;
        let mut next_idx = 64usize;
        let max_consumer_swaps = 64
            + OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
            + DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY
            + 4_096;

        while next_idx < max_consumer_swaps
            && !pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps)
        {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-broad-fallback-burst-{next_idx:05}"),
                next_idx,
                scenario_now,
            );
            next_idx = next_idx.saturating_add(1);
            let discovery_critical =
                irrelevant_observed_swap_requires_discovery_critical_persistence(
                    &swap,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    &discovery_critical_target_buy_mints,
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
                    discovery_critical,
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
                    }
                    if should_buffer_backpressured_irrelevant_observed_swap(
                        discovery_critical,
                        &follow_snapshot,
                        &open_shadow_lots,
                        shadow_strategy_fail_closed,
                    ) {
                        pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                            swap,
                            discovery_critical,
                            processing_started_at,
                            backpressure_started_at: StdInstant::now(),
                            last_backpressure_log_at: None,
                        });
                    } else {
                        dropped_irrelevant_swaps = dropped_irrelevant_swaps.saturating_add(1);
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
                    &format!("sig-broad-fallback-upstream-short-{idx:05}"),
                    idx,
                    scenario_now,
                ));
            }
            upstream_queue_depth_shortly_after_pause = upstream.len();
            while upstream.len() < 2_048 {
                let idx = next_idx;
                next_idx = next_idx.saturating_add(1);
                upstream.push_back(irrelevant_backpressure_swap(
                    &format!("sig-broad-fallback-upstream-long-{idx:05}"),
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

        Ok(BroadUniqueBuyFallbackBackpressureSummary {
            baseline_rows_persisted,
            first_backpressure_pending_requests,
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
            loaded_target_buy_mints: discovery_critical_target_buy_mints.len(),
            dropped_irrelevant_swaps,
            ingestion_paused_by_pending_irrelevant_queue,
        })
    }
