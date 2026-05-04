    fn run_discovery_critical_pending_backlog_output_saturation_scenario(
        block_ingestion_on_full_pending_backlog: bool,
    ) -> Result<DiscoveryCriticalPendingBacklogOutputSaturationSummary> {
        let (_store, db_path) =
            make_test_store("discovery-critical-pending-backlog-output-saturation")?;
        seed_runtime_raw_insert_backpressure(&db_path)?;
        let runtime_store = SqliteStore::open(Path::new(&db_path))?;
        runtime_store.checkpoint_wal_truncate()?;

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
        runtime_store.checkpoint_wal_truncate()?;

        let scenario_now = DateTime::parse_from_rfc3339("2026-04-08T18:09:27Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let contention_before = sqlite_contention_snapshot();
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();

        for idx in 0..64usize {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-discovery-critical-backlog-baseline-{idx:04}"),
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
                    "writer failed to establish clean post-checkpoint throughput before the discovery-critical pending-backlog output saturation scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        };

        let baseline_queue_drain_started = StdInstant::now();
        while writer.snapshot().pending_requests > 0 {
            if baseline_queue_drain_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "writer failed to drain its clean-start baseline backlog before the discovery-critical pending-backlog output saturation scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }

        let mut writer_pending_requests_at_plateau = 0usize;
        for idx in 64..(64 + TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE + 64) {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-discovery-critical-backlog-plateau-{idx:04}"),
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
            if matches!(
                outcome,
                IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure
            ) {
                forget_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                );
                writer_pending_requests_at_plateau = writer.snapshot().pending_requests;
                break;
            }
        }
        if writer_pending_requests_at_plateau == 0 {
            anyhow::bail!(
                "writer failed to reproduce the clean-start 128 pending plateau before the discovery-critical pending-backlog output saturation scenario"
            );
        }

        let snapshot_at_plateau = writer.snapshot();
        let discovery_critical_target_buy_mints = HashSet::from(["token-target".to_string()]);
        let mut pending_irrelevant_swaps = VecDeque::new();
        let processing_started_at = StdInstant::now();
        while pending_irrelevant_swaps.len() < DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY {
            let idx = pending_irrelevant_swaps.len();
            let mut swap = irrelevant_backpressure_swap(
                &format!("sig-discovery-critical-pending-{idx:05}"),
                idx,
                scenario_now,
            );
            swap.token_out = "token-target".to_string();
            pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                swap,
                discovery_critical: true,
                processing_started_at,
                backpressure_started_at: StdInstant::now(),
                last_backpressure_log_at: None,
            });
        }

        let mut upstream = VecDeque::new();
        while upstream.len() < 2_048 {
            let idx = upstream.len();
            let mut swap = irrelevant_backpressure_swap(
                &format!("sig-discovery-critical-upstream-{idx:05}"),
                idx,
                scenario_now,
            );
            swap.token_out = "token-target".to_string();
            upstream.push_back(swap);
        }

        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let shadow_strategy_fail_closed = true;
        let upstream_queue_depth_before_loop = upstream.len();
        let mut dropped_over_capacity_discovery_critical_irrelevant_swaps = 0usize;
        let mut ingestion_polls_attempted = 0usize;

        while let Some(swap) = upstream.pop_front() {
            let discovery_critical =
                irrelevant_observed_swap_requires_discovery_critical_persistence(
                    &swap,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    &discovery_critical_target_buy_mints,
                );
            assert!(
                discovery_critical,
                "the exact remaining live class should be driven by discovery-critical irrelevant target-mint swaps, not by non-critical traffic"
            );

            if block_ingestion_on_full_pending_backlog
                && pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps)
            {
                upstream.push_front(swap);
                break;
            }

            ingestion_polls_attempted = ingestion_polls_attempted.saturating_add(1);
            if should_drop_backpressured_discovery_critical_irrelevant_observed_swap(
                &pending_irrelevant_swaps,
            ) {
                dropped_over_capacity_discovery_critical_irrelevant_swaps =
                    dropped_over_capacity_discovery_critical_irrelevant_swaps.saturating_add(1);
                continue;
            }

            pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                swap,
                discovery_critical: true,
                processing_started_at,
                backpressure_started_at: StdInstant::now(),
                last_backpressure_log_at: None,
            });
        }

        writer.shutdown()?;
        let contention_after = sqlite_contention_snapshot();

        assert_eq!(
            contention_after
                .write_retry_total
                .saturating_sub(contention_before.write_retry_total),
            0,
            "the reduced live-like reproduction should not require sqlite retry churn"
        );
        assert_eq!(
            contention_after
                .busy_error_total
                .saturating_sub(contention_before.busy_error_total),
            0,
            "the reduced live-like reproduction should not require sqlite busy-error churn"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));

        Ok(DiscoveryCriticalPendingBacklogOutputSaturationSummary {
            baseline_rows_persisted,
            writer_pending_requests_at_plateau,
            aggregate_queue_depth_at_plateau: snapshot_at_plateau.aggregate_queue_depth_batches,
            journal_queue_depth_at_plateau: snapshot_at_plateau.journal_queue_depth_batches,
            upstream_queue_depth_before_loop,
            upstream_queue_depth_after_loop: upstream.len(),
            pending_irrelevant_queue_depth: pending_irrelevant_swaps.len(),
            dropped_over_capacity_discovery_critical_irrelevant_swaps,
            ingestion_polls_attempted,
        })
    }

    fn run_discovery_critical_backpressure_refresh_output_saturation_scenario(
        throttle_backpressure_refresh: bool,
    ) -> Result<DiscoveryCriticalBackpressureRefreshOutputSaturationSummary> {
        let (store, db_path) =
            make_test_store("discovery-critical-backpressure-refresh-output-saturation")?;
        seed_runtime_raw_insert_backpressure(&db_path)?;
        seed_test_discovery_critical_target_buy_mints(
            &store,
            &["token-target"],
            &["token-target"],
        )?;
        let runtime_store = SqliteStore::open(Path::new(&db_path))?;
        runtime_store.checkpoint_wal_truncate()?;

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
        runtime_store.checkpoint_wal_truncate()?;

        let scenario_now = DateTime::parse_from_rfc3339("2026-04-08T18:56:42Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();

        for idx in 0..64usize {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-backpressure-refresh-baseline-{idx:04}"),
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
                    "writer failed to establish clean post-checkpoint throughput before the discovery-critical refresh output saturation scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        };

        let baseline_queue_drain_started = StdInstant::now();
        while writer.snapshot().pending_requests > 0 {
            if baseline_queue_drain_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "writer failed to drain its clean-start baseline backlog before the discovery-critical refresh output saturation scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }

        let mut writer_pending_requests_at_plateau = 0usize;
        for idx in 64..(64 + TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE + 64) {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-backpressure-refresh-plateau-{idx:04}"),
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
            if matches!(
                outcome,
                IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure
            ) {
                forget_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                );
                writer_pending_requests_at_plateau = writer.snapshot().pending_requests;
                break;
            }
        }
        if writer_pending_requests_at_plateau == 0 {
            anyhow::bail!(
                "writer failed to reproduce the clean-start 128 pending plateau before the discovery-critical refresh output saturation scenario"
            );
        }

        let snapshot_at_plateau = writer.snapshot();
        let persisted_target_buy_mints = HashSet::from(["token-target".to_string()]);
        let mut discovery_critical_target_buy_mints = persisted_target_buy_mints.clone();
        let mut pending_irrelevant_swaps = VecDeque::new();
        let processing_started_at = StdInstant::now();
        while pending_irrelevant_swaps.len() < DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY {
            let idx = pending_irrelevant_swaps.len();
            let mut swap = irrelevant_backpressure_swap(
                &format!("sig-backpressure-refresh-pending-{idx:05}"),
                idx,
                scenario_now,
            );
            swap.token_out = "token-target".to_string();
            pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                swap,
                discovery_critical: true,
                processing_started_at,
                backpressure_started_at: StdInstant::now(),
                last_backpressure_log_at: None,
            });
        }

        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let shadow_strategy_fail_closed = true;
        let refresh_now = StdInstant::now();
        let mut backpressure_refresh_state =
            DiscoveryCriticalTargetBuyMintsBackpressureRefreshState::default();
        let mut backpressure_refresh_budget_units_remaining =
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE;
        let mut backpressure_refresh_attempts = 0usize;
        let mut dropped_over_capacity_discovery_critical_irrelevant_swaps = 0usize;
        let mut ingestion_polls_attempted = 0usize;
        let mut upstream = VecDeque::new();
        while upstream.len() < 2_048 {
            let idx = upstream.len();
            let mut swap = irrelevant_backpressure_swap(
                &format!("sig-backpressure-refresh-upstream-{idx:05}"),
                idx,
                scenario_now,
            );
            swap.token_out = "token-target".to_string();
            upstream.push_back(swap);
        }
        let upstream_queue_depth_before_loop = upstream.len();

        while let Some(swap) = upstream.pop_front() {
            let should_refresh = if throttle_backpressure_refresh {
                should_refresh_discovery_critical_target_buy_mints_for_backpressure(
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    Some(&mut backpressure_refresh_state),
                    refresh_now,
                )
            } else {
                should_refresh_discovery_critical_target_buy_mints_for_backpressure(
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    None,
                    refresh_now,
                )
            };

            if should_refresh {
                if backpressure_refresh_budget_units_remaining == 0 {
                    upstream.push_front(swap);
                    break;
                }
                backpressure_refresh_budget_units_remaining =
                    backpressure_refresh_budget_units_remaining.saturating_sub(1);
                backpressure_refresh_attempts = backpressure_refresh_attempts.saturating_add(1);
                discovery_critical_target_buy_mints = persisted_target_buy_mints.clone();
            }

            let discovery_critical =
                irrelevant_observed_swap_requires_discovery_critical_persistence(
                    &swap,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    &discovery_critical_target_buy_mints,
                );
            assert!(
                discovery_critical,
                "the remaining live class should still be driven by exact target-mint discovery-critical irrelevant swaps, not by aggregate or journal work"
            );

            ingestion_polls_attempted = ingestion_polls_attempted.saturating_add(1);
            if should_drop_backpressured_discovery_critical_irrelevant_observed_swap(
                &pending_irrelevant_swaps,
            ) {
                dropped_over_capacity_discovery_critical_irrelevant_swaps =
                    dropped_over_capacity_discovery_critical_irrelevant_swaps.saturating_add(1);
                continue;
            }

            pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                swap,
                discovery_critical: true,
                processing_started_at,
                backpressure_started_at: StdInstant::now(),
                last_backpressure_log_at: None,
            });
        }

        writer.shutdown()?;

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));

        Ok(
            DiscoveryCriticalBackpressureRefreshOutputSaturationSummary {
                baseline_rows_persisted,
                writer_pending_requests_at_plateau,
                aggregate_queue_depth_at_plateau: snapshot_at_plateau.aggregate_queue_depth_batches,
                journal_queue_depth_at_plateau: snapshot_at_plateau.journal_queue_depth_batches,
                upstream_queue_depth_before_loop,
                upstream_queue_depth_after_loop: upstream.len(),
                pending_irrelevant_queue_depth: pending_irrelevant_swaps.len(),
                dropped_over_capacity_discovery_critical_irrelevant_swaps,
                ingestion_polls_attempted,
                backpressure_refresh_attempts,
                backpressure_refresh_budget_units_spent: TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE
                    .saturating_sub(backpressure_refresh_budget_units_remaining),
            },
        )
    }
