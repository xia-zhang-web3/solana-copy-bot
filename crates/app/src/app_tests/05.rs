    fn run_pending_retry_discovery_critical_backpressure_refresh_output_saturation_scenario(
        throttle_retry_refresh: bool,
    ) -> Result<DiscoveryCriticalBackpressureRefreshOutputSaturationSummary> {
        let (store, db_path) =
            make_test_store("pending-retry-discovery-critical-refresh-output-saturation")?;
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
        let writer = ObservedSwapWriter::start_for_test(db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(), OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE)?;
        runtime_store.checkpoint_wal_truncate()?;

        let scenario_now = DateTime::parse_from_rfc3339("2026-04-09T08:25:10Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();

        for idx in 0..64usize {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-pending-retry-refresh-baseline-{idx:04}"),
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
                    "writer failed to establish clean post-checkpoint throughput before the pending-retry discovery-critical refresh output saturation scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        };

        let baseline_queue_drain_started = StdInstant::now();
        while writer.snapshot().pending_requests > 0 {
            if baseline_queue_drain_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "writer failed to drain its clean-start baseline backlog before the pending-retry discovery-critical refresh output saturation scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }

        let mut writer_pending_requests_at_plateau = 0usize;
        for idx in 64..(64 + TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE + 64) {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-pending-retry-refresh-plateau-{idx:04}"),
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
                "writer failed to reproduce the clean-start 128 pending plateau before the pending-retry discovery-critical refresh output saturation scenario"
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
                &format!("sig-pending-retry-refresh-pending-{idx:05}"),
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
        let retry_refresh_now = StdInstant::now();
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
                &format!("sig-pending-retry-refresh-upstream-{idx:05}"),
                idx,
                scenario_now,
            );
            swap.token_out = "token-target".to_string();
            upstream.push_back(swap);
        }
        let upstream_queue_depth_before_loop = upstream.len();

        while let Some(swap) = upstream.pop_front() {
            let should_refresh = if throttle_retry_refresh {
                refresh_discovery_critical_target_buy_mints_for_backpressure_if_due(
                    &store,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    &mut discovery_critical_target_buy_mints,
                    &mut backpressure_refresh_state,
                    retry_refresh_now,
                )?
            } else {
                refresh_discovery_critical_target_buy_mints_or_warn(
                    &store,
                    &mut discovery_critical_target_buy_mints,
                )?;
                true
            };

            if should_refresh {
                if backpressure_refresh_budget_units_remaining == 0 {
                    upstream.push_front(swap);
                    break;
                }
                backpressure_refresh_budget_units_remaining =
                    backpressure_refresh_budget_units_remaining.saturating_sub(1);
                backpressure_refresh_attempts = backpressure_refresh_attempts.saturating_add(1);
            }

            assert!(
                pending_irrelevant_swap_queue_is_full(&pending_irrelevant_swaps),
                "the exact reduced repro requires a full bounded pending backlog so the retry loop keeps spinning on the same over-capacity discovery-critical class"
            );

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
                "the retry-loop repro must stay on the same discovery-critical target-mint class that live keeps buffering under fail-closed zero-universe pressure"
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
