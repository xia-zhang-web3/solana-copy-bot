    fn run_irrelevant_observed_swap_no_ownership_surface_saturation_scenario(
        branch: IrrelevantObservedSwapScenarioBranch,
        simulate_old_not_followed_drop_before_writer: bool,
    ) -> Result<IrrelevantNotFollowedNoOwnershipSurfaceSummary> {
        let test_store_name = format!(
            "irrelevant-not-followed-no-ownership-saturation-{branch:?}-old-drop-{simulate_old_not_followed_drop_before_writer}"
        );
        let (_store, db_path) = make_test_store(&test_store_name)?;
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

        let scenario_now = DateTime::parse_from_rfc3339("2026-04-12T17:44:57Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();
        let mut writer_pending_requests_peak = 0usize;
        let mut journal_queue_depth_at_peak = 0usize;
        let mut first_backpressure_pending_requests = 0usize;
        let mut dropped_swaps = 0usize;
        let mut accepted_swaps = 0usize;
        let mut completed_waves = 0usize;

        for idx in 0..(TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE * 4) {
            let swap = irrelevant_observed_swap_for_scenario_branch(
                branch,
                &format!("sig-not-followed-no-ownership-{idx:04}"),
                idx,
                scenario_now,
            );
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            let discovery_critical = false;
            let relevance = classify_observed_swap_shadow_relevance(
                &swap,
                &follow_snapshot,
                &ShadowScheduler::new(),
                &open_shadow_lots,
            );
            match branch {
                IrrelevantObservedSwapScenarioBranch::NotFollowed => {
                    assert!(
                        matches!(
                            relevance,
                            ObservedSwapShadowRelevance::IrrelevantNotFollowed(_)
                        ),
                        "the reduced scenario must stay on the exact proven live irrelevant_not_followed class"
                    );
                }
                IrrelevantObservedSwapScenarioBranch::Unclassified => {
                    assert_eq!(
                        relevance,
                        ObservedSwapShadowRelevance::IrrelevantUnclassified,
                        "the unchanged contrast scenario must remain on the unclassified branch"
                    );
                }
            }

            if simulate_old_not_followed_drop_before_writer
                && matches!(
                    relevance,
                    ObservedSwapShadowRelevance::IrrelevantNotFollowed(_)
                )
            {
                dropped_swaps = dropped_swaps.saturating_add(1);
                forget_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                );
                continue;
            }

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
            let snapshot = writer.snapshot();
            writer_pending_requests_peak =
                writer_pending_requests_peak.max(snapshot.pending_requests);
            journal_queue_depth_at_peak =
                journal_queue_depth_at_peak.max(snapshot.journal_queue_depth_batches);
            match outcome {
                IrrelevantObservedSwapEnqueueOutcome::Enqueued => {
                    accepted_swaps = accepted_swaps.saturating_add(1);
                }
                IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure => {
                    dropped_swaps = dropped_swaps.saturating_add(1);
                    if first_backpressure_pending_requests == 0 {
                        first_backpressure_pending_requests = snapshot.pending_requests;
                    }
                    forget_recent_swap_signature(
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &swap.signature,
                    );
                    completed_waves = completed_waves.saturating_add(1);
                    let drain_started = StdInstant::now();
                    while writer.snapshot().pending_requests > 0 {
                        if drain_started.elapsed() > StdDuration::from_secs(10) {
                            anyhow::bail!(
                                "writer failed to drain a bounded irrelevant_not_followed no-ownership wave"
                            );
                        }
                        std::thread::sleep(StdDuration::from_millis(20));
                    }
                    if completed_waves >= 4 {
                        break;
                    }
                }
            }
        }

        writer.shutdown()?;
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));

        Ok(IrrelevantNotFollowedNoOwnershipSurfaceSummary {
            writer_pending_requests_peak,
            first_backpressure_pending_requests,
            journal_queue_depth_at_peak,
            dropped_swaps,
            accepted_swaps,
            completed_waves,
        })
    }

    fn run_zero_universe_empty_target_noncritical_irrelevant_refill_scenario(
        drop_after_empty_target_best_effort_budget_exhaustion: bool,
    ) -> Result<ZeroUniverseNoncriticalIrrelevantZeroOutputPressureSummary> {
        let (_store, db_path) =
            make_test_store("zero-universe-noncritical-irrelevant-zero-output-pressure")?;
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

        let scenario_now = DateTime::parse_from_rfc3339("2026-04-09T12:06:48Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();

        for idx in 0..64usize {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-zero-output-pressure-baseline-{idx:04}"),
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
                    "writer failed to establish clean post-checkpoint throughput before the zero-output-pressure non-critical scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        };

        let baseline_queue_drain_started = StdInstant::now();
        while writer.snapshot().pending_requests > 0 {
            if baseline_queue_drain_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "writer failed to drain its clean-start baseline backlog before the zero-output-pressure non-critical scenario"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }

        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let shadow_strategy_fail_closed = true;
        let discovery_critical_target_buy_mints = HashSet::new();
        let ingestion_snapshot = Some(infra_snapshot_with_yellowstone_queue(
            scenario_now,
            0,
            0,
            0,
            2_048,
            0,
        ));

        let mut empty_target_best_effort_state =
            ZeroUniverseEmptyTargetNoncriticalBestEffortState::default();
        let mut first_backpressure_pending_requests = 0usize;
        let mut writer_pending_requests_peak = 0usize;
        let mut journal_queue_depth_at_peak = 0usize;
        let mut accepted_noncritical_irrelevant_swaps = 0usize;
        let mut dropped_noncritical_irrelevant_swaps = 0usize;
        let mut first_backpressure_discovery_critical = false;
        let mut completed_waves = 0usize;

        for idx in 64..(64 + TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE * 8) {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-zero-output-pressure-burst-{idx:05}"),
                idx,
                scenario_now,
            );
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            let discovery_critical =
                irrelevant_observed_swap_requires_discovery_critical_persistence(
                    &swap,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    &discovery_critical_target_buy_mints,
                );
            assert!(
                !discovery_critical,
                "the exact 7093fa4 class must stay on non-critical irrelevant ownership, not discovery-critical persistence"
            );
            assert!(
                !should_preemptively_drop_noncritical_irrelevant_observed_swap_under_output_pressure(
                    discovery_critical,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    ingestion_snapshot,
                ),
                "the exact 7093fa4 class must reproduce with yellowstone_output_queue_depth=0 so the output-pressure path is provably inactive"
            );

            if drop_after_empty_target_best_effort_budget_exhaustion
                && should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
                    discovery_critical,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    &discovery_critical_target_buy_mints,
                    empty_target_best_effort_state.exhausted(),
                )
            {
                dropped_noncritical_irrelevant_swaps =
                    dropped_noncritical_irrelevant_swaps.saturating_add(1);
                forget_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                );
                continue;
            }

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
            let snapshot = writer.snapshot();
            writer_pending_requests_peak =
                writer_pending_requests_peak.max(snapshot.pending_requests);
            journal_queue_depth_at_peak =
                journal_queue_depth_at_peak.max(snapshot.journal_queue_depth_batches);

            match outcome {
                IrrelevantObservedSwapEnqueueOutcome::Enqueued => {
                    accepted_noncritical_irrelevant_swaps =
                        accepted_noncritical_irrelevant_swaps.saturating_add(1);
                }
                IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure => {
                    dropped_noncritical_irrelevant_swaps =
                        dropped_noncritical_irrelevant_swaps.saturating_add(1);
                    if first_backpressure_pending_requests == 0 {
                        first_backpressure_pending_requests = snapshot.pending_requests;
                        first_backpressure_discovery_critical = discovery_critical;
                    }
                    if zero_universe_empty_target_noncritical_irrelevant_context(
                        discovery_critical,
                        &follow_snapshot,
                        &open_shadow_lots,
                        shadow_strategy_fail_closed,
                        &discovery_critical_target_buy_mints,
                    ) {
                        empty_target_best_effort_state.mark_exhausted(StdInstant::now());
                    }
                    forget_recent_swap_signature(
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &swap.signature,
                    );
                    completed_waves = completed_waves.saturating_add(1);
                    let drain_started = StdInstant::now();
                    while writer.snapshot().pending_requests > 0 {
                        if drain_started.elapsed() > StdDuration::from_secs(10) {
                            anyhow::bail!(
                                "writer failed to drain a bounded zero-output-pressure non-critical wave after first backpressure"
                            );
                        }
                        std::thread::sleep(StdDuration::from_millis(20));
                    }
                    if completed_waves >= 4
                        && !drop_after_empty_target_best_effort_budget_exhaustion
                    {
                        break;
                    }
                }
            }
        }

        writer.shutdown()?;
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));

        Ok(ZeroUniverseNoncriticalIrrelevantZeroOutputPressureSummary {
            baseline_rows_persisted,
            first_backpressure_pending_requests,
            writer_pending_requests_peak,
            journal_queue_depth_at_peak,
            yellowstone_output_queue_depth: ingestion_snapshot
                .map(|snapshot| snapshot.yellowstone_output_queue_depth)
                .unwrap_or(0),
            yellowstone_output_queue_fill_ratio: ingestion_snapshot
                .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio)
                .unwrap_or(0.0),
            accepted_noncritical_irrelevant_swaps,
            dropped_noncritical_irrelevant_swaps,
            first_backpressure_discovery_critical,
            completed_waves,
            best_effort_budget_exhausted: empty_target_best_effort_state.exhausted(),
        })
    }
