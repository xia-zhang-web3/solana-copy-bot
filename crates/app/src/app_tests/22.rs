    #[test]
    fn zero_universe_noncritical_best_effort_exhaustion_refills_after_writer_pressure_clears_stage1(
    ) -> Result<()> {
        let now = StdInstant::now();
        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let target_buy_mints = HashSet::new();
        let writer_snapshot = maintenance_test_writer_snapshot();
        let mut best_effort_state = ZeroUniverseEmptyTargetNoncriticalBestEffortState::default();
        best_effort_state.mark_exhausted(
            now - ZERO_UNIVERSE_EMPTY_TARGET_NONCRITICAL_BEST_EFFORT_REFILL_COOLDOWN
                - StdDuration::from_secs(1),
        );

        assert!(
            should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
                false,
                &follow_snapshot,
                &open_shadow_lots,
                true,
                &target_buy_mints,
                best_effort_state.exhausted(),
            ),
            "the exact zero-universe empty-target non-critical class should stay dropped until pressure drains and the bounded refill cooldown elapses"
        );

        best_effort_state.refresh_after_writer_pressure_clears(
            &follow_snapshot,
            &open_shadow_lots,
            true,
            &target_buy_mints,
            &writer_snapshot,
            now,
        );

        assert!(
            !best_effort_state.exhausted(),
            "once writer pressure has drained and cooldown elapsed, the empty-target non-critical raw evidence latch must admit another bounded wave"
        );
        assert!(
            !should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
                false,
                &follow_snapshot,
                &open_shadow_lots,
                true,
                &target_buy_mints,
                best_effort_state.exhausted(),
            ),
            "clearing the latch must restore the normal bounded writer admission path without changing relevance"
        );

        let (_store, db_path) =
            make_test_store("zero-universe-noncritical-best-effort-refill-enqueues")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let writer = ObservedSwapWriter::start_for_test(db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(), OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE)?;
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();
        let swap = irrelevant_backpressure_swap("sig-zero-universe-refilled-wave", 0, Utc::now());
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
        assert_eq!(
            outcome,
            IrrelevantObservedSwapEnqueueOutcome::Enqueued,
            "a cleared latch must allow at least one new bounded raw evidence wave to enter the writer"
        );

        writer.shutdown()?;
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
        Ok(())
    }

    #[test]
    fn zero_universe_noncritical_best_effort_exhaustion_does_not_refill_while_writer_pressure_active_stage1(
    ) {
        let now = StdInstant::now();
        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let target_buy_mints = HashSet::new();
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests = 1;
        let mut best_effort_state = ZeroUniverseEmptyTargetNoncriticalBestEffortState::default();
        best_effort_state.mark_exhausted(
            now - ZERO_UNIVERSE_EMPTY_TARGET_NONCRITICAL_BEST_EFFORT_REFILL_COOLDOWN
                - StdDuration::from_secs(1),
        );

        best_effort_state.refresh_after_writer_pressure_clears(
            &follow_snapshot,
            &open_shadow_lots,
            true,
            &target_buy_mints,
            &writer_snapshot,
            now,
        );

        assert!(
            best_effort_state.exhausted(),
            "the empty-target non-critical raw evidence latch must not refill while writer pressure is still active"
        );
        assert!(
            should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
                false,
                &follow_snapshot,
                &open_shadow_lots,
                true,
                &target_buy_mints,
                best_effort_state.exhausted(),
            ),
            "active writer pressure must keep the conservative pre-enqueue drop guard armed"
        );
    }

    #[test]
    fn zero_universe_noncritical_best_effort_exhaustion_does_not_block_discovery_critical_reserved_path_stage1(
    ) {
        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let target_buy_mints = HashSet::new();
        let mut best_effort_state = ZeroUniverseEmptyTargetNoncriticalBestEffortState::default();
        best_effort_state.mark_exhausted(StdInstant::now());

        assert!(
            !should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
                true,
                &follow_snapshot,
                &open_shadow_lots,
                true,
                &target_buy_mints,
                best_effort_state.exhausted(),
            ),
            "discovery-critical irrelevant swaps must remain on the reserved raw evidence path even when the non-critical zero-universe latch is exhausted"
        );
    }

    #[test]
    fn zero_universe_empty_target_noncritical_zero_output_pressure_recreates_repeated_128_refill_waves_stage1(
    ) -> Result<()> {
        let summary = run_zero_universe_empty_target_noncritical_irrelevant_refill_scenario(false)?;

        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should still write normally before the exact 7093fa4 class begins: {summary:?}"
        );
        assert_eq!(
            summary.yellowstone_output_queue_depth, 0,
            "the exact 7093fa4 repro must keep Yellowstone output queue depth at zero so the last output-pressure theory is ruled out for this batch: {summary:?}"
        );
        assert_eq!(
            summary.yellowstone_output_queue_fill_ratio, 0.0,
            "the exact 7093fa4 repro must keep Yellowstone output queue fill ratio at zero so no pressure-gated drop path is active: {summary:?}"
        );
        assert_eq!(
            summary.first_backpressure_pending_requests,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the remaining live class must still hit the one-batch raw-writer soft limit of 128 before any output pressure appears: {summary:?}"
        );
        assert_eq!(
            summary.writer_pending_requests_peak,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the zero-output-pressure class should saturate exactly at the one-batch non-critical writer budget, not above it: {summary:?}"
        );
        assert!(
            !summary.first_backpressure_discovery_critical,
            "the exact 7093fa4 class must be occupied by non-critical irrelevant swaps, not discovery-critical ones: {summary:?}"
        );
        assert!(
            summary.best_effort_budget_exhausted,
            "the reduced live-like repro must actually hit the post-backpressure refill condition, not only the initial one-batch plateau: {summary:?}"
        );
        assert!(
            summary.journal_queue_depth_at_peak <= 1,
            "journal queue must remain in the same low 0..1 class in the exact 7093fa4 repro: {summary:?}"
        );
        assert!(
            summary.accepted_noncritical_irrelevant_swaps
                >= TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "current logic must actually spend the full one-batch raw-writer budget on non-critical irrelevant swaps before first backpressure: {summary:?}"
        );
        assert!(
            summary.completed_waves >= 4,
            "old/current logic must keep refilling the same one-batch plateau in repeated zero-output-pressure waves once the writer drains, which is the exact surviving live class: {summary:?}"
        );
        Ok(())
    }

    #[test]
    fn zero_universe_empty_target_noncritical_exhaustion_preserves_first_batch_and_stops_zero_output_pressure_refill_waves_stage1(
    ) -> Result<()> {
        let old = run_zero_universe_empty_target_noncritical_irrelevant_refill_scenario(false)?;
        let new = run_zero_universe_empty_target_noncritical_irrelevant_refill_scenario(true)?;

        assert_eq!(
            old.first_backpressure_pending_requests, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "old side must reproduce the exact zero-output-pressure 128 plateau first: old={old:?}"
        );
        assert!(
            old.journal_queue_depth_at_peak <= 1,
            "old side must keep journal queue in the low 0..1 class: old={old:?}"
        );
        assert_eq!(
            old.yellowstone_output_queue_depth, 0,
            "old side must keep Yellowstone output queue at zero: old={old:?}"
        );
        assert_eq!(
            old.yellowstone_output_queue_fill_ratio, 0.0,
            "old side must keep Yellowstone output pressure inactive: old={old:?}"
        );
        assert_eq!(
            new.first_backpressure_pending_requests, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the fix must preserve the intended first one-batch best-effort raw persistence contract for exact empty-target zero-universe generic traffic: new={new:?}"
        );
        assert!(
            new.accepted_noncritical_irrelevant_swaps
                >= TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE
                && new.accepted_noncritical_irrelevant_swaps
                    <= TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE + 1,
            "the fix must still persist the initial one-batch best-effort generic slice, plus at most one reclaimed slot, before the exact refill gate engages: new={new:?}"
        );
        assert_eq!(
            new.writer_pending_requests_peak, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the fix must preserve the initial one-batch raw-writer occupancy rather than globally bypassing the generic best-effort contract: new={new:?}"
        );
        assert!(
            new.dropped_noncritical_irrelevant_swaps > 0,
            "after the first batch is exhausted, the fix should make the ownership change explicit by dropping only the later refill waves of the same exact non-critical irrelevant class: new={new:?}"
        );
        assert_eq!(
            new.journal_queue_depth_at_peak, 0,
            "the fix must stay off the journal path for this exact class: new={new:?}"
        );
        assert_eq!(
            new.completed_waves, 1,
            "the fix should keep exactly one best-effort batch for the empty-target zero-universe class, then stop the repeated refill waves that old/current logic kept re-arming: old={old:?} new={new:?}"
        );
        assert!(
            old.completed_waves > new.completed_waves,
            "A/B must prove the surviving class was repeated post-backpressure refill of the same empty-target non-critical batch, not the initial preserved one-batch contract itself: old={old:?} new={new:?}"
        );
        Ok(())
    }

    #[test]
    fn zero_universe_empty_target_noncritical_exhaustion_clears_when_store_backed_exact_target_mints_appear_stage1(
    ) -> Result<()> {
        let (store, db_path) =
            make_test_store("zero-universe-empty-target-noncritical-exhaustion-clears")?;
        seed_test_discovery_critical_target_buy_mints(&store, &["token-target"], &[])?;
        let mut best_effort_state = ZeroUniverseEmptyTargetNoncriticalBestEffortState::default();
        best_effort_state.mark_exhausted(StdInstant::now());
        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let mut target_buy_mints = HashSet::new();
        let mut target_swap = test_swap("sig-empty-target-exhaustion-clears");
        target_swap.token_out = "token-target".to_string();

        reset_zero_universe_empty_target_noncritical_best_effort_exhaustion_if_context_changed(
            &mut best_effort_state,
            &follow_snapshot,
            &open_shadow_lots,
            true,
            &target_buy_mints,
        );
        assert!(
            best_effort_state.exhausted(),
            "the exact empty-target zero-universe context should keep the refill gate armed until the context itself changes"
        );

        refresh_discovery_critical_target_buy_mints_or_warn(&store, &mut target_buy_mints)?;
        assert_eq!(
            target_buy_mints,
            HashSet::from(["token-target".to_string()]),
            "the same store-backed persisted rebuild seam that app uses at runtime must surface the exact target-mint set before the refill gate can clear"
        );
        reset_zero_universe_empty_target_noncritical_best_effort_exhaustion_if_context_changed(
            &mut best_effort_state,
            &follow_snapshot,
            &open_shadow_lots,
            true,
            &target_buy_mints,
        );
        assert!(
            !best_effort_state.exhausted(),
            "once exact target mints appear, the empty-target non-critical refill gate must clear so recovery can proceed on the precise discovery-critical path"
        );
        assert!(
            irrelevant_observed_swap_requires_discovery_critical_persistence(
                &target_swap,
                &follow_snapshot,
                &open_shadow_lots,
                true,
                &target_buy_mints,
            ),
            "the same context change that clears the refill gate must simultaneously make the exact target-mint SOL buy discovery-critical"
        );
        assert!(
            !should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
                true,
                &follow_snapshot,
                &open_shadow_lots,
                true,
                &target_buy_mints,
                true,
            ),
            "the exact discovery-critical recovery path must not be blocked by the earlier empty-target refill gate"
        );
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn enqueue_irrelevant_observed_swap_noncritical_hits_live_128_plateau_stage1() -> Result<()> {
        let (_store, db_path) = make_test_store("irrelevant-noncritical-live-128-plateau")?;
        let blocker_conn = rusqlite::Connection::open(&db_path)?;
        blocker_conn.busy_timeout(StdDuration::from_millis(1))?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let writer = ObservedSwapWriter::start_for_test(db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(), OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE)?;
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();

        for idx in 0..TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-live-128-noncritical-{idx:03}"),
                idx,
                Utc::now(),
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
            "sig-live-128-noncritical-backpressure",
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            Utc::now(),
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
        let snapshot = writer.snapshot();
        assert_eq!(
            snapshot.pending_requests, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the exact live 128 plateau is the non-critical irrelevant try_enqueue soft limit"
        );

        blocker_conn.execute_batch("COMMIT")?;
        let drain_started = StdInstant::now();
        while writer.snapshot().pending_requests > 0 {
            if drain_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("writer failed to drain after non-critical live-128 plateau test");
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
    fn enqueue_irrelevant_observed_swap_discovery_critical_reserved_branch_exceeds_live_128_plateau_stage1(
    ) -> Result<()> {
        let (_store, db_path) = make_test_store("irrelevant-discovery-critical-exceeds-live-128")?;
        let blocker_conn = rusqlite::Connection::open(&db_path)?;
        blocker_conn.busy_timeout(StdDuration::from_millis(1))?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let writer = ObservedSwapWriter::start_for_test(db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(), OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE)?;
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();

        for idx in 0..=TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE {
            let swap = irrelevant_backpressure_swap(
                &format!("sig-live-128-critical-{idx:03}"),
                idx,
                Utc::now(),
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
                    true,
                )
                .await
            })?;
            assert_eq!(
                outcome,
                IrrelevantObservedSwapEnqueueOutcome::Enqueued,
                "discovery-critical irrelevant swaps bypass the 128 soft limit and should still enqueue beyond it"
            );
        }

        let snapshot = writer.snapshot();
        assert!(
            snapshot.pending_requests > TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the discovery-critical reserved branch cannot be the exact live 128 plateau because it keeps enqueueing past 128"
        );

        blocker_conn.execute_batch("COMMIT")?;
        let drain_started = StdInstant::now();
        while writer.snapshot().pending_requests > 0 {
            if drain_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "writer failed to drain after discovery-critical reserved-branch contrast test"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
        writer.shutdown()?;

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
        Ok(())
    }
