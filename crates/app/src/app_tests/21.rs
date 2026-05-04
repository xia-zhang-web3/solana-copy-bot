    #[test]
    fn throttled_discovery_critical_backpressure_refresh_allows_output_queue_drain_stage1(
    ) -> Result<()> {
        let old = run_discovery_critical_backpressure_refresh_output_saturation_scenario(false)?;
        let new = run_discovery_critical_backpressure_refresh_output_saturation_scenario(true)?;

        assert_eq!(
            old.writer_pending_requests_at_plateau, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "old side must start from the same 128 raw-writer plateau: old={old:?}"
        );
        assert_eq!(
            new.writer_pending_requests_at_plateau,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "new side must preserve the same 128 raw-writer plateau so only the backpressure refresh cadence changes: new={new:?}"
        );
        assert_eq!(
            old.aggregate_queue_depth_at_plateau, 0,
            "aggregate queue must remain zero on the old side: old={old:?}"
        );
        assert_eq!(
            new.aggregate_queue_depth_at_plateau, 0,
            "aggregate queue must remain zero on the new side: new={new:?}"
        );
        assert_eq!(
            old.journal_queue_depth_at_plateau, 0,
            "journal queue must remain zero on the old side: old={old:?}"
        );
        assert_eq!(
            new.journal_queue_depth_at_plateau, 0,
            "journal queue must remain zero on the new side: new={new:?}"
        );
        assert_eq!(
            new.backpressure_refresh_attempts, 1,
            "new production path must collapse same-burst backpressure refreshes to one live_runtime read: new={new:?}"
        );
        assert_eq!(
            new.upstream_queue_depth_after_loop, 0,
            "with throttled refresh cadence, the same reduced live-like upstream queue should drain completely instead of staying pinned at 2048: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.dropped_over_capacity_discovery_critical_irrelevant_swaps,
            old.upstream_queue_depth_before_loop,
            "the fix should keep ingestion moving by making over-capacity target-mint drops explicit, not by hiding pressure: old={old:?} new={new:?}"
        );
        assert!(
            old.backpressure_refresh_attempts > new.backpressure_refresh_attempts,
            "A/B must prove the remaining blocker was repeated backpressure refresh work, not another queue theory: old={old:?} new={new:?}"
        );
        Ok(())
    }

    #[test]
    fn pending_retry_discovery_critical_backpressure_refresh_repins_output_queue_stage1(
    ) -> Result<()> {
        let summary =
            run_pending_retry_discovery_critical_backpressure_refresh_output_saturation_scenario(
                false,
            )?;

        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should still write normally before the pending-retry discovery-critical refresh class begins: {summary:?}"
        );
        assert_eq!(
            summary.writer_pending_requests_at_plateau,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the retry-loop repro must start only after the earlier raw-writer fix has already reduced pending requests to 128: {summary:?}"
        );
        assert_eq!(
            summary.aggregate_queue_depth_at_plateau, 0,
            "aggregate queue must stay zero in this retry-loop repro so aggregate theory is ruled out again: {summary:?}"
        );
        assert_eq!(
            summary.journal_queue_depth_at_plateau, 0,
            "journal queue must stay zero in this retry-loop repro so journal theory is ruled out again: {summary:?}"
        );
        assert_eq!(
            summary.upstream_queue_depth_before_loop, 2_048,
            "the reduced live-like repro should begin from a fully saturated Yellowstone output queue: {summary:?}"
        );
        assert!(
            summary.ingestion_polls_attempted > 0,
            "the exact remaining class still keeps polling ingestion; the blocker is the retry-loop refresh work stealing consumer budget, not a total pause: {summary:?}"
        );
        assert!(
            summary.upstream_queue_depth_after_loop > 0,
            "old/current retry-loop path should still leave the upstream queue pinned after burning the whole modeled consumer budget on repeated target-mint refresh work: {summary:?}"
        );
        assert_eq!(
            summary.backpressure_refresh_attempts,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "old/current retry-loop path should spend one live_runtime refresh per retry tick until the modeled consumer budget is exhausted: {summary:?}"
        );
        assert_eq!(
            summary.backpressure_refresh_budget_units_spent,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the exact old-path stall is that retry-loop refresh work itself consumes the whole modeled post-plateau consumer budget before the output queue can drain: {summary:?}"
        );
        assert_eq!(
            summary.pending_irrelevant_queue_depth,
            DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY,
            "the bounded local backlog should remain full but stable; the remaining blocker is the repeated retry-loop refresh work on top of that, not a new local queue explosion: {summary:?}"
        );
        Ok(())
    }

    #[test]
    fn throttled_pending_retry_discovery_critical_backpressure_refresh_allows_output_queue_drain_stage1(
    ) -> Result<()> {
        let old =
            run_pending_retry_discovery_critical_backpressure_refresh_output_saturation_scenario(
                false,
            )?;
        let new =
            run_pending_retry_discovery_critical_backpressure_refresh_output_saturation_scenario(
                true,
            )?;

        assert_eq!(
            old.writer_pending_requests_at_plateau, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "old side must start from the same 128 raw-writer plateau: old={old:?}"
        );
        assert_eq!(
            new.writer_pending_requests_at_plateau,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "new side must preserve the same 128 raw-writer plateau so only the retry-loop refresh cadence changes: new={new:?}"
        );
        assert_eq!(
            old.aggregate_queue_depth_at_plateau, 0,
            "aggregate queue must remain zero on the old side: old={old:?}"
        );
        assert_eq!(
            new.aggregate_queue_depth_at_plateau, 0,
            "aggregate queue must remain zero on the new side: new={new:?}"
        );
        assert_eq!(
            old.journal_queue_depth_at_plateau, 0,
            "journal queue must remain zero on the old side: old={old:?}"
        );
        assert_eq!(
            new.journal_queue_depth_at_plateau, 0,
            "journal queue must remain zero on the new side: new={new:?}"
        );
        assert_eq!(
            new.backpressure_refresh_attempts, 1,
            "new production path must collapse same-burst retry-loop refreshes to one live_runtime read: new={new:?}"
        );
        assert_eq!(
            new.upstream_queue_depth_after_loop, 0,
            "with throttled retry-loop refresh cadence, the same reduced live-like upstream queue should drain completely instead of staying pinned at 2048: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.dropped_over_capacity_discovery_critical_irrelevant_swaps,
            old.upstream_queue_depth_before_loop,
            "the fix should keep ingestion moving by making over-capacity target-mint drops explicit, not by hiding pressure: old={old:?} new={new:?}"
        );
        assert!(
            old.backpressure_refresh_attempts > new.backpressure_refresh_attempts,
            "A/B must prove the surviving blocker was retry-loop refresh churn, not another queue theory: old={old:?} new={new:?}"
        );
        Ok(())
    }

    #[test]
    fn empty_target_discovery_critical_bootstrap_recreates_clean_start_raw_writer_full_plateau_stage1(
    ) -> Result<()> {
        let summary = run_empty_target_discovery_critical_backpressure_scenario(true)?;
        let raw_writer_full_with_single_inflight_batch =
            OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY + TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE;

        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should permit immediate raw persistence before the empty-target discovery-critical plateau forms: {summary:?}"
        );
        assert!(
            summary.first_backpressure_pending_requests >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
                && summary.first_backpressure_pending_requests
                    <= raw_writer_full_with_single_inflight_batch,
            "with the broad empty-target bootstrap contract, generic SOL-leg buys bypass the normal soft limit and fill the raw writer to a full 4096 queued requests, plus at most one in-flight raw batch before the first backpressure event: {summary:?}"
        );
        assert!(
            summary.max_pending_requests_before_pause >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
                && summary.max_pending_requests_before_pause
                    <= raw_writer_full_with_single_inflight_batch,
            "the same empty-target discovery-critical request class should keep pending requests pinned at a full raw writer plus at most one in-flight batch, matching the live 4096 plateau with aggregate/journal queues still at zero: {summary:?}"
        );
        assert_eq!(
            summary.pending_irrelevant_queue_depth_at_pause,
            DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY,
            "after raw pending hits 4096, the app keeps buffering those same broad discovery-critical irrelevant swaps until the local bounded queue also reaches 4096: {summary:?}"
        );
        assert!(
            summary.upstream_queue_depth_shortly_after_pause > 0
                && summary.upstream_queue_depth_shortly_after_pause <= 64,
            "the exact incident class needs a still-small upstream queue when the local pending_irrelevant queue first forces ingestion pause, matching the live 49-depth plateau before later upstream saturation: {summary:?}"
        );
        assert_eq!(
            summary.upstream_queue_depth_after_escalation,
            2_048,
            "once the app pauses on the full local discovery-critical backlog, the modeled upstream queue should be able to reach its own 2048 capacity just like live: {summary:?}"
        );
        assert_eq!(
            summary.aggregate_queue_depth_at_pause, 0,
            "aggregate queue must stay zero in this repro so aggregate theory is ruled out for the exact class under test: {summary:?}"
        );
        assert_eq!(
            summary.journal_queue_depth_at_pause, 0,
            "recent_raw journal queue must stay zero in this repro so journal theory is ruled out for the exact class under test: {summary:?}"
        );
        assert!(
            summary.runtime_wal_bytes_at_pause < 16 * 1024 * 1024,
            "the plateau must reproduce while WAL is still tiny, before any giant-WAL explanation is available: {summary:?}"
        );
        assert_eq!(
            summary.sqlite_write_retry_delta, 0,
            "the plateau must reproduce without sqlite retry churn: {summary:?}"
        );
        assert_eq!(
            summary.sqlite_busy_error_delta, 0,
            "the plateau must reproduce without sqlite busy-error churn: {summary:?}"
        );
        assert!(
            summary.ingestion_paused_by_pending_irrelevant_queue,
            "the app should pause ingestion only after it has fully buffered the same broad discovery-critical irrelevant class locally, which explains why the upstream queue is small first and only later saturates: {summary:?}"
        );
        assert_eq!(
            summary.dropped_irrelevant_swaps, 0,
            "old/current empty-target bootstrap never drops these broad discovery-critical irrelevant buys under backpressure; it buffers them instead: {summary:?}"
        );
        Ok(())
    }

    #[test]
    fn removing_empty_target_discovery_critical_bootstrap_prevents_clean_start_raw_writer_full_plateau_stage1(
    ) -> Result<()> {
        let old = run_empty_target_discovery_critical_backpressure_scenario(true)?;
        let new = run_empty_target_discovery_critical_backpressure_scenario(false)?;
        let normal_irrelevant_soft_limit = OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
            .saturating_sub(TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE);
        let raw_writer_full_with_single_inflight_batch =
            OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY + TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE;

        assert!(
            old.first_backpressure_pending_requests >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
                && old.first_backpressure_pending_requests
                    <= raw_writer_full_with_single_inflight_batch,
            "the old side of the A/B must reproduce a full raw-writer plateau first, with at most one in-flight batch on top of the 4096 queued requests: old={old:?}"
        );
        assert!(
            new.dropped_irrelevant_swaps > 0,
            "without the broad empty-target bootstrap, the app must explicitly drop those backpressured generic irrelevant buys instead of buffering them as discovery-critical: new={new:?}"
        );
        assert!(
            new.first_backpressure_pending_requests <= normal_irrelevant_soft_limit,
            "the fix must stop empty-target generic irrelevant buys from consuming the reserved raw-writer capacity and reduce the first plateau back below 4096: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.pending_irrelevant_queue_depth_at_pause, 0,
            "with the broad empty-target bootstrap removed, those generic buys should no longer fill the bounded pending_irrelevant queue at all: old={old:?} new={new:?}"
        );
        assert!(
            !new.ingestion_paused_by_pending_irrelevant_queue,
            "the same reduced workload should no longer self-pause ingestion on a local 4096 discovery-critical irrelevant backlog: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.upstream_queue_depth_after_escalation, 0,
            "without the local self-pause, the same workload should not re-escalate into a 2048 upstream queue: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.aggregate_queue_depth_at_pause, 0,
            "the fix should stay on the raw-writer request-class ownership path and not depend on aggregate queue changes: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.journal_queue_depth_at_pause, 0,
            "the fix should stay on the raw-writer request-class ownership path and not depend on journal queue changes: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.sqlite_write_retry_delta, 0,
            "the fix should not require new sqlite retry churn: new={new:?}"
        );
        assert_eq!(
            new.sqlite_busy_error_delta, 0,
            "the fix should not require sqlite busy-error churn: new={new:?}"
        );
        assert!(
            new.runtime_wal_bytes_at_pause <= old.runtime_wal_bytes_at_pause * 2 + 1,
            "the fix should not just trade the plateau for runaway WAL growth: old={old:?} new={new:?}"
        );
        Ok(())
    }

    #[test]
    fn legacy_noncritical_irrelevant_backpressure_recreates_clean_start_plateau_before_upstream_saturation_stage1(
    ) -> Result<()> {
        let legacy_noncritical_irrelevant_soft_limit = OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
            .saturating_sub(TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE);
        let summary = run_noncritical_irrelevant_backpressure_plateau_scenario(
            true,
            Some(legacy_noncritical_irrelevant_soft_limit),
        )?;

        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should permit immediate raw persistence before the non-critical irrelevant plateau forms: {summary:?}"
        );
        assert_eq!(
            summary.first_backpressure_pending_requests,
            legacy_noncritical_irrelevant_soft_limit,
            "the first plateau should begin exactly at the normal irrelevant writer soft limit, which explains the live 3968 pending plateau on a 4096-capacity writer with one reserved 128-swap batch: {summary:?}"
        );
        assert!(
            !summary.first_backpressure_discovery_critical,
            "the exact clean-start 3968 plateau must be occupied by non-critical irrelevant requests, not discovery-critical ones: {summary:?}"
        );
        assert_eq!(
            summary.upstream_queue_depth_at_first_backpressure,
            0,
            "the exact clean-start plateau must happen before upstream queue growth begins, matching the live 3968/aggregate=0/upstream=0 slice: {summary:?}"
        );
        assert_eq!(
            summary.max_pending_requests_before_pause,
            legacy_noncritical_irrelevant_soft_limit,
            "the same normal irrelevant enqueue contract should cap pending requests at the soft limit before any upstream queue saturation is needed: {summary:?}"
        );
        assert_eq!(
            summary.pending_irrelevant_queue_depth_at_pause,
            DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY,
            "current logic should then fill the bounded in-memory irrelevant queue to its full 4096-swap capacity before ingestion polling stops: {summary:?}"
        );
        assert!(
            summary.upstream_queue_depth_shortly_after_pause > 0
                && summary.upstream_queue_depth_shortly_after_pause <= 64,
            "the exact incident class needs a still-small upstream queue when local pending_irrelevant first reaches capacity, matching the clean-start live plateau shape before later full escalation: {summary:?}"
        );
        assert_eq!(
            summary.aggregate_queue_depth_at_pause, 0,
            "aggregate queue backlog must stay inactive in this repro so aggregate theory is ruled out for this exact class: {summary:?}"
        );
        assert_eq!(
            summary.journal_queue_depth_at_pause, 0,
            "recent_raw journal backlog must stay inactive in this repro so journal theory is ruled out for this exact class: {summary:?}"
        );
        assert!(
            summary.runtime_wal_bytes_at_pause < 16 * 1024 * 1024,
            "the plateau must happen while WAL is still tiny, before any giant WAL explanation is available: {summary:?}"
        );
        assert_eq!(
            summary.sqlite_write_retry_delta, 0,
            "the plateau must reproduce without sqlite retry growth: {summary:?}"
        );
        assert_eq!(
            summary.sqlite_busy_error_delta, 0,
            "the plateau must reproduce without sqlite busy-error churn: {summary:?}"
        );
        assert!(
            summary.ingestion_paused_by_pending_irrelevant_queue,
            "the app should pause ingestion only after its own pending irrelevant queue fills, which explains why upstream queue growth starts small and only later escalates: {summary:?}"
        );
        assert_eq!(
            summary.upstream_queue_depth_after_escalation, 2_048,
            "once the app pauses on its own full pending_irrelevant queue, the modeled upstream queue should be free to grow to its own capacity and reproduce the later live escalation step: {summary:?}"
        );
        assert_eq!(
            summary.dropped_noncritical_irrelevant_swaps, 0,
            "old/current contract buffers non-critical irrelevant swaps instead of dropping them, which is the exact plateau-forming behavior under test: {summary:?}"
        );
        Ok(())
    }

    #[test]
    fn legacy_noncritical_irrelevant_requests_occupy_clean_start_soft_limit_plateau_with_zero_upstream_stage1(
    ) -> Result<()> {
        let legacy_noncritical_irrelevant_soft_limit = OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
            .saturating_sub(TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE);
        let summary = run_noncritical_irrelevant_backpressure_plateau_scenario(
            true,
            Some(legacy_noncritical_irrelevant_soft_limit),
        )?;

        assert_eq!(
            summary.first_backpressure_pending_requests,
            legacy_noncritical_irrelevant_soft_limit
        );
        assert!(!summary.first_backpressure_discovery_critical);
        assert_eq!(summary.upstream_queue_depth_at_first_backpressure, 0);
        assert_eq!(summary.aggregate_queue_depth_at_pause, 0);
        assert_eq!(summary.journal_queue_depth_at_pause, 0);
        assert_eq!(summary.sqlite_write_retry_delta, 0);
        assert_eq!(summary.sqlite_busy_error_delta, 0);
        assert!(
            summary.runtime_wal_bytes_at_pause < 16 * 1024 * 1024,
            "the soft-limit plateau must reproduce while WAL is still small: {summary:?}"
        );
        Ok(())
    }

    #[test]
    fn bounded_noncritical_irrelevant_try_enqueue_budget_prevents_clean_start_plateau_stage1(
    ) -> Result<()> {
        let legacy_noncritical_irrelevant_soft_limit = OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
            .saturating_sub(TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE);
        let old = run_noncritical_irrelevant_backpressure_plateau_scenario(
            false,
            Some(legacy_noncritical_irrelevant_soft_limit),
        )?;
        let new = run_noncritical_irrelevant_backpressure_plateau_scenario(false, None)?;

        assert_eq!(
            old.first_backpressure_pending_requests,
            legacy_noncritical_irrelevant_soft_limit,
            "the old side of the A/B must reproduce the exact soft-limit plateau first: old={old:?}"
        );
        assert!(
            new.dropped_noncritical_irrelevant_swaps > 0,
            "the new runtime contract must explicitly drop non-critical irrelevant swaps once the bounded raw-writer budget is exhausted: new={new:?}"
        );
        assert_eq!(
            new.first_backpressure_pending_requests,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the production writer fix should start backpressure after a single non-critical irrelevant raw batch instead of allowing a 3968-request plateau: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.max_pending_requests_before_pause,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the same reduced workload should no longer let non-critical irrelevant requests occupy more than one raw batch before backpressure starts: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.pending_irrelevant_queue_depth_at_pause, 0,
            "with the bounded raw-writer budget plus the existing non-critical drop contract, the local pending_irrelevant queue should stay empty on the same workload: old={old:?} new={new:?}"
        );
        assert!(
            !new.ingestion_paused_by_pending_irrelevant_queue,
            "without the 3968 raw plateau, the app should not self-pause ingestion on this same reduced workload: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.upstream_queue_depth_after_escalation, 0,
            "the same reduced workload should no longer re-escalate into a saturated upstream queue once non-critical irrelevant swaps can only consume one raw batch before being dropped: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.aggregate_queue_depth_at_pause, 0,
            "the fix should stay on the raw-writer request-budget path and not depend on aggregate backlog changes: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.journal_queue_depth_at_pause, 0,
            "the fix should stay on the raw-writer request-budget path and not depend on recent_raw journal backlog changes: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.sqlite_write_retry_delta, 0,
            "the fix should not require new sqlite retry churn: new={new:?}"
        );
        assert_eq!(
            new.sqlite_busy_error_delta, 0,
            "the fix should not require sqlite busy errors to improve the plateau: new={new:?}"
        );
        assert!(
            new.runtime_wal_bytes_at_pause < 16 * 1024 * 1024
                && new.runtime_wal_bytes_at_pause
                    <= old.runtime_wal_bytes_at_pause + (4 * 1024 * 1024),
            "the fix should keep WAL in the same tiny clean-start class rather than trading the plateau for runaway growth: old={old:?} new={new:?}"
        );
        Ok(())
    }

    #[test]
    fn zero_universe_empty_target_noncritical_exhaustion_gate_targets_only_exact_request_class_stage1(
    ) {
        let followed_snapshot = {
            let mut snapshot = FollowSnapshot::default();
            snapshot.active.insert("wallet-followed".to_string());
            snapshot
        };
        let open_shadow_lots =
            HashSet::from([("wallet-open".to_string(), "token-open".to_string())]);
        let empty_target_buy_mints = HashSet::new();
        let populated_target_buy_mints = HashSet::from(["token-target".to_string()]);

        assert!(
            should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
                false,
                &FollowSnapshot::default(),
                &HashSet::new(),
                true,
                &empty_target_buy_mints,
                true,
            ),
            "once the first empty-target best-effort batch is already exhausted, the exact same zero-universe non-critical irrelevant class should be dropped before it can refill the one-batch writer budget"
        );
        assert!(
            !should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
                true,
                &FollowSnapshot::default(),
                &HashSet::new(),
                true,
                &empty_target_buy_mints,
                true,
            ),
            "discovery-critical irrelevant swaps must keep their reserved raw-writer path"
        );
        assert!(
            !should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
                false,
                &followed_snapshot,
                &HashSet::new(),
                true,
                &empty_target_buy_mints,
                true,
            ),
            "followed universes must not take the empty-target non-critical refill-drop path"
        );
        assert!(
            !should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
                false,
                &FollowSnapshot::default(),
                &open_shadow_lots,
                true,
                &empty_target_buy_mints,
                true,
            ),
            "open-lot recovery must not take the empty-target non-critical refill-drop path"
        );
        assert!(
            !should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
                false,
                &FollowSnapshot::default(),
                &HashSet::new(),
                false,
                &empty_target_buy_mints,
                true,
            ),
            "non-fail-closed modes must not take the empty-target non-critical refill-drop path"
        );
        assert!(
            !should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
                false,
                &FollowSnapshot::default(),
                &HashSet::new(),
                true,
                &populated_target_buy_mints,
                true,
            ),
            "once exact discovery-critical target mints exist, the generic non-critical class must get its normal best-effort writer contract back"
        );
    }
