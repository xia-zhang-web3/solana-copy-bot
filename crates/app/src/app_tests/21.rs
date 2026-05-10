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
        assert!(
            summary.pending_irrelevant_queue_depth_at_pause
                >= DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY * 3 / 4,
            "after raw pending hits 4096, the app should build a material local discovery-critical irrelevant backlog; exact pause depth is scheduler-sensitive on CI runners: {summary:?}"
        );
        assert!(
            summary.upstream_queue_depth_shortly_after_pause <= 64,
            "the exact incident class should keep upstream pressure small while the local pending_irrelevant queue absorbs the broad class first; exact non-zero timing is scheduler-sensitive: {summary:?}"
        );
        assert!(
            summary.upstream_queue_depth_after_escalation == 0
                || summary.upstream_queue_depth_after_escalation == 2_048,
            "depending on runner scheduling, the modeled upstream queue may either remain drained or reach its 2048 capacity after local pressure forms: {summary:?}"
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
            summary.ingestion_paused_by_pending_irrelevant_queue
                || summary.pending_irrelevant_queue_depth_at_pause
                    >= DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY * 3 / 4,
            "the app should either pause on the local discovery-critical backlog or prove that backlog materially formed before the scheduler drained the upstream queue: {summary:?}"
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
