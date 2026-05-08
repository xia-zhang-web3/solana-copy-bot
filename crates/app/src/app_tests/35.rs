#[test]
    fn noncritical_irrelevant_output_pressure_drop_targets_only_zero_universe_fail_closed_stage1() {
        let followed_snapshot = {
            let mut snapshot = FollowSnapshot::default();
            snapshot.active.insert("wallet-followed".to_string());
            snapshot
        };
        let pressured_snapshot = Some(infra_snapshot_with_yellowstone_queue(
            Utc::now(),
            2_048,
            0,
            2_048,
            2_048,
            20_000,
        ));

        assert!(
            should_preemptively_drop_noncritical_irrelevant_observed_swap_under_output_pressure(
                false,
                &FollowSnapshot::default(),
                &HashSet::new(),
                true,
                pressured_snapshot,
            ),
            "zero-universe fail-closed Yellowstone output pressure should preemptively drop non-critical irrelevant swaps"
        );
        assert!(
            !should_preemptively_drop_noncritical_irrelevant_observed_swap_under_output_pressure(
                true,
                &FollowSnapshot::default(),
                &HashSet::new(),
                true,
                pressured_snapshot,
            ),
            "discovery-critical irrelevant swaps must never be preemptively dropped by the non-critical pressure path"
        );
        assert!(
            !should_preemptively_drop_noncritical_irrelevant_observed_swap_under_output_pressure(
                false,
                &followed_snapshot,
                &HashSet::new(),
                true,
                pressured_snapshot,
            ),
            "followed universes must not take the zero-universe fail-closed fast drop path"
        );
        assert!(
            !should_preemptively_drop_noncritical_irrelevant_observed_swap_under_output_pressure(
                false,
                &FollowSnapshot::default(),
                &HashSet::new(),
                true,
                Some(infra_snapshot_with_yellowstone_queue(
                    Utc::now(),
                    1,
                    0,
                    512,
                    2_048,
                    10,
                )),
            ),
            "light Yellowstone queue pressure must not activate the fast drop path"
        );
    }

    #[test]
    fn noncritical_irrelevant_output_pressure_waves_recreate_post_recovery_2048_repin_stage1(
    ) -> Result<()> {
        let summary = run_noncritical_irrelevant_output_pressure_wave_scenario(false)?;

        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should still write normally before the non-critical output-pressure wave scenario begins: {summary:?}"
        );
        assert_eq!(
            summary.writer_pending_requests_at_wave_peak,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the exact live class must hit the one-batch non-critical irrelevant soft-limit peak of 128 before each wave drains back down: {summary:?}"
        );
        assert!(
            summary.journal_queue_depth_at_wave_peak <= 1,
            "journal queue must stay in the low 0..1 class while the output queue remains pinned: {summary:?}"
        );
        assert_eq!(
            summary.upstream_queue_depth_before_loop, 2_048,
            "the reduced live-like repro should begin from the same 2048 upstream queue saturation as live: {summary:?}"
        );
        assert_eq!(
            summary.completed_waves, 4,
            "the current path should keep re-entering bounded 128-request non-critical waves instead of clearing the saturated upstream queue in one pass: {summary:?}"
        );
        assert!(
            summary.accepted_noncritical_irrelevant_swaps
                >= TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE * summary.completed_waves,
            "current logic must keep burning real raw-writer work on non-critical irrelevant swaps in each wave: {summary:?}"
        );
        assert!(
            summary.upstream_queue_depth_after_loop > 0,
            "after the same bounded number of waves the upstream queue should still remain pinned, matching the live oscillation class: {summary:?}"
        );
        Ok(())
    }

    #[test]
    fn preemptive_noncritical_irrelevant_output_pressure_drop_eliminates_post_recovery_repin_stage1(
    ) -> Result<()> {
        let old = run_noncritical_irrelevant_output_pressure_wave_scenario(false)?;
        let new = run_noncritical_irrelevant_output_pressure_wave_scenario(true)?;

        assert_eq!(
            old.writer_pending_requests_at_wave_peak, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "old side must reproduce the exact 128 non-critical wave peak first: old={old:?}"
        );
        assert!(
            old.journal_queue_depth_at_wave_peak <= 1,
            "journal queue must remain in the same low 0..1 class on the old side: old={old:?}"
        );
        assert_eq!(
            old.upstream_queue_depth_before_loop, 2_048,
            "old side must begin from the same saturated upstream queue: old={old:?}"
        );
        assert_eq!(
            new.accepted_noncritical_irrelevant_swaps, 0,
            "with the production fix, non-critical irrelevant swaps should stop consuming raw-writer budget once Yellowstone output pressure is already severe: new={new:?}"
        );
        assert_eq!(
            new.writer_pending_requests_at_wave_peak, 0,
            "the fix should eliminate the recurring one-batch non-critical wave entirely rather than merely shrinking it: new={new:?}"
        );
        assert_eq!(
            new.upstream_queue_depth_after_loop, 0,
            "the same saturated upstream queue should drain completely once non-critical irrelevant swaps are dropped immediately under severe Yellowstone output pressure: old={old:?} new={new:?}"
        );
        assert!(
            new.dropped_noncritical_irrelevant_swaps >= old.upstream_queue_depth_before_loop,
            "the fix should make the tradeoff explicit by dropping the non-critical irrelevant class instead of burning repeated raw-writer waves on it: old={old:?} new={new:?}"
        );
        assert_eq!(
            new.journal_queue_depth_at_wave_peak, 0,
            "the fix must stay off the recent_raw journal path: new={new:?}"
        );
        Ok(())
    }

    #[test]
    fn app_consumer_loop_telemetry_reports_follow_rejected_ratio_and_resets() {
        let mut telemetry = AppConsumerLoopTelemetry::default();

        telemetry.note_swap_seen();
        telemetry.note_processing_duration(5);
        telemetry.note_swap_seen();
        telemetry.note_follow_rejected();
        telemetry.note_processing_duration(15);
        telemetry.note_swap_seen();
        telemetry.note_follow_rejected();
        telemetry.note_processing_duration(25);

        let snapshot = telemetry.snapshot_and_reset();
        assert_eq!(
            snapshot,
            AppConsumerLoopTelemetrySnapshot {
                swaps_seen: 3,
                follow_rejected: 2,
                follow_rejected_ratio: 2.0 / 3.0,
                processing_ms_p95: 25,
            }
        );

        let empty_snapshot = telemetry.snapshot_and_reset();
        assert_eq!(
            empty_snapshot,
            AppConsumerLoopTelemetrySnapshot {
                swaps_seen: 0,
                follow_rejected: 0,
                follow_rejected_ratio: 0.0,
                processing_ms_p95: 0,
            }
        );
    }

    #[test]
    fn parse_app_log_env_filter_uses_default_when_missing() {
        with_app_log_filter_env(None, None, || {
            parse_app_log_env_filter("info").expect("missing env must use default app log filter");
        });
    }
