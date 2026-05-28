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
        assert!(
            summary.pending_irrelevant_queue_depth_at_pause
                >= DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY * 3 / 4,
            "current logic should then build a material bounded in-memory irrelevant queue before ingestion polling stops; exact depth is scheduler-sensitive on CI runners: {summary:?}"
        );
        assert!(
            summary.upstream_queue_depth_shortly_after_pause <= 64,
            "the exact incident class needs a still-small upstream queue when local pending_irrelevant first reaches capacity, matching the clean-start live plateau shape before later full escalation: {summary:?}"
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
            summary.ingestion_paused_by_pending_irrelevant_queue
                || summary.pending_irrelevant_queue_depth_at_pause
                    >= DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY * 3 / 4,
            "the app should either pause on the local irrelevant queue or prove that queue materially formed before the scheduler drained upstream pressure: {summary:?}"
        );
        assert!(
            summary.upstream_queue_depth_after_escalation == 0
                || summary.upstream_queue_depth_after_escalation == 2_048,
            "depending on runner scheduling, the modeled upstream queue may either remain drained or grow to capacity after local pressure forms: {summary:?}"
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

        assert!(
            old.first_backpressure_pending_requests
                >= legacy_noncritical_irrelevant_soft_limit
                    .saturating_sub(TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE)
                && old.first_backpressure_pending_requests <= legacy_noncritical_irrelevant_soft_limit,
            "the old side of the A/B should first hit backpressure at the legacy soft-limit plateau, allowing one concurrent writer batch of scheduler drift: old={old:?}"
        );
        assert_eq!(
            old.max_pending_requests_before_pause,
            legacy_noncritical_irrelevant_soft_limit,
            "the old side of the A/B must still reproduce the exact soft-limit plateau before pause: old={old:?}"
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
            new.runtime_wal_bytes_at_pause < 16 * 1024 * 1024,
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
            should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
                false,
                &FollowSnapshot::default(),
                &open_shadow_lots,
                true,
                &empty_target_buy_mints,
                true,
            ),
            "fail-closed mode must not let historical open-lot residue reopen the empty-target non-critical path"
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
