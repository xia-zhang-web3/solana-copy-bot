    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_settle_allows_after_stable_window_stage1()
    {
        let mut settle_state = DiscoveryRecentRawJournalSafetySettleState::default();
        let now = StdInstant::now();
        let first_gate =
            discovery_recent_raw_journal_safety_settle_start_gate(&mut settle_state, now);
        assert!(!first_gate.should_start);

        let stable_gate = discovery_recent_raw_journal_safety_settle_start_gate(
            &mut settle_state,
            now + DISCOVERY_RECENT_RAW_JOURNAL_SETTLE_WINDOW,
        );

        assert!(
            stable_gate.should_start,
            "a clean recent_raw journal state that remains stable for the full settle window may start discovery"
        );
    }

    fn recent_raw_journal_gate_cursor(
        ts: &str,
        slot: u64,
        signature: &str,
    ) -> DiscoveryRuntimeCursor {
        DiscoveryRuntimeCursor {
            ts_utc: DateTime::parse_from_rfc3339(ts)
                .expect("valid test cursor ts")
                .with_timezone(&Utc),
            slot,
            signature: signature.to_string(),
        }
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_persisted_lag_allows_scheduled_when_journal_covers_runtime_stage1(
    ) {
        let writer_snapshot = maintenance_test_writer_snapshot();
        let runtime_cursor =
            recent_raw_journal_gate_cursor("2026-04-29T09:00:00Z", 100, "sig-runtime");
        let journal_cursor =
            recent_raw_journal_gate_cursor("2026-04-29T09:00:00Z", 100, "sig-runtime");

        let persisted_gate = discovery_recent_raw_journal_persisted_lag_gate_from_cursors(
            Some(runtime_cursor),
            Some(journal_cursor),
        );

        assert_eq!(
            discovery_recent_raw_journal_backlog_defer_reason(&writer_snapshot),
            None
        );
        assert_eq!(
            persisted_gate.reason, None,
            "scheduled discovery may start only when in-memory backlog is clear and persisted recent_raw covers runtime"
        );
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_persisted_lag_allows_scheduled_when_observed_tail_absent_stage1(
    ) {
        let persisted_gate =
            discovery_recent_raw_journal_persisted_lag_gate_from_cursors(None, None);

        assert_eq!(
            persisted_gate.reason, None,
            "absent runtime raw cursor means there is no current runtime raw tail for the journal to cover"
        );
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_bounded_observed_tail_helper_returns_latest_cursor_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("recent-raw-journal-observed-tail-helper")?;
        assert_eq!(
            store.observed_swaps_tail_cursor_read_only()?,
            None,
            "empty observed_swaps has no raw tail to cover"
        );

        let same_ts = DateTime::parse_from_rfc3339("2026-04-29T09:00:00Z")
            .expect("valid test ts")
            .with_timezone(&Utc);
        let mut lower_slot = test_swap("sig-tail-lower-slot");
        lower_slot.ts_utc = same_ts;
        lower_slot.slot = 100;
        let mut higher_slot = test_swap("sig-tail-higher-slot");
        higher_slot.ts_utc = same_ts;
        higher_slot.slot = 101;
        store.insert_observed_swap(&lower_slot)?;
        store.insert_observed_swap(&higher_slot)?;

        assert_eq!(
            store.observed_swaps_tail_cursor_read_only()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: higher_slot.ts_utc,
                slot: higher_slot.slot,
                signature: higher_slot.signature.clone(),
            }),
            "bounded observed_swaps tail helper must use ts/slot/signature descending cursor order"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
        Ok(())
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_persisted_lag_uses_observed_tail_not_stale_discovery_runtime_state_stage1(
    ) -> Result<()> {
        let (runtime_store, runtime_db_path) =
            make_test_store("recent-raw-journal-observed-tail-gate")?;
        let journal_db_path = runtime_db_path.with_extension("recent-raw.sqlite");
        let stale_cursor =
            recent_raw_journal_gate_cursor("2026-04-29T09:00:00Z", 100, "sig-stale-runtime");
        let observed_tail =
            recent_raw_journal_gate_cursor("2026-04-29T09:00:10Z", 110, "sig-observed-tail");

        runtime_store.upsert_discovery_runtime_cursor(&stale_cursor)?;
        let mut live_swap = test_swap("sig-observed-tail");
        live_swap.ts_utc = observed_tail.ts_utc;
        live_swap.slot = observed_tail.slot;
        runtime_store.insert_observed_swap(&live_swap)?;

        let journal_store = SqliteStore::open(&journal_db_path)?;
        let mut journal_swap = test_swap("sig-stale-runtime");
        journal_swap.ts_utc = stale_cursor.ts_utc;
        journal_swap.slot = stale_cursor.slot;
        journal_store.insert_recent_raw_journal_batch(&[journal_swap], Utc::now())?;
        drop(journal_store);

        let gate = load_discovery_recent_raw_journal_persisted_lag_gate(
            &runtime_store,
            journal_db_path
                .to_str()
                .context("journal db path must be valid utf-8")?,
        );

        assert_eq!(
            gate.reason,
            Some(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_PERSISTED_LAG),
            "journal coverage that only reaches stale discovery_runtime_state must not satisfy the observed_swaps tail gate"
        );
        assert_eq!(gate.observed_tail_cursor, Some(observed_tail));
        assert_eq!(gate.journal_covered_through_cursor, Some(stale_cursor));

        let _ = std::fs::remove_file(&runtime_db_path);
        let _ = std::fs::remove_file(format!("{}-wal", runtime_db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", runtime_db_path.display()));
        let _ = std::fs::remove_file(&journal_db_path);
        let _ = std::fs::remove_file(format!("{}-wal", journal_db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", journal_db_path.display()));
        Ok(())
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_persisted_lag_blocks_scheduled_when_journal_cursor_missing_stage1(
    ) {
        let runtime_cursor =
            recent_raw_journal_gate_cursor("2026-04-29T09:00:00Z", 100, "sig-runtime");

        let persisted_gate = discovery_recent_raw_journal_persisted_lag_gate_from_cursors(
            Some(runtime_cursor),
            None,
        );

        assert_eq!(
            persisted_gate.reason,
            Some(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_PERSISTED_LAG)
        );
        assert_eq!(persisted_gate.lag_seconds, None);
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_persisted_lag_blocks_scheduled_when_journal_is_behind_stage1(
    ) {
        let runtime_cursor =
            recent_raw_journal_gate_cursor("2026-04-29T09:00:10Z", 110, "sig-runtime");
        let journal_cursor =
            recent_raw_journal_gate_cursor("2026-04-29T09:00:00Z", 100, "sig-journal");

        let persisted_gate = discovery_recent_raw_journal_persisted_lag_gate_from_cursors(
            Some(runtime_cursor),
            Some(journal_cursor),
        );

        assert_eq!(
            persisted_gate.reason,
            Some(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_PERSISTED_LAG)
        );
        assert_eq!(persisted_gate.lag_seconds, Some(10));
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_persisted_lag_catch_up_preserves_pending_stage1(
    ) {
        let runtime_cursor =
            recent_raw_journal_gate_cursor("2026-04-29T09:00:10Z", 110, "sig-runtime");
        let journal_cursor =
            recent_raw_journal_gate_cursor("2026-04-29T09:00:00Z", 100, "sig-journal");
        let persisted_gate = discovery_recent_raw_journal_persisted_lag_gate_from_cursors(
            Some(runtime_cursor),
            Some(journal_cursor),
        );
        let now = StdInstant::now();

        let catch_up_gate = discovery_catch_up_retrigger_recent_raw_journal_defer_gate(
            persisted_gate.reason.expect("lag should defer"),
            now,
        );

        assert!(!catch_up_gate.should_start);
        assert!(
            catch_up_gate.keep_pending,
            "catch-up intent must remain pending while persisted recent_raw lag defers retrigger"
        );
        assert!(catch_up_gate.next_retry_at.is_some_and(|retry_at| retry_at
            >= now + DISCOVERY_CATCH_UP_RECENT_RAW_JOURNAL_BACKLOG_RETRY_INTERVAL));
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_persisted_lag_catch_up_starts_when_lag_clears_and_retry_due_stage1(
    ) {
        let writer_snapshot = maintenance_test_writer_snapshot();
        let runtime_cursor =
            recent_raw_journal_gate_cursor("2026-04-29T09:00:00Z", 100, "sig-runtime");
        let journal_cursor =
            recent_raw_journal_gate_cursor("2026-04-29T09:00:00Z", 100, "sig-runtime");
        let persisted_gate = discovery_recent_raw_journal_persisted_lag_gate_from_cursors(
            Some(runtime_cursor),
            Some(journal_cursor),
        );
        let now = StdInstant::now();
        let catch_up_gate = discovery_catch_up_retrigger_recent_raw_journal_backlog_gate(
            &writer_snapshot,
            Some(now),
            now,
        );

        assert!(
            catch_up_gate.should_start,
            "catch-up backlog retry gate should allow start once retry is due"
        );
        assert_eq!(persisted_gate.reason, None);
        assert!(!catch_up_gate.keep_pending);
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_lag_unproven_blocks_start_stage1() {
        let persisted_gate =
            discovery_recent_raw_journal_persisted_lag_unproven_gate("synthetic read-only error");
        let now = StdInstant::now();

        let catch_up_gate = discovery_catch_up_retrigger_recent_raw_journal_defer_gate(
            persisted_gate.reason.expect("unproven lag should defer"),
            now,
        );

        assert_eq!(
            persisted_gate.reason,
            Some(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_LAG_UNPROVEN)
        );
        assert!(!catch_up_gate.should_start);
        assert!(catch_up_gate.keep_pending);
        assert_eq!(
            catch_up_gate.reason,
            Some(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_LAG_UNPROVEN)
        );
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_backlog_catch_up_preserves_pending_stage1(
    ) {
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.journal_queue_depth_batches = 1;
        let now = StdInstant::now();

        let gate = discovery_catch_up_retrigger_recent_raw_journal_backlog_gate(
            &writer_snapshot,
            None,
            now,
        );

        assert!(!gate.should_start);
        assert!(
            gate.keep_pending,
            "catch-up intent must remain pending while recent_raw backlog defers retrigger"
        );
        assert_eq!(
            gate.reason,
            Some(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_BACKLOG)
        );
        assert!(
            gate.next_retry_at.is_some_and(|retry_at| retry_at
                >= now + DISCOVERY_CATCH_UP_RECENT_RAW_JOURNAL_BACKLOG_RETRY_INTERVAL),
            "backlog deferral must set a bounded retry deadline"
        );
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_settle_catch_up_preserves_pending_stage1()
    {
        let mut settle_state = DiscoveryRecentRawJournalSafetySettleState::default();
        let now = StdInstant::now();
        let settle_gate =
            discovery_recent_raw_journal_safety_settle_start_gate(&mut settle_state, now);
        let catch_up_gate = discovery_catch_up_retrigger_recent_raw_journal_defer_gate(
            settle_gate.reason.expect("first clean check should defer"),
            now,
        );

        assert!(!catch_up_gate.should_start);
        assert!(
            catch_up_gate.keep_pending,
            "catch-up intent must remain pending while recent_raw safety settle window defers retrigger"
        );
        assert_eq!(
            catch_up_gate.reason,
            Some(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_SETTLE_WINDOW)
        );
        assert!(catch_up_gate.next_retry_at.is_some());
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_backlog_catch_up_retry_throttles_stage1()
    {
        let writer_snapshot = maintenance_test_writer_snapshot();
        let now = StdInstant::now();
        let retry_at = now + DISCOVERY_CATCH_UP_RECENT_RAW_JOURNAL_BACKLOG_RETRY_INTERVAL;

        let gate = discovery_catch_up_retrigger_recent_raw_journal_backlog_gate(
            &writer_snapshot,
            Some(retry_at),
            now,
        );

        assert!(!gate.should_start);
        assert!(gate.keep_pending);
        assert_eq!(gate.next_retry_at, Some(retry_at));
        assert_eq!(
            gate.reason, None,
            "not-yet-due retry must not log another backlog deferral or busy-loop"
        );
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_backlog_catch_up_starts_when_clear_and_due_stage1(
    ) {
        let writer_snapshot = maintenance_test_writer_snapshot();
        let now = StdInstant::now();

        let gate = discovery_catch_up_retrigger_recent_raw_journal_backlog_gate(
            &writer_snapshot,
            Some(now),
            now,
        );

        assert!(
            gate.should_start,
            "catch-up retrigger should start once recent_raw backlog is clear and retry is due"
        );
        assert!(!gate.keep_pending);
        assert_eq!(gate.next_retry_at, None);
        assert_eq!(gate.reason, None);
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_backlog_catch_up_reschedules_when_due_but_still_backlogged_stage1(
    ) {
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.journal_writer_inflight_rows = 1;
        let now = StdInstant::now();

        let gate = discovery_catch_up_retrigger_recent_raw_journal_backlog_gate(
            &writer_snapshot,
            Some(now),
            now,
        );

        assert!(!gate.should_start);
        assert!(gate.keep_pending);
        assert_eq!(
            gate.reason,
            Some(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_BACKLOG)
        );
        assert!(
            gate.next_retry_at.is_some_and(|retry_at| retry_at
                >= now + DISCOVERY_CATCH_UP_RECENT_RAW_JOURNAL_BACKLOG_RETRY_INTERVAL),
            "due retry with continuing backlog should move the retry deadline forward"
        );
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_running_backlog_aborts_success_path_stage1(
    ) {
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.journal_overflow_depth_batches = 1;
        let persisted_gate =
            discovery_recent_raw_journal_persisted_lag_gate_from_cursors(None, None);

        assert_eq!(
            discovery_recent_raw_journal_abort_reason_from_gates(&writer_snapshot, &persisted_gate,),
            Some(DISCOVERY_CYCLE_ABORTED_DUE_TO_RECENT_RAW_JOURNAL_BACKLOG)
        );
    }

    #[test]
    fn discovery_cycle_deferred_due_to_recent_raw_journal_running_persisted_lag_aborts_success_path_stage1(
    ) {
        let writer_snapshot = maintenance_test_writer_snapshot();
        let runtime_cursor =
            recent_raw_journal_gate_cursor("2026-04-29T09:00:10Z", 110, "sig-runtime");
        let journal_cursor =
            recent_raw_journal_gate_cursor("2026-04-29T09:00:00Z", 100, "sig-journal");
        let persisted_gate = discovery_recent_raw_journal_persisted_lag_gate_from_cursors(
            Some(runtime_cursor),
            Some(journal_cursor),
        );

        assert_eq!(
            discovery_recent_raw_journal_abort_reason_from_gates(&writer_snapshot, &persisted_gate,),
            Some(DISCOVERY_CYCLE_ABORTED_DUE_TO_RECENT_RAW_JOURNAL_PERSISTED_LAG)
        );
    }

    #[test]
    fn discovery_catch_up_scheduler_allows_pressure_override_when_pending_requests_is_the_only_blocker(
    ) {
        let mut discovery_output = discovery_output_for_catch_up_tests(true);
        discovery_output.persisted_stream_catch_up_pressure_override_requested = true;
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests =
            DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD.saturating_add(1);

        assert!(should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));
    }

    #[test]
    fn discovery_catch_up_scheduler_allows_exact_raw_writer_plateau_when_it_is_the_only_remaining_blocker(
    ) {
        let discovery_output = discovery_output_for_catch_up_tests(true);
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests = DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD;

        assert!(discovery_catch_up_pending_requests_only_pressure(
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));
        assert!(discovery_catch_up_pending_requests_is_only_raw_plateau(
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));
        assert!(should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));
        assert_eq!(
            discovery_catch_up_block_reason(
                &discovery_output,
                false,
                &writer_snapshot,
                Some(&maintenance_test_ingestion_snapshot(0.0)),
            ),
            None
        );
    }

    #[test]
    fn replay_fail_closed_catch_up_is_no_longer_deferred_by_the_fixed_one_batch_raw_plateau_stage1()
    {
        let mut discovery_output =
            live_like_fail_closed_no_recent_published_universe_output(Utc::now());
        discovery_output.persisted_stream_catch_up_requested = true;
        discovery_output.persisted_stream_catch_up_pressure_override_requested = false;

        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests = DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD;
        let ingestion_snapshot = maintenance_test_ingestion_snapshot(0.0);

        assert_eq!(
            legacy_discovery_catch_up_block_reason_for_test(
                &discovery_output,
                false,
                &writer_snapshot,
                Some(&ingestion_snapshot),
            ),
            Some(DiscoveryCatchUpBlockReason::WriterPendingRequests),
            "the pre-fix runtime gate treated the fixed one-batch raw plateau as hard pressure and deferred the immediate replay catch-up that would have resumed the fail-closed persisted rebuild"
        );
        assert_eq!(
            discovery_catch_up_block_reason(
                &discovery_output,
                false,
                &writer_snapshot,
                Some(&ingestion_snapshot),
            ),
            None,
            "the fixed runtime gate should allow the immediate replay catch-up once the only remaining signal is the intentional one-batch raw plateau"
        );
        assert!(should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&ingestion_snapshot),
        ));
    }

    #[test]
    fn discovery_catch_up_scheduler_keeps_shadow_queue_as_hard_stop_even_with_pressure_override() {
        let mut discovery_output = discovery_output_for_catch_up_tests(true);
        discovery_output.persisted_stream_catch_up_pressure_override_requested = true;
        let writer_snapshot = maintenance_test_writer_snapshot();

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            true,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(1.0)),
        ));
    }

    #[test]
    fn discovery_catch_up_scheduler_keeps_yellowstone_pressure_as_hard_stop_even_with_pressure_override(
    ) {
        let mut discovery_output = discovery_output_for_catch_up_tests(true);
        discovery_output.persisted_stream_catch_up_pressure_override_requested = true;
        let writer_snapshot = maintenance_test_writer_snapshot();

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(1.0)),
        ));
    }
