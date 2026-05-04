    #[test]
    fn recent_raw_journal_writer_no_pressure_path_collects_only_available_request_stage1(
    ) -> Result<()> {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_queue_capacity_batches
            .store(8, Ordering::Relaxed);
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(8);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:23:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let first_request = recent_raw_journal_write_request_for_test(0, 1, scenario_now);
        let first_rows = first_request.row_count();
        journal_sender.send(first_request)?;
        telemetry.note_journal_queue_enqueued(first_rows);

        let first_request = journal_receiver.recv()?;
        let collected_batch = super::collect_recent_raw_journal_write_batch(
            &journal_receiver,
            first_request,
            4,
            64,
            32,
            &telemetry,
        );

        assert_eq!(
            collected_batch.request_batches, 1,
            "without queue/overflow pressure and without immediately queued work, writer coalescing should not wait for more requests"
        );
        assert_eq!(collected_batch.inserted_swaps.len(), 1);
        assert_eq!(telemetry.snapshot().journal_queue_depth_batches, 0);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_queue_accounting_tolerates_recv_before_enqueue_note_stage1() {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_queue_capacity_batches
            .store(16, Ordering::Relaxed);

        telemetry.note_journal_queue_dequeued(7);
        let before_enqueue_snapshot = telemetry.snapshot();
        assert_eq!(before_enqueue_snapshot.journal_queue_depth_batches, 0);
        assert_eq!(before_enqueue_snapshot.journal_queue_row_debt, 0);

        telemetry.note_journal_queue_enqueued(7);
        let after_out_of_order_pair_snapshot = telemetry.snapshot();
        assert_eq!(
            after_out_of_order_pair_snapshot.journal_queue_depth_batches,
            0
        );
        assert_eq!(after_out_of_order_pair_snapshot.journal_queue_row_debt, 0);

        let capacity_telemetry = ObservedSwapWriterTelemetry::default();
        capacity_telemetry
            .journal_queue_capacity_batches
            .store(16, Ordering::Relaxed);
        for _ in 0..17 {
            capacity_telemetry.note_journal_queue_enqueued(1);
        }
        assert_eq!(
            capacity_telemetry.snapshot().journal_queue_depth_batches,
            16
        );
    }

    #[test]
    fn recent_raw_journal_overflow_row_debt_cap_exceeded_fails_closed_stage1() -> Result<()> {
        let unique = format!(
            "copybot-app-recent-raw-journal-row-debt-cap-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let runtime_db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        runtime_store.run_migrations(&migration_dir)?;
        let mut journal_seed_store = SqliteStore::open(Path::new(&journal_db_path))?;
        journal_seed_store.run_migrations(&migration_dir)?;
        drop(journal_seed_store);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:15:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = ObservedSwapWriter::start_with_config(
            runtime_db_path
                .to_str()
                .context("runtime sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(
                16,
                1,
                false,
                aggregate_write_config(),
                Some(ObservedSwapRecentRawJournalConfig {
                    sqlite_path: journal_db_path
                        .to_str()
                        .context("journal sqlite path must be valid utf-8")?
                        .to_string(),
                    retention_days: 8,
                    writer_queue_capacity_batches: 1,
                    write_coalesce_max_batches: 1,
                    overflow_capacity_batches: 1,
                    skip_prune_while_backlogged: true,
                    skip_startup_prune: true,
                }),
            ),
        )?;
        assert_eq!(
            writer.snapshot().journal_overflow_row_debt_capacity,
            1,
            "cap fixture should be small enough to prove terminal row-debt overflow"
        );

        runtime.block_on(async {
            writer
                .write(&recent_raw_journal_backpressure_swap(0, scenario_now))
                .await
        })?;
        let journal_baseline_started = Instant::now();
        loop {
            let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
            let journal_rows = journal_store
                .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
                .len();
            if journal_rows >= 1 {
                break;
            }
            if journal_baseline_started.elapsed() > StdDuration::from_secs(2) {
                anyhow::bail!("recent_raw journal writer did not persist baseline before cap test");
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }

        let blocker_conn = Connection::open(Path::new(&journal_db_path))
            .context("failed to open journal blocker connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed setting journal blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        for idx in 1..128usize {
            let swap = recent_raw_journal_backpressure_swap(idx, scenario_now);
            let _ = runtime.block_on(async {
                timeout(StdDuration::from_millis(100), writer.write(&swap)).await
            });
            if writer.ensure_running().is_err() {
                break;
            }
        }

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain
                .contains(super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_OVERFLOW_ROW_DEBT_CAP_EXCEEDED),
            "row-debt cap breach must fail closed with explicit reason: {error_chain}"
        );
        let snapshot_at_failure = writer.snapshot();
        assert!(
            snapshot_at_failure.journal_overflow_row_debt
                <= snapshot_at_failure.journal_overflow_row_debt_capacity,
            "the rejected request must not inflate in-memory row debt beyond the cap: {snapshot_at_failure:?}"
        );
        let journal_store_while_blocked = SqliteStore::open(Path::new(&journal_db_path))?;
        let journal_state_while_blocked = journal_store_while_blocked.recent_raw_journal_state()?;
        assert_eq!(
            journal_state_while_blocked.row_count, 1,
            "journal state must not advance while the journal DB write is blocked, even when raw writer fails closed"
        );

        blocker_conn.execute_batch("COMMIT")?;
        drop(blocker_conn);
        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface explicit row-debt cap failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain
                .contains(super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_OVERFLOW_ROW_DEBT_CAP_EXCEEDED),
            "shutdown should preserve explicit terminal cap reason: {shutdown_chain}"
        );

        remove_sqlite_test_files(&runtime_db_path);
        remove_sqlite_test_files(&journal_db_path);
        Ok(())
    }

    #[test]
    fn discovery_aggregate_per_request_write_recreates_post_checkpoint_saturation_even_without_recent_raw_journal_stage1(
    ) -> Result<()> {
        let summary = run_discovery_aggregate_backpressure_scenario(1, 0, false, 0)?;
        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should permit immediate raw persistence before aggregate pressure accumulates on the same runtime DB: {summary:?}"
        );
        assert!(
            summary.max_pending_requests >= 32,
            "current per-request aggregate path should recreate meaningful upstream pending request saturation on the same clean-start workload even when the recent_raw journal path is absent: {summary:?}"
        );
        assert!(
            summary.pending_requests_after_load > 0 || summary.aggregate_queue_depth_after_load > 0,
            "the reduced incident class should still leave visible post-load pressure without depending on a scheduler-sensitive raw-pending sample: {summary:?}"
        );
        assert!(
            summary.aggregate_queue_depth_after_load > 0
                || summary.max_aggregate_queue_depth_batches > 0,
            "the repro must still exercise real downstream aggregate backlog during the modeled load, even if a scheduler-sensitive final sample drains before observation: {summary:?}"
        );
        assert!(
            summary.max_aggregate_queue_depth_batches > 0,
            "the repro must exercise downstream aggregate backlog rather than a raw-path-only slowdown: {summary:?}"
        );
        assert_eq!(
            summary.aggregate_overflow_depth_after_load, 0,
            "the current per-request aggregate path should not have any separate overflow safety valve; raw persistence is coupled directly to the bounded aggregate queue today: {summary:?}"
        );
        assert_eq!(
            summary.max_aggregate_overflow_depth_batches, 0,
            "the current path should reproduce the incident without any extra hidden aggregate backlog layer: {summary:?}"
        );
        assert!(
            summary.runtime_wal_bytes_after_load > 1_000_000,
            "the reduced incident class should regrow real runtime WAL debt on the same clean-start workload: {summary:?}"
        );
        assert!(
            summary.sqlite_write_retry_delta <= 16,
            "the aggregate recurrence repro should not require material retryable sqlite lock growth; queue coupling on the shared runtime DB is sufficient: {summary:?}"
        );
        assert!(
            summary.sqlite_busy_error_delta <= 16,
            "the aggregate recurrence repro should not require material busy-error churn; bounded downstream aggregate backlog is enough to recreate the incident class: {summary:?}"
        );
        assert!(
            !summary.gap_cursor_present_after_load,
            "current hot-path aggregate coupling should reproduce the incident without falling back to an explicit materialization gap: {summary:?}"
        );
        Ok(())
    }

    #[test]
    fn discovery_aggregate_gap_fallback_keeps_raw_ingestion_live_and_replays_gap_after_pressure_stage1(
    ) -> Result<()> {
        let old = run_discovery_aggregate_backpressure_scenario(1, 0, false, 0)?;
        let new = run_discovery_aggregate_backpressure_scenario(
            OBSERVED_SWAP_DISCOVERY_AGGREGATE_WRITE_COALESCE_MAX_BATCHES,
            64,
            true,
            OBSERVED_SWAP_DISCOVERY_AGGREGATE_IDLE_REPLAY_MAX_PAGES,
        )?;

        assert!(
            old.max_pending_requests >= 32,
            "old per-request aggregate path must recreate meaningful raw pending pressure for the A/B proof to be meaningful: old={old:?}"
        );
        assert!(
            new.pending_requests_after_idle < 64,
            "the new path should not leave the raw writer in a large sustained backlog state after the same pressure wave clears and idle replay has run: old={old:?} new={new:?}"
        );
        assert!(
            new.persisted_rows_after_load >= old.persisted_rows_after_load,
            "the fix should not reduce pending depth by simply persisting fewer raw rows under the same modeled workload: old={old:?} new={new:?}"
        );
        assert!(
            old.aggregate_queue_depth_after_load > 0 || old.max_aggregate_queue_depth_batches > 0,
            "old path must still exercise real downstream aggregate backlog during the modeled load so the A/B proof exercises the same shared-db choke, even if the final sample drains before observation: old={old:?}"
        );
        assert!(
            new.aggregate_queue_depth_after_idle == 0 && new.aggregate_overflow_depth_after_idle == 0,
            "the new path should convert sustained shared-db aggregate pressure into an explicit materialization gap/replay path instead of leaving bounded aggregate backlog after idle replay: old={old:?} new={new:?}"
        );
        assert!(
            new.gap_cursor_present_after_load
                || (new.gap_cursor_cleared_after_idle
                    && new.covered_through_reached_tail_after_idle),
            "the new path must either expose a materialization gap after load or clear it through bounded replay before the snapshot; skipped aggregate work must not be silently dropped: old={old:?} new={new:?}"
        );
        assert!(
            new.max_aggregate_queue_depth_batches < old.max_aggregate_queue_depth_batches,
            "the explicit gap fallback should keep the aggregate queue from remaining in the same sustained post-load backlog shape: old={old:?} new={new:?}"
        );
        assert!(
            new.max_aggregate_overflow_depth_batches < 64,
            "the bounded overflow valve must remain visible and bounded while the raw path stays live: old={old:?} new={new:?}"
        );
        assert!(
            new.runtime_wal_bytes_after_load <= old.runtime_wal_bytes_after_load * 2 + 1,
            "the fix should not merely trade queue pressure for runaway runtime WAL growth: old={old:?} new={new:?}"
        );
        assert!(
            new.gap_cursor_cleared_after_idle,
            "once the sustained load stops, the aggregate worker should replay the explicit gap instead of requiring a process restart: old={old:?} new={new:?}"
        );
        assert!(
            new.covered_through_reached_tail_after_idle,
            "idle replay should drive discovery aggregate coverage back to the exact raw tail after the pressure wave clears: old={old:?} new={new:?}"
        );
        Ok(())
    }
