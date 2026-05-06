    #[test]
    fn recent_raw_journal_partial_deadline_exhaustion_retries_unprocessed_suffix_stage1(
    ) -> Result<()> {
        let telemetry = ObservedSwapWriterTelemetry::default();
        let scenario_now = Utc::now();
        let swaps = (0..5usize)
            .map(|idx| recent_raw_journal_backpressure_swap(idx, scenario_now))
            .collect::<Vec<_>>();
        let mut attempt_signatures = Vec::<Vec<String>>::new();
        let mut attempt_index = 0usize;

        super::write_recent_raw_journal_batch_with_deadline_attempts(
            &telemetry,
            &swaps,
            || Instant::now() + StdDuration::from_secs(5),
            |suffix, _deadline| {
                attempt_index = attempt_index.saturating_add(1);
                attempt_signatures.push(
                    suffix
                        .iter()
                        .map(|swap| swap.signature.clone())
                        .collect::<Vec<_>>(),
                );
                match attempt_index {
                    1 => Ok((recent_raw_journal_write_summary_for_test(2, 2), true)),
                    2 => Ok((recent_raw_journal_write_summary_for_test(3, 3), false)),
                    _ => Err(anyhow!("unexpected extra recent_raw journal write attempt")),
                }
            },
        )?;

        assert_eq!(attempt_signatures.len(), 2);
        assert_eq!(
            attempt_signatures[0],
            swaps
                .iter()
                .map(|swap| swap.signature.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            attempt_signatures[1],
            swaps[2..]
                .iter()
                .map(|swap| swap.signature.clone())
                .collect::<Vec<_>>(),
            "deadline exhaustion after a committed prefix must retry only the unprocessed suffix"
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_deadline_exhaustion_without_suffix_progress_blocks_later_batches_stage1(
    ) -> Result<()> {
        let telemetry = ObservedSwapWriterTelemetry::default();
        let scenario_now = Utc::now();
        let swaps = (0..5usize)
            .map(|idx| recent_raw_journal_backpressure_swap(idx, scenario_now))
            .collect::<Vec<_>>();
        let mut attempt_signatures = Vec::<Vec<String>>::new();
        let mut attempt_index = 0usize;

        let error = super::write_recent_raw_journal_batch_with_deadline_attempts(
            &telemetry,
            &swaps,
            || Instant::now() + StdDuration::from_secs(5),
            |suffix, _deadline| {
                attempt_index = attempt_index.saturating_add(1);
                attempt_signatures.push(
                    suffix
                        .iter()
                        .map(|swap| swap.signature.clone())
                        .collect::<Vec<_>>(),
                );
                match attempt_index {
                    1 => Ok((recent_raw_journal_write_summary_for_test(2, 2), true)),
                    2 => Ok((recent_raw_journal_write_summary_for_test(0, 0), true)),
                    _ => Err(anyhow!(
                        "writer must not process future journal batches while suffix ownership is unresolved"
                    )),
                }
            },
        )
        .expect_err("zero-progress suffix deadline exhaustion must fail closed");
        let error_message = format!("{error:#}");
        assert!(
            error_message
                .contains(super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE_EXHAUSTED),
            "zero-progress suffix failure must expose stable deadline reason; error={error_message}"
        );
        assert_eq!(
            attempt_signatures.len(),
            2,
            "writer must stop on unprocessed suffix instead of advancing to any later journal batch"
        );
        assert_eq!(
            attempt_signatures[1],
            swaps[2..]
                .iter()
                .map(|swap| swap.signature.clone())
                .collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_hot_writer_defers_rows_older_than_retention_horizon() -> Result<()> {
        let unique = format!(
            "copybot-app-recent-raw-journal-prune-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let runtime_db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
        let seed_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        seed_store.ensure_observed_swap_writer_tables()?;

        let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let journal_now = Utc::now();
        journal_store.insert_recent_raw_journal_batch(
            &[SwapEvent {
                wallet: "wallet-journal-old".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-journal-old".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-recent-raw-journal-old".to_string(),
                slot: 400,
                ts_utc: journal_now - ChronoDuration::days(10),
                exact_amounts: None,
            }],
            journal_now - ChronoDuration::days(10),
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                runtime_db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, Some(ObservedSwapRecentRawJournalConfig {
                        sqlite_path: journal_db_path
                            .to_str()
                            .context("journal sqlite path must be valid utf-8")?
                            .to_string(),
                        retention_days: 8,
                        writer_queue_capacity_batches: 8,
                        write_coalesce_max_batches:
                            super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES,
                        overflow_capacity_batches: 32,
                        skip_prune_while_backlogged: true,
                        skip_startup_prune: false,
                    }),
                ),
            )?;
            let fresh_swap = SwapEvent {
                wallet: "wallet-journal-fresh".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-journal-fresh".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-recent-raw-journal-fresh".to_string(),
                slot: 401,
                ts_utc: journal_now - ChronoDuration::days(1),
                exact_amounts: None,
            };

            writer.write(&fresh_swap).await?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let journal_rows =
            journal_store.load_observed_swaps_since(journal_now - ChronoDuration::days(30))?;
        assert_eq!(
            journal_rows.len(),
            2,
            "hot recent_raw writer must not run retention prune when skip_prune_while_backlogged=true"
        );
        assert!(journal_rows
            .iter()
            .any(|row| row.signature == "sig-recent-raw-journal-fresh"));
        let journal_state = journal_store.recent_raw_journal_state()?;
        assert_eq!(journal_state.row_count, 2);
        assert!(journal_state.last_pruned_at.is_none());
        let _ = std::fs::remove_file(runtime_db_path);
        let _ = std::fs::remove_file(journal_db_path);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_per_request_write_recreates_post_checkpoint_saturation_without_large_wal_stage1(
    ) -> Result<()> {
        let summary = run_recent_raw_journal_backpressure_scenario(true, 1, 0)?;
        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should permit immediate raw persistence before downstream journal pressure accumulates: {summary:?}"
        );
        assert!(
            summary.max_pending_requests >= 4,
            "current per-request recent_raw journal writes should still recreate observable upstream pending request pressure on the same clean-start workload, even if raw persistence is no longer startup-gated: {summary:?}"
        );
        assert!(
            summary.pending_requests_after_load > 0,
            "the reduced incident class should still leave the raw writer behind the ingestion stream immediately after the modeled load, not just in a transient internal sample: {summary:?}"
        );
        assert!(
            summary.journal_queue_depth_after_load > 0 || summary.max_journal_queue_depth_batches > 0,
            "the repro must still exercise a real downstream recent_raw journal backlog during the modeled load, rather than only raw-path pressure: {summary:?}"
        );
        assert!(
            summary.max_journal_queue_depth_batches > 0,
            "the repro must still exercise downstream recent_raw journal backlog rather than a raw-path-only slowdown: {summary:?}"
        );
        assert!(
            summary.sqlite_write_retry_delta <= 16,
            "this recurrence class should not require material retryable sqlite lock growth to re-saturate; it is enough that the downstream recent_raw journal hot path falls behind: {summary:?}"
        );
        assert!(
            summary.sqlite_busy_error_delta <= 16,
            "the repro should demonstrate queue saturation without relying on material busy-error churn: {summary:?}"
        );
        assert_eq!(
            summary.journal_overflow_depth_after_load, 0,
            "current per-request path should not have any separate overflow safety valve; the raw writer is coupled directly to the bounded journal queue today: {summary:?}"
        );
        assert_eq!(
            summary.max_journal_overflow_depth_batches, 0,
            "the current path should reproduce the incident without any extra hidden backlog layer: {summary:?}"
        );
        assert!(
            summary.runtime_wal_bytes_after_load < 64 * 1024 * 1024,
            "the reduced incident class should still saturate while runtime WAL stays modest rather than exploding into multi-gigabyte debt again: {summary:?}"
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_coalesced_writes_reduce_post_checkpoint_saturation_without_hiding_backlog_stage1(
    ) -> Result<()> {
        let old = run_recent_raw_journal_backpressure_scenario(true, 1, 0)?;
        let new = run_recent_raw_journal_backpressure_scenario(
            true,
            super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES,
            64,
        )?;

        assert!(
            old.max_pending_requests >= 4,
            "old per-request journal path must still recreate observable raw pending pressure for the A/B proof to be meaningful, even if the exact peak is scheduler-sensitive: old={old:?}"
        );
        assert!(
            new.persisted_rows_after_load >= old.persisted_rows_after_load,
            "the fix should not reduce pending depth by simply persisting fewer raw rows under the same modeled workload: old={old:?} new={new:?}"
        );
        assert!(
            new.journal_queue_depth_after_load < 16,
            "the fix may leave a scheduler-sensitive visible journal batch at the end of the modeled load, but it must stay bounded rather than hiding saturation downstream: old={old:?} new={new:?}"
        );
        assert!(
            new.max_journal_queue_depth_batches <= 16,
            "the fix may let the downstream journal queue absorb more visible work temporarily, but it must keep that backlog bounded at the modeled channel capacity without phantom queue growth: old={old:?} new={new:?}"
        );
        assert!(
            new.journal_overflow_depth_after_load <= 64,
            "the new overflow valve must remain bounded and visible at the end of the same modeled load instead of silently growing without limit: old={old:?} new={new:?}"
        );
        assert!(
            new.max_journal_overflow_depth_batches <= 64,
            "the new overflow valve must remain bounded and visible at peak load instead of silently growing without limit: old={old:?} new={new:?}"
        );
        assert!(
            new.runtime_wal_bytes_after_load <= old.runtime_wal_bytes_after_load * 2 + 1,
            "the fix should not merely trade queue pressure for runaway runtime WAL growth: old={old:?} new={new:?}"
        );
        assert!(
            new.sqlite_write_retry_delta <= 16,
            "the new path should improve throughput without hiding material lock contention behind retry growth: new={new:?}"
        );
        assert!(
            new.sqlite_busy_error_delta <= 16,
            "the new path should improve throughput without introducing material busy-error churn: new={new:?}"
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_full_overflow_coalesces_without_blocking_raw_persistence_stage1(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-recent-raw-journal-full-overflow-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let runtime_db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
        let runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        runtime_store.ensure_observed_swap_writer_tables()?;
        let journal_seed_store = SqliteStore::open(Path::new(&journal_db_path))?;
        journal_seed_store.ensure_recent_raw_journal_tables()?;
        drop(journal_seed_store);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:05:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = ObservedSwapWriter::start_with_config(
            runtime_db_path
                .to_str()
                .context("runtime sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(32, 1, Some(ObservedSwapRecentRawJournalConfig {
                    sqlite_path: journal_db_path
                        .to_str()
                        .context("journal sqlite path must be valid utf-8")?
                        .to_string(),
                    retention_days: 8,
                    writer_queue_capacity_batches: 1,
                    write_coalesce_max_batches: 32,
                    overflow_capacity_batches: 2,
                    skip_prune_while_backlogged: true,
                    skip_startup_prune: true,
                }),
            ),
        )?;

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
                anyhow::bail!(
                    "recent_raw journal writer did not persist baseline before pressure test"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }

        let blocker_conn = Connection::open(Path::new(&journal_db_path))
            .context("failed to open journal blocker connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed setting journal blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let write_result = (1..42usize).try_for_each(|idx| -> Result<()> {
            let swap = recent_raw_journal_backpressure_swap(idx, scenario_now);
            match runtime.block_on(async {
                timeout(StdDuration::from_millis(250), writer.write(&swap)).await
            }) {
                Ok(Ok(inserted)) => {
                    assert!(inserted, "pressure test swap should insert once: {idx}");
                    Ok(())
                }
                Ok(Err(error)) => Err(error).with_context(|| {
                    format!("raw write failed while recent_raw journal was blocked at idx={idx}")
                }),
                Err(_) => anyhow::bail!(
                    "raw write timed out behind recent_raw journal backpressure at idx={idx}"
                ),
            }
        });

        let runtime_rows_while_blocked = runtime_store
            .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
            .len();
        let backlog_visible_started = Instant::now();
        let mut snapshot_while_blocked = writer.snapshot();
        while snapshot_while_blocked.journal_queue_depth_batches == 0
            && snapshot_while_blocked.journal_overflow_depth_batches == 0
            && snapshot_while_blocked.journal_queue_row_debt == 0
            && snapshot_while_blocked.journal_overflow_row_debt == 0
            && snapshot_while_blocked.journal_writer_inflight_rows == 0
            && backlog_visible_started.elapsed() < StdDuration::from_secs(2)
        {
            std::thread::sleep(StdDuration::from_millis(10));
            snapshot_while_blocked = writer.snapshot();
        }
        let journal_store_while_blocked = SqliteStore::open(Path::new(&journal_db_path))?;
        let journal_rows_while_blocked = journal_store_while_blocked
            .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
            .len();
        let journal_state_while_blocked = journal_store_while_blocked.recent_raw_journal_state()?;
        blocker_conn.execute_batch("COMMIT")?;
        drop(blocker_conn);

        write_result?;
        assert!(
            runtime_rows_while_blocked >= 42,
            "raw observed_swaps should keep advancing while recent_raw journal is blocked; runtime_rows={runtime_rows_while_blocked}"
        );
        assert!(
            snapshot_while_blocked.journal_queue_depth_batches > 0
                || snapshot_while_blocked.journal_overflow_depth_batches > 0
                || snapshot_while_blocked.journal_writer_inflight_rows > 0,
            "journal backlog should remain visible while downstream writer is blocked, whether it is still in overflow or has moved into queue/inflight: snapshot={snapshot_while_blocked:?}"
        );
        assert!(
            snapshot_while_blocked.journal_overflow_depth_batches
                <= snapshot_while_blocked.journal_overflow_capacity_batches,
            "journal overflow batch depth must stay bounded while adaptive coalescing moves backlog into writes: snapshot={snapshot_while_blocked:?}"
        );
        assert!(
            snapshot_while_blocked.journal_queue_row_debt
                + snapshot_while_blocked.journal_writer_inflight_rows
                + snapshot_while_blocked.journal_overflow_row_debt
                > 0,
            "snapshot should expose row debt that is still queued, overflowed, or inflight: snapshot={snapshot_while_blocked:?}"
        );
        assert!(
            snapshot_while_blocked.journal_overflow_row_debt
                <= snapshot_while_blocked.journal_overflow_row_debt_capacity,
            "journal overflow row debt must stay below the explicit cap: snapshot={snapshot_while_blocked:?}"
        );
        assert_eq!(
            snapshot_while_blocked.journal_overflow_row_debt_capacity, 64,
            "test fixture should prove the explicit row-debt cap derived from overflow batches * raw batch size * journal coalesce limit"
        );
        assert_eq!(
            journal_rows_while_blocked, 1,
            "journal rows must not be marked persisted before the blocked writer actually commits"
        );
        assert_eq!(
            journal_state_while_blocked.row_count, 1,
            "recent_raw journal coverage/state must not advance beyond actually written rows"
        );

        let journal_catchup_started = Instant::now();
        loop {
            let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
            let journal_rows = journal_store
                .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
                .len();
            if journal_rows >= runtime_rows_while_blocked {
                break;
            }
            if journal_catchup_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "recent_raw journal did not catch up after blocker release; journal_rows={journal_rows} runtime_rows={runtime_rows_while_blocked}"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }

        writer.shutdown()?;
        let journal_store_after = SqliteStore::open(Path::new(&journal_db_path))?;
        let journal_rows_after = journal_store_after
            .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
            .len();
        let runtime_rows_after = runtime_store
            .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
            .len();
        assert_eq!(
            journal_rows_after, runtime_rows_after,
            "coalesced overflow rows must be written to recent_raw journal after pressure clears"
        );

        remove_sqlite_test_files(&runtime_db_path);
        remove_sqlite_test_files(&journal_db_path);
        Ok(())
    }
