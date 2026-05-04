    #[test]
    fn observed_swap_writer_aggregate_gap_repair_under_hot_traffic_preserves_monotonic_covered_through_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-hot-gap-repair-monotonic",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let gap_swap = aggregate_gap_replay_test_swap(
            "sig-hot-gap-repair-monotonic-exact",
            840,
            "2026-04-28T13:20:00Z",
        );
        let tail_swap = aggregate_gap_replay_test_swap(
            "sig-hot-gap-repair-monotonic-tail",
            842,
            "2026-04-28T13:20:05Z",
        );
        store.insert_observed_swaps_batch(&[gap_swap.clone(), tail_swap.clone()])?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: gap_swap.ts_utc,
            slot: gap_swap.slot,
            signature: gap_swap.signature.clone(),
        };
        let tail_cursor = DiscoveryRuntimeCursor {
            ts_utc: tail_swap.ts_utc,
            slot: tail_swap.slot,
            signature: tail_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&tail_cursor)?;
        let (aggregate_sender, aggregate_worker) = start_discovery_aggregate_writer_loop_for_test(
            &db_path,
            ObservedSwapWriterConfig::for_test_with_aggregate_tuning(
                64,
                1,
                true,
                aggregate_write_config(),
                1,
                64,
                true,
                1,
                None,
            ),
            128,
        )?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;

        let stop = Arc::new(AtomicBool::new(false));
        let sent_count = Arc::new(AtomicUsize::new(0));
        let traffic = start_hot_aggregate_repair_traffic(
            aggregate_sender.clone(),
            Arc::clone(&stop),
            Arc::clone(&sent_count),
        );
        wait_for_hot_aggregate_repair_traffic(&sent_count)?;
        wait_for_materialization_gap_clear(&store)?;
        stop.store(true, Ordering::Relaxed);
        drop(aggregate_sender);
        traffic
            .join()
            .expect("hot aggregate traffic thread panicked");
        aggregate_worker
            .join()
            .expect("aggregate writer test thread panicked")?;

        let covered_through = store
            .load_discovery_scoring_covered_through_cursor()?
            .context("covered_through cursor should remain present")?;
        assert!(
            super::compare_discovery_runtime_cursors(&covered_through, &tail_cursor)
                != std::cmp::Ordering::Less,
            "continuous hot requests plus bounded repair must keep covered_through monotonic"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_under_hot_traffic_keeps_missing_exact_gap_latched_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-hot-gap-repair-missing",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let covered_swap = aggregate_gap_replay_test_swap(
            "sig-hot-gap-repair-missing-covered",
            849,
            "2026-04-28T13:24:55Z",
        );
        let missing_gap = aggregate_gap_replay_test_swap(
            "sig-hot-gap-repair-missing",
            850,
            "2026-04-28T13:25:00Z",
        );
        let tail_swap = aggregate_gap_replay_test_swap(
            "sig-hot-gap-repair-missing-tail",
            851,
            "2026-04-28T13:25:05Z",
        );
        store.insert_observed_swaps_batch(&[covered_swap.clone(), tail_swap.clone()])?;
        store.apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: missing_gap.ts_utc,
            slot: missing_gap.slot,
            signature: missing_gap.signature.clone(),
        };
        let tail_cursor = DiscoveryRuntimeCursor {
            ts_utc: tail_swap.ts_utc,
            slot: tail_swap.slot,
            signature: tail_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&discovery_runtime_cursor_for_swap(
            &covered_swap,
        ))?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;
        let (aggregate_sender, aggregate_worker) = start_discovery_aggregate_writer_loop_for_test(
            &db_path,
            ObservedSwapWriterConfig::for_test_with_aggregate_tuning(
                64,
                1,
                true,
                aggregate_write_config(),
                1,
                64,
                true,
                1,
                None,
            ),
            128,
        )?;

        let stop = Arc::new(AtomicBool::new(false));
        let sent_count = Arc::new(AtomicUsize::new(0));
        let traffic = start_hot_aggregate_repair_traffic(
            aggregate_sender.clone(),
            Arc::clone(&stop),
            Arc::clone(&sent_count),
        );
        wait_for_hot_aggregate_repair_traffic(&sent_count)?;
        std::thread::sleep(StdDuration::from_millis(250));
        stop.store(true, Ordering::Relaxed);
        drop(aggregate_sender);
        traffic
            .join()
            .expect("hot aggregate traffic thread panicked");
        aggregate_worker
            .join()
            .expect("aggregate writer test thread panicked")?;

        assert!(
            sent_count.load(Ordering::Relaxed) > 0,
            "test must keep hot aggregate requests flowing while missing-gap repair runs"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(gap_cursor),
            "missing exact latched gap row must remain fail-closed under hot traffic"
        );
        let covered_through = store
            .load_discovery_scoring_covered_through_cursor()?
            .context("covered_through cursor should remain present")?;
        assert!(
            super::compare_discovery_runtime_cursors(&covered_through, &tail_cursor)
                != std::cmp::Ordering::Less,
            "missing-gap repair under hot traffic must still preserve monotonic covered_through"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_snapshot_clears_pending_after_fast_successful_enqueue() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-fast-snapshot-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                true,
                aggregate_write_config(),
            )
        })?;

        for idx in 0..8 {
            let swap = SwapEvent {
                wallet: "wallet-fast-snapshot".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: format!("token-fast-snapshot-{idx}"),
                amount_in: 1.0,
                amount_out: 10.0 + idx as f64,
                signature: format!("sig-observed-swap-fast-snapshot-{idx}"),
                slot: 300 + idx as u64,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-14T14:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };
            runtime.block_on(async { writer.enqueue(&swap).await })?;
        }

        let snapshot = wait_for_observed_swap_writer_snapshot(
            &writer,
            "fast aggregate batch drain",
            |snapshot| snapshot.pending_requests == 0 && snapshot.discovery_scoring_ms_p95 >= 1,
        )?;
        assert_eq!(
            snapshot.pending_requests, 0,
            "fast successful batches must not leave phantom pending writer requests"
        );
        assert!(
            snapshot.write_latency_ms_p95 < 250,
            "fast successful batches should not report lock-scale writer latency"
        );
        assert!(
            snapshot.raw_batch_write_ms_p95 >= 1,
            "fast successful batches should still report a non-zero raw batch phase latency sample"
        );
        assert!(
            snapshot.observed_swaps_insert_ms_p95 >= 1,
            "fast successful batches should separately report the observed_swaps insert phase"
        );
        assert!(
            snapshot.wallet_activity_days_ms_p95 >= 1,
            "fast successful batches should separately report the wallet_activity_days upsert phase"
        );
        assert!(
            snapshot.discovery_scoring_ms_p95 >= 1,
            "aggregate-enabled writer batches should report aggregate phase latency separately"
        );
        assert!(
            snapshot.worker_busy_ms_p95 >= snapshot.raw_batch_write_ms_p95,
            "full writer occupancy should be at least as large as the raw batch phase"
        );
        assert!(
            snapshot.worker_busy_ms_p95 >= snapshot.discovery_scoring_ms_p95,
            "full writer occupancy should also cover the discovery scoring phase when aggregates are enabled"
        );
        assert!(
            snapshot.aggregate_queue_depth_batches <= 1,
            "startup-decoupled aggregate batches should leave at most a bounded transient aggregate queue sample behind"
        );
        assert_eq!(
            snapshot.aggregate_queue_capacity_batches, 32,
            "production aggregate queue capacity should be bounded to the raw queue expressed in batches"
        );

        writer.shutdown()?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T13:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 8);
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_snapshot_keeps_discovery_scoring_latency_zero_when_aggregates_disabled(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-no-aggregate-telemetry-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, false, aggregate_write_config(), None),
            )
        })?;

        let swap = SwapEvent {
            wallet: "wallet-no-aggregate-telemetry".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-no-aggregate-telemetry".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-no-aggregate-telemetry".to_string(),
            slot: 330,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-15T09:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        runtime.block_on(async { writer.enqueue(&swap).await })?;
        std::thread::sleep(StdDuration::from_millis(50));

        let snapshot = writer.snapshot();
        assert!(
            snapshot.raw_batch_write_ms_p95 >= 1,
            "aggregate-disabled writer batches should still report raw batch phase latency"
        );
        assert!(
            snapshot.observed_swaps_insert_ms_p95 >= 1,
            "aggregate-disabled writer batches should still report the observed_swaps insert phase"
        );
        assert!(
            snapshot.wallet_activity_days_ms_p95 >= 1,
            "aggregate-disabled writer batches should still report the wallet_activity_days upsert phase"
        );
        assert_eq!(
            snapshot.discovery_scoring_ms_p95, 0,
            "aggregate-disabled writer batches must keep discovery scoring latency at zero"
        );
        assert!(
            snapshot.worker_busy_ms_p95 >= snapshot.raw_batch_write_ms_p95,
            "aggregate-disabled full writer occupancy should still cover the raw batch phase"
        );
        assert_eq!(
            snapshot.aggregate_queue_depth_batches, 0,
            "aggregate-disabled writer must report no aggregate queue backlog"
        );
        assert_eq!(
            snapshot.aggregate_queue_capacity_batches, 0,
            "aggregate-disabled writer must report zero aggregate queue capacity"
        );

        writer.shutdown()?;
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_closes_channel_after_async_batch_insert_failure() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-async-fail-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for async failure trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_activity_days_insert
             BEFORE INSERT ON wallet_activity_days
             BEGIN
                 SELECT RAISE(FAIL, 'forced async observed swap failure');
             END;",
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )
        })?;

        let failing_swap = SwapEvent {
            wallet: "wallet-async-fail".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-async-fail".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-async-fail".to_string(),
            slot: 200,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let subsequent_swap = SwapEvent {
            signature: "sig-observed-swap-after-fail".to_string(),
            slot: 201,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:01:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            ..failing_swap.clone()
        };

        runtime.block_on(async { writer.enqueue(&failing_swap).await })?;
        std::thread::sleep(StdDuration::from_millis(50));

        let error = runtime
            .block_on(async { writer.enqueue(&subsequent_swap).await })
            .expect_err("writer channel should close after async raw batch insert failure");
        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT)
                || error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected enqueue-after-failure error: {error_chain}"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface async raw batch insert failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("forced async observed swap failure"),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T12:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert!(
            swaps.is_empty(),
            "failed async batch insert must not leave partially persisted observed swaps"
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }
