    fn aggregate_write_config() -> DiscoveryAggregateWriteConfig {
        DiscoveryAggregateWriteConfig::default()
    }

    fn migrated_observed_swap_writer_test_db(prefix: &str) -> Result<PathBuf> {
        let unique = format!(
            "{prefix}-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        Ok(db_path)
    }

    fn remove_sqlite_test_files(db_path: &Path) {
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
    }

    fn startup_gate_test_swap(signature: &str, slot: u64) -> SwapEvent {
        SwapEvent {
            wallet: "wallet-startup-gate-test".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("token-{signature}"),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot,
            ts_utc: DateTime::parse_from_rfc3339("2026-04-28T12:30:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        }
    }

    fn aggregate_gap_replay_test_swap(signature: &str, slot: u64, ts_utc: &str) -> SwapEvent {
        SwapEvent {
            wallet: "wallet-aggregate-gap-replay-test".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-aggregate-gap-replay-test".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot,
            ts_utc: DateTime::parse_from_rfc3339(ts_utc)
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        }
    }

    fn hot_aggregate_repair_request(idx: usize) -> super::DiscoveryAggregateWriteRequest {
        super::DiscoveryAggregateWriteRequest {
            inserted_swaps: vec![SwapEvent {
                wallet: format!("wallet-hot-aggregate-repair-{idx:05}"),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: format!("token-hot-aggregate-repair-{idx:05}"),
                amount_in: 1.0,
                amount_out: 10.0 + idx as f64,
                signature: format!("sig-hot-aggregate-repair-{idx:05}"),
                slot: 100_000 + idx as u64,
                ts_utc: DateTime::parse_from_rfc3339("2026-04-28T13:30:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc)
                    + ChronoDuration::milliseconds(idx as i64),
                exact_amounts: None,
            }],
            batch_started: Instant::now(),
        }
    }

    fn start_discovery_aggregate_writer_loop_for_test(
        db_path: &Path,
        config: ObservedSwapWriterConfig,
        channel_capacity_batches: usize,
    ) -> Result<(
        std_mpsc::SyncSender<super::DiscoveryAggregateWriteRequest>,
        thread::JoinHandle<Result<()>>,
    )> {
        let (aggregate_sender, aggregate_receiver) = std_mpsc::sync_channel::<
            super::DiscoveryAggregateWriteRequest,
        >(channel_capacity_batches);
        let (startup_sender, startup_receiver) =
            std_mpsc::channel::<std::result::Result<(), String>>();
        let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
        let aggregate_sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let worker = thread::Builder::new()
            .name("copybot-discovery-aggregate-writer-test".to_string())
            .spawn(move || {
                super::discovery_aggregate_writer_loop(
                    aggregate_sqlite_path,
                    aggregate_receiver,
                    startup_sender,
                    config,
                    telemetry,
                )
            })
            .context("failed to spawn discovery aggregate writer test thread")?;
        match startup_receiver
            .recv_timeout(StdDuration::from_secs(5))
            .context("timed out waiting for aggregate writer startup")?
        {
            Ok(()) => Ok((aggregate_sender, worker)),
            Err(message) => Err(anyhow!(message)).context("aggregate writer startup failed"),
        }
    }

    fn start_hot_aggregate_repair_traffic(
        sender: std_mpsc::SyncSender<super::DiscoveryAggregateWriteRequest>,
        stop: Arc<AtomicBool>,
        sent_count: Arc<AtomicUsize>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut idx = 0usize;
            while !stop.load(Ordering::Relaxed) {
                match sender.try_send(hot_aggregate_repair_request(idx)) {
                    Ok(()) => {
                        idx = idx.saturating_add(1);
                        sent_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(std_mpsc::TrySendError::Full(_request)) => {
                        std::thread::sleep(StdDuration::from_millis(1));
                    }
                    Err(std_mpsc::TrySendError::Disconnected(_request)) => break,
                }
            }
        })
    }

    fn wait_for_hot_aggregate_repair_traffic(sent_count: &AtomicUsize) -> Result<()> {
        let started = Instant::now();
        loop {
            if sent_count.load(Ordering::Relaxed) > 0 {
                return Ok(());
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("timed out waiting for hot aggregate repair traffic");
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn wait_for_materialization_gap_clear(store: &SqliteStore) -> Result<()> {
        let started = Instant::now();
        loop {
            if store
                .load_discovery_scoring_materialization_gap_cursor()?
                .is_none()
            {
                return Ok(());
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("timed out waiting for materialization gap cursor to clear");
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn start_observed_swap_writer_loop_for_startup_test(
        db_path: &Path,
        config: ObservedSwapWriterConfig,
        aggregate_sender: Option<std_mpsc::SyncSender<super::DiscoveryAggregateWriteRequest>>,
        aggregate_startup_receiver: Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
        journal_sender: Option<std_mpsc::SyncSender<super::RecentRawJournalWriteRequest>>,
        journal_startup_receiver: Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
    ) -> Result<ObservedSwapWriter> {
        let (sender, receiver) = tokio::sync::mpsc::channel(config.channel_capacity);
        let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
        let terminal_failure_message = Arc::new(Mutex::new(None));
        let normal_try_enqueue_soft_limit =
            super::observed_swap_writer_normal_try_enqueue_soft_limit(&config);
        let raw_worker_sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let raw_worker_telemetry = Arc::clone(&telemetry);
        let raw_worker_terminal_failure_message = Arc::clone(&terminal_failure_message);
        let raw_worker = thread::Builder::new()
            .name("copybot-observed-swap-writer-startup-test".to_string())
            .spawn(move || {
                let result = super::observed_swap_writer_loop(
                    raw_worker_sqlite_path,
                    receiver,
                    aggregate_sender,
                    aggregate_startup_receiver,
                    journal_sender,
                    journal_startup_receiver,
                    config,
                    raw_worker_telemetry,
                    Arc::clone(&raw_worker_terminal_failure_message),
                );
                if let Err(error) = &result {
                    super::set_terminal_failure_message(
                        &raw_worker_terminal_failure_message,
                        format!("{error:#}"),
                    );
                }
                result
            })
            .context("failed to spawn observed swap writer startup test thread")?;

        Ok(ObservedSwapWriter {
            sender,
            normal_try_enqueue_soft_limit,
            raw_worker: Some(raw_worker),
            aggregate_worker: None,
            journal_worker: None,
            telemetry,
            terminal_failure_message,
        })
    }

    fn wait_for_observed_swap_signatures(
        db_path: &Path,
        expected_signatures: &[&str],
    ) -> Result<Vec<SwapEvent>> {
        let started = Instant::now();
        let since = DateTime::parse_from_rfc3339("2026-04-28T12:29:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        loop {
            let verify_store = SqliteStore::open(db_path)?;
            let swaps = verify_store.load_observed_swaps_since(since)?;
            if expected_signatures
                .iter()
                .all(|signature| swaps.iter().any(|swap| swap.signature == **signature))
            {
                return Ok(swaps);
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "timed out waiting for observed swap signatures {:?}; saw {:?}",
                    expected_signatures,
                    swaps
                        .iter()
                        .map(|swap| swap.signature.as_str())
                        .collect::<Vec<_>>()
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn wait_for_writer_terminal_failure(writer: &ObservedSwapWriter) -> Result<String> {
        let started = Instant::now();
        loop {
            match writer.ensure_running() {
                Ok(()) => {}
                Err(error) => return Ok(format!("{error:#}")),
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("timed out waiting for observed swap writer terminal failure");
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn wait_for_observed_swap_writer_snapshot(
        writer: &ObservedSwapWriter,
        description: &str,
        predicate: impl Fn(&super::ObservedSwapWriterSnapshot) -> bool,
    ) -> Result<super::ObservedSwapWriterSnapshot> {
        let started = Instant::now();
        loop {
            let snapshot = writer.snapshot();
            if predicate(&snapshot) {
                return Ok(snapshot);
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "timed out waiting for observed swap writer {description}; snapshot={snapshot:?}"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn wait_for_discovery_scoring_covered_through_at_least(
        db_path: &Path,
        expected_ts: DateTime<Utc>,
    ) -> Result<()> {
        let started = Instant::now();
        loop {
            let verify_store = SqliteStore::open(db_path)?;
            if verify_store
                .load_discovery_scoring_covered_through()?
                .is_some_and(|covered_through| covered_through >= expected_ts)
            {
                return Ok(());
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "timed out waiting for discovery scoring coverage through {expected_ts}"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn wait_for_discovery_scoring_materialization_gap_cursor(
        db_path: &Path,
    ) -> Result<DiscoveryRuntimeCursor> {
        let started = Instant::now();
        loop {
            let verify_store = SqliteStore::open(db_path)?;
            if let Some(cursor) =
                verify_store.load_discovery_scoring_materialization_gap_cursor()?
            {
                return Ok(cursor);
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("timed out waiting for discovery scoring materialization gap cursor");
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn seed_discovery_scoring_rug_finalize_replay_fixture(
        db_path: &Path,
    ) -> Result<(SwapEvent, SwapEvent, SwapEvent)> {
        let seed_store = SqliteStore::open(db_path)?;
        let covered_swap = SwapEvent {
            wallet: "wallet-rug-finalize-replay".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-rug-finalize-covered".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-rug-finalize-replay-covered".to_string(),
            slot: 300,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-16T10:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let first_replay_swap = SwapEvent {
            wallet: "wallet-rug-finalize-replay".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-rug-finalize-first".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-rug-finalize-replay-first".to_string(),
            slot: 301,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-16T10:00:01Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let tail_replay_swap = SwapEvent {
            wallet: "wallet-rug-finalize-replay".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-rug-finalize-tail".to_string(),
            amount_in: 2.0,
            amount_out: 20.0,
            signature: "sig-rug-finalize-replay-tail".to_string(),
            slot: 302,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-16T10:00:03Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[
            covered_swap.clone(),
            first_replay_swap.clone(),
            tail_replay_swap.clone(),
        ])?;
        seed_store
            .apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        seed_store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: covered_swap.ts_utc,
            slot: covered_swap.slot,
            signature: covered_swap.signature.clone(),
        })?;
        Ok((covered_swap, first_replay_swap, tail_replay_swap))
    }

    fn sqlite_epoch_millis_expression() -> &'static str {
        "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
    }

    fn sqlite_fail_until_epoch_ms(delay: StdDuration) -> i64 {
        let delay_ms = delay.as_millis().min(i64::MAX as u128) as i64;
        Utc::now().timestamp_millis().saturating_add(delay_ms)
    }

    fn assert_discovery_scoring_replay_reached_tail(
        db_path: &Path,
        tail_swap: &SwapEvent,
    ) -> Result<()> {
        let verify_store = SqliteStore::open(db_path)?;
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: tail_swap.ts_utc,
                slot: tail_swap.slot,
                signature: tail_swap.signature.clone(),
            }),
            "successful aggregate replay must advance coverage only after apply, rug finalize, and cursor update succeed"
        );
        assert_eq!(
            verify_store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "successful aggregate replay must not leave materialization gap evidence latched"
        );
        Ok(())
    }

    fn discovery_runtime_cursor_for_swap(swap: &SwapEvent) -> DiscoveryRuntimeCursor {
        DiscoveryRuntimeCursor {
            ts_utc: swap.ts_utc,
            slot: swap.slot,
            signature: swap.signature.clone(),
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct RecentRawJournalBackpressureSummary {
        baseline_rows_persisted: usize,
        pending_requests_after_load: usize,
        journal_queue_depth_after_load: usize,
        journal_overflow_depth_after_load: usize,
        max_pending_requests: usize,
        max_journal_queue_depth_batches: usize,
        max_journal_overflow_depth_batches: usize,
        persisted_rows_after_load: usize,
        runtime_wal_bytes_after_load: u64,
        sqlite_write_retry_delta: u64,
        sqlite_busy_error_delta: u64,
    }

    #[derive(Debug, Clone, Copy)]
    struct DiscoveryAggregateBackpressureSummary {
        baseline_rows_persisted: usize,
        pending_requests_after_load: usize,
        pending_requests_after_idle: usize,
        aggregate_queue_depth_after_load: usize,
        aggregate_queue_depth_after_idle: usize,
        aggregate_overflow_depth_after_load: usize,
        aggregate_overflow_depth_after_idle: usize,
        max_pending_requests: usize,
        max_aggregate_queue_depth_batches: usize,
        max_aggregate_overflow_depth_batches: usize,
        persisted_rows_after_load: usize,
        runtime_wal_bytes_after_load: u64,
        sqlite_write_retry_delta: u64,
        sqlite_busy_error_delta: u64,
        gap_cursor_present_after_load: bool,
        gap_cursor_cleared_after_idle: bool,
        covered_through_reached_tail_after_idle: bool,
    }

    fn recent_raw_journal_backpressure_swap(idx: usize, ts: DateTime<Utc>) -> SwapEvent {
        SwapEvent {
            wallet: format!("wallet-journal-backpressure-{:03}", idx % 32),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("token-journal-backpressure-{:03}", idx % 64),
            amount_in: 1.0,
            amount_out: 10.0 + idx as f64,
            signature: format!("sig-journal-backpressure-{idx:05}"),
            slot: 10_000 + idx as u64,
            ts_utc: ts + ChronoDuration::milliseconds(idx as i64),
            exact_amounts: None,
        }
    }

    fn recent_raw_journal_write_request_for_test(
        start_idx: usize,
        rows: usize,
        ts: DateTime<Utc>,
    ) -> super::RecentRawJournalWriteRequest {
        super::RecentRawJournalWriteRequest {
            inserted_swaps: (start_idx..start_idx.saturating_add(rows))
                .map(|idx| recent_raw_journal_backpressure_swap(idx, ts))
                .collect(),
        }
    }

    fn recent_raw_journal_write_summary_for_test(
        processed_rows: usize,
        inserted_rows: usize,
    ) -> RecentRawJournalWriteSummary {
        RecentRawJournalWriteSummary {
            batch_rows: processed_rows,
            inserted_rows,
            recent_raw_bulk_rows_processed: processed_rows,
            recent_raw_bulk_rows_inserted: inserted_rows,
            ..RecentRawJournalWriteSummary::default()
        }
    }

    fn discovery_aggregate_backpressure_swap(idx: usize, ts: DateTime<Utc>) -> SwapEvent {
        SwapEvent {
            wallet: format!("wallet-aggregate-backpressure-{idx:05}"),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("token-aggregate-backpressure-{idx:05}"),
            amount_in: 1.0,
            amount_out: 20.0 + idx as f64,
            signature: format!("sig-aggregate-backpressure-{idx:05}"),
            slot: 20_000 + idx as u64,
            ts_utc: ts + ChronoDuration::milliseconds(idx as i64),
            exact_amounts: None,
        }
    }
