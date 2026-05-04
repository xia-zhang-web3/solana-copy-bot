    #[test]
    fn parse_token_holders_from_program_accounts_response_counts_unique_nonzero_owners(
    ) -> Result<()> {
        let response = json!({
            "jsonrpc": "2.0",
            "result": [
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "owner": "OwnerA",
                                    "tokenAmount": { "amount": "10" }
                                }
                            }
                        }
                    }
                },
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "owner": "OwnerA",
                                    "tokenAmount": { "amount": "5" }
                                }
                            }
                        }
                    }
                },
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "owner": "OwnerB",
                                    "tokenAmount": { "amount": "0" }
                                }
                            }
                        }
                    }
                },
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "owner": "OwnerC",
                                    "tokenAmount": { "amount": "42" }
                                }
                            }
                        }
                    }
                }
            ]
        });
        let holders = parse_token_holders_from_program_accounts_response(&response)?;
        assert_eq!(holders, 2);
        Ok(())
    }

    #[test]
    fn parse_token_holders_from_program_accounts_response_accepts_wrapped_value_array() -> Result<()>
    {
        let response = json!({
            "jsonrpc": "2.0",
            "result": {
                "value": [
                    {
                        "account": {
                            "data": {
                                "parsed": {
                                    "info": {
                                        "owner": "OwnerX",
                                        "tokenAmount": { "amount": "1" }
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        });
        let holders = parse_token_holders_from_program_accounts_response(&response)?;
        assert_eq!(holders, 1);
        Ok(())
    }

    #[test]
    fn parse_token_holders_from_program_accounts_response_rejects_invalid_shape() {
        let response = json!({
            "jsonrpc": "2.0",
            "result": {
                "value": {
                    "owner": "not-an-array"
                }
            }
        });
        let error = parse_token_holders_from_program_accounts_response(&response)
            .expect_err("invalid response shape must fail");
        assert!(
            error.to_string().contains("missing token accounts array"),
            "unexpected error: {error}"
        );
    }

    fn swap(
        signature: &str,
        wallet: &str,
        ts_utc: DateTime<Utc>,
        token_in: &str,
        token_out: &str,
        slot: u64,
    ) -> SwapEvent {
        SwapEvent {
            signature: signature.to_string(),
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: token_in.to_string(),
            token_out: token_out.to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot,
            ts_utc,
        }
    }

    fn seed_wallet_activity_interrupt_fixture(
        store: &mut SqliteStore,
        since: DateTime<Utc>,
        wallet_count: usize,
        prefix: &str,
    ) -> Result<Vec<String>> {
        let mut swaps = Vec::with_capacity(wallet_count);
        let mut wallet_ids = Vec::with_capacity(wallet_count);
        for idx in 0..wallet_count {
            let wallet_id = format!("{prefix}-wallet-{idx:05}");
            wallet_ids.push(wallet_id.clone());
            swaps.push(swap(
                &format!("{prefix}-sig-{idx:05}"),
                &wallet_id,
                since + Duration::seconds((idx % 3_600) as i64),
                SOL_MINT,
                &format!("{prefix}-token-{idx:05}"),
                idx as u64 + 1,
            ));
        }
        store.insert_observed_swaps_batch_with_activity_days(&swaps)?;
        Ok(wallet_ids)
    }

    fn spawn_interrupt_loop(
        interrupt_handle: rusqlite::InterruptHandle,
        warmup: StdDuration,
    ) -> (Arc<AtomicBool>, thread::JoinHandle<()>) {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let join = thread::spawn(move || {
            if warmup > StdDuration::from_millis(0) {
                thread::sleep(warmup);
            }
            while !stop_flag.load(Ordering::Relaxed) {
                interrupt_handle.interrupt();
                thread::sleep(StdDuration::from_micros(50));
            }
        });
        (stop, join)
    }

    #[derive(Debug, Clone, Copy)]
    struct CheckpointRecurrenceSummary {
        writes_before_reader: usize,
        writes_during_reader: usize,
        max_backlog_frames: i64,
    }

    #[derive(Debug, Clone, Copy)]
    enum CursorCheckpointRecurrenceReader {
        LegacyAfterCursorSingleStatement,
        ChunkedAfterCursor,
        LegacySolLegSingleStatement,
        ChunkedSolLeg,
    }

    impl CursorCheckpointRecurrenceReader {
        fn db_name(self) -> &'static str {
            match self {
                Self::LegacyAfterCursorSingleStatement => {
                    "observed-swap-after-cursor-single-statement-recurrence.db"
                }
                Self::ChunkedAfterCursor => "observed-swap-after-cursor-chunked-recurrence.db",
                Self::LegacySolLegSingleStatement => {
                    "observed-sol-leg-single-statement-recurrence.db"
                }
                Self::ChunkedSolLeg => "observed-sol-leg-chunked-recurrence.db",
            }
        }
    }

    fn run_checkpoint_recurrence_scenario(
        use_paged_reader: bool,
    ) -> Result<CheckpointRecurrenceSummary> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join(if use_paged_reader {
            "observed-swap-window-paged-recurrence.db"
        } else {
            "observed-swap-window-unpaged-recurrence.db"
        });
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-04-07T18:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let reader_window_start = now - Duration::days(2);
        let reader_window_end = now - Duration::hours(1);
        let mut seed_rows = Vec::new();
        for idx in 0..512usize {
            seed_rows.push(swap(
                &format!("sig-checkpoint-seed-{idx:04}"),
                &format!("wallet-seed-{:03}", idx % 24),
                reader_window_start + Duration::seconds(idx as i64),
                SOL_MINT,
                &format!("TokenCheckpointSeed{idx:04}"),
                10_000 + idx as u64,
            ));
        }
        seed_store.insert_observed_swaps_batch_with_activity_days(&seed_rows)?;
        seed_store.checkpoint_wal_truncate()?;

        let writes_completed = Arc::new(AtomicUsize::new(0));
        let stop_writes = Arc::new(AtomicBool::new(false));
        let writer_db_path = db_path.clone();
        let writer_writes_completed = Arc::clone(&writes_completed);
        let writer_stop = Arc::clone(&stop_writes);
        let writer = thread::spawn(move || -> Result<()> {
            let writer_store = SqliteStore::open(Path::new(&writer_db_path))?;
            writer_store
                .conn
                .pragma_update(None, "wal_autocheckpoint", 1_i64)
                .context(
                    "failed to force aggressive wal_autocheckpoint for recurrence test writer",
                )?;
            let mut counter = 0usize;
            while !writer_stop.load(Ordering::Relaxed) {
                let swap = swap(
                    &format!("sig-checkpoint-live-{counter:06}"),
                    &format!("wallet-live-{:03}", counter % 32),
                    now + Duration::milliseconds(counter as i64),
                    SOL_MINT,
                    &format!("TokenCheckpointLive{:06}", counter % 64),
                    20_000 + counter as u64,
                );
                writer_store.insert_observed_swaps_batch_with_activity_days(&[swap])?;
                writer_writes_completed.fetch_add(1, Ordering::Relaxed);
                counter = counter.saturating_add(1);
            }
            Ok(())
        });

        let baseline_started = Instant::now();
        while writes_completed.load(Ordering::Relaxed) < 32 {
            if baseline_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("writer failed to establish post-checkpoint baseline throughput");
            }
            thread::sleep(StdDuration::from_millis(10));
        }
        let writes_before_reader = writes_completed.load(Ordering::Relaxed);

        let reader_started = Arc::new(AtomicBool::new(false));
        let reader_db_path = db_path.clone();
        let reader_started_flag = Arc::clone(&reader_started);
        let reader = thread::spawn(move || -> Result<()> {
            let reader_store = SqliteStore::open_read_only(Path::new(&reader_db_path))?;
            if use_paged_reader {
                reader_store.for_each_observed_swap_in_window_paged(
                    reader_window_start,
                    reader_window_end,
                    32,
                    |swap| {
                        if !reader_started_flag.swap(true, Ordering::Relaxed) {
                            let _ = swap.signature.as_str();
                        }
                        thread::sleep(StdDuration::from_millis(1));
                        Ok(())
                    },
                )?;
            } else {
                reader_store.for_each_observed_swap_in_window(
                    reader_window_start,
                    reader_window_end,
                    |swap| {
                        if !reader_started_flag.swap(true, Ordering::Relaxed) {
                            let _ = swap.signature.as_str();
                        }
                        thread::sleep(StdDuration::from_millis(1));
                        Ok(())
                    },
                )?;
            }
            Ok(())
        });

        let reader_started_wait = Instant::now();
        while !reader_started.load(Ordering::Relaxed) {
            if reader_started_wait.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("reader failed to start recurrence scenario");
            }
            thread::sleep(StdDuration::from_millis(5));
        }

        let monitor_store = SqliteStore::open(Path::new(&db_path))?;
        let mut max_backlog_frames = 0i64;
        while !reader.is_finished() {
            let (_, log_frames, checkpointed_frames) = monitor_store.checkpoint_wal_passive()?;
            max_backlog_frames =
                max_backlog_frames.max(log_frames.saturating_sub(checkpointed_frames));
            thread::sleep(StdDuration::from_millis(10));
        }
        reader
            .join()
            .expect("reader thread panicked")
            .context("reader recurrence scenario failed")?;

        stop_writes.store(true, Ordering::Relaxed);
        writer
            .join()
            .expect("writer thread panicked")
            .context("writer recurrence scenario failed")?;

        let writes_during_reader = writes_completed
            .load(Ordering::Relaxed)
            .saturating_sub(writes_before_reader);
        monitor_store.checkpoint_wal_truncate()?;

        Ok(CheckpointRecurrenceSummary {
            writes_before_reader,
            writes_during_reader,
            max_backlog_frames,
        })
    }

    fn run_cursor_checkpoint_recurrence_scenario(
        reader_mode: CursorCheckpointRecurrenceReader,
    ) -> Result<CheckpointRecurrenceSummary> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join(reader_mode.db_name());
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-04-09T18:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let reader_window_start = now - Duration::days(2);
        let reader_window_end = now - Duration::hours(1);
        let mut seed_rows = Vec::new();
        for idx in 0..8_192usize {
            let token_in = if idx % 2 == 0 {
                SOL_MINT.to_string()
            } else {
                format!("TokenCursorSeedIn{idx:05}")
            };
            let token_out = if idx % 2 == 0 {
                format!("TokenCursorSeedOut{idx:05}")
            } else {
                SOL_MINT.to_string()
            };
            seed_rows.push(swap(
                &format!("sig-cursor-checkpoint-seed-{idx:05}"),
                &format!("wallet-cursor-seed-{:03}", idx % 96),
                reader_window_start + Duration::seconds(idx as i64),
                &token_in,
                &token_out,
                30_000 + idx as u64,
            ));
        }
        seed_store.insert_observed_swaps_batch_with_activity_days(&seed_rows)?;
        seed_store.checkpoint_wal_truncate()?;

        let writes_completed = Arc::new(AtomicUsize::new(0));
        let stop_writes = Arc::new(AtomicBool::new(false));
        let writer_db_path = db_path.clone();
        let writer_writes_completed = Arc::clone(&writes_completed);
        let writer_stop = Arc::clone(&stop_writes);
        let writer = thread::spawn(move || -> Result<()> {
            let writer_store = SqliteStore::open(Path::new(&writer_db_path))?;
            writer_store
                .conn
                .pragma_update(None, "wal_autocheckpoint", 1_i64)
                .context(
                    "failed to force aggressive wal_autocheckpoint for cursor recurrence test writer",
                )?;
            let mut counter = 0usize;
            while !writer_stop.load(Ordering::Relaxed) {
                let live_swap = swap(
                    &format!("sig-cursor-checkpoint-live-{counter:06}"),
                    &format!("wallet-cursor-live-{:03}", counter % 64),
                    now + Duration::milliseconds(counter as i64),
                    SOL_MINT,
                    &format!("TokenCursorLive{:06}", counter % 128),
                    50_000 + counter as u64,
                );
                writer_store.insert_observed_swaps_batch_with_activity_days(&[live_swap])?;
                writer_writes_completed.fetch_add(1, Ordering::Relaxed);
                counter = counter.saturating_add(1);
            }
            Ok(())
        });

        let baseline_started = Instant::now();
        while writes_completed.load(Ordering::Relaxed) < 32 {
            if baseline_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "writer failed to establish post-checkpoint baseline throughput for cursor recurrence scenario"
                );
            }
            thread::sleep(StdDuration::from_millis(10));
        }
        let writes_before_reader = writes_completed.load(Ordering::Relaxed);

        let reader_started = Arc::new(AtomicBool::new(false));
        let reader_db_path = db_path.clone();
        let reader_started_flag = Arc::clone(&reader_started);
        let reader = thread::spawn(move || -> Result<()> {
            let reader_store = SqliteStore::open_read_only(Path::new(&reader_db_path))?;
            let reader_deadline = Instant::now() + StdDuration::from_secs(30);
            match reader_mode {
                CursorCheckpointRecurrenceReader::LegacyAfterCursorSingleStatement => {
                    reader_store.for_each_observed_swap_after_cursor_single_statement_with_budget(
                        reader_window_start - Duration::seconds(1),
                        0,
                        "",
                        4_096,
                        reader_deadline,
                        |swap| {
                            if !reader_started_flag.swap(true, Ordering::Relaxed) {
                                let _ = swap.signature.as_str();
                            }
                            thread::sleep(StdDuration::from_millis(1));
                            Ok(())
                        },
                    )?;
                }
                CursorCheckpointRecurrenceReader::ChunkedAfterCursor => {
                    reader_store.for_each_observed_swap_after_cursor_with_budget(
                        reader_window_start - Duration::seconds(1),
                        0,
                        "",
                        4_096,
                        reader_deadline,
                        |swap| {
                            if !reader_started_flag.swap(true, Ordering::Relaxed) {
                                let _ = swap.signature.as_str();
                            }
                            thread::sleep(StdDuration::from_millis(1));
                            Ok(())
                        },
                    )?;
                }
                CursorCheckpointRecurrenceReader::LegacySolLegSingleStatement => {
                    reader_store
                        .for_each_observed_sol_leg_swap_in_window_after_cursor_single_statement_with_budget(
                            reader_window_start,
                            reader_window_end,
                            None,
                            4_096,
                            reader_deadline,
                            |swap| {
                                if !reader_started_flag.swap(true, Ordering::Relaxed) {
                                    let _ = swap.signature.as_str();
                                }
                                thread::sleep(StdDuration::from_millis(1));
                                Ok(())
                            },
                        )?;
                }
                CursorCheckpointRecurrenceReader::ChunkedSolLeg => {
                    reader_store
                        .for_each_observed_sol_leg_swap_in_window_after_cursor_with_budget(
                            reader_window_start,
                            reader_window_end,
                            None,
                            4_096,
                            reader_deadline,
                            |swap| {
                                if !reader_started_flag.swap(true, Ordering::Relaxed) {
                                    let _ = swap.signature.as_str();
                                }
                                thread::sleep(StdDuration::from_millis(1));
                                Ok(())
                            },
                        )?;
                }
            }
            Ok(())
        });

        let reader_started_wait = Instant::now();
        while !reader_started.load(Ordering::Relaxed) {
            if reader_started_wait.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("reader failed to start cursor recurrence scenario");
            }
            thread::sleep(StdDuration::from_millis(5));
        }

        let monitor_store = SqliteStore::open(Path::new(&db_path))?;
        let mut max_backlog_frames = 0i64;
        while !reader.is_finished() {
            let (_, log_frames, checkpointed_frames) = monitor_store.checkpoint_wal_passive()?;
            max_backlog_frames =
                max_backlog_frames.max(log_frames.saturating_sub(checkpointed_frames));
            thread::sleep(StdDuration::from_millis(10));
        }
        reader
            .join()
            .expect("reader thread panicked")
            .context("cursor recurrence scenario reader failed")?;

        stop_writes.store(true, Ordering::Relaxed);
        writer
            .join()
            .expect("writer thread panicked")
            .context("cursor recurrence scenario writer failed")?;

        let writes_during_reader = writes_completed
            .load(Ordering::Relaxed)
            .saturating_sub(writes_before_reader);
        monitor_store.checkpoint_wal_truncate()?;

        Ok(CheckpointRecurrenceSummary {
            writes_before_reader,
            writes_during_reader,
            max_backlog_frames,
        })
    }
