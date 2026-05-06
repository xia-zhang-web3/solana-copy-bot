    #[test]
    fn observed_swap_writer_does_not_block_runtime_under_sqlite_lock() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-writer-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.ensure_observed_swap_writer_tables()?;

        let blocker_conn = Connection::open(Path::new(&db_path))
            .context("failed to open blocker sqlite connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let swap = SwapEvent {
            wallet: "wallet-async".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-async".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-async".to_string(),
            slot: 123,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        let runtime_handle = thread::spawn(move || -> Result<bool> {
            let runtime = Builder::new_current_thread().enable_all().build()?;
            runtime.block_on(async move {
                let writer =
                    ObservedSwapWriter::start(sqlite_path.clone())?;
                let swap_for_task = swap.clone();
                let insert_task = tokio::spawn(async move { writer.write(&swap_for_task).await });

                timeout(Duration::from_millis(50), sleep(Duration::from_millis(10)))
                    .await
                    .context(
                        "current-thread runtime stalled while observed swap writer was blocked",
                    )?;

                insert_task
                    .await
                    .context("observed swap task join failed")?
            })
        });

        std::thread::sleep(StdDuration::from_millis(250));
        blocker_conn.execute_batch("COMMIT")?;

        let inserted = runtime_handle
            .join()
            .expect("runtime thread panicked")
            .context("observed swap write should succeed after lock release")?;
        assert!(inserted, "observed swap insert should report a fresh write");

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-06T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-async");
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_enqueue_returns_before_locked_batch_commits() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-enqueue-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.ensure_observed_swap_writer_tables()?;

        let blocker_conn = Connection::open(Path::new(&db_path))
            .context("failed to open blocker sqlite connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let swap = SwapEvent {
            wallet: "wallet-enqueue".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-enqueue".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-enqueue".to_string(),
            slot: 124,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async move {
            let writer = ObservedSwapWriter::start(sqlite_path)?;
            timeout(Duration::from_millis(50), writer.enqueue(&swap))
                .await
                .context("observed swap enqueue should not wait for batch commit")??;
            Ok::<ObservedSwapWriter, anyhow::Error>(writer)
        })?;
        let pending_snapshot = writer.snapshot();
        assert_eq!(
            pending_snapshot.pending_requests, 1,
            "snapshot should expose the locked batch as one pending observed swap write"
        );

        let verify_before_commit = SqliteStore::open(Path::new(&db_path))?;
        let before_swaps = verify_before_commit.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert!(
            before_swaps.is_empty(),
            "enqueue should not imply the batch has already committed under sqlite lock"
        );

        std::thread::sleep(StdDuration::from_millis(50));
        blocker_conn.execute_batch("COMMIT")?;
        std::thread::sleep(StdDuration::from_millis(50));
        let committed_snapshot = writer.snapshot();
        assert_eq!(
            committed_snapshot.pending_requests, 0,
            "snapshot should clear pending depth after the blocked batch commits"
        );
        assert!(
            committed_snapshot.write_latency_ms_p95 >= 40,
            "snapshot should retain queue+commit latency once the blocked batch completes"
        );
        assert!(
            committed_snapshot.raw_batch_write_ms_p95 >= 40,
            "snapshot should expose raw batch latency separately when sqlite lock blocks the batch commit"
        );
        writer.shutdown()?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-enqueue");
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_retries_retryable_raw_lock_without_terminal_failure() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-retryable-lock-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.ensure_observed_swap_writer_tables()?;

        let control_conn = Connection::open(Path::new(&db_path))
            .context("failed to open retryable-lock control sqlite connection")?;
        control_conn.execute_batch(
            "CREATE TABLE raw_write_lock_gate(locked INTEGER NOT NULL);
             INSERT INTO raw_write_lock_gate(locked) VALUES (1);
             CREATE TRIGGER block_observed_swap_insert_retryable
             BEFORE INSERT ON observed_swaps
             WHEN (SELECT locked FROM raw_write_lock_gate LIMIT 1) = 1
             BEGIN
                 SELECT RAISE(FAIL, 'database is locked');
             END;",
        )?;

        let sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let swap = SwapEvent {
            wallet: "wallet-retryable-lock".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-retryable-lock".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-retryable-lock".to_string(),
            slot: 127,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-18T10:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        let runtime_handle = thread::spawn(move || -> Result<()> {
            let runtime = Builder::new_current_thread().enable_all().build()?;
            runtime.block_on(async move {
                let writer = ObservedSwapWriter::start(sqlite_path)?;
                timeout(Duration::from_millis(50), writer.enqueue(&swap))
                    .await
                    .context("retryable raw lock enqueue should not block runtime")??;
                timeout(Duration::from_millis(50), sleep(Duration::from_millis(10)))
                    .await
                    .context("current-thread runtime stalled while raw writer retried retryable lock")?;
                timeout(Duration::from_secs(5), async {
                    loop {
                        writer.ensure_running()?;
                        if writer.snapshot().pending_requests == 0 {
                            break Ok::<(), anyhow::Error>(());
                        }
                        sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .context("retryable raw lock batch should eventually drain without terminal writer failure")??;
                writer.ensure_running()?;
                writer.shutdown()?;
                Ok::<(), anyhow::Error>(())
            })
        });

        std::thread::sleep(StdDuration::from_millis(250));
        control_conn.execute("UPDATE raw_write_lock_gate SET locked = 0", [])?;

        runtime_handle
            .join()
            .expect("runtime thread panicked")
            .context("observed swap writer should survive retryable raw lock and recover")?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-18T09:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-retryable-lock");
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_try_enqueue_returns_false_when_channel_is_full() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-try-enqueue-full-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.ensure_observed_swap_writer_tables()?;

        let blocker_conn = Connection::open(Path::new(&db_path))
            .context("failed to open blocker sqlite connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(1, 1, None),
        )?;

        let first_swap = SwapEvent {
            wallet: "wallet-try-enqueue".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-try-enqueue-a".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-try-enqueue-a".to_string(),
            slot: 125,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:10:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        assert!(writer.try_enqueue(&first_swap)?);
        let mut saw_full = false;
        for idx in 0..32u64 {
            let swap = SwapEvent {
                token_out: format!("token-try-enqueue-{idx}"),
                signature: format!("sig-try-enqueue-{idx}"),
                slot: 126 + idx,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:10:01Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                ..first_swap.clone()
            };
            if !writer.try_enqueue(&swap)? {
                saw_full = true;
                break;
            }
        }
        assert!(
            saw_full,
            "non-blocking try_enqueue should report a full channel instead of waiting once the bounded queue saturates"
        );

        blocker_conn.execute_batch("COMMIT")?;
        std::thread::sleep(StdDuration::from_millis(50));
        writer.shutdown()?;
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_critical_enqueue_uses_reserved_capacity_before_full_stage1(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-critical-reserve-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.ensure_observed_swap_writer_tables()?;

        let blocker_conn = Connection::open(Path::new(&db_path))
            .context("failed to open blocker sqlite connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(2, 1, None),
        )?;

        let normal_swap = SwapEvent {
            wallet: "wallet-critical-reserve".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-critical-reserve-normal".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-critical-reserve-normal".to_string(),
            slot: 125,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:12:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let discovery_critical_swap = SwapEvent {
            token_out: "token-critical-reserve-priority".to_string(),
            signature: "sig-critical-reserve-priority".to_string(),
            slot: 126,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:12:01Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            ..normal_swap.clone()
        };

        assert!(writer.try_enqueue(&normal_swap)?);
        assert!(
            !writer.try_enqueue(&SwapEvent {
                token_out: "token-critical-reserve-blocked".to_string(),
                signature: "sig-critical-reserve-blocked".to_string(),
                slot: 127,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:12:02Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                ..normal_swap.clone()
            })?,
            "normal best-effort enqueue should yield once the reserved discovery-critical capacity is the only space left"
        );
        assert!(
            writer.try_enqueue_discovery_critical(&discovery_critical_swap)?,
            "discovery-critical enqueue should still claim the reserved writer slot"
        );

        blocker_conn.execute_batch("COMMIT")?;
        std::thread::sleep(StdDuration::from_millis(50));
        writer.shutdown()?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T12:11:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 2);
        assert!(
            swaps
                .iter()
                .any(|swap| swap.signature == "sig-critical-reserve-priority"),
            "the discovery-critical swap should still persist after the raw writer unblocks"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
