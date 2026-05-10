use super::*;

#[test]
fn snapshot_into_path_with_policy_completes_under_concurrent_source_writes_when_snapshot_is_pinned(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let source_path = temp.path().join("snapshot-source-pinned.db");
    let destination_path = temp.path().join("snapshot-destination-pinned.db");

    {
        let seed_store = SqliteStore::open(Path::new(&source_path))?;
        seed_store
            .conn
            .execute_batch("CREATE TABLE snapshot_source(id INTEGER PRIMARY KEY, value TEXT);")
            .context("failed creating pinned snapshot source table")?;
        let large_value = "x".repeat(2048);
        for idx in 0..2048 {
            seed_store
                .conn
                .execute(
                    "INSERT INTO snapshot_source(id, value) VALUES (?1, ?2)",
                    params![idx, large_value],
                )
                .context("failed seeding pinned snapshot source table")?;
        }
    }

    let source_store = SqliteStore::open_read_only(Path::new(&source_path))?;
    let start_barrier = Arc::new(std::sync::Barrier::new(2));
    let stop_writes = Arc::new(AtomicBool::new(false));
    let writer_path = source_path.clone();
    let writer_barrier = start_barrier.clone();
    let writer_stop = stop_writes.clone();
    let writer = thread::spawn(move || -> Result<()> {
        let writer_store = SqliteStore::open(Path::new(&writer_path))?;
        writer_store
            .conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten concurrent writer busy timeout")?;
        writer_barrier.wait();
        let mut counter: i64 = 3_000;
        while !writer_stop.load(Ordering::Relaxed) {
            let row_id = (counter % 256) + 1;
            let _ = writer_store.conn.execute(
                "UPDATE snapshot_source SET value = ?1 WHERE id = ?2",
                params![format!("writer-{counter}"), row_id],
            );
            if counter % 16 == 0 {
                let _ = writer_store.conn.execute(
                    "INSERT INTO snapshot_source(id, value) VALUES (?1, ?2)",
                    params![counter, format!("writer-insert-{counter}")],
                );
            }
            counter += 1;
        }
        Ok(())
    });

    start_barrier.wait();
    let outcome = source_store.snapshot_into_path_with_policy(
        &destination_path,
        &SqliteSnapshotPolicy {
            busy_timeout: StdDuration::from_millis(5),
            pages_per_step: 256,
            pause_between_steps: StdDuration::from_millis(0),
            retry_backoff_ms: vec![1, 5, 10],
            max_attempt_duration: Some(StdDuration::from_secs(2)),
            pin_source_snapshot: true,
        },
    )?;
    stop_writes.store(true, Ordering::Relaxed);
    writer
        .join()
        .expect("writer thread panicked")
        .context("concurrent writer thread failed")?;

    let SqliteSnapshotOutcome::Written(summary) = outcome else {
        anyhow::bail!("expected pinned source snapshot backup to complete");
    };
    assert!(
        summary.total_page_count > 0,
        "written snapshot must report total page count"
    );
    assert_eq!(
        summary.copied_page_count, summary.total_page_count,
        "written snapshot must report full page coverage"
    );

    let snapshot_store = SqliteStore::open_read_only(&destination_path)?;
    let copied_rows: i64 =
        snapshot_store
            .conn
            .query_row("SELECT COUNT(*) FROM snapshot_source", [], |row| row.get(0))?;
    assert!(
        copied_rows >= 2048,
        "snapshot must contain seeded source rows"
    );
    Ok(())
}

#[test]
fn insert_observed_swaps_batch_returns_insert_flags_in_order() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-swap-batch-flags.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let swap_a = SwapEvent {
        wallet: "wallet-batch".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "token-a".to_string(),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: "sig-batch-a".to_string(),
        slot: 100,
        ts_utc: now,
        exact_amounts: None,
    };
    let swap_b = SwapEvent {
        wallet: "wallet-batch".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "token-b".to_string(),
        amount_in: 2.0,
        amount_out: 20.0,
        signature: "sig-batch-b".to_string(),
        slot: 101,
        ts_utc: now + Duration::seconds(1),
        exact_amounts: None,
    };

    let inserted =
        store.insert_observed_swaps_batch(&[swap_a.clone(), swap_a.clone(), swap_b.clone()])?;
    assert_eq!(inserted, vec![true, false, true]);

    let swaps = store.load_observed_swaps_since(now - Duration::seconds(1))?;
    assert_eq!(swaps.len(), 2);
    assert_eq!(swaps[0].signature, "sig-batch-a");
    assert_eq!(swaps[1].signature, "sig-batch-b");
    Ok(())
}

#[test]
fn observed_swap_roundtrip_preserves_exact_amounts() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-swap-exact-roundtrip.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let swap = SwapEvent {
        wallet: "wallet-exact".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "token-exact".to_string(),
        amount_in: 1.0,
        amount_out: 100.0,
        signature: "sig-exact".to_string(),
        slot: 100,
        ts_utc: now,
        exact_amounts: Some(ExactSwapAmounts {
            amount_in_raw: "1000000000".to_string(),
            amount_in_decimals: 9,
            amount_out_raw: "100000000".to_string(),
            amount_out_decimals: 6,
        }),
    };

    assert!(store.insert_observed_swap(&swap)?);
    let swaps = store.load_observed_swaps_since(now - Duration::seconds(1))?;
    assert_eq!(swaps.len(), 1);
    assert_eq!(swaps[0].exact_amounts, swap.exact_amounts);
    Ok(())
}

#[test]
fn observed_swap_reads_reject_noncanonical_timestamp_and_negative_slot() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-swap-read-fail-closed.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    store.conn.execute(
        "INSERT INTO observed_swaps(signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts)
         VALUES ('sig-bad-ts', 'wallet-bad-ts', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 1, '2026-03-06T12:00:00Z')",
        [],
    )?;
    let bad_ts = store
        .load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-06T11:00:00+00:00")
                .expect("timestamp")
                .with_timezone(&Utc),
        )
        .expect_err("legacy observed reads must reject noncanonical timestamps");
    assert!(format!("{bad_ts:#}").contains("observed_swaps.ts is not canonical UTC"));

    store.conn.execute("DELETE FROM observed_swaps", [])?;
    store.conn.execute(
        "INSERT INTO observed_swaps(signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts)
         VALUES ('sig-negative-slot', 'wallet-negative-slot', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, -1, '2026-03-06T12:00:00+00:00')",
        [],
    )?;
    let bad_slot = store
        .load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-06T11:00:00+00:00")
                .expect("timestamp")
                .with_timezone(&Utc),
        )
        .expect_err("legacy observed reads must reject negative slots");
    assert!(format!("{bad_slot:#}").contains("observed_swaps.slot is negative"));
    Ok(())
}

#[test]
fn legacy_observed_swaps_aggregate_reads_reject_hidden_noncanonical_timestamp() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("legacy-observed-aggregate-hidden-bad-ts.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;
    store.conn.execute(
        "INSERT INTO observed_swaps(signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts)
         VALUES ('sig-good', 'wallet-good', 'raydium', 'SOL', 'TOKEN_A', 1.0, 2.0, 10, '2026-03-06T12:00:00+00:00')",
        [],
    )?;
    store.conn.execute(
        "INSERT INTO observed_swaps(signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts)
         VALUES ('sig-hidden-bad-ts', 'wallet-hidden', 'raydium', 'SOL', 'TOKEN_B', 1.0, 2.0, 11, '2026-03-06T12:30:00Z')",
        [],
    )?;

    let since = DateTime::parse_from_rfc3339("2026-03-06T11:00:00+00:00")
        .expect("timestamp")
        .with_timezone(&Utc);
    let until = DateTime::parse_from_rfc3339("2026-03-06T13:00:00+00:00")
        .expect("timestamp")
        .with_timezone(&Utc);
    let deadline = Instant::now() + StdDuration::from_secs(5);

    for error in [
        store
            .observed_wallet_activity_page_in_window_with_budget(
                since, until, None, 10, 50, deadline,
            )
            .expect_err("wallet activity page must fail closed before LIMIT can hide a bad ts"),
        store
            .observed_swaps_row_count_since(since)
            .expect_err("coverage count must fail closed before aggregate can hide a bad ts"),
        store
            .recent_observed_swap_counts_for_wallets(since, &[String::from("wallet-good")])
            .expect_err("recent wallet counts must fail closed before aggregate can hide a bad ts"),
        store
            .token_market_stats("TOKEN_A", until)
            .expect_err("token market stats must fail closed before aggregate can hide a bad ts"),
    ] {
        assert!(
            format!("{error:#}").contains("observed_swaps.ts is not canonical UTC"),
            "unexpected error: {error:#}"
        );
    }
    Ok(())
}

#[test]
fn insert_observed_swaps_batch_retries_after_write_lock() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-swap-batch-retry.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let blocker_store = SqliteStore::open(Path::new(&db_path))?;
    blocker_store
        .conn
        .busy_timeout(StdDuration::from_millis(1))
        .context("failed to shorten blocker busy timeout")?;
    blocker_store
        .conn
        .execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let worker_db_path = db_path.clone();
    let worker_barrier = barrier.clone();
    let handle = std::thread::spawn(move || -> Result<()> {
        let worker_store = SqliteStore::open(Path::new(&worker_db_path))?;
        worker_store
            .conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten worker busy timeout")?;
        worker_barrier.wait();
        let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let inserted = worker_store.insert_observed_swaps_batch(&[
            SwapEvent {
                wallet: "wallet-retry".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-retry-a".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-observed-swap-batch-retry-a".to_string(),
                slot: 999,
                ts_utc: now,
                exact_amounts: None,
            },
            SwapEvent {
                wallet: "wallet-retry".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-retry-b".to_string(),
                amount_in: 2.0,
                amount_out: 20.0,
                signature: "sig-observed-swap-batch-retry-b".to_string(),
                slot: 1000,
                ts_utc: now + Duration::seconds(1),
                exact_amounts: None,
            },
        ])?;
        assert_eq!(inserted, vec![true, true]);
        Ok(())
    });

    barrier.wait();
    std::thread::sleep(StdDuration::from_millis(250));
    blocker_store.conn.execute_batch("COMMIT")?;
    handle
        .join()
        .expect("worker thread panicked")
        .context("worker batch insert should succeed after retry")?;

    let verify_store = SqliteStore::open(Path::new(&db_path))?;
    let swaps = verify_store.load_observed_swaps_since(
        DateTime::parse_from_rfc3339("2026-03-06T11:59:00Z")
            .expect("timestamp")
            .with_timezone(&Utc),
    )?;
    assert_eq!(swaps.len(), 2);
    Ok(())
}

#[test]
fn delete_observed_swaps_before_applies_time_retention_cutoff() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-swap-retention.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let recent_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    store.insert_observed_swap(&SwapEvent {
        wallet: "wallet-retention".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "token-stale".to_string(),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: "sig-observed-swap-stale".to_string(),
        slot: 1,
        ts_utc: stale_ts,
        exact_amounts: None,
    })?;
    store.insert_observed_swap(&SwapEvent {
        wallet: "wallet-retention".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "token-recent".to_string(),
        amount_in: 2.0,
        amount_out: 20.0,
        signature: "sig-observed-swap-recent".to_string(),
        slot: 2,
        ts_utc: recent_ts,
        exact_amounts: None,
    })?;

    let deleted = store.delete_observed_swaps_before(recent_ts - Duration::days(1))?;
    assert_eq!(deleted, 1);

    let swaps = store.load_observed_swaps_since(stale_ts - Duration::seconds(1))?;
    assert_eq!(swaps.len(), 1);
    assert_eq!(swaps[0].signature, "sig-observed-swap-recent");
    Ok(())
}

#[test]
fn delete_observed_swaps_before_rejects_hidden_noncanonical_timestamp() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-swap-retention-bad-ts.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;
    let cutoff = DateTime::parse_from_rfc3339("2026-03-06T12:00:00+00:00")
        .expect("timestamp")
        .with_timezone(&Utc);

    store.conn.execute(
        "INSERT INTO observed_swaps(signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts)
         VALUES ('sig-hidden-bad-ts', 'wallet-bad-ts', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 1, '2026-03-01T12:00:00Z')",
        [],
    )?;
    let error = store
        .delete_observed_swaps_before(cutoff)
        .expect_err("retention must fail closed before deleting hidden bad observed evidence");
    assert!(format!("{error:#}").contains("observed_swaps.ts is not canonical UTC"));
    let remaining: i64 =
        store
            .conn
            .query_row("SELECT COUNT(*) FROM observed_swaps", [], |row| row.get(0))?;
    assert_eq!(remaining, 1);
    Ok(())
}

#[test]
fn delete_observed_swaps_before_batched_chunks_retention_work() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-swap-retention-batched.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let stale_one = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let stale_two = stale_one + Duration::minutes(1);
    let stale_three = stale_one + Duration::minutes(2);
    let recent_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    for (signature, ts, slot) in [
        ("sig-observed-swap-stale-a", stale_one, 1),
        ("sig-observed-swap-stale-b", stale_two, 2),
        ("sig-observed-swap-stale-c", stale_three, 3),
        ("sig-observed-swap-recent", recent_ts, 4),
    ] {
        store.insert_observed_swap(&SwapEvent {
            wallet: "wallet-retention".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("token-{signature}"),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot,
            ts_utc: ts,
            exact_amounts: None,
        })?;
    }

    let summary = store.delete_observed_swaps_before_batched(recent_ts - Duration::days(1), 1)?;
    assert_eq!(summary.deleted_rows, 3);
    assert_eq!(summary.batches, 3);

    let swaps = store.load_observed_swaps_since(stale_one - Duration::seconds(1))?;
    assert_eq!(swaps.len(), 1);
    assert_eq!(swaps[0].signature, "sig-observed-swap-recent");
    Ok(())
}
