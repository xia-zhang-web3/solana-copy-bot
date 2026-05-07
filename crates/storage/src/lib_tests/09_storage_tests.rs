use super::*;

#[test]
fn observed_sol_leg_swap_cursor_query_filters_and_resumes_in_order() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-sol-leg-page-query.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let base = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    for swap in [
        SwapEvent {
            signature: "noise-aa".to_string(),
            wallet: "wallet-noise".to_string(),
            dex: "raydium".to_string(),
            token_in: "token-a".to_string(),
            token_out: "token-b".to_string(),
            amount_in: 1.0,
            amount_out: 2.0,
            slot: 9,
            ts_utc: base,
            exact_amounts: None,
        },
        SwapEvent {
            signature: "buy-1".to_string(),
            wallet: "wallet-a".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-c".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            slot: 10,
            ts_utc: base + Duration::seconds(1),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "sell-1".to_string(),
            wallet: "wallet-a".to_string(),
            dex: "raydium".to_string(),
            token_in: "token-c".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 5.0,
            amount_out: 0.6,
            slot: 11,
            ts_utc: base + Duration::seconds(2),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "noise-bb".to_string(),
            wallet: "wallet-noise".to_string(),
            dex: "raydium".to_string(),
            token_in: "token-x".to_string(),
            token_out: "token-y".to_string(),
            amount_in: 3.0,
            amount_out: 4.0,
            slot: 12,
            ts_utc: base + Duration::seconds(3),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "buy-2".to_string(),
            wallet: "wallet-b".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-d".to_string(),
            amount_in: 0.8,
            amount_out: 8.0,
            slot: 13,
            ts_utc: base + Duration::seconds(4),
            exact_amounts: None,
        },
    ] {
        assert!(store.insert_observed_swap(&swap)?);
    }

    let mut first_page = Vec::new();
    let first = store.for_each_observed_sol_leg_swap_in_window_after_cursor_with_budget(
        base,
        base + Duration::seconds(10),
        None,
        2,
        std::time::Instant::now() + StdDuration::from_secs(1),
        |swap| {
            first_page.push(swap.signature);
            Ok(())
        },
    )?;
    assert_eq!(first.rows_seen, 2);
    assert!(!first.time_budget_exhausted);
    assert_eq!(
        first.access_path,
        ObservedSolLegCursorAccessPath::SolLegPartialIndex
    );
    assert_eq!(first_page, vec!["buy-1".to_string(), "sell-1".to_string()]);

    let cursor = DiscoveryRuntimeCursor {
        ts_utc: base + Duration::seconds(2),
        slot: 11,
        signature: "sell-1".to_string(),
    };
    let mut second_page = Vec::new();
    let second = store.for_each_observed_sol_leg_swap_in_window_after_cursor_with_budget(
        base,
        base + Duration::seconds(10),
        Some(&cursor),
        2,
        std::time::Instant::now() + StdDuration::from_secs(1),
        |swap| {
            second_page.push(swap.signature);
            Ok(())
        },
    )?;
    assert_eq!(second.rows_seen, 1);
    assert!(!second.time_budget_exhausted);
    assert_eq!(
        second.access_path,
        ObservedSolLegCursorAccessPath::SolLegPartialIndex
    );
    assert_eq!(second_page, vec!["buy-2".to_string()]);
    Ok(())
}

#[test]
fn observed_sol_leg_swap_cursor_query_for_target_buy_mints_skips_non_target_tail_stage1(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("observed-sol-leg-target-mint-filter-query.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let base = DateTime::parse_from_rfc3339("2026-04-10T20:04:38Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    for swap in [
        SwapEvent {
            signature: "target-buy-1".to_string(),
            wallet: "wallet-target".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-target".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            slot: 10,
            ts_utc: base + Duration::seconds(1),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "target-sell-1".to_string(),
            wallet: "wallet-target".to_string(),
            dex: "raydium".to_string(),
            token_in: "token-target".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 10.0,
            amount_out: 1.2,
            slot: 11,
            ts_utc: base + Duration::seconds(2),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "noise-buy-1".to_string(),
            wallet: "wallet-noise".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-noise-a".to_string(),
            amount_in: 0.5,
            amount_out: 5.0,
            slot: 12,
            ts_utc: base + Duration::seconds(3),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "noise-sell-1".to_string(),
            wallet: "wallet-noise".to_string(),
            dex: "raydium".to_string(),
            token_in: "token-noise-a".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 5.0,
            amount_out: 0.4,
            slot: 13,
            ts_utc: base + Duration::seconds(4),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "noise-buy-2".to_string(),
            wallet: "wallet-noise".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-noise-b".to_string(),
            amount_in: 0.7,
            amount_out: 7.0,
            slot: 14,
            ts_utc: base + Duration::seconds(5),
            exact_amounts: None,
        },
    ] {
        assert!(store.insert_observed_swap(&swap)?);
    }

    let cursor = DiscoveryRuntimeCursor {
        ts_utc: base + Duration::seconds(2),
        slot: 11,
        signature: "target-sell-1".to_string(),
    };

    let mut broad_tail = Vec::new();
    let broad = store.for_each_observed_sol_leg_swap_in_window_after_cursor_with_budget(
        base,
        base + Duration::seconds(10),
        Some(&cursor),
        10,
        std::time::Instant::now() + StdDuration::from_secs(1),
        |swap| {
            broad_tail.push(swap.signature);
            Ok(())
        },
    )?;
    assert_eq!(broad.rows_seen, 3);
    assert_eq!(
        broad_tail,
        vec![
            "noise-buy-1".to_string(),
            "noise-sell-1".to_string(),
            "noise-buy-2".to_string()
        ]
    );

    let mut filtered_tail = Vec::new();
    let filtered = store
        .for_each_observed_sol_leg_swap_in_window_after_cursor_for_target_buy_mints_with_budget(
            base,
            base + Duration::seconds(10),
            Some(&cursor),
            &["token-target".to_string()],
            10,
            std::time::Instant::now() + StdDuration::from_secs(1),
            |swap| {
                filtered_tail.push(swap.signature);
                Ok(())
            },
        )?;
    assert_eq!(filtered.rows_seen, 0);
    assert!(filtered_tail.is_empty());
    assert_eq!(
        filtered.access_path,
        ObservedSolLegCursorAccessPath::SolLegPartialIndex
    );
    Ok(())
}

#[test]
fn observed_sol_leg_swap_cursor_query_works_before_and_after_deferred_index_migration() -> Result<()>
{
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-sol-leg-deferred-index-query.db");
    let legacy_migrations = temp.path().join("legacy-migrations");
    copy_migrations_through(&legacy_migrations, "0038_alert_delivery_cursor.sql")?;

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&legacy_migrations)?;

    let base = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    for swap in [
        SwapEvent {
            signature: "noise-aa".to_string(),
            wallet: "wallet-noise".to_string(),
            dex: "raydium".to_string(),
            token_in: "token-a".to_string(),
            token_out: "token-b".to_string(),
            amount_in: 1.0,
            amount_out: 2.0,
            slot: 9,
            ts_utc: base,
            exact_amounts: None,
        },
        SwapEvent {
            signature: "buy-1".to_string(),
            wallet: "wallet-a".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-c".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            slot: 10,
            ts_utc: base + Duration::seconds(1),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "sell-1".to_string(),
            wallet: "wallet-a".to_string(),
            dex: "raydium".to_string(),
            token_in: "token-c".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 5.0,
            amount_out: 0.6,
            slot: 11,
            ts_utc: base + Duration::seconds(2),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "buy-2".to_string(),
            wallet: "wallet-b".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-d".to_string(),
            amount_in: 0.8,
            amount_out: 8.0,
            slot: 13,
            ts_utc: base + Duration::seconds(4),
            exact_amounts: None,
        },
    ] {
        assert!(store.insert_observed_swap(&swap)?);
    }

    let mut before_index = Vec::new();
    let fallback = store.for_each_observed_sol_leg_swap_in_window_after_cursor_with_budget(
        base,
        base + Duration::seconds(10),
        None,
        10,
        std::time::Instant::now() + StdDuration::from_secs(1),
        |swap| {
            before_index.push(swap.signature);
            Ok(())
        },
    )?;
    assert_eq!(
        fallback.access_path,
        ObservedSolLegCursorAccessPath::TsCursorFallback
    );
    assert_eq!(
        before_index,
        vec![
            "buy-1".to_string(),
            "sell-1".to_string(),
            "buy-2".to_string()
        ]
    );

    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let mut after_index = Vec::new();
    let optimized = store.for_each_observed_sol_leg_swap_in_window_after_cursor_with_budget(
        base,
        base + Duration::seconds(10),
        None,
        10,
        std::time::Instant::now() + StdDuration::from_secs(1),
        |swap| {
            after_index.push(swap.signature);
            Ok(())
        },
    )?;
    assert_eq!(
        optimized.access_path,
        ObservedSolLegCursorAccessPath::SolLegPartialIndex
    );
    assert_eq!(after_index, before_index);
    Ok(())
}

#[test]
fn insert_observed_swap_retries_after_write_lock() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-swap-retry.db");
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
        let inserted = worker_store.insert_observed_swap(&SwapEvent {
            wallet: "wallet-retry".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-retry".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-retry".to_string(),
            slot: 999,
            ts_utc: now,
            exact_amounts: None,
        })?;
        assert!(
            inserted,
            "expected observed swap insert to succeed after retry"
        );
        Ok(())
    });

    barrier.wait();
    std::thread::sleep(StdDuration::from_millis(250));
    blocker_store.conn.execute_batch("COMMIT")?;
    handle
        .join()
        .expect("worker thread panicked")
        .context("worker insert should succeed after retry")?;

    let verify_store = SqliteStore::open(Path::new(&db_path))?;
    let swaps = verify_store.load_observed_swaps_since(
        DateTime::parse_from_rfc3339("2026-03-06T11:59:00Z")
            .expect("timestamp")
            .with_timezone(&Utc),
    )?;
    assert_eq!(swaps.len(), 1);
    assert_eq!(swaps[0].signature, "sig-observed-swap-retry");
    Ok(())
}

#[test]
fn snapshot_into_path_with_policy_returns_retryable_busy_after_bounded_destination_lock(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let source_path = temp.path().join("snapshot-source.db");
    let destination_path = temp.path().join("snapshot-destination.db");

    let source_store = SqliteStore::open(Path::new(&source_path))?;
    source_store
        .conn
        .execute_batch("CREATE TABLE snapshot_source(id INTEGER PRIMARY KEY, value TEXT);")
        .context("failed creating snapshot source table")?;
    source_store
        .conn
        .execute(
            "INSERT INTO snapshot_source(id, value) VALUES (1, 'value')",
            [],
        )
        .context("failed seeding snapshot source table")?;

    let blocker = SqliteStore::open(Path::new(&destination_path))?;
    blocker
        .conn
        .busy_timeout(StdDuration::from_millis(1))
        .context("failed to shorten destination blocker busy timeout")?;
    blocker.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

    let outcome = source_store.snapshot_into_path_with_policy(
        &destination_path,
        &SqliteSnapshotPolicy {
            busy_timeout: StdDuration::from_millis(1),
            pages_per_step: 1,
            pause_between_steps: StdDuration::from_millis(1),
            retry_backoff_ms: vec![1, 1],
            max_attempt_duration: Some(StdDuration::from_millis(1)),
            pin_source_snapshot: true,
        },
    )?;
    blocker.conn.execute_batch("ROLLBACK")?;

    let SqliteSnapshotOutcome::RetryableBusy(summary) = outcome else {
        anyhow::bail!("expected retryable busy snapshot outcome");
    };
    assert_eq!(summary.backup_retry_count, 2);
    assert!(
        summary.busy_retry_count + summary.locked_retry_count >= 2,
        "expected bounded retry counters to record contention"
    );
    assert!(
        summary.retry_exhausted_reason.is_some(),
        "retryable busy snapshot outcome must expose exhausted reason"
    );
    Ok(())
}

#[test]
fn snapshot_into_path_with_policy_returns_deferred_after_bounded_attempt_budget() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let source_path = temp.path().join("snapshot-source-large.db");
    let destination_path = temp.path().join("snapshot-destination-large.db");

    let source_store = SqliteStore::open(Path::new(&source_path))?;
    source_store
        .conn
        .execute_batch("CREATE TABLE snapshot_source(id INTEGER PRIMARY KEY, value TEXT);")
        .context("failed creating snapshot source table")?;
    let large_value = "x".repeat(2048);
    for idx in 0..256 {
        source_store
            .conn
            .execute(
                "INSERT INTO snapshot_source(id, value) VALUES (?1, ?2)",
                params![idx, large_value],
            )
            .context("failed seeding large snapshot source table")?;
    }
    let source_metrics = source_store.snapshot_source_metrics()?;
    assert!(
        source_metrics.page_count > 1,
        "large snapshot source must span multiple pages for duration budget test"
    );

    let outcome = source_store.snapshot_into_path_with_policy(
        &destination_path,
        &SqliteSnapshotPolicy {
            busy_timeout: StdDuration::from_millis(1),
            pages_per_step: 1,
            pause_between_steps: StdDuration::from_millis(0),
            retry_backoff_ms: vec![1, 1],
            max_attempt_duration: Some(StdDuration::ZERO),
            pin_source_snapshot: true,
        },
    )?;

    let SqliteSnapshotOutcome::Deferred(summary) = outcome else {
        anyhow::bail!("expected deferred snapshot outcome");
    };
    assert_eq!(
        summary.deferred_reason,
        Some(SqliteSnapshotDeferredReason::AttemptDurationBudgetExceeded)
    );
    assert!(
        summary.backup_step_count >= 1,
        "deferred outcome must report at least one attempted backup step"
    );
    assert!(
        summary.total_page_count >= source_metrics.page_count,
        "deferred outcome must report total page count progress"
    );
    assert!(
        summary.remaining_page_count > 0,
        "deferred outcome must preserve unfinished page count"
    );
    assert!(
        summary.copied_page_count < summary.total_page_count,
        "deferred outcome must not claim full completion"
    );
    Ok(())
}
