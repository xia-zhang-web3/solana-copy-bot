use super::*;

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
