use super::*;

#[test]
fn record_heartbeat_retries_after_write_lock() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("heartbeat-retry.db");
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
        worker_store.record_heartbeat("copybot-app", "alive")?;
        Ok(())
    });

    barrier.wait();
    std::thread::sleep(StdDuration::from_millis(250));
    blocker_store.conn.execute_batch("COMMIT")?;
    handle
        .join()
        .expect("worker thread panicked")
        .context("worker heartbeat should succeed after retry")?;

    let verify_store = SqliteStore::open(Path::new(&db_path))?;
    let count: i64 = verify_store.conn.query_row(
        "SELECT COUNT(*) FROM system_heartbeat WHERE component = ?1 AND status = ?2",
        params!["copybot-app", "alive"],
        |row| row.get(0),
    )?;
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn observed_startup_step_reports_started_and_completed() -> Result<()> {
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let reporter_events = events.clone();
    let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
        reporter_events
            .lock()
            .expect("startup reporter mutex poisoned")
            .push(event);
    });

    let value = run_observed_startup_step(
        "test_startup_step_complete",
        StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_millis(250)),
        ),
        Some(&reporter),
        || Ok::<usize, anyhow::Error>(7),
    )?;
    assert_eq!(value, 7);

    let recorded = events.lock().expect("startup reporter mutex poisoned");
    assert!(
        recorded.iter().any(|event| {
            event.stage == "test_startup_step_complete"
                && event.outcome == StartupStepOutcome::Started
        }),
        "startup step must emit a started event"
    );
    assert!(
        recorded.iter().any(|event| {
            event.stage == "test_startup_step_complete"
                && event.outcome == StartupStepOutcome::Completed
        }),
        "startup step must emit a completed event"
    );
    Ok(())
}
