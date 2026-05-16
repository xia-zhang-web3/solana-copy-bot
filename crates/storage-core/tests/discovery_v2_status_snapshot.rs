use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    ensure_discovery_v2_schema, sqlite_contention_snapshot, DiscoveryRuntimeCursor,
    SqliteDiscoveryStore,
};
use rusqlite::Connection;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn discovery_v2_status_snapshot_round_trips_metadata() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    let now = DateTime::parse_from_rfc3339("2026-05-13T12:00:00+00:00")?.with_timezone(&Utc);
    let window_start =
        DateTime::parse_from_rfc3339("2026-05-12T12:00:00+00:00")?.with_timezone(&Utc);
    let cursor = DiscoveryRuntimeCursor {
        ts_utc: now,
        slot: 42,
        signature: "sig-42".to_string(),
    };

    store.persist_discovery_v2_status_snapshot(
        "policy-a",
        now,
        window_start,
        Some(&cursor),
        r#"{"source":"discovery_v2_operational_window"}"#,
    )?;
    let read_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let row = read_store
        .discovery_v2_status_snapshot_read_only()?
        .expect("snapshot row");

    assert_eq!(row.policy_fingerprint, "policy-a");
    assert_eq!(row.status_now, now);
    assert_eq!(row.status_window_start, window_start);
    assert_eq!(row.runtime_cursor, Some(cursor));
    assert!(row.status_json.contains("discovery_v2_operational_window"));
    Ok(())
}

#[test]
fn discovery_v2_status_snapshot_rejects_partial_cursor_columns() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);
    let conn = Connection::open(&db_path)?;
    conn.execute(
        "INSERT INTO discovery_v2_status_snapshot(
            id, policy_fingerprint, status_now, status_window_start,
            runtime_cursor_ts, runtime_cursor_slot, runtime_cursor_signature,
            status_json, updated_at
         ) VALUES (1, 'policy-a', '2026-05-13T12:00:00+00:00',
            '2026-05-12T12:00:00+00:00', '2026-05-13T12:00:00+00:00',
            NULL, 'sig-42', '{}', '2026-05-13T12:00:00+00:00')",
        [],
    )?;
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = store
        .discovery_v2_status_snapshot_read_only()
        .expect_err("partial cursor must fail closed");
    assert!(err.to_string().contains("cursor columns"));
    Ok(())
}

#[test]
fn discovery_v2_status_snapshot_persist_retries_retryable_sqlite_lock() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let blocker = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&blocker)?;
    blocker.set_busy_timeout(Duration::from_millis(10))?;
    blocker.begin_discovery_v2_quality_prepare_update()?;

    let before = sqlite_contention_snapshot();
    let contender_path = db_path.clone();
    let contender = thread::spawn(move || -> Result<()> {
        let store = SqliteDiscoveryStore::open(&contender_path)?;
        store.set_busy_timeout(Duration::from_millis(10))?;
        let now = DateTime::parse_from_rfc3339("2026-05-13T12:00:00+00:00")?.with_timezone(&Utc);
        let window_start =
            DateTime::parse_from_rfc3339("2026-05-12T12:00:00+00:00")?.with_timezone(&Utc);
        store.persist_discovery_v2_status_snapshot("policy-a", now, window_start, None, "{}")?;
        Ok(())
    });

    thread::sleep(Duration::from_millis(75));
    blocker.rollback_discovery_v2_quality_prepare_update();
    contender.join().expect("contender thread panicked")?;

    let after = sqlite_contention_snapshot();
    assert!(
        after.busy_error_total > before.busy_error_total,
        "expected retryable sqlite busy errors to be counted: before={before:?} after={after:?}"
    );
    assert!(
        after.write_retry_total > before.write_retry_total,
        "expected sqlite write retries to be counted: before={before:?} after={after:?}"
    );
    Ok(())
}
