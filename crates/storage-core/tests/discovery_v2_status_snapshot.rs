use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    ensure_discovery_v2_schema, DiscoveryRuntimeCursor, SqliteDiscoveryStore,
};
use rusqlite::Connection;
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
