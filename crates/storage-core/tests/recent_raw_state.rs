use anyhow::Result;
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use rusqlite::Connection;
use tempfile::tempdir;

#[test]
fn recent_raw_state_rejects_z_cursor_timestamp() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    store.ensure_recent_raw_journal_tables()?;
    drop(store);

    let conn = Connection::open(&db_path)?;
    conn.execute(
        "INSERT INTO recent_raw_journal_state(
            id, covered_through_cursor_ts, covered_through_cursor_slot,
            covered_through_cursor_signature, updated_at
         ) VALUES (1, '2026-05-05T10:00:00Z', 42, 'sig-z', '2026-05-05T10:00:00+00:00')",
        [],
    )?;
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = store
        .recent_raw_journal_state_read_only()
        .expect_err("recent_raw cursor timestamps must use canonical +00:00");
    assert!(err.to_string().contains("canonical UTC offset"));
    Ok(())
}

#[test]
fn recent_raw_state_rejects_negative_cached_counters() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    store.ensure_recent_raw_journal_tables()?;
    drop(store);

    let conn = Connection::open(&db_path)?;
    conn.execute(
        "INSERT INTO recent_raw_journal_state(
            id, row_count, last_batch_rows, last_pruned_rows, updated_at
         ) VALUES (1, -1, 0, 0, '2026-05-05T10:00:00+00:00')",
        [],
    )?;
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = store
        .recent_raw_journal_state_cached_read_only_required()
        .expect_err("negative cached row_count must fail closed");
    assert!(err.to_string().contains("row_count must be non-negative"));
    Ok(())
}
