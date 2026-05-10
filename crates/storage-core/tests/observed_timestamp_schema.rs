use anyhow::Result;
use copybot_storage_core::{
    ensure_discovery_v2_schema, validate_discovery_v2_schema_read_only,
    validate_discovery_v2_status_schema_read_only, SqliteDiscoveryStore,
};
use tempfile::tempdir;

fn assert_malformed_observed_timestamp_index(error: &anyhow::Error) {
    assert!(error
        .to_string()
        .contains("missing or malformed required index: idx_observed_swaps_non_utc_ts"));
}

#[test]
fn schema_validation_requires_observed_swaps_non_utc_timestamp_index() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let writable = rusqlite::Connection::open(&db_path)?;
    writable.execute("DROP INDEX IF EXISTS idx_observed_swaps_non_utc_ts", [])?;
    drop(writable);
    let read_only_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = validate_discovery_v2_schema_read_only(&read_only_store)
        .expect_err("missing observed timestamp validation index must fail schema validation");
    assert_malformed_observed_timestamp_index(&err);
    drop(read_only_store);

    let writable = rusqlite::Connection::open(&db_path)?;
    writable.execute(
        "CREATE INDEX idx_observed_swaps_non_utc_ts ON observed_swaps(ts)",
        [],
    )?;
    drop(writable);
    let read_only_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = validate_discovery_v2_schema_read_only(&read_only_store)
        .expect_err("non-partial observed timestamp validation index must fail schema validation");
    assert_malformed_observed_timestamp_index(&err);
    Ok(())
}

#[test]
fn timestamp_read_guard_requires_valid_observed_timestamp_index() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let writable = rusqlite::Connection::open(&db_path)?;
    writable.execute("DROP INDEX IF EXISTS idx_observed_swaps_non_utc_ts", [])?;
    writable.execute(
        "CREATE INDEX idx_observed_swaps_non_utc_ts ON observed_swaps(ts) WHERE ts LIKE '%+00:00'",
        [],
    )?;
    drop(writable);

    let read_only_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = read_only_store
        .observed_swaps_tail_cursor_read_only()
        .expect_err("malformed timestamp validation index must fail before observed read");
    assert!(err.to_string().contains("missing or malformed"));
    Ok(())
}

#[test]
fn status_schema_validation_does_not_require_runtime_cursor_table() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let writable = rusqlite::Connection::open(&db_path)?;
    writable.execute("DROP TABLE discovery_runtime_state", [])?;
    drop(writable);

    let read_only_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    validate_discovery_v2_status_schema_read_only(&read_only_store)
        .expect("status/publish dry-run schema validation must not require runtime cursor table");
    let err = validate_discovery_v2_schema_read_only(&read_only_store)
        .expect_err("full publication/export schema validation must require runtime cursor table");
    assert!(err
        .to_string()
        .contains("missing required table: discovery_runtime_state"));
    Ok(())
}

#[test]
fn writer_helpers_prepare_timestamp_index_only_for_empty_observed_swaps() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    store.ensure_recent_raw_journal_tables()?;
    drop(store);

    let conn = rusqlite::Connection::open(&db_path)?;
    let valid_index_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM pragma_index_list('observed_swaps')
         WHERE name = 'idx_observed_swaps_non_utc_ts' AND partial = 1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(valid_index_count, 1);
    conn.execute("DROP INDEX IF EXISTS idx_observed_swaps_non_utc_ts", [])?;
    conn.execute(
        "INSERT INTO observed_swaps(
            signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
         ) VALUES ('sig-a', 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 1, '2026-05-05T10:00:00+00:00')",
        [],
    )?;
    drop(conn);

    let store = SqliteDiscoveryStore::open(&db_path)?;
    let err = store.ensure_recent_raw_journal_tables().expect_err(
        "non-empty DB missing timestamp validation index must not rebuild on live ensure",
    );
    assert!(err.to_string().contains("non-empty table"));
    Ok(())
}

#[test]
fn discovery_v2_schema_ensure_does_not_rebuild_timestamp_index_on_non_empty_observed_swaps(
) -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute("DROP INDEX IF EXISTS idx_observed_swaps_non_utc_ts", [])?;
    conn.execute(
        "INSERT INTO observed_swaps(
            signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
         ) VALUES ('sig-non-empty', 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 1, '2026-05-05T10:00:00+00:00')",
        [],
    )?;
    drop(conn);

    let store = SqliteDiscoveryStore::open(&db_path)?;
    let err = ensure_discovery_v2_schema(&store)
        .expect_err("live schema ensure must not rebuild timestamp index on non-empty DB");
    assert!(err.to_string().contains("non-empty table"));
    Ok(())
}

#[test]
fn schema_validation_rejects_desc_observed_timestamp_index() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let writable = rusqlite::Connection::open(&db_path)?;
    let index_sql = writable.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_observed_swaps_non_utc_ts'",
        [],
        |row| row.get::<_, String>(0),
    )?;
    let desc_index_sql = index_sql.replace("ON observed_swaps(ts)", "ON observed_swaps(ts DESC)");
    writable.execute("DROP INDEX IF EXISTS idx_observed_swaps_non_utc_ts", [])?;
    writable.execute_batch(&desc_index_sql)?;
    drop(writable);

    let read_only_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = validate_discovery_v2_schema_read_only(&read_only_store)
        .expect_err("DESC timestamp validation index must fail schema validation");
    assert_malformed_observed_timestamp_index(&err);
    Ok(())
}
