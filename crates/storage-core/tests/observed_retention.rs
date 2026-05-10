use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use tempfile::tempdir;

fn ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

fn insert_observed_row(
    conn: &rusqlite::Connection,
    signature: &str,
    timestamp: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO observed_swaps(
            signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
         ) VALUES (?1, 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 42, ?2)",
        (signature, timestamp),
    )?;
    Ok(())
}

fn observed_row_count(conn: &rusqlite::Connection) -> Result<i64> {
    Ok(conn.query_row("SELECT COUNT(*) FROM observed_swaps", [], |row| row.get(0))?)
}

#[test]
fn observed_retention_delete_rejects_hidden_noncanonical_timestamps() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    store.ensure_observed_swap_writer_tables()?;
    drop(store);

    let conn = rusqlite::Connection::open(&db_path)?;
    insert_observed_row(&conn, "sig-good-old", "2026-05-05T08:00:00+00:00")?;
    insert_observed_row(&conn, "sig-hidden-bad", "2026-05-05T08:30:00Z")?;
    insert_observed_row(&conn, "sig-good-new", "2026-05-05T09:00:00+00:00")?;
    assert_eq!(observed_row_count(&conn)?, 3);
    drop(conn);

    let store = SqliteDiscoveryStore::open(&db_path)?;
    let err = store
        .delete_observed_swaps_before_batch(ts("2026-05-05T10:00:00Z")?, 1)
        .expect_err("retention must fail closed before raw timestamp delete can hide bad rows");
    let message = format!("{err:#}");
    assert!(message.contains("not canonical UTC"));
    drop(store);

    let conn = rusqlite::Connection::open(&db_path)?;
    assert_eq!(observed_row_count(&conn)?, 3);
    Ok(())
}

#[test]
fn recent_raw_prune_rejects_hidden_noncanonical_timestamps() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    store.ensure_recent_raw_journal_tables()?;
    drop(store);

    let conn = rusqlite::Connection::open(&db_path)?;
    insert_observed_row(&conn, "sig-good-old", "2026-05-05T08:00:00+00:00")?;
    insert_observed_row(&conn, "sig-hidden-bad", "2026-02-31T08:30:00+00:00")?;
    insert_observed_row(&conn, "sig-good-new", "2026-05-05T09:00:00+00:00")?;
    assert_eq!(observed_row_count(&conn)?, 3);
    drop(conn);

    let store = SqliteDiscoveryStore::open(&db_path)?;
    let err = store
        .prune_recent_raw_journal_before_batch(
            ts("2026-05-05T10:00:00Z")?,
            1,
            ts("2026-05-05T10:05:00Z")?,
        )
        .expect_err("recent_raw prune must fail closed before raw timestamp delete");
    let message = format!("{err:#}");
    assert!(message.contains("not canonical UTC"));
    drop(store);

    let conn = rusqlite::Connection::open(&db_path)?;
    assert_eq!(observed_row_count(&conn)?, 3);
    Ok(())
}
