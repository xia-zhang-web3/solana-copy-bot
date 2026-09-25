use anyhow::Result;
use copybot_storage_core::SqliteStore;
use rusqlite::Connection;

#[test]
fn committed_foreign_write_invalidates_read_cache_epoch() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("epoch.db");
    let reader = SqliteStore::open(&path)?;
    let writer = Connection::open(&path)?;
    let before = reader.sqlite_data_version()?;
    writer.execute_batch("CREATE TABLE changed (value INTEGER NOT NULL);")?;
    assert_ne!(reader.sqlite_data_version()?, before);
    let after_schema = reader.sqlite_data_version()?;
    writer.execute("INSERT INTO changed VALUES (1)", [])?;
    assert_ne!(reader.sqlite_data_version()?, after_schema);
    Ok(())
}

#[test]
fn wal_commit_becomes_visible_only_after_reader_transaction_ends() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("pinned.db");
    let reader = Connection::open(&path)?;
    reader.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE changed (value INTEGER NOT NULL);")?;
    let writer = Connection::open(&path)?;
    let before: i64 = reader.pragma_query_value(None, "data_version", |r| r.get(0))?;
    reader.execute_batch("BEGIN;")?;
    let _: i64 = reader.query_row("SELECT count(*) FROM changed", [], |r| r.get(0))?;
    writer.execute("INSERT INTO changed VALUES (1)", [])?;
    let pinned: i64 = reader.pragma_query_value(None, "data_version", |r| r.get(0))?;
    assert_eq!(pinned, before);
    reader.execute_batch("COMMIT;")?;
    let after: i64 = reader.pragma_query_value(None, "data_version", |r| r.get(0))?;
    assert_ne!(after, before);
    Ok(())
}
