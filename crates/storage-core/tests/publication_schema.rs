use anyhow::Result;
use copybot_storage_core::{
    ensure_discovery_v2_schema, validate_discovery_v2_schema_read_only, SqliteDiscoveryStore,
};
use tempfile::tempdir;

#[test]
fn schema_validation_requires_single_active_followlist_index() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    rusqlite::Connection::open(&db_path)?
        .execute("DROP INDEX IF EXISTS idx_followlist_one_active_wallet", [])?;
    let read_only_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = validate_discovery_v2_schema_read_only(&read_only_store)
        .expect_err("missing followlist active uniqueness index must fail schema validation");

    assert_malformed_followlist_index(&err);
    drop(read_only_store);

    let writable = rusqlite::Connection::open(&db_path)?;
    writable.execute(
        "CREATE INDEX idx_followlist_one_active_wallet ON followlist(wallet_id)",
        [],
    )?;
    drop(writable);
    let read_only_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = validate_discovery_v2_schema_read_only(&read_only_store)
        .expect_err("malformed followlist active uniqueness index must fail schema validation");

    assert_malformed_followlist_index(&err);
    drop(read_only_store);

    let writable = rusqlite::Connection::open(&db_path)?;
    writable.execute("DROP INDEX IF EXISTS idx_followlist_one_active_wallet", [])?;
    writable.execute(
        "CREATE UNIQUE INDEX idx_followlist_one_active_wallet ON followlist(wallet_id) WHERE active = 10",
        [],
    )?;
    drop(writable);
    let read_only_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = validate_discovery_v2_schema_read_only(&read_only_store)
        .expect_err("wrong followlist active uniqueness predicate must fail schema validation");

    assert_malformed_followlist_index(&err);
    Ok(())
}

fn assert_malformed_followlist_index(error: &anyhow::Error) {
    assert!(error
        .to_string()
        .contains("missing or malformed required index: idx_followlist_one_active_wallet"));
}
