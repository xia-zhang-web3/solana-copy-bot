use anyhow::Result;
use copybot_storage_core::{
    ensure_discovery_v2_schema, validate_discovery_v2_schema_read_only, SqliteDiscoveryStore,
    SqliteStartupPolicy,
};
use std::path::Path;
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

#[test]
fn schema_validation_requires_observed_swaps_read_indexes() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let writable = rusqlite::Connection::open(&db_path)?;
    writable.execute(
        "DROP INDEX IF EXISTS idx_observed_swaps_token_in_out_ts",
        [],
    )?;
    drop(writable);

    let read_only_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = validate_discovery_v2_schema_read_only(&read_only_store)
        .expect_err("missing observed_swaps token-pair read index must fail validation");
    assert!(err
        .to_string()
        .contains("missing or malformed required index: idx_observed_swaps_token_in_out_ts"));
    Ok(())
}

#[test]
fn schema_validation_requires_valid_sol_leg_partial_index() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let writable = rusqlite::Connection::open(&db_path)?;
    writable.execute(
        "DROP INDEX IF EXISTS idx_observed_swaps_sol_leg_ts_slot_signature",
        [],
    )?;
    writable.execute(
        "CREATE INDEX idx_observed_swaps_sol_leg_ts_slot_signature
         ON observed_swaps(ts, slot, signature)
         WHERE token_in = 'So11111111111111111111111111111111111111112'",
        [],
    )?;
    drop(writable);

    let read_only_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = validate_discovery_v2_schema_read_only(&read_only_store)
        .expect_err("wrong SOL-leg partial index predicate must fail validation");
    assert!(err.to_string().contains(
        "missing or malformed required index: idx_observed_swaps_sol_leg_ts_slot_signature"
    ));
    Ok(())
}

#[test]
fn schema_validation_rejects_lowercase_sol_leg_partial_index_predicate() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let writable = rusqlite::Connection::open(&db_path)?;
    writable.execute(
        "DROP INDEX IF EXISTS idx_observed_swaps_sol_leg_ts_slot_signature",
        [],
    )?;
    writable.execute(
        "CREATE INDEX idx_observed_swaps_sol_leg_ts_slot_signature
         ON observed_swaps(ts, slot, signature)
         WHERE token_in = 'so11111111111111111111111111111111111111112'
            OR token_out = 'so11111111111111111111111111111111111111112'",
        [],
    )?;
    drop(writable);

    let read_only_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = validate_discovery_v2_schema_read_only(&read_only_store)
        .expect_err("lowercase SOL mint predicate must fail validation");
    assert!(err.to_string().contains(
        "missing or malformed required index: idx_observed_swaps_sol_leg_ts_slot_signature"
    ));
    Ok(())
}

#[test]
fn schema_ensure_does_not_build_missing_read_indexes_on_non_empty_observed_swaps() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute(
        "DROP INDEX IF EXISTS idx_observed_swaps_token_out_in_ts",
        [],
    )?;
    conn.execute(
        "INSERT INTO observed_swaps(
            signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
         ) VALUES ('sig-read-index', 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 1, '2026-05-05T10:00:00+00:00')",
        [],
    )?;
    drop(conn);

    let store = SqliteDiscoveryStore::open(&db_path)?;
    let err = ensure_discovery_v2_schema(&store)
        .expect_err("live schema ensure must not build missing observed_swaps read indexes");
    assert!(err
        .to_string()
        .contains("required read indexes are missing or malformed"));
    Ok(())
}

#[test]
fn schema_ensure_repairs_malformed_read_index_on_empty_observed_swaps() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute(
        "DROP INDEX IF EXISTS idx_observed_swaps_token_in_out_ts",
        [],
    )?;
    conn.execute(
        "CREATE INDEX idx_observed_swaps_token_in_out_ts
         ON observed_swaps(token_in, ts)",
        [],
    )?;
    drop(conn);

    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let read_only_store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    validate_discovery_v2_schema_read_only(&read_only_store)?;
    Ok(())
}

#[test]
fn writer_helpers_do_not_build_missing_read_indexes_on_non_empty_observed_swaps() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    store.ensure_observed_swap_writer_tables()?;
    drop(store);

    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute(
        "DROP INDEX IF EXISTS idx_observed_swaps_ts_slot_signature",
        [],
    )?;
    conn.execute(
        "INSERT INTO observed_swaps(
            signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
         ) VALUES ('sig-writer-read-index', 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 1, '2026-05-05T10:00:00+00:00')",
        [],
    )?;
    drop(conn);

    let store = SqliteDiscoveryStore::open(&db_path)?;
    let err = store
        .ensure_observed_swap_writer_tables()
        .expect_err("live writer ensure must not build missing read indexes on non-empty DB");
    assert!(err
        .to_string()
        .contains("required read indexes are missing or malformed"));
    Ok(())
}

#[test]
fn startup_records_satisfied_observed_timestamp_index_without_rebuilding() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let migrations_dir = dir.path().join("migrations");
    std::fs::create_dir(&migrations_dir)?;
    std::fs::copy(
        repo_root().join("migrations/0040_observed_swaps_non_utc_ts_index.sql"),
        migrations_dir.join("0040_observed_swaps_non_utc_ts_index.sql"),
    )?;

    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let bootstrap = SqliteDiscoveryStore::open_and_migrate_for_startup(
        &db_path,
        &migrations_dir,
        &SqliteStartupPolicy::default(),
        None,
    )?;
    assert_eq!(bootstrap.applied_migrations, 0);
    assert!(bootstrap.deferred_migrations.is_empty());
    drop(bootstrap.store);

    let conn = rusqlite::Connection::open(&db_path)?;
    let recorded: String = conn.query_row(
        "SELECT version FROM schema_migrations WHERE version = '0040_observed_swaps_non_utc_ts_index.sql'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(recorded, "0040_observed_swaps_non_utc_ts_index.sql");
    Ok(())
}

#[test]
fn startup_defers_mixed_0008_state_instead_of_rebuilding_valid_observed_indexes() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let migrations_dir = dir.path().join("migrations");
    std::fs::create_dir(&migrations_dir)?;
    std::fs::copy(
        repo_root().join("migrations/0008_shadow_perf_indexes.sql"),
        migrations_dir.join("0008_shadow_perf_indexes.sql"),
    )?;

    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let bootstrap = SqliteDiscoveryStore::open_and_migrate_for_startup(
        &db_path,
        &migrations_dir,
        &SqliteStartupPolicy::default(),
        None,
    )?;
    assert_eq!(bootstrap.applied_migrations, 0);
    assert_eq!(
        bootstrap.deferred_migrations,
        vec!["0008_shadow_perf_indexes.sql".to_string()]
    );
    Ok(())
}

#[test]
fn startup_defers_malformed_sol_leg_index_instead_of_recording_satisfied() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let migrations_dir = dir.path().join("migrations");
    std::fs::create_dir(&migrations_dir)?;
    std::fs::copy(
        repo_root().join("migrations/0039_observed_swaps_sol_leg_ts_index.sql"),
        migrations_dir.join("0039_observed_swaps_sol_leg_ts_index.sql"),
    )?;

    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute(
        "DROP INDEX IF EXISTS idx_observed_swaps_sol_leg_ts_slot_signature",
        [],
    )?;
    conn.execute(
        "CREATE INDEX idx_observed_swaps_sol_leg_ts_slot_signature
         ON observed_swaps(ts, slot, signature)
         WHERE token_in = 'So11111111111111111111111111111111111111112'",
        [],
    )?;
    conn.execute(
        "INSERT INTO observed_swaps(
            signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
         ) VALUES ('sig-defer-sol-leg', 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 1, '2026-05-05T10:00:00+00:00')",
        [],
    )?;
    drop(conn);

    let bootstrap = SqliteDiscoveryStore::open_and_migrate_for_startup(
        &db_path,
        &migrations_dir,
        &SqliteStartupPolicy::default(),
        None,
    )?;
    assert_eq!(bootstrap.applied_migrations, 0);
    assert_eq!(
        bootstrap.deferred_migrations,
        vec!["0039_observed_swaps_sol_leg_ts_index.sql".to_string()]
    );
    drop(bootstrap.store);

    let conn = rusqlite::Connection::open(&db_path)?;
    let recorded: i64 = conn.query_row(
        "SELECT COUNT(*) FROM schema_migrations WHERE version = '0039_observed_swaps_sol_leg_ts_index.sql'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(recorded, 0);
    Ok(())
}

#[test]
fn startup_defers_recorded_malformed_sol_leg_index() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let migrations_dir = dir.path().join("migrations");
    std::fs::create_dir(&migrations_dir)?;
    std::fs::copy(
        repo_root().join("migrations/0039_observed_swaps_sol_leg_ts_index.sql"),
        migrations_dir.join("0039_observed_swaps_sol_leg_ts_index.sql"),
    )?;

    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    drop(store);

    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute_batch(
        "DROP INDEX IF EXISTS idx_observed_swaps_sol_leg_ts_slot_signature;
         CREATE INDEX idx_observed_swaps_sol_leg_ts_slot_signature
         ON observed_swaps(ts, slot, signature)
         WHERE token_out = 'So11111111111111111111111111111111111111112';
         INSERT INTO observed_swaps(
            signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
         ) VALUES ('sig-recorded-bad-sol-leg', 'wallet-a', 'raydium', 'TOKEN', 'SOL', 1.0, 2.0, 1, '2026-05-05T10:00:00+00:00');
         INSERT INTO schema_migrations(version, applied_at)
         VALUES ('0039_observed_swaps_sol_leg_ts_index.sql', datetime('now'));",
    )?;
    drop(conn);

    let bootstrap = SqliteDiscoveryStore::open_and_migrate_for_startup(
        &db_path,
        &migrations_dir,
        &SqliteStartupPolicy::default(),
        None,
    )?;
    assert_eq!(bootstrap.applied_migrations, 0);
    assert_eq!(
        bootstrap.deferred_migrations,
        vec!["0039_observed_swaps_sol_leg_ts_index.sql".to_string()]
    );
    Ok(())
}

#[test]
fn startup_defers_projection_covering_index_on_non_empty_projection() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let migrations_dir = dir.path().join("migrations");
    std::fs::create_dir(&migrations_dir)?;
    std::fs::copy(
        repo_root().join("migrations/0043_observed_sol_leg_projection_covering_index.sql"),
        migrations_dir.join("0043_observed_sol_leg_projection_covering_index.sql"),
    )?;

    let store = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&store)?;
    store.ensure_observed_swap_writer_tables()?;
    store.insert_observed_swap(&copybot_core_types::SwapEvent {
        signature: "sig-projection-covering".to_string(),
        wallet: "wallet-a".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "TOKEN-A".to_string(),
        amount_in: 1.0,
        amount_out: 10.0,
        slot: 1,
        ts_utc: chrono::DateTime::parse_from_rfc3339("2026-05-05T10:00:00+00:00")?
            .with_timezone(&chrono::Utc),
        exact_amounts: None,
    })?;
    drop(store);

    let bootstrap = SqliteDiscoveryStore::open_and_migrate_for_startup(
        &db_path,
        &migrations_dir,
        &SqliteStartupPolicy::default(),
        None,
    )?;
    assert_eq!(bootstrap.applied_migrations, 0);
    assert_eq!(
        bootstrap.deferred_migrations,
        vec!["0043_observed_sol_leg_projection_covering_index.sql".to_string()]
    );
    drop(bootstrap.store);

    let conn = rusqlite::Connection::open(&db_path)?;
    let recorded: i64 = conn.query_row(
        "SELECT COUNT(*) FROM schema_migrations
         WHERE version = '0043_observed_sol_leg_projection_covering_index.sql'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(recorded, 0);
    Ok(())
}

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("storage-core crate must live under repo/crates")
}
