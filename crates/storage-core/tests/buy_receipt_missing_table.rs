#[path = "common/buy_attribution_review_fixture.rs"]
mod fixture;

use anyhow::Result;
use copybot_core_types::TokenQuantity;
use fixture::Db;

fn money(db: &Db) -> Result<(String, i64, f64, f64, i64)> {
    Ok(db.conn()?.query_row(
        "SELECT qty_raw,cost_lamports,qty,cost_sol,(SELECT COUNT(*) FROM fills)
         FROM positions WHERE token='mint' AND state='open'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
    )?)
}

fn attempt(hidden: bool) -> Result<()> {
    let mut db = Db::new()?;
    let a = db.seed("dup-a", "leader-a", "buy")?;
    db.buy(&a)?;
    let b = db.seed("dup-b", "leader-b", "buy")?;
    // After valid API setup, retain A's durable facts and B's durable proof.
    let conn = db.conn()?;
    // Explicit historical/corruption fixture, as in the existing orphan controls.
    conn.execute_batch("PRAGMA foreign_keys=OFF;")?;
    conn.execute(
        "DELETE FROM execution_canary_receipt_proofs WHERE order_id=?1",
        [&a],
    )?;
    conn.execute(
        "DELETE FROM execution_canary_receipt_facts WHERE order_id=?1",
        [&b],
    )?;
    drop(conn);
    if hidden {
        db.conn()?.execute_batch(
            "ALTER TABLE execution_canary_receipt_facts RENAME TO unavailable_facts;",
        )?;
    }
    db.reopen()?;
    let version: String = db.conn()?.query_row(
        "SELECT version FROM schema_migrations
         WHERE version='0054_execution_canary_receipt_facts.sql'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(version, "0054_execution_canary_receipt_facts.sql");
    let state_before = snapshot(&db)?;
    let before = money(&db)?;
    assert_eq!(before, ("7000".into(), 1000, 7.0, 0.000001, 1));
    let result = db.store.record_execution_canary_open_position(
        &b,
        "mint",
        7.0,
        Some(TokenQuantity::new(7000, 3)),
        0.000001,
        db.now,
    );
    let after = money(&db)?;
    println!("MISSING_TABLE hidden={hidden} before={before:?} after={after:?} result={result:?}");
    assert!(
        result.is_err(),
        "modern missing claims must not become legacy success"
    );
    assert_eq!(after, before, "no new money or fill marker after rejection");
    assert!(!db.store.execution_canary_fill_exists(&b)?);
    assert_eq!(
        snapshot(&db)?,
        state_before,
        "orders, hidden receipts and schema unchanged"
    );
    Ok(())
}

#[test]
fn available_facts_only_counterpart_blocks_duplicate() -> Result<()> {
    attempt(false)
}

#[test]
fn migrated_missing_facts_cannot_hide_counterpart_after_reopen() -> Result<()> {
    attempt(true)
}

// Separate connections observe all monetary/order rows and retained hidden rows.
// Include DDL and migration metadata to detect accidental schema/history repair.
fn snapshot(db: &Db) -> Result<Vec<(String, Vec<Vec<rusqlite::types::Value>>)>> {
    let conn = db.conn()?;
    let mut tables: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND
            (name IN ('copy_signals','orders','positions','fills','schema_migrations',
                      'execution_canary_receipt_proofs','execution_canary_receipt_facts')
             OR name LIKE 'unavailable_%') ORDER BY name",
        )?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    tables.push("sqlite_master".into());
    let mut state = Vec::new();
    for table in tables {
        let mut stmt = conn.prepare(&format!("SELECT * FROM {table} ORDER BY 1,2"))?;
        let columns = stmt.column_count();
        let rows = stmt
            .query_map([], |r| (0..columns).map(|c| r.get(c)).collect())?
            .collect::<rusqlite::Result<_>>()?;
        state.push((table, rows));
    }
    Ok(state)
}

#[test]
fn migrated_missing_proofs_cannot_turn_proof_only_buy_into_import() -> Result<()> {
    let mut db = Db::new()?;
    let a = db.seed("dup-a", "leader-a", "buy")?;
    db.buy(&a)?;
    let b = db.seed("dup-b", "leader-b", "buy")?;
    // Corruption only after valid public API setup: B is proof-only before hiding.
    db.conn()?.execute(
        "DELETE FROM execution_canary_receipt_facts WHERE order_id=?1",
        [&b],
    )?;
    assert!(db
        .conn()?
        .prepare("SELECT 1 FROM execution_canary_receipt_proofs WHERE order_id=?1")?
        .exists([&b])?);
    db.conn()?.execute_batch(
        "ALTER TABLE execution_canary_receipt_proofs RENAME TO unavailable_proofs;",
    )?;
    db.reopen()?;
    let version: String = db.conn()?.query_row(
        "SELECT version FROM schema_migrations WHERE version='0051_execution_canary_receipt_proofs.sql'",
        [], |r| r.get(0))?;
    assert_eq!(version, "0051_execution_canary_receipt_proofs.sql");
    let state_before = snapshot(&db)?;
    let before = money(&db)?;
    assert_eq!(before, ("7000".into(), 1000, 7.0, 0.000001, 1));
    let result = db.store.record_execution_canary_open_position(
        &b,
        "mint",
        7.0,
        Some(TokenQuantity::new(7000, 3)),
        0.000001,
        db.now,
    );
    let after = money(&db)?;
    println!("MISSING_PROOFS before={before:?} after={after:?} result={result:?}");
    let error = result.expect_err("modern missing proofs cannot become legacy import");
    assert!(format!("{error:#}").contains("0051_execution_canary_receipt_proofs.sql"));
    assert_eq!(after, before);
    assert!(!db.store.execution_canary_fill_exists(&b)?);
    assert_eq!(snapshot(&db)?, state_before);
    Ok(())
}

#[test]
fn migration_metadata_read_errors_are_not_legacy_success() -> Result<()> {
    for broken_view in [false, true] {
        let db = Db::new()?;
        let b = db.seed("metadata-buy", "leader", "buy")?;
        // Keep the store connection open: no reopen-time migration-ledger creation.
        // Explicit corruption fixture retains original metadata and facts intact.
        db.conn()?.execute_batch(
            "ALTER TABLE execution_canary_receipt_facts RENAME TO unavailable_facts;
             ALTER TABLE schema_migrations RENAME TO unavailable_migrations;",
        )?;
        if broken_view {
            db.conn()?.execute_batch(
                "CREATE VIEW schema_migrations AS SELECT version FROM missing_metadata;",
            )?;
        }
        let before = snapshot(&db)?;
        let result = db.store.record_execution_canary_open_position(
            &b,
            "mint",
            7.0,
            Some(TokenQuantity::new(7000, 3)),
            0.000001,
            db.now,
        );
        println!("METADATA_ERROR broken_view={broken_view} result={result:?}");
        let error = result.expect_err("unreadable metadata must not allow a new BUY");
        assert!(
            error.downcast_ref::<rusqlite::Error>().is_some(),
            "SQL error must propagate: {error:#}"
        );
        assert_eq!(snapshot(&db)?, before);
        assert!(!db.store.execution_canary_fill_exists(&b)?);
    }
    Ok(())
}

#[test]
fn recorded_fill_replay_precedes_missing_facts_and_metadata_guard() -> Result<()> {
    use copybot_storage_core::ExecutionCanaryPositionRecordOutcome;
    let db = Db::new()?;
    let a = db.seed("replay", "leader", "buy")?;
    let first = db.buy(&a)?;
    // Existing no-money replay keeps its boundary even when the new-fill guard
    // would reject unavailable receipt/metadata tables.
    db.conn()?.execute_batch(
        "ALTER TABLE execution_canary_receipt_facts RENAME TO unavailable_facts;
         ALTER TABLE schema_migrations RENAME TO unavailable_migrations;",
    )?;
    let before = snapshot(&db)?;
    let replay = db.store.record_execution_canary_open_position(
        &a,
        "mint",
        7.0,
        Some(TokenQuantity::new(7000, 3)),
        0.000001,
        db.now,
    )?;
    assert_eq!(
        replay.outcome,
        ExecutionCanaryPositionRecordOutcome::Existing
    );
    assert_eq!(replay.position.position_id, first.position.position_id);
    assert_eq!(money(&db)?, ("7000".into(), 1000, 7.0, 0.000001, 1));
    assert_eq!(snapshot(&db)?, before);
    Ok(())
}
