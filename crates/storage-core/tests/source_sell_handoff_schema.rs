#[path = "common/source_sell_fixture.rs"]
mod fixture;
#[path = "common/historical_migration_fixture.rs"]
mod historical;
use anyhow::Result;
use copybot_storage_core::{
    source_sell_handoff_schema, SourceSellCandidate as Candidate, SqliteStore,
};
use fixture::*;

#[test]
fn migration65_requires_no64_and_unmigrated_journal_never_claims_handoff() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let migrations = dir.path().join("historical-no64");
    copy_migrations_before(&migrations, "0064")?;
    std::fs::copy(
        std::path::Path::new(MIGRATIONS).join(source_sell_handoff_schema::MIGRATION),
        migrations.join(source_sell_handoff_schema::MIGRATION),
    )?;
    let path = dir.path().join("no64.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&migrations)?;
    let mut db = Db {
        dir,
        path,
        store,
        now: "2026-09-07T12:00:00Z".parse()?,
    };
    db.reopen()?;
    assert!(source_sell_handoff_schema::available(&db.conn()?)?);
    let count: i64 = db.conn()?.query_row(
        "SELECT COUNT(*) FROM schema_migrations WHERE version LIKE '0064%'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(
        count, 0,
        "0065 must work without 0064, independently of current migrations"
    );
    // Synthetic independent migration marker models additive coexistence; not dispatch53 integration.
    db.conn()?.execute("INSERT INTO schema_migrations(version,applied_at) VALUES('0064_fixture_independent.sql',?1)",[db.now.to_rfc3339()])?;
    assert!(source_sell_handoff_schema::available(&db.conn()?)?);
    let dir = tempfile::tempdir()?;
    let store = SqliteStore::open(dir.path().join("journal.db"))?;
    let e = db.sell("journal-only", "source-a");
    store.insert_recent_raw_journal_batch(&[e.clone()], db.now)?;
    let conn = rusqlite::Connection::open(dir.path().join("journal.db"))?;
    assert!(!source_sell_handoff_schema::available(&conn)?);
    assert!(store.load_source_sell_handoff(&e.signature).is_err());
    assert!(store.advance_source_sell_handoff().is_err());
    assert!(store
        .insert_observed_swaps_with_candidates(&[e.clone()], &[Some(Candidate::new(&e, "p"))])
        .is_err());
    assert_eq!(store.load_observed_swaps_since(db.now)?.len(), 1);
    Ok(())
}

#[test]
fn checkpoint_update_fault_rolls_back_and_does_not_skip_pending_job() -> Result<()> {
    for fault in ["abort", "ignore", "commit"] {
        let mut db = Db::new()?;
        db.proven("buy", "source-a")?;
        let e = db.sell("pending", "source-a");
        db.store.insert_observed_swaps_with_candidates(
            &[e.clone()],
            &[Some(Candidate::new(&e, &db.position()?))],
        )?;
        let sql=match fault {
            "abort"=>"CREATE TRIGGER fault BEFORE INSERT ON source_sell_handoff_cursor BEGIN SELECT RAISE(ABORT,'cursor fault'); END;",
            "ignore"=>"CREATE TRIGGER fault BEFORE INSERT ON source_sell_handoff_cursor BEGIN SELECT RAISE(IGNORE); END;",
            _=>"CREATE TABLE parent(id PRIMARY KEY); CREATE TABLE effect(id REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fault AFTER INSERT ON source_sell_handoff_cursor BEGIN INSERT INTO effect VALUES(99); END;",
        };
        db.conn()?.execute_batch(sql)?;
        let before = snapshot(&db.conn()?, &[])?;
        assert!(db.store.advance_source_sell_handoff().is_err());
        assert_eq!(snapshot(&db.conn()?, &[])?, before);
        db.conn()?.execute_batch("DROP TRIGGER fault")?;
        db.reopen()?;
        assert_eq!(
            db.store
                .advance_source_sell_handoff()?
                .unwrap()
                .event
                .signature,
            e.signature
        );
    }
    Ok(())
}

#[test]
fn migration65_upgrade_preserves_prior_schema_and_rows_then_keeps_legacy_unknown() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let old = dir.path().join("old");
    copy_migrations_before(&old, "0065")?;
    let through65 = dir.path().join("through65");
    copy_migrations_before(&through65, "0066")?;
    let path = dir.path().join("upgrade.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&old)?;
    let mut db = Db {
        dir,
        path,
        store,
        now: "2026-09-07T12:00:00Z".parse()?,
    };
    historical::buy(
        &db.conn()?,
        "old-buy",
        "source-a",
        "sig:exec-canary:old-buy",
        db.now,
    )?;
    let e = db.observed("old-event", "source-a")?;
    let p = db.position()?;
    let stage = inserted(db.store.stage_execution_source_sell_intent(&e, &p)?);
    db.store
        .promote_execution_source_sell_intent(&stage.intent_id)?;
    let before = snapshot(&db.conn()?, &["schema_migrations"])?;
    let conn = db.conn()?;
    let schemas = conn
        .prepare("SELECT name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name")?
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert_eq!(db.store.run_migrations(&through65)?, 1);
    db.reopen()?;
    let after = snapshot(&db.conn()?, &["schema_migrations"])?;
    for (table, rows) in before {
        assert_eq!(after[&table], rows, "{table}");
    }
    for (name, sql) in schemas {
        assert_eq!(
            db.conn()?.query_row(
                "SELECT sql FROM sqlite_master WHERE name=?1",
                [&name],
                |r| r.get::<_, String>(0)
            )?,
            sql,
            "{name}"
        );
    }
    assert!(db.store.load_source_sell_handoff(&e.signature)?.is_none());
    assert!(
        !db.store
            .insert_observed_swaps_with_candidates(
                &[e.clone()],
                &[Some(Candidate::new(&e, "current-q"))]
            )?
            .inserted[0]
    );
    assert_eq!(
        db.store
            .load_source_sell_handoff(&e.signature)?
            .unwrap()
            .disposition,
        "unknown"
    );
    assert_eq!(
        format!(
            "{:?}",
            db.store
                .load_execution_source_sell_intent(&stage.intent_id)?
                .unwrap()
        ),
        format!("{stage:?}")
    );
    assert_eq!(db.store.run_migrations(&through65)?, 0);
    Ok(())
}

#[test]
fn retention_tombstone_failure_rolls_back_delete_and_boundary_metadata() -> Result<()> {
    for fault in ["abort", "ignore", "commit"] {
        let db = Db::new()?;
        let e = db.observed("old-pre65", "source-a")?;
        db.conn()?.execute(
            "DELETE FROM source_sell_handoffs WHERE signature=?1",
            [&e.signature],
        )?;
        let sql=match fault {
            "abort"=>"CREATE TRIGGER fault BEFORE INSERT ON source_sell_handoffs BEGIN SELECT RAISE(ABORT,'retention handoff fault'); END;",
            "ignore"=>"CREATE TRIGGER fault BEFORE INSERT ON source_sell_handoffs BEGIN SELECT RAISE(IGNORE); END;",
            _=>"CREATE TABLE parent(id PRIMARY KEY); CREATE TABLE effect(id REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fault AFTER INSERT ON source_sell_handoffs BEGIN INSERT INTO effect VALUES(99); END;",
        };
        db.conn()?.execute_batch(sql)?;
        let before = snapshot(&db.conn()?, &[])?;
        assert!(db
            .store
            .delete_observed_swaps_before_batch(e.ts_utc + chrono::Duration::seconds(1), 1)
            .is_err());
        assert_eq!(snapshot(&db.conn()?, &[])?, before);
    }
    Ok(())
}
