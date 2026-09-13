#[path = "common/source_sell_promotion_fixture.rs"]
mod fixture;
#[path = "common/historical_migration_fixture.rs"]
mod historical;
use anyhow::Result;
use copybot_storage_core::SqliteStore;
use fixture::*;

#[test]
fn signal_binding_ignore_abort_commit_and_missing_post_write_rows_roll_back_everything(
) -> Result<()> {
    for failure in [
        "signal-abort",
        "signal-ignore",
        "binding-abort",
        "binding-ignore",
        "commit",
        "signal-delete",
        "binding-delete",
    ] {
        let mut db = Db::new()?;
        db.proven("a", "source-a")?;
        let staged = prepare(&db, "exit", "source-a")?;
        let conn = db.conn()?;
        conn.execute_batch("CREATE TABLE test_parent(id INTEGER PRIMARY KEY);
            CREATE TABLE test_effect(id INTEGER REFERENCES test_parent(id) DEFERRABLE INITIALLY DEFERRED);")?;
        let trigger = match failure {
            "signal-abort" => "CREATE TRIGGER fault AFTER INSERT ON copy_signals BEGIN INSERT INTO test_effect VALUES(1); SELECT RAISE(ABORT,'signal-fault'); END;",
            "signal-ignore" => "CREATE TRIGGER fault BEFORE INSERT ON copy_signals BEGIN INSERT INTO test_effect VALUES(1); SELECT RAISE(IGNORE); END;",
            "binding-abort" => "CREATE TRIGGER fault AFTER INSERT ON execution_source_sell_promotions BEGIN INSERT INTO test_effect VALUES(1); SELECT RAISE(ABORT,'binding-fault'); END;",
            "binding-ignore" => "CREATE TRIGGER fault BEFORE INSERT ON execution_source_sell_promotions BEGIN INSERT INTO test_effect VALUES(1); SELECT RAISE(IGNORE); END;",
            "commit" => "CREATE TRIGGER fault AFTER INSERT ON execution_source_sell_promotions BEGIN INSERT INTO test_effect VALUES(1); END;",
            "signal-delete" => "CREATE TRIGGER fault AFTER INSERT ON execution_source_sell_promotions BEGIN DELETE FROM copy_signals WHERE signal_id=NEW.signal_id; END;",
            _ => "CREATE TRIGGER fault AFTER INSERT ON execution_source_sell_promotions BEGIN DELETE FROM execution_source_sell_promotions WHERE signal_id=NEW.signal_id; END;",
        };
        conn.execute_batch(trigger)?;
        let before = snapshot(&conn, &[])?;
        let error = format!(
            "{:#}",
            db.store
                .promote_execution_source_sell_intent(&staged.intent_id)
                .unwrap_err()
        );
        let expected = match failure {
            "signal-abort" => "signal-fault",
            "signal-ignore" => "source SELL signal insertion refused",
            "binding-abort" => "binding-fault",
            "binding-ignore" => "source SELL promotion binding insertion refused",
            "commit" => "FOREIGN KEY constraint failed",
            "signal-delete" => "signal missing after insertion",
            _ => "binding changed during insertion",
        };
        assert!(error.contains(expected), "{failure}: {error}");
        assert_eq!(snapshot(&conn, &[])?, before);
        db.reopen()?;
        assert_eq!(snapshot(&conn, &[])?, before);
        conn.execute_batch("DROP TRIGGER fault")?;
        let binding = promoted(
            db.store
                .promote_execution_source_sell_intent(&staged.intent_id)?,
        );
        assert!(
            matches!(db.store.promote_execution_source_sell_intent(&staged.intent_id)?, Outcome::Existing(b) if b == binding)
        );
    }
    Ok(())
}

#[test]
fn sql_domain_and_missing_proof_failures_never_become_legacy_permission() -> Result<()> {
    for sql in [
        "ALTER TABLE fills RENAME COLUMN accounting_basis TO unavailable",
        "ALTER TABLE execution_source_sell_promotions RENAME COLUMN intent_id TO unavailable",
        "UPDATE execution_source_sell_intents SET amount_out_decimals=NULL",
        "UPDATE execution_source_sell_intents SET staged_at='broken'",
        "UPDATE execution_canary_receipt_facts SET wallet_pubkey=x'00'",
    ] {
        let db = Db::new()?;
        db.proven("a", "source-a")?;
        let staged = prepare(&db, "exit", "source-a")?;
        let saved = signal(
            &db,
            &promoted(
                db.store
                    .promote_execution_source_sell_intent(&staged.intent_id)?,
            ),
        )?;
        // The CHECK must be bypassed only for the deliberate malformed exact tuple.
        db.conn()?
            .execute_batch(&format!("PRAGMA ignore_check_constraints=ON; {sql}"))?;
        let before = snapshot(&db.conn()?, &[])?;
        assert!(
            db.store
                .promote_execution_source_sell_intent(&staged.intent_id)
                .is_err(),
            "{sql}"
        );
        assert!(
            db.store
                .execution_sell_intent_position_block_reason(&saved)
                .is_err(),
            "{sql}"
        );
        assert_eq!(snapshot(&db.conn()?, &[])?, before);
    }
    Ok(())
}

#[test]
fn additive_0059_upgrade_preserves_existing_history_schema_and_reopens() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let old = dir.path().join("before-0059");
    copy_migrations_before(&old, "0059")?;
    let path = dir.path().join("upgrade.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&old)?;
    let mut db = Db {
        dir,
        path,
        store,
        now: "2026-09-07T12:00:00Z".parse()?,
    };
    historical::buy(&db.conn()?, "a", "source-a", "sig:exec-canary:a", db.now)?;
    db.store
        .activate_follow_wallet("source-a", db.now, "legacy")?;
    let legacy = db.observed("legacy", "source-a")?;
    db.store.record_execution_sell_intent(&legacy)?;
    let staged = prepare(&db, "staged", "source-a")?;
    let conn = db.conn()?;
    let before = snapshot(
        &conn,
        &[
            "schema_migrations",
            MARKER,
            "execution_failed_sell_sweep_cursors",
            "execution_source_sell_staging_cursor",
            "observed_retention_boundary",
        ],
    )?;
    let schema: String = conn.query_row("SELECT group_concat(sql,char(10)) FROM (SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name)", [], |r|r.get(0))?;
    assert!(db
        .store
        .promote_execution_source_sell_intent(&staged.intent_id)
        .is_err());
    assert_eq!(
        snapshot(
            &conn,
            &[
                "schema_migrations",
                MARKER,
                "execution_failed_sell_sweep_cursors",
                "execution_source_sell_staging_cursor",
                "observed_retention_boundary"
            ]
        )?,
        before
    );
    let through63 = db.dir.path().join("through63");
    historical::prefix(&through63, "0064")?;
    assert_eq!(db.store.run_migrations(&through63)?, 5);
    db.reopen()?;
    assert_eq!(db.store.run_migrations(&through63)?, 0);
    assert_eq!(
        snapshot(
            &conn,
            &[
                "schema_migrations",
                MARKER,
                "execution_failed_sell_sweep_cursors",
                "execution_source_sell_staging_cursor",
                "observed_retention_boundary"
            ]
        )?,
        before
    );
    let after_schema: String = conn.query_row("SELECT group_concat(sql,char(10)) FROM (SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND tbl_name NOT IN ('execution_source_sell_promotions','execution_failed_sell_sweep_cursors','execution_source_sell_staging_cursor','observed_retention_boundary') AND name NOT IN ('idx_buy_receipt_proofs_signature','idx_buy_receipt_facts_signature','idx_buy_receipt_orders_signature') ORDER BY name)", [], |r|r.get(0))?;
    assert_eq!(after_schema, schema);
    assert_eq!(
        conn.query_row(
            "SELECT floor_ts FROM observed_retention_boundary WHERE id=1",
            [],
            |r| r.get::<_, Option<String>>(0)
        )?,
        None
    );
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM execution_source_sell_staging_cursor",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM execution_failed_sell_sweep_cursors",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    assert!(!conn
        .prepare("PRAGMA foreign_key_list('execution_source_sell_promotions')")?
        .exists([])?);
    assert!(!conn.prepare("SELECT 1 FROM sqlite_master WHERE type IN ('trigger','view') AND tbl_name='execution_source_sell_promotions'")?.exists([])?);
    let binding = promoted(
        db.store
            .promote_execution_source_sell_intent(&staged.intent_id)?,
    );
    db.reopen()?;
    assert!(
        matches!(db.store.promote_execution_source_sell_intent(&staged.intent_id)?, Outcome::Existing(b) if b == binding)
    );
    assert!(conn.execute("INSERT INTO execution_source_sell_promotions VALUES('other',?1,'2026-09-07T12:00:00Z')", [&staged.intent_id]).is_err());
    // Public observed retention has no cascade to promotion history.
    db.store.delete_observed_swaps_before_batch(
        staged.event.ts_utc + chrono::Duration::seconds(1),
        10,
    )?;
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM execution_source_sell_promotions",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    assert!(!conn.prepare("PRAGMA foreign_key_check")?.exists([])?);
    Ok(())
}
