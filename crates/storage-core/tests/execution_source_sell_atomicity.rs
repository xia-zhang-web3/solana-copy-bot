#[path = "common/source_sell_fixture.rs"]
mod fixture;
#[path = "common/historical_migration_fixture.rs"]
mod historical;
use anyhow::Result;
use copybot_storage_core::{ExecutionSellIntentOutcome, SqliteStore};
use fixture::*;

#[test]
fn abort_ignore_and_commit_failure_cannot_leave_partial_rows_or_success() -> Result<()> {
    for failure in ["abort", "ignore", "commit"] {
        let mut db = Db::new()?;
        db.proven("a", "source-a")?;
        let position = db.position()?;
        let event = db.observed("sell", "source-a")?;
        let conn = db.conn()?;
        conn.execute_batch("CREATE TABLE test_parent(id INTEGER PRIMARY KEY);
            CREATE TABLE test_side_effect(id INTEGER REFERENCES test_parent(id) DEFERRABLE INITIALLY DEFERRED);")?;
        let trigger = match failure {
            "abort" => "CREATE TRIGGER refuse_stage AFTER INSERT ON execution_source_sell_intents BEGIN
                INSERT INTO test_side_effect VALUES(1); SELECT RAISE(ABORT,'test-stage-abort'); END;",
            "ignore" => "CREATE TRIGGER refuse_stage BEFORE INSERT ON execution_source_sell_intents BEGIN
                INSERT INTO test_side_effect VALUES(1); SELECT RAISE(IGNORE); END;",
            _ => "CREATE TRIGGER refuse_stage AFTER INSERT ON execution_source_sell_intents BEGIN
                INSERT INTO test_side_effect VALUES(1); END;",
        };
        conn.execute_batch(trigger)?;
        let before = snapshot(&conn, &[])?;
        let error = db
            .store
            .stage_execution_source_sell_intent(&event, &position)
            .unwrap_err();
        let error = format!("{error:#}");
        let expected = match failure {
            "abort" => "test-stage-abort",
            "ignore" => "staged SELL insertion refused",
            _ => "FOREIGN KEY constraint failed",
        };
        assert!(error.contains(expected), "{failure}: {error}");
        assert_eq!(snapshot(&conn, &[])?, before, "{failure}");
        db.reopen()?;
        assert_eq!(snapshot(&conn, &[])?, before);
        conn.execute_batch("DROP TRIGGER refuse_stage")?;
        let row = inserted(
            db.store
                .stage_execution_source_sell_intent(&event, &position)?,
        );
        let replay = existing(
            db.store
                .stage_execution_source_sell_intent(&event, &position)?,
        );
        assert_eq!(format!("{row:?}"), format!("{replay:?}"));
    }
    Ok(())
}

#[test]
fn sql_and_domain_errors_remain_errors_and_do_not_change_any_table() -> Result<()> {
    for sql in [
        "ALTER TABLE fills RENAME COLUMN accounting_basis TO unavailable",
        "ALTER TABLE execution_canary_receipt_proofs RENAME COLUMN wallet_pubkey TO unavailable",
        "UPDATE execution_canary_receipt_facts SET wallet_pubkey=x'00'",
        "UPDATE observed_swaps SET qty_in_decimals=NULL",
        "DROP TABLE execution_source_sell_intents",
    ] {
        let db = Db::new()?;
        db.proven("a", "source-a")?;
        let position = db.position()?;
        let event = db.observed("sell", "source-a")?;
        // A partially populated exact tuple is a persisted-domain error, not missing proof.
        db.conn()?.execute_batch(sql)?;
        let before = snapshot(&db.conn()?, &[])?;
        assert!(
            db.store
                .stage_execution_source_sell_intent(&event, &position)
                .is_err(),
            "{sql}"
        );
        assert_eq!(snapshot(&db.conn()?, &[])?, before, "{sql}");
    }
    Ok(())
}

#[test]
fn malformed_staged_history_is_an_error_on_read_and_replay() -> Result<()> {
    let db = Db::new()?;
    db.proven("a", "source-a")?;
    let position = db.position()?;
    let event = db.observed("sell", "source-a")?;
    let row = inserted(
        db.store
            .stage_execution_source_sell_intent(&event, &position)?,
    );
    // The bounded history reader must expose malformed durable data as an error.
    db.conn()?.execute(
        "UPDATE execution_source_sell_intents SET staged_at='invalid'",
        [],
    )?;
    let before = snapshot(&db.conn()?, &[])?;
    assert!(db
        .store
        .load_execution_source_sell_intent(&row.intent_id)
        .is_err());
    assert!(db
        .store
        .stage_execution_source_sell_intent(&event, &position)
        .is_err());
    assert_eq!(snapshot(&db.conn()?, &[])?, before);
    Ok(())
}

#[test]
fn additive_0058_upgrade_preserves_old_rows_schema_and_legacy_queue_then_reopens() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let old = dir.path().join("before-0058");
    copy_migrations_before(&old, "0058")?;
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
    let position = db.position()?;
    db.store
        .activate_follow_wallet("source-a", db.now, "legacy")?;
    let legacy = db.observed("old-legacy-sell", "source-a")?;
    let ExecutionSellIntentOutcome::Inserted(signal) =
        db.store.record_execution_sell_intent(&legacy)?
    else {
        panic!("legacy before migration")
    };
    let staged = db.observed("new-staged-sell", "source-a")?;
    let conn = db.conn()?;
    let before = snapshot(
        &conn,
        &[
            "schema_migrations",
            TABLE,
            "execution_source_sell_promotions",
            "execution_failed_sell_sweep_cursors",
        ],
    )?;
    let old_schema: String = conn.query_row("SELECT group_concat(sql, char(10)) FROM (SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name)", [], |r| r.get(0))?;
    assert!(db
        .store
        .stage_execution_source_sell_intent(&staged, &position)
        .is_err());
    assert_eq!(
        snapshot(
            &conn,
            &[
                "schema_migrations",
                TABLE,
                "execution_source_sell_promotions",
                "execution_failed_sell_sweep_cursors"
            ]
        )?,
        before
    );
    // This historical contract covers 0058..0061; later additive migrations have separate upgrade tests.
    let through_61 = db.dir.path().join("through-0061");
    copy_migrations_before(&through_61, "0062")?;
    assert_eq!(db.store.run_migrations(&through_61)?, 4);
    assert_eq!(db.store.run_migrations(&through_61)?, 0);
    db.reopen()?;
    assert_eq!(db.store.run_migrations(&through_61)?, 0);
    assert_eq!(
        snapshot(
            &conn,
            &[
                "schema_migrations",
                TABLE,
                "execution_source_sell_promotions",
                "execution_failed_sell_sweep_cursors"
            ]
        )?,
        before
    );
    let preserved_schema: String = conn.query_row("SELECT group_concat(sql, char(10)) FROM (SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND tbl_name NOT IN ('execution_source_sell_intents','execution_source_sell_promotions','execution_failed_sell_sweep_cursors') AND name NOT IN ('idx_buy_receipt_proofs_signature','idx_buy_receipt_facts_signature','idx_buy_receipt_orders_signature') ORDER BY name)", [], |r| r.get(0))?;
    assert_eq!(preserved_schema, old_schema);
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM execution_failed_sell_sweep_cursors",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    assert!(!conn
        .prepare("SELECT 1 FROM pragma_foreign_key_list('execution_source_sell_intents')")?
        .exists([])?);
    assert!(!conn.prepare("SELECT 1 FROM sqlite_master WHERE type IN ('trigger','view') AND tbl_name='execution_source_sell_intents'")?.exists([])?);
    assert!(!conn.prepare("SELECT 1 FROM pragma_table_info('execution_source_sell_intents') WHERE name IN ('status','ready','approved')")?.exists([])?);
    let row = inserted(
        db.store
            .stage_execution_source_sell_intent(&staged, &position)?,
    );
    assert_eq!(
        snapshot(
            &conn,
            &[
                "schema_migrations",
                TABLE,
                "execution_source_sell_promotions",
                "execution_failed_sell_sweep_cursors"
            ]
        )?,
        before
    );
    assert_eq!(
        db.store
            .list_execution_quote_canary_owned_sell_signal_candidate_ids(
                "shadow_recorded",
                db.now,
                10
            )?,
        vec![signal.signal_id]
    );
    db.reopen()?;
    assert_eq!(
        format!(
            "{:?}",
            db.store
                .load_execution_source_sell_intent(&row.intent_id)?
                .unwrap()
        ),
        format!("{row:?}")
    );
    assert!(!conn.prepare("PRAGMA foreign_key_check")?.exists([])?);
    Ok(())
}
