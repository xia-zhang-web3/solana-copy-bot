#[path = "common/failed_expense_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::*;

#[test]
fn failed_expense_each_write_rolls_back_and_restart_recovers() -> Result<()> {
    for target in ["facts", "ledger", "completion"] {
        for action in ["RAISE(ABORT,'synthetic write rejection')", "RAISE(IGNORE)"] {
            let mut db = Db::new()?;
            db.detect(ORDER, "signature_status")?;
            let f = db.facts(ORDER, 5000)?;
            let clause = match target {
                "facts" => "BEFORE INSERT ON execution_failed_expense_facts",
                "ledger" => "BEFORE INSERT ON execution_failed_expense_ledger",
                _ => "BEFORE UPDATE ON execution_failed_expense_tasks WHEN NEW.status='complete'",
            };
            db.conn()?.execute_batch(&format!(
                "CREATE TRIGGER fail_write {clause} BEGIN SELECT {action}; END;"
            ))?;
            assert!(
                db.store.apply_failed_expense(ORDER, &f, db.now).is_err(),
                "{target} {action}"
            );
            db.reopen()?;
            assert_eq!(db.count("execution_failed_expense_ledger")?, 0);
            assert_eq!(db.count("execution_failed_expense_facts")?, 0);
            assert_eq!(
                db.store.load_failed_expense_task(ORDER)?.unwrap().status,
                "pending"
            );
            assert_eq!(
                db.store.take_failed_expense_tasks(ROUTE, 1, db.now)?.len(),
                1
            );
            db.conn()?.execute_batch("DROP TRIGGER fail_write")?;
            db.store.apply_failed_expense(ORDER, &f, db.now)?;
            assert_eq!(
                db.report(0)?.cohort_wallet_fee_lamports.as_deref(),
                Some("5000")
            );
        }
    }
    Ok(())
}
#[test]
fn failed_expense_detection_atomicity_preserves_nonterminal_order() -> Result<()> {
    for target in [
        "BEFORE INSERT ON execution_failed_expense_tasks",
        "BEFORE UPDATE ON orders",
    ] {
        for action in [
            "RAISE(ABORT,'synthetic detection rejection')",
            "RAISE(IGNORE)",
        ] {
            let db = Db::new()?;
            db.conn()?.execute_batch(&format!(
                "CREATE TRIGGER fail_detect {target} BEGIN SELECT {action}; END;"
            ))?;
            assert!(db.detect(ORDER, "signature_status").is_err());
            assert_eq!(db.count("execution_failed_expense_tasks")?, 0);
            assert_eq!(
                db.store.load_execution_canary_order(ORDER)?.unwrap().status,
                EXECUTION_STATUS_CANARY_SUBMITTED
            );
        }
    }
    Ok(())
}
#[test]
fn failed_expense_cursor_is_durable_fair_at_same_clock_and_retention_preserves_dedup() -> Result<()>
{
    let mut db = Db::new()?;
    db.detect(ORDER, "signature_status")?;
    db.add("exec-canary:z-b", "sig-b", "buy", db.now)?;
    db.detect("exec-canary:z-b", "receipt_meta")?;
    assert_eq!(
        db.store.take_failed_expense_tasks(ROUTE, 1, db.now)?[0].order_id,
        ORDER
    );
    db.reopen()?;
    assert_eq!(
        db.store.take_failed_expense_tasks(ROUTE, 1, db.now)?[0].order_id,
        "exec-canary:z-b"
    );
    db.store
        .apply_failed_expense("exec-canary:z-b", &db.facts("exec-canary:z-b", 11)?, db.now)?;
    let cutoff = db.now + chrono::Duration::days(50);
    db.store.apply_history_retention(
        HistoryRetentionCutoffs {
            risk_events_before: cutoff,
            copy_signals_before: cutoff,
            orders_before: cutoff,
            shadow_closed_trades_before: cutoff,
            execution_quote_canary_before: cutoff,
        },
        true,
    )?;
    db.reopen()?;
    assert_eq!(db.count("execution_failed_expense_tasks")?, 2);
    assert_eq!(db.count("execution_failed_expense_ledger")?, 1);
    assert_eq!(
        db.store.take_failed_expense_tasks(ROUTE, 1, db.now)?[0].order_id,
        ORDER
    );
    let conn = db.conn()?;
    conn.execute_batch("PRAGMA foreign_keys=ON")?;
    assert!(conn
        .execute("DELETE FROM orders WHERE order_id=?1", [ORDER])
        .is_err());
    assert!(conn
        .execute(
            "DELETE FROM execution_failed_expense_tasks WHERE order_id='exec-canary:z-b'",
            []
        )
        .is_err());
    assert!(!conn.prepare("PRAGMA foreign_key_check")?.exists([])?);
    Ok(())
}
#[test]
fn failed_expense_competing_connections_record_one_transaction() -> Result<()> {
    let db = Db::new()?;
    db.detect(ORDER, "signature_status")?;
    let f = db.facts(ORDER, 9000)?;
    let mut workers = Vec::new();
    for _ in 0..4 {
        let path = db.path.clone();
        let f = f.clone();
        let now = db.now;
        workers.push(std::thread::spawn(move || -> Result<()> {
            let store = SqliteStore::open(&path)?;
            store.apply_failed_expense(ORDER, &f, now)
        }));
    }
    for worker in workers {
        worker.join().unwrap()?;
    }
    assert_eq!(db.count("execution_failed_expense_ledger")?, 1);
    assert_eq!(db.count("fills")?, 0);
    Ok(())
}

#[test]
fn failed_expense_cursor_write_failure_cannot_silently_lose_advancement() -> Result<()> {
    for target in [
        "BEFORE UPDATE ON execution_failed_expense_tasks",
        "BEFORE INSERT ON execution_failed_expense_cursor",
        "BEFORE UPDATE ON execution_failed_expense_cursor",
    ] {
        for action in ["RAISE(ABORT,'synthetic cursor rejection')", "RAISE(IGNORE)"] {
            let mut db = Db::new()?;
            db.detect(ORDER, "signature_status")?;
            let snapshot = || -> Result<(i64, i64, Option<String>)> {
                Ok(db.conn()?.query_row(
                    "SELECT t.attempt_seq,c.sequence,t.last_attempt_at FROM execution_failed_expense_tasks t JOIN execution_failed_expense_cursor c ON c.route=t.route WHERE t.order_id=?1",
                    [ORDER], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?)
            };
            let before = snapshot()?;
            db.conn()?.execute_batch(&format!(
                "CREATE TRIGGER reject_cursor {target} BEGIN SELECT {action}; END;"
            ))?;
            assert!(db
                .store
                .take_failed_expense_tasks(ROUTE, 1, db.now)
                .is_err());
            assert_eq!(snapshot()?, before);
            db.reopen()?;
            assert_eq!(db.conn()?.query_row(
                "SELECT t.attempt_seq,c.sequence,t.last_attempt_at FROM execution_failed_expense_tasks t JOIN execution_failed_expense_cursor c ON c.route=t.route WHERE t.order_id=?1",
                [ORDER], |r| Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?,r.get::<_,Option<String>>(2)?)))?, before);
            db.conn()?.execute_batch("DROP TRIGGER reject_cursor")?;
            assert_eq!(
                db.store.take_failed_expense_tasks(ROUTE, 1, db.now)?.len(),
                1
            );
        }
    }
    Ok(())
}
