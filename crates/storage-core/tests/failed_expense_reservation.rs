#[path = "common/failed_expense_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::FailedExpenseCoverage;
use fixture::{Db, ORDER, ROUTE};
use rusqlite::OptionalExtension;

fn queue(db: &Db) -> Result<(Option<i64>, Vec<(String, i64, Option<String>)>)> {
    let conn = db.conn()?;
    let cursor = conn
        .query_row(
            "SELECT sequence FROM execution_failed_expense_cursor WHERE route=?1",
            [ROUTE],
            |r| r.get(0),
        )
        .optional()?;
    let mut stmt = conn.prepare("SELECT order_id,attempt_seq,last_attempt_at FROM execution_failed_expense_tasks ORDER BY order_id")?;
    let tasks = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok((cursor, tasks))
}

#[test]
fn failed_expense_initial_and_repeated_detection_share_sweep_sequence() -> Result<()> {
    let mut db = Db::new()?;
    for (index, source) in ["signature_status", "receipt_meta"].into_iter().enumerate() {
        db.detect(ORDER, source)?;
        assert_eq!(queue(&db)?.0, Some(index as i64 + 1));
        assert_eq!(queue(&db)?.1[0].1, index as i64 + 1);
        db.reopen()?;
    }
    // Fee is already booked, but native coverage still needs enrichment.
    let mut facts = db.facts(ORDER, 5000)?;
    facts.wallet_native_pre_lamports = None;
    facts.wallet_native_post_lamports = None;
    facts.native_coverage = FailedExpenseCoverage::Missing;
    db.store.apply_failed_expense(ORDER, &facts, db.now)?;
    assert_eq!(db.count("execution_failed_expense_ledger")?, 1);
    db.detect(ORDER, "receipt_meta")?;
    assert_eq!(queue(&db)?.0, Some(3));
    assert_eq!(queue(&db)?.1[0].1, 3);
    db.reopen()?;
    assert_eq!(
        db.store.take_failed_expense_tasks(ROUTE, 1, db.now)?.len(),
        1
    );
    assert_eq!(queue(&db)?.0, Some(4));
    assert_eq!(queue(&db)?.1[0].1, 4);
    db.store
        .apply_failed_expense(ORDER, &db.facts(ORDER, 5000)?, db.now)?;
    let complete = queue(&db)?;
    for source in ["signature_status", "receipt_meta"] {
        assert_eq!(db.detect(ORDER, source)?.status, "complete");
        assert!(db
            .store
            .take_failed_expense_tasks(ROUTE, 1, db.now)?
            .is_empty());
        assert_eq!(queue(&db)?, complete);
    }
    db.store
        .reject_failed_expense(ORDER, "synthetic_identity_conflict")?;
    let conflict = queue(&db)?;
    for source in ["signature_status", "receipt_meta"] {
        assert_eq!(db.detect(ORDER, source)?.status, "conflict");
        assert!(db
            .store
            .take_failed_expense_tasks(ROUTE, 1, db.now)?
            .is_empty());
        assert_eq!(queue(&db)?, conflict);
    }
    assert_eq!(db.count("execution_failed_expense_ledger")?, 1);
    Ok(())
}

#[test]
fn failed_expense_detection_reservation_write_failure_rolls_back_and_reopens() -> Result<()> {
    for mode in ["fresh", "repeat", "new_with_cursor"] {
        for source in ["signature_status", "receipt_meta"] {
            for target in [
                "BEFORE UPDATE OF attempt_seq ON execution_failed_expense_tasks",
                "BEFORE INSERT ON execution_failed_expense_cursor",
                "BEFORE UPDATE ON execution_failed_expense_cursor",
            ] {
                if mode == "fresh" && target.starts_with("BEFORE UPDATE ON") {
                    continue;
                }
                for action in [
                    "RAISE(ABORT,'synthetic reservation failure')",
                    "RAISE(IGNORE)",
                ] {
                    let mut db = Db::new()?;
                    if mode != "fresh" {
                        db.detect(ORDER, source)?;
                    }
                    let id = if mode == "new_with_cursor" {
                        db.add(
                            "exec-canary:new-reservation",
                            "new-reservation",
                            "buy",
                            db.now,
                        )?;
                        "exec-canary:new-reservation"
                    } else {
                        ORDER
                    };
                    let before = queue(&db)?;
                    let order = db.store.load_execution_canary_order(id)?.unwrap();
                    let task = db.store.load_failed_expense_task(id)?;
                    db.conn()?.execute_batch(&format!(
                        "CREATE TRIGGER reject_reservation {target} BEGIN SELECT {action}; END;"
                    ))?;
                    assert!(
                        db.detect(id, source).is_err(),
                        "{mode} {source} {target} {action}"
                    );
                    db.reopen()?;
                    assert_eq!(queue(&db)?, before);
                    assert_eq!(
                        db.store.load_execution_canary_order(id)?.unwrap().status,
                        order.status
                    );
                    let actual = db.store.load_failed_expense_task(id)?;
                    assert_eq!(serde_json::to_value(actual)?, serde_json::to_value(task)?);
                    assert_eq!(db.count("execution_failed_expense_ledger")?, 0);
                    db.conn()?
                        .execute_batch("DROP TRIGGER reject_reservation")?;
                    assert_eq!(db.detect(id, source)?.status, "pending");
                    assert_eq!(queue(&db)?.0, Some(before.0.unwrap_or(0) + 1));
                    db.store
                        .apply_failed_expense(id, &db.facts(id, 5000)?, db.now)?;
                    assert_eq!(db.count("execution_failed_expense_ledger")?, 1);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn failed_expense_reservation_overflow_rolls_back_detection_and_entire_batch() -> Result<()> {
    for detect in [true, false] {
        let mut db = Db::new()?;
        if !detect {
            db.detect(ORDER, "signature_status")?;
            db.add("exec-canary:overflow-b", "overflow-b", "buy", db.now)?;
            db.detect("exec-canary:overflow-b", "receipt_meta")?;
        }
        let limit = if detect { i64::MAX } else { i64::MAX - 1 };
        db.conn()?.execute("INSERT INTO execution_failed_expense_cursor(route,sequence) VALUES(?1,?2) ON CONFLICT(route) DO UPDATE SET sequence=excluded.sequence", rusqlite::params![ROUTE, limit])?;
        let before = queue(&db)?;
        let status = db.store.load_execution_canary_order(ORDER)?.unwrap().status;
        let result = if detect {
            db.detect(ORDER, "signature_status").map(|_| ())
        } else {
            db.store
                .take_failed_expense_tasks(ROUTE, 2, db.now)
                .map(|_| ())
        };
        assert!(format!("{:#}", result.unwrap_err()).contains("overflow"));
        db.reopen()?;
        assert_eq!(queue(&db)?, before);
        assert_eq!(
            db.store.load_execution_canary_order(ORDER)?.unwrap().status,
            status
        );
    }
    Ok(())
}

#[test]
fn failed_expense_legacy_zero_sequences_survive_reopen_and_precede_new_detection() -> Result<()> {
    let mut db = Db::new()?;
    db.detect(ORDER, "signature_status")?;
    db.conn()?.execute_batch("UPDATE execution_failed_expense_tasks SET attempt_seq=0; DELETE FROM execution_failed_expense_cursor;")?;
    db.reopen()?;
    db.add("exec-canary:new-b", "new-b", "buy", db.now)?;
    db.detect("exec-canary:new-b", "receipt_meta")?;
    assert_eq!(
        db.store.take_failed_expense_tasks(ROUTE, 1, db.now)?[0].order_id,
        ORDER
    );
    db.reopen()?;
    assert_eq!(
        db.store.take_failed_expense_tasks(ROUTE, 1, db.now)?[0].order_id,
        "exec-canary:new-b"
    );
    assert_eq!(queue(&db)?.0, Some(3));
    Ok(())
}

#[test]
fn failed_expense_reservation_readback_rejects_silent_corruption() -> Result<()> {
    for detection in [true, false] {
        for (target, mutation) in [
            ("AFTER UPDATE OF attempt_seq ON execution_failed_expense_tasks",
             "UPDATE execution_failed_expense_tasks SET last_attempt_at='corrupted' WHERE order_id=NEW.order_id"),
            ("AFTER INSERT ON execution_failed_expense_cursor",
             "UPDATE execution_failed_expense_cursor SET sequence=0 WHERE route=NEW.route"),
            ("AFTER UPDATE ON execution_failed_expense_cursor WHEN NEW.sequence>0",
             "UPDATE execution_failed_expense_cursor SET sequence=0 WHERE route=NEW.route"),
        ] {
            let mut db = Db::new()?;
            // A previously pending task ensures cursor UPSERT takes its UPDATE path.
            // Fresh detection is then tested on a new B in the same route.
            db.detect(ORDER, "signature_status")?;
            let id = "exec-canary:readback-b";
            db.add(id, "readback-b", "sell", db.now)?;
            if !detection { db.detect(id, "receipt_meta")?; }
            if !detection && target.starts_with("AFTER INSERT") { continue; }
            // AFTER INSERT only applies to a previously absent route cursor.
            if target.starts_with("AFTER INSERT") {
                db.conn()?.execute_batch("DELETE FROM execution_failed_expense_cursor; UPDATE execution_failed_expense_tasks SET attempt_seq=0;")?;
            }
            let before = queue(&db)?;
            let status = db.store.load_execution_canary_order(id)?.unwrap().status;
            db.conn()?.execute_batch(&format!("CREATE TRIGGER corrupt_reservation {target} BEGIN {mutation}; END;"))?;
            let result = if detection { db.detect(id, "receipt_meta").map(|_| ()) }
                else { db.store.take_failed_expense_tasks(ROUTE, 2, db.now).map(|_| ()) };
            assert!(format!("{:#}", result.unwrap_err()).contains("readback"));
            db.reopen()?;
            assert_eq!(queue(&db)?, before);
            assert_eq!(db.store.load_execution_canary_order(id)?.unwrap().status, status);
            db.conn()?.execute_batch("DROP TRIGGER corrupt_reservation")?;
            if detection { db.detect(id, "receipt_meta")?; }
            else { assert_eq!(db.store.take_failed_expense_tasks(ROUTE, 2, db.now)?.len(), 2); }
        }
    }
    Ok(())
}
