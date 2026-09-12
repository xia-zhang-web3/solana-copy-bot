#[path = "common/receipt_operand_fixture.rs"]
mod fixture;
use copybot_storage_core::SqliteStore;
use fixture::*;

#[test]
fn selected_reader_matches_existing_validator_with_exact_knowledge_time_and_reopen() {
    let db = Db::new("reader-known");
    let recorded = db.at + chrono::Duration::nanoseconds(1);
    db.store
        .apply_failed_expense(&db.id, &db.facts(SIGNATURE, Some(5), false), recorded)
        .unwrap();
    let path = db.freeze();
    let before = std::fs::read(&path).unwrap();
    for _ in 0..2 {
        let s = SqliteStore::open_read_only(&path).unwrap();
        let (row, time) = s.failed_expense_receipt_operand(&db.id).unwrap();
        let report = s
            .execution_failed_expense_report(db.at, db.at + chrono::Duration::seconds(1), 1)
            .unwrap();
        assert_eq!(row, report.rows[0]);
        assert_eq!(time.as_deref(), Some("2026-06-02T12:00:00.000000001+00:00"));
        assert_eq!(row.wallet_fee_lamports.as_deref(), Some("5"));
        assert_eq!(row.task.unwrap().status, "pending");
        assert_eq!(row.native_delta_lamports, None);
        assert_eq!(report.coverage, "partial_unresolved");
        assert_eq!(report.cohort_wallet_fee_lamports, None);
    }
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

#[test]
fn selected_reader_does_not_scan_other_orders_or_promote_receiptless_fee() {
    let db = Db::new("reader-bounded");
    let (pending, time) = db.store.failed_expense_receipt_operand(&db.id).unwrap();
    assert!(pending.wallet_fee_lamports.is_none() && time.is_none());
    db.pay(Some(0), true);
    let sig = format!("{}2", "1".repeat(63));
    let other = db.seed("other-corrupt", &sig);
    db.conn()
        .execute(
            "INSERT INTO execution_failed_expense_facts(order_id,facts_json) VALUES(?1,'invalid')",
            [other],
        )
        .unwrap();
    let (row, _) = db.store.failed_expense_receipt_operand(&db.id).unwrap();
    assert_eq!(row.wallet_fee_lamports.as_deref(), Some("0"));
    assert!(db
        .store
        .execution_failed_expense_report(db.at, db.at + chrono::Duration::seconds(1), 1)
        .is_err());
    assert!(db
        .store
        .failed_expense_receipt_operand("exec-canary:missing")
        .is_err());
}

#[test]
fn selected_reader_reuses_task_fact_and_success_validators() {
    for case in [
        "task_wallet",
        "task_signature",
        "task_time",
        "facts_wallet",
        "facts_signature",
        "facts_slot",
        "facts_error",
        "ledger_fee",
        "ledger_payer",
        "ledger_signature",
        "conflict",
        "success",
        "missing_schema",
    ] {
        let db = Db::new(&format!("reader-corrupt-{case}"));
        db.pay(Some(5), false);
        let c = db.conn();
        match case {
            "task_wallet" => {
                c.execute(
                    "UPDATE execution_failed_expense_tasks SET wallet='other'",
                    [],
                )
                .unwrap();
            }
            "task_signature" => {
                c.execute(
                    "UPDATE execution_failed_expense_tasks SET tx_signature='other'",
                    [],
                )
                .unwrap();
            }
            "task_time" => {
                c.execute(
                    "UPDATE execution_failed_expense_tasks SET operation_at='2026-06-02T12:00:01Z'",
                    [],
                )
                .unwrap();
            }
            "ledger_fee" => {
                c.execute(
                    "UPDATE execution_failed_expense_ledger SET wallet_fee_lamports='6'",
                    [],
                )
                .unwrap();
            }
            "ledger_payer" => {
                c.execute(
                    "UPDATE execution_failed_expense_ledger SET payer='other'",
                    [],
                )
                .unwrap();
            }
            "ledger_signature" => {
                c.execute(
                    "UPDATE execution_failed_expense_ledger SET tx_signature='other'",
                    [],
                )
                .unwrap();
            }
            "conflict" => db.store.reject_failed_expense(&db.id, "conflict").unwrap(),
            "success" => {
                c.execute(
                    "INSERT INTO fills(order_id,token,qty) VALUES(?1,?2,1)",
                    rusqlite::params![db.id, WALLET],
                )
                .unwrap();
            }
            "missing_schema" => c
                .execute_batch("DROP TABLE execution_failed_expense_ledger")
                .unwrap(),
            _ => {
                let mut f = db.facts(SIGNATURE, Some(5), false);
                match case {
                    "facts_wallet" => f.wallet = "other".into(),
                    "facts_signature" => f.tx_signature = "other".into(),
                    "facts_slot" => f.slot = 43,
                    "facts_error" => {
                        f.transaction_error =
                            serde_json::json!({"InstructionError":[0,{"Custom":126}]})
                    }
                    _ => unreachable!(),
                }
                c.execute(
                    "UPDATE execution_failed_expense_facts SET facts_json=?1",
                    [serde_json::to_string(&f).unwrap()],
                )
                .unwrap();
            }
        }
        let result = db.store.failed_expense_receipt_operand(&db.id);
        assert!(
            result.is_err() || result.unwrap().0.wallet_fee_lamports.is_none(),
            "{case}"
        );
    }
}

#[test]
fn corrupted_ledger_without_unique_constraints_cannot_repeat_payment() {
    for same_order in [false, true] {
        let db = Db::new(&format!("reader-duplicate-ledger-{same_order}"));
        db.pay(Some(5), false);
        let c = db.conn();
        c.execute_batch("CREATE TABLE duplicate_ledger AS SELECT * FROM execution_failed_expense_ledger;
            DROP TABLE execution_failed_expense_ledger;
            CREATE TABLE execution_failed_expense_ledger AS SELECT * FROM duplicate_ledger;
            DROP TABLE duplicate_ledger;
            INSERT INTO execution_failed_expense_ledger SELECT * FROM execution_failed_expense_ledger;").unwrap();
        if !same_order {
            c.execute("UPDATE execution_failed_expense_ledger SET order_id='different-order' WHERE rowid=2",[]).unwrap();
        }
        assert!(db
            .store
            .failed_expense_receipt_operand(&db.id)
            .unwrap_err()
            .to_string()
            .contains("not unique"));
    }
}
