mod quote_portfolio_receipt_support;
mod quote_portfolio_report_support;
use quote_portfolio_receipt_support as r;
use quote_portfolio_report_support as cli;
use serde_json::json;

#[test]
fn mismatched_and_unavailable_references_never_fall_back_to_supplied_fee() {
    let db = r::Db::new("receipt-ref-errors");
    r::combined(&db);
    db.pay(Some(5), false);
    for field in [
        "amount",
        "missing_amount",
        "unknown_amount",
        "order_id",
        "tx_signature",
        "wallet",
        "payer",
        "operation_at",
        "recorded_at",
        "invalid_time",
        "out_of_window",
    ] {
        let mut input = r::input(&db.id, 5);
        let action = &mut input["events"][0]["action"];
        match field {
            "amount" => action["amount"] = cli::amount(6),
            "missing_amount" => action["amount"] = json!(null),
            "unknown_amount" => action["amount"]["value"] = json!({"unknown":"not supplied"}),
            "order_id" => action["receipt_ref"][field] = json!("exec-canary:missing"),
            "tx_signature" => action["receipt_ref"][field] = json!(format!("{}2", "1".repeat(63))),
            "wallet" | "payer" => {
                action["receipt_ref"]["wallet"] = json!(cli::mint(17));
                action["receipt_ref"]["payer"] = json!(cli::mint(17));
            }
            "operation_at" | "recorded_at" => {
                action["receipt_ref"][field] = json!("2026-06-02T12:00:00.000000001Z")
            }
            "invalid_time" => action["receipt_ref"]["recorded_at"] = json!("invalid"),
            "out_of_window" => input["window"]["start_unix_ms"] = json!((cli::MS + 1).to_string()),
            _ => unreachable!(),
        }
        let v = r::pair_path(&db.freeze(), field, &input);
        r::refused(&v, 0, 100);
        assert_eq!(v["events"][1]["outcome"]["disposition"]["state"], "refused");
    }
}

#[test]
fn structural_modes_and_relabelled_payment_fail_before_any_partial_book() {
    let db = r::Db::new("receipt-structural");
    db.pay(Some(5), false);
    for case in [
        "mixed",
        "mixed_first_unresolved",
        "third_party",
        "multi_wallet",
        "id",
        "position",
        "order",
        "time",
        "amount",
        "unknown_field",
        "canonical",
        "overflow",
        "limit",
    ] {
        let first = r::expense(&db.id, 5);
        let mut second = first.clone();
        match case {
            "mixed" | "mixed_first_unresolved" => {
                second["action"]
                    .as_object_mut()
                    .unwrap()
                    .remove("receipt_ref");
            }
            "third_party" => second["action"]["receipt_ref"]["payer"] = json!(cli::mint(17)),
            "multi_wallet" => {
                second["action"]["receipt_ref"]["wallet"] = json!(cli::mint(17));
                second["action"]["receipt_ref"]["payer"] = json!(cli::mint(17));
            }
            "id" => second["id"] = json!("relabelled"),
            "position" => second["position_id"] = json!("relabelled"),
            "order" => second["action"]["receipt_ref"]["order_id"] = json!("exec-canary:other"),
            "time" => second["sequence"] = json!("3"),
            "amount" => second["action"]["amount"] = cli::amount(6),
            "unknown_field" => second["action"]["receipt_ref"]["extra"] = json!(true),
            "canonical" => second["action"]["amount"]["value"] = json!({"known":"05"}),
            "overflow" => {
                second["action"]["amount"]["value"] = json!({"known":"18446744073709551616"})
            }
            "limit" => {}
            _ => unreachable!(),
        }
        let mut events = vec![first, second];
        if case == "limit" {
            events = vec![events[0].clone(); 257];
        }
        if case == "mixed_first_unresolved" {
            events[0]["action"]["receipt_ref"]["order_id"] = json!("missing");
        }
        let v = r::pair_path(&db.freeze(), case, &cli::scenario(100, 0, events));
        assert_eq!(v["status"], "unavailable", "{case}: {v}");
        assert!(v.get("book").is_none());
        assert!(v["dataset_coverage"]["unknown"].is_string());
        assert_eq!(v["production_green"], false);
    }
}

#[test]
fn pending_missing_schema_and_conflicting_db_evidence_remain_unknown() {
    for case in [
        "pending",
        "fee_missing",
        "conflict",
        "success_collision",
        "ledger_missing",
        "schema_missing",
        "corrupt_facts",
        "db_payer",
        "signature_collision",
        "future_recorded",
        "recorded_before_operation",
    ] {
        let db = r::Db::new(&format!("receipt-db-{case}"));
        r::combined(&db);
        if case != "pending" {
            db.pay(if case == "fee_missing" { None } else { Some(5) }, false);
        }
        let mut input = r::input(&db.id, 5);
        match case {
            "conflict" => db
                .store
                .reject_failed_expense(&db.id, "synthetic_conflict")
                .unwrap(),
            "success_collision" => {
                db.conn()
                    .execute(
                        "INSERT INTO fills(order_id,token,qty,avg_price) VALUES(?1,?2,1,1)",
                        rusqlite::params![db.id, r::WALLET],
                    )
                    .unwrap();
            }
            "ledger_missing" => {
                db.conn()
                    .execute("DELETE FROM execution_failed_expense_ledger", [])
                    .unwrap();
            }
            "schema_missing" => db
                .conn()
                .execute_batch("DROP TABLE execution_failed_expense_ledger")
                .unwrap(),
            "corrupt_facts" => {
                db.conn()
                    .execute(
                        "UPDATE execution_failed_expense_facts SET facts_json='invalid'",
                        [],
                    )
                    .unwrap();
            }
            "db_payer" => {
                let mut facts = db.facts(r::SIGNATURE, Some(5), false);
                facts.payer = Some(cli::mint(17));
                db.conn()
                    .execute(
                        "UPDATE execution_failed_expense_facts SET facts_json=?1",
                        [serde_json::to_string(&facts).unwrap()],
                    )
                    .unwrap();
            }
            "signature_collision" => {
                db.seed("collision", r::SIGNATURE);
            }
            "future_recorded" | "recorded_before_operation" => {
                let t = if case == "future_recorded" {
                    "2026-06-02T12:00:00.002000001Z"
                } else {
                    "2026-06-02T11:59:59.999999999Z"
                };
                db.conn()
                    .execute(
                        "UPDATE execution_failed_expense_ledger SET recorded_at=?1",
                        [t],
                    )
                    .unwrap();
                input["events"][0]["action"]["receipt_ref"]["recorded_at"] = json!(t);
            }
            _ => {}
        }
        // A legacy outer report can also fail on damaged schema; portfolio binding
        // must still expose the same Unknown outcome in both actual CLIs.
        let v = r::pair(&db, case, &input);
        r::refused(&v, 0, 100);
        assert_eq!(v["events"][1]["outcome"]["disposition"]["state"], "refused");
    }
}
