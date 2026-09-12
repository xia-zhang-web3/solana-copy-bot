mod quote_portfolio_receipt_support;
mod quote_portfolio_report_support;
mod quote_portfolio_support;
use copybot_operators::quote_portfolio as kernel;
use quote_portfolio_receipt_support as r;
use quote_portfolio_report_support as cli;
use quote_portfolio_support as k;
use serde_json::json;

#[test]
fn receipt_fee_alone_changes_exact_cash_and_next_buy_boundary_in_both_clis() {
    for fee in [0, 5] {
        let db = r::Db::new(&format!("receipt-matrix-{fee}"));
        r::combined(&db);
        db.pay(Some(fee), false);
        let mut events = vec![
            cli::buy("a60", "A", 1, 1, 60, 100),
            r::expense(&db.id, fee),
            cli::buy("b40", "B", 3, 2, 40, 100),
        ];
        events.push(cli::event("ma","A",4,json!({"kind":"mark","quote":cli::reference("ma","A",1,"sell",100,60),"costs":cli::costs("ma","A","sell")})));
        let input = cli::scenario(100, 2, events);
        let v = r::pair(&db, "matrix", &input);
        let mut p = k::portfolio(100, 2);
        let actions = vec![
            k::buy("a60", "A", 1, 1, 60, 100),
            kernel::Event {
                id: "fee".into(),
                position_id: "unrelated-virtual-position".into(),
                order: kernel::Order {
                    sequence: 2,
                    unix_ms: 2,
                },
                action: kernel::Action::FailedAttemptExpense {
                    amount: k::amount(fee),
                },
            },
            k::buy("b40", "B", 3, 2, 40, 100),
            k::mark("ma", "A", 4, 1, 100, 60),
        ];
        for (i, e) in actions.into_iter().enumerate() {
            let out = p.apply(e);
            k::conservation(&p);
            assert_eq!(
                v["events"][i]["outcome"]["after"]["cash_lamports"],
                out.after.cash_lamports.to_string()
            );
            assert_eq!(
                v["events"][i]["outcome"]["after"]["flows"]["expenses"],
                out.after.flows.expenses.to_string()
            );
        }
        assert_eq!(
            v["events"][2]["outcome"]["disposition"]["state"],
            if fee == 0 { "applied" } else { "skipped" }
        );
        assert_eq!(
            v["events"][1]["outcome"]["after"]["cash_lamports"],
            (40 - fee).to_string()
        );
        assert_eq!(v["book"]["positions"]["A"]["remaining_raw"], "100");
        assert_eq!(v["book"]["locked_rent_lamports"], "0");
        let binding = &v["events"][1]["source_binding"];
        assert_eq!(
            binding["validated_payment"]["wallet_fee_lamports"],
            fee.to_string()
        );
        assert!(binding["native_delta_lamports"].is_null());
        assert!(binding["unexplained_delta_lamports"].is_null());
        assert_eq!(binding["wallet_reconciliation_complete"], false);
        assert_eq!(v["valuation"]["basis"], "assumed_or_synthetic_operands");
        assert_eq!(v["production_green"], false);
    }
}

#[test]
fn exact_zero_and_utc_equal_instants_are_not_unknown() {
    let db = r::Db::new("receipt-equality");
    db.pay(Some(0), true);
    let mut event = r::expense(&db.id, 0);
    event["unix_ms"] = json!(cli::MS.to_string());
    event["action"]["receipt_ref"]["operation_at"] = json!("2026-06-02T14:00:00+02:00");
    let mut input = cli::scenario(100, 0, vec![event]);
    input["window"]["end_unix_ms"] = json!(cli::MS.to_string());
    let v = r::pair(&db, "equality", &input);
    assert_eq!(v["events"][0]["outcome"]["disposition"]["state"], "applied");
    assert_eq!(v["book"]["cash_lamports"], "100");
    assert_eq!(
        v["events"][0]["source_binding"]["validated_payment"]["recorded_at"],
        "2026-06-02T12:00:00.000000000Z"
    );
}

#[test]
fn exact_duplicate_has_historical_outcome_and_distinct_payments_debit_once_each() {
    let db = r::Db::new("receipt-duplicates-positive");
    db.pay(Some(5), false);
    let first = r::expense(&db.id, 5);
    let sig = format!("{}2", "1".repeat(63));
    let id = db.seed("receipt-b", &sig);
    db.store
        .apply_failed_expense(&id, &db.facts(&sig, Some(7), false), db.at)
        .unwrap();
    let mut second = r::expense(&id, 7);
    second["id"] = json!("second-fee");
    second["sequence"] = json!("3");
    second["unix_ms"] = json!((cli::MS + 3).to_string());
    second["action"]["receipt_ref"]["tx_signature"] = json!(sig);
    let input = cli::scenario(100, 0, vec![first.clone(), first, second]);
    let v = r::pair(&db, "duplicates-positive", &input);
    assert_eq!(v["book"]["cash_lamports"], "88");
    assert_eq!(v["book"]["flows"]["expenses"], "12");
    assert_eq!(v["events"][0]["outcome"], v["events"][1]["outcome"]);
    assert_eq!(v["input_coverage"]["kernel_events_complete"], true);
}

#[test]
fn synthetic_receipt_origin_controls_basis_even_when_other_assertions_are_observed() {
    let db = r::Db::new("receipt-origin");
    db.pay(Some(0), false);
    let mut input = cli::scenario(100, 0, vec![r::expense(&db.id, 0)]);
    cli::observed(&mut input);
    input["events"][0]["action"]["receipt_ref"]["source_provenance"] =
        json!({"synthetic":"fixture remains synthetic"});
    let v = r::pair(&db, "origin", &input);
    assert_eq!(v["assumed_or_synthetic"], true);
    assert_eq!(v["book"]["kernel_assumed_or_synthetic"], true);
    assert_eq!(v["valuation"]["basis"], "assumed_or_synthetic_operands");
}
