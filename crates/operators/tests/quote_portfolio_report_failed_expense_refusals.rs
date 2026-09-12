mod quote_portfolio_failed_expense_support;
mod quote_portfolio_report_support;
use quote_portfolio_failed_expense_support as fee;
use quote_portfolio_report_support as cli;
use serde_json::{json, Value};

#[test]
fn unknown_absent_or_underfunded_fee_blocks_following_buy_in_both_clis() {
    let f = fee::fixture("failed-expense-unknown");
    for case in ["unknown", "null", "absent", "cash", "zero-cash"] {
        let mut charge = cli::amount(5);
        if case == "unknown" {
            charge["value"] = json!({"unknown":"unmeasured failed landing"});
        }
        if case == "null" {
            charge = Value::Null;
        }
        if case == "cash" || case == "zero-cash" {
            charge = cli::amount(101);
        }
        let mut expense = fee::fee("fee", 1, charge);
        if case == "absent" {
            expense["action"].as_object_mut().unwrap().remove("amount");
        }
        let input = cli::scenario(
            if case == "zero-cash" { 0 } else { 100 },
            2,
            vec![expense, cli::buy("a60", "A", 2, 1, 60, 100)],
        );
        let v = fee::pair(&f, case, &input);
        assert_eq!(
            fee::code(&v, 0),
            if case.contains("cash") {
                "arithmetic"
            } else {
                "missing_operand"
            }
        );
        assert_eq!(fee::code(&v, 1), "cash_availability_unknown");
        assert_eq!(
            v["events"][1]["outcome"]["disposition"]["refusal"]["expense_event_id"],
            "fee"
        );
        assert_eq!(
            v["events"][0]["outcome"]["before"],
            v["events"][0]["outcome"]["after"]
        );
        assert_eq!(
            v["events"][1]["outcome"]["before"],
            v["events"][1]["outcome"]["after"]
        );
        assert_eq!(v["book"]["flows"]["expenses"], "0");
        assert_eq!(v["book"]["positions"], json!({}));
        assert!(v["valuation"]["full_equity_lamports"]["unknown"].is_string());
        fee::unknown(&v);
    }
}

#[test]
fn duplicate_fee_is_once_conflicting_operands_and_order_keep_existing_contract() {
    let f = fee::fixture("failed-expense-replay");
    let expense = fee::fee("fee", 1, cli::amount(5));
    let v = fee::pair(
        &f,
        "duplicate",
        &cli::scenario(100, 2, vec![expense.clone(), expense.clone()]),
    );
    assert_eq!(v["events"][0]["outcome"], v["events"][1]["outcome"]);
    assert_eq!(v["book"]["cash_lamports"], "95");
    assert_eq!(v["book"]["flows"]["expenses"], "5");
    for case in [
        "amount",
        "provenance",
        "order",
        "time",
        "position",
        "identity-provenance",
    ] {
        let mut changed = expense.clone();
        match case {
            "amount" => changed["action"]["amount"] = cli::amount(6),
            "provenance" => {
                changed["action"]["amount"]["provenance"] = json!({"assumed":"changed"})
            }
            "order" => changed["sequence"] = json!("2"),
            "time" => changed["unix_ms"] = json!((cli::MS + 2).to_string()),
            "position" => changed["position_id"] = json!("other"),
            "identity-provenance" => changed["identity_provenance"] = json!({"assumed":"changed"}),
            _ => unreachable!(),
        }
        let input = cli::scenario(
            100,
            2,
            vec![
                expense.clone(),
                changed,
                cli::buy("a60", "A", 3, 1, 60, 100),
            ],
        );
        let v = fee::pair(&f, case, &input);
        if case == "identity-provenance" {
            assert_eq!(v["status"], "unavailable");
            assert!(v["reason"]
                .as_str()
                .unwrap()
                .contains("caller event identity changed"));
        } else {
            assert_eq!(fee::code(&v, 1), "conflict_id");
            assert_eq!(fee::code(&v, 2), "cash_availability_unknown");
            assert_eq!(v["book"]["cash_lamports"], "95");
            assert_eq!(v["book"]["flows"]["expenses"], "5");
        }
    }
    let mut out_of_order = fee::fee("late", 1, cli::amount(5));
    out_of_order["unix_ms"] = json!((cli::MS + 2).to_string());
    let v = fee::pair(
        &f,
        "out-of-order",
        &cli::scenario(
            100,
            2,
            vec![expense, out_of_order, cli::buy("a60", "A", 3, 1, 60, 100)],
        ),
    );
    assert_eq!(fee::code(&v, 1), "out_of_order");
    assert_eq!(fee::code(&v, 2), "cash_availability_unknown");
}

#[test]
fn malformed_provenance_unknown_fields_and_amounts_are_never_silently_accepted() {
    let f = fee::fixture("failed-expense-malformed");
    for case in [
        "empty",
        "absent-provenance",
        "bad-origin",
        "unknown-field",
        "absent-value",
        "negative",
        "overflow",
        "leading-zero",
        "floating",
    ] {
        let mut e = fee::fee("fee", 1, cli::amount(0));
        match case {
            "empty" => e["action"]["amount"]["provenance"] = json!({"assumed":" "}),
            "absent-provenance" => {
                e["action"]["amount"]
                    .as_object_mut()
                    .unwrap()
                    .remove("provenance");
            }
            "bad-origin" => e["action"]["amount"]["provenance"] = json!({"verified":"no"}),
            "unknown-field" => e["action"]["charge_anyway"] = json!(true),
            "absent-value" => {
                e["action"]["amount"]
                    .as_object_mut()
                    .unwrap()
                    .remove("value");
            }
            "negative" => e["action"]["amount"]["value"] = json!({"known":"-1"}),
            "overflow" => e["action"]["amount"]["value"] = json!({"known":"18446744073709551616"}),
            "leading-zero" => e["action"]["amount"]["value"] = json!({"known":"00"}),
            "floating" => e["action"]["amount"]["value"] = json!({"known":0.0}),
            _ => unreachable!(),
        }
        let input = cli::scenario(100, 2, vec![e, cli::buy("a60", "A", 2, 1, 60, 100)]);
        let v = fee::pair(&f, case, &input);
        assert_eq!(v["status"], "unavailable");
        assert!(v.get("book").is_none());
        fee::unknown(&v);
    }
}

#[test]
fn arithmetic_accumulator_overflow_is_atomic_in_actual_clis() {
    let f = fee::fixture("failed-expense-overflow");
    let input = cli::scenario(
        u64::MAX,
        1,
        vec![
            cli::buy("unit", "A", 1, 1, 1, 1),
            fee::fee("max-less-one", 2, cli::amount(u64::MAX - 1)),
            cli::sell("ten", "A", 3, 1, 10),
            fee::fee("one", 4, cli::amount(1)),
            fee::fee("overflow", 5, cli::amount(1)),
            cli::buy("a60", "B", 6, 1, 60, 100),
        ],
    );
    let v = fee::pair(&f, "overflow", &input);
    assert_eq!(fee::code(&v, 4), "arithmetic");
    assert_eq!(
        v["events"][4]["outcome"]["before"],
        v["events"][4]["outcome"]["after"]
    );
    assert_eq!(v["book"]["cash_lamports"], "9");
    assert_eq!(v["book"]["flows"]["expenses"], u64::MAX.to_string());
    assert_eq!(fee::code(&v, 5), "cash_availability_unknown");
    assert!(v["valuation"]["net_change_lamports"]["unknown"].is_string());
}
