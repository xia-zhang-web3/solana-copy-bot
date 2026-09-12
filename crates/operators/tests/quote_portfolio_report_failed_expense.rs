mod quote_portfolio_failed_expense_support;
mod quote_portfolio_report_support;
mod quote_portfolio_support;
use copybot_operators::quote_portfolio as k;
use quote_portfolio_failed_expense_support as fee;
use quote_portfolio_report_support as cli;
use quote_portfolio_support as kernel;
use serde_json::{json, Value};

#[test]
fn actual_clis_match_kernel_fee_matrix_and_replay_the_same_read_only_inputs() {
    let f = fee::fixture("failed-expense-matrix");
    for charge in [0, 5] {
        for size in [35, 40] {
            for marks in [false, true] {
                let label = format!("fee-{charge}-buy-{size}-marks-{marks}");
                let input = fee::matrix(charge, size, marks, false);
                let v = fee::pair(&f, &label, &input);
                assert_eq!(v["status"], "replayed");
                let mut p = kernel::portfolio(100, 2);
                let mut events = vec![
                    kernel::buy("a60", "A", 1, 1, 60, 100),
                    k::Event {
                        id: "fee".into(),
                        position_id: "portfolio-fee".into(),
                        order: k::Order {
                            sequence: 2,
                            unix_ms: 2,
                        },
                        action: k::Action::FailedAttemptExpense {
                            amount: kernel::amount(charge),
                        },
                    },
                    kernel::buy(&format!("b{size}"), "B", 3, 2, size, 100),
                ];
                if marks {
                    events.push(kernel::mark("ma", "A", 4, 1, 100, 60));
                    if charge + size <= 40 {
                        events.push(kernel::mark(&format!("mb{size}"), "B", 5, 2, 100, size));
                    }
                }
                let mut outcomes = vec![];
                for (index, event) in events.into_iter().enumerate() {
                    let out = p.apply(event);
                    compare(&v["events"][index]["outcome"], &out);
                    kernel::conservation(&p);
                    outcomes.push(out);
                }
                assert_eq!(v["book"]["positions"]["A"]["remaining_raw"], "100");
                assert_eq!(v["book"]["flows"]["expenses"], charge.to_string());
                fee::unknown(&v);
                let repeated = f.pair_path(
                    &format!("{label}-reopen"),
                    Some(&f.dir.join(format!("{label}.input.json"))),
                );
                assert_eq!(v, repeated);
                kernel::record(&format!("cli-{label}"), &p, &outcomes);
            }
        }
    }
}
fn compare(value: &Value, out: &k::Outcome) {
    let disposition = match &out.disposition {
        k::Disposition::Applied => json!({"state":"applied"}),
        k::Disposition::Skipped(reasons) => {
            assert_eq!(
                reasons,
                &vec![k::AdmissionReason::Cash {
                    available: 35,
                    required: 40
                }]
            );
            json!({"state":"skipped","reasons":[{"code":"cash","available":"35","required":"40"}]})
        }
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(value["disposition"], disposition);
    for (name, s) in [("before", &out.before), ("after", &out.after)] {
        assert_eq!(value[name]["cash_lamports"], s.cash_lamports.to_string());
        assert_eq!(value[name]["open_slots"], s.open_slots.to_string());
        assert_eq!(
            value[name]["locked_rent_lamports"],
            s.locked_rent_lamports.to_string()
        );
        for (field, amount) in [
            ("buy_principal", s.flows.buy_principal),
            ("sell_gross", s.flows.sell_gross),
            ("expenses", s.flows.expenses),
            ("rent_deposited", s.flows.rent_deposited),
            ("rent_refunded", s.flows.rent_refunded),
        ] {
            assert_eq!(value[name]["flows"][field], amount.to_string());
        }
        match &s.position {
            None => assert!(value[name]["position"].is_null()),
            Some(p) => {
                assert_eq!(
                    value[name]["position"]["remaining_raw"],
                    p.remaining_raw.to_string()
                );
                assert_eq!(
                    value[name]["position"]["entry"]["principal"],
                    p.entry.principal.to_string()
                );
                assert_eq!(
                    value[name]["position"]["remainder"]["principal"],
                    p.remainder.principal.to_string()
                );
            }
        }
    }
    for (field, expected) in [
        (
            "full_equity_lamports",
            &out.valuation_after.full_equity_lamports,
        ),
        (
            "net_change_lamports",
            &out.valuation_after.net_change_lamports,
        ),
    ] {
        match expected {
            k::Knowledge::Known(n) => assert_eq!(
                value["valuation_after"][field],
                json!({"known":n.to_string()})
            ),
            k::Knowledge::Unknown(_) => {
                assert!(value["valuation_after"][field]["unknown"].is_string())
            }
        }
    }
    assert_eq!(
        value["valuation_after"]["basis"],
        "assumed_or_synthetic_operands"
    );
}

#[test]
fn standalone_and_known_zero_origins_survive_both_cli_and_aggregate_basis() {
    let f = fee::fixture("failed-expense-origins");
    for charge in [0, 5] {
        for origin in ["observed", "assumed", "synthetic"] {
            let mut input = cli::scenario(100, 0, vec![fee::fee("fee", 1, cli::amount(charge))]);
            cli::observed(&mut input);
            input["events"][0]["action"]["amount"]["provenance"] =
                json!({origin:"explicit caller assertion"});
            let v = fee::pair(&f, &format!("{origin}-{charge}"), &input);
            assert_eq!(fee::state(&v, 0), "applied");
            assert_eq!(v["events"][0]["input"], input["events"][0]);
            assert_eq!(v["book"]["cash_lamports"], (100 - charge).to_string());
            assert_eq!(v["book"]["flows"]["expenses"], charge.to_string());
            assert_eq!(v["book"]["positions"], json!({}));
            assert_eq!(
                v["valuation"]["net_change_lamports"],
                json!({"known":(-i128::from(charge)).to_string()})
            );
            assert_eq!(
                v["valuation"]["basis"],
                if origin == "observed" {
                    "caller_observed_operands"
                } else {
                    "assumed_or_synthetic_operands"
                }
            );
            assert_eq!(
                v["book"]["kernel_assumed_or_synthetic"],
                origin != "observed"
            );
            fee::unknown(&v);
        }
    }
}

#[test]
fn unsupported_expense_remains_refused_and_plain_skip_does_not_create_fee() {
    let f = fee::fixture("failed-expense-old-controls");
    let v = fee::pair(&f, "legacy", &fee::matrix(5, 40, true, true));
    assert_eq!(fee::code(&v, 1), "unsupported_expense");
    assert_eq!(fee::state(&v, 2), "applied");
    assert_eq!(v["book"]["flows"]["expenses"], "0");
    assert!(v["valuation"]["net_change_lamports"]["unknown"].is_string());
    let input = cli::scenario(1, 2, vec![cli::buy("a60", "A", 1, 1, 60, 100)]);
    let v = fee::pair(&f, "skip-no-fee", &input);
    assert_eq!(fee::state(&v, 0), "skipped");
    assert_eq!(v["book"]["flows"]["expenses"], "0");
    assert_eq!(v["book"]["cash_lamports"], "1");
}
