mod quote_portfolio_support;
use copybot_operators::quote_portfolio::*;
use quote_portfolio_support::*;

#[test]
fn quantity_change_invalidates_mark100_and_exact_known_zero_marks60() {
    let mut p = portfolio(100, 1);
    applied(&mut p, buy("a", "A", 1, 1, 10, 100));
    let mut outcomes = vec![applied(&mut p, mark("m100", "A", 2, 1, 100, 20))];
    assert_eq!(p.valuation().full_equity_lamports, Knowledge::Known(110));
    outcomes.push(applied(&mut p, sell("s40", "A", 3, 1, 40, 5)));
    assert!(p.book().positions["A"].mark.is_none());
    assert_eq!(p.valuation().missing_marks, vec!["A"]);
    assert!(matches!(
        p.valuation().full_equity_lamports,
        Knowledge::Unknown(_)
    ));
    let before = p.book().flows.clone();
    outcomes.push(applied(&mut p, mark("m60-zero", "A", 4, 1, 60, 0)));
    assert_eq!(p.valuation().full_equity_lamports, Knowledge::Known(95));
    assert_eq!(p.valuation().known_net_marks_lamports, 0);
    assert_eq!(p.book().flows, before);
    assert_eq!(p.book().positions["A"].remaining_raw, 60);
    record("mark-quantity-known-zero", &p, &outcomes);
}

#[test]
fn residual_mark100_is_refused_after_partial_to60() {
    let mut p = portfolio(100, 1);
    applied(&mut p, buy("a", "A", 1, 1, 10, 100));
    applied(&mut p, sell("s40", "A", 2, 1, 40, 5));
    refused(
        &mut p,
        mark("wrong-size", "A", 3, 1, 100, 20),
        Refusal::QuoteBinding,
    );
    assert!(p.book().positions["A"].mark.is_none());
    assert_eq!(p.book().cash_lamports, 95);
}

#[test]
fn mixed_known_unknown_positions_keep_subtotal_and_independent_scope() {
    let mut p = portfolio(100, 2);
    applied(&mut p, buy("a", "A", 1, 1, 10, 100));
    applied(&mut p, buy("b", "B", 2, 2, 10, 100));
    applied(&mut p, mark("ma", "A", 3, 1, 100, 15));
    let v = p.valuation();
    assert_eq!(v.cash_subtotal_lamports, 80);
    assert_eq!(v.known_net_marks_lamports, 15);
    assert_eq!(v.missing_marks, vec!["B"]);
    assert!(matches!(v.full_equity_lamports, Knowledge::Unknown(_)));
    let out = applied(&mut p, mark("mb", "B", 4, 2, 100, 0));
    let v = p.valuation();
    assert_eq!(v.full_equity_lamports, Knowledge::Known(95));
    assert_eq!(
        v.scope,
        ValuationScope::IndependentExactQuotesAndRentBookValue
    );
    assert_eq!(v.basis, EstimateBasis::AssumedOrSyntheticOperands);
    record("mixed-marks", &p, &[out]);
}

#[test]
fn required_unknown_quote_or_output_never_uses_zero() {
    for action in ["buy", "sell", "mark"] {
        for output_only in [false, true] {
            let mut p = portfolio(100, 1);
            let mut e = if action == "buy" {
                buy("e", "A", 1, 1, 10, 100)
            } else {
                applied(&mut p, buy("a", "A", 1, 1, 10, 100));
                if action == "sell" {
                    sell("e", "A", 2, 1, 40, 5)
                } else {
                    mark("e", "A", 2, 1, 100, 5)
                }
            };
            if output_only {
                let Knowledge::Known(q) = event_quote(&mut e) else {
                    unreachable!()
                };
                q.output = Knowledge::Unknown("missing".into());
            } else {
                *event_quote(&mut e) = Knowledge::Unknown("missing".into());
            }
            refused(
                &mut p,
                e,
                Refusal::MissingOperand {
                    field: if output_only {
                        "quote output"
                    } else {
                        "exact quote"
                    },
                    reason: "missing".into(),
                },
            );
        }
    }
}

#[test]
fn each_unknown_required_cost_refuses_buy_sell_and_mark() {
    for action in ["buy", "sell", "mark"] {
        for field in ["base fee", "priority fee", "setup expense", "exit expense"] {
            let mut p = portfolio(100, 1);
            let mut e = if action == "buy" {
                buy("e", "A", 1, 1, 10, 100)
            } else {
                applied(&mut p, buy("a", "A", 1, 1, 10, 100));
                if action == "sell" {
                    sell("e", "A", 2, 1, 40, 5)
                } else {
                    mark("e", "A", 2, 1, 100, 5)
                }
            };
            let c = event_costs(&mut e);
            let cost = match field {
                "base fee" => &mut c.base,
                "priority fee" => &mut c.priority,
                "setup expense" => &mut c.setup,
                "exit expense" => &mut c.exit,
                _ => unreachable!(),
            };
            cost.amount = Knowledge::Unknown("missing".into());
            refused(
                &mut p,
                e,
                Refusal::MissingOperand {
                    field,
                    reason: "missing".into(),
                },
            );
        }
    }
}

#[test]
fn missing_rent_deposit_or_refund_is_not_known_zero() {
    let mut p = portfolio(100, 1);
    let mut e = buy("unknown-rent", "A", 1, 1, 10, 100);
    let Action::Buy { rent_deposit, .. } = &mut e.action else {
        unreachable!()
    };
    rent_deposit.amount = Knowledge::Unknown("missing".into());
    refused(
        &mut p,
        e,
        Refusal::MissingOperand {
            field: "rent deposit",
            reason: "missing".into(),
        },
    );
    applied(&mut p, buy("zero-rent", "A", 2, 1, 10, 100));
    assert_eq!(p.book().locked_rent_lamports, 0);
    let mut e = refund("refund-unknown", "A", 3, "zero-rent", 0);
    let Action::RentRefund { amount, .. } = &mut e.action else {
        unreachable!()
    };
    amount.amount = Knowledge::Unknown("missing".into());
    refused(
        &mut p,
        e,
        Refusal::MissingOperand {
            field: "rent refund",
            reason: "missing".into(),
        },
    );
    let out = applied(&mut p, refund("refund-zero", "A", 4, "zero-rent", 0));
    assert_eq!(out.before, out.after);
}

#[test]
fn unsupported_failed_expense_is_incomplete_replayed_and_never_double_charged() {
    let mut p = portfolio(100, 1);
    let e = Event {
        id: "failed-1".into(),
        position_id: "A".into(),
        order: Order {
            sequence: 1,
            unix_ms: 1,
        },
        action: Action::UnsupportedExpense {
            kind: "failed transaction base fee".into(),
            amount: amount(5),
        },
    };
    let out = refused(
        &mut p,
        e.clone(),
        Refusal::UnsupportedExpense("failed transaction base fee".into()),
    );
    assert_eq!(p.apply(e), out);
    assert_eq!(p.valuation().unresolved.len(), 1);
    let a = applied(&mut p, buy("a", "A", 2, 1, 10, 100));
    let s = applied(&mut p, sell("s", "A", 3, 1, 100, 20));
    assert_eq!(p.book().cash_lamports, 110);
    assert_eq!(p.book().flows.expenses, 0);
    assert!(matches!(
        p.valuation().full_equity_lamports,
        Knowledge::Unknown(_)
    ));
    assert!(matches!(
        p.valuation().net_change_lamports,
        Knowledge::Unknown(_)
    ));
    assert_eq!(p.valuation().cash_subtotal_lamports, 110);
    record("unsupported-failed-expense", &p, &[out, a, s]);
}

#[test]
fn assumed_costs_are_estimates_and_marks_do_not_charge_fees() {
    let mut p = portfolio(100, 1);
    let mut a = buy("a", "A", 1, 1, 10, 100);
    let c = event_costs(&mut a);
    c.base = amount(2);
    c.base.provenance = Provenance::Assumed("base assumption".into());
    c.priority = amount(3);
    c.setup = amount(4);
    applied(&mut p, a);
    let mut m = mark("m", "A", 2, 1, 100, 20);
    let c = event_costs(&mut m);
    c.base = amount(2);
    c.priority = amount(3);
    c.exit = amount(4);
    let before = p.book().flows.clone();
    applied(&mut p, m);
    assert_eq!(p.book().flows, before);
    assert_eq!(p.book().cash_lamports, 81);
    assert_eq!(p.valuation().known_net_marks_lamports, 11);
    assert_eq!(p.valuation().full_equity_lamports, Knowledge::Known(92));
    assert_eq!(
        p.valuation().basis,
        EstimateBasis::AssumedOrSyntheticOperands
    );
    let mut s = sell("s", "A", 3, 1, 100, 20);
    let c = event_costs(&mut s);
    c.base = amount(2);
    c.priority = amount(3);
    c.exit = amount(4);
    applied(&mut p, s);
    assert_eq!(p.book().cash_lamports, 92);
    assert_eq!(p.book().flows.expenses, 18);
    assert_eq!(p.valuation().net_change_lamports, Knowledge::Known(-8));
}

#[test]
fn negative_exact_net_mark_is_preserved_as_an_estimate() {
    let mut p = portfolio(100, 1);
    applied(&mut p, buy("a", "A", 1, 1, 10, 100));
    let mut m = mark("m", "A", 2, 1, 100, 0);
    event_costs(&mut m).exit = amount(5);
    applied(&mut p, m);
    assert_eq!(p.valuation().known_net_marks_lamports, -5);
    assert_eq!(p.valuation().full_equity_lamports, Knowledge::Known(85));
}

#[test]
fn missing_provenance_is_invalid_even_for_known_zero() {
    let mut p = portfolio(100, 1);
    let mut e = buy("a", "A", 1, 1, 10, 100);
    event_costs(&mut e).priority.provenance = Provenance::Observed(" ".into());
    refused(&mut p, e, Refusal::InvalidProvenance);
}
