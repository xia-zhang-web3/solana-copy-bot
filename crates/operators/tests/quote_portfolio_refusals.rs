mod quote_portfolio_support;
use copybot_operators::quote_portfolio::*;
use quote_portfolio_support::*;

#[test]
fn unknown_or_nonzero_initial_history_is_not_empty() {
    for inv in [
        Knowledge::Known(1),
        Knowledge::Unknown("history absent".into()),
    ] {
        let mut i = initial(100, 1);
        i.inventory_raw = inv;
        assert_eq!(
            Portfolio::new(i).unwrap_err(),
            Refusal::UnsupportedInitialHistory
        );
    }
    for flow in [
        Knowledge::Known(1),
        Knowledge::Known(-1),
        Knowledge::Unknown("flows absent".into()),
    ] {
        let mut i = initial(100, 1);
        i.external_transfers_lamports = flow;
        assert_eq!(
            Portfolio::new(i).unwrap_err(),
            Refusal::UnsupportedInitialHistory
        );
    }
}

#[test]
fn conflicting_event_operands_and_order_cannot_change_book() {
    let mut p = portfolio(100, 2);
    let a = buy("a", "A", 1, 1, 10, 100);
    let out = applied(&mut p, a.clone());
    let mut conflict = a.clone();
    conflict.position_id = "B".into();
    refused(&mut p, conflict, Refusal::ConflictId);
    let mut conflict = a.clone();
    conflict.order.sequence = 2;
    refused(&mut p, conflict, Refusal::ConflictId);
    let mut conflict = a.clone();
    event_costs(&mut conflict).priority = amount(1);
    refused(&mut p, conflict, Refusal::ConflictId);
    let mut conflict = a.clone();
    let Knowledge::Known(q) = event_quote(&mut conflict) else {
        unreachable!()
    };
    q.output = Knowledge::Known(101);
    refused(&mut p, conflict, Refusal::ConflictId);
    assert_eq!(p.apply(a), out);
    assert_eq!(p.book().cash_lamports, 90);
    assert_eq!(p.book().open_slots, 1);
    assert_eq!(
        p.last_order(),
        Some(Order {
            sequence: 1,
            unix_ms: 1
        })
    );
}

#[test]
fn out_of_order_sequence_and_time_are_not_silently_sorted() {
    for (seq, time) in [(9, 11), (10, 11), (11, 9)] {
        let mut p = portfolio(100, 1);
        applied(&mut p, buy("a", "A", 10, 1, 10, 100));
        let mut s = sell("s", "A", seq, 1, 40, 5);
        s.order.unix_ms = time;
        let out = refused(&mut p, s.clone(), Refusal::OutOfOrder);
        assert_eq!(p.apply(s), out);
        assert_eq!(
            p.last_order(),
            Some(Order {
                sequence: 10,
                unix_ms: 10
            })
        );
        applied(&mut p, sell("new", "A", 11, 1, 40, 5));
        assert_eq!(p.book().cash_lamports, 95);
        assert!(matches!(
            p.valuation().net_change_lamports,
            Knowledge::Unknown(_)
        ));
    }
}

#[test]
fn invalid_ids_zero_raw_and_oversell_are_atomic_refusals() {
    for (id, pos) in [("", "A"), ("a", ""), ("a b", "A"), ("a", " A"), ("a", "é")] {
        let mut p = portfolio(100, 1);
        refused(
            &mut p,
            buy(id, pos, 1, 1, 10, 100),
            Refusal::InvalidIdentity,
        );
    }
    let mut p = portfolio(100, 1);
    refused(
        &mut p,
        buy(&"a".repeat(129), "A", 1, 1, 10, 100),
        Refusal::InvalidIdentity,
    );
    for (input, raw) in [(0, 100), (10, 0)] {
        let mut p = portfolio(100, 1);
        refused(&mut p, buy("a", "A", 1, 1, input, raw), Refusal::InvalidRaw);
    }
    for raw in [0, 101] {
        let mut p = portfolio(100, 1);
        applied(&mut p, buy("a", "A", 1, 1, 10, 100));
        refused(&mut p, sell("s", "A", 2, 1, raw, 5), Refusal::InvalidRaw);
    }
}

#[test]
fn exact_quote_bindings_are_checked_for_buy_sell_and_mark() {
    for action in ["buy", "sell", "mark"] {
        for mismatch in ["position", "mint", "decimals", "direction", "size"] {
            let mut p = portfolio(100, 2);
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
            let Knowledge::Known(q) = event_quote(&mut e) else {
                unreachable!()
            };
            match mismatch {
                "position" => q.position_id = "B".into(),
                "mint" => q.mint = [2; 32],
                "decimals" => q.decimals = 1,
                "direction" => {
                    q.direction = if action == "buy" {
                        Direction::Sell
                    } else {
                        Direction::Buy
                    }
                }
                "size" => q.input += 1,
                _ => unreachable!(),
            }
            refused(&mut p, e, Refusal::QuoteBinding);
        }
    }
}

#[test]
fn cost_scope_cannot_be_reused_for_another_event_position_or_direction() {
    for mismatch in ["event", "position", "direction", "exit"] {
        let mut p = portfolio(100, 1);
        let mut e = buy("e", "A", 1, 1, 10, 100);
        let c = event_costs(&mut e);
        match mismatch {
            "event" => c.event_id = "other".into(),
            "position" => c.position_id = "B".into(),
            "direction" => c.direction = Direction::Sell,
            "exit" => c.exit = amount(1),
            _ => unreachable!(),
        }
        refused(&mut p, e, Refusal::CostBinding);
    }
    let mut p = portfolio(100, 1);
    applied(&mut p, buy("a", "A", 1, 1, 10, 100));
    let mut e = sell("s", "A", 2, 1, 100, 20);
    event_costs(&mut e).setup = amount(1);
    refused(&mut p, e, Refusal::CostBinding);
}

#[test]
fn missing_position_duplicate_position_and_closed_identity_are_refused() {
    let mut p = portfolio(100, 2);
    refused(
        &mut p,
        sell("missing", "A", 1, 1, 1, 1),
        Refusal::PositionMissing,
    );
    applied(&mut p, buy("a", "A", 2, 1, 10, 100));
    refused(
        &mut p,
        buy("duplicate", "A", 3, 1, 1, 1),
        Refusal::PositionExists,
    );
    applied(&mut p, sell("full", "A", 4, 1, 100, 10));
    refused(&mut p, sell("closed", "A", 5, 1, 1, 1), Refusal::InvalidRaw);
    refused(
        &mut p,
        buy("reuse", "A", 6, 1, 1, 1),
        Refusal::PositionExists,
    );
}

#[test]
fn buy_debit_overflow_and_late_flow_overflow_do_not_partially_commit() {
    let mut p = portfolio(u64::MAX, 1);
    let mut e = buy("overflow", "A", 1, 1, u64::MAX, 1);
    event_costs(&mut e).base = amount(1);
    refused(&mut p, e, Refusal::Arithmetic);
    let mut p = portfolio(u64::MAX, 1);
    applied(&mut p, buy("a", "A", 1, 1, u64::MAX, 1));
    applied(&mut p, sell("s", "A", 2, 1, 1, u64::MAX));
    // Cash and slot staging happens before lifetime principal overflows.
    refused(&mut p, buy("b", "B", 3, 1, 1, 1), Refusal::Arithmetic);
    assert_eq!(p.book().cash_lamports, u64::MAX);
    assert!(!p.book().positions.contains_key("B"));
}

#[test]
fn sell_cash_overflow_underflow_and_expense_overflow_are_atomic() {
    let mut p = portfolio(u64::MAX, 1);
    applied(&mut p, buy("a", "A", 1, 1, 1, 100));
    refused(
        &mut p,
        sell("s", "A", 2, 1, 40, u64::MAX),
        Refusal::Arithmetic,
    );
    let mut p = portfolio(1, 1);
    applied(&mut p, buy("a", "A", 1, 1, 1, 100));
    let mut e = sell("s", "A", 2, 1, 40, 0);
    event_costs(&mut e).exit = amount(1);
    refused(&mut p, e, Refusal::Arithmetic);
    let mut e = sell("s2", "A", 3, 1, 40, 0);
    let c = event_costs(&mut e);
    c.base = amount(u64::MAX);
    c.priority = amount(1);
    refused(&mut p, e, Refusal::Arithmetic);
    assert_eq!(p.book().positions["A"].allocated, Components::default());
}

#[test]
fn net_sell_can_fit_even_when_cash_plus_gross_exceeds_u64() {
    let mut p = portfolio(u64::MAX, 1);
    applied(&mut p, buy("a", "A", 1, 1, 1, 1));
    let mut e = sell("s", "A", 2, 1, 1, 2);
    event_costs(&mut e).base = amount(1);
    applied(&mut p, e);
    assert_eq!(p.book().cash_lamports, u64::MAX);
}

#[test]
fn refund_binding_and_cash_overflow_preserve_locked_rent() {
    let mut p = portfolio(u64::MAX, 1);
    let mut e = buy("a", "A", 1, 1, 1, 1);
    let Action::Buy { rent_deposit, .. } = &mut e.action else {
        unreachable!()
    };
    *rent_deposit = amount(1);
    applied(&mut p, e);
    applied(&mut p, sell("s", "A", 2, 1, 1, 2));
    refused(
        &mut p,
        refund("bad-binding", "A", 3, "other-deposit", 1),
        Refusal::RefundBinding,
    );
    refused(
        &mut p,
        refund("overflow", "A", 4, "a", 1),
        Refusal::Arithmetic,
    );
    assert_eq!(p.book().locked_rent_lamports, 1);
    assert_eq!(p.book().positions["A"].locked_rent_lamports, 1);
}
