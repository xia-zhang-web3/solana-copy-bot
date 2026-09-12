mod quote_portfolio_support;
use copybot_operators::quote_portfolio::*;
use quote_portfolio_support::*;

#[test]
fn successive_partial_allocations_final_exit_and_slot_reuse() {
    let mut p = portfolio(100, 1);
    let mut e = buy("a", "A", 1, 1, 11, 7);
    let c = event_costs(&mut e);
    c.base = amount(2);
    c.priority = amount(5);
    c.setup = amount(1);
    let mut outcomes = vec![applied(&mut p, e)];
    assert_eq!(p.book().cash_lamports, 81);
    let first = applied(&mut p, sell("s1", "A", 2, 1, 1, 3));
    assert_eq!(
        first.allocated_this_event,
        Components {
            principal: 2,
            base: 1,
            priority: 1,
            setup: 1,
            exit: 0
        }
    );
    assert_eq!(p.book().open_slots, 1);
    outcomes.push(first);
    let second = applied(&mut p, sell("s2", "A", 3, 1, 1, 3));
    assert_eq!(
        second.allocated_this_event,
        Components {
            principal: 2,
            base: 0,
            priority: 1,
            setup: 0,
            exit: 0
        }
    );
    assert_eq!(p.book().positions["A"].remaining_raw, 5);
    outcomes.push(second);
    let blocked = p.apply(buy("b-blocked", "B", 4, 2, 1, 1));
    assert_eq!(
        blocked.disposition,
        Disposition::Skipped(vec![AdmissionReason::PositionCap {
            open: 1,
            maximum: 1
        }])
    );
    assert_eq!(blocked.before, blocked.after);
    outcomes.push(blocked);
    let final_event = sell("s-final", "A", 5, 1, 5, 20);
    let final_out = applied(&mut p, final_event.clone());
    assert_eq!(
        final_out.allocated_this_event,
        Components {
            principal: 7,
            base: 1,
            priority: 3,
            setup: 0,
            exit: 0
        }
    );
    assert_eq!(p.book().positions["A"].remainder, Components::default());
    assert_eq!(
        p.book().positions["A"].allocated,
        p.book().positions["A"].entry
    );
    assert_eq!(p.book().open_slots, 0);
    assert_eq!(p.book().cash_lamports, 107);
    let book = p.book().clone();
    assert_eq!(p.apply(final_event), final_out);
    assert_eq!(*p.book(), book);
    outcomes.push(final_out);
    outcomes.push(applied(&mut p, buy("b-allowed", "B", 6, 2, 1, 1)));
    assert_eq!(p.book().open_slots, 1);
    assert_eq!(p.book().cash_lamports, 106);
    record("partial-final-slot", &p, &outcomes);
}

#[test]
fn rent_refund_is_explicit_bound_separate_and_replayed_once() {
    let mut p = portfolio(100, 1);
    let mut e = buy("a", "A", 1, 1, 10, 100);
    let Action::Buy { rent_deposit, .. } = &mut e.action else {
        unreachable!()
    };
    *rent_deposit = amount(20);
    let mut outcomes = vec![applied(&mut p, e)];
    outcomes.push(applied(&mut p, sell("s40", "A", 2, 1, 40, 5)));
    assert_eq!(p.book().cash_lamports, 75);
    assert_eq!(p.book().locked_rent_lamports, 20);
    outcomes.push(applied(&mut p, sell("s60", "A", 3, 1, 60, 5)));
    assert_eq!(p.book().cash_lamports, 80);
    assert_eq!(p.book().locked_rent_lamports, 20);
    assert_eq!(p.valuation().full_equity_lamports, Knowledge::Known(100));
    assert_eq!(p.book().flows.expenses, 0);
    let e = refund("refund-part", "A", 4, "a", 7);
    let out = applied(&mut p, e.clone());
    let book = p.book().clone();
    assert_eq!(p.apply(e), out);
    assert_eq!(*p.book(), book);
    outcomes.push(out);
    outcomes.push(applied(&mut p, refund("refund-rest", "A", 5, "a", 13)));
    assert_eq!(p.book().cash_lamports, 100);
    assert_eq!(p.book().locked_rent_lamports, 0);
    assert_eq!(p.book().flows.rent_refunded, 20);
    assert_eq!(p.book().flows.expenses, 0);
    record("explicit-rent-refund", &p, &outcomes);
    refused(
        &mut p,
        refund("extra-refund", "A", 6, "a", 1),
        Refusal::RentExceeded,
    );
}

#[test]
fn two_same_mint_positions_keep_separate_identity() {
    let mut p = portfolio(100, 2);
    applied(&mut p, buy("a", "A", 1, 9, 10, 100));
    applied(&mut p, buy("b", "B", 2, 9, 20, 100));
    applied(&mut p, sell("s", "A", 3, 9, 40, 7));
    assert_eq!(p.book().positions["A"].remaining_raw, 60);
    assert_eq!(p.book().positions["B"].remaining_raw, 100);
    assert_eq!(p.book().positions["A"].remainder.principal, 6);
    assert_eq!(p.book().positions["B"].remainder.principal, 20);
    assert_eq!(p.book().open_slots, 2);
    assert_eq!(p.book().cash_lamports, 77);
}

#[test]
fn buy_and_sell_replay_return_original_outcomes_even_after_later_events() {
    let mut p = portfolio(100, 1);
    let a = buy("a", "A", 1, 1, 10, 100);
    let out_a = applied(&mut p, a.clone());
    let s = sell("s", "A", 2, 1, 40, 5);
    let out_s = applied(&mut p, s.clone());
    let before = p.book().clone();
    assert_eq!(p.apply(a), out_a);
    assert_eq!(p.apply(s), out_s);
    assert_eq!(*p.book(), before);
    assert_eq!(
        p.last_order(),
        Some(Order {
            sequence: 2,
            unix_ms: 2
        })
    );
    conservation(&p);
}

#[test]
fn sell_net_debit_uses_cash_when_expenses_exceed_proceeds() {
    let mut p = portfolio(100, 1);
    applied(&mut p, buy("a", "A", 1, 1, 10, 100));
    let mut e = sell("s", "A", 2, 1, 100, 1);
    let c = event_costs(&mut e);
    c.base = amount(3);
    c.priority = amount(4);
    c.exit = amount(5);
    let out = applied(&mut p, e);
    assert_eq!(out.before.cash_lamports, 90);
    assert_eq!(out.after.cash_lamports, 79);
    assert_eq!(p.book().flows.expenses, 12);
    assert_eq!(p.book().open_slots, 0);
    assert_eq!(p.valuation().net_change_lamports, Knowledge::Known(-21));
}

#[test]
fn max_raw_allocation_uses_checked_wide_integer_intermediates() {
    let mut p = portfolio(u64::MAX, 1);
    let mut e = buy("a", "A", 1, 1, u64::MAX - 8, u64::MAX);
    let c = event_costs(&mut e);
    c.base = amount(3);
    c.priority = amount(5);
    applied(&mut p, e);
    let out = applied(&mut p, sell("s1", "A", 2, 1, u64::MAX - 1, 0));
    assert_eq!(
        out.allocated_this_event,
        Components {
            principal: u64::MAX - 8,
            base: 3,
            priority: 5,
            setup: 0,
            exit: 0
        }
    );
    assert_eq!(p.book().positions["A"].remaining_raw, 1);
    assert_eq!(p.book().positions["A"].remainder, Components::default());
    let out = applied(&mut p, sell("s2", "A", 3, 1, 1, 0));
    assert_eq!(out.allocated_this_event, Components::default());
    assert_eq!(p.book().cash_lamports, 0);
    assert_eq!(p.book().open_slots, 0);
}
