mod quote_portfolio_support;
use copybot_operators::quote_portfolio::*;
use quote_portfolio_support::*;

#[test]
fn four_actual_public_transition_arms_match_oracle74() {
    for (cash, cap, final_cash, reason_count) in [
        (1_000_000_000, 1, 710_429_710, 2),
        (1_000_000_000, 2, 710_429_710, 1),
        (2_000_000_000, 1, 1_710_429_710, 1),
        (2_000_000_000, 2, 1_108_368_427, 0),
    ] {
        let mut p = portfolio(cash, cap);
        let mut outcomes = Vec::new();
        for (id, pos, seq, mint) in [("buy-a", "A", 1, 1), ("buy-b", "B", 2, 2)] {
            let mut e = buy(id, pos, seq, mint, 600_000_000, 100);
            let Action::Buy {
                costs,
                rent_deposit,
                ..
            } = &mut e.action
            else {
                unreachable!()
            };
            costs.base = amount(5_000);
            costs.priority = amount(7_003);
            costs.setup = amount(10_000);
            *rent_deposit = amount(2_039_280);
            let out = p.apply(e);
            if pos == "A" || reason_count == 0 {
                assert_eq!(out.disposition, Disposition::Applied);
                assert_eq!(
                    out.before.cash_lamports - out.after.cash_lamports,
                    602_061_283
                );
            } else {
                let mut reasons = Vec::new();
                if cash == 1_000_000_000 {
                    reasons.push(AdmissionReason::Cash {
                        available: 397_938_717,
                        required: 602_061_283,
                    });
                }
                if cap == 1 {
                    reasons.push(AdmissionReason::PositionCap {
                        open: 1,
                        maximum: 1,
                    });
                }
                assert_eq!(reasons.len(), reason_count);
                assert_eq!(out.disposition, Disposition::Skipped(reasons));
                assert_eq!(out.before, out.after);
                assert!(!p.book().positions.contains_key("B"));
                assert!(p.valuation().unresolved.is_empty());
            }
            conservation(&p);
            outcomes.push(out);
        }
        let mut e = sell("sell-a40", "A", 3, 1, 40, 312_500_000);
        let c = event_costs(&mut e);
        c.base = amount(5_000);
        c.priority = amount(3_007);
        c.exit = amount(1_000);
        let out = applied(&mut p, e);
        assert_eq!(
            out.after.cash_lamports - out.before.cash_lamports,
            312_490_993
        );
        assert_eq!(
            out.allocated_this_event,
            Components {
                principal: 240_000_000,
                base: 2_000,
                priority: 2_802,
                setup: 4_000,
                exit: 0
            }
        );
        let a = &p.book().positions["A"];
        assert_eq!(a.remaining_raw, 60);
        assert_eq!(a.remainder.priority, 4_201);
        assert_eq!(a.locked_rent_lamports, 2_039_280);
        assert_eq!(p.book().cash_lamports, final_cash);
        assert_eq!(p.book().open_slots, if reason_count == 0 { 2 } else { 1 });
        assert!(matches!(
            p.valuation().full_equity_lamports,
            Knowledge::Unknown(_)
        ));
        outcomes.push(out);
        record(&format!("oracle-{cash}-{cap}"), &p, &outcomes);
    }
}

#[test]
fn exact_cash_boundary_and_one_lamport_short() {
    for cash in [18, 19] {
        let mut p = portfolio(cash, 1);
        let mut e = buy("a", "A", 1, 1, 11, 7);
        let Action::Buy {
            costs,
            rent_deposit,
            ..
        } = &mut e.action
        else {
            unreachable!()
        };
        costs.base = amount(2);
        costs.priority = amount(3);
        costs.setup = amount(1);
        *rent_deposit = amount(2);
        let out = p.apply(e);
        if cash == 19 {
            assert_eq!(out.disposition, Disposition::Applied);
            assert_eq!(p.book().cash_lamports, 0);
            assert_eq!(p.book().open_slots, 1);
        } else {
            assert_eq!(
                out.disposition,
                Disposition::Skipped(vec![AdmissionReason::Cash {
                    available: 18,
                    required: 19
                }])
            );
            assert_eq!(out.before, out.after);
            assert_eq!(p.book().open_slots, 0);
        }
        conservation(&p);
        record(&format!("cash-boundary-{cash}"), &p, &[out]);
    }
}
