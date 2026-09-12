mod quote_portfolio_support;
use copybot_operators::quote_portfolio::*;
use quote_portfolio_support::*;

fn fee(id: &str, seq: u64, n: u64) -> Event {
    Event {
        id: id.into(),
        position_id: "portfolio-fee".into(),
        order: Order {
            sequence: seq,
            unix_ms: seq,
        },
        action: Action::FailedAttemptExpense { amount: amount(n) },
    }
}

fn operand(e: &mut Event) -> &mut Lamports {
    let Action::FailedAttemptExpense { amount } = &mut e.action else {
        unreachable!()
    };
    amount
}

#[test]
fn fee_before_buy_matrix_preserves_positions_and_exact_valuation() {
    for charge in [0, 5] {
        for size in [35, 40] {
            let mut p = portfolio(100, 2);
            let a = applied(&mut p, buy("a", "A", 1, 1, 60, 100));
            assert_eq!((a.before.cash_lamports, a.after.cash_lamports), (100, 40));
            let before = p.book().clone();
            let expense = applied(&mut p, fee("fee", 2, charge));
            assert_eq!(expense.before.cash_lamports, 40);
            assert_eq!(expense.after.cash_lamports, 40 - charge);
            assert_eq!(p.book().positions, before.positions);
            assert_eq!(p.book().open_slots, 1);
            assert_eq!(p.book().locked_rent_lamports, 0);
            assert_eq!(p.book().flows.buy_principal, 60);
            assert_eq!(p.book().flows.expenses, charge);
            assert_eq!(expense.allocated_this_event, Components::default());
            let b = p.apply(buy("b", "B", 3, 2, size, 100));
            let admitted = charge + size <= 40;
            if admitted {
                assert_eq!(b.disposition, Disposition::Applied);
                assert_eq!(b.after.cash_lamports, 40 - charge - size);
            } else {
                assert_eq!(
                    b.disposition,
                    Disposition::Skipped(vec![AdmissionReason::Cash {
                        available: 35,
                        required: 40,
                    }])
                );
                assert_eq!(b.before, b.after);
            }
            conservation(&p);
            assert_eq!(p.book().positions["A"], before.positions["A"]);
            assert!(matches!(
                p.valuation().full_equity_lamports,
                Knowledge::Unknown(_)
            ));
            applied(&mut p, mark("ma", "A", 4, 1, 100, 60));
            if admitted {
                assert!(matches!(
                    p.valuation().full_equity_lamports,
                    Knowledge::Unknown(_)
                ));
                applied(&mut p, mark("mb", "B", 5, 2, 100, size));
            }
            assert_eq!(
                p.valuation().full_equity_lamports,
                Knowledge::Known(100 - i128::from(charge))
            );
            assert_eq!(
                p.valuation().net_change_lamports,
                Knowledge::Known(-i128::from(charge))
            );
            record(&format!("fee-{charge}-buy-{size}"), &p, &[a, expense, b]);
        }
    }
}

#[test]
fn fee_without_positions_and_zero_retain_all_origins_and_signed_net() {
    for charge in [0, 5] {
        for provenance in [
            Provenance::Observed("caller assertion".into()),
            Provenance::Assumed("failed landing scenario".into()),
            Provenance::Synthetic("fixture".into()),
        ] {
            let mut p = portfolio(100, 0);
            let mut e = fee("fee", 1, charge);
            operand(&mut e).provenance = provenance.clone();
            let out = applied(&mut p, e.clone());
            assert_eq!(out.event, e);
            assert_eq!(p.book().cash_lamports, 100 - charge);
            assert_eq!(p.book().flows.expenses, charge);
            assert!(p.book().positions.is_empty());
            assert_eq!((p.book().open_slots, p.book().locked_rent_lamports), (0, 0));
            assert_eq!(
                p.valuation().net_change_lamports,
                Knowledge::Known(-i128::from(charge))
            );
            assert_eq!(
                p.valuation().basis,
                if matches!(provenance, Provenance::Observed(_)) {
                    EstimateBasis::CallerObservedOperands
                } else {
                    EstimateBasis::AssumedOrSyntheticOperands
                }
            );
        }
    }
}

#[test]
fn fee_does_not_change_existing_mark_rent_or_partial_entry_allocation() {
    let mut p = portfolio(100, 2);
    let mut a = buy("a", "A", 1, 1, 60, 100);
    event_costs(&mut a).priority = amount(7);
    if let Action::Buy { rent_deposit, .. } = &mut a.action {
        *rent_deposit = amount(2);
    }
    applied(&mut p, a);
    applied(&mut p, sell("s", "A", 2, 1, 40, 24));
    applied(&mut p, mark("m", "A", 3, 1, 60, 36));
    let before = p.book().clone();
    let net_before = p.valuation().net_change_lamports;
    let mut e = fee("fee", 4, 5);
    e.position_id = "A".into();
    applied(&mut p, e);
    assert_eq!(p.book().positions, before.positions);
    assert_eq!(p.book().locked_rent_lamports, 2);
    assert_eq!(p.book().flows.buy_principal, before.flows.buy_principal);
    assert_eq!(p.book().flows.expenses, before.flows.expenses + 5);
    assert_eq!(net_before, Knowledge::Known(-7));
    assert_eq!(p.valuation().net_change_lamports, Knowledge::Known(-12));
}

#[test]
fn exact_duplicate_is_historical_and_every_conflict_blocks_new_buy() {
    for change in [
        "amount",
        "provenance",
        "sequence",
        "time",
        "action",
        "position",
    ] {
        let mut p = portfolio(100, 2);
        let e = fee("fee", 1, 5);
        let out = applied(&mut p, e.clone());
        applied(&mut p, buy("a", "A", 2, 1, 10, 100));
        assert_eq!(p.apply(e.clone()), out);
        assert_eq!(p.book().cash_lamports, 85);
        let mut changed = e.clone();
        match change {
            "amount" => operand(&mut changed).amount = Knowledge::Known(6),
            "provenance" => operand(&mut changed).provenance = Provenance::Assumed("new".into()),
            "sequence" => changed.order.sequence = 3,
            "time" => changed.order.unix_ms = 3,
            "position" => changed.position_id = "B".into(),
            "action" => changed.action = buy("fee", "B", 3, 2, 1, 1).action,
            _ => unreachable!(),
        }
        refused(&mut p, changed, Refusal::ConflictId);
        refused(
            &mut p,
            buy("b", "B", 3, 2, 1, 1),
            Refusal::CashAvailabilityUnknown {
                expense_event_id: "fee".into(),
            },
        );
        assert_eq!(p.apply(e), out);
        assert_eq!(p.book().flows.expenses, 5);
    }
}

#[test]
fn unknown_or_unreliable_fee_refuses_atomically_and_taints_cash_admission() {
    for case in ["unknown", "provenance", "cash", "order", "identity"] {
        let mut p = portfolio(100, 2);
        applied(&mut p, buy("a", "A", 1, 1, 60, 100));
        let mut e = fee("fee", 2, 5);
        let reason = match case {
            "unknown" => {
                operand(&mut e).amount = Knowledge::Unknown("unmeasured".into());
                Refusal::MissingOperand {
                    field: "failed attempt expense",
                    reason: "unmeasured".into(),
                }
            }
            "provenance" => {
                operand(&mut e).provenance = Provenance::Synthetic(" ".into());
                Refusal::InvalidProvenance
            }
            "cash" => {
                operand(&mut e).amount = Knowledge::Known(41);
                Refusal::Arithmetic
            }
            "order" => {
                e.order.sequence = 1;
                Refusal::OutOfOrder
            }
            "identity" => {
                e.position_id.clear();
                Refusal::InvalidIdentity
            }
            _ => unreachable!(),
        };
        let out = refused(&mut p, e.clone(), reason);
        assert_eq!(p.apply(e), out);
        let out = refused(
            &mut p,
            buy("b", "B", 3, 2, 35, 100),
            Refusal::CashAvailabilityUnknown {
                expense_event_id: "fee".into(),
            },
        );
        assert_eq!(out.before.cash_lamports, 40);
        assert_eq!(p.book().flows.expenses, 0);
        assert_eq!(
            p.valuation().basis,
            EstimateBasis::AssumedOrSyntheticOperands
        );
    }
}

#[test]
fn refused_synthetic_fee_keeps_basis_even_with_no_positions() {
    let mut p = portfolio(0, 0);
    refused(&mut p, fee("fee", 1, 5), Refusal::Arithmetic);
    assert_eq!(
        p.valuation().basis,
        EstimateBasis::AssumedOrSyntheticOperands
    );
    assert_eq!(p.book().cash_lamports, 0);
    assert!(matches!(
        p.valuation().net_change_lamports,
        Knowledge::Unknown(_)
    ));
}

#[test]
fn expense_accumulator_overflow_cannot_commit_the_staged_cash_debit() {
    let mut p = portfolio(u64::MAX, 1);
    applied(&mut p, buy("a", "A", 1, 1, 1, 1));
    applied(&mut p, fee("max-less-one", 2, u64::MAX - 1));
    applied(&mut p, sell("s", "A", 3, 1, 1, 10));
    applied(&mut p, fee("one", 4, 1));
    assert_eq!(p.book().flows.expenses, u64::MAX);
    assert_eq!(p.book().cash_lamports, 9);
    refused(&mut p, fee("overflow", 5, 1), Refusal::Arithmetic);
    assert_eq!(p.book().cash_lamports, 9);
    refused(
        &mut p,
        buy("b", "B", 6, 2, 1, 1),
        Refusal::CashAvailabilityUnknown {
            expense_event_id: "overflow".into(),
        },
    );
}

#[test]
fn cash_skip_creates_no_fee_and_distinct_ids_are_only_scenario_assertions() {
    let mut p = portfolio(100, 0);
    let out = p.apply(buy("skip", "A", 1, 1, 101, 100));
    assert!(matches!(out.disposition, Disposition::Skipped(_)));
    assert_eq!(p.book().flows.expenses, 0);
    assert_eq!(p.book().cash_lamports, 100);
    applied(&mut p, fee("asserted-one", 2, 5));
    applied(&mut p, fee("asserted-two", 3, 5));
    assert_eq!(p.book().flows.expenses, 10);
    assert_eq!(
        p.valuation().basis,
        EstimateBasis::AssumedOrSyntheticOperands
    );
}
