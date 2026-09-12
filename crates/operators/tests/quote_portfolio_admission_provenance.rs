mod quote_portfolio_support;

use copybot_operators::quote_portfolio::*;
use quote_portfolio_support::*;

fn observed() -> Provenance {
    Provenance::Observed("explicit test observation assertion".into())
}

fn observed_buy(id: &str, seq: u64, principal: u64) -> Event {
    let mut event = buy(id, id, seq, 1, principal, 100);
    let Action::Buy {
        quote: Knowledge::Known(quote),
        costs,
        rent_deposit,
        ..
    } = &mut event.action
    else {
        unreachable!()
    };
    quote.provenance = observed();
    for operand in [
        &mut costs.base,
        &mut costs.priority,
        &mut costs.setup,
        &mut costs.exit,
        rent_deposit,
    ] {
        operand.provenance = observed();
    }
    event
}

fn admission_operand<'a>(event: &'a mut Event, component: &str) -> &'a mut Lamports {
    let Action::Buy {
        costs,
        rent_deposit,
        ..
    } = &mut event.action
    else {
        unreachable!()
    };
    match component {
        "base" => &mut costs.base,
        "priority" => &mut costs.priority,
        "setup" => &mut costs.setup,
        "rent" => rent_deposit,
        _ => unreachable!(),
    }
}

#[test]
fn skipped_assumed_or_synthetic_fee_and_rent_keep_basis_through_replay_and_observed_buy() {
    for origin in [
        Provenance::Assumed("admission assumption".into()),
        Provenance::Synthetic("synthetic admission operand".into()),
    ] {
        for component in ["base", "priority", "setup", "rent"] {
            // Isolate the skip: no earlier Applied event can set portfolio basis.
            let mut p = portfolio(100, 1);
            let mut expected_book = p.book().clone();
            let mut event = observed_buy("A", 1, 100);
            let operand = admission_operand(&mut event, component);
            operand.amount = Knowledge::Known(1);
            operand.provenance = origin.clone();
            let out = p.apply(event.clone());
            assert_eq!(
                out.disposition,
                Disposition::Skipped(vec![AdmissionReason::Cash {
                    available: 100,
                    required: 101,
                }])
            );
            assert_eq!(out.before, out.after);
            assert_eq!(out.allocated_this_event, Components::default());
            expected_book.assumed_or_synthetic = true;
            assert_eq!(*p.book(), expected_book); // Only provenance may change.
            assert_eq!(out.valuation_after, p.valuation());
            assert_eq!(
                p.valuation().basis,
                EstimateBasis::AssumedOrSyntheticOperands
            );
            assert_eq!(p.valuation().full_equity_lamports, Knowledge::Known(100));
            assert_eq!(p.valuation().net_change_lamports, Knowledge::Known(0));
            assert!(p.valuation().unresolved.is_empty());
            assert!(p.valuation().missing_marks.is_empty());
            assert_eq!(p.apply(event.clone()), out);
            assert_eq!(*p.book(), expected_book);
            conservation(&p);

            let later = applied(&mut p, observed_buy("B", 2, 1));
            assert_eq!(p.book().cash_lamports, 99);
            assert_eq!(p.book().open_slots, 1);
            assert_eq!(p.book().positions["B"].remaining_raw, 100);
            assert!(!p.book().positions.contains_key("A"));
            assert_eq!(p.book().flows.buy_principal, 1);
            assert_eq!(p.book().flows.expenses, 0);
            assert_eq!(p.book().locked_rent_lamports, 0);
            assert_eq!(
                later.valuation_after.basis,
                EstimateBasis::AssumedOrSyntheticOperands
            );
            let after_later = p.book().clone();
            let replay = p.apply(event);
            assert_eq!(replay, out);
            assert_eq!(*p.book(), after_later);
            assert_eq!(
                p.last_order(),
                Some(Order {
                    sequence: 2,
                    unix_ms: 2
                })
            );
            assert_eq!(
                p.valuation().basis,
                EstimateBasis::AssumedOrSyntheticOperands
            );
            assert!(p.valuation().unresolved.is_empty());
            println!(
                "origin={origin:?}; component={component}; outcome={:?}; later={:?}; replay={:?}; cash_after_skip={}; cash_after_buy={}",
                out.valuation_after.basis, later.valuation_after.basis,
                replay.valuation_after.basis, out.after.cash_lamports, p.book().cash_lamports
            );
            let label = if matches!(origin, Provenance::Assumed(_)) {
                "assumed"
            } else {
                "synthetic"
            };
            record(
                &format!("admission-{label}-{component}"),
                &p,
                &[out, later, replay],
            );
        }
    }
}

#[test]
fn observed_base_zero_admits_and_one_skips_without_assumed_basis() {
    for fee in [0, 1] {
        let mut p = portfolio(100, 1);
        let before = p.book().clone();
        let mut event = observed_buy("A", 1, 100);
        admission_operand(&mut event, "base").amount = Knowledge::Known(fee);
        let out = p.apply(event.clone());
        if fee == 0 {
            assert_eq!(out.disposition, Disposition::Applied);
            assert_eq!(p.book().cash_lamports, 0);
            assert_eq!(p.book().open_slots, 1);
            assert_eq!(p.book().positions["A"].remaining_raw, 100);
            assert_eq!(p.book().flows.buy_principal, 100);
        } else {
            assert_eq!(
                out.disposition,
                Disposition::Skipped(vec![AdmissionReason::Cash {
                    available: 100,
                    required: 101,
                }])
            );
            assert_eq!(*p.book(), before);
            assert_eq!(out.before, out.after);
            assert_eq!(p.valuation().full_equity_lamports, Knowledge::Known(100));
            assert_eq!(p.valuation().net_change_lamports, Knowledge::Known(0));
        }
        assert_eq!(p.book().flows.expenses, 0);
        assert_eq!(p.valuation().basis, EstimateBasis::CallerObservedOperands);
        assert_eq!(
            out.valuation_after.basis,
            EstimateBasis::CallerObservedOperands
        );
        assert!(p.valuation().unresolved.is_empty());
        let after = p.book().clone();
        assert_eq!(p.apply(event), out);
        assert_eq!(*p.book(), after);
        conservation(&p);
        record(&format!("admission-observed-{fee}"), &p, &[out]);
    }
}
