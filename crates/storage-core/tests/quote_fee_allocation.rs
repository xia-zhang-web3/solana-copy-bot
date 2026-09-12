#[path = "common/quote_allocation_fixture.rs"]
mod fixture;
#[path = "common/quote_allocation_sql.rs"]
mod sql;
use anyhow::Result;
use fixture::Fixture;

#[test]
fn partial_exits_spend_buy_fee_once_through_storage_and_feedback() -> Result<()> {
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit(1, "25", Some(2))?;
    f.exit(2, "75", Some(2))?;
    let report = f.report(0, 100)?;
    assert_eq!(
        report
            .trades
            .iter()
            .rev()
            .map(|t| t.priority_fee_lamports_total)
            .collect::<Vec<_>>(),
        vec![Some(4), Some(7)]
    );
    assert_eq!(report.priority_fee_lamports_sum, Some(11));
    let feedback = f.store.executable_wallet_feedback_since(f.opened)?;
    assert_eq!(feedback["wallet"].samples, 2);
    assert!(
        (feedback["wallet"]
            .complete_pnl_after_priority_fee_sol()
            .unwrap()
            - (0.12 - 11e-9))
            .abs()
            < 1e-12
    );
    Ok(())
}

#[test]
fn conserving_rounding_full_exit_remainders_and_exact_json() -> Result<()> {
    for (q, fee, exits, expected) in [
        (100, 7, vec![25, 75], vec![2, 5]),
        (100, 7, vec![100], vec![7]),
        (3, 1, vec![1, 1, 1], vec![1, 0, 0]),
        (100, 0, vec![25, 75], vec![0, 0]),
    ] {
        let f = Fixture::new()?;
        f.buy(&q.to_string(), Some(fee))?;
        let mut allocated = 0;
        for (i, (quantity, allocation)) in exits.iter().zip(expected).enumerate() {
            f.exit(i as i64 + 1, &quantity.to_string(), Some(2))?;
            allocated += allocation;
            let r = f.report(0, 1)?;
            let t = &r.trades[0];
            assert_eq!(t.priority_fee_lamports_total, Some(allocation + 2));
            assert_eq!(
                t.fee_allocation.buy_fee_remaining_lamports,
                Some((fee - allocated).to_string())
            );
            let json = serde_json::to_value(t)?;
            assert_eq!(
                json["fee_allocation"]["observed_buy_fee_lamports"],
                fee.to_string()
            );
            assert_eq!(
                json["fee_allocation"]["buy_fee_allocated_lamports"],
                allocation.to_string()
            );
        }
        assert_eq!(allocated, fee);
    }
    Ok(())
}

#[test]
fn null_negative_and_large_raw_values_do_not_invent_known_fees() -> Result<()> {
    for (quantity, fee, sell, known) in [
        ("100", None, Some(2), false),
        ("100", Some(7), None, false),
        ("100", Some(0), Some(0), true),
        (
            "340282366920938463463374607431768211455",
            Some(7),
            Some(2),
            true,
        ),
        (
            "340282366920938463463374607431768211456",
            Some(7),
            Some(2),
            false,
        ),
        ("garbage", Some(7), Some(2), false),
        ("+100", Some(7), Some(2), false),
        ("-1", Some(7), Some(2), false),
        ("0", Some(7), Some(2), false),
    ] {
        let f = Fixture::new()?;
        f.buy(quantity, fee)?;
        f.exit(1, quantity, sell)?;
        let r = f.report(0, 10)?;
        assert_eq!(
            r.trades[0].priority_fee_lamports_total.is_some(),
            known,
            "{quantity}"
        );
        assert_eq!(r.quote_adjusted_pnl_after_priority_fee_sol.is_some(), known);
    }
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit(1, "100", Some(2))?;
    f.sql("UPDATE execution_quote_canary_events SET priority_fee_lamports=-1 WHERE side='buy'")?;
    assert!(f.report(0, 10)?.priority_fee_lamports_sum.is_none());
    Ok(())
}

#[test]
fn checked_product_overflow_and_over_closes_poison_suffix_without_reset() -> Result<()> {
    for (entry, quantities, reason, known_prefix) in [
        ("100", vec!["101", "1"], "quote_quantity_over_close", 0),
        ("100", vec!["75", "26", "1"], "quote_quantity_over_close", 1),
        ("100", vec!["100", "100"], "quote_quantity_over_close", 1),
        (
            "100",
            vec!["25", "invalid", "75"],
            "invalid_quote_quantity",
            1,
        ),
        (
            "340282366920938463463374607431768211455",
            vec!["170141183460469231731687303715884105728", "1"],
            "quote_fee_allocation_overflow",
            0,
        ),
    ] {
        let f = Fixture::new()?;
        f.buy(entry, Some(7))?;
        for (i, q) in quantities.iter().enumerate() {
            f.exit(i as i64 + 1, q, Some(2))?;
        }
        let r = f.report(0, 100)?;
        assert_eq!(r.pnl_counted_trades, known_prefix);
        assert_eq!(r.trades[0].fee_allocation.reason, reason);
        assert!(r.priority_fee_lamports_sum.is_none());
        assert!(r.trades[0]
            .fee_allocation
            .buy_fee_remaining_lamports
            .is_none());
        assert!(r.trades[0]
            .quote_adjusted_pnl_after_priority_fee_sol
            .is_none());
    }
    Ok(())
}

#[test]
fn skipped_entry_counterfactual_conserves_and_mixed_coverage_stays_unknown() -> Result<()> {
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit(1, "25", Some(2))?;
    f.exit(2, "75", Some(2))?;
    f.sql(
        "UPDATE execution_quote_canary_events SET decision_status='would_skip' WHERE side='buy'",
    )?;
    let r = f.report(0, 100)?;
    assert_eq!(r.skipped_trades, 2);
    assert!(
        (r.skipped_counterfactual_pnl_after_priority_fee_sol.unwrap() - (0.12 - 11e-9)).abs()
            < 1e-12
    );
    assert!(f
        .store
        .executable_wallet_feedback_since(f.opened)?
        .is_empty());
    f.sql("UPDATE execution_quote_canary_events SET decision_status='would_execute' WHERE side='buy'; UPDATE execution_quote_canary_events SET priority_fee_lamports=NULL WHERE signal_id='sell-2'")?;
    let r = f.report(0, 100)?;
    assert_eq!(r.pnl_counted_trades, 1);
    assert_eq!(r.unknown_trades, 1);
    assert!(r.quote_adjusted_pnl_after_priority_fee_sol.is_none());
    assert!(r
        .buy_slippage_buckets
        .iter()
        .all(|b| b.quote_adjusted_pnl_after_priority_fee_sol.is_none()));
    assert!(r
        .threshold_summaries
        .iter()
        .all(|t| t.quote_adjusted_pnl_after_priority_fee_sol.is_none()));
    assert!(!r.readiness_gate.can_start_tiny_execution);
    let feedback = f.store.executable_wallet_feedback_since(f.opened)?;
    assert_eq!(feedback["wallet"].samples, 1);
    assert_eq!(feedback["wallet"].unknown_samples, 1);
    assert!(feedback["wallet"]
        .complete_pnl_after_priority_fee_sol()
        .is_none());
    Ok(())
}

#[test]
fn proven_skipped_buy_with_missing_exit_stays_out_of_executable_feedback() -> Result<()> {
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit(1, "25", Some(2))?;
    f.exit(2, "75", Some(2))?;
    f.sql("UPDATE execution_quote_canary_events SET decision_status='would_skip' WHERE side='buy'; DELETE FROM execution_quote_canary_events WHERE signal_id='sell-1'")?;
    assert!(f
        .report(0, 100)?
        .skipped_counterfactual_pnl_after_priority_fee_sol
        .is_none());
    assert!(f
        .store
        .executable_wallet_feedback_since(f.opened)?
        .is_empty());
    Ok(())
}
