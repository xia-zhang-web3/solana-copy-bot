#[path = "common/quote_allocation_fixture.rs"]
mod fixture;
#[path = "common/quote_skipped_ready_fixture.rs"]
mod ready_fixture;
#[path = "common/quote_allocation_sql.rs"]
mod sql;
use anyhow::Result;
use fixture::Fixture;

#[test]
fn known_zero_is_distinct_from_unknown_gross_or_net() -> Result<()> {
    let f = Fixture::new()?;
    f.buy("100", Some(0))?;
    f.exit_with_output(1, "100", Some(0), "100000000")?;
    f.sql(
        "UPDATE execution_quote_canary_events SET decision_status='would_skip' WHERE side='buy'",
    )?;
    let r = f.report(0, 10)?;
    assert_eq!(r.skipped_counterfactual_pnl_sol, Some(0.0));
    assert_eq!(
        r.skipped_counterfactual_pnl_after_priority_fee_sol,
        Some(0.0)
    );
    assert_eq!(r.trades[0].skipped_counterfactual_pnl_sol, Some(0.0));
    assert_eq!(r.trades[0].priority_fee_lamports_total, Some(0));
    assert_eq!(r.skipped_counterfactual_gross_known_trades, 1);
    assert_eq!(r.skipped_counterfactual_net_known_trades, 1);
    assert_eq!((r.quote_win_count, r.quote_loss_count), (0, 0));
    for side in ["buy", "sell"] {
        f.sql("UPDATE execution_quote_canary_events SET priority_fee_lamports=0")?;
        f.sql(&format!("UPDATE execution_quote_canary_events SET priority_fee_lamports=NULL WHERE side='{side}'"))?;
        let r = f.report(0, 10)?;
        assert_eq!(r.skipped_trades, 1);
        assert_eq!(r.skipped_counterfactual_pnl_sol, Some(0.0));
        assert!(r
            .skipped_counterfactual_pnl_after_priority_fee_sol
            .is_none());
        assert_eq!(
            r.trades[0].skipped_counterfactual_reason.as_deref(),
            Some("priority_fee_total_unknown")
        );
        assert_eq!(r.skipped_counterfactual_net_unknown_trades, 1);
        assert!(!r.readiness_gate.can_start_tiny_execution);
    }
    Ok(())
}

#[test]
fn malformed_amounts_do_not_erase_skip_or_invent_counterfactual_zero() -> Result<()> {
    for (side, column) in [
        ("buy", "quote_in_amount_raw"),
        ("buy", "quote_out_amount_raw"),
        ("sell", "quote_in_amount_raw"),
        ("sell", "quote_out_amount_raw"),
    ] {
        for raw in [
            "invalid",
            "-1",
            "+100",
            "340282366920938463463374607431768211456",
        ] {
            let f = Fixture::new()?;
            f.buy("100", Some(7))?;
            f.exit(1, "100", Some(2))?;
            f.sql("UPDATE execution_quote_canary_events SET decision_status='would_skip' WHERE side='buy'")?;
            f.sql(&format!(
                "UPDATE execution_quote_canary_events SET {column}='{raw}' WHERE side='{side}'"
            ))?;
            let r = f.report(0, 10)?;
            assert_eq!(
                (r.skipped_trades, r.unknown_trades),
                (1, 0),
                "{side}/{column}/{raw}"
            );
            assert!(r.trades[0].skipped_counterfactual_pnl_sol.is_none());
            assert!(r.skipped_counterfactual_pnl_sol.is_none());
            assert!(r
                .skipped_counterfactual_pnl_after_priority_fee_sol
                .is_none());
            assert_eq!(r.skipped_counterfactual_gross_unknown_trades, 1);
            assert_eq!(r.skipped_counterfactual_net_unknown_trades, 1);
            assert_eq!(r.invalid_quote_amount_trades, 1);
            assert_eq!(
                r.trades[0].skipped_counterfactual_reason.as_deref(),
                Some("invalid_quote_amount")
            );
            assert_eq!((r.quote_win_count, r.quote_loss_count), (0, 0));
        }
    }
    Ok(())
}

#[test]
fn partial_fee_allocation_and_zero_sell_output_preserve_known_values() -> Result<()> {
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit_with_output(1, "25", Some(2), "0")?;
    f.exit(2, "75", None)?;
    f.sql(
        "UPDATE execution_quote_canary_events SET decision_status='would_skip' WHERE side='buy'",
    )?;
    let r = f.report(0, 100)?;
    assert_eq!(r.skipped_trades, 2);
    assert_eq!(
        r.trades[1]
            .fee_allocation
            .buy_fee_allocated_lamports
            .as_deref(),
        Some("2")
    );
    assert_eq!(
        r.trades[0]
            .fee_allocation
            .buy_fee_allocated_lamports
            .as_deref(),
        Some("5")
    );
    assert_eq!(
        r.trades[0]
            .fee_allocation
            .buy_fee_remaining_lamports
            .as_deref(),
        Some("0")
    );
    assert!((r.trades[1].skipped_counterfactual_pnl_sol.unwrap() + 0.025).abs() < 1e-12);
    assert!(
        (r.trades[1]
            .skipped_counterfactual_pnl_after_priority_fee_sol
            .unwrap()
            + 0.025
            + 4e-9)
            .abs()
            < 1e-12
    );
    assert!((r.skipped_counterfactual_pnl_sol.unwrap() - 0.01).abs() < 1e-12);
    assert_eq!(
        (
            r.skipped_counterfactual_gross_known_trades,
            r.skipped_counterfactual_net_known_trades
        ),
        (2, 1)
    );
    assert!(r
        .skipped_counterfactual_pnl_after_priority_fee_sol
        .is_none());
    assert_eq!(r.quote_adjusted_pnl_sol, 0.0);
    assert_eq!((r.quote_win_count, r.quote_loss_count), (0, 0));
    let limited = f.report(2, 1)?;
    assert_eq!(limited.trades[0], r.trades[0]);
    assert_eq!(limited.skipped_counterfactual_net_unknown_trades, 1);
    assert!(f
        .store
        .executable_wallet_feedback_since(f.opened)?
        .is_empty());
    Ok(())
}

#[test]
fn mixed_cohort_keeps_rows_amounts_feedback_and_blocks_incomplete_counterfactual() -> Result<()> {
    let f = ready_fixture::ready_mixed("2026-09-05T10:00:00.123456789Z".parse()?)?;
    let complete = f.report(0, 100)?;
    assert!(
        complete.readiness_gate.can_start_tiny_execution,
        "{:?}",
        complete.readiness_gate
    );
    assert_eq!(
        (
            complete.total_closed_trades,
            complete.pnl_counted_trades,
            complete.skipped_trades
        ),
        (31, 30, 1)
    );
    assert!((complete.quote_adjusted_pnl_after_priority_fee_sol.unwrap() - 0.3).abs() < 1e-12);
    let feedback = f.store.executable_wallet_feedback_since(f.opened)?;
    assert_eq!(feedback["wallet"].samples, 30);
    f.sql("DELETE FROM execution_quote_canary_events WHERE signal_id='sell-31'")?;
    let missing = f.report(0, 100)?;
    println!(
        "mixed_missing_observation={}",
        serde_json::to_string(&missing)?
    );
    assert_eq!(
        (
            missing.total_closed_trades,
            missing.pnl_counted_trades,
            missing.skipped_trades,
            missing.unknown_trades
        ),
        (31, 30, 1, 0)
    );
    assert_eq!(missing.trades[1..], complete.trades[1..]);
    assert_eq!(
        missing.shadow_close_breakdown,
        complete.shadow_close_breakdown
    );
    assert_eq!(missing.shadow_pnl_sol, complete.shadow_pnl_sol);
    assert_eq!(
        missing.quote_adjusted_pnl_sol,
        complete.quote_adjusted_pnl_sol
    );
    assert_eq!((missing.quote_win_count, missing.quote_loss_count), (30, 0));
    assert!(missing.skipped_counterfactual_pnl_sol.is_none());
    assert!(missing
        .skipped_counterfactual_pnl_after_priority_fee_sol
        .is_none());
    // Existing combined fee-completeness policy is retained for the executed total.
    assert!(missing.quote_adjusted_pnl_after_priority_fee_sol.is_none());
    assert!(!missing.readiness_gate.can_start_tiny_execution);
    let coverage = missing
        .readiness_gate
        .checks
        .iter()
        .find(|c| c.name == "skipped_counterfactual_coverage")
        .unwrap();
    assert_eq!(coverage.status, "block");
    assert!(coverage
        .value
        .contains("gross known=0 unknown=1; net known=0 unknown=1"));
    assert!(missing
        .readiness_gate
        .checks
        .iter()
        .any(|c| c.name == "unknown_or_missing_quotes" && c.status == "pass"));
    assert_eq!(missing.readiness_gate.min_market_closed_trades, 30);
    assert_eq!(
        missing.quote_diagnostics.entry_all,
        complete.quote_diagnostics.entry_all
    );
    assert!(missing
        .buy_slippage_buckets
        .iter()
        .all(|b| b.after_fee_unknown_trades == 1
            && b.quote_adjusted_pnl_after_priority_fee_sol.is_none()));
    assert!(missing
        .threshold_summaries
        .iter()
        .all(|t| t.unknown_trades == 1 && t.quote_adjusted_pnl_after_priority_fee_sol.is_none()));
    let after_feedback = f.store.executable_wallet_feedback_since(f.opened)?;
    assert_eq!(after_feedback.len(), feedback.len());
    let before = &feedback["wallet"];
    let after = &after_feedback["wallet"];
    assert_eq!(after.samples, before.samples);
    assert_eq!(after.unknown_samples, before.unknown_samples);
    assert_eq!(
        after.known_sample_pnl_after_priority_fee_sol,
        before.known_sample_pnl_after_priority_fee_sol
    );
    assert_eq!(
        after.shadow_positive_executable_negative,
        before.shadow_positive_executable_negative
    );
    Ok(())
}
