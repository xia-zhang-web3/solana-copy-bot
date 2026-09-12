#[path = "common/quote_allocation_fixture.rs"]
mod fixture;
#[path = "common/quote_allocation_sql.rs"]
mod sql;
use anyhow::Result;
use fixture::Fixture;

#[test]
fn three_exits_keep_proven_skipped_buy_when_middle_sell_is_missing() -> Result<()> {
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit(1, "25", Some(2))?;
    f.exit(2, "25", Some(2))?;
    f.exit(3, "50", Some(2))?;
    f.sql(
        "UPDATE execution_quote_canary_events SET decision_status='would_skip' WHERE side='buy'",
    )?;
    let complete = f.report(0, 100)?;
    assert_eq!(complete.skipped_trades, 3);
    assert!(
        (complete
            .skipped_counterfactual_pnl_after_priority_fee_sol
            .unwrap()
            - (0.23 - 13e-9))
            .abs()
            < 1e-12
    );
    f.sql("DELETE FROM execution_quote_canary_events WHERE signal_id='sell-2'")?;
    let r = f.report(0, 100)?;
    println!("three_exit_observation={}", serde_json::to_string(&r)?);
    assert_eq!(r.total_closed_trades, 3);
    assert_eq!(r.trades.len(), 3);
    assert_eq!(r.trades[2], complete.trades[2]);
    assert!(r.trades[1].skipped_counterfactual_pnl_sol.is_none());
    assert!(r
        .skipped_counterfactual_pnl_after_priority_fee_sol
        .is_none());
    assert!(!r.readiness_gate.can_start_tiny_execution);
    assert!(f
        .store
        .executable_wallet_feedback_since(f.opened)?
        .is_empty());
    assert_eq!(
        r.skipped_trades, 3,
        "proven BUY decision is independent of missing SELL"
    );
    assert_eq!(r.unknown_trades, 0);
    assert_eq!(r.missing_exit_quote_trades, 1); // fee-unknown suffix is not another missing SELL
    assert_eq!(r.skipped_counterfactual_gross_known_trades, 2);
    assert_eq!(r.skipped_counterfactual_gross_unknown_trades, 1);
    assert_eq!(r.skipped_counterfactual_net_known_trades, 1);
    assert_eq!(r.skipped_counterfactual_net_unknown_trades, 2);
    assert!(r.skipped_counterfactual_pnl_sol.is_none());
    assert_eq!(
        r.trades[1].skipped_counterfactual_reason.as_deref(),
        Some("missing_exit_quote")
    );
    assert_eq!(
        r.trades[0].skipped_counterfactual_reason.as_deref(),
        Some("missing_exit_quote")
    );
    assert!((r.trades[0].skipped_counterfactual_pnl_sol.unwrap() - 0.06).abs() < 1e-12);
    assert!(r.trades[0]
        .skipped_counterfactual_pnl_after_priority_fee_sol
        .is_none());
    Ok(())
}

#[test]
fn proven_skip_survives_exit_failures_without_borrowing_unrelated_numbers() -> Result<()> {
    for (mutation, reason) in [
        ("", "known_retained_history"),
        ("DELETE FROM execution_quote_canary_events WHERE side='sell'", "missing_exit_quote"),
        ("UPDATE execution_quote_canary_events SET quote_status='error' WHERE side='sell'", "exit_quote_not_ok"),
        ("UPDATE execution_quote_canary_events SET decision_status='would_skip' WHERE side='sell'", "exit_decision_unproven"),
        ("UPDATE execution_quote_canary_events SET decision_status=NULL WHERE side='sell'", "exit_decision_unproven"),
        ("UPDATE execution_quote_canary_events SET wallet_id='foreign' WHERE side='sell'", "exit_binding_conflict"),
        ("UPDATE execution_quote_canary_events SET signal_id='foreign' WHERE side='sell'", "exit_binding_conflict"),
        ("UPDATE execution_quote_canary_events SET shadow_closed_trade_id=999 WHERE side='sell'", "exit_binding_conflict"),
        ("UPDATE execution_quote_canary_events SET event_id='quote:close:999' WHERE side='sell'", "exit_binding_conflict"),
        ("ambiguous", "ambiguous_exit_quote"),
    ] {
        let f = skipped_fixture()?;
        let id = f.exit(1, "100", Some(2))?;
        if mutation == "ambiguous" {
            let mut extra = f.event(false, 1, "100", Some(999));
            extra.event_id = "unrelated-sell".into();
            extra.shadow_closed_trade_id = Some(id);
            extra.quote_out_amount_raw = Some("900000000000".into());
            f.store.record_execution_quote_canary_event(&extra)?;
        } else {
            f.sql(mutation)?;
        }
        let r = f.report(0, 10)?;
        let t = &r.trades[0];
        println!("exit_case={reason} trade={}", serde_json::to_string(t)?);
        assert_eq!((r.total_closed_trades, r.skipped_trades, r.unknown_trades), (1, 1, 0), "{mutation}");
        assert_eq!(t.status, "would_skip");
        assert_eq!(t.reason, "too_much_slippage");
        assert_eq!(t.skipped_counterfactual_reason.as_deref(), Some(reason));
        assert!(t.quote_adjusted_pnl_sol.is_none());
        assert!(t.quote_adjusted_pnl_after_priority_fee_sol.is_none());
        assert!(t.entry_cost_sol.is_none() && t.exit_quote_sol.is_none());
        assert_eq!((r.quote_win_count, r.quote_loss_count, r.pnl_counted_trades), (0, 0, 0));
        let known = reason == "known_retained_history";
        assert_eq!(r.skipped_counterfactual_gross_known_trades, u64::from(known));
        assert_eq!(r.skipped_counterfactual_net_unknown_trades, u64::from(!known));
        assert_eq!(t.skipped_counterfactual_pnl_sol.is_some(), known);
        assert_eq!(r.skipped_counterfactual_pnl_sol.is_some(), known);
        assert_eq!(r.skipped_counterfactual_pnl_after_priority_fee_sol.is_some(), known);
        if known {
            assert!((t.skipped_counterfactual_pnl_sol.unwrap() - 0.01).abs() < 1e-12);
            assert!((t.skipped_counterfactual_pnl_after_priority_fee_sol.unwrap() - (0.01 - 9e-9)).abs() < 1e-12);
        }
        assert!(f.store.executable_wallet_feedback_since(f.opened)?.is_empty());
    }
    Ok(())
}

#[test]
fn unproven_or_unsuccessful_buy_never_becomes_a_known_skip() -> Result<()> {
    for (mutation, reason) in [
        ("DELETE FROM execution_quote_canary_events WHERE side='buy'", "missing_entry_quote"),
        ("UPDATE execution_quote_canary_events SET quote_status='error' WHERE side='buy'", "entry_quote_status:error"),
        ("UPDATE execution_quote_canary_events SET signal_id='wrong' WHERE side='buy'", "entry_binding_conflict"),
        ("UPDATE execution_quote_canary_events SET shadow_closed_trade_id=999 WHERE side='buy'", "entry_binding_conflict"),
        ("UPDATE execution_quote_canary_events SET wallet_id='foreign' WHERE side='buy'", "missing_entry_quote"),
        ("UPDATE execution_quote_canary_events SET token='foreign' WHERE side='buy'", "missing_entry_quote"),
        ("UPDATE execution_quote_canary_events SET event_id='quote:entry-shadow-diag:buy' WHERE side='buy'", "missing_entry_quote"),
        ("ambiguous", "ambiguous_entry_quote"),
        ("persisted_conflict", "entry_binding_conflict"),
    ] {
        let f = skipped_fixture()?;
        f.exit(1, "100", Some(2))?;
        match mutation {
            "ambiguous" => {
                let mut extra = f.event(true, 0, "100", Some(999));
                extra.event_id = "quote:entry:extra".into();
                extra.signal_id = Some("extra".into());
                extra.decision_status = Some("would_skip".into());
                f.store.record_execution_quote_canary_event(&extra)?;
            }
            "persisted_conflict" => f.sql(&format!(
                "INSERT INTO copy_signals(signal_id,wallet_id,side,token,notional_sol,ts,status) VALUES ('buy','foreign','buy','Token',0.1,'{}','shadow_recorded')", f.opened.to_rfc3339()
            ))?,
            _ => f.sql(mutation)?,
        }
        let r = f.report(0, 10)?;
        let t = &r.trades[0];
        assert_eq!((r.total_closed_trades, r.skipped_trades, r.unknown_trades), (1, 0, 1), "{mutation}");
        assert_eq!(t.reason, reason, "{mutation}");
        assert!(t.skipped_counterfactual_reason.is_none());
        assert!(t.skipped_counterfactual_pnl_sol.is_none());
        assert!(r.quote_adjusted_pnl_after_priority_fee_sol.is_none());
        // Feedback formulas are unchanged; only the identity-negative arms are
        // compared here (its existing skip filter does not inspect quote status).
        if !mutation.contains("quote_status") {
            assert_eq!(f.store.executable_wallet_feedback_since(f.opened)?["wallet"].unknown_samples, 1);
        }
    }
    Ok(())
}

#[test]
fn would_execute_with_missing_or_error_sell_preserves_unknown() -> Result<()> {
    for mutation in [
        "DELETE FROM execution_quote_canary_events WHERE side='sell'",
        "UPDATE execution_quote_canary_events SET quote_status='error' WHERE side='sell'",
    ] {
        let f = Fixture::new()?;
        f.buy("100", Some(7))?;
        f.exit(1, "100", Some(2))?;
        f.sql(mutation)?;
        let r = f.report(0, 10)?;
        assert_eq!((r.skipped_trades, r.unknown_trades), (0, 1));
        assert!(r.trades[0].skipped_counterfactual_reason.is_none());
        assert!(r.trades[0].quote_adjusted_pnl_sol.is_none());
        assert!(!r.readiness_gate.can_start_tiny_execution);
        assert_eq!(
            f.store.executable_wallet_feedback_since(f.opened)?["wallet"].unknown_samples,
            1
        );
    }
    Ok(())
}

fn skipped_fixture() -> Result<Fixture> {
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.sql("UPDATE execution_quote_canary_events SET decision_status='would_skip',decision_reason='too_much_slippage' WHERE side='buy'")?;
    Ok(f)
}
