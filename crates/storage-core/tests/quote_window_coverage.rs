#[path = "common/quote_allocation_fixture.rs"]
mod fixture;
#[path = "common/quote_skipped_ready_fixture.rs"]
mod ready_fixture;
#[path = "common/quote_allocation_sql.rs"]
mod sql;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::ExecutionCanaryQuotePnlSummary;
use fixture::Fixture;

fn coverage(r: &ExecutionCanaryQuotePnlSummary, total: u64, sampled: u64) {
    assert_eq!(r.window_total_closed_trades, Some(total));
    assert_eq!(r.sampled_closed_trades, sampled);
    assert_eq!(r.total_closed_trades, sampled);
    assert_eq!(r.trades.len() as u64, sampled);
    assert_eq!(r.omitted_closed_trades, Some(total - sampled));
    assert_eq!(r.financial_totals_scope, "selected_market_closes");
    let check = r
        .readiness_gate
        .checks
        .iter()
        .find(|c| c.name == "quote_window_coverage")
        .unwrap();
    assert_eq!(
        check.status,
        if total == sampled { "pass" } else { "block" }
    );
    assert_eq!(
        check.reason,
        if total == sampled {
            "quote_window_complete"
        } else {
            "quote_window_truncated"
        }
    );
}

#[test]
fn hidden_unknown_buy_sell_and_skipped_counterfactual_block_without_altering_sample() -> Result<()>
{
    for mutation in [
        "DELETE FROM execution_quote_canary_events WHERE signal_id='buy-1'",
        "UPDATE execution_quote_canary_events SET quote_status='error' WHERE signal_id='buy-1'",
        "DELETE FROM execution_quote_canary_events WHERE signal_id='sell-1'",
        "UPDATE execution_quote_canary_events SET quote_status='error' WHERE signal_id='sell-1'",
        "UPDATE execution_quote_canary_events SET decision_status='would_skip' WHERE signal_id='buy-1'; DELETE FROM execution_quote_canary_events WHERE signal_id='sell-1'",
    ] {
        let f = ready_fixture::ready_mixed("2026-09-05T10:00:00.123456789Z".parse()?)?;
        let known_sample = f.report(0, 30)?;
        f.sql(mutation)?;
        let full = f.report(0, 100)?;
        let limited = f.report(0, 30)?;
        coverage(&full, 31, 31);
        coverage(&limited, 31, 30);
        assert!(!full.readiness_gate.can_start_tiny_execution, "{mutation}");
        assert!(!limited.readiness_gate.can_start_tiny_execution, "{mutation}");
        assert_eq!(limited.trades, known_sample.trades);
        assert_eq!(limited.trades, full.trades[..30]);
        assert_eq!(limited.quote_adjusted_pnl_after_priority_fee_sol, known_sample.quote_adjusted_pnl_after_priority_fee_sol);
        assert_eq!(limited.priority_fee_lamports_sum, Some(0));
        assert_eq!(limited.unknown_trades, 0);
        assert_eq!(limited.skipped_counterfactual_net_unknown_trades, 0);
        assert!((limited.quote_adjusted_pnl_after_priority_fee_sol.unwrap() - 0.29).abs() < 1e-12);
        let feedback = f.store.executable_wallet_feedback_since(f.opened)?;
        assert_eq!(feedback["wallet"].samples, 29);
        let omitted_is_excluded_by_feedback = mutation.contains("would_skip");
        assert_eq!(feedback["wallet"].unknown_samples, u64::from(!omitted_is_excluded_by_feedback));
    }
    Ok(())
}

#[test]
fn complete_and_exact_limit_are_positive_controls_but_known_truncation_is_not() -> Result<()> {
    let f = ready_fixture::ready_mixed("2026-09-05T10:00:00.123456789Z".parse()?)?;
    let all = f.report(0, 100)?;
    let equal = f.report(0, 31)?;
    let truncated = f.report(0, 30)?;
    coverage(&all, 31, 31);
    coverage(&equal, 31, 31);
    coverage(&truncated, 31, 30);
    assert!(all.readiness_gate.can_start_tiny_execution);
    assert!(equal.readiness_gate.can_start_tiny_execution);
    assert_eq!(all.trades, equal.trades);
    assert_eq!(
        all.quote_adjusted_pnl_after_priority_fee_sol,
        equal.quote_adjusted_pnl_after_priority_fee_sol
    );
    assert!(!truncated.readiness_gate.can_start_tiny_execution);
    assert_eq!(truncated.readiness_gate.blocker_count, 1);
    assert_eq!(truncated.trades, all.trades[..30]);
    assert_eq!(
        f.store.executable_wallet_feedback_since(f.opened)?["wallet"].samples,
        30
    );
    // The library's existing limit.max(1) behavior is preserved too.
    coverage(&f.report(0, 0)?, 31, 1);
    Ok(())
}

#[test]
fn future_before_window_nonmarket_and_stale_prefix_do_not_inflate_coverage() -> Result<()> {
    let f = ready_fixture::ready_mixed("2026-09-05T10:00:00.123456789Z".parse()?)?;
    let original = f.report(0, 31)?;
    for (signal, close) in [
        ("before", f.opened - Duration::nanoseconds(1)),
        (
            "future",
            f.opened + Duration::hours(1) + Duration::nanoseconds(1),
        ),
        ("non-market", f.opened + Duration::seconds(10)),
        ("stale-close-prefix", f.opened + Duration::seconds(11)),
    ] {
        f.store.insert_shadow_closed_trade(
            signal,
            "other-wallet",
            "Other",
            1.0,
            0.1,
            0.11,
            0.01,
            f.opened - Duration::hours(1),
            close,
        )?;
    }
    f.sql("UPDATE shadow_closed_trades SET close_context='stale_quote_price' WHERE signal_id='non-market'")?;
    let r = f.report(0, 31)?;
    coverage(&r, 31, 31);
    assert_eq!(r.trades, original.trades);
    assert_eq!(
        r.quote_adjusted_pnl_after_priority_fee_sol,
        original.quote_adjusted_pnl_after_priority_fee_sol
    );
    assert!(r.readiness_gate.can_start_tiny_execution);
    // A future market close belongs to the older since-only shadow breakdown.
    assert!(r.shadow_close_breakdown.market_closed_trades > r.window_total_closed_trades.unwrap());
    Ok(())
}

#[test]
fn fractional_inclusive_bounds_and_equal_time_id_order_match_the_count() -> Result<()> {
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    let first = f.exit(1, "25", Some(2))?;
    let second = f.exit(2, "75", Some(2))?;
    let instant = f.opened + Duration::seconds(1);
    f.sql(&format!("UPDATE shadow_closed_trades SET closed_ts='{}'; UPDATE execution_quote_canary_events SET signal_ts='{}' WHERE side='sell'", instant.to_rfc3339(), instant.to_rfc3339()))?;
    let all = f
        .store
        .execution_canary_quote_pnl_summary(instant, instant, 2)?;
    coverage(&all, 2, 2);
    assert_eq!(
        all.trades
            .iter()
            .map(|t| t.shadow_closed_trade_id)
            .collect::<Vec<_>>(),
        vec![second, first]
    );
    assert_eq!(
        all.trades
            .iter()
            .map(|t| t.priority_fee_lamports_total)
            .collect::<Vec<_>>(),
        vec![Some(7), Some(4)]
    );
    let limited = f
        .store
        .execution_canary_quote_pnl_summary(instant, instant, 1)?;
    coverage(&limited, 2, 1);
    assert_eq!(limited.trades[0], all.trades[0]);
    for (since, as_of) in [
        (f.opened, instant - Duration::nanoseconds(1)),
        (
            instant + Duration::nanoseconds(1),
            instant + Duration::seconds(1),
        ),
    ] {
        coverage(
            &f.store
                .execution_canary_quote_pnl_summary(as_of, since, 2)?,
            0,
            0,
        );
    }
    Ok(())
}

#[test]
fn selected_partial_sell_keeps_paid_history_before_window_and_sample() -> Result<()> {
    let mut f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit(1, "25", Some(2))?;
    f.exit(2, "75", Some(2))?;
    let all = f.report(0, 100)?;
    for _ in 0..2 {
        for (since, limit, total) in [(0, 1, 2), (2, 1, 1), (2, 100, 1)] {
            let r = f.report(since, limit)?;
            coverage(&r, total, 1);
            assert_eq!(r.trades[0], all.trades[0]);
            assert_eq!(
                r.trades[0]
                    .fee_allocation
                    .buy_fee_allocated_lamports
                    .as_deref(),
                Some("5")
            );
            assert_eq!(r.priority_fee_lamports_sum, Some(7));
            assert!(
                (r.quote_adjusted_pnl_after_priority_fee_sol.unwrap() - (0.035 - 7e-9)).abs()
                    < 1e-12
            );
        }
        f.reopen()?;
    }
    let feedback = f
        .store
        .executable_wallet_feedback_since(f.opened + Duration::seconds(2))?;
    assert_eq!(feedback["wallet"].samples, 1);
    assert_eq!(
        feedback["wallet"].complete_pnl_after_priority_fee_sol(),
        all.trades[0].quote_adjusted_pnl_after_priority_fee_sol
    );
    Ok(())
}

#[test]
fn unavailable_window_evidence_cannot_be_reported_as_known_zero() -> Result<()> {
    let f = Fixture::new()?;
    f.sql("ALTER TABLE shadow_closed_trades RENAME TO unavailable_closed_trades")?;
    assert!(f.report(0, 30).is_err());
    Ok(())
}
