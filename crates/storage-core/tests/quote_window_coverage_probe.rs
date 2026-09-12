#[path = "common/quote_allocation_fixture.rs"]
mod fixture;
#[path = "common/quote_skipped_ready_fixture.rs"]
mod ready_fixture;
#[path = "common/quote_allocation_sql.rs"]
mod sql;
use anyhow::Result;
#[test]
fn limiting_rows_must_not_turn_incomplete_window_green() -> Result<()> {
    for skipped in [false, true] {
        let f = ready_fixture::ready_mixed("2026-09-05T10:00:00.123456789Z".parse()?)?;
        assert!(f.report(0, 100)?.readiness_gate.can_start_tiny_execution);
        if skipped {
            f.sql("UPDATE execution_quote_canary_events SET decision_status='would_skip' WHERE signal_id='buy-1'")?;
        }
        f.sql("DELETE FROM execution_quote_canary_events WHERE signal_id='sell-1'")?;
        let full = f.report(0, 100)?;
        let limited = f.report(0, 30)?;
        assert!(!full.readiness_gate.can_start_tiny_execution);
        assert_eq!(
            (full.total_closed_trades, limited.total_closed_trades),
            (31, 30)
        );
        println!(
            "limit_observation={}",
            serde_json::json!({"skipped":skipped,"full":full,"limited":limited})
        );
        assert!(
            !limited.readiness_gate.can_start_tiny_execution,
            "display limit hid incomplete financial cohort; skipped={skipped}"
        );
    }
    Ok(())
}

#[test]
fn partial_sell_sample_observation() -> Result<()> {
    let f = fixture::Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit(1, "25", Some(2))?;
    f.exit(2, "75", Some(2))?;
    for (since, limit) in [(0, 100), (0, 1), (2, 1)] {
        let report = f.report(since, limit)?;
        println!(
            "partial_fee_observation={}",
            serde_json::json!({"since":since,"limit":limit,"report":report})
        );
        assert_eq!(report.trades[0].priority_fee_lamports_total, Some(7));
        assert_eq!(
            report.trades[0]
                .fee_allocation
                .buy_fee_allocated_lamports
                .as_deref(),
            Some("5")
        );
        assert_eq!(
            report.trades[0]
                .fee_allocation
                .buy_fee_remaining_lamports
                .as_deref(),
            Some("0")
        );
        assert!(
            (report.trades[0]
                .quote_adjusted_pnl_after_priority_fee_sol
                .unwrap()
                - (0.035 - 7e-9))
                .abs()
                < 1e-12
        );
    }
    Ok(())
}
