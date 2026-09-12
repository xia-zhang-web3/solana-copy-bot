#[path = "common/quote_allocation_fixture.rs"]
mod fixture;
#[path = "common/quote_allocation_sql.rs"]
mod sql;
use anyhow::Result;
use chrono::Duration;
use fixture::Fixture;

#[test]
fn prior_since_limit_repeat_reopen_and_insert_order_preserve_fee() -> Result<()> {
    for reverse in [false, true] {
        let mut f = Fixture::new()?;
        f.buy("100", Some(7))?;
        for n in if reverse { [2, 1] } else { [1, 2] } {
            f.exit(n, if n == 1 { "25" } else { "75" }, Some(2))?;
        }
        for _ in 0..2 {
            let all = f.report(0, 100)?;
            for (since, limit) in [(0, 1), (2, 1), (2, 100)] {
                let r = f.report(since, limit)?;
                assert_eq!(r.trades[0], all.trades[0]);
                assert_eq!(r.trades[0].priority_fee_lamports_total, Some(7));
            }
            let fb = f
                .store
                .executable_wallet_feedback_since(f.opened + Duration::seconds(2))?;
            assert_eq!(fb["wallet"].samples, 1);
            assert_eq!(
                fb["wallet"].complete_pnl_after_priority_fee_sol(),
                all.trades[0].quote_adjusted_pnl_after_priority_fee_sol
            );
            f.reopen()?;
        }
    }
    Ok(())
}

#[test]
fn as_of_bounds_output_history_and_feedback_before_limit() -> Result<()> {
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit(1, "25", Some(2))?;
    f.exit(2, "75", Some(2))?;
    f.exit(3, "100", Some(2))?;
    let as_of = f.opened + Duration::seconds(1);
    let r = f
        .store
        .execution_canary_quote_pnl_summary(as_of, f.opened, 1)?;
    assert_eq!(r.total_closed_trades, 1);
    assert_eq!(r.trades[0].priority_fee_lamports_total, Some(4));
    assert_eq!(
        r.trades[0]
            .fee_allocation
            .buy_fee_remaining_lamports
            .as_deref(),
        Some("5")
    );
    let fb = f.store.executable_wallet_feedback_as_of(f.opened, as_of)?;
    assert_eq!(fb["wallet"].samples, 1);
    assert_eq!(fb["wallet"].unknown_samples, 0);
    assert_eq!(
        fb["wallet"].complete_pnl_after_priority_fee_sol(),
        r.quote_adjusted_pnl_after_priority_fee_sol
    );
    assert!(f
        .store
        .execution_canary_quote_pnl_summary(as_of - Duration::nanoseconds(1), f.opened, 100)?
        .trades
        .is_empty());
    assert_eq!(f.report(0, 100)?.unknown_trades, 1);
    Ok(())
}

#[test]
fn missing_error_skipped_nonmarket_and_removed_history_do_not_reset_budget() -> Result<()> {
    for (change, expected) in [
        ("DELETE FROM execution_quote_canary_events WHERE signal_id='sell-1'", None),
        ("UPDATE execution_quote_canary_events SET quote_status='error' WHERE signal_id='sell-1'", None),
        ("UPDATE execution_quote_canary_events SET decision_status='would_skip' WHERE signal_id='sell-1'", None),
        ("UPDATE shadow_closed_trades SET close_context='stale_quote_price' WHERE signal_id='sell-1'", Some(7)),
        ("DELETE FROM shadow_closed_trades WHERE signal_id='sell-1'", None),
    ] {
        let f = Fixture::new()?; f.buy("100", Some(7))?; f.exit(1, "25", Some(2))?; f.exit(2, "75", Some(2))?;
        f.sql(change)?;
        let r = f.report(2, 1)?;
        assert_eq!(r.total_closed_trades, 1);
        assert_eq!(r.trades[0].priority_fee_lamports_total, expected, "{change}");
        let fb = f.store.executable_wallet_feedback_since(f.opened + Duration::seconds(2))?;
        assert_eq!(fb["wallet"].samples, u64::from(expected.is_some()));
        assert_eq!(fb["wallet"].unknown_samples, u64::from(expected.is_none()));
    }
    Ok(())
}

#[test]
fn equal_close_instants_use_persisted_id_and_long_history_has_no_hidden_cap() -> Result<()> {
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit(1, "25", Some(2))?;
    f.exit(2, "75", Some(2))?;
    let same = (f.opened + Duration::seconds(1)).to_rfc3339();
    f.sql(&format!("UPDATE shadow_closed_trades SET closed_ts='{same}'; UPDATE execution_quote_canary_events SET signal_ts='{same}' WHERE side='sell'"))?;
    let all = f.report(0, 100)?;
    assert_eq!(
        all.trades
            .iter()
            .rev()
            .map(|t| t.priority_fee_lamports_total)
            .collect::<Vec<_>>(),
        vec![Some(4), Some(7)]
    );
    assert_eq!(f.report(0, 1)?.trades[0], all.trades[0]);
    let f = Fixture::new()?;
    f.buy("120", Some(7))?;
    for n in 1..=120 {
        f.exit(n, "1", Some(2))?;
    }
    let r = f.report(120, 1)?;
    assert_eq!(
        r.trades[0]
            .fee_allocation
            .cumulative_exit_quantity_raw
            .as_deref(),
        Some("120")
    );
    assert_eq!(
        r.trades[0]
            .fee_allocation
            .buy_fee_remaining_lamports
            .as_deref(),
        Some("0")
    );
    assert_eq!(r.trades[0].priority_fee_lamports_total, Some(2));
    Ok(())
}

#[test]
fn known_prior_quantity_survives_null_sell_fee_but_undated_orphan_is_unknown() -> Result<()> {
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit(1, "25", None)?;
    f.exit(2, "75", Some(2))?;
    assert_eq!(
        f.report(2, 1)?.trades[0].priority_fee_lamports_total,
        Some(7)
    );
    f.sql("DELETE FROM shadow_closed_trades WHERE signal_id='sell-1'; UPDATE execution_quote_canary_events SET signal_ts=NULL WHERE signal_id='sell-1'")?;
    let r = f.report(2, 1)?;
    assert_eq!(r.trades[0].reason, "missing_close_history");
    assert!(r.trades[0].priority_fee_lamports_total.is_none());
    Ok(())
}
