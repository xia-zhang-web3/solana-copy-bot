#[path = "common/quote_allocation_fixture.rs"]
mod fixture;
#[path = "common/quote_allocation_sql.rs"]
mod sql;
use anyhow::Result;
use chrono::Duration;
use fixture::Fixture;

#[test]
fn full_timestamp_disambiguates_same_second_buys_and_legacy_ids_remain_valid() -> Result<()> {
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit(1, "100", Some(2))?;
    let mut neighbor = f.event(true, 0, "200", Some(100));
    neighbor.event_id = "quote:entry:neighbor".into();
    neighbor.signal_id = Some("neighbor".into());
    neighbor.signal_ts = Some(f.opened + Duration::nanoseconds(1));
    neighbor.request_ts += Duration::nanoseconds(1);
    f.store.record_execution_quote_canary_event(&neighbor)?;
    let r = f.report(0, 10)?;
    assert_eq!(r.trades[0].priority_fee_lamports_total, Some(9));
    assert_eq!(
        r.trades[0].entry_quote_event_id.as_deref(),
        Some("quote:entry:buy")
    );
    f.sql("UPDATE execution_quote_canary_events SET event_id='old-buy' WHERE event_id='quote:entry:buy'; UPDATE execution_quote_canary_events SET event_id='old-sell' WHERE side='sell'")?;
    assert_eq!(
        f.report(0, 10)?.trades[0].priority_fee_lamports_total,
        Some(9)
    );
    // Equivalent full instants are compared as instants, not text/substr/julianday.
    let offset = f
        .opened
        .with_timezone(&chrono::FixedOffset::east_opt(3600).unwrap())
        .to_rfc3339();
    f.sql(&format!(
        "UPDATE execution_quote_canary_events SET signal_ts='{offset}' WHERE event_id='old-buy'"
    ))?;
    assert_eq!(
        f.report(0, 10)?.trades[0].priority_fee_lamports_total,
        Some(9)
    );
    Ok(())
}

#[test]
fn two_full_timestamp_buys_are_unknown_in_both_consumers() -> Result<()> {
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit(1, "100", Some(2))?;
    let mut duplicate = f.event(true, 0, "100", Some(999));
    duplicate.event_id = "quote:entry:second".into();
    duplicate.signal_id = Some("second".into());
    f.store.record_execution_quote_canary_event(&duplicate)?;
    let r = f.report(0, 100)?;
    assert_eq!(r.total_closed_trades, 1);
    assert_eq!(r.trades[0].reason, "ambiguous_entry_quote");
    assert!(r.quote_adjusted_pnl_after_priority_fee_sol.is_none());
    assert_eq!(
        f.store.executable_wallet_feedback_since(f.opened)?["wallet"].unknown_samples,
        1
    );
    Ok(())
}

#[test]
fn conflicting_bindings_do_not_form_duplicate_or_known_samples() -> Result<()> {
    for sql in [
        "UPDATE execution_quote_canary_events SET wallet_id='foreign' WHERE side='sell'",
        "UPDATE execution_quote_canary_events SET token='foreign' WHERE side='sell'",
        "UPDATE execution_quote_canary_events SET signal_id='foreign' WHERE side='sell'",
        "UPDATE execution_quote_canary_events SET side='buy' WHERE side='sell'",
        "UPDATE execution_quote_canary_events SET shadow_closed_trade_id=999 WHERE side='sell'",
        "UPDATE execution_quote_canary_events SET event_id='quote:close:999' WHERE side='sell'",
        "UPDATE execution_quote_canary_events SET signal_ts='2026-09-05T10:00:01.123456788+00:00' WHERE side='sell'",
        "UPDATE execution_quote_canary_events SET request_ts='2026-09-05T09:00:00+00:00' WHERE side='sell'",
        "UPDATE execution_quote_canary_events SET signal_id='wrong' WHERE side='buy'",
        "UPDATE execution_quote_canary_events SET shadow_closed_trade_id=999 WHERE side='buy'",
        "UPDATE execution_quote_canary_events SET wallet_id='foreign' WHERE side='buy'",
        "UPDATE execution_quote_canary_events SET token='foreign' WHERE side='buy'",
    ] {
        let f = Fixture::new()?; f.buy("100", Some(7))?; f.exit(1, "100", Some(2))?; f.sql(sql)?;
        let r = f.report(0, 100)?;
        assert_eq!(r.total_closed_trades, 1, "{sql}");
        assert_eq!(r.unknown_trades, 1, "{sql}");
        assert!(r.trades[0].priority_fee_lamports_total.is_none(), "{sql}");
        assert_eq!(f.store.executable_wallet_feedback_since(f.opened)?["wallet"].unknown_samples, 1, "{sql}");
    }
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    let id = f.exit(1, "100", Some(2))?;
    let mut extra = f.event(false, 1, "100", Some(999));
    extra.event_id = "legacy-duplicate-sell".into();
    extra.shadow_closed_trade_id = Some(id);
    f.store.record_execution_quote_canary_event(&extra)?;
    let r = f.report(0, 100)?;
    assert_eq!(r.total_closed_trades, 1);
    assert_eq!(r.trades[0].reason, "ambiguous_exit_quote");
    assert_eq!(
        f.store.executable_wallet_feedback_since(f.opened)?["wallet"].unknown_samples,
        1
    );
    Ok(())
}

#[test]
fn diagnostic_quotes_never_supply_entry_budget_or_ambiguity() -> Result<()> {
    let f = Fixture::new()?;
    f.buy("100", Some(7))?;
    f.exit(1, "100", Some(2))?;
    let mut diag = f.event(true, 0, "100", Some(1000));
    diag.event_id = "quote:entry-shadow-diag:buy".into();
    f.store.record_execution_quote_canary_event(&diag)?;
    assert_eq!(
        f.report(0, 10)?.trades[0].priority_fee_lamports_total,
        Some(9)
    );
    f.sql("DELETE FROM execution_quote_canary_events WHERE event_id='quote:entry:buy'")?;
    let r = f.report(0, 10)?;
    assert_eq!(r.trades[0].reason, "missing_entry_quote");
    assert_eq!(r.missing_entry_quote_trades, 1);
    Ok(())
}

#[test]
fn persisted_copy_signal_conflicts_are_rejected_when_available() -> Result<()> {
    for (side, column, value) in [
        ("buy", "wallet_id", "foreign"),
        ("buy", "token", "foreign"),
        ("buy", "side", "sell"),
        ("sell", "wallet_id", "foreign"),
        ("sell", "token", "foreign"),
        ("sell", "side", "buy"),
        ("sell", "ts", "2026-09-05T10:00:00+00:00"),
    ] {
        let f = Fixture::new()?;
        f.buy("100", Some(7))?;
        f.exit(1, "100", Some(2))?;
        let id = if side == "buy" { "buy" } else { "sell-1" };
        let time = if side == "buy" {
            f.opened
        } else {
            f.opened + Duration::seconds(1)
        };
        f.sql(&format!("INSERT INTO copy_signals(signal_id,wallet_id,side,token,notional_sol,ts,status) VALUES ('{id}','wallet','{side}','Token',0.1,'{}','shadow_recorded'); UPDATE copy_signals SET {column}='{value}' WHERE signal_id='{id}'",time.to_rfc3339()))?;
        let r = f.report(0, 10)?;
        assert_eq!(r.total_closed_trades, 1);
        assert!(
            r.trades[0].priority_fee_lamports_total.is_none(),
            "{side}/{column}"
        );
        assert_eq!(
            f.store.executable_wallet_feedback_since(f.opened)?["wallet"].unknown_samples,
            1
        );
    }
    Ok(())
}
