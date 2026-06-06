use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::{ExecutionQuoteCanaryEventInsert, SqliteStore};
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

fn open_migrated_store(name: &str) -> Result<SqliteStore> {
    let dir = tempdir()?;
    let db_path = dir.keep().join(format!("{name}.db"));
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    Ok(store)
}

#[test]
fn quote_pnl_summary_reports_buy_shadow_gate_outcomes() -> Result<()> {
    let store = open_migrated_store("execution-canary-quote-pnl-shadow-gate")?;
    let opened = ts("2026-06-02T12:30:00Z");

    record_buy(&store, "recorded", "RecordedToken", "would_execute", opened)?;
    store.record_execution_quote_canary_shadow_gate_event(
        "buy-shadow-recorded",
        "leader-wallet",
        "RecordedToken",
        "buy",
        "shadow_recorded",
        None,
        opened + Duration::milliseconds(15),
    )?;

    record_buy(
        &store,
        "dropped",
        "DroppedToken",
        "would_execute",
        opened + Duration::seconds(1),
    )?;
    store.record_execution_quote_canary_shadow_gate_event(
        "buy-shadow-dropped",
        "leader-wallet",
        "DroppedToken",
        "buy",
        "shadow_dropped",
        Some("recent_sell_cooldown"),
        opened + Duration::seconds(1),
    )?;

    record_buy(
        &store,
        "pending",
        "PendingToken",
        "would_skip",
        opened + Duration::seconds(2),
    )?;

    let summary = store.execution_canary_quote_pnl_summary(
        opened + Duration::seconds(3),
        opened - Duration::seconds(1),
        10,
    )?;
    let gate = &summary.buy_shadow_gate;

    assert_eq!(gate.total_buy_quote_events, 3);
    assert_eq!(gate.quote_would_execute_events, 2);
    assert_eq!(gate.quote_would_skip_events, 1);
    assert_eq!(gate.shadow_gate_sampled_events, 2);
    assert_eq!(gate.shadow_recorded_events, 1);
    assert_eq!(gate.shadow_dropped_events, 1);
    assert_eq!(gate.shadow_pending_events, 1);
    assert_eq!(gate.quote_would_execute_shadow_recorded_events, 1);
    assert_eq!(gate.quote_would_execute_shadow_dropped_events, 1);
    assert_eq!(gate.quote_would_execute_shadow_pending_events, 0);
    assert_eq!(gate.drop_reason_counts[0].reason, "recent_sell_cooldown");
    assert_eq!(gate.drop_reason_counts[0].events, 1);
    assert_eq!(
        gate.quote_would_execute_drop_reason_counts[0].reason,
        "recent_sell_cooldown"
    );
    assert_eq!(gate.quote_would_execute_drop_reason_counts[0].events, 1);
    assert_close(summary.readiness_gate.entry_shadow_gate_drop_rate_pct, 50.0);
    assert_eq!(
        readiness_check_status(&summary, "entry_shadow_gate"),
        Some("block")
    );
    Ok(())
}

#[test]
fn quote_pnl_gate_treats_strategy_shadow_drops_as_warnings() -> Result<()> {
    let store = open_migrated_store("execution-canary-quote-pnl-strategy-shadow-drops")?;
    let opened = ts("2026-06-02T13:30:00Z");

    for (suffix, token, reason, offset) in [
        ("below-notional", "BelowNotionalToken", "below_notional", 0),
        ("low-holders", "LowHoldersToken", "low_holders", 1),
    ] {
        record_buy(
            &store,
            suffix,
            token,
            "would_execute",
            opened + Duration::seconds(offset),
        )?;
        store.record_execution_quote_canary_shadow_gate_event(
            &format!("buy-shadow-{suffix}"),
            "leader-wallet",
            token,
            "buy",
            "shadow_dropped",
            Some(reason),
            opened + Duration::seconds(offset),
        )?;
    }

    let summary = store.execution_canary_quote_pnl_summary(
        opened + Duration::seconds(3),
        opened - Duration::seconds(1),
        10,
    )?;
    let gate = &summary.buy_shadow_gate;
    let entry_shadow_gate = readiness_check(&summary, "entry_shadow_gate").expect("check");

    assert_eq!(gate.quote_would_execute_events, 2);
    assert_eq!(gate.quote_would_execute_shadow_dropped_events, 2);
    assert_eq!(gate.quote_would_execute_drop_reason_counts.len(), 2);
    assert_eq!(entry_shadow_gate.status, "warn");
    assert!(entry_shadow_gate.value.contains("strategy_filtered=2"));
    assert!(entry_shadow_gate.value.contains("unexpected_dropped=0"));
    Ok(())
}

fn record_buy(
    store: &SqliteStore,
    suffix: &str,
    token: &str,
    decision_status: &str,
    opened: DateTime<Utc>,
) -> Result<()> {
    store
        .record_execution_quote_canary_event(&buy_quote(suffix, token, opened, decision_status))
        .map(|_| ())
}

fn buy_quote(
    suffix: &str,
    token: &str,
    opened: DateTime<Utc>,
    decision_status: &str,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: format!("quote:buy:shadow-{suffix}"),
        signal_id: Some(format!("buy-shadow-{suffix}")),
        shadow_closed_trade_id: None,
        wallet_id: "leader-wallet".to_string(),
        token: token.to_string(),
        side: "buy".to_string(),
        quote_status: "ok".to_string(),
        request_ts: opened + Duration::milliseconds(10),
        signal_ts: Some(opened),
        decision_delay_ms: Some(10),
        quote_latency_ms: Some(20),
        leader_notional_sol: Some(0.2),
        quote_in_amount_raw: Some("200000000".to_string()),
        quote_out_amount_raw: Some("100".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.002),
        shadow_price_sol: Some(0.002),
        slippage_bps: Some(15.0),
        price_impact_pct: Some(0.02),
        route_plan_json: None,
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(10_000),
        priority_fee_json: None,
        decision_status: Some(decision_status.to_string()),
        decision_reason: Some("inside_test_limit".to_string()),
        error: None,
    }
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 0.000_000_001,
        "actual={actual} expected={expected}"
    );
}

fn readiness_check_status<'a>(
    summary: &'a copybot_storage_core::ExecutionCanaryQuotePnlSummary,
    name: &str,
) -> Option<&'a str> {
    readiness_check(summary, name).map(|check| check.status.as_str())
}

fn readiness_check<'a>(
    summary: &'a copybot_storage_core::ExecutionCanaryQuotePnlSummary,
    name: &str,
) -> Option<&'a copybot_storage_core::ExecutionCanaryQuoteReadinessCheck> {
    summary
        .readiness_gate
        .checks
        .iter()
        .find(|check| check.name == name)
}
