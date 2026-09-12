#[path = "common/quote_allocation_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use copybot_core_types::{CopySignalRow, Lamports};
use copybot_storage_core::*;
use fixture::Fixture;
use rusqlite::Connection;

const TABLES: [&str; 3] = [
    "execution_quote_canary_events",
    "execution_quote_canary_provider_samples",
    "execution_canary_build_plan_metadata",
];
fn connection(f: &Fixture) -> Result<Connection> {
    Ok(Connection::open(f.dir.path().join("allocation.db"))?)
}
fn signal(f: &Fixture, side: &str) -> CopySignalRow {
    CopySignalRow {
        signal_id: "timing-signal".into(),
        wallet_id: "wallet".into(),
        side: side.into(),
        token: "Token".into(),
        notional_sol: 0.1,
        notional_lamports: Some(Lamports::new(100_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: f.opened,
        status: "execution_sell_intent".into(),
    }
}
fn provider(e: &ExecutionQuoteCanaryEventInsert) -> ExecutionQuoteCanaryProviderSampleInsert {
    ExecutionQuoteCanaryProviderSampleInsert {
        http_request_started_ts: e.http_request_started_ts,
        quote_response_available_ts: e.quote_response_available_ts,
        event_id: e.event_id.clone(),
        provider: PROVIDER_GENERIC_METIS.into(),
        side: e.side.clone(),
        quote_status: e.quote_status.clone(),
        request_ts: e.request_ts,
        quote_latency_ms: e.quote_latency_ms,
        quote_in_amount_raw: e.quote_in_amount_raw.clone(),
        quote_out_amount_raw: e.quote_out_amount_raw.clone(),
        quote_response_json: e.quote_response_json.clone(),
        quote_price_sol: e.quote_price_sol,
        shadow_price_sol: e.shadow_price_sol,
        slippage_bps: e.slippage_bps,
        price_impact_pct: e.price_impact_pct,
        route_plan_json: e.route_plan_json.clone(),
        decision_status: e.decision_status.clone(),
        decision_reason: e.decision_reason.clone(),
        error: None,
    }
}
fn metadata(f: &Fixture, order_id: &str, client: &str) -> ExecutionCanaryBuildPlanMetadata {
    ExecutionCanaryBuildPlanMetadata {
        http_request_started_ts: None,
        quote_response_available_ts: None,
        order_id: order_id.into(),
        signal_id: "timing-signal".into(),
        client_order_id: client.into(),
        quote_event_id: Some("quote:entry:buy".into()),
        quote_request_ts: Some(f.opened),
        quote_source: Some("execution_quote_canary_event".into()),
        quote_status: Some("ok".into()),
        quote_in_amount_raw: Some("100000000".into()),
        quote_out_amount_raw: Some("10".into()),
        quote_response_json: Some("{}".into()),
        quote_price_sol: Some(0.1),
        price_impact_pct: None,
        route_plan_json: None,
        priority_fee_source: None,
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        slippage_bps: None,
        decision_status: Some("would_execute".into()),
        decision_reason: None,
        recorded_ts: f.opened,
    }
}
#[test]
fn legacy_rows_missing_migration_reopen_and_no_backfill_are_explicit_unknown() -> Result<()> {
    let mut f = Fixture::new()?;
    let event = f.event(true, 0, "10", Some(5));
    f.store.record_execution_quote_canary_event(&event)?;
    f.store
        .record_execution_quote_canary_provider_sample(&provider(&event))?;
    f.store.insert_copy_signal(&signal(&f, "buy"))?;
    let order = f
        .store
        .reserve_execution_canary_order("timing-signal", "metis", f.opened)?
        .order;
    let legacy = metadata(&f, &order.order_id, &order.client_order_id);
    f.store
        .record_execution_canary_build_plan_metadata(&legacy)?;
    let conn = connection(&f)?;
    conn.execute(
        "DELETE FROM schema_migrations WHERE version=?1",
        [QUOTE_HTTP_TIMING_MIGRATION],
    )?;
    for table in TABLES {
        conn.execute_batch(&format!(
            "ALTER TABLE {table} DROP COLUMN http_request_started_ts"
        ))?;
    }
    f.reopen()?;
    let loaded = f
        .store
        .load_execution_quote_canary_event_by_id(&event.event_id)?
        .unwrap();
    assert_eq!(
        (
            loaded.http_request_started_ts,
            loaded.decision_delay_ms,
            loaded.quote_latency_ms
        ),
        (None, None, None)
    );
    let sample = f
        .store
        .load_execution_quote_canary_provider_sample(&event.event_id, PROVIDER_GENERIC_METIS)?
        .unwrap();
    assert_eq!(
        (sample.http_request_started_ts, sample.quote_latency_ms),
        (None, None)
    );
    let saved = f
        .store
        .load_execution_canary_build_plan_metadata(&order.order_id)?
        .unwrap();
    assert_eq!(saved.http_request_started_ts, None);
    assert_eq!(saved.quote_request_ts, legacy.quote_request_ts);
    let mut old_json = serde_json::to_value(&saved)?;
    old_json
        .as_object_mut()
        .unwrap()
        .remove("http_request_started_ts");
    let decoded: ExecutionCanaryBuildPlanMetadata = serde_json::from_value(old_json)?;
    assert_eq!(decoded.http_request_started_ts, None);
    let mut actual = event.clone();
    actual.http_request_started_ts = Some(f.opened);
    assert!(f
        .store
        .record_execution_quote_canary_event(&actual)
        .is_err());
    f.store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    for table in TABLES {
        assert!(quote_http_timing_available(&conn, table)?);
        assert_eq!(
            conn.query_row(
                &format!("SELECT count(*) FROM {table} WHERE http_request_started_ts IS NOT NULL"),
                [],
                |r| r.get::<_, i64>(0)
            )?,
            0
        );
    }
    let mut current = saved;
    current.http_request_started_ts = Some(f.opened + Duration::milliseconds(500));
    current.recorded_ts += Duration::seconds(1);
    f.store
        .record_execution_canary_build_plan_metadata(&current)?;
    f.reopen()?;
    assert_eq!(
        f.store
            .load_execution_canary_build_plan_metadata(&order.order_id)?
            .unwrap(),
        current
    );
    Ok(())
}
#[test]
fn damaged_applied_migration_is_an_error_instead_of_legacy_or_zero() -> Result<()> {
    for missing in TABLES {
        let mut f = Fixture::new()?;
        let event = f.event(true, 0, "10", Some(5));
        f.store.record_execution_quote_canary_event(&event)?;
        let conn = connection(&f)?;
        conn.execute_batch(&format!(
            "ALTER TABLE {missing} DROP COLUMN http_request_started_ts"
        ))?;
        f.reopen()?;
        assert!(f
            .store
            .load_execution_quote_canary_event_by_id(&event.event_id)
            .is_err());
        assert!(f
            .store
            .load_execution_quote_canary_provider_sample(&event.event_id, PROVIDER_GENERIC_METIS)
            .is_err());
        assert!(f
            .store
            .load_execution_canary_build_plan_metadata("absent")
            .is_err());
        assert!(f.report(-1, 10).is_err());
        assert!(f
            .store
            .run_migrations(std::path::Path::new(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../migrations"
            )))
            .is_err());
    }
    Ok(())
}
#[test]
fn current_sell_sample_updates_but_stale_version_cannot_replace_payload_or_time() -> Result<()> {
    let f = Fixture::new()?;
    f.store.insert_copy_signal(&signal(&f, "sell"))?;
    let mut event = f.event(false, 0, "10", Some(5));
    event.signal_id = Some("timing-signal".into());
    event.shadow_closed_trade_id = None;
    event.http_request_started_ts = Some(event.request_ts + Duration::milliseconds(400));
    f.store.record_execution_quote_canary_event(&event)?;
    let old = provider(&event);
    f.store
        .record_execution_quote_canary_provider_sample(&old)?;
    event.request_ts += Duration::seconds(1);
    event.http_request_started_ts = Some(event.request_ts + Duration::milliseconds(600));
    event.quote_out_amount_raw = Some("200000000".into());
    f.store.record_execution_quote_canary_event(&event)?;
    let fresh = provider(&event);
    f.store
        .record_execution_quote_canary_provider_sample(&fresh)?;
    f.store
        .record_execution_quote_canary_provider_sample(&old)?;
    let loaded = f
        .store
        .load_execution_quote_canary_provider_sample(&event.event_id, PROVIDER_GENERIC_METIS)?
        .unwrap();
    assert_eq!(loaded, fresh);
    let event_loaded = f
        .store
        .load_execution_quote_canary_event_by_id(&event.event_id)?
        .unwrap();
    assert_eq!(
        event_loaded.http_request_started_ts,
        event.http_request_started_ts
    );
    assert_eq!(event_loaded.decision_delay_ms, Some(1610));
    Ok(())
}
#[test]
fn timing_coverage_keeps_the_financial_cohort_and_blocks_empty_or_mixed_readiness() -> Result<()> {
    let f = Fixture::new()?;
    let empty = f.report(-1, 10)?;
    assert_ne!(empty.readiness_gate.status, "ready");
    assert!(empty
        .readiness_gate
        .checks
        .iter()
        .any(|c| c.value == "unknown"));
    f.buy("10", Some(5))?;
    f.exit(1, "5", Some(5))?;
    f.exit(2, "5", Some(5))?;
    // Two independently bound entries permit partial actual coverage without changing economics.
    let conn = connection(&f)?;
    let mut second_buy = f.event(true, 1, "10", Some(5));
    second_buy.event_id = "quote:entry:buy2".into();
    second_buy.signal_id = Some("buy2".into());
    f.store.record_execution_quote_canary_event(&second_buy)?;
    conn.execute(
        "UPDATE shadow_closed_trades SET opened_ts=?1 WHERE signal_id='sell-2'",
        [(f.opened + Duration::seconds(1)).to_rfc3339()],
    )?;
    let legacy = f.report(-1, 10)?;
    assert_eq!(legacy.total_closed_trades, 2);
    assert_eq!(legacy.pnl_counted_trades, 2);
    let conn = connection(&f)?;
    conn.execute("UPDATE execution_quote_canary_events SET http_request_started_ts=?1 WHERE side='sell' AND signal_id='sell-1'",[(f.opened+Duration::seconds(1)+Duration::milliseconds(410)).to_rfc3339()])?;
    conn.execute("UPDATE execution_quote_canary_events SET http_request_started_ts=?1 WHERE event_id='quote:entry:buy2'",[(f.opened+Duration::seconds(1)+Duration::milliseconds(410)).to_rfc3339()])?;
    let mixed = f.report(-1, 10)?;
    assert_eq!(
        mixed.quote_diagnostics.entry_all.decision_delay_ms_samples,
        1
    );
    assert_eq!(
        mixed.quote_diagnostics.entry_all.decision_delay_ms_unknown,
        1
    );
    assert!(mixed
        .readiness_gate
        .checks
        .iter()
        .any(|c| c.value.contains("partial") && c.status == "block"));
    assert_eq!(mixed.total_closed_trades, legacy.total_closed_trades);
    assert_eq!(mixed.pnl_counted_trades, legacy.pnl_counted_trades);
    for (left, right) in legacy.trades.iter().zip(&mixed.trades) {
        assert_eq!(
            left.quote_adjusted_pnl_after_priority_fee_sol,
            right.quote_adjusted_pnl_after_priority_fee_sol
        );
        assert_eq!(left.fee_allocation, right.fee_allocation);
    }
    assert!(mixed
        .trades
        .iter()
        .any(|t| t.exit_decision_delay_ms == Some(410)));
    assert!(mixed
        .trades
        .iter()
        .any(|t| t.exit_decision_delay_ms.is_none()));
    assert!(mixed
        .readiness_gate
        .checks
        .iter()
        .filter(|c| c.value == "unknown")
        .all(|c| c.status != "pass"));
    let json = serde_json::to_value(&mixed)?;
    assert!(!json.to_string().contains("NaN"));
    Ok(())
}
