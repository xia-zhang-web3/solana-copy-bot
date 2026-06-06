use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, ExecutionQuoteCanaryProviderSampleInsert, SqliteStore,
    PROVIDER_GENERIC_METIS, PROVIDER_PUMP_FUN_PAID,
};
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
fn provider_comparison_reports_paid_vs_generic_delta() -> Result<()> {
    let store = open_migrated_store("execution-quote-provider-compare")?;
    let now = ts("2026-06-06T08:00:00Z");
    let event = quote_event(now);
    store.record_execution_quote_canary_event(&event)?;
    store.record_execution_quote_canary_provider_sample(&provider_sample(
        &event,
        PROVIDER_GENERIC_METIS,
        120,
        600.0,
        0.106,
        None,
    ))?;
    store.record_execution_quote_canary_provider_sample(&provider_sample(
        &event,
        PROVIDER_PUMP_FUN_PAID,
        80,
        250.0,
        0.1025,
        None,
    ))?;

    let summary = store.execution_quote_canary_provider_comparison_summary(
        now,
        now - chrono::Duration::seconds(1),
        10,
    )?;
    let latest = summary.latest.first().expect("provider comparison event");

    assert_eq!(summary.total_events, 1);
    assert_eq!(summary.paired_events, 1);
    assert_eq!(summary.both_ok_events, 1);
    assert_eq!(summary.pump_fun_better_slippage_events, 1);
    assert_eq!(summary.generic_better_slippage_events, 0);
    assert_eq!(summary.avg_generic_latency_ms, 120.0);
    assert_eq!(summary.avg_pump_fun_latency_ms, 80.0);
    assert_eq!(summary.avg_pump_fun_minus_generic_slippage_bps, -350.0);
    assert_eq!(
        latest.better_provider.as_deref(),
        Some(PROVIDER_PUMP_FUN_PAID)
    );
    assert_eq!(latest.slippage_delta_bps, Some(-350.0));
    assert_eq!(latest.latency_delta_ms, Some(-40));
    Ok(())
}

fn quote_event(request_ts: DateTime<Utc>) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: "quote:entry:provider-compare".to_string(),
        signal_id: Some("provider-compare".to_string()),
        shadow_closed_trade_id: None,
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMint".to_string(),
        side: "buy".to_string(),
        quote_status: "ok".to_string(),
        request_ts,
        signal_ts: Some(request_ts),
        decision_delay_ms: Some(7),
        quote_latency_ms: Some(120),
        leader_notional_sol: Some(0.2),
        quote_in_amount_raw: Some("200000000".to_string()),
        quote_out_amount_raw: Some("2000000".to_string()),
        quote_response_json: Some("{\"routePlan\":[]}".to_string()),
        quote_price_sol: Some(0.106),
        shadow_price_sol: Some(0.1),
        slippage_bps: Some(600.0),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[]".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(10_000),
        priority_fee_json: Some("{\"recommended\":10000}".to_string()),
        decision_status: Some("would_skip".to_string()),
        decision_reason: Some("slippage_above_limit".to_string()),
        error: None,
    }
}

fn provider_sample(
    event: &ExecutionQuoteCanaryEventInsert,
    provider: &str,
    latency_ms: u64,
    slippage_bps: f64,
    quote_price_sol: f64,
    error: Option<String>,
) -> ExecutionQuoteCanaryProviderSampleInsert {
    ExecutionQuoteCanaryProviderSampleInsert {
        event_id: event.event_id.clone(),
        provider: provider.to_string(),
        side: event.side.clone(),
        quote_status: if error.is_some() { "error" } else { "ok" }.to_string(),
        request_ts: event.request_ts,
        quote_latency_ms: Some(latency_ms),
        quote_in_amount_raw: event.quote_in_amount_raw.clone(),
        quote_out_amount_raw: event.quote_out_amount_raw.clone(),
        quote_response_json: event.quote_response_json.clone(),
        quote_price_sol: Some(quote_price_sol),
        shadow_price_sol: event.shadow_price_sol,
        slippage_bps: Some(slippage_bps),
        price_impact_pct: event.price_impact_pct,
        route_plan_json: event.route_plan_json.clone(),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("within_slippage_limit".to_string()),
        error,
    }
}
