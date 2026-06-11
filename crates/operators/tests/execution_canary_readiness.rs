use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_operators::execution_canary_readiness::{build_report_from_db_path, parse_args_from};
use copybot_storage_core::{ExecutionCanaryBuildPlanMetadata, SqliteStore};
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

#[test]
fn execution_canary_readiness_operator_reports_latest_metadata() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteStore::open(&db_path)?;
    store.ensure_history_retention_tables()?;
    let now = ts("2026-06-02T12:00:00Z");
    let reserve = store.reserve_execution_canary_order("buy-operator", "metis-canary", now)?;
    store.record_execution_canary_build_plan_metadata(&ExecutionCanaryBuildPlanMetadata {
        order_id: reserve.order.order_id.clone(),
        signal_id: reserve.order.signal_id.clone(),
        client_order_id: reserve.order.client_order_id.clone(),
        recorded_ts: now + chrono::Duration::milliseconds(25),
        quote_source: Some("execution_quote_canary_event".to_string()),
        quote_event_id: Some("quote:operator".to_string()),
        quote_status: Some("ok".to_string()),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("123456".to_string()),
        quote_response_json: None,
        quote_request_ts: None,
        quote_price_sol: Some(0.000081),
        price_impact_pct: Some(0.04),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_source: Some("execution_quote_canary_event".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(12_345),
        priority_fee_json: Some("{\"recommended\":12345}".to_string()),
        slippage_bps: Some(12.5),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("inside_limits".to_string()),
    })?;
    drop(store);

    let report = build_report_from_db_path(&db_path, now + chrono::Duration::seconds(1));
    let summary = report.summary.expect("operator summary should exist");
    let window = report.window.expect("operator window should exist");
    let latest = summary.latest.expect("latest order should be present");

    assert_eq!(report.reason_class, "execution_canary_readiness_loaded");
    assert!(report.db_opened);
    assert!(report.readiness_green);
    assert!(!report.production_green);
    assert_eq!(summary.readiness_status, "would_enter");
    assert_eq!(window.total_orders, 1);
    assert_eq!(window.would_enter_orders, 1);
    assert_eq!(window.latest_priority_fee_lamports, Some(12_345));
    assert_eq!(latest.quote_event_id.as_deref(), Some("quote:operator"));
    assert_eq!(latest.priority_fee_lamports, Some(12_345));
    Ok(())
}

#[test]
fn execution_canary_readiness_cli_accepts_config_or_db_path() -> Result<()> {
    let by_db = parse_args_from([
        "--db-path",
        "state/live_runtime.db",
        "--json",
        "--limit",
        "25",
    ])?;
    let by_config = parse_args_from(["--config", "/etc/solana-copy-bot/live.toml", "--json"])?;

    assert!(by_db.db_path.is_some());
    assert_eq!(by_db.limit, 25);
    assert!(by_config.config_path.is_some());
    assert_eq!(by_config.limit, 50);
    assert!(parse_args_from(["--json"]).is_err());
    assert!(parse_args_from(["--db-path", "db.sqlite", "--json", "--limit", "0"]).is_err());
    Ok(())
}
