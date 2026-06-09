use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_operators::execution_canary_quote_pnl::build_report_from_db_path;
use copybot_storage_core::{
    ExecutionCanaryBuildPlanMetadata, ExecutionQuoteCanaryEventInsert, SqliteStore,
    EXECUTION_ERROR_SIMULATION_FAILED, EXECUTION_SIMULATION_STATUS_FAILED,
};
use tempfile::tempdir;

const ROUTE: &str = "metis-swap-instructions-dry-run";

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

#[test]
fn quality_summary_flags_entry_flow_loss() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;

    let opened_ts = ts("2026-06-09T16:54:29Z");
    let closed_ts = opened_ts + Duration::seconds(45);
    store.insert_shadow_closed_trade_exact(
        "sell-market-not-found",
        "leader-wallet",
        "TokenMarketNotFound",
        100.0,
        None,
        0.10,
        0.11,
        0.01,
        opened_ts,
        closed_ts,
    )?;
    insert_signal(&store, "buy-market-not-found", "buy", opened_ts)?;
    store.record_execution_quote_canary_event(&quote_event(opened_ts))?;
    record_failed_buy_order(&store, opened_ts)?;
    drop(store);

    let report = build_report_from_db_path(&db_path, ts("2026-06-09T17:00:00Z"));
    let quality = report
        .tiny_execution_quality
        .as_ref()
        .unwrap_or_else(|| panic!("quality report missing: {:?}", report.error));

    assert_eq!(report.reason_class, "execution_canary_quote_pnl_loaded");
    assert_eq!(quality.verdict, "entry_flow_loss");
    assert_eq!(quality.quote_canary_entry_would_execute_trades, 1);
    assert_eq!(quality.tiny_entry_ordered_trades, 1);
    assert_eq!(quality.tiny_entry_confirmed_trades, 0);
    assert_eq!(quality.tiny_entry_failed_orders, 1);
    assert_eq!(quality.entry_confirmed_coverage_pct, 0.0);
    assert_eq!(quality.tiny_exit_failed_orders, 0);
    assert!(quality.top_flow_blockers.iter().any(|blocker| {
        blocker.stage == "entry_order"
            && blocker.reason == "simulation_failed:other"
            && blocker.count == 1
    }));
    Ok(())
}

fn insert_signal(
    store: &SqliteStore,
    signal_id: &str,
    side: &str,
    signal_ts: DateTime<Utc>,
) -> Result<()> {
    store.insert_copy_signal(&CopySignalRow {
        signal_id: signal_id.to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: side.to_string(),
        token: "TokenMarketNotFound".to_string(),
        notional_sol: 0.01,
        notional_lamports: Some(Lamports::new(10_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: signal_ts,
        status: "shadow_recorded".to_string(),
    })?;
    Ok(())
}

fn record_failed_buy_order(store: &SqliteStore, signal_ts: DateTime<Utc>) -> Result<()> {
    let reserve = store.reserve_execution_canary_order(
        "buy-market-not-found",
        ROUTE,
        signal_ts + Duration::milliseconds(100),
    )?;
    store.record_execution_canary_build_plan_metadata(&ExecutionCanaryBuildPlanMetadata {
        order_id: reserve.order.order_id.clone(),
        signal_id: reserve.order.signal_id.clone(),
        client_order_id: reserve.order.client_order_id.clone(),
        recorded_ts: signal_ts + Duration::milliseconds(10),
        quote_source: Some("execution_quote_canary_provider:generic_metis".to_string()),
        quote_event_id: Some("quote:entry:market-not-found".to_string()),
        quote_status: Some("ok".to_string()),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("500000".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.0001),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
        priority_fee_source: Some("execution_quote_canary_event".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(747_449),
        priority_fee_json: Some("{\"recommended\":747449}".to_string()),
        slippage_bps: Some(12.5),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("fresh_submit_quote_within_slippage_limit".to_string()),
    })?;
    store.mark_execution_canary_built(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(120),
    )?;
    let error = "swap-instructions dry-run returned HTTP 400 Bad Request: Market not found";
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(140),
        EXECUTION_SIMULATION_STATUS_FAILED,
        Some(error),
    )?;
    store.mark_execution_canary_failed(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(160),
        EXECUTION_ERROR_SIMULATION_FAILED,
        error,
    )?;
    Ok(())
}

fn quote_event(signal_ts: DateTime<Utc>) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: "quote:entry:market-not-found".to_string(),
        signal_id: Some("buy-market-not-found".to_string()),
        shadow_closed_trade_id: None,
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMarketNotFound".to_string(),
        side: "buy".to_string(),
        quote_status: "ok".to_string(),
        request_ts: signal_ts + Duration::milliseconds(10),
        signal_ts: Some(signal_ts),
        decision_delay_ms: Some(20),
        quote_latency_ms: Some(80),
        leader_notional_sol: Some(0.2),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("500000".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.0001),
        shadow_price_sol: Some(0.0001),
        slippage_bps: Some(15.0),
        price_impact_pct: Some(0.02),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(747_449),
        priority_fee_json: Some("{\"recommended\":747449}".to_string()),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("fresh_submit_quote_within_slippage_limit".to_string()),
        error: None,
    }
}
