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
            && blocker.reason == "simulation_failed:market_not_found"
            && blocker.count == 1
    }));
    Ok(())
}

#[test]
fn quality_summary_flags_exit_flow_loss() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;

    let opened_ts = ts("2026-06-09T16:55:00Z");
    let closed_ts = opened_ts + Duration::seconds(30);
    store.insert_shadow_closed_trade_exact(
        "sell-exit-missing",
        "leader-wallet",
        "TokenMarketNotFound",
        100.0,
        None,
        0.10,
        0.12,
        0.02,
        opened_ts,
        closed_ts,
    )?;
    let close_id = store
        .list_execution_quote_canary_close_candidates_for_signal("sell-exit-missing", 10)?
        .into_iter()
        .next()
        .expect("close candidate")
        .id;
    insert_signal(&store, "buy-exit-missing", "buy", opened_ts)?;
    insert_signal(&store, "sell-exit-missing", "sell", closed_ts)?;
    store.record_execution_quote_canary_event(&quote_event_for(
        "quote:entry:exit-missing",
        Some("buy-exit-missing"),
        None,
        "buy",
        opened_ts,
    ))?;
    store.record_execution_quote_canary_event(&quote_event_for(
        "quote:exit:exit-missing",
        Some("sell-exit-missing"),
        Some(close_id),
        "sell",
        closed_ts,
    ))?;
    let buy_order_id = record_confirmed_buy_order(&store, opened_ts)?;
    store.record_execution_canary_confirmed_buy_fill(
        &buy_order_id,
        "TokenMarketNotFound",
        100.0,
        None,
        0.01,
        opened_ts + Duration::milliseconds(1600),
    )?;
    drop(store);

    let report = build_report_from_db_path(&db_path, ts("2026-06-09T17:00:00Z"));
    let quality = report
        .tiny_execution_quality
        .as_ref()
        .unwrap_or_else(|| panic!("quality report missing: {:?}", report.error));

    assert_eq!(quality.verdict, "sell_flow_loss");
    assert_eq!(quality.tiny_entry_confirmed_trades, 1);
    assert_eq!(quality.quote_canary_exit_would_execute_trades, 1);
    assert_eq!(quality.tiny_exit_ordered_trades, 0);
    assert_eq!(quality.tiny_exit_missing_orders, 1);
    assert!(quality.top_flow_blockers.iter().any(|blocker| {
        blocker.stage == "exit_order"
            && blocker.reason == "missing_tiny_sell_order_after_would_execute"
            && blocker.count == 1
    }));
    Ok(())
}

#[test]
fn quality_does_not_flag_missing_exit_when_position_is_already_closed() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;

    let opened_ts = ts("2026-06-09T16:56:00Z");
    let closed_ts = opened_ts + Duration::seconds(30);
    store.insert_shadow_closed_trade_exact(
        "sell-merged-close",
        "leader-wallet",
        "TokenMarketNotFound",
        100.0,
        None,
        0.10,
        0.10,
        0.0,
        opened_ts,
        closed_ts,
    )?;
    let close_id = store
        .list_execution_quote_canary_close_candidates_for_signal("sell-merged-close", 10)?
        .into_iter()
        .next()
        .expect("close candidate")
        .id;
    insert_signal(&store, "buy-exit-missing", "buy", opened_ts)?;
    insert_signal(&store, "sell-merged-close", "sell", closed_ts)?;
    insert_signal(&store, "sell-other-close", "sell", closed_ts)?;
    store.record_execution_quote_canary_event(&quote_event_for(
        "quote:entry:closed-position",
        Some("buy-exit-missing"),
        None,
        "buy",
        opened_ts,
    ))?;
    store.record_execution_quote_canary_event(&quote_event_for(
        "quote:exit:closed-position",
        Some("sell-merged-close"),
        Some(close_id),
        "sell",
        closed_ts,
    ))?;
    let buy_order_id = record_confirmed_buy_order(&store, opened_ts)?;
    store.record_execution_canary_confirmed_buy_fill(
        &buy_order_id,
        "TokenMarketNotFound",
        100.0,
        None,
        0.01,
        opened_ts + Duration::milliseconds(1600),
    )?;
    let sell_order_id = record_confirmed_sell_order(&store, closed_ts)?;
    store.close_execution_canary_confirmed_sell_fill(
        &sell_order_id,
        "TokenMarketNotFound",
        100.0,
        None,
        0.0002,
        0.000001,
        closed_ts + Duration::milliseconds(1800),
    )?;
    drop(store);

    let report = build_report_from_db_path(&db_path, ts("2026-06-09T17:00:00Z"));
    let quality = report
        .tiny_execution_quality
        .as_ref()
        .unwrap_or_else(|| panic!("quality report missing: {:?}", report.error));
    let proof = report
        .tiny_execution_proof
        .as_ref()
        .unwrap_or_else(|| panic!("proof report missing: {:?}", report.error));
    let trade = proof.trades.first().expect("proof trade");

    assert_eq!(trade.proof_status, "tiny_closed");
    assert_eq!(trade.proof_reason, "closed_without_matched_exit_order");
    assert_eq!(proof.summary.tiny_unique_closed_positions, 1);
    assert_eq!(quality.tiny_exit_missing_orders, 0);
    assert!(!quality.top_flow_blockers.iter().any(|blocker| {
        blocker.stage == "exit_order"
            && blocker.reason == "missing_tiny_sell_order_after_would_execute"
    }));
    assert_ne!(quality.verdict, "sell_flow_loss");
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

fn record_confirmed_buy_order(store: &SqliteStore, signal_ts: DateTime<Utc>) -> Result<String> {
    let reserve = store.reserve_execution_canary_order(
        "buy-exit-missing",
        ROUTE,
        signal_ts + Duration::milliseconds(100),
    )?;
    store.record_execution_canary_build_plan_metadata(&ExecutionCanaryBuildPlanMetadata {
        order_id: reserve.order.order_id.clone(),
        signal_id: reserve.order.signal_id.clone(),
        client_order_id: reserve.order.client_order_id.clone(),
        recorded_ts: signal_ts + Duration::milliseconds(10),
        quote_source: Some("execution_quote_canary_provider:generic_metis".to_string()),
        quote_event_id: Some("quote:entry:exit-missing".to_string()),
        quote_status: Some("ok".to_string()),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("500000".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.0001),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
        priority_fee_source: Some("execution_quote_canary_event".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(900_000),
        priority_fee_json: Some("{\"recommended\":900000}".to_string()),
        slippage_bps: Some(12.5),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("fresh_submit_quote_within_slippage_limit".to_string()),
    })?;
    store.mark_execution_canary_built(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(120),
    )?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(140),
        "passed",
        None,
    )?;
    store.mark_execution_canary_submitted(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(500),
        "buy-signature",
    )?;
    store.mark_execution_canary_confirmed(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(1500),
    )?;
    Ok(reserve.order.order_id)
}

fn record_confirmed_sell_order(store: &SqliteStore, signal_ts: DateTime<Utc>) -> Result<String> {
    let reserve = store.reserve_execution_canary_order(
        "sell-other-close",
        ROUTE,
        signal_ts + Duration::milliseconds(100),
    )?;
    store.record_execution_canary_build_plan_metadata(&ExecutionCanaryBuildPlanMetadata {
        order_id: reserve.order.order_id.clone(),
        signal_id: reserve.order.signal_id.clone(),
        client_order_id: reserve.order.client_order_id.clone(),
        recorded_ts: signal_ts + Duration::milliseconds(10),
        quote_source: Some("execution_quote_canary_provider:generic_metis".to_string()),
        quote_event_id: Some("quote:exit:other-close".to_string()),
        quote_status: Some("ok".to_string()),
        quote_in_amount_raw: Some("500000".to_string()),
        quote_out_amount_raw: Some("20000000".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.0002),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
        priority_fee_source: Some("execution_quote_canary_event".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(900_000),
        priority_fee_json: Some("{\"recommended\":900000}".to_string()),
        slippage_bps: Some(12.5),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("owned_position".to_string()),
    })?;
    store.mark_execution_canary_built(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(120),
    )?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(140),
        "passed",
        None,
    )?;
    store.mark_execution_canary_submitted(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(500),
        "sell-signature",
    )?;
    store.mark_execution_canary_confirmed(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(1500),
    )?;
    Ok(reserve.order.order_id)
}

fn quote_event(signal_ts: DateTime<Utc>) -> ExecutionQuoteCanaryEventInsert {
    quote_event_for(
        "quote:entry:market-not-found",
        Some("buy-market-not-found"),
        None,
        "buy",
        signal_ts,
    )
}

fn quote_event_for(
    event_id: &str,
    signal_id: Option<&str>,
    close_id: Option<i64>,
    side: &str,
    signal_ts: DateTime<Utc>,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id: signal_id.map(ToString::to_string),
        shadow_closed_trade_id: close_id,
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMarketNotFound".to_string(),
        side: side.to_string(),
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
