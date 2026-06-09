use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_operators::execution_canary_quote_pnl::build_report_from_db_path;
use copybot_operators::execution_canary_quote_pnl_sell_side::SellSideDiagnosticsReport;
use copybot_storage_core::{
    ExecutionCanaryBuildPlanMetadata, ExecutionQuoteCanaryEventInsert, SqliteStore,
    EXECUTION_ERROR_BUILD_FAILED, EXECUTION_ERROR_SIMULATION_FAILED,
    EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE, EXECUTION_ERROR_TERMINAL_SELL_SIMULATION_FAILED,
    EXECUTION_SIMULATION_STATUS_FAILED,
};
use tempfile::tempdir;

const ROUTE: &str = "metis-swap-instructions-dry-run";

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

#[test]
fn quote_pnl_report_groups_sell_side_failure_diagnostics() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = ts("2026-06-02T12:00:00Z");

    record_simulation_failed_order(&store, "sell-1788", "Token1788", now)?;
    record_terminal_no_route_order(
        &store,
        "sell-no-route",
        "TokenNoRoute",
        now + Duration::seconds(1),
    )?;
    record_terminal_simulation_order(
        &store,
        "sell-terminal-sim",
        "TokenTerminalSimulation",
        now + Duration::seconds(2),
    )?;
    store.record_execution_canary_open_position(
        "open-position",
        "TokenStillOpen",
        10.0,
        None,
        0.01,
        now,
    )?;
    drop(store);

    let report = build_report_from_db_path(&db_path, ts("2026-06-02T13:00:00Z"));
    let diagnostics = report
        .sell_side_diagnostics
        .as_ref()
        .unwrap_or_else(|| panic!("sell diagnostics missing: {:?}", report.error));

    assert_eq!(diagnostics.recent_sell_orders, 3);
    assert_eq!(diagnostics.failed_sell_orders, 3);
    assert_eq!(diagnostics.simulation_failed_orders, 1);
    assert_eq!(diagnostics.terminal_no_route_orders, 1);
    assert_eq!(diagnostics.terminal_simulation_orders, 1);
    assert_eq!(diagnostics.open_position_count, 1);
    assert_eq!(diagnostics.open_position_tokens, ["TokenStillOpen"]);
    assert_token_class(
        diagnostics,
        "Token1788",
        EXECUTION_ERROR_SIMULATION_FAILED,
        "custom_program_error:0x1788",
        "inspect_simulation_error_and_route_source",
    );
    assert_token_class(
        diagnostics,
        "TokenNoRoute",
        EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE,
        "terminal_no_route",
        "inspect_terminal_no_route_proof",
    );
    assert_token_class(
        diagnostics,
        "TokenTerminalSimulation",
        EXECUTION_ERROR_TERMINAL_SELL_SIMULATION_FAILED,
        "custom_program_error:0x1788",
        "inspect_terminal_simulation_proof",
    );
    Ok(())
}

fn record_simulation_failed_order(
    store: &SqliteStore,
    signal_id: &str,
    token: &str,
    signal_ts: DateTime<Utc>,
) -> Result<()> {
    insert_signal(store, signal_id, token, signal_ts)?;
    let reserve = store.reserve_execution_canary_order(signal_id, ROUTE, signal_ts)?;
    record_metadata(store, &reserve.order.order_id, signal_id, token, signal_ts)?;
    store.mark_execution_canary_built(&reserve.order.order_id, signal_ts)?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        signal_ts,
        EXECUTION_SIMULATION_STATUS_FAILED,
        Some("Transaction simulation failed: custom program error: 0x1788"),
    )?;
    store.mark_execution_canary_failed(
        &reserve.order.order_id,
        signal_ts,
        EXECUTION_ERROR_SIMULATION_FAILED,
        "Transaction simulation failed: custom program error: 0x1788",
    )?;
    Ok(())
}

fn record_terminal_no_route_order(
    store: &SqliteStore,
    signal_id: &str,
    token: &str,
    signal_ts: DateTime<Utc>,
) -> Result<()> {
    insert_signal(store, signal_id, token, signal_ts)?;
    let reserve = store.reserve_execution_canary_order(signal_id, ROUTE, signal_ts)?;
    store.mark_execution_canary_failed(
        &reserve.order.order_id,
        signal_ts,
        EXECUTION_ERROR_BUILD_FAILED,
        "owned_sell_quote_failed: NO_ROUTES_FOUND; pump_fun: Bonding curve for mint not found",
    )?;
    store.mark_execution_canary_terminal_sell_no_route_blocked(
        &reserve.order.order_id,
        "terminal_failed_sell_no_route_written_off",
    )?;
    Ok(())
}

fn record_terminal_simulation_order(
    store: &SqliteStore,
    signal_id: &str,
    token: &str,
    signal_ts: DateTime<Utc>,
) -> Result<()> {
    record_simulation_failed_order(store, signal_id, token, signal_ts)?;
    let order = store
        .load_execution_canary_order_by_signal(signal_id)?
        .expect("terminal simulation order");
    store.mark_execution_canary_terminal_sell_simulation_blocked(
        &order.order_id,
        "terminal_failed_sell_simulation_written_off",
    )?;
    Ok(())
}

fn insert_signal(
    store: &SqliteStore,
    signal_id: &str,
    token: &str,
    signal_ts: DateTime<Utc>,
) -> Result<()> {
    store.insert_copy_signal(&CopySignalRow {
        signal_id: signal_id.to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "sell".to_string(),
        token: token.to_string(),
        notional_sol: 0.01,
        notional_lamports: Some(Lamports::new(10_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: signal_ts,
        status: "shadow_recorded".to_string(),
    })?;
    Ok(())
}

fn record_metadata(
    store: &SqliteStore,
    order_id: &str,
    signal_id: &str,
    token: &str,
    signal_ts: DateTime<Utc>,
) -> Result<()> {
    store.record_execution_quote_canary_event(&quote_event(signal_id, token, signal_ts))?;
    store.record_execution_canary_build_plan_metadata(&ExecutionCanaryBuildPlanMetadata {
        order_id: order_id.to_string(),
        signal_id: signal_id.to_string(),
        client_order_id: "client-order".to_string(),
        recorded_ts: signal_ts,
        quote_source: Some("execution_quote_canary_provider:generic_metis".to_string()),
        quote_event_id: Some(format!("quote:{signal_id}")),
        quote_status: Some("ok".to_string()),
        quote_in_amount_raw: Some("500000".to_string()),
        quote_out_amount_raw: Some("10000000".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.0001),
        price_impact_pct: Some(0.01),
        route_plan_json: Some(format!(
            "[{{\"swapInfo\":{{\"label\":\"Metis\",\"inputMint\":\"{token}\"}}}}]"
        )),
        priority_fee_source: Some("execution_quote_canary_event".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(12_345),
        priority_fee_json: Some("{\"recommended\":12345}".to_string()),
        slippage_bps: Some(12.5),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("owned_position".to_string()),
    })?;
    Ok(())
}

fn quote_event(
    signal_id: &str,
    token: &str,
    signal_ts: DateTime<Utc>,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: format!("quote:{signal_id}"),
        signal_id: Some(signal_id.to_string()),
        shadow_closed_trade_id: None,
        wallet_id: "leader-wallet".to_string(),
        token: token.to_string(),
        side: "sell".to_string(),
        quote_status: "ok".to_string(),
        request_ts: signal_ts,
        signal_ts: Some(signal_ts),
        decision_delay_ms: Some(10),
        quote_latency_ms: Some(20),
        leader_notional_sol: Some(0.01),
        quote_in_amount_raw: Some("500000".to_string()),
        quote_out_amount_raw: Some("10000000".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.0001),
        shadow_price_sol: Some(0.0001),
        slippage_bps: Some(12.5),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(12_345),
        priority_fee_json: Some("{\"recommended\":12345}".to_string()),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("owned_position".to_string()),
        error: None,
    }
}

fn assert_token_class(
    diagnostics: &SellSideDiagnosticsReport,
    token: &str,
    err_code: &str,
    error_class: &str,
    next_action: &str,
) {
    let item = diagnostics
        .failures_by_token
        .iter()
        .find(|item| item.token == token)
        .unwrap_or_else(|| panic!("missing token {token}: {diagnostics:?}"));
    assert!(item.err_codes.iter().any(|value| value == err_code));
    assert!(item
        .simulation_error_classes
        .iter()
        .any(|value| value == error_class));
    assert_eq!(item.next_action, next_action);
}
