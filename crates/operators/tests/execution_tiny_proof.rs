use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_operators::execution_canary_quote_pnl::build_report_from_db_path;
use copybot_storage_core::{
    ExecutionCanaryBuildPlanMetadata, ExecutionQuoteCanaryEventInsert, SqliteStore,
    EXECUTION_ERROR_BUILD_FAILED, EXECUTION_ERROR_SIMULATION_FAILED,
    EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE, EXECUTION_SIMULATION_STATUS_FAILED,
    EXECUTION_SIMULATION_STATUS_PASSED,
};
use tempfile::tempdir;

const ROUTE: &str = "metis-swap-instructions-dry-run";

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

#[test]
fn execution_quote_pnl_report_includes_tiny_execution_proof() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;

    let opened_win = ts("2026-06-02T12:00:00Z");
    let closed_win = opened_win + Duration::seconds(30);
    store.insert_shadow_closed_trade_exact(
        "sell-win",
        "leader-wallet",
        "TokenWin",
        100.0,
        None,
        0.10,
        0.13,
        0.03,
        opened_win,
        closed_win,
    )?;
    let close_id_win = store
        .list_execution_quote_canary_close_candidates_for_signal("sell-win", 10)?
        .into_iter()
        .next()
        .expect("win close candidate")
        .id;
    insert_signal(&store, "buy-win", "buy", "TokenWin", opened_win)?;
    insert_signal(&store, "sell-win", "sell", "TokenWin", closed_win)?;
    store.record_execution_quote_canary_event(&quote_event(
        "quote:buy:win",
        Some("buy-win"),
        None,
        "TokenWin",
        "buy",
        opened_win,
        None,
        "would_execute",
        "inside_limits",
        40,
    ))?;
    store.record_execution_quote_canary_shadow_gate_event(
        "buy-win",
        "leader-wallet",
        "TokenWin",
        "buy",
        "shadow_recorded",
        None,
        opened_win + Duration::milliseconds(20),
    )?;
    store.record_execution_quote_canary_event(&quote_event(
        "quote:sell:win",
        Some("sell-win"),
        Some(close_id_win),
        "TokenWin",
        "sell",
        closed_win,
        Some(23_456),
        "would_execute",
        "owned_position",
        25,
    ))?;
    let buy_order_id = record_confirmed_order(
        &store,
        "buy-win",
        "quote:buy:win",
        12_345,
        "would_execute",
        "inside_limits",
        opened_win,
        "buy-signature",
    )?;
    store.record_execution_canary_open_position(
        "exec-canary-pos:recovery-orphan:TokenWin",
        "TokenWin",
        1.0,
        None,
        0.001,
        opened_win - Duration::seconds(5),
    )?;
    store.record_execution_canary_confirmed_buy_fill(
        &buy_order_id,
        "TokenWin",
        100.0,
        None,
        0.01,
        opened_win + Duration::milliseconds(1600),
    )?;
    let sell_order_id = record_confirmed_order(
        &store,
        "sell-win",
        "quote:sell:win",
        23_456,
        "would_execute",
        "owned_position",
        closed_win,
        "sell-signature",
    )?;
    store.close_execution_canary_confirmed_sell_fill(
        &sell_order_id,
        "TokenWin",
        101.0,
        None,
        0.00012,
        0.000001,
        closed_win + Duration::milliseconds(1600),
    )?;
    let opened_skip = ts("2026-06-02T12:01:00Z");
    let closed_skip = opened_skip + Duration::seconds(20);
    store.insert_shadow_closed_trade_exact(
        "sell-skip",
        "leader-wallet",
        "TokenSkip",
        100.0,
        None,
        0.20,
        0.22,
        0.02,
        opened_skip,
        closed_skip,
    )?;
    insert_signal(&store, "buy-skip", "buy", "TokenSkip", opened_skip)?;
    store.record_execution_quote_canary_event(&quote_event(
        "quote:buy:skip",
        Some("buy-skip"),
        None,
        "TokenSkip",
        "buy",
        opened_skip,
        Some(34_567),
        "would_skip",
        "entry_slippage_too_high",
        30,
    ))?;
    store.record_execution_quote_canary_shadow_gate_event(
        "buy-skip",
        "leader-wallet",
        "TokenSkip",
        "buy",
        "shadow_dropped",
        Some("entry_slippage_too_high"),
        opened_skip + Duration::milliseconds(20),
    )?;
    record_failed_simulation_order(
        &store,
        "sell-failed",
        "quote:sell:failed",
        opened_skip + Duration::seconds(5),
    )?;
    record_terminal_no_route_order(&store, "sell-no-route", opened_skip + Duration::seconds(6))?;
    record_pump_fun_bonding_curve_order(
        &store,
        "sell-pump-fun-bonding",
        opened_skip + Duration::seconds(7),
    )?;
    drop(store);

    let report = build_report_from_db_path(&db_path, ts("2026-06-02T13:00:00Z"));
    let proof = report
        .tiny_execution_proof
        .as_ref()
        .unwrap_or_else(|| panic!("tiny proof should exist: {:?}", report.error));

    assert_eq!(report.reason_class, "execution_canary_quote_pnl_loaded");
    assert_eq!(proof.summary.shadow_market_closed_trades, 2);
    assert_eq!(proof.summary.canary_entry_would_execute_trades, 1);
    assert_eq!(proof.summary.canary_exit_would_execute_trades, 1);
    assert_eq!(proof.summary.tiny_entry_ordered_trades, 1);
    assert_eq!(proof.summary.tiny_entry_confirmed_trades, 1);
    assert_eq!(proof.summary.tiny_exit_ordered_trades, 1);
    assert_eq!(proof.summary.tiny_exit_confirmed_trades, 1);
    assert_eq!(proof.summary.tiny_closed_positions, 1);
    assert_eq!(proof.summary.tiny_unique_closed_positions, 1);
    assert_eq!(proof.summary.tiny_open_positions, 0);
    assert_close(proof.summary.shadow_pnl_sol, 0.05);
    assert_close(proof.summary.tiny_realized_pnl_sol, 0.00112);
    assert_close(proof.summary.tiny_vs_shadow_delta_sol, -0.04888);
    assert_eq!(proof.entry_funnel.total_buy_quote_events, 2);
    assert_eq!(proof.entry_funnel.quote_would_execute_events, 1);
    assert_eq!(proof.entry_funnel.quote_would_skip_events, 1);
    assert_eq!(proof.entry_funnel.actionable_quote_would_execute_events, 1);
    assert_eq!(proof.entry_funnel.actionable_tiny_ordered_events, 1);
    assert_eq!(proof.entry_funnel.actionable_tiny_confirmed_events, 1);
    assert_eq!(proof.entry_funnel.actionable_tiny_missing_order_events, 0);
    assert_eq!(proof.entry_funnel.shadow_recorded_events, 1);
    assert_eq!(proof.entry_funnel.shadow_dropped_events, 1);
    assert_eq!(proof.entry_funnel.shadow_pending_events, 0);
    assert_eq!(proof.entry_funnel.tiny_ordered_events, 1);
    assert_eq!(proof.entry_funnel.tiny_confirmed_events, 1);
    assert_eq!(proof.entry_funnel.tiny_missing_order_events, 1);
    assert_eq!(
        proof.entry_funnel.tiny_missing_order_shadow_dropped_events,
        1
    );
    assert_eq!(
        proof.entry_funnel.tiny_missing_order_shadow_recorded_events,
        0
    );
    assert_eq!(
        proof.entry_funnel.tiny_missing_order_shadow_pending_events,
        0
    );
    assert_reason(proof, "position", "tiny_closed", 1);
    assert_reason(proof, "entry_decision", "entry_decision:would_skip", 1);
    assert_eq!(proof.latency.entry_quote_latency_ms.samples, 2);
    assert_eq!(proof.latency.entry_quote_latency_ms.max_ms, 40);
    assert_close(proof.latency.entry_quote_latency_ms.avg_ms, 35.0);
    assert_eq!(proof.latency.exit_quote_latency_ms.samples, 1);
    assert_eq!(proof.latency.entry_signal_to_submit_ms.avg_ms, 500.0);
    assert_eq!(proof.latency.entry_quote_to_submit_ms.avg_ms, 490.0);
    assert_eq!(proof.latency.entry_submit_to_confirm_ms.avg_ms, 1000.0);
    assert_eq!(proof.latency.exit_signal_to_submit_ms.avg_ms, 500.0);
    assert_eq!(proof.latency.exit_quote_to_submit_ms.avg_ms, 490.0);
    assert_eq!(proof.latency.exit_submit_to_confirm_ms.avg_ms, 1000.0);
    let win = proof
        .trades
        .iter()
        .find(|trade| trade.token == "TokenWin")
        .expect("win trade");
    assert_eq!(win.proof_status, "tiny_closed");
    assert!(win
        .tiny_buy_order
        .as_ref()
        .is_some_and(|order| order.tx_signature_present));
    assert!(win
        .tiny_sell_order
        .as_ref()
        .is_some_and(|order| order.tx_signature_present));

    let recent_buy = proof
        .recent_orders
        .iter()
        .find(|order| order.signal_id == "buy-win")
        .expect("recent buy order");
    assert_eq!(recent_buy.quote_event_id.as_deref(), Some("quote:buy:win"));
    assert_eq!(
        recent_buy.quote_source.as_deref(),
        Some("execution_quote_canary_event")
    );
    assert_eq!(recent_buy.priority_fee_lamports, Some(12_345));
    assert_eq!(recent_buy.decision_status.as_deref(), Some("would_execute"));
    assert!(proof.order_failure_counts.iter().any(|count| {
        count.side == "sell"
            && count.err_code == EXECUTION_ERROR_SIMULATION_FAILED
            && count.simulation_status == EXECUTION_SIMULATION_STATUS_FAILED
            && count.simulation_error_class == "custom_program_error:0x1788"
            && count.orders == 1
    }));
    assert!(proof.order_failure_counts.iter().any(|count| {
        count.side == "sell"
            && count.err_code == EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE
            && count.simulation_error_class == "terminal_no_route"
            && count.orders == 1
    }));
    assert!(proof.order_failure_counts.iter().any(|count| {
        count.side == "sell"
            && count.err_code == EXECUTION_ERROR_BUILD_FAILED
            && count.simulation_error_class == "pump_fun_bonding_curve_not_found"
            && count.orders == 1
    }));
    Ok(())
}

fn insert_signal(
    store: &SqliteStore,
    signal_id: &str,
    side: &str,
    token: &str,
    signal_ts: DateTime<Utc>,
) -> Result<()> {
    store.insert_copy_signal(&CopySignalRow {
        signal_id: signal_id.to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: side.to_string(),
        token: token.to_string(),
        notional_sol: 0.01,
        notional_lamports: Some(Lamports::new(10_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: signal_ts,
        status: "shadow_recorded".to_string(),
    })?;
    Ok(())
}

fn record_confirmed_order(
    store: &SqliteStore,
    signal_id: &str,
    quote_event_id: &str,
    priority_fee_lamports: u64,
    decision_status: &str,
    decision_reason: &str,
    signal_ts: DateTime<Utc>,
    tx_signature: &str,
) -> Result<String> {
    let reserve_ts = signal_ts + Duration::milliseconds(100);
    let reserve = store.reserve_execution_canary_order(signal_id, ROUTE, reserve_ts)?;
    store.record_execution_canary_build_plan_metadata(&ExecutionCanaryBuildPlanMetadata {
        order_id: reserve.order.order_id.clone(),
        signal_id: reserve.order.signal_id.clone(),
        client_order_id: reserve.order.client_order_id.clone(),
        recorded_ts: signal_ts + Duration::milliseconds(10),
        quote_source: Some("execution_quote_canary_event".to_string()),
        quote_event_id: Some(quote_event_id.to_string()),
        quote_status: Some("ok".to_string()),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("500000".to_string()),
        quote_response_json: None,
        quote_request_ts: None,
        quote_price_sol: Some(0.0001),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_source: Some("execution_quote_canary_event".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(priority_fee_lamports),
        priority_fee_json: Some("{\"recommended\":12345}".to_string()),
        slippage_bps: Some(12.5),
        decision_status: Some(decision_status.to_string()),
        decision_reason: Some(decision_reason.to_string()),
    })?;
    store.mark_execution_canary_built(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(120),
    )?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(140),
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    store.mark_execution_canary_submitted(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(500),
        tx_signature,
    )?;
    store.mark_execution_canary_confirmed(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(1500),
    )?;
    Ok(reserve.order.order_id)
}

fn record_failed_simulation_order(
    store: &SqliteStore,
    signal_id: &str,
    quote_event_id: &str,
    signal_ts: DateTime<Utc>,
) -> Result<String> {
    insert_signal(store, signal_id, "sell", "TokenFailed", signal_ts)?;
    let reserve_ts = signal_ts + Duration::milliseconds(100);
    let reserve = store.reserve_execution_canary_order(signal_id, ROUTE, reserve_ts)?;
    store.record_execution_canary_build_plan_metadata(&ExecutionCanaryBuildPlanMetadata {
        order_id: reserve.order.order_id.clone(),
        signal_id: reserve.order.signal_id.clone(),
        client_order_id: reserve.order.client_order_id.clone(),
        recorded_ts: signal_ts + Duration::milliseconds(10),
        quote_source: Some("execution_quote_canary_provider:generic_metis".to_string()),
        quote_event_id: Some(quote_event_id.to_string()),
        quote_status: Some("ok".to_string()),
        quote_in_amount_raw: Some("500000".to_string()),
        quote_out_amount_raw: Some("10000000".to_string()),
        quote_response_json: None,
        quote_request_ts: None,
        quote_price_sol: Some(0.0001),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_source: Some("execution_quote_canary_event".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(12_345),
        priority_fee_json: Some("{\"recommended\":12345}".to_string()),
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
        EXECUTION_SIMULATION_STATUS_FAILED,
        Some("Transaction simulation failed: custom program error: 0x1788"),
    )?;
    store.mark_execution_canary_failed(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(160),
        EXECUTION_ERROR_SIMULATION_FAILED,
        "Transaction simulation failed: custom program error: 0x1788",
    )?;
    Ok(reserve.order.order_id)
}

fn record_terminal_no_route_order(
    store: &SqliteStore,
    signal_id: &str,
    signal_ts: DateTime<Utc>,
) -> Result<String> {
    insert_signal(store, signal_id, "sell", "TokenNoRoute", signal_ts)?;
    let reserve = store.reserve_execution_canary_order(signal_id, ROUTE, signal_ts)?;
    store.mark_execution_canary_failed(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(10),
        EXECUTION_ERROR_BUILD_FAILED,
        "owned_sell_quote_failed: NO_ROUTES_FOUND; pump_fun: Bonding curve for mint not found",
    )?;
    store.mark_execution_canary_terminal_sell_no_route_blocked(
        &reserve.order.order_id,
        "terminal_failed_sell_no_route_written_off",
    )?;
    Ok(reserve.order.order_id)
}

fn record_pump_fun_bonding_curve_order(
    store: &SqliteStore,
    signal_id: &str,
    signal_ts: DateTime<Utc>,
) -> Result<String> {
    insert_signal(store, signal_id, "sell", "TokenPumpFunBonding", signal_ts)?;
    let reserve = store.reserve_execution_canary_order(signal_id, ROUTE, signal_ts)?;
    store.mark_execution_canary_failed(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(10),
        EXECUTION_ERROR_BUILD_FAILED,
        "pump_fun paid quote returned HTTP 400: Bonding curve for mint not found",
    )?;
    Ok(reserve.order.order_id)
}

#[allow(clippy::too_many_arguments)]
fn quote_event(
    event_id: &str,
    signal_id: Option<&str>,
    close_id: Option<i64>,
    token: &str,
    side: &str,
    signal_ts: DateTime<Utc>,
    priority_fee_lamports: Option<u64>,
    decision_status: &str,
    decision_reason: &str,
    quote_latency_ms: u64,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id: signal_id.map(ToString::to_string),
        shadow_closed_trade_id: close_id,
        wallet_id: "leader-wallet".to_string(),
        token: token.to_string(),
        side: side.to_string(),
        quote_status: "ok".to_string(),
        request_ts: signal_ts + Duration::milliseconds(10),
        signal_ts: Some(signal_ts),
        decision_delay_ms: Some(15),
        quote_latency_ms: Some(quote_latency_ms),
        leader_notional_sol: Some(0.2),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("500000".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.0001),
        shadow_price_sol: Some(0.0001),
        slippage_bps: Some(15.0),
        price_impact_pct: Some(0.02),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports,
        priority_fee_json: Some("{\"recommended\":12345}".to_string()),
        decision_status: Some(decision_status.to_string()),
        decision_reason: Some(decision_reason.to_string()),
        error: None,
    }
}

fn assert_reason(
    proof: &copybot_storage_core::ExecutionTinyProofReport,
    stage: &str,
    reason: &str,
    trades: u64,
) {
    assert!(
        proof
            .reason_counts
            .iter()
            .any(|count| count.stage == stage && count.reason == reason && count.trades == trades),
        "missing reason {stage}/{reason}: {:?}",
        proof.reason_counts
    );
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 0.000_000_001,
        "actual={actual} expected={expected}"
    );
}
