use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{
    CopySignalRow, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, SqliteStore, EXECUTION_ERROR_BUILD_FAILED,
    EXECUTION_SIMULATION_STATUS_PASSED,
};
use tempfile::tempdir;

const ROUTE: &str = "metis-swap-instructions-dry-run";

#[test]
fn terminal_no_route_retry_does_not_attach_old_sell_to_new_position() -> Result<()> {
    let store = open_migrated_store("terminal-no-route-new-position")?;
    let now = ts("2026-06-09T12:00:00Z");
    let token = "TokenMint";

    let old_buy_order_id = confirmed_buy_order(&store, "buy-old", token, now)?;
    store.record_execution_canary_open_position(
        &old_buy_order_id,
        token,
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        now + Duration::seconds(4),
    )?;
    let old_sell = signal("sell-old", "sell", token, now + Duration::seconds(10));
    store.insert_copy_signal(&old_sell)?;
    store.record_execution_quote_canary_event(&sell_quote_event(
        "quote:old-sell",
        &old_sell,
        now + Duration::seconds(11),
    ))?;
    let old_sell_order = store.reserve_execution_canary_order(
        &old_sell.signal_id,
        ROUTE,
        now + Duration::seconds(12),
    )?;
    store.mark_execution_canary_failed(
        &old_sell_order.order.order_id,
        now + Duration::seconds(13),
        EXECUTION_ERROR_BUILD_FAILED,
        "owned_sell_quote_failed: NO_ROUTES_FOUND",
    )?;
    store.mark_execution_canary_terminal_sell_no_route_blocked(
        &old_sell_order.order.order_id,
        "terminal_failed_sell_no_route_written_off",
    )?;

    let retry_after = now + Duration::seconds(700);
    assert_eq!(
        store.list_terminal_no_route_sell_execution_quote_event_ids_for_route(
            ROUTE,
            retry_after,
            10
        )?,
        vec!["quote:old-sell".to_string()]
    );

    store.close_execution_canary_open_position(
        token,
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.0,
        1e-12,
        now + Duration::seconds(20),
    )?;
    let new_buy_order_id =
        confirmed_buy_order(&store, "buy-new", token, now + Duration::seconds(30))?;
    store.record_execution_canary_open_position(
        &new_buy_order_id,
        token,
        20.0,
        Some(TokenQuantity::new(20_000, 3)),
        0.02,
        now + Duration::seconds(34),
    )?;

    assert!(store
        .list_terminal_no_route_sell_execution_quote_event_ids_for_route(ROUTE, retry_after, 10)?
        .is_empty());
    Ok(())
}

#[test]
fn terminal_no_route_retry_ignores_sell_quote_before_position_opened() -> Result<()> {
    let store = open_migrated_store("terminal-no-route-before-open")?;
    let now = ts("2026-06-09T13:00:00Z");
    let token = "TokenMint";

    let buy_order_id = confirmed_buy_order(&store, "buy-before-open", token, now)?;
    let early_sell = signal(
        "sell-before-open",
        "sell",
        token,
        now + Duration::seconds(10),
    );
    store.insert_copy_signal(&early_sell)?;
    store.record_execution_quote_canary_event(&sell_quote_event(
        "quote:sell-before-open",
        &early_sell,
        now + Duration::seconds(11),
    ))?;
    let sell_order = store.reserve_execution_canary_order(
        &early_sell.signal_id,
        ROUTE,
        now + Duration::seconds(12),
    )?;
    store.mark_execution_canary_failed(
        &sell_order.order.order_id,
        now + Duration::seconds(13),
        EXECUTION_ERROR_BUILD_FAILED,
        "owned_sell_quote_failed: NO_ROUTES_FOUND",
    )?;
    store.mark_execution_canary_terminal_sell_no_route_blocked(
        &sell_order.order.order_id,
        "terminal_failed_sell_no_route_written_off",
    )?;
    store.record_execution_canary_open_position(
        &buy_order_id,
        token,
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        now + Duration::seconds(20),
    )?;

    assert!(store
        .list_terminal_no_route_sell_execution_quote_event_ids_for_route(
            ROUTE,
            now + Duration::seconds(700),
            10
        )?
        .is_empty());
    Ok(())
}

fn confirmed_buy_order(
    store: &SqliteStore,
    signal_id: &str,
    token: &str,
    now: DateTime<Utc>,
) -> Result<String> {
    let buy_signal = signal(signal_id, "buy", token, now);
    store.insert_copy_signal(&buy_signal)?;
    let reserve =
        store.reserve_execution_canary_order(signal_id, ROUTE, now + Duration::seconds(1))?;
    store.mark_execution_canary_built(&reserve.order.order_id, now + Duration::seconds(2))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + Duration::seconds(3),
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    store.mark_execution_canary_submitted(
        &reserve.order.order_id,
        now + Duration::seconds(4),
        &format!("tx-{signal_id}"),
    )?;
    store.mark_execution_canary_confirmed(&reserve.order.order_id, now + Duration::seconds(5))?;
    Ok(reserve.order.order_id)
}

fn signal(signal_id: &str, side: &str, token: &str, ts: DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: signal_id.to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: side.to_string(),
        token: token.to_string(),
        notional_sol: 0.01,
        notional_lamports: Some(Lamports::new(10_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn sell_quote_event(
    event_id: &str,
    signal: &CopySignalRow,
    request_ts: DateTime<Utc>,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id: Some(signal.signal_id.clone()),
        shadow_closed_trade_id: Some(42),
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: "sell".to_string(),
        quote_status: "ok".to_string(),
        request_ts,
        signal_ts: Some(signal.ts),
        decision_delay_ms: Some(1000),
        quote_latency_ms: Some(50),
        leader_notional_sol: Some(signal.notional_sol),
        quote_in_amount_raw: Some("10000".to_string()),
        quote_out_amount_raw: Some("11000000".to_string()),
        quote_response_json: Some("{\"loadedLongtailToken\":true}".to_string()),
        quote_price_sol: Some(0.0011),
        shadow_price_sol: Some(0.001),
        slippage_bps: Some(50.0),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(12_345),
        priority_fee_json: Some("{\"recommended\":12345}".to_string()),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("within_slippage_limit".to_string()),
        error: None,
    }
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

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}
