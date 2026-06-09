use super::*;
use copybot_core_types::{
    CopySignalRow, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};

#[tokio::test]
async fn sell_quote_event_skips_when_same_token_sell_is_in_flight() -> Result<()> {
    let db_path = unique_sell_token_in_flight_path("same-token");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let token = "TokenMint";
    let active_signal = sell_signal("active", token, now);
    let blocked_signal = sell_signal("blocked", token, now + chrono::Duration::seconds(1));
    let quote_event_id = "quote:close:sell-token-in-flight";
    store.record_execution_canary_open_position(
        "existing-confirmed-buy",
        token,
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        now,
    )?;
    store.insert_copy_signal(&active_signal)?;
    store.insert_copy_signal(&blocked_signal)?;
    let active = store.reserve_execution_canary_sell_order_unless_token_in_flight(
        &active_signal.signal_id,
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN,
        now + chrono::Duration::seconds(2),
    )?;
    store.record_execution_quote_canary_event(&sell_quote(
        quote_event_id,
        &blocked_signal,
        now + chrono::Duration::seconds(3),
    ))?;
    let config = sell_token_in_flight_config();

    let summary = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
        &config,
        &store,
        quote_event_id,
        now + chrono::Duration::seconds(4),
    )
    .await?
    .expect("sell quote event should be processed");

    assert_eq!(summary.sell_candidates, 1);
    assert_eq!(summary.sell_execute, 1);
    assert_eq!(summary.existing, 1);
    assert_eq!(summary.reserved, 0);
    assert_eq!(summary.failed, 0);
    assert_eq!(summary.skipped_reason, Some("sell_token_in_flight"));
    assert_eq!(
        summary.last_order_id.as_deref(),
        Some(active.order.order_id.as_str())
    );
    assert!(store
        .load_execution_canary_order_by_signal(&blocked_signal.signal_id)?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn sell_quote_event_skips_stale_sell_before_latest_live_buy() -> Result<()> {
    let db_path = unique_sell_token_in_flight_path("stale-before-latest-buy");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let token = "TokenMint";
    let stale_sell = sell_signal(
        "stale-before-buy",
        token,
        now - chrono::Duration::minutes(20),
    );
    let buy_signal = buy_signal("fresh-buy", token, now);
    let quote_event_id = "quote:close:stale-before-latest-buy";
    store.insert_copy_signal(&stale_sell)?;
    store.insert_copy_signal(&buy_signal)?;
    let buy_order_id = mark_confirmed_buy_order(&store, &buy_signal.signal_id, now)?;
    store.record_execution_canary_open_position(
        &buy_order_id,
        token,
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        now + chrono::Duration::seconds(4),
    )?;
    store.record_execution_quote_canary_event(&sell_quote(
        quote_event_id,
        &stale_sell,
        now + chrono::Duration::seconds(5),
    ))?;
    let config = sell_token_in_flight_config();

    let summary = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
        &config,
        &store,
        quote_event_id,
        now + chrono::Duration::seconds(6),
    )
    .await?
    .expect("sell quote event should be processed");

    assert_eq!(summary.sell_candidates, 1);
    assert_eq!(summary.sell_execute, 0);
    assert_eq!(summary.reserved, 0);
    assert_eq!(summary.failed, 0);
    assert_eq!(summary.skipped_reason, Some("sell_before_latest_buy"));
    assert!(store
        .load_execution_canary_order_by_signal(&stale_sell.signal_id)?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn sell_token_in_flight_config() -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_tiny_submit_enabled = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_wallet_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_keypair_path = "/tmp/non-empty-keypair.json".to_string();
    config.submit_adapter_http_url = "http://127.0.0.1:9".to_string();
    config.quote_canary_base_url = "http://127.0.0.1:9".to_string();
    config.max_submit_attempts = 3;
    config.swap_transaction_dry_run_enabled = true;
    config
}

fn sell_signal(label: &str, token: &str, ts: chrono::DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: format!("shadow:sig-sell-token-in-flight-{label}:leader:{token}"),
        wallet_id: "leader".to_string(),
        side: "sell".to_string(),
        token: token.to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn buy_signal(label: &str, token: &str, ts: chrono::DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        side: "buy".to_string(),
        ..sell_signal(label, token, ts)
    }
}

fn mark_confirmed_buy_order(
    store: &SqliteStore,
    signal_id: &str,
    now: chrono::DateTime<Utc>,
) -> Result<String> {
    let reserve = store.reserve_execution_canary_order(
        signal_id,
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN,
        now + chrono::Duration::seconds(1),
    )?;
    store
        .mark_execution_canary_built(&reserve.order.order_id, now + chrono::Duration::seconds(2))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + chrono::Duration::seconds(3),
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    store.mark_execution_canary_submitted(
        &reserve.order.order_id,
        now + chrono::Duration::seconds(4),
        "confirmed-buy-tx",
    )?;
    store.mark_execution_canary_confirmed(
        &reserve.order.order_id,
        now + chrono::Duration::seconds(5),
    )?;
    Ok(reserve.order.order_id)
}

fn sell_quote(
    event_id: &str,
    signal: &CopySignalRow,
    request_ts: chrono::DateTime<Utc>,
) -> copybot_storage_core::ExecutionQuoteCanaryEventInsert {
    copybot_storage_core::ExecutionQuoteCanaryEventInsert {
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

fn unique_sell_token_in_flight_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-sell-token-in-flight-{name}-{}-{nanos}",
        std::process::id()
    ))
}
