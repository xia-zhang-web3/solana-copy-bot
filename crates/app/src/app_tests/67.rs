use super::*;
use copybot_core_types::{
    ExactSwapAmounts, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn quote_canary_sweeps_owned_sell_signal_without_shadow_close() -> Result<()> {
    let db_path = unique_owned_sell_quote_test_path("owned-sell-sweep");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    store.record_execution_canary_open_position(
        "confirmed-buy-order",
        "TokenMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        now - chrono::Duration::minutes(10),
    )?;
    let signal = owned_sell_quote_signal(now - chrono::Duration::minutes(9));
    store.insert_copy_signal(&signal)?;
    store.insert_observed_swap(&owned_sell_quote_observed_swap(&signal))?;
    let (base_url, server) = serve_owned_sell_quote().await?;
    let mut config = owned_sell_quote_config(&base_url);
    config.canary_tiny_submit_enabled = false;
    let runner = ExecutionCanaryRunner::new(config);

    let summary = runner.process_tick(&store, now).await?;
    server.await?;
    let event = store
        .load_execution_quote_canary_event_by_id(&format!(
            "quote:owned-close:{}",
            signal.signal_id
        ))?
        .expect("owned sell quote event should be recorded");

    assert_eq!(summary.quote_close_inserted, 1);
    assert_eq!(summary.quote_would_execute, 1);
    assert_eq!(event.side, "sell");
    assert_eq!(event.shadow_closed_trade_id, None);
    assert_eq!(event.quote_in_amount_raw.as_deref(), Some("10000"));
    assert_eq!(event.quote_out_amount_raw.as_deref(), Some("1200000000"));
    assert_eq!(event.decision_status.as_deref(), Some("would_execute"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn quote_canary_sweeps_owned_stale_close_for_open_position() -> Result<()> {
    let db_path = unique_owned_sell_quote_test_path("owned-stale-close");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let opened = now - chrono::Duration::minutes(20);
    let closed = now - chrono::Duration::minutes(10);
    let stale_signal_id = "stale-close-42-1770000000000";
    store.record_execution_canary_open_position(
        "confirmed-stale-buy-order",
        "TokenMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        opened,
    )?;
    store.insert_shadow_closed_trade_exact_with_context(
        stale_signal_id,
        "leader-wallet",
        "TokenMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.2,
        0.0001,
        -0.1999,
        copybot_storage_core::SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
        opened,
        closed,
    )?;
    let (base_url, server) = serve_owned_sell_quote().await?;
    let mut config = owned_sell_quote_config(&base_url);
    config.canary_tiny_submit_enabled = false;
    config.canary_max_signal_age_seconds = 60;
    let runner = ExecutionCanaryRunner::new(config);

    let summary = runner.process_tick(&store, now).await?;
    server.await?;
    let signal = store
        .load_copy_signal_by_signal_id(stale_signal_id)?
        .expect("stale close should create a synthetic sell signal");
    let event = store
        .load_execution_quote_canary_event_by_id("quote:owned-stale-close:1")?
        .expect("owned stale close quote event should be recorded");

    assert_eq!(signal.side, "sell");
    assert_eq!(signal.status, "shadow_recorded");
    assert_eq!(summary.quote_close_inserted, 1);
    assert_eq!(summary.quote_would_execute, 1);
    assert_eq!(event.signal_id.as_deref(), Some(stale_signal_id));
    assert_eq!(event.shadow_closed_trade_id, Some(1));
    assert_eq!(event.quote_in_amount_raw.as_deref(), Some("10000"));
    assert_eq!(event.quote_out_amount_raw.as_deref(), Some("1200000000"));
    assert_eq!(event.decision_status.as_deref(), Some("would_execute"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn quote_canary_retries_owned_sell_priority_fee() -> Result<()> {
    let db_path = unique_owned_sell_quote_test_path("owned-sell-priority-retry");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let buy_signal = owned_buy_quote_signal(now - chrono::Duration::minutes(10));
    store.insert_copy_signal(&buy_signal)?;
    let buy_order = store.reserve_execution_canary_order(
        &buy_signal.signal_id,
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN,
        now - chrono::Duration::minutes(10),
    )?;
    store.record_execution_canary_open_position(
        &buy_order.order.order_id,
        "TokenMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        now - chrono::Duration::minutes(10),
    )?;
    let sell_signal = owned_sell_quote_signal(now - chrono::Duration::minutes(9));
    store.insert_copy_signal(&sell_signal)?;
    record_owned_sell_quote_with_priority(
        &store,
        &sell_signal,
        now - chrono::Duration::minutes(8),
        Some(crate::execution_quote_canary_helpers::QUOTE_STATUS_SKIPPED),
        None,
        Some("{\"reason\":\"priority_fee_transient_unavailable\"}"),
        Some("priority_fee: operation timed out"),
    )?;
    let (priority_url, server) = serve_owned_sell_priority_fee(44_000).await?;
    let mut config = owned_sell_quote_config("http://127.0.0.1:9");
    config.priority_fee_canary_enabled = true;
    config.priority_fee_canary_rpc_url = priority_url;
    config.priority_fee_canary_timeout_ms = 1_000;
    config.priority_fee_canary_min_request_interval_ms = 1;
    let runner = ExecutionCanaryRunner::new(config);

    let summary = runner.process_tick(&store, now).await?;
    server.await?;
    let event = store
        .load_execution_quote_canary_event_by_id(&format!(
            "quote:owned-close:{}",
            sell_signal.signal_id
        ))?
        .expect("owned sell quote event should exist");

    assert_eq!(summary.quote_close_existing, 1);
    assert_eq!(summary.quote_would_execute, 1);
    assert_eq!(event.priority_fee_status.as_deref(), Some("ok"));
    assert_eq!(event.priority_fee_lamports, Some(44_000));
    assert!(event.error.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

async fn serve_owned_sell_quote() -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("owned sell quote request");
        let mut buffer = [0_u8; 4096];
        let read = socket.read(&mut buffer).await.expect("read request");
        let body = String::from_utf8_lossy(&buffer[..read]).to_string();
        assert!(body.starts_with("GET /quote?"));
        assert!(body.contains("amount=10000"));
        let response_body = r#"{"inputMint":"TokenMint","inAmount":"10000","outputMint":"So11111111111111111111111111111111111111112","outAmount":"1200000000","priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        let _ = socket.write_all(response.as_bytes()).await;
    });
    Ok((base_url, server))
}

async fn serve_owned_sell_priority_fee(
    lamports: u64,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("priority fee request");
        let mut buffer = [0_u8; 4096];
        let read = socket.read(&mut buffer).await.expect("read request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.contains("qn_estimatePriorityFees"));
        let body = format!(r#"{{"jsonrpc":"2.0","id":1,"result":{{"recommended":{lamports}}}}}"#);
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = socket.write_all(response.as_bytes()).await;
    });
    Ok((base_url, server))
}

fn owned_sell_quote_config(quote_base_url: &str) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.quote_canary_enabled = true;
    config.quote_canary_base_url = quote_base_url.to_string();
    config.quote_canary_sell_slippage_bps = 500;
    config.canary_batch_limit = 10;
    config.canary_max_signal_age_seconds = 60;
    config
}

fn owned_buy_quote_signal(ts: chrono::DateTime<Utc>) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: "shadow:sig-owned-buy:leader-wallet:buy:TokenMint".to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn owned_sell_quote_signal(ts: chrono::DateTime<Utc>) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: "shadow:sig-owned-sell:leader-wallet:sell:TokenMint".to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "sell".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 1.2,
        notional_lamports: Some(Lamports::new(1_200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn record_owned_sell_quote_with_priority(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
    priority_fee_status: Option<&str>,
    priority_fee_lamports: Option<u64>,
    priority_fee_json: Option<&str>,
    error: Option<&str>,
) -> Result<()> {
    store.record_execution_quote_canary_event(
        &copybot_storage_core::ExecutionQuoteCanaryEventInsert {
            event_id: format!("quote:owned-close:{}", signal.signal_id),
            signal_id: Some(signal.signal_id.clone()),
            shadow_closed_trade_id: None,
            wallet_id: signal.wallet_id.clone(),
            token: signal.token.clone(),
            side: "sell".to_string(),
            quote_status: "ok".to_string(),
            request_ts: now,
            signal_ts: Some(signal.ts),
            decision_delay_ms: Some(7),
            quote_latency_ms: Some(11),
            leader_notional_sol: Some(signal.notional_sol),
            quote_in_amount_raw: Some("10000".to_string()),
            quote_out_amount_raw: Some("1200000000".to_string()),
            quote_response_json: None,
            quote_price_sol: Some(0.12),
            shadow_price_sol: Some(0.12),
            slippage_bps: Some(10.0),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
            priority_fee_status: priority_fee_status.map(str::to_string),
            priority_fee_lamports,
            priority_fee_json: priority_fee_json.map(str::to_string),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: error.map(str::to_string),
        },
    )?;
    Ok(())
}

fn owned_sell_quote_observed_swap(signal: &copybot_core_types::CopySignalRow) -> SwapEvent {
    SwapEvent {
        wallet: signal.wallet_id.clone(),
        dex: "pumpswap".to_string(),
        token_in: signal.token.clone(),
        token_out: "So11111111111111111111111111111111111111112".to_string(),
        amount_in: 10.0,
        amount_out: 1.2,
        signature: "sig-owned-sell".to_string(),
        slot: 42,
        ts_utc: signal.ts,
        exact_amounts: Some(ExactSwapAmounts {
            amount_in_raw: "10000".to_string(),
            amount_in_decimals: 3,
            amount_out_raw: "1200000000".to_string(),
            amount_out_decimals: 9,
        }),
    }
}

fn unique_owned_sell_quote_test_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "copybot-{name}-{}-{}.db",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ))
}
