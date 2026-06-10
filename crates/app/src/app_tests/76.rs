use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn owned_sell_quote_event_selects_pump_fun_paid_after_generic_no_route() -> Result<()> {
    let db_path = unique_pump_fun_owned_sell_test_path("quote-event");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    store.record_execution_canary_open_position(
        "existing-buy",
        "TokenMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.8,
        now - chrono::Duration::minutes(2),
    )?;
    let signal = pump_fun_owned_sell_signal("quote-event", now);
    store.insert_copy_signal(&signal)?;
    let (base_url, server) = serve_owned_sell_generic_no_route_then_pump_quote().await?;
    let runner = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(
        pump_fun_quote_config(&base_url),
    );

    let summary = runner
        .process_tick(
            &store,
            "shadow_recorded",
            now + chrono::Duration::seconds(1),
            now - chrono::Duration::hours(1),
            5,
        )
        .await?;
    server.await?;
    let event_id = format!("quote:owned-close:{}", signal.signal_id);
    let event = store
        .load_execution_quote_canary_event_by_id(&event_id)?
        .expect("owned sell quote event should be recorded");
    let pump_sample = store
        .load_execution_quote_canary_provider_sample(
            &event_id,
            copybot_storage_core::PROVIDER_PUMP_FUN_PAID,
        )?
        .expect("owned sell should record pump.fun provider sample");
    let generic_sample = store
        .load_execution_quote_canary_provider_sample(
            &event_id,
            copybot_storage_core::PROVIDER_GENERIC_METIS,
        )?
        .expect("owned sell should keep generic provider evidence");

    assert_eq!(summary.close_inserted, 1);
    assert_eq!(event.quote_status, "ok");
    assert_eq!(event.quote_in_amount_raw.as_deref(), Some("10000"));
    assert_eq!(event.quote_out_amount_raw.as_deref(), Some("1200000000"));
    assert!(event
        .route_plan_json
        .as_deref()
        .unwrap_or_default()
        .contains("Pump.fun Paid"));
    assert_eq!(pump_sample.quote_status, "ok");
    assert_eq!(generic_sample.quote_status, "error");
    assert!(generic_sample
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("NO_ROUTES_FOUND"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn owned_sell_metadata_falls_back_to_pump_fun_paid_after_generic_no_route() -> Result<()> {
    let db_path = unique_pump_fun_owned_sell_test_path("metadata");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    store.record_execution_canary_open_position(
        "existing-buy",
        "TokenMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.8,
        now,
    )?;
    let (base_url, server) = serve_owned_sell_balance_generic_no_route_then_pump_quote().await?;
    let mut metadata = crate::execution_submit_adapter::ExecutionBuildPlanMetadata::default();
    metadata.quote_source =
        Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS.to_string());
    metadata.quote_event_id = Some("quote:owned-close:test".to_string());
    metadata.priority_fee_status = Some("ok".to_string());
    metadata.priority_fee_lamports = Some(22_000);
    metadata.decision_status = Some("would_execute".to_string());
    metadata.decision_reason = Some("owned_position".to_string());

    let refreshed = crate::execution_canary_route::owned_position_sell_metadata(
        &pump_fun_submit_config(&base_url),
        &store,
        "TokenMint",
        metadata,
    )
    .await?;
    server.await?;

    assert_eq!(
        refreshed.quote_source.as_deref(),
        Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_PUMP_FUN_PAID)
    );
    assert_eq!(
        refreshed.quote_event_id.as_deref(),
        Some("quote:owned-close:test")
    );
    assert_eq!(refreshed.priority_fee_lamports, Some(22_000));
    assert_eq!(refreshed.decision_status.as_deref(), Some("would_execute"));
    assert_eq!(refreshed.quote_in_amount_raw.as_deref(), Some("10000"));
    assert_eq!(
        refreshed.quote_out_amount_raw.as_deref(),
        Some("1200000000")
    );
    assert!(refreshed
        .route_plan_json
        .as_deref()
        .unwrap_or_default()
        .contains("Pump.fun Paid"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn owned_sell_metadata_falls_back_to_generic_after_selected_pump_fun_error() -> Result<()> {
    let db_path = unique_pump_fun_owned_sell_test_path("selected-pump-error");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    store.record_execution_canary_open_position(
        "existing-buy",
        "TokenMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.8,
        now,
    )?;
    let (base_url, server) = serve_owned_sell_balance_pump_error_then_generic_quote().await?;
    let mut metadata = crate::execution_submit_adapter::ExecutionBuildPlanMetadata::default();
    metadata.quote_source =
        Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_PUMP_FUN_PAID.to_string());
    metadata.quote_event_id = Some("quote:owned-close:test".to_string());
    metadata.priority_fee_status = Some("ok".to_string());
    metadata.priority_fee_lamports = Some(22_000);
    metadata.decision_status = Some("would_execute".to_string());
    metadata.decision_reason = Some("owned_position".to_string());

    let refreshed = crate::execution_canary_route::owned_position_sell_metadata(
        &pump_fun_submit_config(&base_url),
        &store,
        "TokenMint",
        metadata,
    )
    .await?;
    server.await?;

    assert_eq!(
        refreshed.quote_source.as_deref(),
        Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS)
    );
    assert_eq!(
        refreshed.quote_event_id.as_deref(),
        Some("quote:owned-close:test")
    );
    assert_eq!(refreshed.priority_fee_lamports, Some(22_000));
    assert_eq!(refreshed.quote_in_amount_raw.as_deref(), Some("10000"));
    assert_eq!(refreshed.quote_out_amount_raw.as_deref(), Some("900000000"));
    assert!(refreshed
        .route_plan_json
        .as_deref()
        .unwrap_or_default()
        .contains("Pump.fun Amm"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

async fn serve_owned_sell_generic_no_route_then_pump_quote(
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let generic = read_owned_sell_http_request(&listener).await;
        assert!(generic.body.starts_with("GET /quote?"));
        assert!(generic.body.contains("amount=10000"));
        write_owned_sell_http_response(
            generic.socket,
            "400 Bad Request",
            r#"{"error":"No routes found","errorCode":"NO_ROUTES_FOUND"}"#,
        )
        .await;

        let pump = read_owned_sell_http_request(&listener).await;
        assert!(pump.body.starts_with("GET /pump-fun/quote?"));
        assert!(pump.body.contains("type=SELL"));
        assert!(pump.body.contains("amount=10000"));
        write_owned_sell_http_response(pump.socket, "200 OK", pump_fun_sell_quote_json()).await;
    });
    Ok((base_url, server))
}

async fn serve_owned_sell_balance_generic_no_route_then_pump_quote(
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let balance = read_owned_sell_http_request(&listener).await;
        assert!(balance
            .body
            .contains("\"method\":\"getTokenAccountsByOwner\""));
        write_owned_sell_http_response(
            balance.socket,
            "200 OK",
            r#"{"jsonrpc":"2.0","id":"execution-canary-owned-sell-balance","result":{"value":[{"account":{"data":{"parsed":{"info":{"tokenAmount":{"amount":"10000","decimals":3}}}}}}]}}"#,
        )
        .await;

        let generic = read_owned_sell_http_request(&listener).await;
        assert!(generic.body.starts_with("GET /quote?"));
        assert!(generic.body.contains("amount=10000"));
        write_owned_sell_http_response(
            generic.socket,
            "400 Bad Request",
            r#"{"error":"No routes found","errorCode":"NO_ROUTES_FOUND"}"#,
        )
        .await;

        let pump = read_owned_sell_http_request(&listener).await;
        assert!(pump.body.starts_with("GET /pump-fun/quote?"));
        assert!(pump.body.contains("type=SELL"));
        assert!(pump.body.contains("amount=10000"));
        write_owned_sell_http_response(pump.socket, "200 OK", pump_fun_sell_quote_json()).await;
    });
    Ok((base_url, server))
}

async fn serve_owned_sell_balance_pump_error_then_generic_quote(
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let balance = read_owned_sell_http_request(&listener).await;
        assert!(balance
            .body
            .contains("\"method\":\"getTokenAccountsByOwner\""));
        write_owned_sell_http_response(
            balance.socket,
            "200 OK",
            r#"{"jsonrpc":"2.0","id":"execution-canary-owned-sell-balance","result":{"value":[{"account":{"data":{"parsed":{"info":{"tokenAmount":{"amount":"10000","decimals":3}}}}}}]}}"#,
        )
        .await;

        let pump = read_owned_sell_http_request(&listener).await;
        assert!(pump.body.starts_with("GET /pump-fun/quote?"));
        assert!(pump.body.contains("type=SELL"));
        assert!(pump.body.contains("amount=10000"));
        write_owned_sell_http_response(
            pump.socket,
            "400 Bad Request",
            r#"{"error":"Bonding curve for mint not found","errorCode":"NOT_FOUND"}"#,
        )
        .await;

        let generic = read_owned_sell_http_request(&listener).await;
        assert!(generic.body.starts_with("GET /quote?"));
        assert!(generic.body.contains("amount=10000"));
        write_owned_sell_http_response(generic.socket, "200 OK", generic_sell_quote_json()).await;
    });
    Ok((base_url, server))
}

struct OwnedSellHttpRequest {
    socket: tokio::net::TcpStream,
    body: String,
}

async fn read_owned_sell_http_request(listener: &tokio::net::TcpListener) -> OwnedSellHttpRequest {
    let (mut socket, _) = listener.accept().await.expect("http request");
    let mut buffer = [0_u8; 8192];
    let read = socket.read(&mut buffer).await.expect("read request");
    OwnedSellHttpRequest {
        socket,
        body: String::from_utf8_lossy(&buffer[..read]).to_string(),
    }
}

async fn write_owned_sell_http_response(
    mut socket: tokio::net::TcpStream,
    status: &str,
    body: &str,
) {
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write response");
}

fn pump_fun_owned_sell_signal(
    name: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-pump-owned-sell:{name}:leader-wallet:sell:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: "sell".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 1.2,
        notional_lamports: Some(Lamports::new(1_200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn pump_fun_quote_config(base_url: &str) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.quote_canary_enabled = true;
    config.quote_canary_base_url = base_url.to_string();
    config.quote_canary_timeout_ms = 1_000;
    config.quote_canary_sell_slippage_bps = 500;
    config.quote_canary_pump_fun_parallel_enabled = true;
    config
}

fn pump_fun_submit_config(base_url: &str) -> ExecutionConfig {
    let mut config = pump_fun_quote_config(base_url);
    config.submit_adapter_http_url = base_url.to_string();
    config
}

fn pump_fun_sell_quote_json() -> &'static str {
    r#"{"quote":{"mint":"TokenMint","type":"SELL","inAmount":"10000","inTokenAddress":"TokenMint","outAmount":"1200000000","outTokenAddress":"So11111111111111111111111111111111111111112","meta":{"isCompleted":false,"inDecimals":3,"outDecimals":9},"priceImpactPct":"0.01"}}"#
}

fn generic_sell_quote_json() -> &'static str {
    r#"{"inputMint":"TokenMint","inAmount":"10000","outputMint":"So11111111111111111111111111111111111111112","outAmount":"900000000","priceImpactPct":"0.02","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#
}

fn unique_pump_fun_owned_sell_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-pump-fun-owned-sell-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
