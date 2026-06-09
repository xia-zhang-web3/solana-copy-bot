use super::*;
use copybot_core_types::{
    CopySignalRow, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn first_attempt_sell_quote_error_records_failed_order() -> Result<()> {
    let db_path = unique_first_attempt_sell_failure_path("quote-error");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let token = "TokenMint";
    let signal = first_attempt_sell_signal(token, now + chrono::Duration::seconds(1));
    let quote_event_id = "quote:close:first-attempt-error";
    store.record_execution_canary_open_position(
        "existing-confirmed-buy",
        token,
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        now,
    )?;
    store.insert_copy_signal(&signal)?;
    store.record_execution_quote_canary_event(&first_attempt_sell_quote(
        quote_event_id,
        &signal,
        now + chrono::Duration::seconds(2),
    ))?;
    let (base_url, server) = serve_first_attempt_sell_no_route().await?;
    let config = first_attempt_sell_config(&base_url);

    let summary = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
        &config,
        &store,
        quote_event_id,
        now + chrono::Duration::seconds(3),
    )
    .await?
    .expect("fresh sell quote event should be processed");
    server.await??;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("fresh sell quote failure should be persisted as an order");

    assert_eq!(summary.sell_candidates, 1);
    assert_eq!(summary.sell_execute, 1);
    assert_eq!(summary.reserved, 1);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.entry_gate_blocked, 0);
    assert_eq!(summary.skipped_reason, Some("owned_sell_quote_error"));
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_BUILD_FAILED)
    );
    assert_eq!(order.attempt, 1);
    assert!(order
        .simulation_error
        .as_deref()
        .unwrap_or_default()
        .contains("NO_ROUTES_FOUND"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

async fn serve_first_attempt_sell_no_route() -> Result<(String, tokio::task::JoinHandle<Result<()>>)>
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        let (mut socket, _) = listener.accept().await?;
        let read = socket.read(&mut buffer).await?;
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.contains("getTokenAccountsByOwner"));
        write_first_attempt_response(
            &mut socket,
            "200 OK",
            r#"{"jsonrpc":"2.0","id":"execution-canary-owned-sell-balance","result":{"value":[{"account":{"data":{"parsed":{"info":{"mint":"TokenMint","tokenAmount":{"amount":"10000","decimals":3,"uiAmountString":"10"}}}}}}]}}"#,
        )
        .await?;

        let (mut socket, _) = listener.accept().await?;
        let read = socket.read(&mut buffer).await?;
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("GET /quote?"));
        assert!(request.contains("amount=10000"));
        write_first_attempt_response(
            &mut socket,
            "400 Bad Request",
            r#"{"error":"No routes found","errorCode":"NO_ROUTES_FOUND"}"#,
        )
        .await?;
        Ok(())
    });
    Ok((base_url, server))
}

async fn write_first_attempt_response(
    socket: &mut tokio::net::TcpStream,
    status: &str,
    body: &str,
) -> Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    socket.write_all(response.as_bytes()).await?;
    Ok(())
}

fn first_attempt_sell_config(base_url: &str) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_tiny_submit_enabled = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_wallet_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_keypair_path = "/tmp/non-empty-keypair.json".to_string();
    config.submit_adapter_http_url = base_url.to_string();
    config.quote_canary_base_url = base_url.to_string();
    config.quote_canary_timeout_ms = 1_000;
    config.max_submit_attempts = 3;
    config.swap_transaction_dry_run_enabled = true;
    config
}

fn first_attempt_sell_signal(token: &str, ts: chrono::DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: format!("shadow:sig-first-attempt-sell:leader:{token}"),
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

fn first_attempt_sell_quote(
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

fn unique_first_attempt_sell_failure_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-first-attempt-sell-failure-{name}-{}-{nanos}",
        std::process::id()
    ))
}
