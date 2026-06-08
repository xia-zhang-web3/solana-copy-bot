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
