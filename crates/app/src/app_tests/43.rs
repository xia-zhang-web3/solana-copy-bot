use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

#[tokio::test]
async fn execution_canary_records_hot_observed_buy_quote_before_shadow_join() -> Result<()> {
    let db_path = unique_execution_canary_test_path("hot-observed-buy-quote");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let quote_base_url = format!("http://{}", listener.local_addr()?);
    let quote_server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("quote request");
        let mut buffer = [0_u8; 2048];
        let _ = socket.read(&mut buffer).await.expect("read quote request");
        let body = r#"{"inAmount":"200000000","outAmount":"1000000","priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write quote response");
    });

    let now = Utc::now();
    let swap = copybot_core_types::SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpfun".to_string(),
        token_in: crate::execution_quote_canary_helpers::SOL_MINT.to_string(),
        token_out: "TokenMint".to_string(),
        amount_in: 0.2,
        amount_out: 1.0,
        signature: "sig-hot-observed".to_string(),
        slot: 42,
        ts_utc: now - chrono::Duration::milliseconds(25),
        exact_amounts: Some(copybot_core_types::ExactSwapAmounts {
            amount_in_raw: "200000000".to_string(),
            amount_in_decimals: 9,
            amount_out_raw: "1000000".to_string(),
            amount_out_decimals: 6,
        }),
    };

    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.quote_canary_enabled = true;
    config.quote_canary_base_url = quote_base_url;
    config.quote_canary_buy_size_sol = 0.2;
    config.quote_canary_buy_slippage_bps = 50;
    config.quote_canary_timeout_ms = 1_000;
    let runner = ExecutionCanaryRunner::new(config);

    let summary = runner
        .process_hot_observed_buy_quote(&store, &swap, now)
        .await?;
    quote_server.await?;

    assert_eq!(summary.quote_entry_candidates, 1);
    assert_eq!(summary.quote_entry_inserted, 1);
    assert_eq!(summary.quote_would_execute, 1);
    assert_eq!(
        summary.last_quote_event_id.as_deref(),
        Some("quote:entry:shadow:sig-hot-observed:leader-wallet:buy:TokenMint")
    );

    store.insert_copy_signal(&copybot_core_types::CopySignalRow {
        signal_id: "shadow:sig-hot-observed:leader-wallet:buy:TokenMint".to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: swap.ts_utc,
        status: "shadow_recorded".to_string(),
    })?;
    let signal = copybot_shadow::ShadowSignalResult {
        signal_id: "shadow:sig-hot-observed:leader-wallet:buy:TokenMint".to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        latency_ms: 25,
        closed_qty: 0.0,
        realized_pnl_sol: 0.0,
        has_open_lots_after_signal: Some(true),
    };
    let hot_shadow_summary = runner
        .process_recorded_shadow_signal(&store, &signal, now)
        .await?;
    assert_eq!(hot_shadow_summary.quote_entry_existing, 1);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn hot_observed_buy_quote_runs_before_priority_fee_sample() -> Result<()> {
    let db_path = unique_execution_canary_test_path("hot-observed-buy-quote-before-fee");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;

    let quote_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let quote_base_url = format!("http://{}", quote_listener.local_addr()?);
    let priority_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let priority_url = format!("http://{}", priority_listener.local_addr()?);
    let (quote_seen_tx, quote_seen_rx) = oneshot::channel();

    let quote_server = tokio::spawn(async move {
        let (mut socket, _) = quote_listener.accept().await.expect("quote request");
        let mut buffer = [0_u8; 2048];
        let _ = socket.read(&mut buffer).await.expect("read quote request");
        let _ = quote_seen_tx.send(());
        let body = r#"{"inAmount":"200000000","outAmount":"1000000","priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write quote response");
    });

    let priority_server = tokio::spawn(async move {
        let (mut socket, _) = priority_listener
            .accept()
            .await
            .expect("priority fee request");
        let mut buffer = [0_u8; 2048];
        let _ = socket
            .read(&mut buffer)
            .await
            .expect("read priority request");
        quote_seen_rx.await.expect("quote must be requested first");
        let body = r#"{"result":{"recommended":12345}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write priority response");
    });

    let now = Utc::now();
    let swap = copybot_core_types::SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpfun".to_string(),
        token_in: crate::execution_quote_canary_helpers::SOL_MINT.to_string(),
        token_out: "TokenMint".to_string(),
        amount_in: 0.2,
        amount_out: 1.0,
        signature: "sig-hot-observed-priority-order".to_string(),
        slot: 42,
        ts_utc: now - chrono::Duration::milliseconds(25),
        exact_amounts: Some(copybot_core_types::ExactSwapAmounts {
            amount_in_raw: "200000000".to_string(),
            amount_in_decimals: 9,
            amount_out_raw: "1000000".to_string(),
            amount_out_decimals: 6,
        }),
    };

    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.quote_canary_enabled = true;
    config.quote_canary_base_url = quote_base_url;
    config.quote_canary_buy_size_sol = 0.2;
    config.quote_canary_buy_slippage_bps = 50;
    config.quote_canary_timeout_ms = 1_000;
    config.priority_fee_canary_enabled = true;
    config.priority_fee_canary_rpc_url = priority_url;
    config.priority_fee_canary_account = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".to_string();
    config.priority_fee_canary_timeout_ms = 1_000;
    let runner = ExecutionCanaryRunner::new(config);

    let summary = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        runner.process_hot_observed_buy_quote(&store, &swap, now),
    )
    .await??;
    quote_server.await?;
    priority_server.await?;

    assert_eq!(summary.quote_entry_inserted, 1);
    assert_eq!(summary.quote_would_execute, 1);
    let event = store
        .load_latest_execution_quote_canary_entry_event(
            "shadow:sig-hot-observed-priority-order:leader-wallet:buy:TokenMint",
        )?
        .expect("hot observed quote event");
    assert_eq!(event.priority_fee_lamports, Some(12_345));
    assert_eq!(event.decision_status.as_deref(), Some("would_execute"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}
