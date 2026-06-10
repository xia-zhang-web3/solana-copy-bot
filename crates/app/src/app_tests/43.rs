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
async fn hot_observed_buy_retries_transient_token_not_tradable() -> Result<()> {
    let db_path = unique_execution_canary_test_path("hot-observed-token-not-tradable-retry");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let quote_base_url = format!("http://{}", listener.local_addr()?);
    let quote_server = tokio::spawn(async move {
        for attempt in 0..2 {
            let (mut socket, _) = listener.accept().await.expect("quote request");
            let mut buffer = [0_u8; 2048];
            let read = socket.read(&mut buffer).await.expect("read quote request");
            let request = String::from_utf8_lossy(&buffer[..read]);
            assert!(request.starts_with("GET /quote?"));
            if attempt == 0 {
                write_http_response(
                    &mut socket,
                    400,
                    r#"{"error":"The token TokenMint is not tradable","errorCode":"TOKEN_NOT_TRADABLE"}"#,
                )
                .await;
            } else {
                write_http_response(
                    &mut socket,
                    200,
                    r#"{"inAmount":"200000000","outAmount":"1000000","priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#,
                )
                .await;
            }
        }
    });

    let now = Utc::now();
    let swap = copybot_core_types::SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpfun".to_string(),
        token_in: crate::execution_quote_canary_helpers::SOL_MINT.to_string(),
        token_out: "TokenMint".to_string(),
        amount_in: 0.2,
        amount_out: 1.0,
        signature: "sig-hot-observed-token-not-tradable-retry".to_string(),
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

    assert_eq!(summary.quote_entry_inserted, 1);
    assert_eq!(summary.quote_would_execute, 1);
    let event = store
        .load_latest_execution_quote_canary_entry_event(
            "shadow:sig-hot-observed-token-not-tradable-retry:leader-wallet:buy:TokenMint",
        )?
        .expect("hot observed quote event");
    assert_eq!(event.quote_status, "ok");
    assert_eq!(event.decision_status.as_deref(), Some("would_execute"));

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

async fn write_http_response(socket: &mut tokio::net::TcpStream, status: u16, body: &str) {
    let response = format!(
        "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write quote response");
}

#[tokio::test]
async fn hot_observed_buy_records_paid_pump_fun_provider_comparison() -> Result<()> {
    let db_path = unique_execution_canary_test_path("hot-observed-paid-pump-compare");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let quote_base_url = format!("http://{}", listener.local_addr()?);
    let quote_server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.expect("quote request");
            let mut buffer = [0_u8; 4096];
            let read = socket.read(&mut buffer).await.expect("read quote request");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let body = if request.starts_with("GET /pump-fun/quote") {
                r#"{"quote":{"mint":"TokenMint","bondingCurve":"Curve","type":"BUY","inAmount":"200000000","inAmountUi":0.2,"inTokenAddress":"So11111111111111111111111111111111111111112","outAmount":"1900000","outAmountUi":1.9,"outTokenAddress":"TokenMint","meta":{"isCompleted":false,"outDecimals":6,"inDecimals":9,"totalSupply":"1","currentMarketCapInSol":1.0}}}"#
            } else {
                r#"{"inAmount":"200000000","outAmount":"1800000","priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write quote response");
        }
    });

    let now = Utc::now();
    let swap = copybot_core_types::SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpfun".to_string(),
        token_in: crate::execution_quote_canary_helpers::SOL_MINT.to_string(),
        token_out: "TokenMint".to_string(),
        amount_in: 0.2,
        amount_out: 2.0,
        signature: "sig-hot-observed-paid-pump".to_string(),
        slot: 42,
        ts_utc: now - chrono::Duration::milliseconds(25),
        exact_amounts: Some(copybot_core_types::ExactSwapAmounts {
            amount_in_raw: "200000000".to_string(),
            amount_in_decimals: 9,
            amount_out_raw: "2000000".to_string(),
            amount_out_decimals: 6,
        }),
    };

    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.quote_canary_enabled = true;
    config.quote_canary_base_url = quote_base_url;
    config.quote_canary_pump_fun_parallel_enabled = true;
    config.quote_canary_buy_size_sol = 0.2;
    config.quote_canary_buy_slippage_bps = 1_000;
    config.quote_canary_timeout_ms = 1_000;
    let runner = ExecutionCanaryRunner::new(config);

    let summary = runner
        .process_hot_observed_buy_quote(&store, &swap, now)
        .await?;
    quote_server.await?;

    assert_eq!(summary.quote_entry_inserted, 1);
    let comparison = store.execution_quote_canary_provider_comparison_summary(
        now,
        now - chrono::Duration::seconds(1),
        10,
    )?;
    assert_eq!(comparison.paired_events, 1);
    assert_eq!(comparison.pump_fun_better_slippage_events, 1);
    assert_eq!(
        comparison
            .latest
            .first()
            .and_then(|event| event.better_provider.as_deref()),
        Some(copybot_storage_core::PROVIDER_PUMP_FUN_PAID)
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn public_platform_fee_fallback_does_not_mark_quote_executable() -> Result<()> {
    let db_path = unique_execution_canary_test_path("public-platform-fee-fallback");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;

    let primary_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let primary_url = format!("http://{}", primary_listener.local_addr()?);
    let primary_server = tokio::spawn(async move {
        for _ in 0..4 {
            let (mut socket, _) = primary_listener.accept().await.expect("primary quote");
            let mut buffer = [0_u8; 2048];
            let _ = socket.read(&mut buffer).await.expect("read primary quote");
            write_http_response(
                &mut socket,
                400,
                r#"{"error":"The token TokenMint is not tradable","errorCode":"TOKEN_NOT_TRADABLE"}"#,
            )
            .await;
        }
    });

    let now = Utc::now();
    let signal = copybot_shadow::ShadowSignalResult {
        signal_id: "shadow:sig-public-platform-fee:leader-wallet:buy:TokenMint".to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        latency_ms: 25,
        closed_qty: 0.0,
        realized_pnl_sol: 0.0,
        has_open_lots_after_signal: Some(true),
    };
    store.insert_copy_signal(&copybot_core_types::CopySignalRow {
        signal_id: signal.signal_id.clone(),
        wallet_id: signal.wallet_id.clone(),
        side: signal.side.clone(),
        token: signal.token.clone(),
        notional_sol: signal.notional_sol,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: now - chrono::Duration::milliseconds(25),
        status: "shadow_recorded".to_string(),
    })?;

    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.quote_canary_enabled = true;
    config.quote_canary_base_url = primary_url;
    config.quote_canary_public_parallel_enabled = true;
    config.quote_canary_public_base_url = "http://127.0.0.1:9".to_string();
    config.quote_canary_buy_size_sol = 0.2;
    config.quote_canary_buy_slippage_bps = 1_000;
    config.quote_canary_timeout_ms = 1_000;
    let runner = ExecutionCanaryRunner::new(config);

    let summary = runner
        .process_recorded_shadow_signal(&store, &signal, now)
        .await?;
    primary_server.await?;
    let event = store
        .load_latest_execution_quote_canary_entry_event(&signal.signal_id)?
        .expect("quote event should exist");

    assert_eq!(summary.quote_entry_errors, 1);
    assert_eq!(summary.quote_would_execute, 0);
    assert_eq!(event.quote_status, "error");
    assert_eq!(event.decision_status.as_deref(), Some("unknown"));
    assert!(event
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("TOKEN_NOT_TRADABLE"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}
