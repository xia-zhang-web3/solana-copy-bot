use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn swap_instructions_dry_run_posts_http_before_submit_disabled() -> Result<()> {
    let db_path = unique_swap_instructions_test_path("http-ok");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("swap request");
        let mut buffer = [0_u8; 8192];
        let read = socket.read(&mut buffer).await.expect("read request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /swap-instructions "));
        assert!(request.contains("\"userPublicKey\":\"11111111111111111111111111111111\""));
        assert!(request.contains("\"quoteResponse\""));
        assert!(request.contains("\"loadedLongtailToken\":true"));
        assert!(request.contains("\"prioritizationFeeLamports\":22000"));
        assert!(request.contains("\"useSharedAccounts\":false"));
        let body = r#"{"computeBudgetInstructions":[{}],"setupInstructions":[{}],"swapInstruction":{},"cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":["alt"],"simulationError":null}"#;
        write_http_json(&mut socket, body).await;
    });
    let now = Utc::now();
    let signal = swap_instructions_signal("http-ok", now);
    store.insert_copy_signal(&signal)?;
    record_swap_instructions_quote(&store, &signal, now)?;
    let config = swap_instructions_config(base_url, true);
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let state_machine =
        crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(config, adapter);

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    server.await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("order should exist");

    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.submit_disabled, 1);
    assert_eq!(
        order.simulation_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED)
    );
    assert!(order
        .simulation_error
        .as_deref()
        .unwrap_or_default()
        .contains("metis_swap_instructions_no_shared_accounts_ok"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn swap_instructions_dry_run_missing_swap_instruction_fails_simulation() -> Result<()> {
    let db_path = unique_swap_instructions_test_path("http-missing-swap");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("swap request");
        let mut buffer = [0_u8; 4096];
        let _ = socket.read(&mut buffer).await.expect("read request");
        write_http_json(&mut socket, r#"{"setupInstructions":[]}"#).await;
    });
    let now = Utc::now();
    let signal = swap_instructions_signal("http-missing-swap", now);
    store.insert_copy_signal(&signal)?;
    record_swap_instructions_quote(&store, &signal, now)?;
    let config = swap_instructions_config(base_url, true);
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let state_machine =
        crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(config, adapter);

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    server.await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("order should exist");

    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.submit_disabled, 0);
    assert_eq!(
        order.simulation_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_SIMULATION_STATUS_FAILED)
    );
    assert!(order
        .simulation_error
        .as_deref()
        .unwrap_or_default()
        .contains("missing swapInstruction"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn swap_instructions_dry_run_simulation_error_fails_before_submit() -> Result<()> {
    let db_path = unique_swap_instructions_test_path("http-simulation-error");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("swap request");
        let mut buffer = [0_u8; 4096];
        let _ = socket.read(&mut buffer).await.expect("read request");
        write_http_json(
            &mut socket,
            r#"{"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":{"error":"Slippage tolerance exceeded","errorCode":"TRANSACTION_ERROR"}}"#,
        )
        .await;
    });
    let now = Utc::now();
    let signal = swap_instructions_signal("http-simulation-error", now);
    store.insert_copy_signal(&signal)?;
    record_swap_instructions_quote(&store, &signal, now)?;
    let config = swap_instructions_config(base_url, true);
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let state_machine =
        crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(config, adapter);

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    server.await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("order should exist");

    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.submit_disabled, 0);
    assert_eq!(
        order.simulation_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_SIMULATION_STATUS_FAILED)
    );
    assert!(order
        .simulation_error
        .as_deref()
        .unwrap_or_default()
        .contains("swap-instructions dry-run simulation error"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn swap_instructions_dry_run_retries_missing_account_without_shared_accounts() -> Result<()> {
    let db_path = unique_swap_instructions_test_path("http-no-shared");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.expect("first swap request");
        let mut buffer = [0_u8; 4096];
        let _ = first.read(&mut buffer).await.expect("read first request");
        write_http_json(
            &mut first,
            r#"{"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":{"error":"Error processing Instruction 5: An account required by the instruction is missing","errorCode":"TRANSACTION_ERROR"}}"#,
        )
        .await;
        drop(first);

        let (mut second, _) = listener.accept().await.expect("retry swap request");
        let read = second.read(&mut buffer).await.expect("read retry request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.contains("\"useSharedAccounts\":false"));
        write_http_json(
            &mut second,
            r#"{"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":null}"#,
        )
        .await;
    });
    let now = Utc::now();
    let signal = swap_instructions_signal("http-no-shared", now);
    store.insert_copy_signal(&signal)?;
    record_swap_instructions_quote(&store, &signal, now)?;
    let config = swap_instructions_config(base_url, true);
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let state_machine =
        crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(config, adapter);

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    server.await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("order should exist");

    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.submit_disabled, 1);
    assert_eq!(
        order.simulation_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED)
    );
    assert!(order
        .simulation_error
        .as_deref()
        .unwrap_or_default()
        .contains("metis_swap_instructions_no_shared_accounts_ok"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn swap_instructions_dry_run_retries_missing_token_program() -> Result<()> {
    let db_path = unique_swap_instructions_test_path("http-retry-token-program");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.expect("first swap request");
        let mut buffer = [0_u8; 4096];
        let read = first.read(&mut buffer).await.expect("read first request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /swap-instructions "));
        write_http_status(
            &mut first,
            500,
            r#"{"error":"Missing token program for TokenMint"}"#,
        )
        .await;
        drop(first);

        let (mut second, _) = listener.accept().await.expect("retry swap request");
        let read = second.read(&mut buffer).await.expect("read retry request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /swap-instructions "));
        write_http_json(
            &mut second,
            r#"{"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":null}"#,
        )
        .await;
    });
    let now = Utc::now();
    let signal = swap_instructions_signal("http-retry-token-program", now);
    store.insert_copy_signal(&signal)?;
    record_swap_instructions_quote(&store, &signal, now)?;
    let config = swap_instructions_config(base_url, true);
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let state_machine =
        crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(config, adapter);

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    server.await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("order should exist");

    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.submit_disabled, 1);
    assert!(order
        .simulation_error
        .as_deref()
        .unwrap_or_default()
        .contains("attempts=2"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn swap_instructions_dry_run_does_not_fallback_to_public_builder() -> Result<()> {
    let db_path = unique_swap_instructions_test_path("http-public-fallback");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let primary_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let primary_url = format!("http://{}", primary_listener.local_addr()?);
    let primary_server = tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        for _ in 0..3 {
            let (mut socket, _) = primary_listener.accept().await.expect("primary request");
            let read = socket
                .read(&mut buffer)
                .await
                .expect("read primary request");
            let request = String::from_utf8_lossy(&buffer[..read]);
            assert!(request.starts_with("POST /swap-instructions "));
            write_http_status(
                &mut socket,
                500,
                r#"{"error":"Missing token program for TokenMint"}"#,
            )
            .await;
        }
    });
    let now = Utc::now();
    let signal = swap_instructions_signal("http-public-fallback", now);
    store.insert_copy_signal(&signal)?;
    record_swap_instructions_quote(&store, &signal, now)?;
    let mut config = swap_instructions_config(primary_url, true);
    config.quote_canary_public_parallel_enabled = true;
    config.quote_canary_public_base_url = "http://127.0.0.1:9".to_string();
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let state_machine =
        crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(config, adapter);

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    primary_server.await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("order should exist");

    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.submit_disabled, 1);
    assert_eq!(
        order.simulation_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED)
    );
    let proof = order.simulation_error.as_deref().unwrap_or_default();
    assert!(proof.contains("metis_swap_instructions_missing_token_program_soft_failed"));
    assert!(!proof.contains("public_fallback"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn metis_dry_run_does_not_fallback_to_leader_wallet() -> Result<()> {
    let db_path = unique_swap_instructions_test_path("missing-canary-wallet");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = swap_instructions_signal("missing-canary-wallet", now);
    store.insert_copy_signal(&signal)?;
    record_swap_instructions_quote(&store, &signal, now)?;
    let mut config = swap_instructions_config("http://127.0.0.1:1".to_string(), true);
    config.canary_wallet_pubkey.clear();
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let state_machine =
        crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(config, adapter);

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("order should exist");

    assert_eq!(summary.failed, 1);
    assert_eq!(
        order.simulation_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_SIMULATION_STATUS_FAILED)
    );
    assert!(order
        .simulation_error
        .as_deref()
        .unwrap_or_default()
        .contains("missing user public key"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

async fn write_http_json(socket: &mut tokio::net::TcpStream, body: &str) {
    write_http_status(socket, 200, body).await;
}

async fn write_http_status(socket: &mut tokio::net::TcpStream, status: u16, body: &str) {
    let response = format!(
        "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write response");
}

fn swap_instructions_config(base_url: String, enabled: bool) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_wallet_pubkey = "11111111111111111111111111111111".to_string();
    config.quote_canary_enabled = true;
    config.quote_canary_base_url = base_url;
    config.quote_canary_timeout_ms = 1_000;
    config.quote_canary_buy_slippage_bps = 500;
    config.swap_instructions_dry_run_enabled = enabled;
    config
}

fn swap_instructions_signal(
    signal_id: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-swap-instructions:leader-wallet:{signal_id}:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn record_swap_instructions_quote(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
) -> Result<()> {
    store.record_execution_quote_canary_event(
        &copybot_storage_core::ExecutionQuoteCanaryEventInsert {
            event_id: format!("quote:entry:{}", signal.signal_id),
            signal_id: Some(signal.signal_id.clone()),
            shadow_closed_trade_id: None,
            wallet_id: signal.wallet_id.clone(),
            token: signal.token.clone(),
            side: "buy".to_string(),
            quote_status: "ok".to_string(),
            request_ts: now,
            signal_ts: Some(signal.ts),
            decision_delay_ms: Some(7),
            quote_latency_ms: Some(11),
            leader_notional_sol: Some(signal.notional_sol),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_response_json: Some(
                r#"{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"TokenMint","outAmount":"123456","otherAmountThreshold":"117283","swapMode":"ExactIn","slippageBps":500,"platformFee":null,"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}],"loadedLongtailToken":true}"#.to_string(),
            ),
            quote_price_sol: Some(0.081),
            shadow_price_sol: Some(0.08),
            slippage_bps: Some(125.0),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(22_000),
            priority_fee_json: Some("{\"recommended\":22000}".to_string()),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: None,
        },
    )?;
    Ok(())
}

fn unique_swap_instructions_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-swap-instructions-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
