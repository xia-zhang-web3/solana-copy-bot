use super::*;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn pump_fun_paid_quote_builds_pump_fun_swap_transaction_payload() -> Result<()> {
    let db_path = unique_pump_fun_swap_test_path("payload");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        assert_pump_fun_request(
            read_http_request(&listener).await,
            "/pump-fun/swap-instructions",
        )
        .await;
        assert_pump_fun_request(read_http_request(&listener).await, "/pump-fun/swap").await;
    });
    let now = Utc::now();
    let signal = pump_fun_swap_signal(now);
    store.insert_copy_signal(&signal)?;
    store.record_execution_quote_canary_event(&quote_event(&signal, now))?;
    store.record_execution_quote_canary_provider_sample(
        &copybot_storage_core::ExecutionQuoteCanaryProviderSampleInsert {
            event_id: format!("quote:entry:{}", signal.signal_id),
            provider: copybot_storage_core::PROVIDER_PUMP_FUN_PAID.to_string(),
            side: "buy".to_string(),
            quote_status: "ok".to_string(),
            request_ts: now,
            quote_latency_ms: Some(9),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_response_json: Some(pump_quote_json()),
            quote_price_sol: Some(0.081),
            shadow_price_sol: Some(0.08),
            slippage_bps: Some(125.0),
            price_impact_pct: Some(0.01),
            route_plan_json: Some(r#"[{"swapInfo":{"label":"Pump.fun Paid"}}]"#.to_string()),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: None,
        },
    )?;
    let config = pump_fun_swap_config(&base_url);
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
    assert_eq!(summary.signing_envelope_built, 1);
    assert_eq!(
        summary.last_signing_envelope_mode.as_deref(),
        Some(
            crate::execution_signing_envelope::EXECUTION_SIGNING_ENVELOPE_MODE_SERIALIZED_TRANSACTION_DRY_RUN
        )
    );
    assert_eq!(summary.submit_disabled, 1);
    let simulation = order.simulation_error.as_deref().unwrap_or_default();
    assert!(simulation.contains("pump_fun_swap_instructions_ok"));
    assert!(simulation.contains("pump_fun_swap_transaction_ok"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn pump_fun_swap_instructions_error_is_soft_when_transaction_payload_passes() -> Result<()> {
    let db_path = unique_pump_fun_swap_test_path("instructions-soft-error");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_http_request(&listener).await;
        assert!(request.starts_with("POST /pump-fun/swap-instructions "));
        write_http_status_json(
            request.into_socket,
            400,
            r#"{"status":"error","message":"Failed to simulate transaction {\"InstructionError\":[3,{\"Custom\":6063}]}"}"#,
        )
        .await;

        assert_pump_fun_request(read_http_request(&listener).await, "/pump-fun/swap").await;
    });
    let now = Utc::now();
    let signal = pump_fun_swap_signal(now);
    store.insert_copy_signal(&signal)?;
    store.record_execution_quote_canary_event(&quote_event(&signal, now))?;
    store.record_execution_quote_canary_provider_sample(
        &copybot_storage_core::ExecutionQuoteCanaryProviderSampleInsert {
            event_id: format!("quote:entry:{}", signal.signal_id),
            provider: copybot_storage_core::PROVIDER_PUMP_FUN_PAID.to_string(),
            side: "buy".to_string(),
            quote_status: "ok".to_string(),
            request_ts: now,
            quote_latency_ms: Some(9),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_response_json: Some(pump_quote_json()),
            quote_price_sol: Some(0.081),
            shadow_price_sol: Some(0.08),
            slippage_bps: Some(125.0),
            price_impact_pct: Some(0.01),
            route_plan_json: Some(r#"[{"swapInfo":{"label":"Pump.fun Paid"}}]"#.to_string()),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: None,
        },
    )?;
    let config = pump_fun_swap_config(&base_url);
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
    assert_eq!(summary.signing_envelope_built, 1);
    assert_eq!(summary.submit_disabled, 1);
    let simulation = order.simulation_error.as_deref().unwrap_or_default();
    assert!(simulation.contains("pump_fun_swap_instructions_soft_failed"));
    assert!(simulation.contains("pump_fun_swap_transaction_ok"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn pump_fun_swap_transaction_simulation_error_marks_order_failed() -> Result<()> {
    let db_path = unique_pump_fun_swap_test_path("instructions-simulation-error");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_http_request(&listener).await;
        assert!(request.starts_with("POST /pump-fun/swap-instructions "));
        write_http_json(
            request.into_socket,
            r#"{"simulationError":"Transaction simulation failed: custom program error: 0x1788","instructions":[{"keys":[],"programId":"ComputeBudget111111111111111111111111111111","data":[2]}]}"#,
        )
        .await;

        let request = read_http_request(&listener).await;
        assert!(request.starts_with("POST /pump-fun/swap "));
        write_http_json(
            request.into_socket,
            r#"{"simulationError":"Transaction simulation failed: custom program error: 0x1788"}"#,
        )
        .await;
    });
    let now = Utc::now();
    let signal = pump_fun_swap_signal(now);
    store.insert_copy_signal(&signal)?;
    store.record_execution_quote_canary_event(&quote_event(&signal, now))?;
    store.record_execution_quote_canary_provider_sample(
        &copybot_storage_core::ExecutionQuoteCanaryProviderSampleInsert {
            event_id: format!("quote:entry:{}", signal.signal_id),
            provider: copybot_storage_core::PROVIDER_PUMP_FUN_PAID.to_string(),
            side: "buy".to_string(),
            quote_status: "ok".to_string(),
            request_ts: now,
            quote_latency_ms: Some(9),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_response_json: Some(pump_quote_json()),
            quote_price_sol: Some(0.081),
            shadow_price_sol: Some(0.08),
            slippage_bps: Some(125.0),
            price_impact_pct: Some(0.01),
            route_plan_json: Some(r#"[{"swapInfo":{"label":"Pump.fun Paid"}}]"#.to_string()),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: None,
        },
    )?;
    let config = pump_fun_swap_config(&base_url);
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
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_SIMULATION_FAILED)
    );
    assert!(order
        .simulation_error
        .as_deref()
        .unwrap_or_default()
        .contains("pump.fun swap transaction dry-run simulation error"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn generic_pump_fun_amm_missing_token_program_keeps_generic_failure() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_http_request(&listener).await;
        assert!(request.starts_with("POST /pump-fun/swap-instructions "));
        write_http_json(
            request.into_socket,
            r#"{"instructions":[{"keys":[],"programId":"ComputeBudget111111111111111111111111111111","data":[2]}]}"#,
        )
        .await;

        let request = read_http_request(&listener).await;
        assert!(request.starts_with("POST /pump-fun/swap "));
        write_http_json(
            request.into_socket,
            r#"{"error":"Missing token program for TokenMint"}"#,
        )
        .await;

        let request = read_http_request(&listener).await;
        assert!(request.starts_with("POST /swap-instructions "));
        assert!(request.contains("\"useSharedAccounts\":false"));
        write_http_json(
            request.into_socket,
            r#"{"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":{"error":"Error processing Instruction 5: An account required by the instruction is missing"}}"#,
        )
        .await;

        let request = read_http_request(&listener).await;
        assert!(request.starts_with("POST /swap-instructions "));
        assert!(request.contains("\"useSharedAccounts\":false"));
        write_http_json(
            request.into_socket,
            r#"{"error":"Missing token program for TokenMint"}"#,
        )
        .await;

        let request = read_http_request(&listener).await;
        assert!(request.starts_with("POST /swap "));
        write_http_json(
            request.into_socket,
            r#"{"error":"Missing token program for TokenMint"}"#,
        )
        .await;
    });
    let mut config = pump_fun_swap_config(&base_url);
    config.quote_canary_pump_fun_parallel_enabled = true;
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let request = generic_pump_fun_submit_request(&config);
    let plan = adapter.build_transaction_plan(&request)?;

    let error = adapter
        .simulate_transaction_plan(&plan)
        .await
        .expect_err("direct and generic missing token program should stay visible");
    server.await?;

    let error = error.to_string();
    assert!(error.contains("pump.fun direct swap failed"));
    assert!(error.contains("generic Metis swap failed"));
    assert!(error.contains("Missing token program"));
    let payload = plan
        .serialized_transaction_payload_slot
        .as_ref()
        .expect("serialized payload slot")
        .load()?;
    assert!(payload.is_none());
    Ok(())
}

async fn assert_pump_fun_request(request: CapturedRequest, path: &str) {
    assert!(request.starts_with(&format!("POST {path} ")));
    assert!(request.contains("\"wallet\":\"11111111111111111111111111111111\""));
    assert!(request.contains("\"type\":\"BUY\""));
    assert!(request.contains("\"mint\":\"TokenMint\""));
    assert!(request.contains("\"inAmount\":\"10000000\""));
    assert!(request.contains("\"priorityFeeLevel\":\"high\""));
    let body = if path.ends_with("swap-instructions") {
        r#"{"instructions":[{"keys":[],"programId":"ComputeBudget111111111111111111111111111111","data":[2]}]}"#
    } else {
        r#"{"tx":"AQIDBA=="}"#
    };
    write_http_json(request.into_socket, body).await;
}

struct CapturedRequest {
    into_socket: tokio::net::TcpStream,
    body: String,
}

impl std::ops::Deref for CapturedRequest {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

async fn read_http_request(listener: &tokio::net::TcpListener) -> CapturedRequest {
    let (mut socket, _) = listener.accept().await.expect("http request");
    let mut buffer = [0_u8; 8192];
    let read = socket.read(&mut buffer).await.expect("read request");
    CapturedRequest {
        into_socket: socket,
        body: String::from_utf8_lossy(&buffer[..read]).to_string(),
    }
}

async fn write_http_json(socket: tokio::net::TcpStream, body: &str) {
    write_http_status_json(socket, 200, body).await;
}

async fn write_http_status_json(mut socket: tokio::net::TcpStream, status: u16, body: &str) {
    let response = format!(
        "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write response");
}

fn pump_fun_swap_signal(ts: chrono::DateTime<Utc>) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: "shadow:sig-pump-fun-swap:leader-wallet:buy:TokenMint".to_string(),
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

fn quote_event(
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
) -> copybot_storage_core::ExecutionQuoteCanaryEventInsert {
    copybot_storage_core::ExecutionQuoteCanaryEventInsert {
        event_id: format!("quote:entry:{}", signal.signal_id),
        signal_id: Some(signal.signal_id.clone()),
        shadow_closed_trade_id: None,
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: "buy".to_string(),
        quote_status: "error".to_string(),
        request_ts: now,
        signal_ts: Some(signal.ts),
        decision_delay_ms: Some(7),
        quote_latency_ms: Some(11),
        leader_notional_sol: Some(signal.notional_sol),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("123456".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.081),
        shadow_price_sol: Some(0.08),
        slippage_bps: Some(125.0),
        price_impact_pct: Some(0.01),
        route_plan_json: Some(r#"[{"swapInfo":{"label":"Pump.fun Amm"}}]"#.to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(22_000),
        priority_fee_json: Some("{\"recommended\":22000}".to_string()),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("within_slippage_limit".to_string()),
        error: None,
    }
}

fn pump_quote_json() -> String {
    r#"{"quote":{"mint":"TokenMint","bondingCurve":"BondingCurve","type":"BUY","inAmount":"10000000","inTokenAddress":"So11111111111111111111111111111111111111112","outAmount":"123456","outTokenAddress":"TokenMint","meta":{"isCompleted":false,"outDecimals":6,"inDecimals":9}}}"#.to_string()
}

fn pump_fun_swap_config(base_url: &str) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_wallet_pubkey = "11111111111111111111111111111111".to_string();
    config.quote_canary_enabled = true;
    config.quote_canary_base_url = base_url.to_string();
    config.quote_canary_timeout_ms = 1_000;
    config.quote_canary_buy_slippage_bps = 500;
    config.swap_instructions_dry_run_enabled = true;
    config.swap_transaction_dry_run_enabled = true;
    config
}

fn unique_pump_fun_swap_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-pump-fun-swap-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}

fn generic_pump_fun_submit_request(
    config: &ExecutionConfig,
) -> crate::execution_submit_adapter::ExecutionSubmitRequest {
    crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: "order-generic-pump-fun-missing-token-program".to_string(),
        signal_id: "signal-generic-pump-fun-missing-token-program".to_string(),
        client_order_id: "client-generic-pump-fun-missing-token-program".to_string(),
        attempt: 1,
        route: config.canary_route.clone(),
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMint".to_string(),
        side: "buy".to_string(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: config.canary_wallet_pubkey.clone(),
        entry_route_plan_json: None,
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
            quote_source: Some(
                crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS.to_string(),
            ),
            quote_event_id: Some("quote:entry:generic-pump-fun".to_string()),
            quote_request_ts: None,
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_response_json: None,
            quote_price_sol: Some(0.081),
            price_impact_pct: Some(0.01),
            route_plan_json: Some(r#"[{"swapInfo":{"label":"Pump.fun Amm"}}]"#.to_string()),
            priority_fee_source: Some("test".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(22_000),
            priority_fee_json: Some(r#"{"recommended":22000}"#.to_string()),
            slippage_bps: Some(125.0),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
        },
    }
}
