use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn pump_fun_bonding_quote_selects_pump_fun_swap_instructions() -> Result<()> {
    let db_path = unique_provider_selector_test_path("pump-selected");
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
        assert!(request.contains("\"wallet\":\"11111111111111111111111111111111\""));
        assert!(request.contains("\"type\":\"BUY\""));
        assert!(request.contains("\"mint\":\"TokenMint\""));
        assert!(request.contains("\"inAmount\":\"10000000\""));
        assert!(request.contains("\"priorityFeeLevel\":\"high\""));
        write_http_json(
            request.into_socket,
            r#"{"instructions":[{"keys":[],"programId":"ComputeBudget111111111111111111111111111111","data":[2]}]}"#,
        )
        .await;
    });
    let now = Utc::now();
    let signal = provider_selector_signal("pump-selected", now);
    store.insert_copy_signal(&signal)?;
    record_provider_selector_quote(&store, &signal, now, "error", None)?;
    record_provider_sample(
        &store,
        &signal,
        now,
        copybot_storage_core::PROVIDER_PUMP_FUN_PAID,
        "ok",
        Some(pump_quote_json(false)),
        Some("would_execute"),
        None,
    )?;
    let config = provider_selector_config(base_url);
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
        .contains("pump_fun_swap_instructions_ok"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn pump_fun_not_found_keeps_generic_metis_swap_instructions() -> Result<()> {
    let db_path = unique_provider_selector_test_path("generic-selected");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_http_request(&listener).await;
        assert!(request.starts_with("POST /swap-instructions "));
        assert!(request.contains("\"quoteResponse\""));
        write_http_json(
            request.into_socket,
            r#"{"swapInstruction":{},"computeBudgetInstructions":[],"setupInstructions":[],"otherInstructions":[],"addressLookupTableAddresses":[],"cleanupInstruction":null,"simulationError":null}"#,
        )
        .await;
    });
    let now = Utc::now();
    let signal = provider_selector_signal("generic-selected", now);
    store.insert_copy_signal(&signal)?;
    record_provider_selector_quote(&store, &signal, now, "ok", Some(generic_quote_json()))?;
    record_provider_sample(
        &store,
        &signal,
        now,
        copybot_storage_core::PROVIDER_PUMP_FUN_PAID,
        "error",
        None,
        Some("unknown"),
        Some(
            r#"pump.fun paid quote returned HTTP 400 Bad Request: {"status":"error","message":"Bonding curve for mint not found"}"#,
        ),
    )?;
    let config = provider_selector_config(base_url);
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
        .contains("metis_swap_instructions_ok"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn generic_public_platform_fee_does_not_replace_buildable_metis_quote() -> Result<()> {
    let db_path = unique_provider_selector_test_path("public-fee-fallback");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let request = read_http_request(&listener).await;
        assert!(request.starts_with("POST /swap-instructions "));
        assert!(request.contains("\"quoteResponse\""));
        assert!(request.contains("\"platformFee\":null"));
        assert!(!request.contains("\"feeBps\":20"));
        write_http_json(
            request.into_socket,
            r#"{"swapInstruction":{},"computeBudgetInstructions":[],"setupInstructions":[],"otherInstructions":[],"addressLookupTableAddresses":[],"cleanupInstruction":null,"simulationError":null}"#,
        )
        .await;
    });
    let now = Utc::now();
    let signal = provider_selector_signal("public-fee-fallback", now);
    store.insert_copy_signal(&signal)?;
    record_provider_selector_quote(
        &store,
        &signal,
        now,
        "ok",
        Some(public_platform_fee_quote_json()),
    )?;
    record_provider_sample_with_slippage(
        &store,
        &signal,
        now,
        copybot_storage_core::PROVIDER_GENERIC_METIS,
        "ok",
        Some(generic_quote_json()),
        Some("would_execute"),
        None,
        -455.0,
    )?;
    record_provider_sample_with_slippage(
        &store,
        &signal,
        now,
        copybot_storage_core::PROVIDER_GENERIC_PUBLIC,
        "ok",
        Some(public_platform_fee_quote_json()),
        Some("would_execute"),
        None,
        -849.0,
    )?;
    let config = provider_selector_config(base_url);
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
    let metadata = store
        .load_execution_canary_build_plan_metadata(&order.order_id)?
        .expect("metadata should exist");

    assert_eq!(summary.entry_gate_blocked, 0);
    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.submit_disabled, 1);
    assert_eq!(
        metadata.quote_source.as_deref(),
        Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS)
    );
    assert!(order
        .simulation_error
        .as_deref()
        .unwrap_or_default()
        .contains("metis_swap_instructions_ok"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
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

async fn write_http_json(mut socket: tokio::net::TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write response");
}

fn provider_selector_config(base_url: String) -> ExecutionConfig {
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
    config.swap_instructions_dry_run_enabled = true;
    config
}

fn provider_selector_signal(
    signal_id: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-provider-selector:leader-wallet:{signal_id}:TokenMint"),
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

fn record_provider_selector_quote(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
    quote_status: &str,
    quote_response_json: Option<String>,
) -> Result<()> {
    store.record_execution_quote_canary_event(&quote_event(
        signal,
        now,
        quote_status,
        quote_response_json,
    ))?;
    Ok(())
}

fn record_provider_sample(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
    provider: &str,
    quote_status: &str,
    quote_response_json: Option<String>,
    decision_status: Option<&str>,
    error: Option<&str>,
) -> Result<()> {
    record_provider_sample_with_slippage(
        store,
        signal,
        now,
        provider,
        quote_status,
        quote_response_json,
        decision_status,
        error,
        125.0,
    )
}

fn record_provider_sample_with_slippage(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
    provider: &str,
    quote_status: &str,
    quote_response_json: Option<String>,
    decision_status: Option<&str>,
    error: Option<&str>,
    slippage_bps: f64,
) -> Result<()> {
    store.record_execution_quote_canary_provider_sample(
        &copybot_storage_core::ExecutionQuoteCanaryProviderSampleInsert {
            event_id: format!("quote:entry:{}", signal.signal_id),
            provider: provider.to_string(),
            side: "buy".to_string(),
            quote_status: quote_status.to_string(),
            request_ts: now,
            quote_latency_ms: Some(9),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_response_json,
            quote_price_sol: Some(0.081),
            shadow_price_sol: Some(0.08),
            slippage_bps: Some(slippage_bps),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Paid\"}}]".to_string()),
            decision_status: decision_status.map(ToString::to_string),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: error.map(ToString::to_string),
        },
    )?;
    Ok(())
}

fn quote_event(
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
    quote_status: &str,
    quote_response_json: Option<String>,
) -> copybot_storage_core::ExecutionQuoteCanaryEventInsert {
    copybot_storage_core::ExecutionQuoteCanaryEventInsert {
        event_id: format!("quote:entry:{}", signal.signal_id),
        signal_id: Some(signal.signal_id.clone()),
        shadow_closed_trade_id: None,
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: "buy".to_string(),
        quote_status: quote_status.to_string(),
        request_ts: now,
        signal_ts: Some(signal.ts),
        decision_delay_ms: Some(7),
        quote_latency_ms: Some(11),
        leader_notional_sol: Some(signal.notional_sol),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("123456".to_string()),
        quote_response_json,
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
    }
}

fn generic_quote_json() -> String {
    r#"{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"TokenMint","outAmount":"123456","otherAmountThreshold":"117283","swapMode":"ExactIn","slippageBps":500,"platformFee":null,"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#.to_string()
}

fn public_platform_fee_quote_json() -> String {
    r#"{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"TokenMint","outAmount":"120000","otherAmountThreshold":"114000","swapMode":"ExactIn","slippageBps":500,"platformFee":{"amount":"240","feeBps":20},"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#.to_string()
}

fn pump_quote_json(is_completed: bool) -> String {
    format!(
        r#"{{"quote":{{"mint":"TokenMint","bondingCurve":"BondingCurve","type":"BUY","inAmount":"10000000","inTokenAddress":"So11111111111111111111111111111111111111112","outAmount":"123456","outTokenAddress":"TokenMint","meta":{{"isCompleted":{is_completed},"outDecimals":6,"inDecimals":9}}}}}}"#
    )
}

fn unique_provider_selector_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-provider-selector-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
