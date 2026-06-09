use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn swap_instructions_missing_account_allows_swap_transaction_proof() -> Result<()> {
    let db_path = unique_soft_swap_test_path("instructions-missing-account-transaction-ok");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let mut buffer = [0_u8; 8192];

        let (mut first, _) = listener.accept().await.expect("first instructions request");
        let read = first.read(&mut buffer).await.expect("read first request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /swap-instructions "));
        write_soft_swap_json(&mut first, missing_account_instructions_json()).await;
        drop(first);

        let (mut second, _) = listener.accept().await.expect("retry instructions request");
        let read = second.read(&mut buffer).await.expect("read retry request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /swap-instructions "));
        assert!(request.contains("\"useSharedAccounts\":false"));
        write_soft_swap_json(&mut second, missing_account_instructions_json()).await;
        drop(second);

        let (mut third, _) = listener.accept().await.expect("swap transaction request");
        let read = third.read(&mut buffer).await.expect("read swap request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /swap "));
        write_soft_swap_json(
            &mut third,
            r#"{"swapTransaction":"AQIDBA==","simulationError":null}"#,
        )
        .await;
    });
    let now = Utc::now();
    let signal = soft_swap_signal(now);
    store.insert_copy_signal(&signal)?;
    record_soft_swap_quote(&store, &signal, now)?;
    let config = soft_swap_config(base_url);
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
    assert_eq!(summary.failed, 0);
    assert_eq!(
        order.simulation_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED)
    );
    let proof = order.simulation_error.as_deref().unwrap_or_default();
    assert!(proof.contains("metis_swap_instructions_missing_account_soft_failed"));
    assert!(proof.contains("metis_swap_transaction_ok"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn selected_public_quote_uses_public_builder() -> Result<()> {
    let db_path = unique_soft_swap_test_path("selected-public-builder");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let public_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let mut buffer = [0_u8; 8192];

        let (mut first, _) = listener
            .accept()
            .await
            .expect("public instructions request");
        let read = first
            .read(&mut buffer)
            .await
            .expect("read public instructions");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /swap-instructions "));
        assert!(!request.contains("x-api-key"));
        write_soft_swap_json(&mut first, valid_soft_swap_instructions_json()).await;
        drop(first);

        let (mut second, _) = listener.accept().await.expect("public swap request");
        let read = second.read(&mut buffer).await.expect("read public swap");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /swap "));
        assert!(!request.contains("x-api-key"));
        write_soft_swap_json(&mut second, valid_soft_swap_transaction_json()).await;
    });
    let now = Utc::now();
    let signal = soft_swap_signal(now);
    store.insert_copy_signal(&signal)?;
    record_soft_swap_quote(&store, &signal, now)?;
    record_soft_swap_public_provider_sample(&store, &signal, now)?;
    let mut config = soft_swap_config("http://127.0.0.1:9".to_string());
    config.quote_canary_public_parallel_enabled = true;
    config.quote_canary_public_base_url = public_url;
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
    assert_eq!(summary.failed, 0);
    let proof = order.simulation_error.as_deref().unwrap_or_default();
    assert!(proof.contains("metis_swap_instructions_public_selected_ok"));
    assert!(proof.contains("metis_swap_transaction_public_selected_ok"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

async fn write_soft_swap_json(socket: &mut tokio::net::TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write response");
}

fn missing_account_instructions_json() -> &'static str {
    r#"{"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":{"error":"Error processing Instruction 5: An account required by the instruction is missing","errorCode":"TRANSACTION_ERROR"}}"#
}

fn valid_soft_swap_instructions_json() -> &'static str {
    r#"{"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":null}"#
}

fn valid_soft_swap_transaction_json() -> &'static str {
    r#"{"swapTransaction":"AQIDBA==","simulationError":null}"#
}

fn fresh_public_quote_json() -> &'static str {
    r#"{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"TokenMint","outAmount":"123456","otherAmountThreshold":"117283","swapMode":"ExactIn","slippageBps":500,"platformFee":null,"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}],"loadedLongtailToken":true}"#
}

fn soft_swap_config(base_url: String) -> ExecutionConfig {
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
    config.swap_transaction_dry_run_enabled = true;
    config
}

fn soft_swap_signal(ts: chrono::DateTime<Utc>) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: "shadow:sig-soft-swap:leader-wallet:buy:TokenMint".to_string(),
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

fn record_soft_swap_quote(
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

fn record_soft_swap_public_provider_sample(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
) -> Result<()> {
    store.record_execution_quote_canary_provider_sample(
        &copybot_storage_core::ExecutionQuoteCanaryProviderSampleInsert {
            event_id: format!("quote:entry:{}", signal.signal_id),
            provider: copybot_storage_core::PROVIDER_GENERIC_PUBLIC.to_string(),
            side: "buy".to_string(),
            quote_status: "ok".to_string(),
            request_ts: now,
            quote_latency_ms: Some(9),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_response_json: Some(fresh_public_quote_json().to_string()),
            quote_price_sol: Some(0.081),
            shadow_price_sol: Some(0.08),
            slippage_bps: Some(125.0),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: None,
        },
    )?;
    Ok(())
}

fn unique_soft_swap_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-soft-swap-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
