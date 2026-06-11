use super::*;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn metis_market_not_found_does_not_fallback_to_public_instructions_builder() -> Result<()> {
    let primary_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let primary_url = format!("http://{}", primary_listener.local_addr()?);
    let primary_server = tokio::spawn(async move {
        let (mut socket, _) = primary_listener.accept().await.expect("primary request");
        let mut buffer = [0_u8; 4096];
        let read = socket
            .read(&mut buffer)
            .await
            .expect("read primary request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /swap-instructions "));
        write_alternate_builder_status(
            &mut socket,
            400,
            r#"{"error":"Market HFHxWF8bXVJ5DbysUfmNM7mHrS969f1Z5sgD1R1Wp55D not found","errorCode":"MARKET_NOT_FOUND"}"#,
        )
        .await;
    });
    let mut config = alternate_builder_config(primary_url);
    config.quote_canary_public_parallel_enabled = true;
    config.quote_canary_public_base_url = "http://127.0.0.1:9".to_string();
    let plan = alternate_builder_plan(
        &config,
        crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS,
    )?;

    let error = crate::execution_swap_instructions_http::fetch_swap_instructions_dry_run(
        &reqwest::Client::new(),
        &config,
        &plan,
    )
    .await
    .expect_err("paid Metis builder error should stay visible");
    primary_server.await?;

    assert!(error.to_string().contains("Market"));
    Ok(())
}

#[tokio::test]
async fn generic_public_metadata_uses_paid_metis_transaction_builder() -> Result<()> {
    let metis_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let metis_url = format!("http://{}", metis_listener.local_addr()?);
    let metis_server = tokio::spawn(async move {
        let (mut socket, _) = metis_listener.accept().await.expect("metis request");
        let mut buffer = [0_u8; 4096];
        let read = socket.read(&mut buffer).await.expect("read metis request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /swap "));
        assert!(request.contains("x-api-key"));
        write_alternate_builder_json(&mut socket, valid_alternate_transaction_json()).await;
    });
    let mut config = alternate_builder_config(metis_url);
    config.quote_canary_api_key = "metis-key".to_string();
    config.quote_canary_public_parallel_enabled = true;
    config.quote_canary_public_base_url = "http://127.0.0.1:9".to_string();
    let plan = alternate_builder_plan(
        &config,
        crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_PUBLIC,
    )?;

    let proof = crate::execution_swap_transaction_http::fetch_swap_transaction_dry_run(
        &reqwest::Client::new(),
        &config,
        &plan,
    )
    .await?
    .expect("proof should exist");
    metis_server.await?;

    assert_eq!(proof.source, "metis_no_shared_accounts");
    assert!(proof
        .summary
        .contains("metis_swap_transaction_no_shared_accounts_ok"));
    assert!(proof.summary.contains("skip_user_accounts_rpc_calls=false"));
    Ok(())
}

#[tokio::test]
async fn generic_public_metadata_uses_paid_metis_instructions_builder() -> Result<()> {
    let metis_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let metis_url = format!("http://{}", metis_listener.local_addr()?);
    let metis_server = tokio::spawn(async move {
        let (mut socket, _) = metis_listener.accept().await.expect("metis request");
        let mut buffer = [0_u8; 4096];
        let read = socket.read(&mut buffer).await.expect("read metis request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /swap-instructions "));
        assert!(request.contains("x-api-key"));
        write_alternate_builder_json(&mut socket, valid_alternate_instructions_json()).await;
    });
    let mut config = alternate_builder_config(metis_url);
    config.quote_canary_api_key = "metis-key".to_string();
    config.quote_canary_public_parallel_enabled = true;
    config.quote_canary_public_base_url = "http://127.0.0.1:9".to_string();
    let plan = alternate_builder_plan(
        &config,
        crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_PUBLIC,
    )?;

    let proof = crate::execution_swap_instructions_http::fetch_swap_instructions_dry_run(
        &reqwest::Client::new(),
        &config,
        &plan,
    )
    .await?
    .expect("proof should exist");
    metis_server.await?;

    assert!(proof.contains("metis_swap_instructions_no_shared_accounts_ok"));
    Ok(())
}

#[tokio::test]
async fn pump_fun_amm_missing_account_uses_static_cu_transaction_fallback() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        for attempt in 0..4 {
            let (mut socket, _) = listener.accept().await.expect("metis request");
            let read = socket.read(&mut buffer).await.expect("read metis request");
            let request = String::from_utf8_lossy(&buffer[..read]);
            assert!(request.starts_with("POST /swap "));
            match attempt {
                0 => assert_no_shared_builder_request(&request),
                1 => assert_no_shared_builder_request(&request),
                2 => assert_no_shared_skip_user_accounts_request(&request),
                _ => assert_no_shared_skip_user_accounts_static_cu_request(&request),
            }
            if attempt < 3 {
                write_alternate_builder_json(&mut socket, missing_account_transaction_json()).await;
            } else {
                write_alternate_builder_json(&mut socket, valid_alternate_transaction_json()).await;
            }
        }
    });
    let config = alternate_builder_config(base_url);
    let plan = alternate_builder_plan(
        &config,
        crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS,
    )?;

    let proof = crate::execution_swap_transaction_http::fetch_swap_transaction_dry_run(
        &reqwest::Client::new(),
        &config,
        &plan,
    )
    .await?
    .expect("static CU proof should exist");
    server.await?;

    assert_eq!(
        proof.source,
        "metis_no_shared_accounts_skip_user_accounts_static_cu"
    );
    assert!(proof.summary.contains("dynamic_compute_unit_limit=false"));
    assert!(proof.summary.contains("simulation_error=none"));
    Ok(())
}

#[tokio::test]
async fn pump_fun_amm_missing_account_keeps_generic_simulation_failure_without_paid_fallback(
) -> Result<()> {
    let metis_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let metis_url = format!("http://{}", metis_listener.local_addr()?);
    let metis_server = tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        for attempt in 0..4 {
            let (mut socket, _) = metis_listener.accept().await.expect("metis request");
            let read = socket.read(&mut buffer).await.expect("read metis request");
            let request = String::from_utf8_lossy(&buffer[..read]);
            assert!(request.starts_with("POST /swap "));
            match attempt {
                0 => assert_no_shared_builder_request(&request),
                1 => assert_no_shared_builder_request(&request),
                2 => assert_no_shared_skip_user_accounts_request(&request),
                _ => assert_no_shared_skip_user_accounts_static_cu_request(&request),
            }
            write_alternate_builder_json(&mut socket, missing_account_transaction_json()).await;
        }
    });
    let mut config = alternate_builder_config(metis_url);
    config.quote_canary_public_parallel_enabled = true;
    config.quote_canary_public_base_url = "http://127.0.0.1:9".to_string();
    config.quote_canary_pump_fun_parallel_enabled = true;
    let plan = alternate_builder_plan(
        &config,
        crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS,
    )?;

    let error = crate::execution_swap_transaction_http::fetch_swap_transaction_dry_run(
        &reqwest::Client::new(),
        &config,
        &plan,
    )
    .await
    .expect_err("generic missing-account simulation error should stay visible");
    metis_server.await?;

    let error = error.to_string();
    assert!(error.contains("swap transaction dry-run simulation error"));
    assert!(error.contains("no_shared_accounts_ok"));
    assert!(error.contains("skip_user_accounts_rpc_calls=true"));
    assert!(error.contains("dynamic_compute_unit_limit=false"));
    Ok(())
}

#[tokio::test]
async fn pump_fun_amm_no_route_keeps_generic_no_route_failure() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        let (mut socket, _) = listener.accept().await.expect("generic request");
        let read = socket
            .read(&mut buffer)
            .await
            .expect("read generic request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /swap "));
        write_alternate_builder_status(
            &mut socket,
            400,
            r#"{"error":"No routes found","errorCode":"NO_ROUTES_FOUND"}"#,
        )
        .await;
    });
    let mut config = alternate_builder_config(base_url);
    config.quote_canary_pump_fun_parallel_enabled = true;
    let plan = alternate_builder_plan(
        &config,
        crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS,
    )?;

    let error = crate::execution_swap_transaction_http::fetch_swap_transaction_dry_run(
        &reqwest::Client::new(),
        &config,
        &plan,
    )
    .await
    .expect_err("generic no-route should stay visible");
    server.await?;

    assert!(error.to_string().contains("No routes found"));
    Ok(())
}

#[tokio::test]
async fn tiny_submit_enabled_rejects_swap_transaction_when_rpc_simulation_fails() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut swap_socket, _) = listener.accept().await.expect("swap request");
        let mut buffer = [0_u8; 4096];
        let read = swap_socket
            .read(&mut buffer)
            .await
            .expect("read swap request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST /swap "));
        assert_no_shared_builder_request(&request);
        write_alternate_builder_json(&mut swap_socket, valid_alternate_transaction_json()).await;

        let (mut rpc_socket, _) = listener.accept().await.expect("rpc simulation request");
        let read = rpc_socket
            .read(&mut buffer)
            .await
            .expect("read rpc request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.starts_with("POST / "));
        assert!(request.contains("\"method\":\"simulateTransaction\""));
        assert!(request.contains("\"sigVerify\":false"));
        assert!(request.contains("\"replaceRecentBlockhash\":true"));
        write_alternate_builder_json(
            &mut rpc_socket,
            r#"{"jsonrpc":"2.0","id":"execution-swap-transaction-simulate","result":{"value":{"err":{"InstructionError":[6,"MissingAccount"]},"logs":["Program JUP6 failed: An account required by the instruction is missing"]}}}"#,
        )
        .await;
    });
    let mut config = alternate_builder_config(base_url.clone());
    config.canary_tiny_submit_enabled = true;
    config.submit_adapter_http_url = base_url;
    let plan = alternate_builder_plan(
        &config,
        crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS,
    )?;

    let error = crate::execution_swap_transaction_http::fetch_swap_transaction_dry_run(
        &reqwest::Client::new(),
        &config,
        &plan,
    )
    .await
    .expect_err("RPC simulation failure should block submit payload");
    server.await?;

    let error = error.to_string();
    assert!(error.contains("swap transaction RPC simulation failed"));
    assert!(error.contains("MissingAccount"));
    Ok(())
}

async fn write_alternate_builder_json(socket: &mut tokio::net::TcpStream, body: &str) {
    write_alternate_builder_status(socket, 200, body).await;
}

async fn write_alternate_builder_status(
    socket: &mut tokio::net::TcpStream,
    status: u16,
    body: &str,
) {
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

fn assert_no_shared_builder_request(request: &str) {
    assert!(request.contains("\"useSharedAccounts\":false"));
    assert!(!request.contains("\"skipUserAccountsRpcCalls\":true"));
    assert!(request.contains("\"dynamicComputeUnitLimit\":true"));
}

fn assert_no_shared_skip_user_accounts_request(request: &str) {
    assert!(request.contains("\"useSharedAccounts\":false"));
    assert!(request.contains("\"skipUserAccountsRpcCalls\":true"));
    assert!(request.contains("\"dynamicComputeUnitLimit\":true"));
}

fn assert_no_shared_skip_user_accounts_static_cu_request(request: &str) {
    assert!(request.contains("\"useSharedAccounts\":false"));
    assert!(request.contains("\"skipUserAccountsRpcCalls\":true"));
    assert!(request.contains("\"dynamicComputeUnitLimit\":false"));
}

fn alternate_builder_config(base_url: String) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_wallet_pubkey = "11111111111111111111111111111111".to_string();
    config.quote_canary_base_url = base_url;
    config.quote_canary_buy_slippage_bps = 500;
    config.quote_canary_timeout_ms = 1_000;
    config.swap_instructions_dry_run_enabled = true;
    config.swap_transaction_dry_run_enabled = true;
    config
}

fn alternate_builder_plan(
    config: &ExecutionConfig,
    quote_source: &str,
) -> Result<crate::execution_submit_adapter::ExecutionTransactionPlan> {
    let request = crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: "order-alternate-builder".to_string(),
        signal_id: "signal-alternate-builder".to_string(),
        client_order_id: "client-alternate-builder".to_string(),
        attempt: 1,
        route: config.canary_route.clone(),
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMint".to_string(),
        side: "buy".to_string(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: config.canary_wallet_pubkey.clone(),
        entry_route_plan_json: None,
        metadata: alternate_builder_metadata(quote_source),
    };
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    adapter.build_transaction_plan(&request)
}

fn alternate_builder_metadata(
    quote_source: &str,
) -> crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
    crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
        quote_source: Some(quote_source.to_string()),
        quote_event_id: Some("quote:entry:signal-alternate-builder".to_string()),
        quote_request_ts: None,
        quote_status: Some("ok".to_string()),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("123456".to_string()),
        quote_response_json: Some(alternate_quote_json().to_string()),
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
    }
}

fn alternate_quote_json() -> &'static str {
    r#"{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"TokenMint","outAmount":"123456","otherAmountThreshold":"117283","swapMode":"ExactIn","slippageBps":500,"platformFee":null,"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}],"loadedLongtailToken":true}"#
}

fn valid_alternate_instructions_json() -> &'static str {
    r#"{"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":null}"#
}

fn valid_alternate_transaction_json() -> &'static str {
    r#"{"swapTransaction":"AQIDBA==","simulationError":null}"#
}

fn missing_account_transaction_json() -> &'static str {
    r#"{"swapTransaction":"AQIDBA==","simulationError":{"error":"Error processing Instruction 5: An account required by the instruction is missing","errorCode":"TRANSACTION_ERROR"}}"#
}
