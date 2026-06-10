use super::*;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn migrated_pumpswap_uses_direct_builder_before_pump_fun_endpoints() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let token = "FNVhryGP7Epjbr9iWLv75ABCYzY3au4icYHNdExn9jZ3";
    let pool = "FhmcZfBdmvaYdphQ4qcd8qjs6wUv1Viic4UkiWqiZxxd";
    let server = tokio::spawn(async move {
        let pool_accounts = read_http_request(&listener).await;
        assert!(pool_accounts.contains("\"method\":\"getMultipleAccounts\""));
        assert!(pool_accounts.contains(pool));
        write_http_json(
            pool_accounts.into_socket,
            &format!(
                r#"{{"jsonrpc":"2.0","id":"execution-pumpswap-direct-get-multiple-accounts","result":{{"value":[{},{}]}}}}"#,
                rpc_account_json(
                    &pumpswap_global_config_data(),
                    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
                ),
                rpc_account_json(
                    &pumpswap_pool_data(token),
                    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
                )
            ),
        )
        .await;

        let mint_accounts = read_http_request(&listener).await;
        assert!(mint_accounts.contains("\"method\":\"getMultipleAccounts\""));
        assert!(mint_accounts.contains(token));
        write_http_json(
            mint_accounts.into_socket,
            &format!(
                r#"{{"jsonrpc":"2.0","id":"execution-pumpswap-direct-get-multiple-accounts","result":{{"value":[{},{}]}}}}"#,
                rpc_account_json(&[], "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
                rpc_account_json(&[], "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
            ),
        )
        .await;

        let blockhash = read_http_request(&listener).await;
        assert!(
            blockhash.contains("getLatestBlockhash"),
            "{}",
            blockhash.body
        );
        write_http_json(
            blockhash.into_socket,
            r#"{"jsonrpc":"2.0","id":"execution-pumpswap-direct-latest-blockhash","result":{"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1}}}"#,
        )
        .await;

        let simulation = read_http_request(&listener).await;
        assert!(simulation.contains("\"method\":\"simulateTransaction\""));
        assert!(simulation.contains("\"sigVerify\":false"));
        write_http_json(
            simulation.into_socket,
            r#"{"jsonrpc":"2.0","id":"execution-swap-transaction-simulate","result":{"value":{"err":null,"logs":[]}}}"#,
        )
        .await;
    });
    let mut config = pump_fun_direct_config(&base_url);
    config.canary_tiny_submit_enabled = true;
    config.submit_adapter_http_url = base_url.clone();
    config.quote_canary_pump_fun_parallel_enabled = false;
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let request = generic_migrated_pumpswap_request(&config, token, pool);
    let plan = adapter.build_transaction_plan(&request)?;

    let result = adapter.simulate_transaction_plan(&plan).await?;
    server.await?;
    let payload = plan
        .serialized_transaction_payload_slot
        .as_ref()
        .expect("serialized payload slot")
        .load()?
        .expect("serialized payload");

    assert_eq!(
        result.status,
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED
    );
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("pumpswap_direct_buy_transaction_ok"));
    assert_eq!(payload.source, "pumpswap_direct");
    Ok(())
}

#[tokio::test]
async fn migrated_pumpswap_sell_uses_direct_builder_before_generic_swap() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let token = "FNVhryGP7Epjbr9iWLv75ABCYzY3au4icYHNdExn9jZ3";
    let pool = "FhmcZfBdmvaYdphQ4qcd8qjs6wUv1Viic4UkiWqiZxxd";
    let server = tokio::spawn(async move {
        let pool_accounts = read_http_request(&listener).await;
        assert!(pool_accounts.contains("\"method\":\"getMultipleAccounts\""));
        assert!(pool_accounts.contains(pool));
        write_http_json(
            pool_accounts.into_socket,
            &format!(
                r#"{{"jsonrpc":"2.0","id":"execution-pumpswap-direct-get-multiple-accounts","result":{{"value":[{},{}]}}}}"#,
                rpc_account_json(
                    &pumpswap_global_config_data(),
                    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
                ),
                rpc_account_json(
                    &pumpswap_pool_data(token),
                    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
                )
            ),
        )
        .await;

        let mint_accounts = read_http_request(&listener).await;
        assert!(mint_accounts.contains("\"method\":\"getMultipleAccounts\""));
        assert!(mint_accounts.contains(token));
        write_http_json(
            mint_accounts.into_socket,
            &format!(
                r#"{{"jsonrpc":"2.0","id":"execution-pumpswap-direct-get-multiple-accounts","result":{{"value":[{},{}]}}}}"#,
                rpc_account_json(&[], "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
                rpc_account_json(&[], "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
            ),
        )
        .await;

        let blockhash = read_http_request(&listener).await;
        assert!(
            blockhash.contains("getLatestBlockhash"),
            "{}",
            blockhash.body
        );
        write_http_json(
            blockhash.into_socket,
            r#"{"jsonrpc":"2.0","id":"execution-pumpswap-direct-latest-blockhash","result":{"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1}}}"#,
        )
        .await;

        let simulation = read_http_request(&listener).await;
        assert!(simulation.contains("\"method\":\"simulateTransaction\""));
        assert!(simulation.contains("\"sigVerify\":false"));
        write_http_json(
            simulation.into_socket,
            r#"{"jsonrpc":"2.0","id":"execution-swap-transaction-simulate","result":{"value":{"err":null,"logs":[]}}}"#,
        )
        .await;
    });
    let mut config = pump_fun_direct_config(&base_url);
    config.canary_tiny_submit_enabled = true;
    config.submit_adapter_http_url = base_url.clone();
    config.quote_canary_pump_fun_parallel_enabled = false;
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let request = generic_migrated_pumpswap_sell_request(&config, token, pool);
    let plan = adapter.build_transaction_plan(&request)?;

    let result = adapter.simulate_transaction_plan(&plan).await?;
    server.await?;
    let payload = plan
        .serialized_transaction_payload_slot
        .as_ref()
        .expect("serialized payload slot")
        .load()?
        .expect("serialized payload");
    let raw = BASE64_STANDARD.decode(&payload.serialized_transaction_base64)?;

    assert_eq!(
        result.status,
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED
    );
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("pumpswap_direct_sell_transaction_ok"));
    assert!(contains_bytes(&raw, &[198, 46, 21, 82, 180, 217, 232, 112]));
    assert_eq!(payload.source, "pumpswap_direct");
    Ok(())
}

#[tokio::test]
async fn pump_fun_paid_sell_bonding_curve_miss_falls_back_to_pumpswap_direct() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let token = "FNVhryGP7Epjbr9iWLv75ABCYzY3au4icYHNdExn9jZ3";
    let pool = "FhmcZfBdmvaYdphQ4qcd8qjs6wUv1Viic4UkiWqiZxxd";
    let server = tokio::spawn(async move {
        let pump_instructions = read_http_request(&listener).await;
        assert!(pump_instructions.starts_with("POST /pump-fun/swap-instructions "));
        assert!(pump_instructions.contains("\"type\":\"SELL\""));
        assert!(pump_instructions.contains(token));
        assert!(pump_instructions.contains("\"inAmount\":\"123456\""));
        write_http_json(
            pump_instructions.into_socket,
            r#"{"instructions":[{"keys":[],"programId":"ComputeBudget111111111111111111111111111111","data":[2]}]}"#,
        )
        .await;

        let pump_swap = read_http_request(&listener).await;
        assert!(pump_swap.starts_with("POST /pump-fun/swap "));
        write_http_status(
            pump_swap.into_socket,
            400,
            r#"{"status":"error","message":"Bonding curve account not found: migrated"}"#,
        )
        .await;

        let quote = read_http_request(&listener).await;
        assert!(quote.starts_with("GET /quote?"));
        assert!(quote.contains(token));
        write_http_json(quote.into_socket, &generic_sell_quote_json(token, pool)).await;

        let pool_accounts = read_http_request(&listener).await;
        assert!(pool_accounts.contains("\"method\":\"getMultipleAccounts\""));
        assert!(pool_accounts.contains(pool));
        write_http_json(
            pool_accounts.into_socket,
            &format!(
                r#"{{"jsonrpc":"2.0","id":"execution-pumpswap-direct-get-multiple-accounts","result":{{"value":[{},{}]}}}}"#,
                rpc_account_json(
                    &pumpswap_global_config_data(),
                    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
                ),
                rpc_account_json(
                    &pumpswap_pool_data(token),
                    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
                )
            ),
        )
        .await;

        let mint_accounts = read_http_request(&listener).await;
        assert!(mint_accounts.contains("\"method\":\"getMultipleAccounts\""));
        write_http_json(
            mint_accounts.into_socket,
            &format!(
                r#"{{"jsonrpc":"2.0","id":"execution-pumpswap-direct-get-multiple-accounts","result":{{"value":[{},{}]}}}}"#,
                rpc_account_json(&[], "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
                rpc_account_json(&[], "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
            ),
        )
        .await;

        let blockhash = read_http_request(&listener).await;
        assert!(blockhash.contains("getLatestBlockhash"));
        write_http_json(
            blockhash.into_socket,
            r#"{"jsonrpc":"2.0","id":"execution-pumpswap-direct-latest-blockhash","result":{"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1}}}"#,
        )
        .await;

        let simulation = read_http_request(&listener).await;
        assert!(simulation.contains("\"method\":\"simulateTransaction\""));
        write_http_json(
            simulation.into_socket,
            r#"{"jsonrpc":"2.0","id":"execution-swap-transaction-simulate","result":{"value":{"err":null,"logs":[]}}}"#,
        )
        .await;
    });
    let mut config = pump_fun_direct_config(&base_url);
    config.canary_tiny_submit_enabled = true;
    config.submit_adapter_http_url = base_url.clone();
    config.quote_canary_pump_fun_parallel_enabled = true;
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let request = pump_fun_paid_migrated_pumpswap_sell_request(&config, token);
    let plan = adapter.build_transaction_plan(&request)?;

    let result = adapter.simulate_transaction_plan(&plan).await?;
    server.await?;
    let payload = plan
        .serialized_transaction_payload_slot
        .as_ref()
        .expect("serialized payload slot")
        .load()?
        .expect("serialized payload");
    let proof = result.error.as_deref().unwrap_or_default();

    assert!(proof.contains("pump_fun_direct_swap_soft_failed"));
    assert!(proof.contains("pumpswap_direct_sell_transaction_ok"));
    assert!(proof.contains("quote_age_ms_at_build="));
    assert_eq!(payload.source, "pumpswap_direct");
    Ok(())
}

#[test]
fn pumpswap_custom_errors_are_named_in_diagnostics() {
    let text = r#"err={"InstructionError":[6,{"Custom":6004}]} fallback={"Custom":6001}"#;
    let annotated = crate::execution_pumpswap_error::annotate_pumpswap_custom_errors(text);

    assert!(annotated.contains("6004:ExceededSlippage"));
    assert!(annotated.contains("6001:ZeroBaseAmount"));
}

#[tokio::test]
async fn migrated_pumpswap_direct_failure_is_kept_when_generic_build_fails() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let token = "FNVhryGP7Epjbr9iWLv75ABCYzY3au4icYHNdExn9jZ3";
    let pool = "FhmcZfBdmvaYdphQ4qcd8qjs6wUv1Viic4UkiWqiZxxd";
    let server = tokio::spawn(async move {
        let direct_accounts = read_http_request(&listener).await;
        assert!(direct_accounts.contains("\"method\":\"getMultipleAccounts\""));
        assert!(direct_accounts.contains(pool));
        write_http_json(
            direct_accounts.into_socket,
            r#"{"jsonrpc":"2.0","id":"execution-pumpswap-direct-get-multiple-accounts","error":{"message":"direct account read failed"}}"#,
        )
        .await;

        let instructions = read_http_request(&listener).await;
        assert!(instructions.starts_with("POST /swap-instructions "));
        write_http_json(
            instructions.into_socket,
            r#"{"swapInstruction":{},"computeBudgetInstructions":[],"setupInstructions":[],"otherInstructions":[],"addressLookupTableAddresses":[],"cleanupInstruction":null}"#,
        )
        .await;

        let swap = read_http_request(&listener).await;
        assert!(swap.starts_with("POST /swap "));
        write_http_json(
            swap.into_socket,
            r#"{"error":"generic metis build failed"}"#,
        )
        .await;
    });
    let mut config = pump_fun_direct_config(&base_url);
    config.canary_tiny_submit_enabled = true;
    config.submit_adapter_http_url = base_url.clone();
    config.quote_canary_pump_fun_parallel_enabled = false;
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let request = generic_migrated_pumpswap_request(&config, token, pool);
    let plan = adapter.build_transaction_plan(&request)?;

    let error = adapter
        .simulate_transaction_plan(&plan)
        .await
        .expect_err("PumpSwap direct and generic build failure should be visible");
    server.await?;
    let error = error.to_string();

    assert!(error.contains("PumpSwap direct build failed"));
    assert!(error.contains("direct account read failed"));
    assert!(error.contains("generic Metis swap failed"));
    assert!(error.contains("generic metis build failed"));
    Ok(())
}

#[tokio::test]
async fn generic_pump_fun_amm_route_uses_direct_pump_fun_swap_builder() -> Result<()> {
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
    let mut config = pump_fun_direct_config(&base_url);
    config.quote_canary_pump_fun_parallel_enabled = true;
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let request = generic_pump_fun_amm_request(&config);
    let plan = adapter.build_transaction_plan(&request)?;

    let result = adapter.simulate_transaction_plan(&plan).await?;
    server.await?;
    let payload = plan
        .serialized_transaction_payload_slot
        .as_ref()
        .expect("serialized payload slot")
        .load()?
        .expect("serialized payload");

    assert_eq!(
        result.status,
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED
    );
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("pump_fun_swap_transaction_ok"));
    assert_eq!(payload.source, "pump_fun_paid");
    Ok(())
}

#[tokio::test]
async fn pump_fun_swap_transaction_rpc_simulation_failure_blocks_payload() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let swap = read_http_request(&listener).await;
        assert!(swap.starts_with("POST /pump-fun/swap "));
        write_http_json(swap.into_socket, r#"{"tx":"AQIDBA=="}"#).await;

        let rpc = read_http_request(&listener).await;
        assert!(rpc.starts_with("POST / "));
        assert!(rpc.contains("\"method\":\"simulateTransaction\""));
        write_http_json(
            rpc.into_socket,
            r#"{"jsonrpc":"2.0","id":"execution-swap-transaction-simulate","result":{"value":{"err":{"InstructionError":[6,"MissingAccount"]},"logs":["Program pAMM failed: missing account"]}}}"#,
        )
        .await;
    });
    let mut config = pump_fun_direct_config(&base_url);
    config.canary_tiny_submit_enabled = true;
    config.submit_adapter_http_url = base_url;
    let request = generic_pump_fun_amm_request(&config);
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let plan = adapter.build_transaction_plan(&request)?;

    let error =
        crate::execution_pump_fun_swap_transaction_http::fetch_pump_fun_swap_transaction_dry_run(
            &reqwest::Client::new(),
            &config,
            &plan,
        )
        .await
        .expect_err("RPC simulation failure should block Pump.fun tx payload");
    server.await?;

    let error = error.to_string();
    assert!(error.contains("swap transaction RPC simulation failed"));
    assert!(error.contains("pump_fun_paid"));
    assert!(error.contains("MissingAccount"));
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
    write_http_status(socket, 200, body).await;
}

async fn write_http_status(socket: tokio::net::TcpStream, status: u16, body: &str) {
    let response = format!(
        "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let mut socket = socket;
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write response");
}

fn generic_migrated_pumpswap_request(
    config: &ExecutionConfig,
    token: &str,
    pool: &str,
) -> crate::execution_submit_adapter::ExecutionSubmitRequest {
    let route_plan = format!(r#"[{{"swapInfo":{{"ammKey":"{pool}","label":"Pump.fun Amm"}}}}]"#);
    crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: "order-migrated-pumpswap".to_string(),
        signal_id: "signal-migrated-pumpswap".to_string(),
        client_order_id: "client-migrated-pumpswap".to_string(),
        attempt: 1,
        route: config.canary_route.clone(),
        wallet_id: "leader-wallet".to_string(),
        token: token.to_string(),
        side: "buy".to_string(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: config.canary_wallet_pubkey.clone(),
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
            quote_source: Some(
                crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS.to_string(),
            ),
            quote_event_id: Some("quote:entry:migrated-pumpswap".to_string()),
            quote_request_ts: None,
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_response_json: Some(format!(
                r#"{{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"{token}","outAmount":"123456","otherAmountThreshold":"117283","swapMode":"ExactIn","slippageBps":500,"routePlan":{route_plan}}}"#
            )),
            quote_price_sol: Some(0.081),
            price_impact_pct: Some(0.01),
            route_plan_json: Some(route_plan),
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

fn generic_migrated_pumpswap_sell_request(
    config: &ExecutionConfig,
    token: &str,
    pool: &str,
) -> crate::execution_submit_adapter::ExecutionSubmitRequest {
    let route_plan = format!(r#"[{{"swapInfo":{{"ammKey":"{pool}","label":"Pump.fun Amm"}}}}]"#);
    crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: "order-migrated-pumpswap-sell".to_string(),
        signal_id: "signal-migrated-pumpswap-sell".to_string(),
        client_order_id: "client-migrated-pumpswap-sell".to_string(),
        attempt: 1,
        route: config.canary_route.clone(),
        wallet_id: "leader-wallet".to_string(),
        token: token.to_string(),
        side: "sell".to_string(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: config.canary_wallet_pubkey.clone(),
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
            quote_source: Some(
                crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS.to_string(),
            ),
            quote_event_id: Some("quote:exit:migrated-pumpswap".to_string()),
            quote_request_ts: None,
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some("123456".to_string()),
            quote_out_amount_raw: Some("10000".to_string()),
            quote_response_json: Some(format!(
                r#"{{"inputMint":"{token}","inAmount":"123456","outputMint":"So11111111111111111111111111111111111111112","outAmount":"10000","otherAmountThreshold":"9500","swapMode":"ExactIn","slippageBps":500,"routePlan":{route_plan}}}"#
            )),
            quote_price_sol: Some(0.081),
            price_impact_pct: Some(0.01),
            route_plan_json: Some(route_plan),
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

fn pump_fun_paid_migrated_pumpswap_sell_request(
    config: &ExecutionConfig,
    token: &str,
) -> crate::execution_submit_adapter::ExecutionSubmitRequest {
    crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: "order-pump-fun-paid-migrated-pumpswap-sell".to_string(),
        signal_id: "signal-pump-fun-paid-migrated-pumpswap-sell".to_string(),
        client_order_id: "client-pump-fun-paid-migrated-pumpswap-sell".to_string(),
        attempt: 1,
        route: config.canary_route.clone(),
        wallet_id: "leader-wallet".to_string(),
        token: token.to_string(),
        side: "sell".to_string(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: config.canary_wallet_pubkey.clone(),
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
            quote_source: Some(
                crate::execution_quote_provider_selection::QUOTE_SOURCE_PUMP_FUN_PAID.to_string(),
            ),
            quote_event_id: Some("quote:exit:pump-fun-paid-migrated".to_string()),
            quote_request_ts: None,
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some("123456".to_string()),
            quote_out_amount_raw: Some("10000".to_string()),
            quote_response_json: Some(format!(
                r#"{{"quote":{{"mint":"{token}","type":"SELL","inAmount":"123456","inTokenAddress":"{token}","outAmount":"10000","outTokenAddress":"So11111111111111111111111111111111111111112","meta":{{"isCompleted":false,"inDecimals":6,"outDecimals":9}}}}}}"#
            )),
            quote_price_sol: Some(0.081),
            price_impact_pct: Some(0.01),
            route_plan_json: Some(r#"[{"swapInfo":{"label":"Pump.fun Paid"}}]"#.to_string()),
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

fn generic_sell_quote_json(token: &str, pool: &str) -> String {
    format!(
        r#"{{"inputMint":"{token}","inAmount":"123456","outputMint":"So11111111111111111111111111111111111111112","outAmount":"10000","otherAmountThreshold":"9500","swapMode":"ExactIn","slippageBps":500,"priceImpactPct":"0.01","routePlan":[{{"swapInfo":{{"ammKey":"{pool}","label":"Pump.fun Amm"}}}}]}}"#
    )
}

fn rpc_account_json(data: &[u8], owner: &str) -> String {
    format!(
        r#"{{"data":["{}","base64"],"executable":false,"lamports":1,"owner":"{owner}","rentEpoch":0,"space":{}}}"#,
        BASE64_STANDARD.encode(data),
        data.len()
    )
}

fn pumpswap_global_config_data() -> Vec<u8> {
    let recipient = pubkey_bytes("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM");
    let mut data = vec![149, 8, 156, 202, 160, 252, 176, 217];
    data.extend_from_slice(&recipient);
    data.extend_from_slice(&20_u64.to_le_bytes());
    data.extend_from_slice(&5_u64.to_le_bytes());
    data.push(0);
    for _ in 0..8 {
        data.extend_from_slice(&recipient);
    }
    data.extend_from_slice(&5_u64.to_le_bytes());
    data.extend_from_slice(&recipient);
    data.extend_from_slice(&recipient);
    data.extend_from_slice(&recipient);
    data.push(0);
    for _ in 0..7 {
        data.extend_from_slice(&recipient);
    }
    data.push(0);
    for _ in 0..8 {
        data.extend_from_slice(&recipient);
    }
    data.extend_from_slice(&0_u64.to_le_bytes());
    data
}

fn pumpswap_pool_data(token: &str) -> Vec<u8> {
    let creator = pubkey_bytes("11111111111111111111111111111111");
    let quote_mint = pubkey_bytes(token);
    let lp_mint = pubkey_bytes("pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn");
    let pool_base = pubkey_bytes("8WjPNmbxm4rPDLgeTf8GuK26gqtb23txhGb3pwaFSKJT");
    let pool_quote = pubkey_bytes("3B2QRQdRn79FK7MyYZ57yGW3ieK2cS5q13S8Kjjorn7x");
    let coin_creator = pubkey_bytes("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM");
    let mut data = vec![241, 154, 109, 4, 17, 177, 109, 188];
    data.push(255);
    data.extend_from_slice(&0_u16.to_le_bytes());
    data.extend_from_slice(&creator);
    data.extend_from_slice(&pubkey_bytes("So11111111111111111111111111111111111111112"));
    data.extend_from_slice(&quote_mint);
    data.extend_from_slice(&lp_mint);
    data.extend_from_slice(&pool_base);
    data.extend_from_slice(&pool_quote);
    data.extend_from_slice(&1_000_000_u64.to_le_bytes());
    data.extend_from_slice(&coin_creator);
    data.push(0);
    data.push(0);
    data.resize(300, 0);
    data
}

fn pubkey_bytes(value: &str) -> [u8; 32] {
    crate::execution_pumpswap_accounts::parse_pubkey(value, "test pubkey")
        .expect("valid test pubkey")
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn pump_fun_direct_config(base_url: &str) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_wallet_pubkey = "11111111111111111111111111111111".to_string();
    config.quote_canary_base_url = base_url.to_string();
    config.quote_canary_timeout_ms = 1_000;
    config.quote_canary_buy_slippage_bps = 500;
    config.swap_instructions_dry_run_enabled = true;
    config.swap_transaction_dry_run_enabled = true;
    config
}

fn generic_pump_fun_amm_request(
    config: &ExecutionConfig,
) -> crate::execution_submit_adapter::ExecutionSubmitRequest {
    crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: "order-generic-pump-fun-amm".to_string(),
        signal_id: "signal-generic-pump-fun-amm".to_string(),
        client_order_id: "client-generic-pump-fun-amm".to_string(),
        attempt: 1,
        route: config.canary_route.clone(),
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMint".to_string(),
        side: "buy".to_string(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: config.canary_wallet_pubkey.clone(),
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
            quote_source: Some(
                crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS.to_string(),
            ),
            quote_event_id: Some("quote:entry:generic-pump-fun-amm".to_string()),
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
