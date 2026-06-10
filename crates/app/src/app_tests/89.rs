use super::*;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let mut socket = socket;
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write response");
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
