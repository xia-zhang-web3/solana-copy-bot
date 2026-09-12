use super::execution_build_plan_refresh_contract::{pump_fun_metadata, write_http_status_json};
use super::execution_state_machine_tiny_submit_route::tiny_route_signal;
use crate::execution_build_plan_refresh::refresh_tiny_buy_build_plan_metadata;
use crate::execution_submit_adapter::ExecutionBuildPlanMetadata;
use anyhow::Result;
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;

pub(super) fn metadata(input: u64, output: u64, slippage: f64) -> ExecutionBuildPlanMetadata {
    let mut m = pump_fun_metadata();
    m.quote_source = Some("generic_metis".into());
    m.quote_in_amount_raw = Some(input.to_string());
    m.quote_out_amount_raw = Some(output.to_string());
    // Token decimals = 0; SOL decimals = 9. Price is coherent with raw amounts.
    m.quote_price_sol = Some(input as f64 / 1_000_000_000.0 / output as f64);
    m.slippage_bps = Some(slippage);
    m.quote_response_json = Some(json!({"meta":{"outDecimals":0}}).to_string());
    m.route_plan_json = Some(json!([{"swapInfo":{"label":"Pump.fun Amm"}}]).to_string());
    m
}

pub(super) fn quote(input: &str, output: &str) -> Value {
    json!({"inputMint":"So11111111111111111111111111111111111111112",
        "outputMint":"TokenMint", "inAmount":input,"outAmount":output,
        "otherAmountThreshold":output,"swapMode":"ExactIn","slippageBps":500,
        "platformFee":null,"priceImpactPct":"0.01",
        "routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]})
}

pub(super) async fn refresh(
    m: ExecutionBuildPlanMetadata,
    input: u64,
    body: Value,
) -> Result<(ExecutionConfig, ExecutionBuildPlanMetadata)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let mut config = ExecutionConfig::default();
    config.canary_buy_size_sol = input as f64 / 1_000_000_000.0;
    config.quote_canary_buy_slippage_bps = 500;
    config.quote_canary_base_url = format!("http://{}", listener.local_addr()?);
    config.quote_canary_timeout_ms = 1_000;
    let server = tokio::spawn(async move {
        let mut socket = listener.accept().await.unwrap().0;
        let mut buf = [0; 8192];
        let n = socket.read(&mut buf).await.unwrap();
        let request = String::from_utf8_lossy(&buf[..n]);
        assert!(request.starts_with("GET /quote?"));
        assert!(request.contains(&format!("amount={input}&")), "{request}");
        write_http_status_json(&mut socket, 200, &body.to_string()).await;
    });
    let signal = tiny_route_signal("fresh-size", chrono::Utc::now());
    let fresh =
        refresh_tiny_buy_build_plan_metadata(&reqwest::Client::new(), &config, &signal, m).await?;
    tokio::time::timeout(std::time::Duration::from_secs(3), server).await??;
    Ok((config, fresh))
}

pub(super) fn close(actual: f64, expected: f64, tolerance: f64) {
    assert!(
        actual.is_finite() && (actual - expected).abs() <= tolerance,
        "actual={actual:.17e}, expected={expected:.17e}, tolerance={tolerance}"
    );
}

pub(super) fn assert_quote(
    m: &ExecutionBuildPlanMetadata,
    input: u64,
    output: u64,
    expected_slippage: f64,
    decision: &str,
) {
    assert_eq!(
        m.quote_in_amount_raw.as_deref(),
        Some(input.to_string().as_str())
    );
    assert_eq!(
        m.quote_out_amount_raw.as_deref(),
        Some(output.to_string().as_str())
    );
    close(
        m.quote_price_sol.unwrap(),
        input as f64 / (u128::from(output) * 1_000_000_000) as f64,
        1e-18,
    );
    close(m.slippage_bps.unwrap(), expected_slippage, 1e-9);
    assert_eq!(m.decision_status.as_deref(), Some(decision));
}
