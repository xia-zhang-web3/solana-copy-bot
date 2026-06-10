use super::*;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;

#[tokio::test]
#[ignore = "requires live RPC and PumpSwap pool env; performs simulateTransaction only"]
async fn pumpswap_direct_buy_live_simulation_passes() -> Result<()> {
    let rpc_url = required_env("COPYBOT_PUMPSWAP_DIRECT_LIVE_RPC_URL")?;
    let wallet = required_env("COPYBOT_PUMPSWAP_DIRECT_LIVE_WALLET")?;
    let token = required_env("COPYBOT_PUMPSWAP_DIRECT_LIVE_TOKEN")?;
    let pool = required_env("COPYBOT_PUMPSWAP_DIRECT_LIVE_POOL")?;
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_wallet_pubkey = wallet.clone();
    config.submit_adapter_http_url = rpc_url;
    config.canary_tiny_submit_enabled = true;
    config.swap_transaction_dry_run_enabled = true;
    config.quote_canary_timeout_ms = 5_000;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let request = live_pumpswap_direct_request(&config, &wallet, &token, &pool);
    let plan = adapter.build_transaction_plan(&request)?;
    let result =
        crate::execution_pumpswap_direct_builder::fetch_pumpswap_direct_buy_transaction_dry_run(
            &reqwest::Client::new(),
            &config,
            &plan,
        )
        .await?
        .expect("direct PumpSwap result");

    assert_eq!(result.source, "pumpswap_direct");
    assert!(result.summary.contains("rpc_simulation=passed"));
    Ok(())
}

#[tokio::test]
#[ignore = "requires live RPC, wallet with token balance, and PumpSwap pool env; simulateTransaction only"]
async fn pumpswap_direct_sell_live_simulation_passes() -> Result<()> {
    let rpc_url = required_env("COPYBOT_PUMPSWAP_DIRECT_LIVE_RPC_URL")?;
    let wallet = required_env("COPYBOT_PUMPSWAP_DIRECT_LIVE_WALLET")?;
    let token = required_env("COPYBOT_PUMPSWAP_DIRECT_LIVE_SELL_TOKEN")?;
    let pool = required_env("COPYBOT_PUMPSWAP_DIRECT_LIVE_SELL_POOL")?;
    let amount_raw = required_env("COPYBOT_PUMPSWAP_DIRECT_LIVE_SELL_AMOUNT_RAW")?;
    let min_output_raw = required_env("COPYBOT_PUMPSWAP_DIRECT_LIVE_SELL_MIN_OUTPUT_RAW")?;
    let config = live_probe_config(&rpc_url, &wallet);
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let request = live_pumpswap_direct_sell_request(
        &config,
        &wallet,
        &token,
        &pool,
        &amount_raw,
        &min_output_raw,
    );
    let plan = adapter.build_transaction_plan(&request)?;
    let result =
        crate::execution_pumpswap_direct_builder::fetch_pumpswap_direct_transaction_dry_run(
            &reqwest::Client::new(),
            &config,
            &plan,
        )
        .await?
        .expect("direct PumpSwap result");

    assert_eq!(result.source, "pumpswap_direct");
    assert!(result
        .summary
        .contains("pumpswap_direct_sell_transaction_ok"));
    assert!(result.summary.contains("rpc_simulation=passed"));
    Ok(())
}

fn live_pumpswap_direct_request(
    config: &ExecutionConfig,
    wallet: &str,
    token: &str,
    pool: &str,
) -> crate::execution_submit_adapter::ExecutionSubmitRequest {
    let route_plan = format!(r#"[{{"swapInfo":{{"ammKey":"{pool}","label":"Pump.fun Amm"}}}}]"#);
    crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: "live-pumpswap-direct-probe".to_string(),
        signal_id: "live-pumpswap-direct-probe".to_string(),
        client_order_id: "live-pumpswap-direct-probe".to_string(),
        attempt: 1,
        route: config.canary_route.clone(),
        wallet_id: "live-probe-wallet".to_string(),
        token: token.to_string(),
        side: "buy".to_string(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: wallet.to_string(),
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
            quote_source: Some(
                crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS.to_string(),
            ),
            quote_event_id: Some("quote:live-pumpswap-direct-probe".to_string()),
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("1".to_string()),
            quote_response_json: Some(format!(
                r#"{{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"{token}","outAmount":"1","otherAmountThreshold":"1","swapMode":"ExactIn","slippageBps":500,"routePlan":{route_plan}}}"#
            )),
            quote_price_sol: Some(0.0),
            price_impact_pct: Some(0.0),
            route_plan_json: Some(route_plan),
            priority_fee_source: Some("live-probe".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(22_000),
            priority_fee_json: None,
            slippage_bps: Some(500.0),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("live_probe".to_string()),
        },
    }
}

fn live_pumpswap_direct_sell_request(
    config: &ExecutionConfig,
    wallet: &str,
    token: &str,
    pool: &str,
    amount_raw: &str,
    min_output_raw: &str,
) -> crate::execution_submit_adapter::ExecutionSubmitRequest {
    let route_plan = format!(r#"[{{"swapInfo":{{"ammKey":"{pool}","label":"Pump.fun Amm"}}}}]"#);
    crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: "live-pumpswap-direct-sell-probe".to_string(),
        signal_id: "live-pumpswap-direct-sell-probe".to_string(),
        client_order_id: "live-pumpswap-direct-sell-probe".to_string(),
        attempt: 1,
        route: config.canary_route.clone(),
        wallet_id: "live-probe-wallet".to_string(),
        token: token.to_string(),
        side: "sell".to_string(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: wallet.to_string(),
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
            quote_source: Some(
                crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS.to_string(),
            ),
            quote_event_id: Some("quote:live-pumpswap-direct-sell-probe".to_string()),
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some(amount_raw.to_string()),
            quote_out_amount_raw: Some(min_output_raw.to_string()),
            quote_response_json: Some(format!(
                r#"{{"inputMint":"{token}","inAmount":"{amount_raw}","outputMint":"So11111111111111111111111111111111111111112","outAmount":"{min_output_raw}","otherAmountThreshold":"{min_output_raw}","swapMode":"ExactIn","slippageBps":500,"routePlan":{route_plan}}}"#
            )),
            quote_price_sol: Some(0.0),
            price_impact_pct: Some(0.0),
            route_plan_json: Some(route_plan),
            priority_fee_source: Some("live-probe".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(22_000),
            priority_fee_json: None,
            slippage_bps: Some(500.0),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("live_probe".to_string()),
        },
    }
}

fn live_probe_config(rpc_url: &str, wallet: &str) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_wallet_pubkey = wallet.to_string();
    config.submit_adapter_http_url = rpc_url.to_string();
    config.canary_tiny_submit_enabled = true;
    config.swap_transaction_dry_run_enabled = true;
    config.quote_canary_timeout_ms = 5_000;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name)
        .map(|value| value.trim().to_string())
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("missing {name}"))
}
