use super::execution_pump_fun_direct_builder_contract::{
    pump_fun_direct_config, pumpswap_global_config_data, pumpswap_pool_data, read_http_request,
    rpc_account_json, write_http_json,
};
use super::*;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;

#[tokio::test]
async fn sell_can_use_entry_pumpswap_amm_when_current_route_is_meteora() -> Result<()> {
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
    config.quote_canary_pump_fun_parallel_enabled = false;
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let request = meteora_sell_with_entry_pumpswap_request(&config, token, pool);
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
        .contains("pumpswap_direct_sell_transaction_ok"));
    assert_eq!(payload.source, "pumpswap_direct");
    Ok(())
}

#[test]
fn direct_error_uses_entry_pumpswap_context_without_marking_generic_meteora() -> Result<()> {
    let config = pump_fun_direct_config("http://127.0.0.1:1");
    let token = "FNVhryGP7Epjbr9iWLv75ABCYzY3au4icYHNdExn9jZ3";
    let pool = "FhmcZfBdmvaYdphQ4qcd8qjs6wUv1Viic4UkiWqiZxxd";
    let request = meteora_sell_with_entry_pumpswap_request(&config, token, pool);
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let plan = adapter.build_transaction_plan(&request)?;
    let message = r#"simulate failed {"InstructionError":[6,{"Custom":6004}]}"#;
    let direct = crate::execution_submit_adapter::test_pumpswap_direct_error_for_plan(
        &plan,
        &anyhow::anyhow!(message),
        120,
    );
    let generic =
        crate::execution_submit_adapter::test_execution_error_text_for_plan(&plan, message, 120);

    assert!(direct.contains("pamm_custom_errors=6004:ExceededSlippage"));
    assert!(!generic.contains("pamm_custom_errors="));
    Ok(())
}

fn meteora_sell_with_entry_pumpswap_request(
    config: &ExecutionConfig,
    token: &str,
    pool: &str,
) -> crate::execution_submit_adapter::ExecutionSubmitRequest {
    let entry_route_plan =
        format!(r#"[{{"swapInfo":{{"ammKey":"{pool}","label":"Pump.fun Amm"}}}}]"#);
    let current_route_plan =
        r#"[{"swapInfo":{"ammKey":"11111111111111111111111111111111","label":"Meteora DAMM v2"}}]"#;
    crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: "order-meteora-sell-entry-pumpswap".to_string(),
        signal_id: "signal-meteora-sell-entry-pumpswap".to_string(),
        client_order_id: "client-meteora-sell-entry-pumpswap".to_string(),
        attempt: 1,
        route: config.canary_route.clone(),
        wallet_id: "leader-wallet".to_string(),
        token: token.to_string(),
        side: "sell".to_string(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: config.canary_wallet_pubkey.clone(),
        entry_route_plan_json: Some(entry_route_plan),
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
            quote_source: Some(
                crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS.to_string(),
            ),
            quote_event_id: Some("quote:exit:meteora-entry-pumpswap".to_string()),
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some("123456".to_string()),
            quote_out_amount_raw: Some("10000".to_string()),
            quote_response_json: Some(format!(
                r#"{{"inputMint":"{token}","inAmount":"123456","outputMint":"So11111111111111111111111111111111111111112","outAmount":"10000","otherAmountThreshold":"9500","swapMode":"ExactIn","slippageBps":500,"routePlan":{current_route_plan}}}"#
            )),
            quote_price_sol: Some(0.081),
            price_impact_pct: Some(0.01),
            route_plan_json: Some(current_route_plan.to_string()),
            priority_fee_source: Some("test".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(22_000),
            priority_fee_json: Some(r#"{"recommended":22000}"#.to_string()),
            slippage_bps: Some(125.0),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            ..crate::execution_submit_adapter::ExecutionBuildPlanMetadata::default()
        },
    }
}
