use super::*;

pub(super) fn generic_pump_fun_amm_request(
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
        entry_route_plan_json: None,
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
            rpc_owned_sell: None,
            rpc_owned_live: None,
            owned_sell_amount: None,
            protected_capital: None,
            http_request_started_ts: None,
            quote_response_available_ts: None,
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
            priority_fee_json: Some(crate::app_tests::priority_fee_fixture::total_json(22_000)),
            slippage_bps: Some(125.0),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
        },
    }
}
