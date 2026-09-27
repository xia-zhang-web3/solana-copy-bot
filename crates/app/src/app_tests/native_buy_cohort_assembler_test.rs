use anyhow::Result;

#[test]
fn cohort_native_buy_zero_fee_uses_real_jupiter_assembler() -> Result<()> {
    use crate::execution_instruction_bundle_binding::BundleRequest;
    use crate::execution_submit_adapter::{
        ExecutionBuildPlanMetadata, ExecutionSubmitAdapter, ExecutionSubmitRequest,
        JupiterMetisDryRunExecutionAdapter,
    };
    use serde_json::Value;
    let quote: Value = serde_json::from_str(include_str!(
        "generic_buy_fixtures/owner-sol-usdc-20260924-quote.json"
    ))?;
    let bundle: Value = serde_json::from_str(include_str!(
        "generic_buy_fixtures/owner-sol-usdc-20260924-instructions.json"
    ))?;
    let wallet = "BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk";
    let mut c = crate::app_tests::generic_buy_fixture::config("http://127.0.0.1:9");
    c.canary_route = "jupiter_swap_instructions".into();
    c.canary_wallet_pubkey = wallet.into();
    c.execution_signer_pubkey = wallet.into();
    c.pretrade_max_priority_fee_lamports = 50_000;
    c.technical_cohort = Some(copybot_config::TechnicalCohortConfig {
        policy: copybot_config::TECHNICAL_COHORT_V1.into(),
        activate: true,
        run_id: "assembler-fixture".into(),
        wallet_ids: vec!["leader".into()],
        mint_policy: copybot_config::CLASSIC_SPL_MINT_V1.into(),
        route: c.canary_route.clone(),
        activated_at: "fixture".into(),
        deadline: "fixture".into(),
        max_wait_seconds: 3600,
        max_buy_count: 1,
        max_source_sell_count: 1,
    });
    let request = ExecutionSubmitRequest {
        order_id: "exec-canary:native-buy-v1:fixture".into(),
        signal_id: "native-buy-v1:fixture".into(),
        client_order_id: "copybot:native-buy-v1:fixture".into(),
        attempt: 1,
        route: c.canary_route.clone(),
        wallet_id: "leader".into(),
        token: quote["outputMint"].as_str().unwrap().into(),
        side: "buy".into(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 50,
        wallet_pubkey: wallet.into(),
        entry_route_plan_json: None,
        metadata: ExecutionBuildPlanMetadata {
            quote_event_id: Some("cohort-fixture-quote".into()),
            quote_status: Some("ok".into()),
            quote_in_amount_raw: Some("10000000".into()),
            quote_out_amount_raw: quote["outAmount"].as_str().map(str::to_owned),
            quote_response_json: Some(quote.to_string()),
            route_plan_json: Some(quote["routePlan"].to_string()),
            priority_fee_status: Some("ok".into()),
            priority_fee_lamports: Some(0),
            priority_fee_json: Some(crate::app_tests::priority_fee_fixture::total_json(0)),
            ..Default::default()
        },
    };
    let plan =
        JupiterMetisDryRunExecutionAdapter::new(c.clone()).build_transaction_plan(&request)?;
    let bound = BundleRequest::capture(&plan)?.bind(&bundle)?;
    let payload = crate::execution_guarded_generic_buy::assemble(&c, &plan, &bound, 50_000_001)?
        .serialized_transaction_base64;
    let fee = crate::execution_priority_fee_wire::decode_priority_fee(&payload)?;
    assert_eq!((fee.price, fee.total), (0, 0));
    Ok(())
}
