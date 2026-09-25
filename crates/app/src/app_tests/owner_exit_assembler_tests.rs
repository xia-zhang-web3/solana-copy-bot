//! The real owner-exit SELL assembler accepts Jupiter's omitted zero CU-price.
use super::{owner_buy_fixture, owner_exit_test_fixture as fixture};
use crate::execution_instruction_bundle_binding::BundleRequest;
use crate::execution_submit_adapter::{
    ExecutionBuildPlanMetadata, ExecutionSubmitAdapter, ExecutionSubmitRequest,
    JupiterMetisDryRunExecutionAdapter,
};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};

fn case(base: &str) -> Result<(copybot_config::ExecutionConfig, ExecutionSubmitRequest)> {
    let wallet = fixture::wallet();
    let mut config = owner_buy_fixture::config(&wallet, base, "/tmp/owner-exit-assembler-stop");
    config.owner_technical_buy = None;
    config.pretrade_min_sol_reserve = 0.160_200_031;
    let quote = fixture::quote();
    let request = ExecutionSubmitRequest {
        order_id: "owner-exit-assembler-order".into(),
        signal_id: "owner-exit:owner-exit-assembler-intent".into(),
        client_order_id: "owner-exit-assembler-client".into(),
        attempt: 1,
        route: "jupiter_swap_instructions".into(),
        wallet_id: wallet.clone(),
        token: fixture::USDC.into(),
        side: "sell".into(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 50,
        wallet_pubkey: wallet,
        entry_route_plan_json: None,
        metadata: ExecutionBuildPlanMetadata {
            quote_event_id: Some("owner-exit-assembler-quote".into()),
            quote_status: Some("ok".into()),
            quote_in_amount_raw: Some("1167085".into()),
            quote_out_amount_raw: Some(fixture::OUT.to_string()),
            quote_response_json: Some(quote.to_string()),
            route_plan_json: Some(quote["routePlan"].to_string()),
            slippage_bps: Some(50.0),
            priority_fee_status: Some("ok".into()),
            priority_fee_lamports: Some(0),
            priority_fee_json: Some(super::priority_fee_fixture::total_json(0)),
            ..Default::default()
        },
    };
    Ok((config, request))
}

#[tokio::test]
async fn owner_exit_omitted_zero_price_reaches_local_simulation() -> Result<()> {
    let server = fixture::Server::start().await?;
    server.state.lock().unwrap().mode = "omit-price";
    let (config, request) = case(&server.url)?;
    let plan =
        JupiterMetisDryRunExecutionAdapter::new(config.clone()).build_transaction_plan(&request)?;
    let response = fixture::bundle_without_price()?;
    let mut original = fixture::instructions(fixture::key().verifying_key().to_bytes())?;
    original.remove(1);
    let old_wire = crate::execution_native_floor::prepare_final_native_floor(
        fixture::key().verifying_key().to_bytes(),
        [9; 32],
        &original,
        160_200_031,
    )?;
    assert!(crate::execution_priority_fee_proof::prove(
        &request,
        old_wire.payload(),
        config.pretrade_max_priority_fee_lamports,
    )
    .unwrap_err()
    .to_string()
    .contains("priority_fee_explicit_cu_price_required"));
    let bundle = BundleRequest::capture(&plan)?.bind(&response)?;
    let built = crate::execution_guarded_generic_sell::assemble(&config, &plan, &bundle)?;
    let fee = crate::execution_priority_fee_wire::decode_priority_fee(
        &built.serialized_transaction_base64,
    )?;
    assert_eq!((fee.limit.get(), fee.price, fee.total), (200_000, 0, 0));
    let floor = crate::execution_native_floor::verify_final_native_floor(
        &built.serialized_transaction_base64,
        fixture::key().verifying_key().to_bytes(),
        160_200_031,
    )?;
    assert_eq!(floor.reserve_lamports(), 160_200_031);
    crate::execution_owner_exit_wire::verify(&request, &built.serialized_transaction_base64)?;

    let http = reqwest::Client::builder().no_proxy().build()?;
    let result = crate::execution_guarded_generic_sell::prepare(&http, &config, &plan).await?;
    let crate::execution_guarded_generic_buy::GenericBuyOutcome::Built(simulated) = result else {
        panic!("owner exit did not build");
    };
    assert_eq!(
        simulated.serialized_transaction_base64,
        built.serialized_transaction_base64
    );
    assert!(simulated.summary.contains("rpc_simulation=passed"));
    assert_eq!(server.calls("instructions"), 1);
    assert_eq!(server.calls("simulateTransaction"), 1);
    assert_eq!(server.calls("sendTransaction"), 0);
    Ok(())
}

#[test]
fn owner_exit_zero_price_refuses_bad_fee_and_scope() -> Result<()> {
    let (config, request) = case("http://127.0.0.1:1")?;
    let plan =
        JupiterMetisDryRunExecutionAdapter::new(config.clone()).build_transaction_plan(&request)?;
    let baseline = fixture::bundle_without_price()?;
    for (case, value, expected) in [
        (
            "malformed",
            with_price(&baseline, &[3, 0]),
            "priority_fee_duplicate_or_malformed_price",
        ),
        (
            "nonzero",
            with_price(&baseline, &[vec![3], 1_u64.to_le_bytes().to_vec()].concat()),
            "owner_exit_zero_priority_mismatch",
        ),
        (
            "duplicate",
            {
                let mut value =
                    with_price(&baseline, &[vec![3], 0_u64.to_le_bytes().to_vec()].concat());
                let ix = value["computeBudgetInstructions"][1].clone();
                value["computeBudgetInstructions"]
                    .as_array_mut()
                    .unwrap()
                    .push(ix);
                value
            },
            "priority_fee_duplicate_or_malformed_price",
        ),
        (
            "missing-limit",
            {
                let mut value = baseline.clone();
                value["computeBudgetInstructions"] = json!([]);
                value
            },
            "priority_fee_explicit_cu_limit_required",
        ),
    ] {
        let bundle = BundleRequest::capture(&plan)?.bind(&value)?;
        let error = crate::execution_guarded_generic_sell::assemble(&config, &plan, &bundle)
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{case}: {error}");
    }
    let mut wrong_amount = baseline.clone();
    wrong_amount["swapInstruction"]["data"] = json!(STANDARD.encode([0]));
    let bundle = BundleRequest::capture(&plan)?.bind(&wrong_amount)?;
    assert!(
        crate::execution_guarded_generic_sell::assemble(&config, &plan, &bundle)
            .unwrap_err()
            .to_string()
            .contains("owner_exit_wire")
    );

    let mut other = plan.clone();
    other.signal_id = "source-sell:fixture".into();
    let bundle = BundleRequest::capture(&other)?.bind(&baseline)?;
    assert!(
        crate::execution_guarded_generic_sell::assemble(&config, &other, &bundle)
            .unwrap_err()
            .to_string()
            .contains("priority_fee_explicit_cu_price_required")
    );
    let mut wrong_wallet = plan.clone();
    wrong_wallet.wallet_pubkey = bs58::encode([8; 32]).into_string();
    assert!(BundleRequest::capture(&wrong_wallet).is_err());
    Ok(())
}

fn with_price(baseline: &Value, data: &[u8]) -> Value {
    let mut value = baseline.clone();
    value["computeBudgetInstructions"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "programId":"ComputeBudget111111111111111111111111111111",
            "accounts":[],"data":STANDARD.encode(data),
        }));
    value
}
