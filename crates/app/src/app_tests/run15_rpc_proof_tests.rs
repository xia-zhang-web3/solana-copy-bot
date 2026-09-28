use super::run15_rpc_proof_fixture as f;
use crate::execution_owned_sell_rpc::decode;
use crate::execution_submit_adapter::{ExecutionBuildPlanMetadata, ExecutionSubmitRequest};
use anyhow::Result;
use serde_json::json;

#[test]
fn saved_raydium_source_sell_and_historical_buy_reach_finalized_proof() -> Result<()> {
    let sell = f::source_sell();
    // The accepted Pump-only predicate has zero matches on this untouched real source.
    assert!(!sell["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .iter()
        .any(|k| k == "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"));
    assert_eq!(decode::sell(&sell, &f::sell_admission(&sell))?, 451313058);
    let buy = f::source_buy();
    let (anchor, facts) = f::anchor(&buy, 163956354);
    // This is only a layout control: historical source BUY is never a follower receipt.
    decode::buy(&buy, &anchor, &facts)?;
    Ok(())
}
#[test]
fn saved_raydium_source_refuses_wrong_owner_program_raw_and_version() -> Result<()> {
    let v = f::source_sell();
    let admission = f::sell_admission(&v);
    let mut wrong = v.clone();
    wrong["meta"]["preTokenBalances"][3]["owner"] = json!(f::MINT);
    assert!(decode::sell(&wrong, &admission).is_err());
    let mut wrong = v.clone();
    wrong["transaction"]["message"]["accountKeys"][15] = json!(f::MINT);
    assert!(decode::sell(&wrong, &admission).is_err());
    let mut wrong = v.clone();
    let mut raw = bs58::decode(
        wrong["meta"]["innerInstructions"][0]["instructions"][0]["data"]
            .as_str()
            .unwrap(),
    )
    .into_vec()?;
    raw[1] ^= 1;
    wrong["meta"]["innerInstructions"][0]["instructions"][0]["data"] =
        json!(bs58::encode(raw).into_string());
    assert!(decode::sell(&wrong, &admission).is_err());
    for version in [json!("legacy"), json!(0), json!(1)] {
        let mut wrong = v.clone();
        wrong["version"] = version;
        wrong["transaction"]["message"]["transactionConfig"] = json!(null);
        assert!(decode::sell(&wrong, &admission).is_err());
    }
    Ok(())
}
fn request(payload_tx: &serde_json::Value) -> ExecutionSubmitRequest {
    let quote = super::run15_buy_wire_fixture::quote(f::MINT, 10_000_000, 100_000_000, 50);
    ExecutionSubmitRequest {
        order_id: "model-buy".into(),
        signal_id: "native-buy-v1:model".into(),
        client_order_id: "model".into(),
        attempt: 1,
        route: "jupiter_swap_instructions".into(),
        wallet_id: f::LEADER.into(),
        token: f::MINT.into(),
        side: "buy".into(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 50,
        wallet_pubkey: payload_tx["transaction"]["message"]["accountKeys"][0]
            .as_str()
            .unwrap()
            .into(),
        entry_route_plan_json: None,
        metadata: ExecutionBuildPlanMetadata {
            quote_in_amount_raw: Some("10000000".into()),
            quote_out_amount_raw: Some("100000000".into()),
            quote_response_json: Some(quote.to_string()),
            route_plan_json: Some(quote["routePlan"].to_string()),
            priority_fee_lamports: Some(2000),
            priority_fee_json: Some(super::priority_fee_fixture::total_json(2000)),
            priority_fee_status: Some("ok".into()),
            ..Default::default()
        },
    }
}
#[test]
fn model_follower_temp_wsol_raydium_buy_passes_wire_and_finalized_anchor() -> Result<()> {
    let (payload, tx) = f::model_follower()?;
    let request = request(&tx);
    crate::execution_owner_buy_wire::verify_cohort(&request, &payload)?;
    let (anchor, facts) = f::anchor(&tx, 100_000_000);
    decode::buy(&tx, &anchor, &facts)?;
    assert!(tx["meta"]["preTokenBalances"]
        .as_array()
        .unwrap()
        .iter()
        .all(|r| r["mint"] != f::SOL || r["owner"] != request.wallet_pubkey));
    Ok(())
}
#[test]
fn model_follower_new_target_has_proven_creation_and_separate_rent() -> Result<()> {
    let (payload, tx) = f::model_follower_with_output_and_target(100_000_000, 160_200_031, true)?;
    crate::execution_owner_buy_wire::verify_cohort(&request(&tx), &payload)?;
    let (anchor, facts) = f::anchor(&tx, 100_000_000);
    decode::buy(&tx, &anchor, &facts)?;
    assert_eq!(facts.wallet_native_post.as_u64(), 989_981_000 - 2_039_280);
    let mut wrong = tx;
    wrong["meta"]["innerInstructions"][1]["instructions"]
        .as_array_mut()
        .unwrap()
        .pop();
    assert!(decode::buy(&wrong, &anchor, &facts).is_err());
    Ok(())
}
#[test]
fn follower_gate_refuses_arbitrary_route_and_anchor_refuses_missing_temp_owner() -> Result<()> {
    let (payload, tx) = f::model_follower()?;
    let mut request = request(&tx);
    let mut quote: serde_json::Value =
        serde_json::from_str(request.metadata.quote_response_json.as_deref().unwrap())?;
    quote["routePlan"][0]["swapInfo"]["label"] = json!("Metis");
    request.metadata.quote_response_json = Some(quote.to_string());
    assert!(crate::execution_owner_buy_wire::verify_cohort(&request, &payload).is_err());
    let (anchor, facts) = f::anchor(&tx, 100_000_000);
    let mut wrong = tx.clone();
    wrong["meta"]["innerInstructions"][0]["instructions"][3]["parsed"]["info"]["owner"] =
        json!(f::LEADER);
    assert!(decode::buy(&wrong, &anchor, &facts).is_err());
    let mut wrong = tx;
    wrong["meta"]["postTokenBalances"][1]["owner"] = json!(f::LEADER);
    assert!(decode::buy(&wrong, &anchor, &facts).is_err());
    Ok(())
}
