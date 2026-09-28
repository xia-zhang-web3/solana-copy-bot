use crate::app_tests::{run15_receipt_projection as projection, run15_rpc_proof_fixture as f};
use anyhow::Result;
use copybot_storage_core::{
    ExecutionCanaryReceiptProof, ReceiptDecomposition, ReceiptTokenCoverage,
};
use serde_json::json;

#[test]
fn run15_receipt_projection_proves_new_target_but_missing_or_raw_lifecycle_stays_unknown(
) -> Result<()> {
    let (payload, raw) = f::model_follower_with_output_and_target(100_000_000, 160_200_031, true)?;
    let (_anchor, known) = f::anchor(&raw, 100_000_000);
    let proof = ExecutionCanaryReceiptProof {
        tx_signature: known.tx_signature.clone(),
        wallet_pubkey: known.wallet_pubkey.clone(),
        token: f::MINT.into(),
        side: "buy".into(),
        confirmation_status: "confirmed".into(),
        slot: Some(known.slot),
        confirmed_at: chrono::Utc::now(),
        reason: "model-confirmation".into(),
    };
    let receipt = projection::parsed(&raw, &payload)?;
    let parse = |v: &serde_json::Value| {
        super::facts_from_transaction_json(
            "model-buy",
            &proof,
            &json!({"result":v}),
        )
    };
    // Reproduces the original incomplete account-key-only projection first.
    let mut indexed = raw.clone();
    indexed["transaction"]["message"]["accountKeys"] =
        receipt["transaction"]["message"]["accountKeys"].clone();
    let unknown = parse(&indexed)?.facts;
    assert_eq!(unknown.token_coverage, ReceiptTokenCoverage::Unresolved);
    assert_eq!(
        unknown.token_coverage_reason.as_deref(),
        Some("receipt_token_creation_unproven")
    );
    let known = parse(&receipt)?.facts;
    assert_eq!(known.token_coverage, ReceiptTokenCoverage::ProvenLifecycle);
    assert_eq!(known.token_delta.unwrap().raw, 100_000_000);
    assert_eq!(known.wallet_native_post.as_u64(), 987_941_720);
    assert_eq!(known.transaction_fee.unwrap().as_u64(), 19_000);
    assert_eq!(known.decomposition, ReceiptDecomposition::Unresolved);
    let mut missing = receipt.clone();
    missing["meta"]["innerInstructions"][1]["instructions"]
        .as_array_mut()
        .unwrap()
        .pop();
    assert_eq!(
        parse(&missing)?.facts.token_coverage,
        ReceiptTokenCoverage::Unresolved
    );
    let mut wrong = receipt.clone();
    wrong["meta"]["innerInstructions"][1]["instructions"][3]["parsed"]["info"]["owner"] =
        json!(f::LEADER);
    assert_eq!(
        parse(&wrong)?.facts.token_coverage,
        ReceiptTokenCoverage::Unresolved
    );
    let (anchor, facts) = f::anchor(&receipt, 100_000_000);
    crate::execution_owned_sell_rpc::decode::buy(&receipt, &anchor, &facts)?;
    Ok(())
}
