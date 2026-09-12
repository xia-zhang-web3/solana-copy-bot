//! Synthetic transfer-only payload: exact input and floor, no DEX execution claim.
pub(super) fn buy(payer: [u8; 32], blockhash: [u8; 32], amount: u64) -> String {
    let mut instructions = super::priority_fee_fixture::budget(200_000, 10_000);
    instructions.push(super::native_funding_fixture::transfer(
        payer, [52; 32], amount,
    ));
    crate::execution_native_floor::prepare_final_native_floor(
        payer,
        blockhash,
        &instructions,
        50_000_001,
    )
    .unwrap()
    .payload()
    .to_owned()
}
pub(super) fn funding() -> super::initial_sol_rpc_fixture::FundingRpc {
    // Synthetic full-fee observation within the approved per-transaction cap.
    super::initial_sol_rpc_fixture::FundingRpc {
        fee: Some(100_000),
        ..Default::default()
    }
}

pub(super) fn bundle(payer: [u8; 32], blockhash: [u8; 32], amount: u64) -> serde_json::Value {
    bundle_with_price(payer, blockhash, amount, 110_000)
}

pub(super) fn bundle_with_price(
    payer: [u8; 32],
    blockhash: [u8; 32],
    amount: u64,
    micro_lamports_per_cu: u64,
) -> serde_json::Value {
    use base64::Engine;
    let mut bundle =
        super::generic_sell_synthetic_fixture::bundle(payer, 200_000, micro_lamports_per_cu);
    let ix = super::native_funding_fixture::transfer(payer, [52; 32], amount);
    bundle["swapInstruction"] = serde_json::json!({"programId":bs58::encode(ix.program_id).into_string(),
        "accounts":ix.accounts.iter().map(|a| serde_json::json!({"pubkey":bs58::encode(a.pubkey).into_string(),"isSigner":a.is_signer,"isWritable":a.is_writable})).collect::<Vec<_>>(),
        "data":base64::engine::general_purpose::STANDARD.encode(ix.data)});
    bundle["blockhashWithMetadata"]["blockhash"] = serde_json::json!(blockhash.to_vec());
    bundle
}
