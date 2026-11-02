//! Fixed validated wire keys; only untrusted RPC token metadata changes.
use crate::execution_controlled_receipt_lot::bounded::receipt::token_delta;
use anyhow::Result;
use serde_json::{json, Value};
fn fixture() -> Result<(Vec<String>, String, Value)> {
    use crate::execution_pumpswap_accounts::*;
    let wallet = "7v54NWdBtkjuAFJrLGsS2SXnuk8nKam81mZJeeYxVFi9".to_string();
    let mint = copybot_storage_core::OWNER_EXIT_MINT;
    let ata = bs58::encode(associated_token_address(
        &parse_pubkey(&wallet, "mock-wallet")?,
        &parse_pubkey(mint, "mock-mint")?,
        &token_program_id(),
    ))
    .into_string();
    let row = |amount: &str| json!({"accountIndex":1,"owner":wallet,"mint":mint,"programId":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA","uiTokenAmount":{"amount":amount,"decimals":6}});
    let value=json!({"meta":{"preTokenBalances":[row("100")],"postTokenBalances":[row("7100")]}});
    Ok((vec![wallet.clone(),ata],wallet,value))
}
#[test]
fn local_source_receipt_valid_paired_canonical_account_control() -> Result<()> {
    let (keys, w, v) = fixture()?;
    assert_eq!(token_delta(&v, &keys, &w)?, 7000);
    Ok(())
}
#[test]
fn local_source_receipt_outside_wire_account_cannot_create_delta() -> Result<()> {
    let (keys, w, mut v) = fixture()?;
    v["meta"]["preTokenBalances"][0]["accountIndex"] = json!(999);
    v["meta"]["postTokenBalances"][0]["accountIndex"] = json!(999);
    assert!(token_delta(&v, &keys, &w).is_err());
    Ok(())
}
#[test]
fn local_source_receipt_duplicate_index_even_same_aggregate_delta_refuses() -> Result<()> {
    let (keys, w, mut v) = fixture()?;
    let pre = v["meta"]["preTokenBalances"][0].clone();
    let mut post = v["meta"]["postTokenBalances"][0].clone();
    post["uiTokenAmount"]["amount"] = json!("3600");
    // The old sum-only check returned the correct 7000 delta for these duplicate rows.
    v["meta"]["preTokenBalances"] = json!([pre, pre]);
    v["meta"]["postTokenBalances"] = json!([post, post]);
    assert!(token_delta(&v, &keys, &w).is_err());
    Ok(())
}
#[test]
fn local_source_receipt_non_classic_program_refuses() -> Result<()> {
    let (keys, w, mut v) = fixture()?;
    v["meta"]["postTokenBalances"][0]["programId"] = json!("11111111111111111111111111111111");
    assert!(token_delta(&v, &keys, &w).is_err());
    Ok(())
}
