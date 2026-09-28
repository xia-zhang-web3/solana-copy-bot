//! Explicit model follower wire/quote, never a historical source BUY or live receipt.
use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::{PubkeyBytes, SolanaInstruction};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use ed25519_dalek::{Signer, SigningKey};
use serde_json::{json, Value};

pub(crate) fn quote(mint: &str, input: u64, output: u64, slippage: u64) -> Value {
    let mut q = super::owner_buy_wire_fixture::quote();
    q["outputMint"] = json!(mint); q["inAmount"] = json!(input.to_string());
    q["outAmount"] = json!(output.to_string()); q["slippageBps"] = json!(slippage);
    q["otherAmountThreshold"] = json!((output * (10_000 - slippage) / 10_000).to_string());
    q["routePlan"][0]["swapInfo"]["outputMint"] = json!(mint);
    q["routePlan"][0]["swapInfo"]["inAmount"] = json!(input.to_string());
    q["routePlan"][0]["swapInfo"]["outAmount"] = json!(output.to_string());
    q
}
pub(crate) fn instructions(wallet: PubkeyBytes, mint: &str, input: u64, output: u64) -> Result<Vec<SolanaInstruction>> {
    let old_mint = parse_pubkey(super::owner_buy_wire_fixture::USDC,"old_mint")?;
    let mint = parse_pubkey(mint,"model_mint")?;
    let old_destination = associated_token_address(&wallet,&old_mint,&token_program_id());
    let destination = associated_token_address(&wallet,&mint,&token_program_id());
    let jupiter = parse_pubkey(crate::execution_owner_buy_wire::JUPITER,"model_jup")?;
    let mut ixs = super::owner_buy_wire_fixture::instructions(wallet)?;
    for i in &mut ixs {
        for a in &mut i.accounts {
            if a.pubkey == old_mint {a.pubkey = mint;}
            if a.pubkey == old_destination {a.pubkey = destination;}
        }
        if i.program_id == system_program_id() {i.data[4..12].copy_from_slice(&input.to_le_bytes());}
        if i.program_id == jupiter {
            i.data[16..24].copy_from_slice(&input.to_le_bytes());
            i.data[24..32].copy_from_slice(&output.to_le_bytes());
        }
    }
    Ok(ixs)
}
pub(crate) fn signed(mint: &str, input: u64, output: u64, floor: u64) -> Result<(String,String,PubkeyBytes)> {
    let signer = SigningKey::from_bytes(&[11;32]);
    let wallet = signer.verifying_key().to_bytes();
    let instructions = instructions(wallet,mint,input,output)?;
    let floor = crate::execution_native_floor::prepare_final_native_floor(wallet,[9;32],&instructions,floor)?;
    let mut bytes = STANDARD.decode(floor.payload())?;
    let sig = signer.sign(&bytes[65..]);
    bytes[1..65].copy_from_slice(&sig.to_bytes());
    Ok((STANDARD.encode(bytes),bs58::encode(sig.to_bytes()).into_string(),wallet))
}
