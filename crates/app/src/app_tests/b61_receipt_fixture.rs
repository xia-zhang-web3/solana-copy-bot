//! One synthetic full SELL message and its conserved receipt, built from actual keys/indexes.
use crate::execution_solana_tx::{
    serialize_unsigned_legacy_transaction, SolanaAccountMeta as Meta, SolanaInstruction,
};
use crate::execution_transaction_wire::decode_message;
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use ed25519_dalek::{Signature, VerifyingKey};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::Path;

pub(super) const MINT: [u8; 32] = [61; 32];
pub(super) const LEADER: [u8; 32] = [62; 32];
const TOKEN_ACCOUNT: [u8; 32] = [63; 32];
const POOL: [u8; 32] = [64; 32];
const POOL_TOKEN: [u8; 32] = [65; 32];
const AMM: [u8; 32] = [66; 32];
pub(super) const RAW: u64 = 7000;
pub(super) const GROSS: u64 = 100_000_000;
pub(super) const FEE: u64 = 25_000; // 5000 base + 200000 CU * 100000 micro-lamports.
pub(super) const CASH: u64 = GROSS - FEE;
pub(super) fn key(bytes: [u8; 32]) -> String {
    bs58::encode(bytes).into_string()
}

fn token_program() -> Result<[u8; 32]> {
    crate::execution_pumpswap_accounts::parse_pubkey(
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        "B61 token program",
    )
}
fn instructions(payer: [u8; 32]) -> Result<Vec<SolanaInstruction>> {
    let mut instructions = super::priority_fee_fixture::budget(200_000, 100_000);
    instructions.push(SolanaInstruction {
        program_id: AMM,
        accounts: vec![
            Meta::signer_writable(payer),
            Meta::writable(TOKEN_ACCOUNT),
            Meta::writable(POOL_TOKEN),
            Meta::writable(POOL),
            Meta::readonly(MINT),
            Meta::readonly(token_program()?),
        ],
        data: [RAW.to_le_bytes(), GROSS.to_le_bytes()].concat(),
    });
    Ok(instructions)
}
pub(super) fn bundle(payer: [u8; 32]) -> Result<Value> {
    let mut all: Vec<_> = instructions(payer)?.into_iter().map(|ix| json!({
        "programId":key(ix.program_id),"accounts":ix.accounts.iter().map(|a| json!({"pubkey":key(a.pubkey),"isSigner":a.is_signer,"isWritable":a.is_writable})).collect::<Vec<_>>(),"data":STANDARD.encode(ix.data)})).collect();
    let swap = all.pop().unwrap();
    let mut b = super::generic_sell_synthetic_fixture::bundle(payer, 200_000, 100_000);
    b["computeBudgetInstructions"] = json!(all);
    b["swapInstruction"] = swap;
    Ok(b)
}
pub(super) fn transaction(payer: [u8; 32]) -> Result<String> {
    let instructions = instructions(payer)?;
    Ok(STANDARD.encode(serialize_unsigned_legacy_transaction(
        payer,
        [9; 32],
        &instructions,
    )?))
}

// This is executed inside the RPC server BEFORE replying to sendTransaction.
// Verify wire signature independently; the RPC response cannot establish identity.
pub(super) fn accept_send(
    path: &Path,
    payer: [u8; 32],
    payload: &str,
    now: i64,
) -> Result<(String, Value)> {
    let wire = STANDARD.decode(payload)?;
    ensure!(wire.len() > 65 && wire[0] == 1, "single signer framing");
    let signature = Signature::from_slice(&wire[1..65])?;
    VerifyingKey::from_bytes(&payer)?.verify_strict(&wire[65..], &signature)?;
    let signature = bs58::encode(signature.to_bytes()).into_string();
    let conn = rusqlite::Connection::open(path)?;
    let (order_id, message_hash, transaction_hash, wallet, token, side): (String,String,String,String,String,String) = conn.query_row(
        "SELECT order_id,message_sha256,transaction_sha256,wallet,token,side FROM execution_canary_dispatch WHERE tx_signature=?1", [&signature],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
    ensure!(message_hash == format!("{:x}", Sha256::digest(&wire[65..])));
    ensure!(transaction_hash == format!("{:x}", Sha256::digest(&wire)));
    ensure!(wallet == key(payer) && token == key(MINT) && side == "sell");
    let order_signature: String = conn.query_row(
        "SELECT tx_signature FROM orders WHERE order_id=?1 AND status='execution_canary_submitted'",
        [&order_id],
        |r| r.get(0),
    )?;
    ensure!(
        order_signature == signature,
        "committed dispatch/order before HTTP"
    );
    let decoded = decode_message(payload, |_| Ok(()))?;
    ensure!(decoded.binding.message_bytes == wire[65..]);
    // The fixture's quote, simulation and signed instruction describe the same full exit.
    let expected = decode_message(&transaction(payer)?, |_| Ok(()))?;
    ensure!(
        decoded.binding.message_bytes == expected.binding.message_bytes,
        "unexpected signed message"
    );
    let accounts = &decoded.binding.accounts;
    let index = |k| {
        accounts
            .iter()
            .position(|a| a.pubkey == k)
            .context("receipt key missing from signed message")
    };
    let wallet_index = index(payer)?;
    ensure!(wallet_index == 0);
    let ta = index(TOKEN_ACCOUNT)?;
    let pt = index(POOL_TOKEN)?;
    let pool = index(POOL)?;
    let mut pre = vec![1_u64; accounts.len()];
    pre[0] = 1_000_000_000;
    pre[pool] = 1_000_000_000;
    pre[ta] = 2_039_280;
    pre[pt] = 2_039_280;
    let mut post = pre.clone();
    post[0] += CASH;
    post[pool] -= GROSS;
    ensure!(pre.iter().sum::<u64>() - post.iter().sum::<u64>() == FEE);
    let balance = |i, owner, raw: u64| {
        json!({"accountIndex":i,"owner":key(owner),"mint":key(MINT),
        "programId":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA","uiTokenAmount":{"amount":raw.to_string(),"decimals":3}})
    };
    ensure!(RAW + 10_000 == 0 + 17_000, "token conservation");
    let message = json!({"header":{"numRequiredSignatures":decoded.binding.required_signatures,
        "numReadonlySignedAccounts":decoded.binding.readonly_signed,"numReadonlyUnsignedAccounts":decoded.binding.readonly_unsigned},
        "accountKeys":accounts.iter().map(|a|json!({"pubkey":key(a.pubkey),"signer":a.is_signer,"writable":a.is_writable})).collect::<Vec<_>>(),
        "recentBlockhash":key([9;32]), "instructions":decoded.instructions.iter().map(|i|json!({
            "programIdIndex":i.program_index,"accounts":i.account_indices,"data":bs58::encode(&i.data).into_string()})).collect::<Vec<_>>()});
    let transfer_data = [vec![12], RAW.to_le_bytes().to_vec(), vec![3]].concat();
    let inner = json!([{"index":2,"instructions":[{"programIdIndex":index(token_program()?)?,
        "accounts":[ta,index(MINT)?,pt,wallet_index],"data":bs58::encode(transfer_data).into_string(),"stackHeight":2}]}]);
    Ok((
        signature.clone(),
        json!({"slot":4242,"blockTime":now,"version":"legacy",
        "transaction":{"signatures":[signature],"message":message},
        "meta":{"err":null,"fee":FEE,"preBalances":pre,"postBalances":post,
            "preTokenBalances":[balance(ta,payer,RAW),balance(pt,POOL,10_000)],
            "postTokenBalances":[balance(ta,payer,0),balance(pt,POOL,17_000)],"innerInstructions":inner,"logMessages":[]}}),
    ))
}
