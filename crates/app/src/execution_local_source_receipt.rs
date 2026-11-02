//! Bind finalized RPC bytes to the exact external SOURCE plan; no financial writes.
use crate::execution_owned_sell_rpc::fractional::transport::Transport;
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use copybot_config::ExecutionConfig;
use copybot_storage_core::SqliteStore;
use serde_json::{json, Value};
pub(crate) fn token_delta(v: &Value, keys: &[String], wallet: &str) -> Result<i128> {
    use crate::execution_pumpswap_accounts::*;
    let mint = copybot_storage_core::OWNER_EXIT_MINT;
    let account = bs58::encode(associated_token_address(
        &parse_pubkey(wallet, "local_source_wallet")?,
        &parse_pubkey(mint, "local_source_mint")?,
        &token_program_id(),
    ))
    .into_string();
    let keys = keys.iter().map(String::as_str).collect::<Vec<_>>();
    crate::execution_owned_sell_rpc::decode::delta(v, &keys, wallet, &account, mint, 6)
}

pub(crate) fn record(
    s: &SqliteStore,
    c: &ExecutionConfig,
    signature: &str,
    slot: u64,
    side: &str,
    v: &Value,
) -> Result<()> {
    copybot_config::validate_local_bounded_session(c)?;
    let model = c
        .local_bounded_session
        .as_ref()
        .context("local_source_config")?;
    let source = c
        .controlled_receipt_lot
        .as_ref()
        .context("local_source_scope")?;
    let p = model
        .cycles
        .iter()
        .find(|p| {
            if side == "buy" {
                p.buy_signature == signature
            } else {
                p.sell_signature == signature
            }
        })
        .context("local_source_signature")?;
    let expected = if side == "buy" {
        &p.buy_message_sha256
    } else {
        &p.sell_message_sha256
    };
    ensure!(
        v["slot"].as_u64() == Some(slot)
            && v.pointer("/meta/err") == Some(&Value::Null)
            && v["transaction"][1] == "base64",
        "local_source_finalized_receipt"
    );
    let payload = v["transaction"][0].as_str().context("local_source_wire")?;
    let (message, fee) = crate::execution_priority_fee_wire::decode_priority_fee_message(payload)?;
    let raw = STANDARD.decode(payload)?;
    ensure!(
        raw.len() >= 65
            && raw[0] == 1
            && message.binding.signature_count == 1
            && message.binding.required_signatures == 1
            && bs58::encode(&raw[1..65]).into_string() == signature
            && bs58::encode(message.binding.accounts[0].pubkey).into_string()
                == source.source_wallet
            && fee.message_sha256 == *expected,
        "local_source_message_binding"
    );
    let keys = message
        .binding
        .accounts
        .iter()
        .map(|a| bs58::encode(a.pubkey).into_string())
        .collect::<Vec<_>>();
    let delta = token_delta(v, &keys, &source.source_wallet)?;
    ensure!(
        delta
            == if side == "buy" {
                i128::from(p.acquired_raw)
            } else {
                -i128::from(p.sold_raw)
            },
        "local_source_receipt_delta"
    );
    s.record_local_source_receipt(
        signature,
        side,
        slot,
        expected,
        &fee.transaction_sha256,
        &crate::execution_owned_sell_rpc::digest(serde_json::to_vec(v)?),
        chrono::Utc::now(),
    )
}
pub(crate) async fn buy<T: Transport + ?Sized>(
    s: &SqliteStore,
    c: &ExecutionConfig,
    rpc: &mut T,
    signature: &str,
    slot: u64,
    check: &mut (impl FnMut() -> Result<()> + Send),
) -> Result<()> {
    if c.local_bounded_session.is_none() {
        return Ok(());
    }
    let request = json!({"jsonrpc":"2.0","id":"local-source-finalized-wire","method":"getTransaction","params":[signature,{"encoding":"base64","commitment":"finalized","maxSupportedTransactionVersion":0}]});
    let response = rpc.read(request.clone(), check).await?;
    check()?;
    ensure!(
        response["jsonrpc"] == "2.0"
            && response["id"] == request["id"]
            && response.get("error").is_none(),
        "local_source_rpc_binding"
    );
    record(s, c, signature, slot, "buy", &response["result"])
}
