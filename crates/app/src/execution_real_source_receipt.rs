//! Finalized normal SOURCE wire is bound to observed admission and strict AMM facts.
#[path="execution_real_source_binding.rs"]
pub(crate) mod binding;
use crate::execution_owned_sell_rpc::fractional::transport::Transport;
use anyhow::{ensure,Context,Result};
use base64::{engine::general_purpose::STANDARD,Engine};
use copybot_config::ExecutionConfig;
use copybot_storage_core::SqliteStore;
use serde_json::{json,Value};
pub(crate) fn record(s:&SqliteStore,c:&ExecutionConfig,signature:&str,slot:u64,side:&str,parsed:&Value,v:&Value)->Result<()>{
    copybot_config::validate_real_session(c)?;
    let p=c.real_session.as_ref().context("real_session_missing")?;
    let event=s.real_source_event(signature)?;
    ensure!(event.facts.slot==slot && event.facts.wallet==p.source_wallet && v["slot"].as_u64()==Some(slot)
        && v.pointer("/meta/err")==Some(&Value::Null) && v["transaction"][1]=="base64","real_session_finalized_identity");
    let payload=v["transaction"][0].as_str().context("real_session_source_wire")?;
    let raw=STANDARD.decode(payload)?;
    copybot_ingestion::verify_source_legacy_wire(&event.info,&raw)?;
    let (message,fee)=crate::execution_priority_fee_wire::decode_priority_fee_message(payload)?;
    binding::verify(parsed,&message)?;
    ensure!(raw[0]==1 && bs58::encode(&raw[1..65]).into_string()==signature
        && bs58::encode(message.binding.accounts[0].pubkey).into_string()==p.source_wallet,"real_session_source_signature");
    let key=ed25519_dalek::VerifyingKey::from_bytes(&message.binding.accounts[0].pubkey)?;
    key.verify_strict(&message.binding.message_bytes,&ed25519_dalek::Signature::from_slice(&raw[1..65])?)?;
    let amount=if side=="buy" {crate::execution_owned_sell_rpc::decode::source_buy(parsed,&event)?}
        else {crate::execution_owned_sell_rpc::decode::sell(parsed,&event)?;
            event.facts.exact_amounts.as_ref().context("real_session_source_exact")?.amount_in_raw.parse::<u64>()?};
    let keys=message.binding.accounts.iter().map(|a|bs58::encode(a.pubkey).into_string()).collect::<Vec<_>>();
    let delta=crate::execution_controlled_receipt_lot::bounded::receipt::token_delta(v,&keys,&p.source_wallet)?;
    ensure!(delta==if side=="buy" {i128::from(amount)}else{-i128::from(amount)},"real_session_source_delta");
    s.record_real_source_receipt(signature,side,slot,&fee.message_sha256,&fee.transaction_sha256,
        &crate::execution_owned_sell_rpc::digest(serde_json::to_vec(v)?),amount,chrono::Utc::now())
}
pub(crate) async fn buy<T:Transport+?Sized>(s:&SqliteStore,c:&ExecutionConfig,rpc:&mut T,signature:&str,slot:u64,check:&mut(impl FnMut()->Result<()>+Send))->Result<()>{
    if c.real_session.is_none(){return Ok(())}
    let mut values=vec![];
    for encoding in ["json","base64"] {
        let request=json!({"jsonrpc":"2.0","id":"real-session-source-finalized","method":"getTransaction","params":[signature,{"encoding":encoding,"commitment":"finalized","maxSupportedTransactionVersion":0}]});
        let response=rpc.read(request.clone(),check).await?;check()?;
        ensure!(response["jsonrpc"]=="2.0" && response["id"]==request["id"] && response.get("error").is_none(),"real_session_source_rpc");
        values.push(response["result"].clone());
    }
    record(s,c,signature,slot,"buy",&values[0],&values[1])
}
/// A fresh session cannot adopt a wallet's historical USDC inventory as a new lot.
pub(crate) async fn entry_inventory<T:Transport+?Sized>(c:&ExecutionConfig,rpc:&mut T,source_slot:u64,check:&mut(impl FnMut()->Result<()>+Send))->Result<()>{
    let Some(p)=&c.real_session else{return Ok(())};
    let request=json!({"jsonrpc":"2.0","id":"real-session-entry-inventory","method":"getTokenAccountsByOwner","params":[p.bot_wallet,{"mint":p.mint},{"encoding":"jsonParsed","commitment":"finalized","minContextSlot":source_slot}]});
    let response=rpc.read(request.clone(),check).await?;check()?;
    ensure!(response["jsonrpc"]=="2.0" && response["id"]==request["id"] && response.get("error").is_none(),"real_session_entry_rpc");
    ensure!(response["result"]["context"]["slot"].as_u64().is_some_and(|s|s>=source_slot),"real_session_entry_context");
    let canonical=crate::execution_controlled_receipt_lot::canonical(c)?;
    let rows=response["result"]["value"].as_array().context("real_session_entry_inventory")?;
    ensure!(rows.len()==1 && rows[0]["pubkey"]==canonical,"real_session_entry_account");
    let row=&rows[0];let info=&row["account"]["data"]["parsed"]["info"];
    ensure!(row["account"]["owner"]=="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        && info["owner"]==p.bot_wallet && info["mint"]==p.mint && info["tokenAmount"]["decimals"]==6
        && info["tokenAmount"]["amount"]==p.personal_usdc_raw.to_string(),"real_session_prior_inventory_obligation");
    Ok(())
}
