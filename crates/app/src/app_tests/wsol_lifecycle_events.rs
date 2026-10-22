//! LOCAL protobuf projection of the exact prepared message + modeled CPI receipt.
//! Only the AMM effects/CPI are synthetic; all message bytes/keys/identities are preserved.
use anyhow::{Context, Result};
use prost::Message as _;
use serde_json::Value;
use yellowstone_grpc_proto::prelude::*;
fn info(wire: &str, r: &Value, index: u64) -> Result<SubscribeUpdateTransactionInfo> {
    let m = crate::execution_transaction_wire::decode_message(wire, |_| Ok(()))?;
    let t = &r["result"];
    let meta = &t["meta"];
    let key = |s: &str| -> Result<u32> {
        Ok(m.binding
            .accounts
            .iter()
            .position(|a| bs58::encode(a.pubkey).into_string() == s)
            .context("LOCAL CPI account missing from actual message")? as u32)
    };
    let tokens = |name: &str| -> Result<Vec<TokenBalance>> {
        meta[name]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| {
                Ok(TokenBalance {
                    account_index: b["accountIndex"].as_u64().unwrap() as u32,
                    mint: b["mint"].as_str().unwrap().into(),
                    owner: b["owner"].as_str().unwrap().into(),
                    program_id: b["programId"].as_str().unwrap().into(),
                    ui_token_amount: Some(UiTokenAmount {
                        amount: b["uiTokenAmount"]["amount"].as_str().unwrap().into(),
                        decimals: b["uiTokenAmount"]["decimals"].as_u64().unwrap() as u32,
                        ui_amount: b["uiTokenAmount"]["amount"]
                            .as_str()
                            .unwrap()
                            .parse::<f64>()?
                            / 10f64.powi(b["uiTokenAmount"]["decimals"].as_u64().unwrap() as i32),
                        ui_amount_string: b["uiTokenAmount"]["uiAmountString"]
                            .as_str()
                            .unwrap()
                            .into(),
                    }),
                })
            })
            .collect()
    };
    let mut inner = Vec::new();
    for g in meta["innerInstructions"].as_array().unwrap() {
        let mut instructions = Vec::new();
        for i in g["instructions"].as_array().unwrap() {
            instructions.push(InnerInstruction {
                program_id_index: key(i["programId"].as_str().unwrap())?,
                accounts: i["accounts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|a| Ok(u8::try_from(key(a.as_str().unwrap())?)?))
                    .collect::<Result<Vec<_>>>()?,
                data: bs58::decode(i["data"].as_str().unwrap()).into_vec()?,
                stack_height: Some(i["stackHeight"].as_u64().unwrap() as u32),
            });
        }
        inner.push(InnerInstructions {
            index: g["index"].as_u64().unwrap() as u32,
            instructions,
        });
    }
    let signature = bs58::decode(t["transaction"]["signatures"][0].as_str().unwrap()).into_vec()?;
    let message = Message {
        header: Some(MessageHeader {
            num_required_signatures: u32::try_from(m.binding.required_signatures)?,
            num_readonly_signed_accounts: m
                .binding
                .accounts
                .iter()
                .filter(|a| a.is_signer && !a.is_writable)
                .count() as u32,
            num_readonly_unsigned_accounts: m
                .binding
                .accounts
                .iter()
                .filter(|a| !a.is_signer && !a.is_writable)
                .count() as u32,
        }),
        account_keys: m
            .binding
            .accounts
            .iter()
            .map(|a| a.pubkey.to_vec())
            .collect(),
        recent_blockhash: m.recent_blockhash.to_vec(),
        instructions: m
            .instructions
            .iter()
            .map(|i| {
                Ok(CompiledInstruction {
                    program_id_index: key(&bs58::encode(i.program.pubkey).into_string())?,
                    accounts: i
                        .accounts
                        .iter()
                        .map(|a| Ok(u8::try_from(key(&bs58::encode(a.pubkey).into_string())?)?))
                        .collect::<Result<Vec<_>>>()?,
                    data: i.data.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?,
        versioned: false,
        ..Default::default()
    };
    Ok(SubscribeUpdateTransactionInfo {
        signature: signature.clone(),
        is_vote: false,
        transaction: Some(Transaction {
            signatures: vec![signature],
            message: Some(message),
        }),
        meta: Some(TransactionStatusMeta {
            fee: meta["fee"].as_u64().unwrap(),
            pre_balances: meta["preBalances"]
                .as_array()
                .unwrap()
                .iter()
                .map(|x| x.as_u64().unwrap())
                .collect(),
            post_balances: meta["postBalances"]
                .as_array()
                .unwrap()
                .iter()
                .map(|x| x.as_u64().unwrap())
                .collect(),
            pre_token_balances: tokens("preTokenBalances")?,
            post_token_balances: tokens("postTokenBalances")?,
            inner_instructions: inner,
            ..Default::default()
        }),
        index,
        ..Default::default()
    })
}
pub(super) fn replace(raw: Vec<u8>, wire: &str, r: &Value) -> Result<Vec<u8>> {
    let mut u = SubscribeUpdate::decode(raw.as_slice())?;
    let sig = bs58::decode(
        r["result"]["transaction"]["signatures"][0]
            .as_str()
            .unwrap(),
    )
    .into_vec()?;
    match u.update_oneof.as_mut().unwrap() {
        subscribe_update::UpdateOneof::Transaction(t) => {
            if t.transaction.as_ref().unwrap().signature == sig {
                let index = t.transaction.as_ref().unwrap().index;
                t.transaction = Some(info(wire, r, index)?);
                assert_eq!(t.slot, r["result"]["slot"].as_u64().unwrap());
            }
        }
        subscribe_update::UpdateOneof::Block(b) => {
            for t in &mut b.transactions {
                if t.signature == sig {
                    *t = info(wire, r, t.index)?;
                    assert_eq!(b.slot, r["result"]["slot"].as_u64().unwrap());
                }
            }
        }
        _ => {}
    }
    Ok(u.encode_to_vec())
}

pub(super) fn protocols(meta: &mut Value) {
    for key in ["programs", "raydium"] {
        meta[key]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!(crate::execution_owner_buy_wire::RAYDIUM));
    }
    meta["programs"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!(crate::execution_owner_buy_wire::JUPITER));
}
