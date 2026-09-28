//! Saved source protobuf plus explicitly modeled parent headers/follower frame.
#[path = "../../../ingestion/src/source_tests/run15_mixed_rpc.rs"]
mod run15_mixed_rpc;
#[path = "../../../ingestion/src/source_tests/source_selection_rpc.rs"]
mod source_selection_rpc;
use anyhow::{Context, Result};
use prost::Message;
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::*;

pub(super) const SOURCE_SLOT: u64 = 451302689;
pub(super) const BOT_SLOT: u64 = 451313057;
pub(super) const SELL_SLOT: u64 = 451313058;
pub(super) fn hash(slot: u64) -> String {
    let mut bytes = [63; 32];
    bytes[..8].copy_from_slice(&slot.to_le_bytes());
    bs58::encode(bytes).into_string()
}
fn envelope(one: subscribe_update::UpdateOneof) -> Vec<u8> {
    run15_mixed_rpc::envelope(one, chrono::Utc::now().timestamp()).encode_to_vec()
}
pub(super) fn block(
    slot: u64,
    hash: String,
    parent: String,
    transactions: Vec<SubscribeUpdateTransactionInfo>,
    count: u64,
) -> Vec<u8> {
    envelope(subscribe_update::UpdateOneof::Block(SubscribeUpdateBlock {
        slot,
        blockhash: hash,
        parent_slot: slot - 1,
        parent_blockhash: parent,
        block_time: Some(UnixTimestamp {
            timestamp: chrono::Utc::now().timestamp(),
        }),
        executed_transaction_count: count,
        transactions,
        ..Default::default()
    }))
}
pub(super) fn source_buy(path: &std::path::Path) -> Result<(Vec<u8>, Vec<u8>)> {
    let wire = std::fs::read(path.join("source-buy727.pb"))?;
    let update = SubscribeUpdate::decode(wire.as_slice())?;
    let Some(subscribe_update::UpdateOneof::Transaction(t)) = update.update_oneof else {
        anyhow::bail!("source BUY frame type");
    };
    let info = t.transaction.context("source BUY info")?;
    let header = block(
        SOURCE_SLOT,
        hash(SOURCE_SLOT),
        hash(SOURCE_SLOT - 1),
        vec![info],
        696,
    );
    Ok((wire, header))
}
pub(super) fn follower(raw: &Value, parent: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut raw = raw.clone();
    raw["transaction"]["message"]["recentBlockhash"] = json!(bs58::encode([9; 32]).into_string());
    let keys = raw["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .clone();
    // The model's parsed initializeAccount3 is represented by its actual SPL ABI
    // on the protobuf boundary. This changes only the model, never saved source.
    let groups = raw["meta"]["innerInstructions"].as_array_mut().unwrap();
    for group in groups {
        for ix in group["instructions"].as_array_mut().unwrap() {
            if ix.get("parsed").is_none() {
                continue;
            }
            let info = &ix["parsed"]["info"];
            let at = |name: &str| keys.iter().position(|k| k == &info[name]).unwrap();
            let owner = bs58::decode(info["owner"].as_str().unwrap()).into_vec()?;
            let mut data = vec![18];
            data.extend(owner);
            *ix = json!({"programIdIndex":ix["programIdIndex"],"accounts":[at("account"),at("mint")],
                "data":bs58::encode(data).into_string(),"stackHeight":2});
        }
    }
    let tx = run15_mixed_rpc::transaction(&raw, 0)?;
    let info = tx.transaction.clone().unwrap();
    Ok((
        envelope(subscribe_update::UpdateOneof::Transaction(tx)),
        block(BOT_SLOT, parent.into(), hash(BOT_SLOT - 1), vec![info], 1),
    ))
}
pub(super) fn mixed(
    path: &std::path::Path,
) -> Result<(Vec<Vec<u8>>, Vec<u8>, Vec<Vec<u8>>, String)> {
    let full = std::fs::read(path.join("sell-block725.pb"))?;
    let decoded = SubscribeUpdate::decode(full.as_slice())?;
    let Some(subscribe_update::UpdateOneof::Block(b)) = decoded.update_oneof else {
        anyhow::bail!("mixed block type");
    };
    let tx = |info: &SubscribeUpdateTransactionInfo| {
        envelope(subscribe_update::UpdateOneof::Transaction(
            SubscribeUpdateTransaction {
                slot: b.slot,
                transaction: Some(info.clone()),
            },
        ))
    };
    let config = |t: &&SubscribeUpdateTransactionInfo| {
        t.transaction
            .as_ref()
            .unwrap()
            .message
            .as_ref()
            .unwrap()
            .config
            .is_some()
    };
    let before = b.transactions[..775]
        .iter()
        .filter(config)
        .map(tx)
        .collect();
    let after = b.transactions[776..]
        .iter()
        .filter(config)
        .map(tx)
        .collect();
    Ok((before, full, after, b.parent_blockhash))
}

// Logical replay cadence expires cache normally; no wall-clock market claim.
pub(super) fn is_block(payload: &[u8]) -> bool {
    matches!(
        SubscribeUpdate::decode(payload).unwrap().update_oneof,
        Some(subscribe_update::UpdateOneof::Block(_))
    )
}
