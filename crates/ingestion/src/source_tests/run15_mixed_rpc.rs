//! Offline RPC structure encoder. Unlike the source-selection adapter this
//! keeps V1 config for transport refusal checks; it never blesses V1 swap facts.
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use yellowstone_grpc_proto::prelude::*;

fn optional_u32(config: &Value, name: &str) -> Result<Option<u32>> {
    match &config[name] {
        Value::Null => Ok(None),
        value => Ok(Some(u32::try_from(
            value.as_u64().context("noninteger V1 config")?,
        )?)),
    }
}

pub(super) fn transaction(raw: &Value, index: u64) -> Result<SubscribeUpdateTransaction> {
    let mut structure = raw.clone();
    let message = &raw["transaction"]["message"];
    let config = message.get("transactionConfig");
    let v1 = raw["version"].as_u64() == Some(1);
    ensure!(!v1 || config.is_some(), "V1 config marker missing");
    ensure!(
        v1 || raw["version"] == "legacy" || raw["version"] == 0,
        "unsupported RPC version"
    );
    // Legacy/V0 and this captured V1 use the same JSON compiled-index fields.
    // Validate those fields with the strict adapter, then restore the actual
    // marker before protobuf encoding. No V1 record reaches its swap replay.
    if v1 {
        structure["version"] = 0.into();
    }
    structure["transaction"]["message"]
        .as_object_mut()
        .context("missing RPC message")?
        .remove("transactionConfig");
    let mut tx = super::source_selection_rpc::update(&structure)?;
    let info = tx.transaction.as_mut().unwrap();
    info.index = index;
    if let Some(config) = config {
        ensure!(config.is_object() || config.is_null(), "invalid V1 config");
        info.transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .config = Some(TransactionConfig {
            priority_fee: match &config["priorityFee"] {
                Value::Null => None,
                value => Some(value.as_u64().context("noninteger priority fee")?),
            },
            compute_unit_limit: optional_u32(config, "computeUnitLimit")?,
            loaded_accounts_data_size_limit: optional_u32(config, "loadedAccountsDataSizeLimit")?,
            heap_size: optional_u32(config, "heapSize")?,
        });
    }
    Ok(tx)
}

pub(super) fn block(record: &Value) -> Result<SubscribeUpdateBlock> {
    let b = &record["result"];
    let slot = record["params"][0]
        .as_u64()
        .context("missing requested slot")?;
    let raw_transactions = b["transactions"].as_array().context("missing block txs")?;
    let transactions = raw_transactions
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let mut value = value.clone();
            value["slot"] = slot.into();
            value["blockTime"] = b["blockTime"].clone();
            Ok(transaction(&value, index as u64)?.transaction.unwrap())
        })
        .collect::<Result<_>>()?;
    Ok(SubscribeUpdateBlock {
        slot,
        blockhash: b["blockhash"].as_str().context("missing blockhash")?.into(),
        parent_slot: b["parentSlot"].as_u64().context("missing parent slot")?,
        parent_blockhash: b["previousBlockhash"]
            .as_str()
            .context("missing parent blockhash")?
            .into(),
        block_time: b["blockTime"]
            .as_i64()
            .map(|timestamp| UnixTimestamp { timestamp }),
        block_height: b["blockHeight"]
            .as_u64()
            .map(|block_height| BlockHeight { block_height }),
        executed_transaction_count: raw_transactions.len() as u64,
        transactions,
        ..Default::default()
    })
}

pub(super) fn envelope(
    update_oneof: subscribe_update::UpdateOneof,
    seconds: i64,
) -> SubscribeUpdate {
    SubscribeUpdate {
        created_at: Some(yellowstone_grpc_proto::prost_types::Timestamp { seconds, nanos: 0 }),
        update_oneof: Some(update_oneof),
        ..Default::default()
    }
}
