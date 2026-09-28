use super::{meta, transaction, value::*};
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use yellowstone_grpc_proto::prelude::*;

pub(crate) fn parse(slot: u64, v: &Value) -> Result<SubscribeUpdateBlock> {
    ensure!(
        v.is_object(),
        "http_recovery_block_unavailable_or_unconfirmed"
    );
    let parent_slot = uint(v, "parentSlot")?;
    ensure!(
        slot > 0 && parent_slot < slot,
        "http_recovery_invalid_parent_slot"
    );
    base58(&v["blockhash"], Some(32))?;
    base58(&v["previousBlockhash"], Some(32))?;
    let rows = array(&v["transactions"])?;
    // The request explicitly asks for rewards. Missing/null block rewards
    // cannot be classified as a proved empty array.
    array(&v["rewards"])?;
    ensure!(
        rows.len() <= super::super::yellowstone_block_association::MAX_BLOCK_TRANSACTIONS,
        "http_recovery_transaction_count_bound"
    );
    let transactions = rows
        .iter()
        .enumerate()
        .map(|(index, row)| transaction::parse(row, index as u64))
        .collect::<Result<Vec<_>>>()?;
    let mut signatures = std::collections::HashSet::new();
    ensure!(
        transactions
            .iter()
            .all(|tx| signatures.insert(tx.signature.clone())),
        "http_recovery_duplicate_signature"
    );
    let block_time = match &v["blockTime"] {
        Value::Null => None,
        n => Some(UnixTimestamp {
            timestamp: n.as_i64().context("http_recovery_block_time")?,
        }),
    };
    let num_partitions = optional_uint(v, "numRewardPartitions")?
        .map(|num_partitions| NumPartitions { num_partitions });
    Ok(SubscribeUpdateBlock {
        slot,
        blockhash: text(v, "blockhash")?.into(),
        parent_slot,
        parent_blockhash: text(v, "previousBlockhash")?.into(),
        rewards: Some(Rewards {
            rewards: meta::rewards(&v["rewards"])?,
            num_partitions,
        }),
        block_time,
        block_height: optional_uint(v, "blockHeight")?
            .map(|block_height| BlockHeight { block_height }),
        executed_transaction_count: transactions.len() as u64,
        transactions,
        ..Default::default()
    })
}
