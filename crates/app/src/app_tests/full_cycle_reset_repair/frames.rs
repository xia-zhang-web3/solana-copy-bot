//! Saved headers/selected Info; unrelated full-block entries are explicit LOCAL vote operands.
use anyhow::{Context, Result};
use copybot_core_types::association_delivery::AdmissionFacts;
use prost::Message;
use serde_json::Value;
use yellowstone_grpc_proto::prelude::*;

pub(super) fn transaction(a: &AdmissionFacts) -> Result<SubscribeUpdateTransactionInfo> {
    let mut info = SubscribeUpdateTransactionInfo::decode(a.info.encoded.as_slice())?;
    let meta = info.meta.as_mut().context("saved metadata")?;
    for (row, bits) in meta
        .pre_token_balances
        .iter_mut()
        .chain(&mut meta.post_token_balances)
        .zip(&a.info.float_bits)
    {
        match (&mut row.ui_token_amount, bits) {
            (Some(amount), Some(bits)) => amount.ui_amount = f64::from_bits(*bits),
            (None, None) => {}
            _ => anyhow::bail!("saved float optionality"),
        }
    }
    Ok(info)
}
fn envelope(one: subscribe_update::UpdateOneof) -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(one),
        ..Default::default()
    }
}
pub(super) fn admission(a: &AdmissionFacts) -> Result<Vec<u8>> {
    let mut update = envelope(subscribe_update::UpdateOneof::Transaction(
        SubscribeUpdateTransaction {
            slot: a.facts.slot,
            transaction: Some(transaction(a)?),
            ..Default::default()
        },
    ));
    if let copybot_core_types::association_delivery::MessageTime::CreatedAt { seconds, nanos } =
        a.message_time
    {
        update.created_at = Some(yellowstone_grpc_proto::prost_types::Timestamp {
            seconds,
            nanos: nanos as i32,
        });
    }
    Ok(update.encode_to_vec())
}
pub(super) fn block(value: &Value, a: &AdmissionFacts) -> Result<Vec<u8>> {
    let observation = &value["observation"];
    let count = value["executed_transaction_count"]
        .as_u64()
        .context("saved block count")?;
    let slot = observation["child"]["slot"]
        .as_u64()
        .context("saved block slot")?;
    let mut transactions: Vec<_> = (0..count)
        .map(|index| SubscribeUpdateTransactionInfo {
            index,
            is_vote: true,
            signature: {
                let mut v = vec![17; 64];
                v[..8].copy_from_slice(&index.to_le_bytes());
                v
            },
            ..Default::default()
        })
        .collect();
    if slot == a.facts.slot {
        let source = transaction(a)?;
        let index = usize::try_from(source.index)?;
        anyhow::ensure!(index < transactions.len(), "saved index");
        transactions[index] = source;
    }
    Ok(
        envelope(subscribe_update::UpdateOneof::Block(SubscribeUpdateBlock {
            slot,
            blockhash: observation["child"]["hash"]
                .as_str()
                .context("child hash")?
                .into(),
            parent_slot: observation["parent"]["slot"]
                .as_u64()
                .context("parent slot")?,
            parent_blockhash: observation["parent"]["hash"]
                .as_str()
                .context("parent hash")?
                .into(),
            executed_transaction_count: count,
            transactions,
            block_time: (slot == a.facts.slot).then_some(UnixTimestamp {
                timestamp: 1793800517,
            }),
            ..Default::default()
        }))
        .encode_to_vec(),
    )
}
