//! Explicit synthetic background load, full anchor-sized blocks at real 3.8/s.
//! Foreign signatures prevent saved selected BUYs becoming fabricated fresh trades.
use chrono::{DateTime, Utc};
use futures_util::{stream, Stream};
use std::{pin::Pin, sync::Arc};
use yellowstone_grpc_proto::{prelude::*, prost_types::Timestamp};

pub(super) fn incoming(
    anchor: Arc<SubscribeUpdateBlock>,
    wallet: Vec<u8>,
    started: DateTime<Utc>,
) -> Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, tonic::Status>> + Send>> {
    let clock = tokio::time::Instant::now();
    Box::pin(stream::unfold((0u64, false), move |(index, marker)| {
        let anchor = anchor.clone();
        let wallet = wallet.clone();
        async move {
            let nanos = index * 1_000_000_000 * 10 / 38;
            tokio::time::sleep_until(clock + std::time::Duration::from_nanos(nanos)).await;
            let created = started + chrono::Duration::nanoseconds(nanos as i64);
            let time = Some(Timestamp {
                seconds: created.timestamp(),
                nanos: created.timestamp_subsec_nanos() as i32,
            });
            let oneof = if marker {
                // Valid selected transaction with no instructions/swap: still charged
                // by the SAME Reader, rather than modeled as an uncharged block count.
                let mut signature = vec![249; 64];
                signature[..8].copy_from_slice(&index.to_le_bytes());
                subscribe_update::UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    slot: anchor.slot + index,
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: signature.clone(),
                        transaction: Some(Transaction {
                            signatures: vec![signature],
                            message: Some(Message {
                                header: Some(MessageHeader {
                                    num_required_signatures: 1,
                                    ..Default::default()
                                }),
                                account_keys: vec![wallet],
                                recent_blockhash: vec![248; 32],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        meta: Some(TransactionStatusMeta {
                            pre_balances: vec![1],
                            post_balances: vec![1],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                })
            } else {
                let mut block = (*anchor).clone();
                if index != 0 {
                    block.slot = anchor.slot + index;
                    block.parent_slot = block.slot - 1;
                    block.parent_blockhash = if index == 1 {
                        anchor.blockhash.clone()
                    } else {
                        hash(block.parent_slot)
                    };
                    block.blockhash = hash(block.slot);
                    block.block_time = Some(UnixTimestamp {
                        timestamp: created.timestamp(),
                    });
                    for info in &mut block.transactions {
                        let mut signature = vec![248; 64];
                        signature[..8].copy_from_slice(&index.to_le_bytes());
                        signature[8..16].copy_from_slice(&info.index.to_le_bytes());
                        info.signature = signature.clone();
                        if let Some(tx) = &mut info.transaction {
                            if let Some(first) = tx.signatures.first_mut() {
                                *first = signature;
                            }
                            if let Some(message) = &mut tx.message {
                                let count = message
                                    .header
                                    .as_ref()
                                    .map(|h| h.num_required_signatures)
                                    .unwrap_or(0)
                                    as usize;
                                for key in message.account_keys.iter_mut().take(count) {
                                    *key = vec![248; 32];
                                }
                            }
                        }
                    }
                }
                subscribe_update::UpdateOneof::Block(block)
            };
            let next = if !marker && index > 0 && index % 64 == 0 {
                (index, true)
            } else {
                (index + 1, false)
            };
            Some((
                Ok(SubscribeUpdate {
                    created_at: time,
                    update_oneof: Some(oneof),
                    ..Default::default()
                }),
                next,
            ))
        }
    }))
}
fn hash(slot: u64) -> String {
    let mut bytes = [248; 32];
    bytes[..8].copy_from_slice(&slot.to_le_bytes());
    bs58::encode(bytes).into_string()
}
