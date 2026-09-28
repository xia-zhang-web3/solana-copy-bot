use super::{capture_scope_fixture as fixture, run15_mixed_rpc as rpc};
use crate::{source::durable::DeliveryReceiver, ReplayInput};
use anyhow::{Context, Result};
use copybot_core_types::association_delivery::{Delivery, DeliveryEvent, SessionGap, Terminal};
use prost::Message as _;
use serde_json::{json, Value};
use std::{collections::HashSet, path::Path};
use yellowstone_grpc_proto::prelude::*;

fn tx(update: &SubscribeUpdate) -> &SubscribeUpdateTransaction {
    match update.update_oneof.as_ref().unwrap() {
        subscribe_update::UpdateOneof::Transaction(tx) => tx,
        _ => panic!("transaction fixture"),
    }
}
fn config_tx(update: &SubscribeUpdate, seed: u8, versioned: bool) -> SubscribeUpdate {
    let mut update = update.clone();
    let subscribe_update::UpdateOneof::Transaction(tx) = update.update_oneof.as_mut().unwrap()
    else {
        panic!("transaction fixture")
    };
    let info = tx.transaction.as_mut().unwrap();
    info.signature = vec![seed; 64];
    let transaction = info.transaction.as_mut().unwrap();
    transaction.signatures[0] = info.signature.clone();
    let message = transaction.message.as_mut().unwrap();
    message.versioned = versioned;
    message.config = Some(TransactionConfig::default());
    update
}
fn input(ns: u64, update: SubscribeUpdate) -> ReplayInput {
    ReplayInput::Update {
        offset_ns: ns,
        payload: update.encode_to_vec(),
    }
}
fn policy() -> copybot_config::IngestionConfig {
    use copybot_config::{AssociationDeliveryConfig, DeliveryBudget};
    let b = |count, bytes| DeliveryBudget { count, bytes };
    let mut c = copybot_config::IngestionConfig::default();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    c.yellowstone_x_token = "offline-fixture".into();
    c.yellowstone_delivery_mode = "durable_association_v1".into();
    c.yellowstone_association = Some(AssociationDeliveryConfig {
        pending: b(1024, 16 << 20),
        blocks: b(256, 256 << 20),
        history: b(2048, 32 << 20),
        outputs: b(17, 4 << 20),
        queue: b(4, 8 << 20),
        inbox: b(20000, 128 << 20),
        input_bytes: 8 << 20,
        metadata_bytes: 32 << 20,
        pending_ttl_ms: 60_000,
        block_ttl_ms: 60_000,
        history_ttl_ms: 120_000,
        tick_ms: 1000,
        sqlite_busy_ms: 100,
    });
    c
}
async fn replay(
    config: copybot_config::IngestionConfig,
    inputs: Vec<ReplayInput>,
    scope: Option<HashSet<String>>,
) -> Result<(Vec<Delivery>, crate::DurableIngressSnapshot)> {
    let (sender, input) = tokio::sync::mpsc::channel(2);
    let mut receiver = DeliveryReceiver::replay(&config, "mixed-v1".into(), input, scope, None)?;
    let producer = tokio::spawn(async move {
        for input in inputs {
            sender.send(input).await.unwrap();
        }
    });
    let mut deliveries = vec![];
    while let Some(envelope) = receiver.next().await? {
        deliveries.push(envelope.delivery.clone());
    }
    producer.await?;
    Ok((deliveries, receiver.ingress_snapshot()))
}

#[test]
fn proto_preserves_config_and_decoder_refuses_it_independently_of_versioned() {
    let original = fixture::update(&fixture::fixtures()[0]);
    let runtime = fixture::runtime(None);
    let decode = |update: &SubscribeUpdate| {
        crate::source::yellowstone_facts::decode_yellowstone_swap_facts(
            tx(update),
            &runtime.interested_program_ids,
            &runtime.raydium_program_ids,
            &runtime.pumpswap_program_ids,
        )
    };
    assert!(decode(&original).facts.unwrap().is_some());
    for versioned in [false, true] {
        let update = config_tx(&original, 55, versioned);
        let decoded = SubscribeUpdate::decode(update.encode_to_vec().as_slice()).unwrap();
        assert!(tx(&decoded)
            .transaction
            .as_ref()
            .unwrap()
            .transaction
            .as_ref()
            .unwrap()
            .message
            .as_ref()
            .unwrap()
            .config
            .is_some());
        let refusal = decode(&decoded);
        assert!(refusal.facts.unwrap().is_none());
        assert_eq!(
            refusal.miss,
            Some(crate::source::yellowstone_facts::DecodeMiss::UnsupportedMessageConfig)
        );
    }
}

#[tokio::test]
async fn mixed_protobuf_keeps_parent_index_ping_reset_end_without_v1_admissions() -> Result<()> {
    let original = fixture::update(&fixture::fixtures()[0]);
    let mut target = tx(&original).clone();
    target.transaction.as_mut().unwrap().index = 2;
    let target = rpc::envelope(subscribe_update::UpdateOneof::Transaction(target), 1);
    let before = config_tx(&target, 71, false);
    let after = config_tx(&target, 72, true);
    let mut oversized_info = before.clone();
    let subscribe_update::UpdateOneof::Transaction(t) =
        oversized_info.update_oneof.as_mut().unwrap()
    else {
        unreachable!()
    };
    // A legacy info cardinality error must not turn unrelated V1 into SessionGap.
    t.transaction
        .as_mut()
        .unwrap()
        .meta
        .as_mut()
        .unwrap()
        .log_messages = vec![String::new(); 257];
    let block = SubscribeUpdateBlock {
        slot: tx(&target).slot,
        blockhash: bs58::encode([41; 32]).into_string(),
        parent_slot: tx(&target).slot - 1,
        parent_blockhash: bs58::encode([40; 32]).into_string(),
        transactions: vec![
            tx(&before).transaction.clone().unwrap(),
            tx(&after).transaction.clone().unwrap(),
            tx(&target).transaction.clone().unwrap(),
        ],
        ..Default::default()
    };
    let encoded =
        rpc::envelope(subscribe_update::UpdateOneof::Block(block.clone()), 1).encode_to_vec();
    let preserved = SubscribeUpdate::decode(encoded.as_slice())?;
    let subscribe_update::UpdateOneof::Block(preserved) = preserved.update_oneof.unwrap() else {
        unreachable!()
    };
    assert_eq!(preserved, block);
    let (deliveries, telemetry) = replay(
        policy(),
        vec![
            input(1, oversized_info),
            input(
                2,
                rpc::envelope(
                    subscribe_update::UpdateOneof::Ping(SubscribeUpdatePing {}),
                    1,
                ),
            ),
            input(3, target),
            input(
                4,
                rpc::envelope(subscribe_update::UpdateOneof::Block(block.clone()), 1),
            ),
            input(5, after),
            ReplayInput::Tick(6),
            ReplayInput::Reset(7),
            ReplayInput::End(8),
        ],
        None,
    )
    .await?;
    assert_eq!(telemetry.admissions, 1);
    assert_eq!(telemetry.admission_rejections, 0);
    assert_eq!(
        telemetry.decode_misses
            [crate::source::yellowstone_facts::DecodeMiss::UnsupportedMessageConfig as usize],
        2
    );
    assert!(deliveries
        .windows(2)
        .all(|w| w[1].sequence == w[0].sequence + 1));
    assert!(deliveries.iter().any(|d| matches!(&d.event,
        DeliveryEvent::Parent(p) if p.child.slot == block.slot && p.parent.slot == block.parent_slot)));
    assert!(deliveries.iter().any(|d| matches!(&d.event,
        DeliveryEvent::Terminal { result: Terminal::ProviderAsserted(a), .. }
            if a.transaction_index == 2)));
    for gap in [SessionGap::Reset, SessionGap::End] {
        assert!(deliveries
            .iter()
            .any(|d| d.event == DeliveryEvent::Session(gap.clone())));
    }
    assert!(!deliveries
        .iter()
        .any(|d| matches!(d.event, DeliveryEvent::Session(SessionGap::Rejected(_)))));
    Ok(())
}

#[tokio::test]
async fn config_on_retained_signature_preserves_provider_conflict_disposition() -> Result<()> {
    use copybot_core_types::association_delivery::Unresolved;
    let original = fixture::update(&fixture::fixtures()[0]);
    let mut conflicting = original.clone();
    let subscribe_update::UpdateOneof::Transaction(tx) = conflicting.update_oneof.as_mut().unwrap()
    else {
        unreachable!()
    };
    tx.transaction
        .as_mut()
        .unwrap()
        .transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .config = Some(TransactionConfig::default());
    let (deliveries, telemetry) = replay(
        policy(),
        vec![
            input(1, original),
            input(2, conflicting),
            ReplayInput::End(3),
        ],
        None,
    )
    .await?;
    assert_eq!(telemetry.admissions, 1);
    assert_eq!(telemetry.duplicates, 1);
    assert!(deliveries.iter().any(|d| matches!(
        &d.event,
        DeliveryEvent::Terminal {
            result: Terminal::Unresolved(Unresolved::ConflictingTransaction),
            ..
        }
    )));
    Ok(())
}

fn read(path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}
fn save_frame(directory: &Path, name: &str, update: SubscribeUpdate) -> Result<Value> {
    use sha2::{Digest, Sha256};
    let bytes = update.encode_to_vec();
    let path = directory.join(name);
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)?;
    use std::io::Write;
    file.write_all(&bytes)?;
    Ok(
        json!({"file": name, "sha256": format!("{:x}", Sha256::digest(&bytes)), "bytes": bytes.len()}),
    )
}

/// Private corpus path is supplied by the authorized offline batch. This is an
/// RPC reconstruction through the real protobuf bridge, not a Geyser capture.
#[tokio::test]
#[ignore = "requires saved read-only corpus and explicit output directory"]
async fn saved_raydium_mixed_block_replays_and_exports_full_frames() -> Result<()> {
    let corpus = std::path::PathBuf::from(std::env::var("COPYBOT_RUN15_CORPUS_DIR")?);
    let output = std::path::PathBuf::from(std::env::var("COPYBOT_RUN15_FRAMES_DIR")?);
    std::fs::create_dir(&output)?;
    let raw_buy = read(&corpus.join("response-727.json"))?;
    let buy = rpc::transaction(
        &raw_buy["result"],
        raw_buy["result"]["transactionIndex"]
            .as_u64()
            .context("BUY index")?,
    )?;
    let raw_sell_block = read(&corpus.join("response-725.json"))?;
    let block = rpc::block(&raw_sell_block)?;
    let sell = block.transactions[775].clone();
    assert_eq!(
        bs58::encode(&sell.signature).into_string(),
        "5qUFxPr8EZusxeKyDUWeDvRtkfJtjiYNyNqAQBxC442SvDFQsG6cebbrZvkf9aeTmiMgtBULYGBsagaXyDmi6oD4"
    );
    assert_eq!(block.slot, 451313058);
    let unsupported = |i: &SubscribeUpdateTransactionInfo| {
        crate::source::yellowstone_facts::unsupported_message_config(i)
    };
    assert_eq!(
        block.transactions[..775]
            .iter()
            .filter(|i| unsupported(i))
            .count(),
        133
    );
    let seconds = block.block_time.as_ref().unwrap().timestamp;
    let mut inputs = vec![];
    for info in &block.transactions[..775] {
        if unsupported(info) {
            inputs.push(input(
                inputs.len() as u64 + 1,
                rpc::envelope(
                    subscribe_update::UpdateOneof::Transaction(SubscribeUpdateTransaction {
                        slot: block.slot,
                        transaction: Some(info.clone()),
                    }),
                    seconds,
                ),
            ));
        }
    }
    let sell_tx = SubscribeUpdateTransaction {
        slot: block.slot,
        transaction: Some(sell),
    };
    inputs.push(input(
        200,
        rpc::envelope(
            subscribe_update::UpdateOneof::Transaction(sell_tx.clone()),
            seconds,
        ),
    ));
    inputs.push(input(
        201,
        rpc::envelope(subscribe_update::UpdateOneof::Block(block.clone()), seconds),
    ));
    for (n, info) in block.transactions[776..]
        .iter()
        .filter(|i| unsupported(i))
        .enumerate()
    {
        inputs.push(input(
            202 + n as u64,
            rpc::envelope(
                subscribe_update::UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    slot: block.slot,
                    transaction: Some(info.clone()),
                }),
                seconds,
            ),
        ));
    }
    inputs.push(ReplayInput::Reset(300));
    inputs.push(ReplayInput::End(301));
    let config =
        copybot_config::load_from_path(std::env::var("COPYBOT_RUN15_RUNTIME_CONFIG")?)?.ingestion;
    let wallet = "7EoQc9N9QrGMf6JR2j8yy4rmY1ZsexPuAXoTCQtDDbSF".to_owned();
    let (deliveries, telemetry) = replay(config, inputs, Some(HashSet::from([wallet]))).await?;
    assert_eq!(telemetry.admissions, 1);
    assert_eq!(telemetry.admission_rejections, 0);
    assert_eq!(
        telemetry.decode_misses
            [crate::source::yellowstone_facts::DecodeMiss::UnsupportedMessageConfig as usize],
        143
    );
    assert!(deliveries.iter().any(|d| matches!(&d.event,
        DeliveryEvent::Terminal { result: Terminal::ProviderAsserted(a), .. }
            if a.transaction_index == 775 && a.slot == 451313058)));
    let mut frames = vec![
        save_frame(
            &output,
            "source-buy727.pb",
            rpc::envelope(
                subscribe_update::UpdateOneof::Transaction(buy),
                raw_buy["result"]["blockTime"].as_i64().unwrap(),
            ),
        )?,
        save_frame(
            &output,
            "source-sell725.pb",
            rpc::envelope(subscribe_update::UpdateOneof::Transaction(sell_tx), seconds),
        )?,
        save_frame(
            &output,
            "sell-block725.pb",
            rpc::envelope(subscribe_update::UpdateOneof::Block(block), seconds),
        )?,
    ];
    // Real captured successor chain, preserving each full mixed block and index.
    for sequence in (693..725).rev() {
        let raw = read(&corpus.join(format!("response-{sequence}.json")))?;
        if raw["method"] != "getBlock" {
            continue;
        }
        let b = rpc::block(&raw)?;
        let time = b.block_time.as_ref().unwrap().timestamp;
        frames.push(save_frame(
            &output,
            &format!("block-{sequence}.pb"),
            rpc::envelope(subscribe_update::UpdateOneof::Block(b), time),
        )?);
    }
    std::fs::write(
        output.join("MANIFEST.json"),
        serde_json::to_vec_pretty(&json!({
            "evidence": "RPC-reconstructed protobuf; not live transport continuity",
            "source_buy_fixture": "real historical source, not follower receipt",
            "source_sell_slot": 451313058, "source_sell_index": 775,
            "prefix_config_transactions": 133, "whole_block_config_transactions": 143,
            "v1_admissions": 0, "selected_admissions": telemetry.admissions,
            "frames": frames, "deliveries": deliveries,
        }))?,
    )?;
    Ok(())
}
