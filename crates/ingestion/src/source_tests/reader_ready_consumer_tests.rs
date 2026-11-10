//! Finite scheduler bursts through the actual Reader and empty-scope Bridge.
//! This proves ready-consumer fairness, not recovery of a stalled consumer.
use super::*;
use crate::source::durable::{bridge::Bridge, queue, recovery::RecoveryCursor};
use copybot_config::{AssociationDeliveryConfig, DeliveryBudget, IngestionConfig};
use copybot_core_types::{association_delivery::DeliveryEvent, association_recovery::ReplayScope};
use std::{collections::HashSet, time::Duration};
use yellowstone_grpc_proto::prost_types::Timestamp;

fn limits() -> AssociationDeliveryConfig {
    let b = |count, bytes| DeliveryBudget { count, bytes };
    AssociationDeliveryConfig {
        pending: b(64, 8 << 20),
        blocks: b(64, 8 << 20),
        history: b(64, 8 << 20),
        outputs: b(64, 8 << 20),
        queue: b(64, 8 << 20),
        inbox: b(1024, 16 << 20),
        input_bytes: 1 << 20,
        metadata_bytes: 1 << 20,
        pending_ttl_ms: 60_000,
        block_ttl_ms: 60_000,
        history_ttl_ms: 120_000,
        tick_ms: 1000,
        sqlite_busy_ms: 100,
    }
}

fn transaction(n: usize) -> SubscribeUpdate {
    let mut update = SubscribeUpdate::decode(
        include_bytes!("../../../app/tests/fixtures/capture/quote_sol_wallet_11_buy.pb").as_slice(),
    )
    .unwrap();
    update.created_at = Some(Timestamp {
        seconds: 1_790_000_000,
        nanos: n as i32,
    });
    let Some(subscribe_update::UpdateOneof::Transaction(tx)) = &mut update.update_oneof else {
        panic!("transaction fixture required")
    };
    tx.slot = 42;
    tx.transaction.as_mut().unwrap().signature[..8].copy_from_slice(&(n as u64).to_le_bytes());
    update
}

fn block(slot: u64) -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Block(SubscribeUpdateBlock {
            slot,
            parent_slot: slot - 1,
            blockhash: bs58::encode([slot as u8; 32]).into_string(),
            parent_blockhash: bs58::encode([(slot - 1) as u8; 32]).into_string(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ready_empty_scope_bridge_preserves_65_ordinary_frames_and_blocks() -> Result<()> {
    ready_burst(65).await
}

#[tokio::test(flavor = "current_thread")]
async fn ready_empty_scope_bridge_preserves_130_ordinary_frames_and_blocks() -> Result<()> {
    ready_burst(130).await
}

async fn ready_burst(count: usize) -> Result<()> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("capture.db");
    let db = rusqlite::Connection::open(&path)?;
    db.execute_batch(copybot_storage_core::capture_scope::SCHEMA)?;
    db.execute(
        "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,100,1048576)",
        [],
    )?;
    let mut config = IngestionConfig::default();
    config.source = "yellowstone_grpc".into();
    config.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    config.yellowstone_x_token = "LOCAL-ready-consumer".into();
    config.capture_scope_db = Some(path.to_str().unwrap().into());
    let source = crate::source::YellowstoneGrpcSource::new(&config)?;
    let mut runtime = (*source.runtime_config).clone();
    runtime.admission_wallets = Some(HashSet::new());
    let sorted = |s: &HashSet<String>| {
        let mut values: Vec<_> = s.iter().cloned().collect();
        values.sort();
        values
    };
    let scope = ReplayScope {
        policy: "protected_complete_recovery_v1".into(),
        wallets: vec![],
        programs: sorted(&runtime.interested_program_ids),
        raydium_programs: sorted(&runtime.raydium_program_ids),
        pumpswap_programs: sorted(&runtime.pumpswap_program_ids),
    };
    let limits = limits();
    let telemetry = Arc::new(DurableIngressTelemetry::default());
    let (send, mut receive) = queue::channel(limits.queue.count, limits.queue.bytes);
    let mut bridge = Bridge::new(
        &runtime,
        &limits,
        "LOCAL-ready".into(),
        send,
        None,
        telemetry.clone(),
    )?;
    bridge.enable_recovery(
        RecoveryCursor::new(scope, None, limits.history.clone())?,
        &limits,
    )?;
    assert_eq!(bridge.begin_replay()?, None);
    let mut updates = vec![block(42)];
    updates.extend((0..count).map(transaction));
    updates.push(block(43));
    let expected: Vec<_> = updates.iter().map(Message::encode_to_vec).collect();
    let expected_signatures: Vec<_> = (0..count)
        .map(|n| {
            let update = transaction(n);
            let Some(subscribe_update::UpdateOneof::Transaction(tx)) = update.update_oneof else {
                unreachable!()
            };
            bs58::encode(tx.transaction.unwrap().signature).into_string()
        })
        .collect();
    let mut reader = Reader::start(
        futures_util::stream::iter(updates.into_iter().map(Ok)),
        futures_util::sink::drain(),
        limits.blocks.count,
        limits.blocks.bytes,
        limits.input_bytes,
        telemetry.clone(),
        bridge.begin_capture()?,
    )?;
    let mut captured_bytes = Vec::new();
    let mut signatures = Vec::new();
    let mut checkpoints = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(mut captured) = reader.next().await {
            captured.dequeue();
            let update = captured.update()?;
            captured_bytes.push(update.encode_to_vec());
            bridge.update(captured_bytes.len() as u64, update).await?;
            while let Ok(envelope) = receive.try_recv() {
                match envelope.delivery.event {
                    DeliveryEvent::OrdinaryLive(v) => signatures.push(v.facts.signature),
                    DeliveryEvent::ParentCheckpoint(v) => {
                        checkpoints.push(v.observation.child.slot)
                    }
                    DeliveryEvent::ProtectedEvidence(v) => {
                        assert_ne!(
                            v.source_kind,
                            copybot_core_types::association_delivery::ProtectedSourceKind::Stream
                        );
                    }
                    other => panic!("unexpected protected admission in empty scope: {other:?}"),
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let terminal = reader.end().await?;
    assert!(
        matches!(terminal, End::Eof),
        "ready consumer lost a finite burst: {}",
        match terminal {
            End::Budget(reason) => reason,
            _ => "unexpected non-EOF terminal",
        }
    );
    assert_eq!(
        captured_bytes, expected,
        "Reader must preserve every byte and order"
    );
    assert_eq!(
        signatures, expected_signatures,
        "all ordinary swaps reach actual Bridge output"
    );
    assert_eq!(checkpoints, vec![42, 43]);
    let snapshot = telemetry.processing.snapshot();
    assert!(snapshot.input_queue_count_max <= 64);
    assert!(snapshot.input_queue_bytes_max <= limits.blocks.bytes as u64);
    assert_eq!(snapshot.input_queue_count, 0);
    assert_eq!(snapshot.input_queue_bytes, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn dequeued_but_held_64_captured_permits_still_refuse_without_waiting() -> Result<()> {
    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    let mut initial = vec![block(42)];
    initial.extend((0..63).map(transaction));
    let stream = futures_util::stream::iter(initial.into_iter().map(Ok))
        .chain(futures_util::stream::once(async move {
            wait.await.unwrap();
            Ok(transaction(64))
        }))
        .boxed();
    let telemetry = Arc::new(DurableIngressTelemetry::default());
    let mut reader = Reader::start(
        stream,
        futures_util::sink::drain(),
        64,
        8 << 20,
        1 << 20,
        telemetry.clone(),
        CaptureScope::new(&[], std::iter::empty(), Default::default(), None)
            .retain_ordinary_live(true),
    )?;
    let mut held = Vec::new();
    for _ in 0..64 {
        let mut captured = reader.next().await.unwrap();
        captured.dequeue();
        held.push(captured);
    }
    assert!(held[0].is_block());
    assert_eq!(telemetry.processing.snapshot().input_queue_count, 0);
    assert!(held.iter().all(|value| value._count.num_permits() == 1));
    release.send(()).unwrap();
    let terminal = tokio::time::timeout(Duration::from_secs(1), reader.end()).await??;
    assert!(matches!(terminal, End::Budget("LiveCaptureCount")));
    assert!(reader.next().await.is_none());
    assert_eq!(held.len(), 64, "dequeue must not release lifetime permits");
    Ok(())
}
