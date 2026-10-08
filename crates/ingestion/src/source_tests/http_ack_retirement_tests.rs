//! Current emitted checkpoint receipt, not an inherited same-slot cursor.
use super::*;
use crate::source::durable::{queue, telemetry::DurableIngressTelemetry};
use copybot_config::{AssociationDeliveryConfig, DeliveryBudget, IngestionConfig};
use copybot_core_types::association_recovery::{DurableCheckpoint, ReplayScope};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use yellowstone_grpc_proto::prelude::SubscribeUpdate;

fn hash(slot: u64) -> String {
    bs58::encode([slot as u8; 32]).into_string()
}
fn block(slot: u64, parent: u64) -> SubscribeUpdateBlock {
    SubscribeUpdateBlock {
        slot,
        parent_slot: parent,
        blockhash: hash(slot),
        parent_blockhash: hash(parent),
        ..Default::default()
    }
}
fn checkpoint(scope: &ReplayScope, block: &SubscribeUpdateBlock) -> BlockCheckpoint {
    BlockCheckpoint {
        scope: scope.clone(),
        observation: crate::source::durable::parent::observation(block),
        executed_transaction_count: 0,
        supplied_transaction_count: 0,
        claims: vec![],
    }
}
fn limits() -> AssociationDeliveryConfig {
    let b = |count, bytes| DeliveryBudget { count, bytes };
    AssociationDeliveryConfig {
        pending: b(16, 1 << 20),
        blocks: b(8, 4 << 20),
        history: b(32, 1 << 20),
        outputs: b(16, 1 << 20),
        queue: b(16, 1 << 20),
        inbox: b(1024, 16 << 20),
        input_bytes: 1 << 20,
        metadata_bytes: 1 << 20,
        pending_ttl_ms: 60000,
        block_ttl_ms: 60000,
        history_ttl_ms: 120000,
        tick_ms: 1000,
        sqlite_busy_ms: 100,
    }
}

#[tokio::test]
async fn inherited_same_session_cursor_never_retires_before_current_emission_and_ack() -> Result<()>
{
    let mut c = IngestionConfig::default();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    c.yellowstone_x_token = "local-only".into();
    let wallets = HashSet::from([bs58::encode([3; 32]).into_string()]);
    let scope = crate::replay_scope(&c, &wallets)?;
    let source = crate::source::YellowstoneGrpcSource::new(&c)?;
    let mut runtime = (*source.runtime_config).clone();
    runtime.admission_wallets = Some(wallets);
    let limits = limits();
    let mut cursor = RecoveryCursor::new(
        scope.clone(),
        Some(DurableCheckpoint {
            session: "ack:0".into(),
            sequence: 0,
            block: checkpoint(&scope, &block(42, 40)),
            from_slot: 40,
            overlap: vec![],
        }),
        limits.history.clone(),
    )?;
    let hold = Arc::new(AtomicBool::new(true));
    cursor.http_hold = Some(hold.clone());
    let telemetry = Arc::new(DurableIngressTelemetry::default());
    let (sender, mut receiver) = queue::channel(16, 1 << 20);
    let mut bridge = Bridge::new(&runtime, &limits, "ack".into(), sender, None, telemetry)?;
    // This is the same source generation after prior emissions, not a fresh UUID.
    bridge.sequence = 10;
    bridge.enable_recovery(cursor.clone(), &limits)?;
    bridge.begin_replay()?;
    bridge.http_block(0, &block(40, 39)).await?;
    bridge.retire_acknowledged_complete_blocks()?;
    assert_eq!(
        bridge.adapter.block_cache_usage().0,
        1,
        "old durable slot42 is not pregate retirement authority"
    );
    bridge.http_block(1, &block(42, 40)).await?;
    bridge.retire_acknowledged_complete_blocks()?;
    assert_eq!(
        bridge.adapter.block_cache_usage().0,
        2,
        "same-session old sequence0 is not ACK of new sequence11"
    );
    let mut emitted = None;
    for _ in 0..2 {
        let envelope = receiver.recv().await.unwrap();
        if let DeliveryEvent::ParentCheckpoint(p) = &envelope.delivery.event {
            emitted = Some(DurableCheckpoint {
                session: envelope.delivery.session.clone(),
                sequence: envelope.delivery.sequence,
                block: p.clone(),
                from_slot: 40,
                overlap: vec![],
            });
        }
    }
    let emitted = emitted.unwrap();
    assert_eq!(emitted.sequence, 11);
    cursor.acknowledge(emitted.clone())?;
    bridge.retire_acknowledged_complete_blocks()?;
    assert_eq!(
        bridge.adapter.block_cache_usage(),
        (0, 0),
        "exact current-generation checkpoint ACK releases complete blocks"
    );
    bridge.http_block(2, &block(45, 42)).await?;
    assert_eq!(bridge.adapter.block_cache_usage().0, 1);
    // A current ACK with a changed emitted child/hash is still refused.
    let envelope = receiver.recv().await.unwrap();
    let DeliveryEvent::ParentCheckpoint(p) = &envelope.delivery.event else {
        panic!("checkpoint")
    };
    let mut wrong = p.clone();
    wrong.observation.child.hash = hash(99);
    cursor.acknowledge(DurableCheckpoint {
        session: envelope.delivery.session.clone(),
        sequence: envelope.delivery.sequence,
        block: wrong,
        from_slot: 42,
        overlap: vec![],
    })?;
    assert!(bridge
        .retire_acknowledged_complete_blocks()
        .unwrap_err()
        .to_string()
        .contains("ack_identity"));
    assert_eq!(bridge.adapter.block_cache_usage().0, 1);
    bridge.begin_replay()?;
    assert!(bridge.recovery.as_ref().unwrap().last_checkpoint.is_none());
    assert!(bridge
        .recovery
        .as_ref()
        .unwrap()
        .first_checkpoint_sequence
        .is_none());
    bridge.retire_acknowledged_complete_blocks()?;
    assert_eq!(
        bridge.adapter.block_cache_usage().0,
        1,
        "begin_replay resets retirement eligibility"
    );
    assert!(hold.load(Ordering::Acquire));
    Ok(())
}

#[tokio::test]
async fn closed_unknown_supported_source_refusal_closes_bridge_hold() -> Result<()> {
    // The association causal tests exercise exact Info/float/history. Here the
    // production Bridge refusal path must synchronously restore the same hold.
    let mut c = IngestionConfig::default();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    c.yellowstone_x_token = "local-only".into();
    let saved: serde_json::Value = serde_json::from_str(include_str!(
        "../../../storage-core/tests/fixtures/recovery_06.json"
    ))?;
    let wallet = saved["authority"]["wallet_ids"][0]
        .as_str()
        .unwrap()
        .to_owned();
    let wallets = HashSet::from([wallet]);
    let scope = crate::replay_scope(&c, &wallets)?;
    let source = crate::source::YellowstoneGrpcSource::new(&c)?;
    let mut runtime = (*source.runtime_config).clone();
    runtime.admission_wallets = Some(wallets);
    let limits = limits();
    let hold = Arc::new(AtomicBool::new(false));
    let mut cursor = RecoveryCursor::new(scope, None, limits.history.clone())?;
    cursor.http_hold = Some(hold.clone());
    let (sender, mut receiver) = queue::channel(16, 1 << 20);
    let mut bridge = Bridge::new(
        &runtime,
        &limits,
        "closed".into(),
        sender,
        None,
        Arc::new(Default::default()),
    )?;
    bridge.enable_recovery(cursor, &limits)?;
    let original: copybot_core_types::association_delivery::Delivery =
        serde_json::from_value(saved["delivery"].clone())?;
    let DeliveryEvent::Admission(admission) = original.event else {
        panic!("saved first admission")
    };
    let mut info = SubscribeUpdateTransactionInfo::decode(admission.info.encoded.as_slice())?;
    let meta = info.meta.as_mut().unwrap();
    let rows = meta
        .pre_token_balances
        .iter_mut()
        .chain(&mut meta.post_token_balances);
    for (row, bits) in rows.zip(&admission.info.float_bits) {
        if let (Some(amount), Some(bits)) = (&mut row.ui_token_amount, bits) {
            amount.ui_amount = f64::from_bits(*bits);
        }
    }
    let transaction = SubscribeUpdateTransaction {
        slot: admission.facts.slot,
        transaction: Some(info),
    };
    let decoded = crate::source::yellowstone_facts::decode_yellowstone_swap_facts(
        &transaction,
        &runtime.interested_program_ids,
        &runtime.raydium_program_ids,
        &runtime.pumpswap_program_ids,
    );
    assert_eq!(
        decoded.facts.unwrap().unwrap().signer,
        admission.facts.wallet,
        "actual saved supported source operand"
    );
    bridge
        .adapter
        .retire_complete_blocks_through(transaction.slot)
        .unwrap();
    bridge.begin_replay()?;
    // Same-generation replay resets ACK eligibility, never the closed prefix.
    // Even accepting a full old block again cannot make its unseen Info fresh.
    bridge
        .http_block(0, &block(transaction.slot, transaction.slot - 1))
        .await?;
    let checkpoint = receiver.recv().await.unwrap();
    assert!(matches!(
        checkpoint.delivery.event,
        DeliveryEvent::ParentCheckpoint(_)
    ));
    assert!(!hold.load(Ordering::Acquire));
    let result = bridge
        .update(
            0,
            &SubscribeUpdate {
                update_oneof: Some(
                    yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Transaction(
                        transaction,
                    ),
                ),
                ..Default::default()
            },
        )
        .await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("ClosedBlockUnknownInfo"));
    assert!(hold.load(Ordering::Acquire));
    let event = receiver.recv().await.unwrap();
    assert!(
        matches!(&event.delivery.event,DeliveryEvent::Session(SessionGap::Rejected(r)) if r=="ClosedBlockUnknownInfo")
    );
    Ok(())
}

#[tokio::test]
async fn chosen_older_checkpoint_ack_checks_hash_and_retires_only_covered_prefix() -> Result<()> {
    for correct_hash in [false, true] {
        let mut c = IngestionConfig::default();
        c.source = "yellowstone_grpc".into();
        c.yellowstone_grpc_url = "http://127.0.0.1:1".into();
        c.yellowstone_x_token = "local-only".into();
        let wallets = HashSet::from([bs58::encode([3; 32]).into_string()]);
        let scope = crate::replay_scope(&c, &wallets)?;
        let source = crate::source::YellowstoneGrpcSource::new(&c)?;
        let limits = limits();
        let cursor = RecoveryCursor::new(scope, None, limits.history.clone())?;
        let (sender, mut receiver) = queue::channel(16, 1 << 20);
        let mut bridge = Bridge::new(
            &source.runtime_config,
            &limits,
            "older".into(),
            sender,
            None,
            Arc::new(Default::default()),
        )?;
        bridge.enable_recovery(cursor.clone(), &limits)?;
        bridge.begin_replay()?;
        bridge.http_block(0, &block(42, 40)).await?;
        bridge.http_block(1, &block(45, 42)).await?;
        assert_eq!(
            bridge.adapter.block_cache_usage().0,
            2,
            "unacknowledged full blocks remain cached"
        );
        let envelope = receiver.recv().await.unwrap();
        let DeliveryEvent::ParentCheckpoint(p) = &envelope.delivery.event else {
            panic!("older checkpoint")
        };
        let mut chosen = p.clone();
        if !correct_hash {
            chosen.observation.child.hash = hash(99);
        }
        cursor.acknowledge(DurableCheckpoint {
            session: envelope.delivery.session.clone(),
            sequence: envelope.delivery.sequence,
            block: chosen,
            from_slot: 40,
            overlap: vec![],
        })?;
        let result = bridge.retire_acknowledged_complete_blocks();
        if correct_hash {
            result?;
            assert_eq!(
                bridge.adapter.block_cache_usage().0,
                1,
                "newer unacknowledged slot45 remains cached"
            );
        } else {
            assert!(result.unwrap_err().to_string().contains("ack_identity"));
            assert_eq!(
                bridge.adapter.block_cache_usage().0,
                2,
                "wrong older hash cannot release any cached block"
            );
        }
    }
    Ok(())
}
