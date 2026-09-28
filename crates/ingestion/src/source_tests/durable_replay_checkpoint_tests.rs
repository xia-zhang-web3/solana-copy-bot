use super::durable_transport_fixture::Fixture;
use crate::source::durable::recovery::RecoveryGate;
use crate::{replay_scope, DeliveryReceiver};
use copybot_config::{AssociationDeliveryConfig, DeliveryBudget, IngestionConfig};
use copybot_core_types::{association_delivery::*, association_parent::*, association_recovery::*};
use futures_util::stream;
use prost::Message as _;
use std::{collections::HashSet, time::Duration};
use yellowstone_grpc_proto::prelude::*;
fn config(url: String) -> IngestionConfig {
    let mut c = IngestionConfig::default();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_grpc_url = url;
    c.yellowstone_x_token = "synthetic-local-only".into();
    c.yellowstone_program_ids = vec!["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".into()];
    c.subscribe_program_ids = c.yellowstone_program_ids.clone();
    c.yellowstone_delivery_mode = "durable_association_v1".into();
    let b = |count, bytes| DeliveryBudget { count, bytes };
    c.yellowstone_association = Some(AssociationDeliveryConfig {
        pending: b(16, 1 << 20),
        blocks: b(384, 64 << 20),
        history: b(32, 8 << 20),
        outputs: b(16, 1 << 20),
        queue: b(32, 4 << 20),
        inbox: b(1024, 8 << 20),
        input_bytes: 8 << 20,
        metadata_bytes: 8 << 20,
        pending_ttl_ms: 60_000,
        block_ttl_ms: 60_000,
        history_ttl_ms: 120_000,
        tick_ms: 1000,
        sqlite_busy_ms: 100,
    });
    c
}
fn key(slot: u64) -> BlockKey {
    BlockKey {
        slot,
        hash: bs58::encode([slot as u8; 32]).into_string(),
    }
}
fn block(slot: u64, parent: u64) -> SubscribeUpdateBlock {
    SubscribeUpdateBlock {
        slot,
        parent_slot: parent,
        blockhash: key(slot).hash,
        parent_blockhash: key(parent).hash,
        ..Default::default()
    }
}
fn head(scope: ReplayScope) -> DurableCheckpoint {
    DurableCheckpoint {
        session: "model-committed".into(),
        sequence: 8,
        from_slot: 40,
        overlap: vec![],
        block: BlockCheckpoint {
            scope,
            observation: ParentObservation {
                child: key(42),
                parent: key(40),
                issue: None,
            },
            executed_transaction_count: 0,
            supplied_transaction_count: 0,
            claims: vec![],
        },
    }
}
#[test]
fn restored_gate_requires_exact_parent_child_hash_before_forward_blocks() {
    let c = config("http://127.0.0.1:1".into());
    let scope = replay_scope(&c, &HashSet::from(["leader".into()])).unwrap();
    let saved = head(scope.clone());
    let mut gate = RecoveryGate::new(Some(saved.clone()), 384);
    assert!(!gate.ready());
    assert_eq!(gate.from_slot(), Some(40));
    assert!(gate.block(&block(45, 42)).is_err());
    assert!(!gate.ready());
    let mut gate = RecoveryGate::new(Some(saved.clone()), 384);
    assert!(!gate.block(&block(40, 39)).unwrap());
    let mut changed = block(42, 40);
    changed.blockhash = key(43).hash;
    assert!(gate.block(&changed).is_err());
    assert!(!gate.ready());
    let mut gate = RecoveryGate::new(Some(saved), 384);
    assert!(!gate.block(&block(40, 39)).unwrap());
    assert!(gate.block(&block(42, 40)).unwrap());
    assert!(gate.ready());
    assert!(gate.block(&block(45, 42)).unwrap());
    assert!(gate.block(&block(45, 42)).unwrap());
    assert!(gate.block(&block(45, 41)).is_err());
    assert!(gate.block(&block(48, 47)).is_err());
}
#[test]
fn anchor_info_float_bits_and_original_index_are_not_replaced_by_hash_match() {
    let c = config("http://127.0.0.1:1".into());
    let mut saved = head(replay_scope(&c, &HashSet::from(["leader".into()])).unwrap());
    saved.block.executed_transaction_count = 1;
    saved.block.supplied_transaction_count = 1;
    saved.block.claims = vec![CheckpointClaim {
        signature: bs58::encode([7u8; 64]).into_string(),
        transaction_index: 0,
        info: InfoIdentity {
            encoded: vec![1],
            float_bits: vec![Some((-0.0f64).to_bits())],
        },
    }];
    let mut gate = RecoveryGate::new(Some(saved), 384);
    assert!(!gate.block(&block(40, 39)).unwrap());
    let mut anchor = block(42, 40);
    anchor.executed_transaction_count = 1;
    anchor.transactions = vec![SubscribeUpdateTransactionInfo {
        signature: vec![7u8; 64],
        index: 0,
        ..Default::default()
    }];
    assert!(gate.block(&anchor).is_err());
    assert!(!gate.ready());
}
async fn refused(code: tonic::Code, subscribe: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming = stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(Fixture {
                messages: vec![],
                terminal: (!subscribe).then_some(code),
                reject_subscribe: subscribe.then_some(code),
            }))
            .serve_with_incoming(incoming),
    );
    let c = config(url);
    let wallets = HashSet::from(["leader".into()]);
    let saved = head(replay_scope(&c, &wallets).unwrap());
    let mut receiver = DeliveryReceiver::start_recovering_labeled(
        &c,
        "replay-rejected".into(),
        wallets,
        None,
        Some(saved),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut rejected = false;
        loop {
            match receiver.next().await {
                Ok(Some(e)) => match &e.delivery.event {
                    DeliveryEvent::Session(SessionGap::Rejected(reason)) => {
                        assert!(reason.contains("Unavailable"));
                        rejected = true;
                    }
                    DeliveryEvent::Admission(_) => panic!("unavailable replay admitted facts"),
                    _ => {}
                },
                Err(_) => {
                    assert!(rejected);
                    break;
                }
                Ok(None) => panic!("unavailable replay silently completed"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(receiver.ingress_snapshot().reconnects, 1);
    receiver.stop();
    server.abort();
}
#[tokio::test]
async fn definitive_subscribe_and_preanchor_stream_history_rejection_do_not_retry() {
    for code in [
        tonic::Code::OutOfRange,
        tonic::Code::InvalidArgument,
        tonic::Code::Unimplemented,
        tonic::Code::NotFound,
    ] {
        refused(code, true).await;
        refused(code, false).await;
    }
}
fn model_tx(slot: u64) -> SubscribeUpdateTransaction {
    let mut tx = super::jupiter_raydium_fixture::buy(false, false, 7, 10_000_000);
    tx.slot = slot;
    tx
}
fn wallets() -> HashSet<String> {
    HashSet::from([bs58::encode([11u8; 32]).into_string()])
}
fn update_block(
    mut b: SubscribeUpdateBlock,
    tx: Option<&SubscribeUpdateTransaction>,
) -> SubscribeUpdate {
    if let Some(tx) = tx {
        b.transactions = vec![tx.transaction.clone().unwrap()];
        b.executed_transaction_count = 1;
    }
    SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Block(b)),
        ..Default::default()
    }
}
async fn running(
    messages: Vec<SubscribeUpdate>,
    saved: Option<DurableCheckpoint>,
    pending: usize,
) -> (DeliveryReceiver, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming = stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(Fixture {
                messages,
                terminal: None,
                reject_subscribe: None,
            }))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let mut c = config(url);
    c.yellowstone_association.as_mut().unwrap().pending.count = pending;
    (
        DeliveryReceiver::start_recovering_labeled(
            &c,
            "replay-model".into(),
            wallets(),
            None,
            saved,
        )
        .unwrap(),
        server,
    )
}
fn old_head() -> DurableCheckpoint {
    let c = config("http://127.0.0.1:1".into());
    let mut h = head(replay_scope(&c, &wallets()).unwrap());
    h.from_slot = 4;
    h.block.observation = ParentObservation {
        child: key(7),
        parent: key(6),
        issue: None,
    };
    h
}
#[tokio::test]
async fn selected_embedded_info_before_anchor_is_held_then_recovered_with_missing_time() {
    let tx = model_tx(4);
    let (mut receiver, server) = running(
        vec![
            update_block(block(4, 3), Some(&tx)),
            update_block(block(6, 4), None),
            update_block(block(7, 6), None),
        ],
        Some(old_head()),
        16,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut parents = vec![];
        let mut admissions = 0;
        let mut assertions = 0;
        loop {
            let e = receiver.next().await.unwrap().unwrap();
            match &e.delivery.event {
                DeliveryEvent::Parent(p) => parents.push(p.child.slot),
                DeliveryEvent::Admission(a) => {
                    assert_eq!(parents, vec![4, 6]);
                    assert_eq!(a.facts.slot, 4);
                    assert_eq!(a.message_time, MessageTime::Missing);
                    assert_eq!(
                        a.info.encoded,
                        tx.transaction.as_ref().unwrap().encode_to_vec()
                    );
                    admissions += 1;
                }
                DeliveryEvent::Terminal {
                    result: Terminal::ProviderAsserted(p),
                    ..
                } => {
                    assert_eq!(p.slot, 4);
                    assert_eq!(p.transaction_index, 0);
                    assert_eq!(p.blockhash, key(4).hash);
                    assertions += 1;
                }
                DeliveryEvent::ParentCheckpoint(h) if h.observation.child.slot == 7 => {
                    assert_eq!((admissions, assertions), (1, 1));
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    receiver.stop();
    server.abort();
}
#[tokio::test]
async fn partial_or_ambiguous_producer_block_cannot_assert_a_pending_transaction() {
    for mode in ["partial", "index", "signature"] {
        let tx = model_tx(4);
        let mut b = block(4, 3);
        b.executed_transaction_count = 2;
        b.transactions = vec![tx.transaction.clone().unwrap()];
        if mode != "partial" {
            let mut second = b.transactions[0].clone();
            second.index = 1;
            if mode == "index" {
                second.index = 0;
                second.signature = vec![43; 64];
            }
            b.transactions.push(second);
        }
        let (mut receiver, server) = running(
            vec![
                SubscribeUpdate {
                    update_oneof: Some(subscribe_update::UpdateOneof::Transaction(tx)),
                    ..Default::default()
                },
                update_block(b, None),
            ],
            None,
            16,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(3), async {
            let mut admissions = 0;
            let mut rejected = false;
            loop {
                match receiver.next().await {
                    Ok(Some(e)) => match e.delivery.event {
                        DeliveryEvent::Admission(_) => admissions += 1,
                        DeliveryEvent::Terminal { .. } | DeliveryEvent::ParentCheckpoint(_) => {
                            panic!("{mode}: incomplete block produced proof")
                        }
                        DeliveryEvent::Session(SessionGap::Rejected(_)) => rejected = true,
                        _ => {}
                    },
                    Err(_) => {
                        assert!(rejected);
                        assert_eq!(admissions, 1);
                        break;
                    }
                    Ok(None) => panic!("{mode}: invalid producer silently completed"),
                }
            }
        })
        .await
        .unwrap();
        receiver.stop();
        server.abort();
    }
}
#[tokio::test]
async fn restart_foreign_info_mutation_or_missing_known_info_refuses_before_admission() {
    for mode in ["foreign", "missing"] {
        let tx = model_tx(4);
        let ray = HashSet::from([super::jupiter_raydium_fixture::AMM.to_string()]);
        let decoded = crate::source::yellowstone_facts::decode_yellowstone_swap_facts(
            &tx,
            &ray,
            &ray,
            &HashSet::new(),
        )
        .facts
        .unwrap()
        .unwrap();
        let info = tx.transaction.as_ref().unwrap();
        let original = AdmissionFacts {
            facts: CheckedFacts {
                signature: decoded.signature,
                slot: 4,
                wallet: decoded.signer,
                token_in: decoded.token_in,
                token_out: decoded.token_out,
                amount_in_bits: decoded.amount_in.to_bits(),
                amount_out_bits: decoded.amount_out.to_bits(),
                exact_amounts: decoded.exact_amounts,
                programs: decoded.program_ids,
                dex: decoded.dex_hint,
                program_fallback: false,
            },
            info: InfoIdentity {
                encoded: info.encode_to_vec(),
                float_bits: info
                    .meta
                    .iter()
                    .flat_map(|m| m.pre_token_balances.iter().chain(&m.post_token_balances))
                    .map(|r| r.ui_token_amount.as_ref().map(|a| a.ui_amount.to_bits()))
                    .collect(),
            },
            message_time: MessageTime::Missing,
        };
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("reopened.sqlite");
        copybot_storage_core::SqliteStore::open(&path)
            .unwrap()
            .run_migrations(std::path::Path::new(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../migrations"
            )))
            .unwrap();
        let limits = copybot_storage_core::association_inbox::InboxLimits {
            count: 1024,
            bytes: 8 << 20,
            busy_ms: 100,
        };
        let mut inbox =
            copybot_storage_core::association_inbox::AssociationInbox::open(&path, limits).unwrap();
        let model = old_head();
        inbox.configure_replay_scope(&model.block.scope).unwrap();
        for (sequence, event) in [
            (0, DeliveryEvent::Admission(original.clone())),
            (1, DeliveryEvent::ParentCheckpoint(model.block.clone())),
        ] {
            inbox
                .persist(
                    &Delivery {
                        session: "prior-process".into(),
                        sequence,
                        arrival_offset_ns: sequence,
                        event,
                    },
                    &CandidateGeneration::Unknown,
                )
                .unwrap();
        }
        drop(inbox);
        let mut inbox =
            copybot_storage_core::association_inbox::AssociationInbox::open(&path, limits).unwrap();
        let saved = inbox
            .replay_checkpoint(&model.block.scope)
            .unwrap()
            .unwrap();
        assert_eq!(saved.overlap, vec![original.clone()]);
        assert_eq!(saved.from_slot, 4);
        assert!(
            inbox
                .identity(&original.facts.signature)
                .unwrap()
                .unwrap()
                .recovery
        );
        let mut changed = tx;
        changed
            .transaction
            .as_mut()
            .unwrap()
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .account_keys[0] = vec![74; 32];
        if mode == "missing" {
            changed.transaction.as_mut().unwrap().signature = vec![43; 64];
            changed
                .transaction
                .as_mut()
                .unwrap()
                .transaction
                .as_mut()
                .unwrap()
                .signatures = vec![vec![43; 64]];
        }
        let (mut receiver, server) = running(
            vec![
                update_block(block(4, 3), Some(&changed)),
                update_block(block(6, 4), None),
                update_block(block(7, 6), None),
            ],
            Some(saved),
            16,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(3), async {
            let mut duplicate = false;
            let mut gap = false;
            loop {
                match receiver.next().await {
                    Ok(Some(e)) => {
                        inbox
                            .persist(&e.delivery, &CandidateGeneration::Unknown)
                            .unwrap();
                        match e.delivery.event {
                            DeliveryEvent::Duplicate {
                                original: stored,
                                observed_info,
                                observed_slot,
                                message_time,
                            } => {
                                assert_eq!(stored, original);
                                assert_eq!(observed_slot, 4);
                                assert_ne!(observed_info, original.info);
                                assert_eq!(message_time, MessageTime::Missing);
                                duplicate = true;
                            }
                            DeliveryEvent::Session(SessionGap::Rejected(reason)) => {
                                assert_eq!(duplicate, mode == "foreign");
                                assert_eq!(
                                    reason,
                                    if mode == "foreign" {
                                        "replay_durable_info_conflict"
                                    } else {
                                        "replay_durable_info_missing"
                                    }
                                );
                                gap = true;
                            }
                            DeliveryEvent::Admission(_)
                            | DeliveryEvent::Terminal { .. }
                            | DeliveryEvent::ParentCheckpoint(_) => {
                                panic!("mutated durable fact reached admission")
                            }
                            _ => {}
                        }
                    }
                    Err(_) => {
                        assert_eq!(duplicate, mode == "foreign");
                        assert!(gap);
                        break;
                    }
                    Ok(None) => panic!("mutated replay silently completed"),
                }
            }
        })
        .await
        .unwrap();
        receiver.stop();
        server.abort();
        let stored = inbox.identity(&original.facts.signature).unwrap().unwrap();
        assert_eq!(stored.conflict, mode == "foreign");
        assert!(stored.recovery);
        assert_eq!(stored.admission, original);
        assert!(stored.terminal.is_none());
        assert_eq!(
            inbox
                .replay_checkpoint(&model.block.scope)
                .unwrap()
                .unwrap()
                .block,
            model.block
        );
    }
}
