//! Real local tonic + HTTP adapter and SQLite commits; no provider or finances.
#[path = "saved_reward_replay/mod.rs"]
mod saved_reward_replay;
#[path = "http_delivery_replay/mod.rs"]
mod http_delivery_replay;
#[path = "http_anchor_diagnostic_tests.rs"]
mod anchor_diagnostic_tests;
use super::durable_transport_fixture::Fixture;
use crate::{replay_scope, DeliveryEnvelope, DeliveryReceiver};
use copybot_config::{
    AssociationDeliveryConfig, DeliveryBudget, HttpRecoveryConfig, IngestionConfig,
};
use copybot_core_types::{association_delivery::*, association_recovery::*};
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    SqliteStore,
};
use futures_util::{stream, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};
use yellowstone_grpc_proto::prelude::*;
fn raw(slot: u64) -> Value {
    let parent = match slot {
        40 => 39,
        42 => 40,
        45 => 42,
        48 => 45,
        50 => 48,
        _ => panic!("fixture slot"),
    };
    json!({"parentSlot":parent,"previousBlockhash":hash(parent),"blockhash":hash(slot),
        "blockTime":1700000000+slot,"blockHeight":slot,"transactions":[],"rewards":[]})
}
fn hash(slot: u64) -> String {
    bs58::encode([slot as u8; 32]).into_string()
}
fn block(slot: u64) -> SubscribeUpdateBlock {
    crate::source::http_recovery::normalize_confirmed_http_block(slot, &raw(slot)).unwrap()
}
fn checkpoint(scope: &ReplayScope, slot: u64) -> BlockCheckpoint {
    let b = block(slot);
    BlockCheckpoint {
        scope: scope.clone(),
        observation: copybot_core_types::association_parent::ParentObservation {
            child: copybot_core_types::association_parent::BlockKey {
                slot: b.slot,
                hash: b.blockhash.clone(),
            },
            parent: copybot_core_types::association_parent::BlockKey {
                slot: b.parent_slot,
                hash: b.parent_blockhash.clone(),
            },
            issue: None,
        },
        executed_transaction_count: 0,
        supplied_transaction_count: 0,
        claims: vec![],
    }
}
#[derive(Clone)]
struct Live(Fixture);
#[tonic::async_trait]
impl geyser_server::Geyser for Live {
    type SubscribeStream = <Fixture as geyser_server::Geyser>::SubscribeStream;
    type SubscribeDeshredStream = <Fixture as geyser_server::Geyser>::SubscribeDeshredStream;
    async fn subscribe(
        &self,
        r: tonic::Request<tonic::Streaming<SubscribeRequest>>,
    ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        let r = r.into_inner().message().await?.unwrap();
        assert_eq!(
            r.from_slot, None,
            "full-block replay is prohibited in HTTP mode"
        );
        assert_eq!(r.commitment, Some(CommitmentLevel::Confirmed as i32));
        Ok(tonic::Response::new(Box::pin(
            stream::iter(self.0.messages.clone().into_iter().map(Ok)).chain(stream::pending()),
        )))
    }
    async fn subscribe_deshred(
        &self,
        r: tonic::Request<tonic::Streaming<SubscribeDeshredRequest>>,
    ) -> Result<tonic::Response<Self::SubscribeDeshredStream>, tonic::Status> {
        geyser_server::Geyser::subscribe_deshred(&self.0, r).await
    }
    async fn subscribe_replay_info(
        &self,
        r: tonic::Request<SubscribeReplayInfoRequest>,
    ) -> Result<tonic::Response<SubscribeReplayInfoResponse>, tonic::Status> {
        geyser_server::Geyser::subscribe_replay_info(&self.0, r).await
    }
    async fn ping(
        &self,
        r: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PongResponse>, tonic::Status> {
        geyser_server::Geyser::ping(&self.0, r).await
    }
    async fn get_latest_blockhash(
        &self,
        r: tonic::Request<GetLatestBlockhashRequest>,
    ) -> Result<tonic::Response<GetLatestBlockhashResponse>, tonic::Status> {
        geyser_server::Geyser::get_latest_blockhash(&self.0, r).await
    }
    async fn get_block_height(
        &self,
        r: tonic::Request<GetBlockHeightRequest>,
    ) -> Result<tonic::Response<GetBlockHeightResponse>, tonic::Status> {
        geyser_server::Geyser::get_block_height(&self.0, r).await
    }
    async fn get_slot(
        &self,
        r: tonic::Request<GetSlotRequest>,
    ) -> Result<tonic::Response<GetSlotResponse>, tonic::Status> {
        geyser_server::Geyser::get_slot(&self.0, r).await
    }
    async fn is_blockhash_valid(
        &self,
        r: tonic::Request<IsBlockhashValidRequest>,
    ) -> Result<tonic::Response<IsBlockhashValidResponse>, tonic::Status> {
        geyser_server::Geyser::is_blockhash_valid(&self.0, r).await
    }
    async fn get_version(
        &self,
        r: tonic::Request<GetVersionRequest>,
    ) -> Result<tonic::Response<GetVersionResponse>, tonic::Status> {
        geyser_server::Geyser::get_version(&self.0, r).await
    }
}
struct Servers {
    http: String,
    grpc: String,
    tasks: Vec<JoinHandle<()>>,
    calls: Arc<Mutex<Vec<Value>>>,
}
impl Drop for Servers {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}
async fn servers(mode: &'static str, anchor: u64, delay: Arc<AtomicBool>) -> Servers {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let seen = calls.clone();
    let h = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut bytes = Vec::new();
            let mut chunk = [0u8; 4096];
            let (end, len) = loop {
                let n = socket.read(&mut chunk).await.unwrap();
                if n == 0 {
                    return;
                }
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let header = std::str::from_utf8(&bytes[..end]).unwrap();
                    let len = header
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|s| s.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (end + 4, len);
                }
                assert!(bytes.len() < 1 << 20);
            };
            while bytes.len() < end + len {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
            }
            let request: Value = serde_json::from_slice(&bytes[end..end + len]).unwrap();
            seen.lock().unwrap().push(request.clone());
            let result = if request["method"] == "getBlocks" {
                assert_eq!(request["params"][2]["commitment"], "confirmed");
                let from = request["params"][0].as_u64().unwrap();
                let to = request["params"][1].as_u64().unwrap();
                json!([40, 42, 45, 48, 50]
                    .into_iter()
                    .filter(|s| *s >= from && *s <= to && !(mode == "missing" && *s == 45))
                    .collect::<Vec<_>>())
            } else {
                assert_eq!(request["method"], "getBlock");
                assert_eq!(request["params"][1]["maxSupportedTransactionVersion"], 1);
                assert_eq!(request["params"][1]["commitment"], "confirmed");
                let slot = request["params"][0].as_u64().unwrap();
                if slot == 48 && delay.swap(false, Ordering::AcqRel) {
                    tokio::time::sleep(Duration::from_millis(600)).await;
                }
                if mode == "null" && slot == 45 {
                    Value::Null
                } else {
                    let mut v = raw(slot);
                    if mode == "anchor-order" && slot == 48 {
                        v = anchor_diagnostic_tests::transaction_raw();
                    }
                    if mode == "anchor-malformed" && slot == 48 { v["rewards"]=Value::Null; }
                    if mode == "conflict" && slot == 45 {
                        v["blockhash"] = json!(hash(99));
                    }
                    v
                }
            };
            let mut body =
                serde_json::to_vec(&json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
                    .unwrap();
            if mode=="anchor-envelope" && request["method"]=="getBlock" && request["params"][0]==48 {
                body=b"{invalid original JSON".to_vec();
            }
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(&body).await;
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc = format!("http://{}", listener.local_addr().unwrap());
    let incoming = stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let g = tokio::spawn(async move {
        let anchor_block = if mode == "anchor-order" {
            anchor_diagnostic_tests::unordered_block()
        } else { block(anchor) };
        let mut messages = vec![SubscribeUpdate {
            update_oneof: Some(subscribe_update::UpdateOneof::Block(anchor_block)),
            ..Default::default()
        }];
        if mode.starts_with("live-") {
            messages.push(SubscribeUpdate {
                update_oneof: Some(subscribe_update::UpdateOneof::Block(block(42))),
                ..Default::default()
            });
            messages.push(SubscribeUpdate {
                update_oneof: Some(if mode == "live-gap" {
                    subscribe_update::UpdateOneof::Block(block(50))
                } else {
                    subscribe_update::UpdateOneof::Transaction(SubscribeUpdateTransaction {
                        transaction: None,
                        slot: 45,
                    })
                }),
                ..Default::default()
            });
        }
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(Live(Fixture {
                messages,
                terminal: None,
                reject_subscribe: None,
            })))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    Servers {
        http,
        grpc,
        tasks: vec![h, g],
        calls,
    }
}
fn config(s: &Servers) -> IngestionConfig {
    let mut c = IngestionConfig::default();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_grpc_url = s.grpc.clone();
    c.yellowstone_x_token = "local-fixture-only".into();
    c.yellowstone_delivery_mode = "durable_association_v1".into();
    let b = |count, bytes| DeliveryBudget { count, bytes };
    c.yellowstone_association = Some(AssociationDeliveryConfig {
        pending: b(16, 1 << 20),
        blocks: b(32, 4 << 20),
        history: b(32, 1 << 20),
        outputs: b(16, 1 << 20),
        queue: b(16, 1 << 20),
        inbox: b(1024, 16 << 20),
        input_bytes: 1 << 20,
        metadata_bytes: 1 << 20,
        pending_ttl_ms: 60_000,
        block_ttl_ms: 60_000,
        history_ttl_ms: 120_000,
        tick_ms: 1000,
        sqlite_busy_ms: 100,
    });
    c.yellowstone_http_recovery = Some(HttpRecoveryConfig {
        anchor_evidence_dir: None,
        broker_url: s.http.clone(),
        broker_token: String::new(),
        range_slots: 2,
        max_response_bytes: 1 << 20,
        timeout_ms: 2000,
        fetch_concurrency: 1,
    });
    c
}
fn inbox(path: &std::path::Path) -> AssociationInbox {
    AssociationInbox::open(
        path,
        InboxLimits {
            count: 1024,
            bytes: 16 << 20,
            busy_ms: 100,
        },
    )
    .unwrap()
}
fn initialize(
    path: &std::path::Path,
    c: &IngestionConfig,
    wallets: &HashSet<String>,
) -> (AssociationInbox, ReplayScope) {
    let (mut db, scope) = initialize_empty(path, c, wallets);
    db.persist(
        &Delivery {
            session: "initial".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::ParentCheckpoint(checkpoint(&scope, 42)),
        },
        &CandidateGeneration::Unknown,
    )
    .unwrap();
    (db, scope)
}
fn initialize_empty(
    path: &std::path::Path,
    c: &IngestionConfig,
    wallets: &HashSet<String>,
) -> (AssociationInbox, ReplayScope) {
    SqliteStore::open(path)
        .unwrap()
        .run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))
        .unwrap();
    let scope = replay_scope(c, wallets).unwrap();
    let mut db = inbox(path);
    db.configure_replay_scope(&scope).unwrap();
    (db, scope)
}
fn persist(
    db: &mut AssociationInbox,
    r: &DeliveryReceiver,
    e: &DeliveryEnvelope,
    scope: &ReplayScope,
) {
    db.persist(&e.delivery, &CandidateGeneration::Unknown)
        .unwrap();
    if let DeliveryEvent::ParentCheckpoint(p) = &e.delivery.event {
        r.acknowledge_checkpoint(db.replay_checkpoint(scope).unwrap().unwrap())
            .unwrap();
        r.acknowledge_parent(p.observation.child.slot);
    }
}
#[tokio::test]
async fn missing_null_and_conflicting_http_history_keep_financial_hold() {
    for mode in ["missing", "null", "conflict"] {
        let server = servers(mode, 48, Arc::new(AtomicBool::new(false))).await;
        let c = config(&server);
        let wallets = HashSet::from([bs58::encode([3; 32]).into_string()]);
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("negative.sqlite");
        let (mut db, scope) = initialize(&path, &c, &wallets);
        let mut r = DeliveryReceiver::start_recovering_labeled(
            &c,
            format!("negative-{mode}"),
            wallets,
            None,
            db.replay_checkpoint(&scope).unwrap(),
        )
        .unwrap();
        let hold = r.http_continuity_hold().unwrap();
        assert!(hold.load(Ordering::Acquire));
        tokio::time::timeout(Duration::from_secs(4),async {
            let mut rejected=false;loop {
                match r.next().await {
                    Ok(Some(e))=>{persist(&mut db,&r,&e,&scope);if matches!(&e.delivery.event,DeliveryEvent::Session(SessionGap::Rejected(s))if s=="HttpRecoveryRefused"){rejected=true;}}
                    Err(e)=>{assert!(rejected);assert!(e.to_string().contains("confirmed_http_recovery_refused"));break;}
                    Ok(None)=>panic!("incomplete chain silently accepted"),
                }
            }
        }).await.unwrap();
        assert!(hold.load(Ordering::Acquire));
        let recovery = r.ingress_snapshot().processing.http_recovery;
        assert!(!recovery.caught_up_to_anchor);
        assert!(
            recovery.durable_completed_slot < 48,
            "valid partial progress stays explicit; contradictory live anchor is not accepted"
        );
        r.stop();
        assert!(
            server
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|v| v["method"] == "getBlock")
                .count()
                <= 4
        );
    }
}
#[tokio::test]
async fn restart_mid_catchup_uses_committed_cursor_and_accepts_skipped_slots() {
    let delay = Arc::new(AtomicBool::new(true));
    let first = servers("valid", 48, delay).await;
    let c = config(&first);
    let wallets = HashSet::from([bs58::encode([3; 32]).into_string()]);
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("restart.sqlite");
    let (mut db, scope) = initialize(&path, &c, &wallets);
    let mut r = DeliveryReceiver::start_recovering_labeled(
        &c,
        "before-restart".into(),
        wallets.clone(),
        None,
        db.replay_checkpoint(&scope).unwrap(),
    )
    .unwrap();
    let hold = r.http_continuity_hold().unwrap();
    tokio::time::timeout(Duration::from_secs(3),async {
        loop {let e=r.next().await.unwrap().unwrap();persist(&mut db,&r,&e,&scope);
            if matches!(&e.delivery.event,DeliveryEvent::ParentCheckpoint(p)if p.observation.child.slot==45){break;}}
    }).await.unwrap();
    assert!(hold.load(Ordering::Acquire));
    assert_eq!(
        db.replay_checkpoint(&scope)
            .unwrap()
            .unwrap()
            .block
            .observation
            .child
            .slot,
        45
    );
    r.stop();
    drop(r);
    drop(db);
    drop(first);
    let second = servers("valid", 50, Arc::new(AtomicBool::new(false))).await;
    let c = config(&second);
    let mut db = inbox(&path);
    let saved = db.replay_checkpoint(&scope).unwrap().unwrap();
    assert_eq!(saved.from_slot, 42);
    let mut r = DeliveryReceiver::start_recovering_labeled(
        &c,
        "after-restart".into(),
        wallets,
        None,
        Some(saved),
    )
    .unwrap();
    let hold = r.http_continuity_hold().unwrap();
    assert!(hold.load(Ordering::Acquire));
    tokio::time::timeout(Duration::from_secs(3),async {
        loop {let e=r.next().await.unwrap().unwrap();persist(&mut db,&r,&e,&scope);
            if matches!(&e.delivery.event,DeliveryEvent::ParentCheckpoint(p)if p.observation.child.slot==50){break;}}
        while hold.load(Ordering::Acquire){tokio::time::sleep(Duration::from_millis(5)).await;}
    }).await.unwrap();
    let s = r.ingress_snapshot().processing.http_recovery;
    assert_eq!(s.first_from_slot, 42);
    assert_eq!(s.live_anchor_slot, 50);
    assert_eq!(s.durable_completed_slot, 50);
    assert!(s.caught_up_to_anchor);
    assert!(s.recovered_blocks >= 4);
    assert!(!second
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|v| v["method"] == "getBlock" && v["params"][0] == 40));
    r.stop();
    assert!(hold.load(Ordering::Acquire));
}
#[tokio::test]
async fn live_refusal_closes_hold_before_awaiting_a_full_delivery_queue() {
    for mode in ["live-gap", "live-adapter"] {
        let server = servers(mode, 40, Arc::new(AtomicBool::new(false))).await;
        let mut c = config(&server);
        c.yellowstone_association.as_mut().unwrap().queue.count = 1;
        let wallets = HashSet::from([bs58::encode([3; 32]).into_string()]);
        let tmp = tempfile::tempdir().unwrap();
        let (mut db, scope) = initialize_empty(&tmp.path().join("live.sqlite"), &c, &wallets);
        let mut r =
            DeliveryReceiver::start_recovering_labeled(&c, mode.into(), wallets, None, None)
                .unwrap();
        let hold = r.http_continuity_hold().unwrap();
        let first = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let e = r.next().await.unwrap().unwrap();
                persist(&mut db, &r, &e, &scope);
                if matches!(&e.delivery.event, DeliveryEvent::ParentCheckpoint(p)
                    if p.observation.child.slot == 40)
                {
                    break e;
                }
            }
        })
        .await
        .unwrap();
        // Keep the first permit while its real SQLite ACK releases fresh input.
        tokio::time::timeout(Duration::from_secs(1), async {
            while hold.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        drop(first);
        // Slot 42 now owns the only output permit; rejection cannot be emitted.
        tokio::time::timeout(Duration::from_secs(1), async {
            while !hold.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(r.ingress_snapshot().last_parent_slot, 42);
        let queued = r.next().await.unwrap().unwrap();
        assert!(
            matches!(&queued.delivery.event, DeliveryEvent::ParentCheckpoint(p)
            if p.observation.child.slot == 42)
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), r.next())
                .await
                .is_err(),
            "rejection is blocked by the held output permit, while hold is already true"
        );
        assert!(hold.load(Ordering::Acquire));
        drop(queued);
        let rejected = r.next().await.unwrap().unwrap();
        assert!(matches!(
            &rejected.delivery.event,
            DeliveryEvent::Session(SessionGap::Rejected(_))
        ));
        r.stop();
    }
}
