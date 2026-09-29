//! Actual saved04 results through Rust→real HTTPS broker→durable ACK→live tail.
mod fixture;
mod live;
#[path = "../lease_docker_replay_tests.rs"]
mod lease_docker_replay_tests;
use super::{config, initialize_empty, persist, Servers};
use crate::{source::http_recovery::ConfirmedHttpRecovery, DeliveryReceiver};
use copybot_core_types::{association_delivery::*, association_parent::*, association_recovery::*};
use fixture::{FAULT, FIRST, LAST};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc, Mutex},
    time::Duration,
};
fn root() -> PathBuf {
    let path = PathBuf::from(std::env::var("COPYBOT_HTTP_DELIVERY_EVIDENCE_DIR").unwrap());
    std::fs::create_dir_all(&path).unwrap();
    path
}
fn checkpoint(
    scope: &ReplayScope,
    b: &yellowstone_grpc_proto::prelude::SubscribeUpdateBlock,
) -> BlockCheckpoint {
    BlockCheckpoint {
        scope: scope.clone(),
        observation: ParentObservation {
            child: BlockKey {
                slot: b.slot,
                hash: b.blockhash.clone(),
            },
            parent: BlockKey {
                slot: b.parent_slot,
                hash: b.parent_blockhash.clone(),
            },
            issue: None,
        },
        executed_transaction_count: b.executed_transaction_count,
        supplied_transaction_count: b.transactions.len() as u64,
        claims: vec![],
    }
}
fn settings(http: &str, grpc: &str) -> copybot_config::IngestionConfig {
    let s = Servers {
        http: http.into(),
        grpc: grpc.into(),
        tasks: vec![],
        calls: Arc::new(Mutex::new(vec![])),
    };
    let mut c = config(&s);
    let b = c.yellowstone_association.as_mut().unwrap();
    b.input_bytes = 8 << 20;
    b.blocks.count = 64;
    b.blocks.bytes = 128 << 20;
    b.outputs.bytes = 16 << 20;
    b.queue.bytes = 16 << 20;
    // Accepted live package metadata limit; the tiny prior synthetic fixture's
    // 1MiB budget cannot hold eleven actual full-block identity maps.
    b.metadata_bytes = 96 << 20;
    b.inbox.count = 4096;
    b.inbox.bytes = 64 << 20;
    let h = c.yellowstone_http_recovery.as_mut().unwrap();
    h.range_slots = 1024;
    h.max_response_bytes = 8 << 20;
    h.timeout_ms = 15000;
    h.fetch_concurrency = 4;
    c
}
fn witness(path: &Path, dbpath: &Path, decision: Value) {
    let sql = rusqlite::Connection::open(dbpath).unwrap();
    for table in ["orders", "fills", "positions", "copy_signals"] {
        assert_eq!(
            sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            0
        );
    }
    let dest = path.join("ACK.sqlite");
    assert!(!dest.exists());
    sql.execute("VACUUM INTO ?1", [dest.to_str().unwrap()])
        .unwrap();
    std::fs::write(
        path.join("RUST_CAUSAL_RESULT.json"),
        serde_json::to_vec_pretty(&decision).unwrap(),
    )
    .unwrap();
}
async fn until(
    db: &mut copybot_storage_core::association_inbox::AssociationInbox,
    r: &mut DeliveryReceiver,
    scope: &ReplayScope,
    target: u64,
) -> Vec<u64> {
    tokio::time::timeout(Duration::from_secs(35), async {
        let mut ack = vec![];
        loop {
            let e = r.next().await.unwrap().unwrap();
            assert!(!matches!(e.delivery.event, DeliveryEvent::Admission(_)));
            persist(db, r, &e, scope);
            if let DeliveryEvent::ParentCheckpoint(b) = e.delivery.event {
                assert_eq!(
                    db.replay_checkpoint(scope)
                        .unwrap()
                        .unwrap()
                        .block
                        .observation
                        .child,
                    b.observation.child
                );
                ack.push(b.observation.child.slot);
                if b.observation.child.slot == target {
                    break;
                }
            }
        }
        ack
    })
    .await
    .unwrap()
}
fn requests(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path.join("upstream-requests.jsonl"))
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect()
}
#[tokio::test]
#[ignore = "explicit private saved04/local real broker only; no provider"]
async fn delayed_and_transient_reads_reach_saved_ack_anchor_and_continuing_stream() {
    let output = root();
    let corpus_path = output.join("corpus");
    let corpus = fixture::load(&corpus_path);
    for scenario in ["delay5", "close-once"] {
        let out = output.join(scenario);
        let broker = fixture::start(&out, &corpus_path, scenario, 480).await;
        let messages = std::iter::once(corpus.blocks[&LAST].clone())
            .chain((LAST + 1..=LAST + 32).map(|s| fixture::tail(&corpus, s)))
            .collect();
        let producer = live::start(messages).await;
        let c = settings(&broker.url, &producer.url);
        let wallets = HashSet::from([bs58::encode([242u8; 32]).into_string()]);
        let dbpath = out.join("runtime.sqlite");
        let (mut db, scope) = initialize_empty(&dbpath, &c, &wallets);
        db.persist(
            &Delivery {
                session: "existing-saved545".into(),
                sequence: 0,
                arrival_offset_ns: 0,
                event: DeliveryEvent::ParentCheckpoint(checkpoint(
                    &scope,
                    &corpus.blocks[&(FIRST + 1)],
                )),
            },
            &CandidateGeneration::Unknown,
        )
        .unwrap();
        let mut r = DeliveryReceiver::start_recovering_labeled(
            &c,
            format!("delivery-{scenario}"),
            wallets,
            None,
            db.replay_checkpoint(&scope).unwrap(),
        )
        .unwrap();
        let mut acks = until(&mut db, &mut r, &scope, FAULT - 1).await;
        let sent_during = producer.sent.load(Ordering::Relaxed);
        let hold_during = r.http_continuity_hold().unwrap().load(Ordering::Acquire);
        acks.extend(until(&mut db, &mut r, &scope, LAST + 28).await);
        let telemetry = r.ingress_snapshot();
        assert!(telemetry.processing.http_recovery.caught_up_to_anchor);
        assert_eq!(telemetry.processing.http_recovery.live_anchor_slot, LAST);
        assert!(
            producer.sent.load(Ordering::Relaxed) < 33,
            "producer is still running after catchup"
        );
        if scenario == "delay5" {
            assert!(sent_during > 1);
            assert!(hold_during);
        }
        assert_eq!(acks, (FIRST + 1..=LAST + 28).collect::<Vec<_>>());
        let calls = requests(&out);
        let fault: Vec<_> = calls
            .iter()
            .filter(|r| r["method"] == "getBlock" && r["slot"] == FAULT)
            .collect();
        assert_eq!(fault.len(), if scenario == "close-once" { 2 } else { 1 });
        if fault.len() == 2 {
            assert_ne!(fault[0]["request_id"], fault[1]["request_id"]);
        }
        r.stop();
        drop(r);
        drop(db);
        witness(
            &out,
            &dbpath,
            json!({"status":"PASS_OFFLINE_REAL_BROKER_ACK", "scenario":scenario,
            "saved_blocks":11,"saved_response_bytes":corpus.bodies.values().map(Vec::len).sum::<usize>(),
            "committed_ack_slots":acks,"fault_attempts":fault.len(),"sent_during_partial_ack":sent_during,
            "hold_during_partial_ack":hold_during,"local_anchor":LAST,"live_tail_committed":LAST+28,
            "actual_probe04_anchor_reached":false,"provider_calls":0,"financial_rows":0}),
        );
        drop(producer);
        drop(broker);
    }
}
#[tokio::test]
#[ignore = "explicit private saved04/local real broker only; no provider"]
async fn retry_exhaustion_preserves_partial_cursor_and_restart_uses_same_owner_clock() {
    let output = root();
    let corpus_path = output.join("corpus");
    let corpus = fixture::load(&corpus_path);
    let out = output.join("exhaustion-restart");
    let broker = fixture::start(&out, &corpus_path, "close-always", 480).await;
    let initial_clock = std::fs::read(out.join("PROBE_CLOCK.json")).unwrap();
    let producer = live::start(vec![corpus.blocks[&LAST].clone()]).await;
    let mut c = settings(&broker.url, &producer.url);
    let wallets = HashSet::from([bs58::encode([242u8; 32]).into_string()]);
    let dbpath = out.join("runtime.sqlite");
    let (mut db, scope) = initialize_empty(&dbpath, &c, &wallets);
    db.persist(
        &Delivery {
            session: "existing-saved545".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::ParentCheckpoint(checkpoint(
                &scope,
                &corpus.blocks[&(FIRST + 1)],
            )),
        },
        &CandidateGeneration::Unknown,
    )
    .unwrap();
    let mut r = DeliveryReceiver::start_recovering_labeled(
        &c,
        "before-transient-exhaustion".into(),
        wallets.clone(),
        None,
        db.replay_checkpoint(&scope).unwrap(),
    )
    .unwrap();
    let partial = until(&mut db, &mut r, &scope, FAULT - 1).await;
    let mut rejected = false;
    tokio::time::timeout(Duration::from_secs(20),async {
        loop {match r.next().await {
            Ok(Some(e))=>{assert!(!matches!(e.delivery.event,DeliveryEvent::Admission(_)));persist(&mut db,&r,&e,&scope);
                if matches!(e.delivery.event,DeliveryEvent::Session(SessionGap::Rejected(ref why))if why=="HttpRecoveryRefused"){rejected=true;}}
            Err(e)=>{assert!(rejected);assert!(e.to_string().contains("confirmed_http_recovery_refused"));break;}
            Ok(None)=>panic!("temporary failure exhaustion silently accepted")
        }}
    }).await.unwrap();
    assert!(r.http_continuity_hold().unwrap().load(Ordering::Acquire));
    assert_eq!(
        db.replay_checkpoint(&scope)
            .unwrap()
            .unwrap()
            .block
            .observation
            .child
            .slot,
        FAULT - 1
    );
    let calls = requests(&out);
    assert_eq!(
        calls
            .iter()
            .filter(|r| r["method"] == "getBlock" && r["slot"] == FAULT)
            .count(),
        3
    );
    r.stop();
    drop(r);
    drop(producer);
    std::fs::write(out.join("REPAIRED_TEST"), b"local upstream recovered").unwrap();
    let producer = live::start(
        std::iter::once(corpus.blocks[&LAST].clone())
            .chain((LAST + 1..=LAST + 4).map(|s| fixture::tail(&corpus, s)))
            .collect(),
    )
    .await;
    c.yellowstone_grpc_url = producer.url.clone();
    let mut r = DeliveryReceiver::start_recovering_labeled(
        &c,
        "after-transient-restart".into(),
        wallets,
        None,
        db.replay_checkpoint(&scope).unwrap(),
    )
    .unwrap();
    let completed = until(&mut db, &mut r, &scope, LAST + 3).await;
    assert_eq!(completed, (FAULT - 1..=LAST + 3).collect::<Vec<_>>());
    assert_eq!(
        std::fs::read(out.join("PROBE_CLOCK.json")).unwrap(),
        initial_clock
    );
    let after = requests(&out);
    for call in &after[calls.len()..] {
        if call["method"] == "getBlock" {
            assert!(call["slot"].as_u64().unwrap() >= FAULT - 2);
        }
    }
    r.stop();
    drop(r);
    drop(db);
    witness(
        &out,
        &dbpath,
        json!({"status":"PASS_PARTIAL_CURSOR_RESTART", "partial_acks":partial,
        "partial_head":FAULT-1,"attempts_before_restart":3,"restart_acks":completed,
        "immutable_clock_sha256":format!("{:x}",sha2::Sha256::digest(&initial_clock)),
        "owner_session_restarted":false,"provider_calls":0,"financial_rows":0}),
    );
}
#[tokio::test]
#[ignore = "explicit private saved04/local real broker only; no provider"]
async fn expired_original_session_refuses_first_outbound_after_new_rust_client() {
    let output = root();
    let corpus_path = output.join("corpus");
    fixture::load(&corpus_path);
    let out = output.join("session-expiry");
    let broker = fixture::start(&out, &corpus_path, "normal", 2).await;
    let clock = std::fs::read(out.join("PROBE_CLOCK.json")).unwrap();
    let client =
        ConfirmedHttpRecovery::new(&broker.url, None, 1024, 8 << 20, Duration::from_secs(15))
            .unwrap();
    assert_eq!(client.slots(FIRST, LAST).await.unwrap().len(), 11);
    let calls_before = requests(&out).len();
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert!(client
        .slots(FIRST, LAST)
        .await
        .unwrap_err()
        .to_string()
        .contains("session_deadline_exhausted"));
    let restarted =
        ConfirmedHttpRecovery::new(&broker.url, None, 1024, 8 << 20, Duration::from_secs(15))
            .unwrap();
    let error = restarted.slots(FIRST, LAST).await.unwrap_err().to_string();
    assert!(error.contains("reason=session_deadline_exhausted"));
    assert!(error.contains("reservation_id=unknown"));
    assert_eq!(requests(&out).len(), calls_before);
    assert_eq!(std::fs::read(out.join("PROBE_CLOCK.json")).unwrap(), clock);
    std::fs::write(out.join("RUST_CAUSAL_RESULT.json"),serde_json::to_vec_pretty(&json!({
        "status":"PASS_IMMUTABLE_SESSION_EXPIRY", "first_outbound":calls_before,"outbound_after_expiry":0,
        "new_rust_client_refused":error,"provider_calls":0})).unwrap()).unwrap();
}
use sha2::Digest;
