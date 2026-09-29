//! Actual probe03 bodies through adapter, bridge, committed SQLite ACK and restart.
//! Local anchor985/continuations are not the probe's unachieved live anchor081.
mod corpus;
mod local_servers;
use super::{config, inbox, initialize_empty, persist};
use crate::{
    source::http_recovery::{normalize_confirmed_http_block, ConfirmedHttpRecovery},
    DeliveryReceiver,
};
use copybot_core_types::{association_delivery::*, association_parent::*, association_recovery::*};
use corpus::{FIRST, LAST};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
    sync::atomic::Ordering,
    time::Duration,
};
use yellowstone_grpc_proto::prelude::*;

fn modeled(slot: u64, parent: u64, previous: &str, hash: &str) -> Value {
    json!({"parentSlot":parent,"previousBlockhash":previous,"blockhash":hash,
        "blockTime":1700000000,"blockHeight":slot,"transactions":[],"rewards":[]})
}
fn encoded(raw: &Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"jsonrpc":"2.0","id":0,"result":raw})).unwrap()
}
fn checkpoint(scope: &ReplayScope, block: &SubscribeUpdateBlock) -> BlockCheckpoint {
    BlockCheckpoint {
        scope: scope.clone(),
        observation: ParentObservation {
            child: BlockKey {
                slot: block.slot,
                hash: block.blockhash.clone(),
            },
            parent: BlockKey {
                slot: block.parent_slot,
                hash: block.parent_blockhash.clone(),
            },
            issue: None,
        },
        executed_transaction_count: block.executed_transaction_count,
        supplied_transaction_count: block.transactions.len() as u64,
        claims: vec![],
    }
}
async fn consume(
    db: &mut copybot_storage_core::association_inbox::AssociationInbox,
    receiver: &mut DeliveryReceiver,
    scope: &ReplayScope,
    target: u64,
) -> Vec<BlockCheckpoint> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut acknowledged = vec![];
        loop {
            let envelope = receiver.next().await.unwrap().unwrap();
            assert!(
                !matches!(&envelope.delivery.event, DeliveryEvent::Admission(_)),
                "historical data never becomes a fresh trading candidate"
            );
            persist(db, receiver, &envelope, scope);
            if let DeliveryEvent::ParentCheckpoint(block) = &envelope.delivery.event {
                let head = db.replay_checkpoint(scope).unwrap().unwrap();
                assert_eq!(head.block.observation.child, block.observation.child);
                assert_eq!(
                    (head.session.as_str(), head.sequence),
                    (
                        envelope.delivery.session.as_str(),
                        envelope.delivery.sequence
                    )
                );
                acknowledged.push(block.clone());
                if block.observation.child.slot == target {
                    break;
                }
            }
        }
        let hold = receiver.http_continuity_hold().unwrap();
        while hold.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        acknowledged
    })
    .await
    .unwrap()
}
fn sqlite_provenance(path: &Path, slots: &[u64], scope: &ReplayScope) {
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    connection.pragma_update(None, "query_only", true).unwrap();
    let rows: Vec<String> = connection
        .prepare(
            "SELECT delivery FROM association_inbox_events WHERE session='saved-corpus-replay:0'",
        )
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let saved: Vec<Delivery> = rows
        .iter()
        .map(|row| serde_json::from_str(row).unwrap())
        .collect();
    for slot in slots {
        assert_eq!(saved.iter().filter(|d| matches!(&d.event, DeliveryEvent::ParentCheckpoint(b) if b.observation.child.slot == *slot && b.scope == *scope)).count(), 1);
    }
    for table in ["orders", "fills", "positions"] {
        assert_eq!(
            connection
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
    }
}
fn export_witness(path: &Path, corpus: &corpus::Corpus) {
    let Ok(output) = std::env::var("COPYBOT_PROBE03_REPLAY_EVIDENCE_DIR") else {
        return;
    };
    let output = Path::new(&output);
    std::fs::create_dir_all(output).unwrap();
    let artifact = output.join("saved-replay.sqlite");
    assert!(!artifact.exists(), "preserve previous fixture evidence");
    // This connection touches only the local test DB; VACUUM INTO captures WAL
    // facts consistently without copying a live database's base file alone.
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .execute("VACUUM INTO ?1", [artifact.to_str().unwrap()])
        .unwrap();
    let head: String = connection
        .query_row(
            "SELECT head FROM association_replay_cursor WHERE id=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let rows: Vec<String> = connection.prepare("SELECT delivery FROM association_inbox_events WHERE session='saved-corpus-replay:0' ORDER BY sequence").unwrap().query_map([], |r| r.get(0)).unwrap().map(Result::unwrap).collect();
    drop(connection);
    let mut acknowledgements = vec![];
    for wire in rows {
        let delivery: Delivery = serde_json::from_str(&wire).unwrap();
        if let DeliveryEvent::ParentCheckpoint(block) = &delivery.event {
            let slot = block.observation.child.slot;
            if !(FIRST..=LAST).contains(&slot) {
                continue;
            }
            use sha2::{Digest, Sha256};
            let body = &corpus.bodies[&slot];
            let raw = corpus::result(body);
            let fees: u64 = raw["transactions"]
                .as_array()
                .unwrap()
                .iter()
                .map(|tx| tx["meta"]["fee"].as_u64().unwrap())
                .sum();
            acknowledgements.push(
                json!({"slot":slot,"session":delivery.session,"sequence":delivery.sequence,
                "event_sha256":format!("{:x}",Sha256::digest(wire.as_bytes())),
                "response_sha256":format!("{:x}",Sha256::digest(body)),
                "checkpoint":block,"transaction_fee_lamports_sum":fees,
                "original_block_rewards":raw["rewards"]}),
            );
        }
    }
    assert_eq!(acknowledgements.len(), 4);
    let head: DurableCheckpoint = serde_json::from_str(&head).unwrap();
    assert_eq!(head.block.observation.child.slot, LAST + 3);
    assert!(std::fs::metadata(&artifact).unwrap().len() < 4 << 20);
    let witness = json!({"status":"PASS_OFFLINE_SAVED_CORPUS_ACK", "actual_saved_blocks":4,
        "actual_transaction_count":4113,"saved_real_checkpoint_acks":acknowledgements,
        "fixture_only":{"modeled_predecessor_slot":FIRST-1,"local_anchor_slot":LAST,
            "continuation_slot":LAST+1,"restart_head_slot":LAST+3,"http_envelope_change":"JSON-RPC id only"},
        "live_probe_anchor":{"slot":451592081u64,"reached_by_probe03":false,"proved_by_this_test":false},
        "raw_exact_adapter_response_count":4,"financial_rows":0,"provider_requests":0,"restart_cursor":head});
    std::fs::write(
        output.join("WITNESS.json"),
        serde_json::to_vec_pretty(&witness).unwrap(),
    )
    .unwrap();
}
#[tokio::test]
#[ignore = "explicit private sealed probe03 corpus only; loopback, no provider"]
async fn all_four_saved_fee_blocks_reach_committed_ack_and_local_anchor() {
    let path = std::env::var("COPYBOT_PROBE03_HTTP_EVIDENCE_DIR").unwrap();
    let corpus = corpus::load(Path::new(&path));
    let exact =
        local_servers::start(corpus.bodies.clone(), Some(corpus.list.clone()), vec![]).await;
    let client =
        ConfirmedHttpRecovery::new(&exact.http, None, 1024, 8 << 20, Duration::from_secs(10))
            .unwrap();
    let actual_slots = client.slots(FIRST, 451_592_081).await.unwrap();
    assert!(
        actual_slots.contains(&451_592_081),
        "observed live anchor belongs to saved slot list, not this modeled replay"
    );
    let mut parsed = BTreeMap::new();
    for (slot, original) in &corpus.bodies {
        let recovered = client.block(*slot).await.unwrap();
        assert_eq!(
            recovered.raw_response, *original,
            "direct adapter consumes exact original response bytes"
        );
        corpus::financial_facts(&corpus::result(original), &recovered.block);
        parsed.insert(*slot, recovered.block);
    }
    assert_eq!(exact.calls.lock().unwrap().len(), 5);
    drop(exact);
    // Model the unarchived predecessor only, retaining the real hash link.
    let first = &parsed[&FIRST];
    let ancestor_hash = bs58::encode([180u8; 32]).into_string();
    let predecessor = modeled(
        FIRST - 1,
        FIRST - 2,
        &ancestor_hash,
        &first.parent_blockhash,
    );
    let next_hash = bs58::encode([186u8; 32]).into_string();
    let continuation = modeled(LAST + 1, LAST, &parsed[&LAST].blockhash, &next_hash);
    let mut bodies = corpus.bodies.clone();
    bodies.insert(FIRST - 1, encoded(&predecessor));
    bodies.insert(LAST + 1, encoded(&continuation));
    let server = local_servers::start(
        bodies.clone(),
        None,
        vec![
            parsed[&LAST].clone(),
            normalize_confirmed_http_block(LAST + 1, &continuation).unwrap(),
        ],
    )
    .await;
    let mut c = config(&server);
    let budgets = c.yellowstone_association.as_mut().unwrap();
    budgets.input_bytes = 8 << 20;
    budgets.blocks.bytes = 32 << 20;
    budgets.outputs.bytes = 8 << 20;
    budgets.queue.bytes = 8 << 20;
    let http = c.yellowstone_http_recovery.as_mut().unwrap();
    http.range_slots = 1024;
    http.max_response_bytes = 8 << 20;
    http.timeout_ms = 10_000;
    http.fetch_concurrency = 4;
    let wallets = HashSet::from([bs58::encode([242u8; 32]).into_string()]);
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("saved-replay.sqlite");
    let (mut db, scope) = initialize_empty(&db_path, &c, &wallets);
    db.persist(
        &Delivery {
            session: "modeled-existing-checkpoint".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::ParentCheckpoint(checkpoint(&scope, first)),
        },
        &CandidateGeneration::Unknown,
    )
    .unwrap();
    let mut receiver = DeliveryReceiver::start_recovering_labeled(
        &c,
        "saved-corpus-replay".into(),
        wallets.clone(),
        None,
        db.replay_checkpoint(&scope).unwrap(),
    )
    .unwrap();
    let acknowledgements = consume(&mut db, &mut receiver, &scope, LAST + 1).await;
    assert_eq!(
        acknowledgements
            .iter()
            .map(|b| b.observation.child.slot)
            .collect::<Vec<_>>(),
        (FIRST..=LAST + 1).collect::<Vec<_>>()
    );
    for block in &acknowledgements[..4] {
        let original = &parsed[&block.observation.child.slot];
        assert_eq!(block.observation.child.hash, original.blockhash);
        assert_eq!(block.observation.parent.hash, original.parent_blockhash);
        assert_eq!(
            block.executed_transaction_count,
            original.executed_transaction_count
        );
        assert_eq!(
            block.supplied_transaction_count,
            original.transactions.len() as u64
        );
    }
    let telemetry = receiver.ingress_snapshot().processing.http_recovery;
    assert_eq!(telemetry.live_anchor_slot, LAST);
    assert!(telemetry.caught_up_to_anchor);
    // This counter includes the newly committed live continuation, not only HTTP.
    assert_eq!(telemetry.durable_completed_slot, LAST + 1);
    receiver.stop();
    drop(receiver);
    drop(db);
    drop(server);
    sqlite_provenance(&db_path, &(FIRST..=LAST).collect::<Vec<_>>(), &scope);
    let mut db = inbox(&db_path);
    let saved = db.replay_checkpoint(&scope).unwrap().unwrap();
    assert_eq!(saved.block.observation.child.slot, LAST + 1);
    assert_eq!(saved.from_slot, LAST);
    // Restart uses the committed cursor and catches up to another local anchor.
    let anchor_hash = bs58::encode([187u8; 32]).into_string();
    let restarted = modeled(LAST + 2, LAST + 1, &next_hash, &anchor_hash);
    let tail = modeled(
        LAST + 3,
        LAST + 2,
        &anchor_hash,
        &bs58::encode([188u8; 32]).into_string(),
    );
    bodies.insert(LAST + 2, encoded(&restarted));
    let server = local_servers::start(
        bodies,
        None,
        vec![
            normalize_confirmed_http_block(LAST + 2, &restarted).unwrap(),
            normalize_confirmed_http_block(LAST + 3, &tail).unwrap(),
        ],
    )
    .await;
    c.yellowstone_grpc_url = server.grpc.clone();
    c.yellowstone_http_recovery.as_mut().unwrap().broker_url = server.http.clone();
    let mut receiver = DeliveryReceiver::start_recovering_labeled(
        &c,
        "saved-corpus-restart".into(),
        wallets,
        None,
        Some(saved),
    )
    .unwrap();
    let acknowledgements = consume(&mut db, &mut receiver, &scope, LAST + 3).await;
    assert_eq!(
        acknowledgements
            .iter()
            .map(|b| b.observation.child.slot)
            .collect::<Vec<_>>(),
        vec![LAST + 1, LAST + 2, LAST + 3]
    );
    assert!(!server
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|r| r["method"] == "getBlock" && r["params"][0].as_u64().unwrap() < LAST));
    receiver.stop();
    drop(receiver);
    drop(db);
    drop(server);
    sqlite_provenance(&db_path, &(FIRST..=LAST).collect::<Vec<_>>(), &scope);
    export_witness(&db_path, &corpus);
    eprintln!("PROBE03_REPLAY saved_blocks=4 exact_adapter_bodies=4 real_transactions=4113 saved_checkpoint_acks=4 local_anchor=451591985 live_probe_anchor=451592081_not_reached live_continuation=451591986 restart_head=451591988 financial_rows=0 provider_requests=0");
}
