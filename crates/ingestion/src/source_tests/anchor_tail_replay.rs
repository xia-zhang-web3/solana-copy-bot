//! One saved anchor through actual loopback recovery, commit/readback ACK and release.
use super::*;
use crate::source::http_recovery::normalize_confirmed_http_block;
use crate::source::tests::anchor_tail_pair::load;
use crate::source::yellowstone_facts::decode_yellowstone_swap_facts;
use copybot_core_types::association_parent::{BlockKey, ParentObservation};
use std::collections::BTreeMap;

// Reuse the accepted finite LOCAL server, never a provider. Its modeled mode
// preserves JSON result values while rebinding/re-encoding the RPC envelope.
#[path = "saved_reward_replay/local_servers.rs"]
mod transport;
mod corpus {
    pub(super) fn result(body: &[u8]) -> serde_json::Value {
        serde_json::from_slice::<serde_json::Value>(body).unwrap()["result"].clone()
    }
}
fn modeled(slot: u64, parent: u64, previous: &str, hash: &str) -> Value {
    json!({"parentSlot":parent,"previousBlockhash":previous,"blockhash":hash,
        "blockTime":1700000000,"blockHeight":slot,"transactions":[],"rewards":[]})
}
fn encoded(raw: &Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"jsonrpc":"2.0","id":0,"result":raw})).unwrap()
}
fn saved_checkpoint(scope: &ReplayScope, b: &SubscribeUpdateBlock) -> BlockCheckpoint {
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
fn wallet(block: &SubscribeUpdateBlock, c: &IngestionConfig) -> String {
    let rays: HashSet<_> = c.raydium_program_ids.iter().cloned().collect();
    let pumps: HashSet<_> = c.pumpswap_program_ids.iter().cloned().collect();
    let interest = rays.union(&pumps).cloned().collect();
    block
        .transactions
        .iter()
        .find_map(|info| {
            decode_yellowstone_swap_facts(
                &SubscribeUpdateTransaction {
                    slot: block.slot,
                    transaction: Some(info.clone()),
                    ..Default::default()
                },
                &interest,
                &rays,
                &pumps,
            )
            .facts
            .ok()
            .flatten()
            .map(|f| f.signer)
        })
        .expect("saved anchor has actual checked swap")
}

#[tokio::test]
#[ignore = "pinned COPY08 pair; actual loopback only, no provider or finances"]
async fn saved_anchor_tail_http_live_committed_ack_releases_hold_then_next_checkpoint() {
    let pair = load();
    let anchor = pair.grpc.slot;
    let prior = pair.grpc.parent_slot;
    let next = anchor + 1;
    let prior_raw = modeled(prior, prior - 1, &hash(201), &pair.grpc.parent_blockhash);
    let overlap_raw = modeled(prior - 1, prior - 2, &hash(200), &hash(201));
    let next_raw = modeled(next, anchor, &pair.grpc.blockhash, &hash(202));
    let prior_block = normalize_confirmed_http_block(prior, &prior_raw).unwrap();
    let next_block = normalize_confirmed_http_block(next, &next_raw).unwrap();
    let server = transport::start(
        BTreeMap::from([
            (prior - 1, encoded(&overlap_raw)),
            (prior, encoded(&prior_raw)),
            (anchor, pair.raw_http.clone()),
            (next, encoded(&next_raw)),
        ]),
        None,
        vec![pair.grpc.clone(), next_block],
    )
    .await;
    let mut c = config(&server);
    let budgets = c.yellowstone_association.as_mut().unwrap();
    budgets.input_bytes = 8 << 20;
    budgets.blocks.bytes = 32 << 20;
    budgets.history.bytes = 8 << 20;
    budgets.outputs.bytes = 8 << 20;
    budgets.queue.bytes = 8 << 20;
    let http = c.yellowstone_http_recovery.as_mut().unwrap();
    http.max_response_bytes = 8 << 20;
    http.range_slots = 4;
    http.timeout_ms = 10_000;
    let wallets = HashSet::from([wallet(&pair.http, &c)]);
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("anchor-ack.sqlite");
    let (mut db, scope) = initialize_empty(&path, &c, &wallets);
    db.persist(
        &Delivery {
            session: "LOCAL-modeled-prior".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::ParentCheckpoint(saved_checkpoint(&scope, &prior_block)),
        },
        &CandidateGeneration::Unknown,
    )
    .unwrap();
    let mut r = DeliveryReceiver::start_recovering_labeled(
        &c,
        "LOCAL-saved-anchor-tail".into(),
        wallets,
        None,
        db.replay_checkpoint(&scope).unwrap(),
    )
    .unwrap();
    let hold = r.http_continuity_hold().unwrap();
    let mut admissions = vec![];
    let mut proof = None;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let e = r
                .next()
                .await
                .unwrap()
                .expect("active LOCAL Reader must continue");
            if let DeliveryEvent::Admission(a) = &e.delivery.event {
                assert!(
                    matches!(a.message_time, MessageTime::RecoveredBlock { .. }),
                    "historical anchor never becomes fresh SOURCE"
                );
                admissions.push(a.clone());
            }
            if let DeliveryEvent::ParentCheckpoint(p) = &e.delivery.event {
                let slot = p.observation.child.slot;
                if slot == anchor {
                    assert!(hold.load(Ordering::Acquire));
                    let before = db.replay_checkpoint(&scope).unwrap().unwrap();
                    assert_eq!(before.block.observation.child.slot, prior);
                    db.persist(&e.delivery, &CandidateGeneration::Unknown)
                        .unwrap();
                    let committed = db.replay_checkpoint(&scope).unwrap().unwrap();
                    assert_eq!(committed.block.observation.child.slot, anchor);
                    assert_eq!(committed.block.observation.child.hash, pair.grpc.blockhash);
                    assert!(
                        hold.load(Ordering::Acquire),
                        "SQLite commit alone cannot clear hold before ACK"
                    );
                    r.acknowledge_checkpoint(committed.clone()).unwrap();
                    r.acknowledge_parent(anchor);
                    while hold.load(Ordering::Acquire) {
                        tokio::task::yield_now().await;
                    }
                    let released = r.continuity_snapshot();
                    assert!(
                        released.context.active_reader.is_some(),
                        "Reader remains active at verified release"
                    );
                    proof = Some(json!({"before_ack":before,"committed_anchor":committed,
                        "hold_before_commit":true,"hold_after_commit_before_ACK":true,
                        "hold_after_ACK":false,"release_context":released}));
                    continue;
                }
                persist(&mut db, &r, &e, &scope);
                if slot == next {
                    break;
                }
            } else {
                persist(&mut db, &r, &e, &scope);
            }
        }
    })
    .await
    .unwrap();
    let committed_next = db.replay_checkpoint(&scope).unwrap().unwrap();
    assert_eq!(committed_next.block.observation.child.slot, next);
    assert_eq!(
        committed_next.block.observation.parent.hash,
        pair.grpc.blockhash
    );
    assert!(!hold.load(Ordering::Acquire));
    assert!(!admissions.is_empty());
    let processing = r.ingress_snapshot().processing.http_recovery;
    assert!(processing.caught_up_to_anchor);
    assert_eq!(processing.live_anchor_slot, anchor);
    assert!(server
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|q| q["method"] == "getBlock" && q["params"][0] == anchor));
    r.stop();
    drop(r);
    drop(db);
    let sql = rusqlite::Connection::open(&path).unwrap();
    for table in ["orders", "fills", "positions", "copy_signals"] {
        assert_eq!(
            sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row
                .get::<_, u64>(0))
                .unwrap(),
            0
        );
    }
    let witness = json!({"kind":"LOCAL_SAVED_ANCHOR_ACTUAL_HTTP_LIVE_SQLITE_ACK_RELEASE",
        "saved_anchor":anchor,"actual_transactions":pair.grpc.transactions.len(),
        "modeled_prior":prior,"modeled_next":next,"historical_admissions":admissions,
        "modeled_replay_overlap_parent":prior-1,
        "causal_anchor_ACK":proof.expect("anchor was committed before next"),"next_durable_checkpoint":committed_next,
        "financial_rows":0,"provider_requests":0,"freshness_retagged":false,
        "transport_envelope":"LOCAL id and JSON re-encoding; original result values unchanged; original seven-file pair retained",
        "production_caps_changed":false,"LOCAL_config":"8MiB input/output,32MiB blocks;4slot range;10s HTTP"});
    if let Ok(output) = std::env::var("COPYBOT_ANCHOR_ACK_OUTPUT_DIR") {
        let output = std::path::Path::new(&output);
        std::fs::create_dir_all(output).unwrap();
        let artifact = output.join("anchor-ack.sqlite");
        assert!(!artifact.exists());
        sql.execute("VACUUM INTO ?1", [artifact.to_str().unwrap()])
            .unwrap();
        let mut out = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(output.join("WITNESS.json"))
            .unwrap();
        use std::io::Write;
        serde_json::to_writer_pretty(&mut out, &witness).unwrap();
        out.write_all(b"\n").unwrap();
        out.sync_all().unwrap();
    }
    eprintln!("SAVED_ANCHOR_ACK real_transactions={} historical_admissions={} hold_before_ACK=true hold_after_ACK=false next_checkpoint={next} financial_rows=0 provider_requests=0",pair.grpc.transactions.len(),witness["historical_admissions"].as_array().unwrap().len());
}
