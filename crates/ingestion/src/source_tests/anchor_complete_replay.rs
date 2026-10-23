//! COPY09 complete HTTP selection through LOCAL recovery, SQLite ACK and restart.
use super::*;
use crate::source::http_recovery::normalize_confirmed_http_block;
use crate::source::tests::anchor_tail_pair::load;
use crate::source::yellowstone_facts::decode_yellowstone_swap_facts;
use copybot_core_types::association_parent::{BlockKey, ParentObservation};
use prost::Message;
use std::collections::BTreeMap;

// Reuse the accepted finite LOCAL server, never a provider. Its modeled mode
// preserves JSON result values while rebinding/re-encoding the RPC envelope.
#[path = "saved_reward_replay/local_servers.rs"]
pub(super) mod transport;
mod corpus {
    pub(super) fn result(body: &[u8]) -> serde_json::Value {
        serde_json::from_slice::<serde_json::Value>(body).unwrap()["result"].clone()
    }
}
pub(super) async fn local_server(
    bodies: BTreeMap<u64, Vec<u8>>,
    messages: Vec<SubscribeUpdateBlock>,
) -> Servers {
    transport::start(bodies, None, messages).await
}
pub(super) fn modeled(slot: u64, parent: u64, previous: &str, hash: &str) -> Value {
    json!({"parentSlot":parent,"previousBlockhash":previous,"blockhash":hash,
        "blockTime":1700000000,"blockHeight":slot,"transactions":[],"rewards":[]})
}
pub(super) fn encoded(raw: &Value) -> Vec<u8> {
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
pub(super) fn wallet(block: &SubscribeUpdateBlock, c: &IngestionConfig) -> String {
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

fn exact_info(info: &SubscribeUpdateTransactionInfo) -> InfoIdentity {
    InfoIdentity {
        encoded: info.encode_to_vec(),
        float_bits: info
            .meta
            .iter()
            .flat_map(|m| m.pre_token_balances.iter().chain(&m.post_token_balances))
            .map(|r| r.ui_token_amount.as_ref().map(|a| a.ui_amount.to_bits()))
            .collect(),
    }
}

#[tokio::test]
#[ignore = "pinned COPY09 pair; selected completeHTTP, LOCAL transport only"]
async fn saved_anchor_complete_http_selected_ack_release_then_restart_preserves_identity() {
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
        "LOCAL-saved-anchor-complete".into(),
        wallets.clone(),
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
                let expected = pair
                    .http
                    .transactions
                    .iter()
                    .find(|i| bs58::encode(&i.signature).into_string() == a.facts.signature)
                    .unwrap();
                assert_eq!(
                    a.info,
                    exact_info(expected),
                    "actual admission must contain full selected HTTP Info"
                );
                assert_eq!(
                    a.message_time,
                    MessageTime::RecoveredBlock {
                        block_time: pair.http.block_time.as_ref().map(|t| t.timestamp)
                    }
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
    let first: Vec<_> = admissions
        .iter()
        .map(|a| {
            (
                a.facts.signature.clone(),
                db.identity(&a.facts.signature).unwrap().unwrap(),
            )
        })
        .collect();
    drop(db);
    let mut db = inbox(&path);
    let saved = db.replay_checkpoint(&scope).unwrap().unwrap();
    assert_eq!(saved.block.observation.child.slot, next);
    let restart_anchor = next + 1;
    let restart_tail = next + 2;
    let restart_raw = modeled(restart_anchor, next, &hash(202), &hash(203));
    let tail_raw = modeled(restart_tail, restart_anchor, &hash(203), &hash(204));
    let resumed = transport::start(
        BTreeMap::from([
            (prior - 1, encoded(&overlap_raw)),
            (prior, encoded(&prior_raw)),
            (anchor, pair.raw_http.clone()),
            (next, encoded(&next_raw)),
            (restart_anchor, encoded(&restart_raw)),
            (restart_tail, encoded(&tail_raw)),
        ]),
        None,
        vec![
            normalize_confirmed_http_block(restart_anchor, &restart_raw).unwrap(),
            normalize_confirmed_http_block(restart_tail, &tail_raw).unwrap(),
        ],
    )
    .await;
    c.yellowstone_grpc_url = resumed.grpc.clone();
    c.yellowstone_http_recovery.as_mut().unwrap().broker_url = resumed.http.clone();
    let mut replay = DeliveryReceiver::start_recovering_labeled(
        &c,
        "LOCAL-complete-restart".into(),
        wallets,
        None,
        Some(saved),
    )
    .unwrap();
    let mut replay_envelopes = 0usize;
    tokio::time::timeout(Duration::from_secs(20),async {
        loop {
            let e=replay.next().await.unwrap().unwrap();
            if let DeliveryEvent::Admission(a)=&e.delivery.event {
                let (_,old)=first.iter().find(|(signature,_)|signature==&a.facts.signature)
                    .expect("restart may only replay previously persisted selected HTTP");
                assert_eq!(a,&old.admission,"replay envelope must preserve the original Admission");
                replay_envelopes+=1;
            }
            persist(&mut db,&replay,&e,&scope);
            if matches!(&e.delivery.event,DeliveryEvent::ParentCheckpoint(p)if p.observation.child.slot==restart_tail){break;}
        }
    }).await.unwrap();
    assert!(replay_envelopes>0,"actual saved HTTP overlap was replayed");
    let restart_checkpoint = db.replay_checkpoint(&scope).unwrap().unwrap();
    assert_eq!(
        restart_checkpoint.block.observation.child.slot,
        restart_tail
    );
    for (signature, old) in &first {
        let kept = db.identity(signature).unwrap().unwrap();
        assert_eq!(kept.admission, old.admission);
        assert_eq!(kept.candidate, old.candidate);
        assert_eq!(
            (&kept.first_session, kept.first_sequence),
            (&old.first_session, old.first_sequence)
        );
        assert!(!kept.conflict);
    }
    assert!(
        resumed
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|q| q["method"] == "getBlock" && q["params"][0] == anchor),
        "restart revalidates same saved complete HTTP in durable overlap"
    );
    replay.stop();
    drop(replay);
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
    let witness = json!({"kind":"LOCAL_COMPLETE_HTTP_ANCHOR_ACTUAL_SQLITE_ACK_RELEASE_RESTART",
        "saved_anchor":anchor,"actual_transactions":pair.grpc.transactions.len(),
        "modeled_prior":prior,"modeled_next":next,"historical_admissions":admissions,
        "modeled_replay_overlap_parent":prior-1,
        "causal_anchor_ACK":proof.expect("anchor was committed before next"),"next_durable_checkpoint":committed_next,
        "restart_durable_checkpoint":restart_checkpoint,"immutable_first_admissions":first.len(),
        "replay_Admission_envelopes":replay_envelopes,"new_immutable_admissions_on_restart":0,
        "selected_admission_Info":"exact direct complete HTTP including IEEE witnesses; historical origin",
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
    eprintln!("SAVED_COMPLETE_ANCHOR_ACK real_transactions={} historical_admissions={} hold_before_ACK=true hold_after_ACK=false next_checkpoint={next} financial_rows=0 provider_requests=0",pair.grpc.transactions.len(),witness["historical_admissions"].as_array().unwrap().len());
}
