//! Accepted anchor comparisons now retain both exact representations to forward.
use super::*;
use crate::protected_delivery::ProtectedDelivery;
use crate::source::http_recovery::estimated_block_time_tests::{pair03, pair03_http_body};
use crate::source::tests::anchor_tail_pair::load;
use copybot_config::ProtectedRecoveryConfig;
use copybot_storage_core::capture_scope::CaptureStore;
use prost::Message;
use std::collections::BTreeMap;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protected_estimated_time_dual_anchor_provenance() {
    let (grpc, _) = pair03();
    run(
        grpc,
        pair03_http_body(),
        "ESTIMATED_BLOCK_TIME_METADATA",
        ProtectedSourceKind::CompleteLiveBlock,
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires sealed COPY08 pair via COPYBOT_ANCHOR_PAIR_DIR; LOCAL servers only"]
async fn protected_terminal_unwind_dual_anchor_provenance() {
    let pair = load();
    run(
        pair.grpc,
        pair.raw_http,
        "SUCCESSFUL_TERMINAL_UNWIND",
        ProtectedSourceKind::CompleteLiveBlock,
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires sealed COPY09 pair via COPYBOT_ANCHOR_PAIR_DIR; LOCAL servers only"]
async fn protected_selected_http_dual_anchor_provenance() {
    let pair = load();
    run(
        pair.grpc,
        pair.raw_http,
        "HTTP_COMPLETES_LIVE_PAYLOAD",
        ProtectedSourceKind::ConfirmedHttp,
    )
    .await;
}
async fn run(
    grpc: SubscribeUpdateBlock,
    raw_http: Vec<u8>,
    comparison: &str,
    selected_source: ProtectedSourceKind,
) {
    let anchor = grpc.slot;
    let prior = grpc.parent_slot;
    let next = anchor + 1;
    let prior_raw =
        anchor_complete_replay::modeled(prior, prior - 1, &hash(201), &grpc.parent_blockhash);
    let overlap_raw = anchor_complete_replay::modeled(prior - 1, prior - 2, &hash(200), &hash(201));
    let next_raw = anchor_complete_replay::modeled(next, anchor, &grpc.blockhash, &hash(202));
    let prior_block = crate::normalize_confirmed_http_block(prior, &prior_raw).unwrap();
    let next_block = crate::normalize_confirmed_http_block(next, &next_raw).unwrap();
    let server = anchor_complete_replay::local_server(
        BTreeMap::from([
            (prior - 1, anchor_complete_replay::encoded(&overlap_raw)),
            (prior, anchor_complete_replay::encoded(&prior_raw)),
            (anchor, raw_http.clone()),
            (next, anchor_complete_replay::encoded(&next_raw)),
        ]),
        vec![grpc.clone(), next_block],
    )
    .await;
    let mut c = config(&server);
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().canonicalize().unwrap();
    let capture = directory.join("capture.db");
    let b = c.yellowstone_association.as_mut().unwrap();
    b.input_bytes = 8 << 20;
    b.blocks.bytes = 32 << 20;
    b.history.bytes = 8 << 20;
    b.outputs.bytes = 8 << 20;
    b.queue.bytes = 32 << 20;
    b.metadata_bytes = 16 << 20;
    let h = c.yellowstone_http_recovery.as_mut().unwrap();
    h.max_response_bytes = 8 << 20;
    h.range_slots = 4;
    h.timeout_ms = 10000;
    let normalized_http = crate::normalize_confirmed_http_block(
        anchor,
        &serde_json::from_slice::<Value>(&raw_http).unwrap()["result"],
    )
    .unwrap();
    let wallet = anchor_complete_replay::wallet(
        if selected_source == ProtectedSourceKind::ConfirmedHttp {
            &normalized_http
        } else {
            &grpc
        },
        &c,
    );
    let sql = rusqlite::Connection::open(&capture).unwrap();
    sql.execute_batch(copybot_storage_core::capture_scope::SCHEMA)
        .unwrap();
    sql.execute(
        "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,100000,1073741824)",
        [],
    )
    .unwrap();
    sql.execute("INSERT INTO capture_requests(request_key,payload,expires) VALUES('LOCAL-anchor','{}',1e100)",[]).unwrap();
    sql.execute("INSERT INTO capture_members VALUES(1,?)", [&wallet])
        .unwrap();
    drop(sql);
    c.capture_scope_db = Some(capture.to_str().unwrap().into());
    c.capture_recovery = Some(ProtectedRecoveryConfig {
        policy: "protected_complete_recovery_v1".into(),
        checkpoint_directory: directory.to_str().unwrap().into(),
        migrations_directory: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../migrations")
            .canonicalize()
            .unwrap()
            .to_str()
            .unwrap()
            .into(),
        max_scope_wallets: 128,
        max_scope_revisions: 128,
        provenance_policy: "historical_missed_no_execution_v1".into(),
    });
    let mut store = CaptureStore::open(&capture).unwrap();
    store.start().unwrap();
    let scope = store.prepare_scope().unwrap();
    let mut actor = ProtectedDelivery::open(&c, store, scope).unwrap();
    let seed = Delivery {
        session: "LOCAL-modeled-prior".into(),
        sequence: 0,
        arrival_offset_ns: 0,
        event: DeliveryEvent::ParentCheckpoint(BlockCheckpoint {
            scope: actor.replay_scope.clone(),
            observation: copybot_core_types::association_parent::ParentObservation {
                child: copybot_core_types::association_parent::BlockKey {
                    slot: prior_block.slot,
                    hash: prior_block.blockhash.clone(),
                },
                parent: copybot_core_types::association_parent::BlockKey {
                    slot: prior_block.parent_slot,
                    hash: prior_block.parent_blockhash.clone(),
                },
                issue: None,
            },
            executed_transaction_count: 0,
            supplied_transaction_count: 0,
            claims: vec![],
        }),
    };
    let head = actor.persist(&seed).unwrap().unwrap();
    assert!(actor.commit_checkpoint(&head, 2_000_000_000.).unwrap());
    let mut receiver = DeliveryReceiver::start_recovering_labeled(
        &c,
        "LOCAL-dual-anchor".into(),
        HashSet::from([wallet]),
        None,
        Some(head),
    )
    .unwrap();
    let mut complete = false;
    let mut forward = 0;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let envelope = receiver.next().await.unwrap().unwrap();
            if let Some(head) = actor.persist(&envelope.delivery).unwrap() {
                if actor.store.recovery_outbox_pending().unwrap() {
                    assert!(!actor.commit_checkpoint(&head, 2_000_000_000.).unwrap());
                    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
                    let out = std::process::Command::new(
                        std::env::var("CAPTURE_TEST_PYTHON").unwrap_or_else(|_| "python3".into()),
                    )
                    .arg(root.join("tools/tests/capture_recovery_consumer_fixture.py"))
                    .arg(&capture)
                    .arg(directory.join("ledger.db"))
                    .output()
                    .unwrap();
                    assert!(
                        out.status.success(),
                        "{}",
                        String::from_utf8_lossy(&out.stderr)
                    );
                    let value: Value = serde_json::from_slice(&out.stdout).unwrap();
                    assert_eq!(value["ledger_unchanged"], true);
                    forward = value["dispositions"].as_array().unwrap().len();
                }
                assert!(actor.commit_checkpoint(&head, 2_000_000_000.).unwrap());
                complete = head.block.observation.child.slot == next;
                receiver.acknowledge_checkpoint(head).unwrap();
                if complete {
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    assert!(complete);
    assert!(forward > 0);
    receiver.stop();
    let sql = rusqlite::Connection::open(&capture).unwrap();
    let mut q = sql
        .prepare(
            "SELECT evidence_id,raw,provenance_json FROM capture_recovery_evidence WHERE slot=?",
        )
        .unwrap();
    let proofs: Vec<(String, Vec<u8>, String)> = q
        .query_map([anchor.to_string()], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        proofs.len(),
        2,
        "both original representations retained before checkpoint"
    );
    let live = proofs
        .iter()
        .find(|(_, raw, _)| raw == &grpc.encode_to_vec())
        .unwrap();
    let http = proofs
        .iter()
        .find(|(_, raw, _)| serde_json::from_slice::<Value>(raw).is_ok())
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&http.1).unwrap()["result"],
        serde_json::from_slice::<Value>(&raw_http).unwrap()["result"]
    );
    let (selected, other) = if selected_source == ProtectedSourceKind::ConfirmedHttp {
        (http, live)
    } else {
        (live, http)
    };
    let link:(String,String,String)=sql.query_row("SELECT other_evidence_sha256,json_extract(metadata_json,'$.comparison'),json_extract(metadata_json,'$.selected_source') FROM capture_recovery_anchor_links WHERE evidence_sha256=?",[&selected.0],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(
        link,
        (
            other.0.clone(),
            comparison.into(),
            serde_json::to_value(selected_source)
                .unwrap()
                .as_str()
                .unwrap()
                .into()
        )
    );
    let wrong: i64 = sql
        .query_row(
            "SELECT count(*) FROM capture_recovery r WHERE slot=? AND evidence_id!=?",
            rusqlite::params![anchor.to_string(), selected.0],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        wrong, 0,
        "selected original live Info cannot be linked as raw HTTP equality"
    );
}
