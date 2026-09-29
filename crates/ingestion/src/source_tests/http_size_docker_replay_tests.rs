//! Real saved06 facts and explicitly modeled dense logs, separate Docker roles.
use super::super::initialize_empty;
use super::{checkpoint, live, until, witness};
use crate::{
    source::http_recovery::{normalize_confirmed_http_block, ConfirmedHttpRecovery},
    DeliveryReceiver,
};
use copybot_core_types::association_delivery::*;
use prost::Message;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;
const REAL: u64 = 451_658_617;
const LAST: u64 = REAL + 4;
const CAP: usize = 16 << 20;
const WAVES: u64 = 4;
struct Docker {
    root: PathBuf,
    url: String,
    process: Child,
}
impl Drop for Docker {
    fn drop(&mut self) {
        let _ = std::fs::write(self.root.join("STOP_HOST_TEST"), b"stop");
        let limit = std::time::Instant::now() + Duration::from_secs(20);
        while std::time::Instant::now() < limit {
            if self.process.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}
fn save(path: impl AsRef<Path>, v: &Value) {
    std::fs::write(path, serde_json::to_vec_pretty(v).unwrap()).unwrap();
}
fn hash(raw: &[u8]) -> String {
    format!("{:x}", Sha256::digest(raw))
}
fn empty(slot: u64, previous: String, blockhash: String) -> Value {
    json!({"parentSlot":slot-1,"previousBlockhash":previous,"blockhash":blockhash,
        "blockTime":1700000000,"blockHeight":slot,"transactions":[],"rewards":[]})
}
fn tail(slot: u64) -> SubscribeUpdateBlock {
    normalize_confirmed_http_block(
        slot,
        &empty(
            slot,
            bs58::encode([(slot - 1) as u8; 32]).into_string(),
            bs58::encode([slot as u8; 32]).into_string(),
        ),
    )
    .unwrap()
}
fn corpus(root: &Path) -> BTreeMap<u64, SubscribeUpdateBlock> {
    let source = PathBuf::from(std::env::var("COPYBOT_PROBE06_HTTP_EVIDENCE_DIR").unwrap());
    let raw = std::fs::read(source.join("response-000011.json")).unwrap();
    let meta: Value =
        serde_json::from_slice(&std::fs::read(source.join("response-000011.meta.json")).unwrap())
            .unwrap();
    assert_eq!(raw.len(), 7_795_854);
    assert_eq!(hash(&raw), meta["response_sha256"]);
    assert_eq!(meta["request"]["params"][0], REAL);
    let actual: Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(
        actual["result"]["transactions"].as_array().unwrap().len(),
        1347
    );
    std::fs::create_dir(root.join("corpus")).unwrap();
    let mut results = BTreeMap::from([(REAL, actual["result"].clone())]);
    results.insert(
        REAL - 2,
        empty(
            REAL - 2,
            bs58::encode([14; 32]).into_string(),
            bs58::encode([13; 32]).into_string(),
        ),
    );
    results.insert(
        REAL - 1,
        empty(
            REAL - 1,
            bs58::encode([13; 32]).into_string(),
            actual["result"]["previousBlockhash"]
                .as_str()
                .unwrap()
                .into(),
        ),
    );
    for slot in REAL + 1..=LAST {
        let mut v = actual["result"].clone();
        v["parentSlot"] = json!(slot - 1);
        v["previousBlockhash"] = if slot == REAL + 1 {
            actual["result"]["blockhash"].clone()
        } else {
            json!(bs58::encode([(slot - 1) as u8; 32]).into_string())
        };
        v["blockhash"] = json!(bs58::encode([slot as u8; 32]).into_string());
        let rows = v["transactions"].as_array_mut().unwrap();
        for row in rows.iter_mut() {
            if row["meta"]["logMessages"].is_null() {
                row["meta"]["logMessages"] = json!([]);
            }
            row["meta"]["logMessages"]
                .as_array_mut()
                .unwrap()
                .push(json!("Program log: explicit local size model "));
        }
        let envelope = json!({"jsonrpc":"2.0","id":1,"result":v});
        let extra = CAP - 128 - serde_json::to_vec(&envelope).unwrap().len();
        v = envelope["result"].clone();
        for (index, row) in v["transactions"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .enumerate()
        {
            let logs = row["meta"]["logMessages"].as_array_mut().unwrap();
            let count = extra / 1347 + usize::from(index < extra % 1347);
            let text = logs.last().unwrap().as_str().unwrap().to_owned() + &"x".repeat(count);
            *logs.last_mut().unwrap() = json!(text);
        }
        assert_eq!(
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":v}))
                .unwrap()
                .len(),
            CAP - 128
        );
        // Only logMessages and modeled parent/hash fields change. All original
        // numeric/token/error/signature/instruction facts remain deep-equal.
        for (modeled, original) in v["transactions"]
            .as_array()
            .unwrap()
            .iter()
            .zip(actual["result"]["transactions"].as_array().unwrap())
        {
            let mut restored = modeled.clone();
            restored["meta"]["logMessages"] = original["meta"]["logMessages"].clone();
            assert_eq!(&restored, original);
        }
        results.insert(slot, v);
    }
    results.insert(
        LAST + 1,
        empty(
            LAST + 1,
            bs58::encode([LAST as u8; 32]).into_string(),
            bs58::encode([(LAST + 1) as u8; 32]).into_string(),
        ),
    );
    let mut blocks = BTreeMap::new();
    let mut facts = vec![];
    for (slot, result) in results {
        let block = normalize_confirmed_http_block(slot, &result).unwrap();
        let raw = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":result})).unwrap();
        if slot > REAL && slot <= LAST {
            assert!(block.encoded_len() > 8 << 20);
            assert!(block.encoded_len() + 512 < CAP + 512);
        }
        facts.push(json!({"slot":slot,"raw_bytes":raw.len(),"normalized_proto_bytes":block.encoded_len(),
            "transaction_count":block.transactions.len(),"modeled":slot!=REAL,"sha256":hash(&raw),
            "fee_lamports_sum":block.transactions.iter().map(|t|t.meta.as_ref().unwrap().fee as u128).sum::<u128>().to_string()}));
        std::fs::write(root.join(format!("corpus/{slot}.json")), raw).unwrap();
        blocks.insert(slot, block);
    }
    save(
        root.join("CORPUS_BINDING.json"),
        &json!({"saved_original_sha256":hash(&std::fs::read(source.join("response-000011.json")).unwrap()),
        "actual_saved_slot":REAL,"dense_model":"1347 original transactions and exact money facts, distributed additional test-only logMessages",
        "missing_original616":"not recovered; empty fixture parent only","blocks":facts}),
    );
    blocks
}
async fn start(root: &Path) -> Docker {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let process = Command::new(std::env::var("COPYBOT_HTTP_DELIVERY_PYTHON").unwrap())
        .arg("-B")
        .arg(repo.join("tools/tests/http_size_docker_fixture.py"))
        .arg("--root")
        .arg(root)
        .arg("--corpus")
        .arg(root.join("corpus"))
        .arg("--helper")
        .arg(std::env::var("COPYBOT_PROBE06_CONTROLLER_HELPER").unwrap())
        .arg("--rust-pid")
        .arg(std::process::id().to_string())
        .arg("--front-memory")
        .arg(std::env::var("COPYBOT_HTTP_SIZE_FRONT_MEMORY").unwrap_or_else(|_| "1g".into()))
        .arg("--backend-memory")
        .arg(std::env::var("COPYBOT_HTTP_SIZE_BACKEND_MEMORY").unwrap_or_else(|_| "1g".into()))
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(root.join("host.stderr.log")).unwrap())
        .spawn()
        .unwrap();
    let mut fixture = Docker {
        root: root.into(),
        url: String::new(),
        process,
    };
    let limit = std::time::Instant::now() + Duration::from_secs(55);
    while !root.join("HOST_READY.json").exists() {
        assert!(
            fixture.process.try_wait().unwrap().is_none(),
            "host startup failed; see host.stderr.log"
        );
        assert!(std::time::Instant::now() < limit, "Docker ready deadline");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let v: Value =
        serde_json::from_slice(&std::fs::read(root.join("HOST_READY.json")).unwrap()).unwrap();
    fixture.url = v["front_url"].as_str().unwrap().into();
    fixture
}
fn client(url: &str) -> ConfirmedHttpRecovery {
    ConfirmedHttpRecovery::new(url, None, 1024, CAP, Duration::from_secs(15)).unwrap()
}
fn ledger(root: &Path) -> u64 {
    rusqlite::Connection::open_with_flags(
        root.join("control/broker-ledger.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap()
    .query_row("SELECT attempts FROM head WHERE id=1", [], |r| r.get(0))
    .unwrap()
}
fn outbound(root: &Path) -> usize {
    std::fs::read_to_string(root.join("control/upstream-requests.jsonl"))
        .unwrap()
        .lines()
        .count()
}
fn failure(root: &Path) -> Vec<Value> {
    std::fs::read_dir(root.join("control/http-evidence"))
        .unwrap()
        .filter_map(|p| {
            let p = p.unwrap().path();
            if !p
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("failure-")
            {
                return None;
            }
            Some(serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap())
        })
        .collect()
}
fn verify_archive(root: &Path, raw: &[u8]) {
    let mut count = 0;
    let digest = hash(raw);
    for p in std::fs::read_dir(root.join("control/http-evidence")).unwrap() {
        let p = p.unwrap().path();
        if !p.to_str().unwrap().ends_with(".meta.json") {
            continue;
        }
        let m: Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        if m["response_sha256"] == digest {
            let name = p
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .replace(".meta.json", ".json");
            assert_eq!(std::fs::read(p.with_file_name(name)).unwrap(), raw);
            assert_eq!(m["bytes"], raw.len());
            count += 1;
        }
    }
    assert!(count > 0, "client bytes missing from durable archive");
}
async fn snapshot(root: &Path, label: &str) {
    std::fs::write(root.join("control/MEMORY_SNAPSHOT"), label).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
}
#[tokio::test]
#[ignore = "explicit saved06/local Docker HTTPS→UDS→Rust only; no providers"]
async fn full_http_size_contract_dense_parallel_memory_durable_ack_and_restart() {
    let root = PathBuf::from(std::env::var("COPYBOT_HTTP_SIZE_DOCKER_EVIDENCE_DIR").unwrap());
    assert!(!root.exists(), "preserve prior evidence");
    std::fs::create_dir_all(&root).unwrap();
    let blocks = corpus(&root);
    let fixture = start(&root).await;
    let clock = std::fs::read(root.join("control/PROBE_CLOCK.json")).unwrap();
    let producer = live::start((LAST + 1..=LAST + 180).map(tail).collect()).await;
    let old =
        copybot_config::load_from_path(std::env::var("COPYBOT_PROBE06_CONFIG").unwrap()).unwrap();
    let mut c = old.ingestion;
    c.yellowstone_grpc_url = producer.url.clone();
    c.yellowstone_x_token = "local-offline-fixture".into();
    let a = c.yellowstone_association.as_mut().unwrap();
    assert_eq!(a.input_bytes, 8 << 20);
    assert_eq!(a.queue.bytes, 8 << 20);
    assert_eq!(a.queue.count, 4);
    a.input_bytes = CAP;
    a.queue.bytes = 4 * (CAP + 512);
    let h = c.yellowstone_http_recovery.as_mut().unwrap();
    h.broker_url = fixture.url.clone();
    h.broker_token = String::new();
    h.max_response_bytes = CAP;
    let wallets = HashSet::from([bs58::encode([242; 32]).into_string()]);
    let dbpath = root.join("runtime.sqlite");
    let (mut db, scope) = initialize_empty(&dbpath, &c, &wallets);
    db.persist(
        &Delivery {
            session: "size-parent-only".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::ParentCheckpoint(checkpoint(&scope, &blocks[&(REAL - 1)])),
        },
        &CandidateGeneration::Unknown,
    )
    .unwrap();
    let mut receiver = DeliveryReceiver::start_recovering_labeled(
        &c,
        "size-ack".into(),
        wallets.clone(),
        None,
        db.replay_checkpoint(&scope).unwrap(),
    )
    .unwrap();
    let ack = until(&mut db, &mut receiver, &scope, LAST + 2).await;
    assert_eq!(ack, (REAL - 1..=LAST + 2).collect::<Vec<_>>());
    assert!(
        receiver
            .ingress_snapshot()
            .processing
            .http_recovery
            .caught_up_to_anchor
    );
    snapshot(&root, "before-parallel-waves").await;
    std::fs::write(root.join("control/PARALLEL_MODE"), b"four").unwrap();
    let reader = client(&fixture.url);
    let mut waves = vec![];
    for wave in 1..=WAVES {
        let begin = ledger(&root);
        let started = std::time::Instant::now();
        let fetches = async {
            let (g1, g2, g3, g4) = tokio::join!(
                reader.block(REAL + 1),
                reader.block(REAL + 2),
                reader.block(REAL + 3),
                reader.block(REAL + 4)
            );
            [g1.unwrap(), g2.unwrap(), g3.unwrap(), g4.unwrap()]
        };
        let (responses, continued) = tokio::join!(
            fetches,
            until(&mut db, &mut receiver, &scope, LAST + 1 + 32 * wave)
        );
        // Hold all four real normalized bodies while continued ACK is consumed.
        // Evidence hashing is outside the async producer/consumer interval.
        let mut observations = vec![];
        for (slot, got) in (REAL + 1..=LAST).zip(responses) {
            assert_eq!(got.block, blocks[&slot]);
            assert!((CAP - 128..=CAP).contains(&got.raw_response.len()));
            verify_archive(&root, &got.raw_response);
            observations.push(
                json!({"slot":slot,"bytes":got.raw_response.len(),"sha256":hash(&got.raw_response),
                "normalized_proto_bytes":got.block.encoded_len()}),
            );
        }
        assert_eq!(ledger(&root), begin + 4);
        snapshot(&root, &format!("after-wave-{wave}")).await;
        waves.push(json!({"wave":wave,"responses":observations,"continued_ack_count":continued.len(),"elapsed_ms":started.elapsed().as_millis()}));
    }
    std::fs::remove_file(root.join("control/PARALLEL_MODE")).unwrap();
    receiver.stop();
    drop(receiver);
    drop(producer);
    assert_eq!(
        std::fs::read(root.join("control/PROBE_CLOCK.json")).unwrap(),
        clock
    );
    let cursor = db.replay_checkpoint(&scope).unwrap().unwrap();
    let restart_head = LAST + 32 * WAVES + 2;
    for slot in LAST + 32 * WAVES..=restart_head {
        let b = tail(slot);
        let v = empty(slot, b.parent_blockhash, b.blockhash);
        std::fs::write(
            root.join(format!("corpus/{slot}.json")),
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":v})).unwrap(),
        )
        .unwrap();
    }
    let producer = live::start(vec![tail(restart_head), tail(restart_head + 1)]).await;
    c.yellowstone_grpc_url = producer.url.clone();
    let mut receiver = DeliveryReceiver::start_recovering_labeled(
        &c,
        "size-restart".into(),
        wallets,
        None,
        Some(cursor.clone()),
    )
    .unwrap();
    let restart = until(&mut db, &mut receiver, &scope, restart_head + 1).await;
    assert_eq!(
        restart,
        vec![restart_head - 1, restart_head, restart_head + 1]
    );
    receiver.stop();
    drop(receiver);
    drop(producer);
    std::fs::write(root.join("control/MODE"), b"exact").unwrap();
    let exact = client(&fixture.url).block(REAL + 1).await.unwrap();
    assert_eq!(exact.raw_response.len(), CAP);
    assert_eq!(exact.block, blocks[&(REAL + 1)]);
    verify_archive(&root, &exact.raw_response);
    let mut controls = vec![];
    for mode in ["declared-over", "chunked-over", "frame-over"] {
        std::fs::write(root.join("control/MODE"), mode).unwrap();
        let (n, requests) = (ledger(&root), outbound(&root));
        let error = client(&fixture.url).block(REAL + 1).await.unwrap_err();
        assert_eq!(ledger(&root), n + 1);
        assert_eq!(outbound(&root), requests + 1);
        controls.push(
            json!({"mode":mode,"error":error.to_string(),"outbound_attempts":1,"rpc_retries":0}),
        );
    }
    std::fs::write(root.join("control/MODE"), b"normal").unwrap();
    std::fs::write(root.join("control/HTTP_STOP"), b"stop").unwrap();
    let (n, requests) = (ledger(&root), outbound(&root));
    let error = client(&fixture.url).block(REAL + 1).await.unwrap_err();
    assert_eq!(ledger(&root), n);
    assert_eq!(outbound(&root), requests);
    std::fs::remove_file(root.join("control/HTTP_STOP")).unwrap();
    controls.push(json!({"mode":"authority-stop","error":error.to_string(),"outbound_attempts":0,"rpc_retries":0}));
    assert_eq!(
        std::fs::read(root.join("control/PROBE_CLOCK.json")).unwrap(),
        clock
    );
    snapshot(&root, "after-controls").await;
    let failures = failure(&root);
    drop(db);
    witness(
        &root,
        &dbpath,
        json!({"status":"PASS_LOCAL_SIZE_ACK_MEMORY","real_saved_slot":REAL,"real_transaction_count":1347,
        "dense_modeled_slots":[REAL+1,REAL+2,REAL+3,LAST],"first_ack_slots":ack,"fixture_anchor":LAST+1,
        "actual_live_anchor_proved":false,"four_parallel_waves":waves,"original_cursor":cursor,"restart_ack_slots":restart,
        "raw_bound_bytes":CAP,"input_bytes":CAP,"capture_queue_bytes":4*(CAP+512),"capture_queue_count":4,
        "unchanged_other_association_bounds":true,"clock_unchanged":true,"controls":controls,"failure_facts":failures,
        "exact_raw_boundary_bytes":exact.raw_response.len(),"provider_calls":0,"signatures":0,"submissions":0}),
    );
    drop(fixture);
}
