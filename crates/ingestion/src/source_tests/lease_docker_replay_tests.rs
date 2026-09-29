//! Four actual saved05 blocks; host controller save → Docker bind Gate → ACK.
use super::super::initialize_empty;
use super::{checkpoint, live, settings, until, witness};
use crate::{
    source::http_recovery::{normalize_confirmed_http_block, ConfirmedHttpRecovery},
    DeliveryReceiver,
};
use copybot_core_types::association_delivery::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;
const FIRST: u64 = 451_638_963;
const LAST: u64 = 451_638_966;
struct DockerFixture {
    root: PathBuf,
    url: String,
    process: Child,
}
impl Drop for DockerFixture {
    fn drop(&mut self) {
        let _ = std::fs::write(self.root.join("STOP_HOST_TEST"), b"stopped");
        let until = std::time::Instant::now() + Duration::from_secs(15);
        while std::time::Instant::now() < until {
            if self.process.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}
fn load(root: &Path) -> BTreeMap<u64, SubscribeUpdateBlock> {
    let source = PathBuf::from(std::env::var("COPYBOT_PROBE05_HTTP_EVIDENCE_DIR").unwrap());
    let corpus = root.join("corpus");
    std::fs::create_dir_all(&corpus).unwrap();
    let mut blocks = BTreeMap::new();
    let mut seals = vec![];
    for entry in std::fs::read_dir(&source).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap();
        if !name.ends_with(".meta.json") {
            continue;
        }
        let meta: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        if meta["method"] != "getBlock" {
            continue;
        }
        let slot = meta["request"]["params"][0].as_u64().unwrap();
        if !(FIRST..=LAST).contains(&slot) {
            continue;
        }
        let body = std::fs::read(path.with_file_name(name.replace(".meta.json", ".json"))).unwrap();
        assert_eq!(
            format!("{:x}", Sha256::digest(&body)),
            meta["response_sha256"].as_str().unwrap()
        );
        let raw: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(raw["id"], meta["request"]["id"]);
        let block = normalize_confirmed_http_block(slot, &raw["result"]).unwrap();
        assert_eq!(
            block.transactions.len(),
            raw["result"]["transactions"].as_array().unwrap().len()
        );
        assert!(blocks.insert(slot, block).is_none());
        std::fs::write(corpus.join(format!("{slot}.json")), &body).unwrap();
        seals.push(
            json!({"slot":slot,"bytes":body.len(),"response_sha256":meta["response_sha256"]}),
        );
    }
    assert_eq!(
        blocks.keys().copied().collect::<Vec<_>>(),
        (FIRST..=LAST).collect::<Vec<_>>()
    );
    for slot in FIRST + 1..=LAST {
        assert_eq!(
            blocks[&slot].parent_blockhash,
            blocks[&(slot - 1)].blockhash
        );
    }
    std::fs::write(
        root.join("SAVED05_CORPUS_BINDING.json"),
        serde_json::to_vec_pretty(&seals).unwrap(),
    )
    .unwrap();
    blocks
}
async fn start(root: &Path) -> DockerFixture {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let log = std::fs::File::create(root.join("host-fixture.stderr.log")).unwrap();
    let process = Command::new(
        std::env::var("COPYBOT_HTTP_DELIVERY_PYTHON").unwrap_or_else(|_| "python3".into()),
    )
    .arg("-B")
    .arg(repo.join("tools/tests/http_lease_docker_fixture.py"))
    .arg("--root")
    .arg(root)
    .arg("--corpus")
    .arg(root.join("corpus"))
    .arg("--helper")
    .arg(std::env::var("COPYBOT_PROBE05_CONTROLLER_HELPER").unwrap())
    .arg("--fault-slot")
    .arg((FIRST + 1).to_string())
    .stdout(Stdio::null())
    .stderr(log)
    .spawn()
    .unwrap();
    let mut fixture = DockerFixture {
        root: root.into(),
        url: String::new(),
        process,
    };
    let limit = std::time::Instant::now() + Duration::from_secs(30);
    while !root.join("HOST_READY.json").exists() {
        assert!(
            fixture.process.try_wait().unwrap().is_none(),
            "Docker/controller startup failed; inspect host-fixture.stderr.log"
        );
        assert!(
            std::time::Instant::now() < limit,
            "Docker fixture startup deadline"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ready: Value =
        serde_json::from_slice(&std::fs::read(root.join("HOST_READY.json")).unwrap()).unwrap();
    fixture.url = ready["front_url"].as_str().unwrap().into();
    fixture
}
fn tail(block: &SubscribeUpdateBlock, slot: u64) -> SubscribeUpdateBlock {
    let previous = if slot == LAST + 1 {
        block.blockhash.clone()
    } else {
        bs58::encode([(slot - 1) as u8; 32]).into_string()
    };
    normalize_confirmed_http_block(
        slot,
        &json!({"parentSlot":slot-1,"previousBlockhash":previous,
        "blockhash":bs58::encode([slot as u8;32]).into_string(),"blockTime":1700000000,
        "blockHeight":slot,"transactions":[],"rewards":[]}),
    )
    .unwrap()
}
async fn command(root: &Path, id: u64, mode: &str) {
    std::fs::write(
        root.join("TEST_COMMAND.json"),
        serde_json::to_vec(&json!({"id":id,"mode":mode})).unwrap(),
    )
    .unwrap();
    let limit = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(bytes) = std::fs::read(root.join("TEST_COMMAND_ACK.json")) {
            if serde_json::from_slice::<Value>(&bytes).unwrap()["id"] == id {
                break;
            }
        }
        assert!(
            std::time::Instant::now() < limit,
            "host control command deadline"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
fn outbound(root: &Path) -> usize {
    std::fs::read_to_string(root.join("control/upstream-requests.jsonl"))
        .unwrap()
        .lines()
        .count()
}
fn ledger(root: &Path) -> (u64, u64, u64) {
    let connection = rusqlite::Connection::open_with_flags(
        root.join("control/broker-ledger.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    connection
        .query_row(
            "SELECT attempts,rpc_cu,usd_nano FROM head WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap()
}
fn failures(root: &Path) -> BTreeMap<PathBuf, Value> {
    std::fs::read_dir(root.join("control/http-evidence"))
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            if !path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("failure-")
            {
                return None;
            }
            Some((
                path.clone(),
                serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap(),
            ))
        })
        .collect()
}
#[tokio::test]
#[ignore = "explicit saved05 actual Mac Docker bind/local HTTPS only; no providers"]
async fn host_lease_renewal_docker_gate_reaches_ack_and_denials_never_outbound() {
    let root = PathBuf::from(std::env::var("COPYBOT_HTTP_LEASE_DOCKER_EVIDENCE_DIR").unwrap());
    assert!(!root.exists(), "preserve prior causal run evidence");
    std::fs::create_dir_all(&root).unwrap();
    let blocks = load(&root);
    let fixture = start(&root).await;
    let clock = std::fs::read(root.join("control/PROBE_CLOCK.json")).unwrap();
    let producer = live::start(
        std::iter::once(blocks[&LAST].clone())
            .chain((LAST + 1..=LAST + 32).map(|s| tail(&blocks[&LAST], s)))
            .collect(),
    )
    .await;
    let mut c = settings(&fixture.url, &producer.url);
    let wallets = HashSet::from([bs58::encode([242u8; 32]).into_string()]);
    let dbpath = root.join("runtime.sqlite");
    let (mut db, scope) = initialize_empty(&dbpath, &c, &wallets);
    db.persist(
        &Delivery {
            session: "saved05-owned-fixture-checkpoint".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::ParentCheckpoint(checkpoint(&scope, &blocks[&(FIRST + 1)])),
        },
        &CandidateGeneration::Unknown,
    )
    .unwrap();
    let mut receiver = DeliveryReceiver::start_recovering_labeled(
        &c,
        "lease-docker-ack".into(),
        wallets.clone(),
        None,
        db.replay_checkpoint(&scope).unwrap(),
    )
    .unwrap();
    let first = until(&mut db, &mut receiver, &scope, LAST + 28).await;
    assert_eq!(first, (FIRST + 1..=LAST + 28).collect::<Vec<_>>());
    assert!(
        receiver
            .ingress_snapshot()
            .processing
            .http_recovery
            .caught_up_to_anchor
    );
    assert!(
        producer.sent.load(std::sync::atomic::Ordering::Relaxed) < 33,
        "producer remains active after ACK"
    );
    receiver.stop();
    drop(receiver);
    drop(producer);
    assert_eq!(
        std::fs::read(root.join("control/PROBE_CLOCK.json")).unwrap(),
        clock
    );
    // Restart the unchanged Rust observer with the same Docker broker/clock/ledger.
    let models: Vec<_> = (LAST + 27..=LAST + 29)
        .map(|slot| tail(&blocks[&LAST], slot))
        .collect();
    for block in &models {
        let raw = json!({"parentSlot":block.parent_slot,"previousBlockhash":block.parent_blockhash,
            "blockhash":block.blockhash,"blockTime":1700000000,"blockHeight":block.slot,"transactions":[],"rewards":[]});
        std::fs::write(
            root.join(format!("corpus/{}.json", block.slot)),
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":0,"result":raw})).unwrap(),
        )
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let saved = db.replay_checkpoint(&scope).unwrap().unwrap();
    let producer = live::start(vec![
        tail(&blocks[&LAST], LAST + 29),
        tail(&blocks[&LAST], LAST + 30),
    ])
    .await;
    c.yellowstone_grpc_url = producer.url.clone();
    let mut receiver = DeliveryReceiver::start_recovering_labeled(
        &c,
        "lease-docker-restart".into(),
        wallets,
        None,
        Some(saved.clone()),
    )
    .unwrap();
    let restart = until(&mut db, &mut receiver, &scope, LAST + 30).await;
    assert_eq!(restart, vec![LAST + 28, LAST + 29, LAST + 30]);
    receiver.stop();
    drop(receiver);
    drop(producer);
    let after_restart = ledger(&root);
    assert_eq!(
        std::fs::read(root.join("control/PROBE_CLOCK.json")).unwrap(),
        clock
    );
    let before_outbound = outbound(&root);
    let before_ledger = ledger(&root);
    let mut controls = vec![];
    for (index, mode, predicate) in [
        (1, "expired", "lease_expiry"),
        (2, "generation", "lease_generation"),
        (3, "stop", "read_only_stop"),
        (4, "clock_binding", "clock_binding"),
        (5, "clock_deadline", "clock_deadline"),
        (6, "missing", "lease_visibility_exhausted"),
    ] {
        command(&root, index, mode).await;
        let previous_failures = failures(&root);
        let error =
            ConfirmedHttpRecovery::new(&fixture.url, None, 1024, 8 << 20, Duration::from_secs(15))
                .unwrap()
                .slots(FIRST, LAST)
                .await
                .unwrap_err();
        assert_eq!(outbound(&root), before_outbound);
        assert_eq!(ledger(&root), before_ledger);
        let observed: Vec<_> = failures(&root)
            .into_iter()
            .filter(|(path, _)| !previous_failures.contains_key(path))
            .map(|(_, fact)| fact)
            .collect();
        assert!(
            observed
                .iter()
                .any(|fact| fact["gate_predicate"] == predicate),
            "missing precise authority predicate {predicate}: {observed:?}"
        );
        assert!(observed.iter().all(|fact| fact["reservation_id"].is_null()));
        controls.push(json!({"mode":mode,"expected_predicate":predicate,"rust_terminal_error":error.to_string(),
            "new_outbound":0,"new_reservations":0,"persisted_failure_facts":observed}));
    }
    command(&root, 7, "valid").await;
    assert_eq!(
        std::fs::read(root.join("control/PROBE_CLOCK.json")).unwrap(),
        clock
    );
    let resumed =
        ConfirmedHttpRecovery::new(&fixture.url, None, 1024, 8 << 20, Duration::from_secs(15))
            .unwrap();
    assert_eq!(resumed.slots(FIRST, LAST).await.unwrap().len(), 4);
    assert_eq!(ledger(&root).0, before_ledger.0 + 1);
    drop(db);
    witness(
        &root,
        &dbpath,
        json!({"status":"PASS_ACTUAL_DOCKER_BIND_ACK_AUTHORITY_CONTROLS",
        "actual_saved_probe05_blocks":4,"actual_slots":[FIRST,FIRST+1,FIRST+2,LAST],"ack_slots":first,
        "fixture_anchor":LAST,"continued_live_head":LAST+28,"durable_cursor":saved,
        "restart_ack_slots":restart,"restart_original_clock_unchanged":true,
        "same_clock_after_new_rust_client":true,"reservation_head_after_new_client":after_restart,
        "terminal_controls":controls,"provider_calls":0,"signatures":0,"submissions":0}),
    );
    drop(fixture);
}
