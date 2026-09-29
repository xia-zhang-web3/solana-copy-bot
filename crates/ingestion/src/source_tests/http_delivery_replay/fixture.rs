//! Opt-in private saved04 corpus plus task-owned actual Python broker process.
use crate::source::http_recovery::normalize_confirmed_http_block;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;
pub(super) const FIRST: u64 = 451_617_544;
pub(super) const LAST: u64 = 451_617_554;
pub(super) const FAULT: u64 = 451_617_551;
pub(super) struct Corpus {
    pub bodies: BTreeMap<u64, Vec<u8>>,
    pub blocks: BTreeMap<u64, SubscribeUpdateBlock>,
}
pub(super) fn load(output: &Path) -> Corpus {
    let root = PathBuf::from(std::env::var("COPYBOT_PROBE04_HTTP_EVIDENCE_DIR").unwrap());
    std::fs::create_dir_all(output).unwrap();
    let mut bodies = BTreeMap::new();
    let mut blocks = BTreeMap::new();
    for index in 1..=12 {
        let base = format!("response-{index:06}");
        let meta: Value =
            serde_json::from_slice(&std::fs::read(root.join(format!("{base}.meta.json"))).unwrap())
                .unwrap();
        if meta["method"] != "getBlock" {
            continue;
        };
        let body = std::fs::read(root.join(format!("{base}.json"))).unwrap();
        assert_eq!(
            format!("{:x}", Sha256::digest(&body)),
            meta["response_sha256"].as_str().unwrap()
        );
        assert_eq!(body.len() as u64, meta["bytes"].as_u64().unwrap());
        let slot = meta["request"]["params"][0].as_u64().unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["id"], meta["request"]["id"]);
        let block = normalize_confirmed_http_block(slot, &value["result"]).unwrap();
        assert_eq!(
            block.transactions.len(),
            value["result"]["transactions"].as_array().unwrap().len()
        );
        for (raw, tx) in value["result"]["transactions"]
            .as_array()
            .unwrap()
            .iter()
            .zip(&block.transactions)
        {
            assert_eq!(
                raw["meta"]["fee"].as_u64().unwrap(),
                tx.meta.as_ref().unwrap().fee
            );
        }
        std::fs::write(output.join(format!("{slot}.json")), &body).unwrap();
        assert!(blocks.insert(slot, block).is_none());
        bodies.insert(slot, body);
    }
    assert_eq!(
        bodies.keys().copied().collect::<Vec<_>>(),
        (FIRST..=LAST).collect::<Vec<_>>()
    );
    for slot in FIRST + 1..=LAST {
        assert_eq!(
            blocks[&slot].parent_blockhash,
            blocks[&(slot - 1)].blockhash
        );
    }
    Corpus { bodies, blocks }
}
pub(super) fn tail(corpus: &Corpus, slot: u64) -> SubscribeUpdateBlock {
    let previous = if slot == LAST + 1 {
        corpus.blocks[&LAST].blockhash.clone()
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
pub(super) struct Broker {
    pub directory: PathBuf,
    pub url: String,
    process: Child,
}
impl Drop for Broker {
    fn drop(&mut self) {
        let _ = std::fs::write(self.directory.join("STOP_TEST"), b"stop");
        let until = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < until {
            if self.process.try_wait().ok().flatten().is_some() {
                return;
            };
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}
pub(super) async fn start(
    directory: &Path,
    corpus: &Path,
    scenario: &str,
    session_seconds: u64,
) -> Broker {
    std::fs::create_dir_all(directory).unwrap();
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let stderr = std::fs::File::create(directory.join("fixture.stderr.log")).unwrap();
    let process = Command::new(
        std::env::var("COPYBOT_HTTP_DELIVERY_PYTHON").unwrap_or_else(|_| "python3".into()),
    )
    .arg("-B")
    .arg(repo.join("tools/tests/http_recovery_delivery_server.py"))
    .args(["--serve", "--scenario", scenario, "--directory"])
    .arg(directory)
    .arg("--corpus")
    .arg(corpus)
    .arg("--fault-slot")
    .arg(FAULT.to_string())
    .arg("--session-seconds")
    .arg(session_seconds.to_string())
    .stdout(Stdio::null())
    .stderr(stderr)
    .spawn()
    .unwrap();
    let mut broker = Broker {
        directory: directory.into(),
        url: String::new(),
        process,
    };
    let until = std::time::Instant::now() + Duration::from_secs(15);
    while !directory.join("ready.json").exists() {
        assert!(
            broker.process.try_wait().unwrap().is_none(),
            "actual localbroker exited; inspect fixture.stderr.log"
        );
        assert!(
            std::time::Instant::now() < until,
            "actual localbroker startup deadline"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ready: Value =
        serde_json::from_slice(&std::fs::read(directory.join("ready.json")).unwrap()).unwrap();
    broker.url = ready["front_url"].as_str().unwrap().into();
    broker
}
