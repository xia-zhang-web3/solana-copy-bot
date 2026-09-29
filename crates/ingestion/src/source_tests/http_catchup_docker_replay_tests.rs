//! Probe07's 180-slot gap through actual HTTPS/UDS, ordered SQLite ACK and tonic.
#[path = "http_catchup_corpus.rs"]
mod corpus;
#[path = "http_catchup_linux_app_tests.rs"]
mod linux_app;
#[path = "http_catchup_live.rs"]
mod live;
use super::super::{initialize_empty, persist};
use super::{checkpoint, until, witness};
use crate::DeliveryReceiver;
use copybot_core_types::association_delivery::*;
use corpus::{save, ANCHOR, CURSOR, FIRST, PERIOD_MS};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};
struct Docker {
    root: PathBuf,
    url: String,
    child: Child,
}
impl Drop for Docker {
    fn drop(&mut self) {
        let _ = std::fs::write(self.root.join("STOP_HOST_TEST"), b"stop");
        let deadline = Instant::now() + Duration::from_secs(25);
        while Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
async fn start(root: &Path) -> Docker {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let child = Command::new(std::env::var("COPYBOT_HTTP_DELIVERY_PYTHON").unwrap())
        .arg("-B")
        .arg(repo.join("tools/tests/http_size_docker_fixture.py"))
        .arg("--root")
        .arg(root)
        .arg("--corpus")
        .arg(root.join("corpus"))
        .arg("--helper")
        .arg(std::env::var("COPYBOT_PROBE07_CONTROLLER_HELPER").unwrap())
        .arg("--rust-pid")
        .arg(std::process::id().to_string())
        .arg("--front-memory")
        .arg("512m")
        .arg("--backend-memory")
        .arg("768m")
        .arg("--server-script")
        .arg(repo.join("tools/tests/http_catchup_docker_server.py"))
        .arg("--duration")
        .arg("390")
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(root.join("host.stderr.log")).unwrap())
        .spawn()
        .unwrap();
    let mut d = Docker {
        root: root.into(),
        url: String::new(),
        child,
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    while !root.join("HOST_READY.json").exists() {
        assert!(
            d.child.try_wait().unwrap().is_none(),
            "host startup failed; see host.stderr.log"
        );
        assert!(Instant::now() < deadline, "Docker ready deadline");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ready: Value =
        serde_json::from_slice(&std::fs::read(root.join("HOST_READY.json")).unwrap()).unwrap();
    d.url = ready["front_url"].as_str().unwrap().into();
    d
}
fn lines(root: &Path, name: &str) -> Vec<Value> {
    std::fs::read_to_string(root.join(format!("control/{name}")))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}
fn failures(root: &Path) -> Vec<Value> {
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
fn snapshot(r: &DeliveryReceiver, produced: u64, at: f64) -> Value {
    let t = r.ingress_snapshot();
    let p = t.processing;
    let h = p.http_recovery;
    json!({"elapsed_s":at,"latest_produced":produced,"last_received":t.last_received_block_slot,
        "durable":t.last_durably_stored_parent_slot,"produced_minus_durable":produced.saturating_sub(t.last_durably_stored_parent_slot.max(CURSOR)),
        "queue_count":p.input_queue_count,"queue_bytes":p.input_queue_bytes,
        "queue_count_max":p.input_queue_count_max,"queue_bytes_max":p.input_queue_bytes_max,
        "recovered":h.recovered_slot,"live_anchor":h.live_anchor_slot,"telemetry_backlog":h.current_backlog_slots,
        "caught_up":h.caught_up_to_anchor,"hold":r.http_continuity_hold().unwrap().load(Ordering::Acquire)})
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "explicit saved07/local full-gap Docker HTTPS→UDS→Rust; no providers"]
async fn probe07_full_gap_delayed_and_incomplete_reads_ordered_ack_continuing_full_stream() {
    let root = PathBuf::from(std::env::var("COPYBOT_HTTP_CATCHUP_EVIDENCE_DIR").unwrap());
    assert!(!root.exists(), "preserve previous evidence");
    std::fs::create_dir_all(&root).unwrap();
    let corpus = Arc::new(corpus::load(&root));
    let docker = start(&root).await;
    let clock = std::fs::read(root.join("control/PROBE_CLOCK.json")).unwrap();
    let producer = live::start(corpus.clone(), ANCHOR).await;
    let config =
        copybot_config::load_from_path(std::env::var("COPYBOT_PROBE07_CONFIG").unwrap()).unwrap();
    let mut c = config.ingestion;
    c.yellowstone_grpc_url = producer.url.clone();
    c.yellowstone_x_token = "local-offline-fixture".into();
    let a = c.yellowstone_association.as_ref().unwrap();
    assert_eq!(a.input_bytes, 16 << 20);
    assert_eq!(a.queue.bytes, 67_110_912);
    assert_eq!(a.queue.count, 4);
    assert_eq!(a.blocks.bytes, 805_306_368);
    assert_eq!(a.blocks.count, 384);
    assert_eq!(a.metadata_bytes, 100_663_296);
    let h = c.yellowstone_http_recovery.as_mut().unwrap();
    assert_eq!(h.fetch_concurrency, 4);
    assert_eq!(h.max_response_bytes, 16 << 20);
    h.broker_url = docker.url.clone();
    h.broker_token = String::new();
    h.timeout_ms = 30000;
    let wallets = HashSet::from([bs58::encode([242; 32]).into_string()]);
    let dbpath = root.join("runtime.sqlite");
    let (mut db, scope) = initialize_empty(&dbpath, &c, &wallets);
    db.persist(
        &Delivery {
            session: "saved07-confirmed-cursor-model".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::ParentCheckpoint(checkpoint(&scope, &corpus.block(CURSOR))),
        },
        &CandidateGeneration::Unknown,
    )
    .unwrap();
    let mut receiver = DeliveryReceiver::start_recovering_labeled(
        &c,
        "probe07-offline-gap".into(),
        wallets.clone(),
        None,
        db.replay_checkpoint(&scope).unwrap(),
    )
    .unwrap();
    let begin = Instant::now();
    let deadline = begin + Duration::from_secs(350);
    let mut ack = vec![];
    let mut observations = vec![];
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    let mut error = None;
    let mut released = None;
    loop {
        tokio::select! {
            biased;
            _=tokio::time::sleep_until(deadline.into())=>{error=Some("local full-gap 350s criterion deadline".to_string());break;}
            _=tick.tick()=>{
                observations.push(snapshot(&receiver,producer.sent.load(Ordering::Relaxed),begin.elapsed().as_secs_f64()));
                save(root.join("PROGRESS.json"),&json!({"acks":ack,"observations":observations}));
            }
            result=receiver.next()=>{
                let envelope=match result {Ok(Some(e))=>e,Ok(None)=>{error=Some("receiver ended without target ACK".into());break;},Err(e)=>{error=Some(format!("receiver failed: {e:#}"));break;}};
                assert!(!matches!(envelope.delivery.event,DeliveryEvent::Admission(_)));
                if let DeliveryEvent::Session(ref event)=envelope.delivery.event {
                    save(root.join("LAST_SESSION_EVENT.json"),&json!({"event":format!("{event:?}"),"elapsed_s":begin.elapsed().as_secs_f64()}));
                }
                let slot=if let DeliveryEvent::ParentCheckpoint(ref b)=envelope.delivery.event {Some(b.observation.child.slot)}else{None};
                if let Some(slot)=slot {
                    if slot < ANCHOR {assert!(receiver.http_continuity_hold().unwrap().load(Ordering::Acquire));}
                }
                persist(&mut db,&receiver,&envelope,&scope);
                if let Some(slot)=slot {
                    let stored=db.replay_checkpoint(&scope).unwrap().unwrap();assert_eq!(stored.block.observation.child.slot,slot);
                    ack.push(slot);
                    let observation=snapshot(&receiver,producer.sent.load(Ordering::Relaxed),begin.elapsed().as_secs_f64());
                    if slot==ANCHOR+1 {
                        if receiver.http_continuity_hold().unwrap().load(Ordering::Acquire) {
                            error=Some("continuity HOLD remains after anchor; live reader was interrupted".into());
                            observations.push(observation);
                            break;
                        }
                        released=Some(observation.clone());
                    }
                    observations.push(observation);
                    save(root.join("PROGRESS.json"),&json!({"acks":ack,"observations":observations}));
                    if slot>=ANCHOR+12 {break;}
                }
            }
        }
    }
    let final_snapshot = snapshot(
        &receiver,
        producer.sent.load(Ordering::Relaxed),
        begin.elapsed().as_secs_f64(),
    );
    let checkpoint_end = db
        .replay_checkpoint(&scope)
        .unwrap()
        .unwrap()
        .block
        .observation
        .child
        .slot;
    receiver.stop();
    drop(producer);
    let mut restart_ack = vec![];
    if error.is_none() {
        drop(receiver);
        let restarted = live::start(corpus.clone(), checkpoint_end + 1).await;
        c.yellowstone_grpc_url = restarted.url.clone();
        let mut receiver2 = DeliveryReceiver::start_recovering_labeled(
            &c,
            "probe07-offline-restart".into(),
            wallets.clone(),
            None,
            db.replay_checkpoint(&scope).unwrap(),
        )
        .unwrap();
        restart_ack = until(&mut db, &mut receiver2, &scope, checkpoint_end + 3).await;
        receiver2.stop();
        drop(restarted);
        assert_eq!(
            std::fs::read(root.join("control/PROBE_CLOCK.json")).unwrap(),
            clock
        );
    }
    let requests = lines(&root, "upstream-requests.jsonl");
    let events = lines(&root, "upstream-events.jsonl");
    let failure = failures(&root);
    let count_attempt = |slot: u64| {
        requests
            .iter()
            .filter(|r| r["method"] == "getBlock" && r["slot"] == slot)
            .count()
    };
    let completed = |slot: u64| {
        events
            .iter()
            .find(|e| e["stage"] == "complete" && e["slot"] == slot)
            .map(|e| e["at_unix"].as_f64().unwrap())
    };
    let early = completed(CURSOR + 3)
        .zip(completed(CURSOR + 4))
        .zip(completed(CURSOR + 1).zip(completed(CURSOR + 2)))
        .map(|((c, d), (a, b))| c.max(d) < a.min(b))
        .unwrap_or(false);
    let incomplete = failure
        .iter()
        .any(|f| f.to_string().contains("IncompleteRead"));
    let ledger = rusqlite::Connection::open_with_flags(
        root.join("control/broker-ledger.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let reservations: u64 = ledger
        .query_row("SELECT attempts FROM head WHERE id=1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(reservations, requests.len() as u64);
    assert_eq!(
        std::fs::read(root.join("control/PROBE_CLOCK.json")).unwrap(),
        clock
    );
    let passed = error.is_none()
        && ack == (CURSOR..=ANCHOR + 12).collect::<Vec<_>>()
        && early
        && incomplete
        && count_attempt(CURSOR + 1) == 1
        && count_attempt(CURSOR + 2) == 2
        && final_snapshot["caught_up"].as_bool().unwrap()
        && !restart_ack.is_empty();
    witness(
        &root,
        &dbpath,
        json!({"passed":passed,"error":error,"cursor_start":CURSOR,"cursor_end":checkpoint_end,
        "modeled_anchor":ANCHOR,"gap_slot_span":ANCHOR-CURSOR,"saved_original_slots":[FIRST,CURSOR,CURSOR+3,CURSOR+4],
        "model":"all180 full blocks representative of four saved07 templates; only block parent/hash/coordinate modeled",
        "restart_ack":restart_ack,"period_ms":PERIOD_MS,"recovery_elapsed_s":begin.elapsed().as_secs_f64(),"acks":ack,"observations":observations,
        "hold_released":released,"final":final_snapshot,"early_prefetched_complete":early,"incomplete_read":incomplete,
        "660_attempts":count_attempt(CURSOR+1),"661_attempts":count_attempt(CURSOR+2),"reservations":reservations,
        "requests":requests,"events":events,"failures":failure,"provider_calls":0,"signatures":0,"submissions":0,
        "rust_runtime":"native Mac scoped test; Linux helper cgroups measured; not a full Linux daemon stress proof"}),
    );
    drop(docker);
    assert!(passed, "see persisted causal result for full-gap obstacle");
    assert_eq!(ack, (CURSOR..=ANCHOR + 12).collect::<Vec<_>>());
    assert!(early);
    assert!(incomplete);
    assert_eq!(count_attempt(CURSOR + 1), 1);
    assert_eq!(count_attempt(CURSOR + 2), 2);
    assert!(final_snapshot["caught_up"].as_bool().unwrap());
}
