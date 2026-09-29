//! Exact installed Linux artifact closes the identical gap; no daemon compilation.
use super::super::super::initialize_empty;
use super::{checkpoint, corpus, failures, lines, start};
use copybot_core_types::association_delivery::*;
use corpus::{save, Corpus, ANCHOR, CURSOR, PERIOD_MS, TAIL};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};
struct App {
    root: PathBuf,
    phase: &'static str,
    child: Child,
}
impl Drop for App {
    fn drop(&mut self) {
        let _ = std::fs::write(self.root.join(format!("{}-APP_STOP", self.phase)), b"stop");
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
async fn app(root: &Path, phase: &'static str, grpc: &str, wallet: &str) -> App {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let child = Command::new(std::env::var("COPYBOT_HTTP_DELIVERY_PYTHON").unwrap())
        .arg("-B")
        .arg(repo.join("tools/tests/http_catchup_app_fixture.py"))
        .arg("--root")
        .arg(root)
        .arg("--phase")
        .arg(phase)
        .arg("--config")
        .arg(std::env::var("COPYBOT_PROBE07_CONFIG").unwrap())
        .arg("--install")
        .arg(std::env::var("COPYBOT_OFFLINE_APP_INSTALL").unwrap_or_else(|_| std::env::var("COPYBOT_PROBE08_INSTALLED_DIR").unwrap()))
        .arg("--expected-binary-sha256")
        .arg(std::env::var("COPYBOT_OFFLINE_APP_SHA256").unwrap_or_else(|_| "b216993597f83ae53d1d66f4946885ffeddf594a88410d86ec38ec61088be53d".into()))
        .args(if std::env::var("COPYBOT_OFFLINE_ALLOW_UNMANIFESTED_DEV").as_deref() == Ok("1") {
            vec!["--allow-unmanifested-dev"]
        } else { vec![] })
        .arg("--grpc-port")
        .arg(grpc.rsplit(':').next().unwrap())
        .arg("--wallet")
        .arg(wallet)
        .arg("--app-arenas")
        .arg(std::env::var("COPYBOT_OFFLINE_APP_ARENAS").unwrap_or_else(|_| "default".into()))
        .arg("--blocks-bytes")
        .arg(std::env::var("COPYBOT_OFFLINE_BLOCKS_BYTES").unwrap_or_else(|_| "805306368".into()))
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(root.join(format!("{phase}-APP_HOST.stderr.log"))).unwrap())
        .spawn()
        .unwrap();
    let mut a = App {
        root: root.into(),
        phase,
        child,
    };
    let deadline = Instant::now() + Duration::from_secs(45);
    while !root.join(format!("{phase}-APP_READY.json")).exists() {
        let ready = root.join(format!("{phase}-CONFIG_READY.json"));
        if ready.exists() && !root.join(format!("{phase}-CONFIG_VALID")).exists() {
            copybot_config::load_from_path(root.join(format!("{phase}-config.toml"))).unwrap();
            std::fs::write(
                root.join(format!("{phase}-CONFIG_VALID")),
                b"typed schema accepted",
            )
            .unwrap();
        }
        assert!(
            a.child.try_wait().unwrap().is_none(),
            "Linux transport/app preparation failed: phase-specific stderr"
        );
        assert!(Instant::now() < deadline, "Linux app ready deadline");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    a
}
fn head(path: &Path) -> u64 {
    let c = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap();
    let text: String = c
        .query_row(
            "SELECT head FROM association_replay_cursor WHERE id=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    serde_json::from_str::<Value>(&text).unwrap()["block"]["observation"]["child"]["slot"]
        .as_u64()
        .unwrap()
}
fn logs(root: &Path, phase: &str) -> String {
    std::fs::read_to_string(root.join(format!("{phase}-APP.log"))).unwrap_or_default()
}
fn reported_cursor(root: &Path, phase: &str, initial: u64) -> u64 {
    logs(root, phase).lines().filter_map(|line| {
        let envelope: Value = serde_json::from_str(&line[line.find('{')?..]).ok()?;
        envelope["durable_slot"].as_u64()
            .or_else(|| envelope["last_durably_stored_parent_slot"].as_u64())
    }).fold(initial, u64::max)
}
fn retention_log_samples(root: &Path, phase: &str) -> Vec<Value> {
    logs(root, phase)
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let envelope: Value = serde_json::from_str(&line[line.find('{')?..]).ok()?;
            let number = |field: &str| -> Option<u64> {
                envelope[field].as_u64().or_else(|| {
                    line.split_once(&format!("{field}: ")).and_then(|(_, right)| {
                        right.chars().take_while(|c| c.is_ascii_digit()).collect::<String>().parse().ok()
                    })
                })
            };
            Some(json!({
                "log_index":index,
                "time_utc":line.split_whitespace().next(),
                "message":envelope["message"],
                "recovered_slot":envelope["recovered_slot"],
                "input_queue_count":number("input_queue_count")?,
                "input_queue_bytes":number("input_queue_bytes")?,
                "block_cache_count":number("block_cache_count")?,
                "block_cache_encoded_bytes":number("block_cache_encoded_bytes")?,
            }))
        })
        .collect()
}
fn app_memory_summary(root: &Path, phase: &str) -> Value {
    let metrics: Value = serde_json::from_slice(
        &std::fs::read(root.join(format!("{phase}-APP_METRICS.json"))).unwrap_or_default(),
    ).unwrap_or(Value::Null);
    let stopped: Value = serde_json::from_slice(
        &std::fs::read(root.join(format!("{phase}-APP_STOPPED.json"))).unwrap_or_default(),
    ).unwrap_or(Value::Null);
    let samples = metrics["samples"].as_array();
    json!({
        "cgroup_peak_bytes":samples.and_then(|rows| rows.iter().filter_map(|v|v["memory_peak"].as_u64()).max()),
        "minimum_vm_available_bytes":samples.and_then(|rows| rows.iter().filter_map(|v|v["MemAvailable_bytes"].as_u64()).min()),
        "sample_count":samples.map(Vec::len).unwrap_or(0),
        "oom_killed":stopped["State"]["OOMKilled"],
        "exit_code":stopped["State"]["ExitCode"],
        "memory_cap_bytes":stopped["HostConfig"]["Memory"],
    })
}
async fn observe(
    root: &Path,
    _path: &Path,
    phase: &str,
    app: &mut App,
    producer: &super::live::Server,
    corpus: &Corpus,
    target: u64,
    initial_cursor: u64,
    need_decline: bool,
    post_anchor_seconds: f64,
) -> (Vec<Value>, Option<String>) {
    let begin = Instant::now();
    let mut rows = vec![];
    let mut error = None;
    let mut anchor_backlog = None;
    let mut maximum_backlog = 0;
    let mut anchor_seen_at = None;
    let mut prior_cursor = initial_cursor;
    let mut observed_admissions: Vec<(u64, f64)> = Vec::new();
    loop {
        let elapsed = begin.elapsed().as_secs_f64();
        // The app's Linux SQLite WAL must not share an mmap with macOS SQLite
        // while it is running. Its ACK is checked independently after stop.
        let cursor = reported_cursor(root, phase, initial_cursor);
        let latest = producer.sent.load(Ordering::Relaxed);
        let backlog = latest.saturating_sub(cursor);
        for slot in prior_cursor.saturating_add(1)..=cursor {
            observed_admissions.push((slot, elapsed));
        }
        prior_cursor = cursor;
        observed_admissions.retain(|(_, seen)| elapsed - *seen < 60.0);
        let cache_model_bytes = observed_admissions.iter()
            .map(|(slot, _)| corpus.modeled_encoded_bytes(*slot))
            .sum::<usize>();
        let queued_from = cursor.max(ANCHOR);
        let queue_model_bytes = (queued_from.saturating_add(1)..=latest)
            .map(|slot| corpus.modeled_encoded_bytes(slot).saturating_add(528))
            .sum::<usize>();
        let text = logs(root, phase);
        let completed = text.contains("recovery_completed=true")
            || text.contains("\"recovery_completed\":true");
        let log_lines = text.lines().collect::<Vec<_>>();
        let completion_index = log_lines.iter().position(|l| {
            l.contains("recovery_completed=true") || l.contains("\"recovery_completed\":true")
        });
        let nonpending = completion_index.is_some_and(|i| {
            log_lines[i + 1..]
                .iter()
                .any(|l| l.contains("execution canary") && l.contains("kill_switch_active"))
        });
        if cursor >= ANCHOR && anchor_backlog.is_none() {
            anchor_backlog = Some(backlog);
            anchor_seen_at = Some(elapsed);
        }
        let after_anchor = anchor_seen_at.map(|at| elapsed - at);
        rows.push(json!({"elapsed_s":elapsed,"cursor":cursor,"latest_produced":latest,
            "backlog":backlog,"public_completed":completed,"public_hold_released_tick":nonpending,
            "post_anchor_observed_s":after_anchor,
            "live_queue_model_count":latest.saturating_sub(queued_from),
            "live_queue_model_encoded_bytes":queue_model_bytes,
            "association_cache_model_count":observed_admissions.len(),
            "association_cache_model_encoded_bytes":cache_model_bytes}));
        save(
            root.join(format!("{phase}-APP_PROGRESS.json")),
            &json!({"observations":rows}),
        );
        maximum_backlog = maximum_backlog.max(backlog);
        let declining = !need_decline || (maximum_backlog > backlog && backlog <= 4);
        let duration_met = after_anchor.is_some_and(|seconds| seconds >= post_anchor_seconds);
        if cursor >= target && completed && nonpending && declining && duration_met && latest < TAIL {
            break;
        }
        if app.child.try_wait().unwrap().is_some() {
            error = Some("actual Linux daemon/helper exited before criterion".to_string());
            break;
        }
        if text.contains("association input rejected: BlockCapacity") {
            error = Some("actual Linux association BlockCapacity".to_string());
            break;
        }
        if text.contains("LiveCaptureBytes") || text.contains("LiveCaptureCount") {
            error = Some("actual Linux live capture capacity".to_string());
            break;
        }
        if begin.elapsed() > Duration::from_secs(300) {
            error = Some("actual Linux300s bounded criterion exhausted".to_string());
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    (rows, error)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "exact bound Linux app with local fullgap fixture only; no providers/signers"]
async fn probe07_exact_linux_artifact_fullgap_continued_stream_durable_ack_and_restart() {
    let root = PathBuf::from(std::env::var("COPYBOT_HTTP_CATCHUP_EVIDENCE_DIR").unwrap());
    assert!(!root.exists());
    std::fs::create_dir_all(&root).unwrap();
    let corpus = Arc::new(corpus::load(&root));
    let docker = start(&root).await;
    let clock = std::fs::read(root.join("control/PROBE_CLOCK.json")).unwrap();
    let producer = super::live::start(corpus.clone(), ANCHOR).await;
    let source =
        copybot_config::load_from_path(std::env::var("COPYBOT_PROBE07_CONFIG").unwrap()).unwrap();
    let mut c = source.ingestion;
    c.yellowstone_grpc_url = producer.url.clone();
    c.yellowstone_x_token = "offline-no-provider".into();
    let wallet = bs58::encode([242; 32]).into_string();
    let wallets = HashSet::from([wallet.clone()]);
    c.yellowstone_replay_wallets = vec![wallet.clone()];
    std::fs::create_dir(root.join("app-state")).unwrap();
    let dbpath = root.join("app-state/live_runtime.db");
    let (mut db, scope) = initialize_empty(&dbpath, &c, &wallets);
    db.persist(
        &Delivery {
            session: "modeled-cursor659-only".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::ParentCheckpoint(checkpoint(&scope, &corpus.block(CURSOR))),
        },
        &CandidateGeneration::Unknown,
    )
    .unwrap();
    drop(db);
    let mut actual = app(&root, "main", &producer.url, &wallet).await;
    let (observations, error) = observe(
        &root,
        &dbpath,
        "main",
        &mut actual,
        &producer,
        &corpus,
        ANCHOR + 48,
        CURSOR,
        true,
        60.0,
    )
    .await;
    drop(actual);
    let first_end = head(&dbpath);
    drop(producer);
    let main_retention_log_samples = retention_log_samples(&root, "main");
    let mut restart_rows = vec![];
    let mut restart_error = None;
    if error.is_none() {
        corpus::ensure_tail(&root, &corpus, first_end - 1, first_end + 1);
        let live = super::live::start(corpus.clone(), first_end + 1).await;
        let mut restarted = app(&root, "restart", &live.url, &wallet).await;
        (restart_rows, restart_error) = observe(
            &root,
            &dbpath,
            "restart",
            &mut restarted,
            &live,
            &corpus,
            first_end + 12,
            first_end,
            false,
            0.0,
        )
        .await;
        drop(restarted);
        drop(live);
    }
    let requests = lines(&root, "upstream-requests.jsonl");
    let restart_retention_log_samples = retention_log_samples(&root, "restart");
    let main_app_memory = app_memory_summary(&root, "main");
    let restart_app_memory = app_memory_summary(&root, "restart");
    let app_artifact: Value = serde_json::from_slice(&std::fs::read(root.join("main-APP_READY.json")).unwrap()).unwrap();
    let events = lines(&root, "upstream-events.jsonl");
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
    let failure = failures(&root);
    let incomplete = failure
        .iter()
        .any(|f| f.to_string().contains("IncompleteRead"));
    let conn =
        rusqlite::Connection::open_with_flags(&dbpath, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let mut parent = conn
        .prepare("SELECT first_observation FROM association_parent_blocks")
        .unwrap();
    let mut chain = parent
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| serde_json::from_str::<Value>(&r.unwrap()).unwrap())
        .collect::<Vec<_>>();
    chain.sort_by_key(|v| v["child"]["slot"].as_u64().unwrap());
    let end = head(&dbpath);
    for rows in chain.windows(2) {
        assert_eq!(rows[0]["child"], rows[1]["parent"]);
    }
    assert_eq!(chain.last().unwrap()["child"]["slot"], end);
    let financial = ([
        "orders",
        "fills",
        "positions",
        "copy_signals",
        "execution_canary_receipt_facts",
        "owner_exit_intents",
        "native_buy_technical_cohort",
    ])
    .into_iter()
    .map(|t| {
        let n: u64 = conn
            .query_row(&format!("SELECT count(*) FROM {t}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
        (t, n)
    })
    .collect::<std::collections::BTreeMap<_, _>>();
    let ledger = rusqlite::Connection::open_with_flags(
        root.join("control/broker-ledger.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let count: u64 = ledger
        .query_row("SELECT attempts FROM head WHERE id=1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, requests.len() as u64);
    assert_eq!(
        std::fs::read(root.join("control/PROBE_CLOCK.json")).unwrap(),
        clock
    );
    let measured = !main_retention_log_samples.is_empty();
    let passed = error.is_none() && restart_error.is_none() && early && incomplete && measured;
    save(
        root.join("LINUX_APP_CAUSAL_RESULT.json"),
        &json!({"passed":passed,"error":error,"restart_error":restart_error,
        "cursor_start":CURSOR,"model_anchor":ANCHOR,"first_cursor_end":first_end,"restart_cursor_end":end,"gap_slot_span":ANCHOR-CURSOR,
        "period_ms":PERIOD_MS,"main_observations":observations,"restart_observations":restart_rows,"parents":chain,
        "required_post_anchor_observed_seconds":60,
        "main_direct_retention_telemetry":main_retention_log_samples,
        "restart_direct_retention_telemetry":restart_retention_log_samples,
        "direct_retention_measured":measured,
        "main_app_memory":main_app_memory,"restart_app_memory":restart_app_memory,
        "retention_model":"live_queue_model uses producer-emitted full blocks pending a durable cursor; encoded bytes include 528B modeled envelope/permit charge and may lead tonic delivery by one block. association_cache_model uses ACK slot timestamps at 250ms sampling and source encoded bytes inside a rolling 60s window; it is an approximation, not a heap/RSS measurement. Direct queue counters, when emitted, appear separately.",
        "early_prefetched_complete":early,"incomplete_read":incomplete,"reservations":count,"requests":requests,"events":events,
        "failures":failure,"financial":financial,"provider_calls":0,"signatures":0,"submissions":0,
        "artifact":app_artifact,"app_resource_cap_bytes":3usize<<30,
        "hold_observable":"existing canarytick enabled=false/skipped_reason kill_switch_active after ingress_http_recovery_pending; flags remain off"}),
    );
    drop(docker);
    assert!(
        passed,
        "see Linux persisted result for exact actualartifact obstacle"
    );
}
