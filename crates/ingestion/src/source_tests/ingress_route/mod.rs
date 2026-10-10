//! Test-only efd durable route via real relays; exact source clock, no external I/O.
mod corpus;
mod grpc;
mod http;
mod native;
mod rpc;
use crate::DeliveryReceiver;
use anyhow::{ensure, Context, Result};
use copybot_config::{AssociationDeliveryConfig, HttpRecoveryConfig, IngestionConfig};
use copybot_core_types::association_delivery::*;
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    SqliteStore,
};
use futures_util::stream;
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use yellowstone_grpc_proto::prelude::*;

fn sample(r: &DeliveryReceiver, corpus: &corpus::Corpus, elapsed: f64) -> Value {
    let s = r.ingress_snapshot();
    let h = &s.processing.http_recovery;
    json!({"elapsed_seconds":elapsed,"observed_unix_ns":chrono::Utc::now().timestamp_nanos_opt().unwrap(),
      "producer_latest_slot":corpus.latest(),"received_blocks":s.received_blocks,
      "received_transactions":s.received_transactions,"input_received_bytes":s.processing.input_received_bytes,
      "last_received_block_slot":s.last_received_block_slot,"durable_slot":s.last_durably_stored_parent_slot,
      "producer_durable_gap_slots":corpus.latest().saturating_sub(s.last_durably_stored_parent_slot),
      "reconnects":s.reconnects,"http_from":h.first_from_slot,"http_anchor":h.live_anchor_slot,
      "http_recovered_slot":h.recovered_slot,"http_durable_completed_slot":h.durable_completed_slot,
      "http_backlog":h.current_backlog_slots,"recovery_completed":h.caught_up_to_anchor,
      "input_queue_count":s.processing.input_queue_count,"input_queue_bytes":s.processing.input_queue_bytes,
      "input_queue_max":s.processing.input_queue_count_max,"input_queue_bytes_max":s.processing.input_queue_bytes_max,
      "selected_source":s.selected_source,"admissions":s.admissions,"decode_errors":s.decode_errors})
}
fn sql_counts(path: &std::path::Path) -> Result<Value> {
    let sql = rusqlite::Connection::open(path)?;
    let mut counts = serde_json::Map::new();
    for table in [
        "native_buy_cohort_decisions",
        "native_buy_decisions",
        "copy_signals",
        "orders",
        "fills",
        "positions",
        "execution_canary_receipt_facts",
    ] {
        let n: u64 = sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
        counts.insert(table.into(), json!(n));
    }
    Ok(Value::Object(counts))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit isolated local relay fixture only, no provider"]
async fn real_relay_reader_decoder_native_fresh_and_recovered_buy() -> Result<()> {
    let out = PathBuf::from(std::env::var("INGRESS_E2E_OUT")?);
    std::fs::create_dir_all(&out)?;
    std::fs::create_dir_all(out.join("anchor-pairs"))?;
    let corpus = Arc::new(corpus::Corpus::load()?);
    let started = Instant::now();
    let offered = Arc::new(Mutex::new(vec![]));
    let subscriptions = Arc::new(AtomicU64::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:19600").await?;
    let incoming = stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let g = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(
                geyser_server::GeyserServer::new(grpc::Fixture {
                    corpus: corpus.clone(),
                    subscriptions: subscriptions.clone(),
                    offered: offered.clone(),
                })
                .max_encoding_message_size(32 << 20),
            )
            .serve_with_incoming(incoming),
    );
    // 19602 belongs to the bidirectional delay proxy on Geyser <-> backend.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:19603").await?;
    let calls = Arc::new(Mutex::new(vec![]));
    let h = tokio::spawn(http::serve(listener, corpus.clone(), calls.clone()));
    let mut c = IngestionConfig::default();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_delivery_mode = "durable_association_v1".into();
    c.yellowstone_grpc_url = std::env::var("INGRESS_GRPC_URL")?;
    c.yellowstone_x_token = "MOCK-OFFLINE-NO-CREDENTIAL".into();
    c.yellowstone_reconnect_initial_ms = 100;
    c.yellowstone_reconnect_max_ms = 1000;
    c.fetch_concurrency = 10;
    let limits: AssociationDeliveryConfig = serde_json::from_slice(&std::fs::read(
        PathBuf::from(std::env::var("INGRESS_CORPUS_DIR")?).join("limits.json"),
    )?)?;
    ensure!(
        limits.blocks.count == 384 && limits.blocks.bytes == 1664 << 20,
        "accepted blocks cap"
    );
    c.yellowstone_association = Some(limits.clone());
    c.yellowstone_http_recovery = Some(HttpRecoveryConfig {
        anchor_evidence_dir: Some(out.join("anchor-pairs").to_str().unwrap().into()),
        broker_url: "http://127.0.0.1:19603".into(),
        broker_token: String::new(),
        range_slots: 1024,
        max_response_bytes: 16 << 20,
        timeout_ms: 30_000,
        fetch_concurrency: 10,
        raw_window_blocks: Some(32),
    });
    let path = out.join("ACK.sqlite");
    ensure!(!path.exists(), "isolated fresh db");
    SqliteStore::open(&path)?.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let wallets = HashSet::from([corpus.wallet.clone()]);
    let scope = crate::replay_scope(&c, &wallets)?;
    let mut db = AssociationInbox::open(
        &path,
        InboxLimits {
            count: limits.inbox.count,
            bytes: limits.inbox.bytes,
            busy_ms: limits.sqlite_busy_ms,
        },
    )?;
    db.configure_replay_scope(&scope)?;
    native::authority(&mut db, &corpus)?;
    // Pair runner supplies one future absolute epoch to both isolated namespaces.
    if let Ok(wait) = (corpus.started - chrono::Utc::now()).to_std() {
        tokio::time::sleep(wait).await;
    }
    let mut r = DeliveryReceiver::start_recovering_labeled(
        &c,
        "MOCK-offline-real-route".into(),
        wallets,
        None,
        None,
    )?;
    let hold = r.http_continuity_hold().context("HTTP hold")?;
    let mut acks = vec![];
    let mut events = vec![];
    let mut fences = vec![];
    let mut quotes = vec![];
    let mut timeline = vec![];
    let mut fresh_admission_created = None;
    let seconds = std::env::var("INGRESS_TIMEOUT_SECONDS")
        .unwrap_or_else(|_| "55".into())
        .parse::<u64>()?;
    ensure!((1..=180).contains(&seconds), "bounded fixture timeout");
    let result = tokio::time::timeout(Duration::from_secs(seconds), async {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            let e = tokio::select! {
                e = r.next() => e?.context("route closed")?,
                _ = interval.tick() => { timeline.push(sample(&r, &corpus, started.elapsed().as_secs_f64())); continue; }
            };
            let observed = chrono::Utc::now();
            // Facts/Admission originate in transport + production decoder only.
            db.persist_at(&e.delivery, &CandidateGeneration::Unknown, observed)?;
            if matches!(&e.delivery.event, DeliveryEvent::Session(SessionGap::StartedContinuityUnknown)) {
                let slot = native::fence(&mut db, &corpus, &e.delivery.session)?;
                fences.push(json!({"session":e.delivery.session,"slot":slot,"sampled_unix_ns":observed.timestamp_nanos_opt().unwrap(),"boundary":"independent finite producer clock, never ACK"}));
            }
            if let DeliveryEvent::Admission(a) = &e.delivery.event {
                let exact = a.facts.exact_amounts.as_ref().context("saved shape exact swap amounts")?;
                events.push(json!({"kind":"Admission","signature":a.facts.signature,"slot":a.facts.slot,"message_time":a.message_time,"exact_amounts":exact,"received_unix_ns":observed.timestamp_nanos_opt().unwrap(),"continuity_hold":hold.load(Ordering::Acquire)}));
                ensure!(a.facts.wallet == corpus.wallet && exact.amount_in_raw == "21616157" && exact.amount_out_raw == "115186657", "common production decoder facts");
                if a.facts.slot == corpus::FRESH {
                    fresh_admission_created = Some(a.message_time.clone());
                    ensure!(!hold.load(Ordering::Acquire), "fresh admitted during recovery hold");
                }
            }
            if let DeliveryEvent::ParentCheckpoint(p) = &e.delivery.event {
                let committed = db.replay_checkpoint(&scope)?.context("durable cursor")?;
                ensure!(committed.block.observation.child == p.observation.child, "parent/hash durable identity");
                r.acknowledge_checkpoint(committed)?;
                r.acknowledge_parent(p.observation.child.slot);
                let s = r.ingress_snapshot();
                acks.push(json!({"slot":p.observation.child.slot,"parent_slot":p.observation.parent.slot,"hash":p.observation.child.hash,"ack_unix_ns":observed.timestamp_nanos_opt().unwrap(),"producer_to_checkpoint_ms":(observed-corpus.created(p.observation.child.slot)).num_milliseconds(),"phase":if s.reconnects == 0 {"before_recovery"} else if s.processing.http_recovery.caught_up_to_anchor {"after_recovery"} else {"during_recovery"},"producer_latest_slot":corpus.latest()}));
            }
            if let DeliveryEvent::Terminal { signature, result: Terminal::ProviderAsserted(proof), .. } = &e.delivery.event {
                if proof.slot == corpus::FRESH { quotes.push(native::mock_quote(&path, &corpus, signature, proof.slot)?); }
            }
            if matches!(&e.delivery.event, DeliveryEvent::ParentCheckpoint(p) if p.observation.child.slot == corpus::LAST) { break; }
        }
        Ok::<_, anyhow::Error>(())
    }).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    timeline.push(sample(&r, &corpus, started.elapsed().as_secs_f64()));
    let snapshot = r.ingress_snapshot();
    r.stop();
    drop(r);
    g.abort();
    h.abort();
    let counts = sql_counts(&path)?;
    let http_bytes: u64 = calls
        .lock()
        .unwrap()
        .iter()
        .map(|v| v["response_bytes"].as_u64().unwrap())
        .sum();
    // Persist all observations before any RED postcondition; raw pair artifacts stay.
    std::fs::write(
        out.join("DIAGNOSTIC.json"),
        serde_json::to_vec_pretty(&json!({
            "accepted_source_sha":"efd835be93af2cd3d8aef9ac6140b932af881f01",
            "elapsed_seconds":started.elapsed().as_secs_f64(),"common_start_unix_ns":corpus.started.timestamp_nanos_opt().unwrap(),
            "producer_step_ns":corpus::STEP_NS,"finite_latest_slot":corpus::LAST,
            "synthetic_wire_padding_target_bytes":4_160_000,"fixture_queue_declared_max_bytes":(corpus::LAST-corpus::FIRST+1)*4_160_000,
            "events":events,"acks":acks,"fences":fences,"offered":offered.lock().unwrap().clone(),
            "http":calls.lock().unwrap().clone(),"http_response_bytes":http_bytes,"quotes":quotes,
            "timeline":timeline,"final":sample_snapshot(&snapshot,&corpus),"sql_counts":counts,
            "subscriptions":subscriptions.load(Ordering::Acquire),"bounded_route_result":format!("{result:?}"),
            "producer_signatures":"synthetic identities only; no cryptographic signatures created",
            "old_and_recovered_decision_expected":0,"orders_receipts_expected":0,
            "external_provider_calls":0,"financial_calls":0,"historical_internal_reproduced":false
        }))?,
    )?;
    result.context("bounded actual transport route")??;
    ensure!(
        subscriptions.load(Ordering::Acquire) == 2,
        "single controlled disconnect"
    );
    ensure!(quotes.len() == 1, "fresh BUY exactly once");
    let expected = corpus::stamp(corpus.created(corpus::FRESH));
    ensure!(
        fresh_admission_created
            == Some(MessageTime::CreatedAt {
                seconds: expected.seconds,
                nanos: expected.nanos as u32
            }),
        "producer CreatedAt preserved through every hop"
    );
    for slot in [corpus::OLD, corpus::RECOVERED, corpus::FRESH] {
        ensure!(
            db.identity(&corpus.signature(slot))?.is_some(),
            "transport synthetic BUY all observed"
        );
    }
    ensure!(
        counts["native_buy_cohort_decisions"] == 1,
        "old/recovered no decision; fresh exactly once"
    );
    for table in [
        "orders",
        "fills",
        "positions",
        "execution_canary_receipt_facts",
    ] {
        ensure!(counts[table] == 0, "no financial calls/tables");
    }
    ensure!(http_bytes < 5_057_282_048, "unchanged archive threshold");
    ensure!(
        snapshot.processing.http_recovery.caught_up_to_anchor,
        "HTTP recovery must complete"
    );
    ensure!(
        snapshot.last_durably_stored_parent_slot >= corpus::LAST,
        "durable linked last ACK"
    );
    ensure!(
        corpus
            .latest()
            .saturating_sub(snapshot.last_durably_stored_parent_slot)
            <= 8,
        "independent producer/durable progress"
    );
    std::fs::write(
        out.join("RESULT.json"),
        serde_json::to_vec_pretty(&json!({
            "status":"PASS_OFFLINE_TRANSPORT_NATIVE_MOCK_ONLY","accepted_source_sha":"efd835be93af2cd3d8aef9ac6140b932af881f01",
            "installed_app_unchanged":true,"producer_latest_slot":corpus.latest(),"durable_slot":snapshot.last_durably_stored_parent_slot,
            "fresh_quote":quotes,"old_buy":"fence/stale refusal; zero native decision","recovered_buy":"RecoveredBlock refusal; zero native decision",
            "http_requests":calls.lock().unwrap().len(),"http_response_bytes":http_bytes,"archive_threshold":5_057_282_048u64,
            "anchor_match":"unchanged full comparator; preserved anchor-pairs plus durable linked ACKs","durable_ack_slots":acks,
            "financial_tables":0,"external_provider_calls":0,"signatures_created":0,"live_copy_proven":false
        }))?,
    )?;
    Ok(())
}
fn sample_snapshot(s: &crate::DurableIngressSnapshot, corpus: &corpus::Corpus) -> Value {
    json!({"producer_latest_slot":corpus.latest(),"received_blocks":s.received_blocks,"received_transactions":s.received_transactions,
      "durable_slot":s.last_durably_stored_parent_slot,"reconnects":s.reconnects,
      "http_backlog":s.processing.http_recovery.current_backlog_slots,"recovery_completed":s.processing.http_recovery.caught_up_to_anchor,
      "http_anchor":s.processing.http_recovery.live_anchor_slot,"selected_source":s.selected_source,
      "admissions":s.admissions,"decode_errors":s.decode_errors})
}
