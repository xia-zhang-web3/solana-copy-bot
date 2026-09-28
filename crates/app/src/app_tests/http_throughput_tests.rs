//! Sustained genuine transport→association→SQLite measurement, no execution.
use super::http_throughput_fixture as f;
use crate::association_consumer::AssociationConsumer;
use anyhow::{Context, Result};
use copybot_config::{ExecutionConfig, HttpRecoveryConfig};
use copybot_ingestion::IngestionService;
use copybot_storage_core::SqliteStore;
use prost::Message;
use serde_json::json;
use std::{
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit saved corpus/config; 40s real sustained loopback cadence"]
async fn full_blocks_two_relays_processing_and_durable_commit_at_three_per_second() -> Result<()> {
    for isolated_reader in [false, true] {
        scenario(isolated_reader).await?;
    }
    Ok(())
}
async fn scenario(isolated_reader: bool) -> Result<()> {
    let root = PathBuf::from(std::env::var("COPYBOT_RUN15_EVIDENCE_DIR")?).join(format!(
        "throughput-{isolated_reader}-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    std::fs::create_dir_all(&root)?;
    let mut app = copybot_config::load_from_path(std::env::var("COPYBOT_RUN15_RUNTIME_CONFIG")?)?;
    app.execution = ExecutionConfig {
        canary_entry_submit_enabled: false,
        ..Default::default()
    };
    app.ingestion.yellowstone_x_token = "local-only".into();
    // Match the daemon's effective subscription fallback, rather than emitting
    // no individual transactions when the explicit Yellowstone list is empty.
    if app.ingestion.yellowstone_program_ids.is_empty() {
        app.ingestion.yellowstone_program_ids = app.ingestion.subscribe_program_ids.clone();
    }
    if app.ingestion.yellowstone_program_ids.is_empty() {
        app.ingestion.yellowstone_program_ids = app
            .ingestion
            .raydium_program_ids
            .iter()
            .chain(&app.ingestion.pumpswap_program_ids)
            .cloned()
            .collect();
    }
    app.ingestion.yellowstone_replay_wallets = vec![bs58::encode([19; 32]).into_string()];
    app.ingestion.yellowstone_http_recovery = isolated_reader.then(|| HttpRecoveryConfig {
        broker_url: "http://127.0.0.1:1".into(),
        broker_token: "local-only".into(),
        range_slots: 64,
        max_response_bytes: app
            .ingestion
            .yellowstone_association
            .as_ref()
            .unwrap()
            .input_bytes,
        timeout_ms: 1000,
        fetch_concurrency: 1,
    });
    let bodies = f::bodies_from_http(&PathBuf::from(std::env::var("COPYBOT_RUN15_CORPUS_DIR")?))?;
    let fixture = f::Fixture {
        control: Arc::default(),
        bodies,
        programs: app
            .ingestion
            .yellowstone_program_ids
            .iter()
            .map(|p| bs58::decode(p).into_vec())
            .collect::<std::result::Result<_, _>>()?,
    };
    anyhow::ensure!(
        fixture
            .bodies
            .iter()
            .all(|b| b.transactions.iter().any(|info| fixture.selected(info))),
        "throughput fixture must include provider-selected individual transaction workload"
    );
    let body_facts = fixture.bodies.iter().map(|b| json!({
        "protobuf_bytes":b.encoded_len(),"transactions":b.transactions.len(),
        "config_markers":b.transactions.iter().filter(|i|i.transaction.as_ref()
            .and_then(|t|t.message.as_ref()).is_some_and(|m|m.config.is_some())).count(),
        "individual_transaction_stream_count":b.transactions.iter().filter(|i|fixture.selected(i)).count(),
    })).collect::<Vec<_>>();
    let control = Arc::clone(&fixture.control);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let incoming = futures_util::stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let tonic = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(yellowstone_grpc_proto::prelude::geyser_server::GeyserServer::new(fixture))
            .serve_with_incoming(incoming),
    );
    let mut relays = f::Relays::start(&root, port).await?;
    app.ingestion.yellowstone_grpc_url = relays.url.clone();
    copybot_config::validate_association_delivery(&app)?;
    let path = root.join("throughput.sqlite");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
    let sql = rusqlite::Connection::open(&path)?;
    let mut service = IngestionService::build_for_app(&app)?;
    let mut consumer =
        AssociationConsumer::start(&mut service, &app.ingestion, &path.to_string_lossy())
            .await?
            .context("throughput actual consumer")?;
    let started = Instant::now();
    let mut watermarks = vec![];
    let mut last_parent = 0;
    let mut backlog_max = 0;
    let mut end_to_end_us_max = 0;
    tokio::time::timeout(Duration::from_secs(30),async {
        loop {
            consumer.poll(&store).await?;
            let (wire,_) = consumer.diagnostic_snapshots();
            let produced = control.produced.load(Ordering::SeqCst);
            let committed = if wire.last_durably_stored_parent_slot < f::START_SLOT {0}
                else {wire.last_durably_stored_parent_slot-f::START_SLOT+1};
            backlog_max = backlog_max.max(produced.saturating_sub(committed));
            anyhow::ensure!(!control.overflow.load(Ordering::SeqCst),"throughput fixed source batch queue overflow");
            if wire.last_durably_stored_parent_slot != last_parent {
                let age = control.produced_at.lock().unwrap()[committed as usize-1].elapsed();
                end_to_end_us_max = end_to_end_us_max.max(age.as_micros());
                watermarks.push(json!({"elapsed_us":started.elapsed().as_micros(),
                    "produced":produced,"received":wire.received_blocks,"committed":committed,
                    "backlog":produced.saturating_sub(committed),"block_end_to_end_us":age.as_micros(),
                    "input_queue_count":wire.processing.input_queue_count,
                    "input_queue_bytes":wire.processing.input_queue_bytes}));
                last_parent = wire.last_durably_stored_parent_slot;
            }
            if committed==f::COUNT { break Ok::<_,anyhow::Error>(()); }
        }
    }).await.context("sustained throughput drain timeout")??;
    // Persistence can finish before the enclosing update timer is recorded.
    // Observe that timer without waiting for a nonexistent 61st delivery.
    let final_snapshot = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let (wire, _) = consumer.diagnostic_snapshots();
            if wire.processing.block_update.count == f::COUNT {
                break wire;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("final throughput measurement observation")?;
    let wall_us = started.elapsed().as_micros();
    let (source_span_us, source_interval_us_max) = {
        let produced = control.produced_at.lock().unwrap();
        assert_eq!(produced.len(), f::COUNT as usize);
        (
            produced
                .last()
                .unwrap()
                .duration_since(produced[0])
                .as_micros(),
            produced
                .windows(2)
                .map(|pair| pair[1].duration_since(pair[0]).as_micros())
                .max()
                .unwrap(),
        )
    };
    let head: u64 = sql.query_row(
        "SELECT json_extract(head,'$.block.observation.child.slot') FROM association_replay_cursor",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(head, f::START_SLOT + f::COUNT - 1);
    assert_eq!(final_snapshot.received_blocks, f::COUNT);
    assert_eq!(final_snapshot.reconnects, 0);
    assert_eq!(final_snapshot.admissions, 0);
    assert!(final_snapshot.received_transactions > 0);
    if isolated_reader {
        assert!(final_snapshot.processing.filtered_foreign_transactions > 0);
    } else {
        assert!(final_snapshot.decoded_swaps > 0);
    }
    assert_eq!(control.requests.load(Ordering::SeqCst), 1);
    let mut financial = serde_json::Map::new();
    for table in [
        "orders",
        "fills",
        "positions",
        "owner_exit_intents",
        "native_buy_technical_cohort",
    ] {
        let count: u64 =
            sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
        assert_eq!(count, 0);
        financial.insert(table.into(), count.into());
    }
    drop(consumer);
    tonic.abort();
    let relay_states = relays.finish().await?;
    for role in ["front", "backend"] {
        assert_eq!(relay_states[role]["phase"], "stopped");
        // Only backend attempts TCP upstream; front opens the Unix relay.
        assert_eq!(
            relay_states[role]["connect_attempts"],
            u64::from(role == "backend")
        );
        assert_eq!(relay_states[role]["connections"], 1);
        assert_eq!(relay_states[role]["failures"], 0);
    }
    let s = final_snapshot.processing;
    let timing = |s: &copybot_ingestion::TimingSnapshot| {
        json!({
        "count":s.count,"total_us":s.total_us,"max_us":s.max_us})
    };
    let output = json!({"result":"OFFLINE_SUSTAINED_TRANSPORT_PASS","isolated_reader":isolated_reader,
        "runtime":{"os":std::env::consts::OS,"arch":std::env::consts::ARCH,
            "debug_assertions":cfg!(debug_assertions),"tokio_workers":4,
            "relay_host":"canonical Python relays on host loopback","installed_container_cgroup":"NOT_TESTED"},
        "models":"Repeated captured transaction bodies; header slots/hashes, empty block rewards and 3/s cadence modeled. Saved getBlock requests disabled rewards; no historical reward completeness, provider latency or lagged claim.",
        "blocks":f::COUNT,"interval_us":f::INTERVAL_US,"wall_us":wall_us,"body_facts":body_facts,
        "observed_source_span_us":source_span_us,"observed_source_interval_us_max":source_interval_us_max,
        "produced_minus_committed_max":backlog_max,"block_end_to_end_us_max":end_to_end_us_max,
        "association":timing(&s.association),"whole_update":timing(&s.update),
        "block_update":timing(&s.block_update),"individual_transaction_update":timing(&s.transaction_update),
        "input_queue_age":timing(&s.input_age),"envelope_through_committed_ack":timing(&s.durable_ack),
        "input_queue_count_max":s.input_queue_count_max,"input_queue_bytes_max":s.input_queue_bytes_max,
        "captured_charged_bytes":s.input_received_bytes,"received_individual_transactions":final_snapshot.received_transactions,
        "filtered_foreign_transactions":s.filtered_foreign_transactions,
        "decoded_swaps":final_snapshot.decoded_swaps,"foreign_signer":final_snapshot.foreign_signer,
        "watermarks":watermarks,"relay_states":relay_states,"financial_counts":financial,
        "external_DataLoss_cause":"UNKNOWN"});
    std::fs::write(
        root.join("RESULT.json"),
        serde_json::to_vec_pretty(&output)?,
    )?;
    Ok(())
}
