//! Local wall-clock causal witness; no daemon, provider, submit or market proof.
use super::*;
use crate::source::durable::{queue, recovery::RecoveryCursor};
use chrono::Utc;
use copybot_config::IngestionConfig;
use copybot_core_types::{association_delivery::*, association_recovery::*};
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    SqliteStore,
};
use std::{
    collections::HashSet,
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::sync::oneshot;
mod corpus;
mod http;
mod live;
#[path = "../recovery_live_control.rs"]
mod native_control;

#[derive(Debug)]
struct Drained {
    elapsed: Duration,
    lag_ms: i64,
    slot: u64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires sealed saved06 bodies/independent anchors/accepted limits; real delayed loopback only"]
async fn saved_06_real_delayed_bodies_sqlite_ack_reader_drain_then_fresh_mock_quote_10_32() {
    let profiles: &[usize] = match std::env::var("COPYBOT_RECOVERY_06_E2E_PROFILE") {
        Err(std::env::VarError::NotPresent) => &[0, 1],
        Ok(value) if value == "465" => &[0],
        Ok(value) if value == "659" => &[1],
        other => panic!("expected absent/465/659 COPYBOT_RECOVERY_06_E2E_PROFILE: {other:?}"),
    };
    for &number in profiles {
        run_profile(number).await.unwrap();
    }
}

async fn run_profile(number: usize) -> Result<()> {
    let corpus = Arc::new(corpus::Corpus::load(number)?);
    let server = http::Server::start(corpus.clone()).await?;
    let metadata: serde_json::Value = serde_json::from_slice(&std::fs::read(std::env::var(
        "COPYBOT_RECOVERY_06_LIMITS_FILE",
    )?)?)?;
    let limits: AssociationDeliveryConfig =
        serde_json::from_value(metadata["yellowstone_association"].clone())?;
    ensure!(
        limits.blocks.count == 384 && limits.blocks.bytes == 1664 << 20,
        "accepted live budgets"
    );
    let http_config = HttpRecoveryConfig {
        anchor_evidence_dir: None,
        broker_url: server.url.clone(),
        broker_token: String::new(),
        range_slots: 1024,
        max_response_bytes: 16 << 20,
        timeout_ms: 30_000,
        fetch_concurrency: 10,
        raw_window_blocks: Some(32),
    };
    let mut config = IngestionConfig::default();
    config.source = "yellowstone_grpc".into();
    config.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    config.yellowstone_x_token = "local-no-provider".into();
    config.yellowstone_delivery_mode = "durable_association_v1".into();
    config.fetch_concurrency = 10;
    config.yellowstone_association = Some(limits.clone());
    config.yellowstone_http_recovery = Some(http_config.clone());
    copybot_config::validate_delivery_source(&config)?;
    let saved: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../storage-core/tests/fixtures/recovery_06.json"
    ))?;
    let wallet = saved["authority"]["wallet_ids"][0]
        .as_str()
        .context("saved wallet")?
        .to_owned();
    let wallets = HashSet::from([wallet.clone()]);
    let scope = crate::replay_scope(&config, &wallets)?;
    let source = crate::source::YellowstoneGrpcSource::new(&config)?;
    let mut runtime = (*source.runtime_config).clone();
    runtime.admission_wallets = Some(wallets);
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("causal.sqlite");
    SqliteStore::open(&path)?.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let mut inbox = AssociationInbox::open(
        &path,
        InboxLimits {
            count: limits.inbox.count,
            bytes: limits.inbox.bytes,
            busy_ms: limits.sqlite_busy_ms,
        },
    )?;
    inbox.configure_replay_scope(&scope)?;
    let initial = corpus.initial()?;
    let initial_slot = initial.slot;
    let checkpoint = BlockCheckpoint {
        scope: scope.clone(),
        observation: crate::source::durable::parent::observation(&initial),
        executed_transaction_count: initial.executed_transaction_count,
        supplied_transaction_count: initial.transactions.len() as u64,
        claims: vec![],
    };
    inbox.persist(
        &Delivery {
            session: "MOCK-initial-durable-parent".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::ParentCheckpoint(checkpoint),
        },
        &CandidateGeneration::Unknown,
    )?;
    drop(initial);
    let hold = Arc::new(AtomicBool::new(true));
    let mut cursor = RecoveryCursor::new(
        scope.clone(),
        inbox.replay_checkpoint(&scope)?,
        limits.history.clone(),
    )?;
    cursor.http_hold = Some(hold.clone());
    let telemetry = Arc::new(DurableIngressTelemetry::default());
    let (sender, mut receiver) = queue::channel(limits.queue.count, limits.queue.bytes);
    let (ready_tx, mut ready_rx) = oneshot::channel();
    let (stop_tx, stop_rx) = oneshot::channel();
    let own_cursor = cursor.clone();
    let own_telemetry = telemetry.clone();
    let anchor = Arc::new(corpus.anchor.clone());
    let start = Instant::now();
    let activated = Utc::now();
    // The production API requires authority + first epoch BEFORE first admission.
    // This establishes a new offline fixture authority, with an immutable deadline;
    // the same authority appends the latest live epoch only after real Reader drain.
    let _initial_authority = native_control::Control::register(
        &mut inbox,
        &path,
        &format!("MOCK-real-recovery-{number}:0"),
        initial_slot,
        activated,
        activated,
    );
    let client =
        ConfirmedHttpRecovery::new(&server.url, None, 1024, 16 << 20, Duration::from_secs(30))?;
    let worker = tokio::spawn(async move {
        let mut bridge = Bridge::new(
            &runtime,
            &limits,
            format!("MOCK-real-recovery-{number}"),
            sender,
            None,
            own_telemetry.clone(),
        )?;
        bridge.enable_recovery(own_cursor.clone(), &limits)?;
        bridge
            .emit(
                ns(start),
                DeliveryEvent::Session(SessionGap::StartedContinuityUnknown),
            )
            .await?;
        let from = bridge.begin_replay()?.context("durable from slot")?;
        let incoming = live::incoming(anchor.clone(), bs58::decode(wallet).into_vec()?, activated);
        let mut reader = Reader::start(
            incoming,
            futures_util::sink::drain(),
            limits.blocks.count,
            limits.blocks.bytes,
            limits.input_bytes,
            own_telemetry.clone(),
            bridge.begin_capture()?,
        )?;
        let mut captured_anchor = reader.next().await.context("independent captured anchor")?;
        let Some(subscribe_update::UpdateOneof::Block(observed)) =
            captured_anchor.update()?.update_oneof.as_ref()
        else {
            anyhow::bail!("captured anchor kind")
        };
        catch_up(
            &client,
            &http_config,
            from,
            observed,
            &mut bridge,
            start,
            &own_telemetry,
        )
        .await?;
        ensure!(!bridge.replay_waiting_anchor(), "anchor durable ACK");
        reader.clear_verified();
        captured_anchor.dequeue();
        drop(captured_anchor);
        drop(anchor);
        loop {
            let Some(mut value) = reader.next().await else {
                let reason = match reader.end().await? {
                    End::Budget(r) => r.to_owned(),
                    End::Status(e) => format!("{e}"),
                    End::Ping(e) => format!("{e}"),
                    End::Eof => "EOF".into(),
                };
                anyhow::bail!("Reader refused before drain: {reason}");
            };
            value.dequeue();
            let update = value.update()?;
            let timestamp = update
                .created_at
                .as_ref()
                .context("real scheduled CreatedAt")?;
            let created =
                chrono::DateTime::from_timestamp(timestamp.seconds, timestamp.nanos as u32)
                    .context("CreatedAt")?;
            let block = value.is_block();
            // End the borrow before update() reconstructs/borrows the next operand.
            process(&mut bridge, ns(start), value.update()?, &own_telemetry).await?;
            drop(value);
            if !block {
                continue;
            }
            bridge.wait_checkpoint(Duration::from_secs(30)).await?;
            let snapshot = own_telemetry.snapshot();
            let slot = own_cursor
                .committed_slot()?
                .context("latest durable parent")?;
            let lag_ms = (Utc::now() - created).num_milliseconds();
            if snapshot.processing.input_queue_count == 0
                && snapshot.last_received_block_slot == slot
                && lag_ms < 1000
            {
                ensure!(
                    !own_cursor
                        .http_hold
                        .as_ref()
                        .unwrap()
                        .load(Ordering::Acquire),
                    "Reader interruption hold"
                );
                let elapsed = start.elapsed();
                ensure!(
                    elapsed <= Duration::from_secs(120),
                    "actual catchup/drain exceeds age horizon"
                );
                ready_tx
                    .send(Drained {
                        elapsed,
                        lag_ms,
                        slot,
                    })
                    .map_err(|_| anyhow::anyhow!("ready consumer lost"))?;
                // Keep owning Reader until native mock completes. DROP closes hold;
                // stopping it before control would manufacture a continuity release.
                let _ = stop_rx.await;
                drop(reader);
                return Ok::<_, anyhow::Error>(());
            }
        }
    });
    let mut worker = worker;
    let mut session = String::new();
    let mut sequence = 0u64;
    let mut stored = 0u64;
    let drained = tokio::time::timeout(Duration::from_secs(125), async {
        loop { tokio::select! {
            ready = &mut ready_rx => { return Ok::<_, anyhow::Error>(ready?); }
            done = &mut worker => { done??; anyhow::bail!("worker stopped before fresh control"); }
            envelope = receiver.recv() => {
                let Some(envelope) = envelope else {
                    // A producer refusal closes the delivery channel before select!
                    // necessarily observes its JoinHandle. Preserve its actual cause.
                    (&mut worker).await??;
                    anyhow::bail!("delivery producer closed without worker refusal");
                };
                let observed = Utc::now();
                inbox.persist_at(&envelope.delivery, &CandidateGeneration::Unknown, observed)?;
                session = envelope.delivery.session.clone(); sequence = envelope.delivery.sequence + 1; stored += 1;
                if let DeliveryEvent::ParentCheckpoint(p) = &envelope.delivery.event {
                    let committed = inbox.replay_checkpoint(&scope)?.context("committed SQLite cursor")?;
                    ensure!(committed.block.observation.child == p.observation.child, "SQLite parent ACK identity");
                    cursor.acknowledge(committed)?; telemetry.acknowledge_parent(p.observation.child.slot);
                }
                telemetry.processing.durable_ack(envelope.elapsed());
            }
        } }
    }).await;
    let snapshot = telemetry.snapshot();
    let drained = match drained {
        Ok(Ok(v)) => v,
        other => {
            eprintln!("REAL_SAVED_RECOVERY count={} elapsed_ms={} result={other:?} helper={} snapshot={snapshot:?} provider_requests=0", corpus.count, start.elapsed().as_millis(), server.counts.witness());
            worker.abort();
            anyhow::bail!("actual saved recovery failed");
        }
    };
    ensure!(
        snapshot.processing.input_queue_count_max <= 384
            && snapshot.processing.input_queue_bytes_max <= 1664 << 20,
        "unchanged live capture caps"
    );
    ensure!(
        snapshot.received_transactions > 0,
        "other update kinds charged through Reader"
    );
    ensure!(
        server.counts.blocks.load(Ordering::Relaxed) == corpus.count,
        "actual full range normalized"
    );
    ensure!(
        server.counts.max_active.load(Ordering::Relaxed) <= 10,
        "HTTP width"
    );
    ensure!(
        server.counts.max_preparing.load(Ordering::Relaxed) <= 10
            && server.counts.prepared_blocks.load(Ordering::Relaxed) == corpus.count
            && server.counts.response_floor_checks.load(Ordering::Relaxed) == corpus.count
            && server
                .counts
                .response_floor_violations
                .load(Ordering::Relaxed)
                == 0,
        "bounded helper preparation and every recorded response service floor"
    );
    ensure!(
        snapshot.processing.http_recovery.caught_up_to_anchor
            && snapshot.last_durably_stored_parent_slot == drained.slot,
        "latest live durable parent"
    );
    ensure!(
        !hold.load(Ordering::Acquire),
        "continuity before fresh control"
    );
    let sampled = Utc::now();
    let control = native_control::Control::register(
        &mut inbox,
        &path,
        &session,
        drained.slot,
        activated,
        sampled,
    );
    let created = Utc::now();
    let observed = Utc::now();
    let outcome = control.assert_fresh_once(&mut inbox, sequence, created, observed);
    let quote_completed = Utc::now();
    ensure!(
        quote_completed - created < chrono::Duration::seconds(1),
        "actual fresh-to-mock latency"
    );
    ensure!(outcome == "MOCK_ONLY_NO_SUBMIT", "one mock quote only");
    eprintln!("REAL_SAVED_RECOVERY count={} actual_elapsed_ms={} reader_count_max={} reader_bytes_max={} drained_input_age_ms={} latest_durable_slot={} deliveries={} normalized_bodies={} served_id_adapted_body_bytes={} http_width_max={} fresh_age_ms={} quote={outcome} helper={} provider_requests=0 market_decoder_control=false failed_attempt_service_is_lower_bound=true list_latency_unrecorded=true snapshot={snapshot:?}",
        corpus.count, drained.elapsed.as_millis(), snapshot.processing.input_queue_count_max, snapshot.processing.input_queue_bytes_max, drained.lag_ms, drained.slot, stored,
        server.counts.blocks.load(Ordering::Relaxed), server.counts.bytes.load(Ordering::Relaxed), server.counts.max_active.load(Ordering::Relaxed), (quote_completed-created).num_milliseconds(), server.counts.witness());
    let _ = stop_tx.send(());
    worker.await??;
    ensure!(
        hold.load(Ordering::Acquire),
        "STOP immediately restores hold"
    );
    Ok(())
}
