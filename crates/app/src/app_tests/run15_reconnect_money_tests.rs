//! True tonic→two canonical relays→daemon consumer, interruption after BUY.
//! Prices, receipts, skipped slots and execution boundaries are explicit offline models.
use super::{
    run15_full_path_fixture as f, run15_full_path_frames as frames, run15_full_path_server::Server,
    run15_reconnect_tonic_fixture::Fixture, run15_rpc_proof_fixture as proof,
};
use crate::{association_consumer::AssociationConsumer, execution_canary::ExecutionCanaryRunner};
use anyhow::{Context, Result};
use copybot_ingestion::IngestionService;
use copybot_storage_core::SqliteStore;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::atomic::Ordering, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit saved corpus/config and offline model boundaries"]
async fn tonic_two_relays_break_after_buy_replays_sell_unknown_restart_no_send_two() -> Result<()> {
    for unknown in [true] {
        scenario(unknown, false, false).await?;
    }
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit saved corpus and genuine local HTTP recovery"]
async fn tonic_two_relays_http_catchup_sell_unknown_restart_no_send_two() -> Result<()> {
    scenario(true, true, false).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real HTTP anchor conflict after recovered source SELL"]
async fn tonic_two_relays_http_bad_anchor_never_sends_recovered_sell() -> Result<()> {
    scenario(true, true, true).await
}
async fn scenario(unknown: bool, http_mode: bool, bad_anchor: bool) -> Result<()> {
    let corpus = PathBuf::from(std::env::var("COPYBOT_RUN15_CORPUS_DIR")?);
    let input = PathBuf::from(std::env::var("COPYBOT_RUN15_FRAMES_DIR")?);
    let config_path = PathBuf::from(std::env::var("COPYBOT_RUN15_RUNTIME_CONFIG")?);
    let evidence_root = PathBuf::from(std::env::var("COPYBOT_RUN15_EVIDENCE_DIR")?);
    let root = evidence_root.join(format!(
        "tonic-recovery-case-{unknown}-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    std::fs::create_dir(&root)?;
    let (payload, model) =
        proof::model_follower_with_output_and_target(f::OWNED, 985_000_000, true)?;
    let e = f::evidence(&corpus, &model)?;
    let rpc_buy = f::parsed(&model, &payload)?;
    let server = Server::new(e, rpc_buy, unknown, root.join("model.db")).await?;
    let mut app = f::config(&config_path, &root, &server.url, &model)?;
    let mut fixture = Fixture::build(&input, &model)?;
    fixture.http_mode = http_mode;
    if http_mode {
        // The old model encoder omitted returnData absence. Serve a complete
        // modeled gRPC Info matching the independent complete HTTP projection.
        let Some(yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Block(b)) =
            fixture.follower.update_oneof.as_mut()
        else {
            unreachable!()
        };
        *b = copybot_ingestion::normalize_confirmed_http_block(
            b.slot,
            &super::run15_http_rpc_fixture::block(b)?,
        )?;
    }
    let recovery_http = if http_mode {
        Some(
            super::run15_http_rpc_fixture::Http::start(
                fixture.clone(),
                &corpus,
                root.join("raw-http"),
                bad_anchor,
            )
            .await?,
        )
    } else {
        None
    };
    if let Some(http) = &recovery_http {
        app.ingestion.yellowstone_http_recovery = Some(copybot_config::HttpRecoveryConfig {
            anchor_evidence_dir: None,
            broker_url: http.url.clone(),
            broker_token: String::new(),
            range_slots: 1024,
            fetch_concurrency: app.ingestion.fetch_concurrency,
            max_response_bytes: 8_388_608,
            timeout_ms: 5000,
        });
    }
    let control = fixture.control.clone();
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
    let mut relays = Relays::start(&root, port).await?;
    app.ingestion.yellowstone_grpc_url = relays.url.clone();
    app.ingestion.yellowstone_reconnect_initial_ms = 10;
    app.ingestion.yellowstone_reconnect_max_ms = 10;
    let io = f::io(&app.execution, &corpus, payload, &model)?;
    let path = root.join("model.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
    copybot_storage_core::ensure_discovery_v2_schema(&store)?;
    let sql = Connection::open(&path)?;
    let runner = ExecutionCanaryRunner::new(app.execution.clone())
        .for_ingestion(&app.ingestion, &path.to_string_lossy())?
        .with_native_buy_mock(io.clone());
    let mut service = IngestionService::build_for_app(&app)?;
    let mut consumer = AssociationConsumer::start_with_execution(
        &mut service,
        &app.ingestion,
        &app.execution,
        &path.to_string_lossy(),
    )
    .await?
    .context("actual cohort consumer")?;
    let runner = runner.with_ingress_hold(consumer.http_continuity_hold());
    let mut fence =
        crate::execution_owned_sell_rpc::fractional::transport::Parsed(|r: Value| async move {
            let result = match r["method"].as_str() {
                Some("getGenesisHash") => json!("11111111111111111111111111111111"),
                Some("getSlot") => json!(frames::SOURCE_SLOT - 1),
                other => anyhow::bail!("unexpected fence {other:?}"),
            };
            Ok(json!({"jsonrpc":"2.0","id":r["id"],"result":result}))
        });
    let consume = async {
        loop {
            if control.done.load(Ordering::SeqCst) {
                break;
            }
            match tokio::time::timeout(
                Duration::from_millis(50),
                consumer.poll_with_transport(&store, Some(&mut fence)),
            )
            .await
            {
                Err(_) => continue,
                Ok(value) => match value {
                    Ok(()) => {}
                    Err(e) if e.to_string() == "association delivery stopped" => break,
                    Err(e) => return Err(e),
                },
            }
        }
        Ok::<_, anyhow::Error>(consumer.diagnostic_snapshots())
    };
    let execute = async {
        let mut first = true;
        loop {
            server.healthy()?;
            let summary = runner.process_tick(&store, chrono::Utc::now()).await?;
            if first && !store.list_native_buy_pending(1)?.is_empty()
                || summary.quote_entry_inserted > 0
                || summary.state_machine_reserved > 0
            {
                eprintln!("model BUY tick: quote={} reserved={} built={} simulated={} failed={} blocker={:?}",
                    summary.quote_entry_inserted,summary.state_machine_reserved,summary.state_machine_built,
                    summary.state_machine_simulated,summary.state_machine_failed,summary.state_machine_skipped_reason);
                first = false;
            }
            if io.counts.lock().unwrap().send == 1 {
                let position = store.load_execution_canary_open_position(proof::MINT)?;
                if position
                    .as_ref()
                    .and_then(|p| p.qty_exact.as_ref())
                    .is_some_and(|q| q.raw() == f::OWNED)
                {
                    control.bought.store(true, Ordering::SeqCst);
                    let durable:bool=sql.query_row("SELECT EXISTS(SELECT 1 FROM association_replay_cursor WHERE json_extract(head,'$.block.observation.child.slot')=?1 AND json_array_length(json_extract(head,'$.block.claims'))=1)", [frames::BOT_SLOT],|r|r.get(0))?;
                    if durable {
                        control.anchor_durable.store(true, Ordering::SeqCst);
                    }
                }
            }
            let id: Option<String> = sql
                .query_row(
                    "SELECT order_id FROM rpc_owned_sell_dispatches LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(id) = id {
                let send_completed = server.calls.lock().unwrap().iter().any(|r| r["method"] == "sendTransaction" && r["model_response_completed"] == true);
                if server.sends() == 1 && send_completed
                    && (unknown || store.load_execution_canary_cash_settlement(&id)?.is_some())
                {
                    break Ok::<_, anyhow::Error>(id);
                }
            }
            anyhow::ensure!(
                summary.last_error.is_none()
                    && summary.state_machine_failed == 0
                    && summary.state_machine_entry_gate_blocked == 0,
                "daemon tick refused: {summary:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let settle = async {
        let id = execute.await?;
        control.done.store(true, Ordering::SeqCst);
        Ok::<_, anyhow::Error>(id)
    };
    let outcome = tokio::time::timeout(Duration::from_secs(120), async {
        tokio::try_join!(consume, settle)
    })
    .await
    .context("full daemon model timeout")?;
    if bad_anchor {
        let error = outcome.expect_err("conflicting HTTP anchor accepted");
        assert!(format!("{error:#}").contains("confirmed_http_recovery_refused"), "{error:#}");
        assert_eq!(io.counts.lock().unwrap().send, 1, "confirmed BUY missing");
        assert_eq!(server.sends(), 0, "SELL sent before full-chain validation");
        let recovered_sells: i64 = sql.query_row("SELECT count(*) FROM association_inbox_identities WHERE json_extract(admission,'$.facts.slot')=451313058", [], |r|r.get(0))?;
        assert_eq!(recovered_sells, 1, "negative control never reached recovered SELL");
        let order_count: i64 = sql.query_row("SELECT count(*) FROM orders o JOIN copy_signals s ON s.signal_id=o.signal_id WHERE s.side='sell'", [], |r|r.get(0))?;
        assert_eq!(order_count, 0);
        std::fs::write(evidence_root.join("http-bad-anchor-money.json"), serde_json::to_vec_pretty(&json!({"result":"OFFLINE_NEGATIVE_PASS","error":format!("{error:#}"),"recovered_source_sell":recovered_sells,"buy_send":1,"sell_send":0}))?)?;
        control.done.store(true, Ordering::SeqCst);
        drop(consumer); tonic.abort(); relays.finish().await?;
        return Ok(());
    }
    let ((wire, _interval), id) = outcome?;
    if http_mode {
        let progress = &wire.processing.http_recovery;
        assert!(progress.caught_up_to_anchor, "{progress:?}");
        assert!(progress.durable_completed_slot >= progress.live_anchor_slot, "{progress:?}");
        assert!(progress.max_backlog_slots > progress.current_backlog_slots, "{progress:?}");
    }
    let tail = control.tail.lock().unwrap().len();
    assert!(tail > 0, "no stream commits overlapped SELL");
    assert_eq!(
        (wire.admissions, wire.selected_source, wire.selected_bot),
        (4, 2, 2), // replay generation observes bot Info again; durable first identity stays single.
        "{wire:?}"
    );
    // Consumer telemetry resets every 30 seconds; durable identities are cumulative.
    assert_eq!(
        sql.query_row(
            "SELECT count(*) FROM association_inbox_identities",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        3
    );
    assert_eq!(io.counts.lock().unwrap().send, 1);
    assert_eq!(server.sends(), 1);
    for method in ["simulateTransaction", "sendTransaction"] {
        let calls = server.calls.lock().unwrap();
        let call = calls.iter().find(|r| r["method"] == method).unwrap();
        assert!(
            call["model_parent_after"].as_i64().unwrap()
                > call["model_parent_before"].as_i64().unwrap(),
            "no persisted parent commit across {method}"
        );
    }
    let (n,d,h,raw):(u64,String,u64,u64)=sql.query_row("SELECT json_extract(decision,'$.inventory.numerator'),json_extract(decision,'$.inventory.denominator'),json_extract(decision,'$.owned_raw'),json_extract(decision,'$.selected_raw') FROM fractional_sell_decisions LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    assert_eq!(
        (n, d.as_str(), h, raw),
        (264731434, "6770149697", f::OWNED, f::SOLD)
    );
    let pre_recovery = store
        .load_execution_canary_open_position(proof::MINT)?
        .unwrap()
        .qty_exact
        .unwrap()
        .raw();
    if unknown {
        assert_eq!(pre_recovery, f::OWNED);
        assert!(store.load_execution_canary_cash_settlement(&id)?.is_none());
    }
    assert!(wire.reconnects >= 1, "no genuine transport interruption");
    assert_eq!(sql.query_row("SELECT json_extract(admission,'$.message_time') FROM association_inbox_identities WHERE json_extract(admission,'$.facts.slot')=?1", [frames::SOURCE_SLOT],|r|r.get::<_,String>(0))?, "Missing");
    assert_eq!(
        sql.query_row(
            "SELECT count(*) FROM native_buy_cohort_decisions",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    assert_eq!(control.requests.lock().unwrap()[1].is_none(), http_mode);
    if let Some(http) = &recovery_http {
        let calls = http.calls.lock().unwrap();
        assert!(calls.iter().any(|c| c["method"] == "getBlocks"));
        assert!(calls
            .iter()
            .any(|c| c["method"] == "getBlock" && c["params"][0] == frames::SELL_SLOT));
    }
    drop(consumer);
    drop(runner);
    drop(store);
    let reopened = SqliteStore::open(&path)?;
    let recovered = ExecutionCanaryRunner::new(app.execution.clone())
        .for_ingestion(&app.ingestion, &path.to_string_lossy())?
        .with_native_buy_mock(io.clone());
    control.done.store(false, Ordering::SeqCst);
    let requests_before = control.requests.lock().unwrap().len();
    let mut restarted_service = IngestionService::build_for_app(&app)?;
    let mut restarted_consumer = AssociationConsumer::start_with_execution(
        &mut restarted_service,
        &app.ingestion,
        &app.execution,
        &path.to_string_lossy(),
    )
    .await?
    .context("restart consumer")?;
    let recovered = recovered.with_ingress_hold(restarted_consumer.http_continuity_hold());
    tokio::time::timeout(Duration::from_secs(5), async {
        while control.requests.lock().unwrap().len() <= requests_before {
            restarted_consumer
                .poll_with_transport(&reopened, Some(&mut fence))
                .await?;
        }
        for _ in 0..20 {
            restarted_consumer
                .poll_with_transport(&reopened, Some(&mut fence))
                .await?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("durable cursor restoration on restart")??;
    assert_eq!(
        control.requests.lock().unwrap().last().unwrap().is_none(),
        http_mode
    );
    control.done.store(true, Ordering::SeqCst);
    drop(restarted_consumer);
    if unknown {
        // Recovery is a background job: observe a completed UNKNOWN batch before
        // making the modeled receipt available, rather than counting foreground ticks.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let s = recovered
                    .process_tick(&reopened, chrono::Utc::now())
                    .await?;
                if s.orphan_recovery_checked == 1 && s.last_error.is_some() {
                    break Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("restart UNKNOWN recovery batch")??;
        assert_eq!(server.sends(), 1, "UNKNOWN restart resent");
        assert!(reopened
            .load_execution_canary_cash_settlement(&id)?
            .is_none());
        server.unknown.store(false, Ordering::SeqCst);
    }
    let cash = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            recovered
                .process_tick(&reopened, chrono::Utc::now())
                .await?;
            if let Some(cash) = reopened.load_execution_canary_cash_settlement(&id)? {
                break Ok::<_, anyhow::Error>(cash);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("confirmed partial SELL settlement")??;
    assert_eq!(
        (cash.sold_quantity.raw(), cash.remaining_quantity.raw()),
        (f::SOLD, f::OWNED - f::SOLD)
    );
    assert_eq!(
        reopened
            .load_execution_canary_open_position(proof::MINT)?
            .unwrap()
            .qty_exact
            .unwrap()
            .raw(),
        f::OWNED - f::SOLD
    );
    assert_eq!(server.sends(), 1, "confirmed/restarted SELL resent");
    assert_eq!(io.counts.lock().unwrap().send, 1);
    let methods = server
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter_map(|r| r["method"].as_str().map(String::from))
        .collect::<Vec<_>>();
    for method in [
        "getBlock",
        "getTokenAccountsByOwnerAtSlot",
        "quote",
        "simulateTransaction",
        "sendTransaction",
        "getTransaction",
    ] {
        assert!(methods.iter().any(|m| m == method), "missing {method}");
    }
    let output = PathBuf::from(std::env::var("COPYBOT_RUN15_EVIDENCE_DIR")?);
    std::fs::write(
        output.join(format!(
            "tonic-recovery-http-{http_mode}-unknown-{unknown}.json"
        )),
        serde_json::to_vec_pretty(&json!({
        "result":"OFFLINE_MODEL_PASS","source":"actual response727 BUY / response725 SELL; unchanged slots/signatures/raw",
        "follower_receipt":format!("explicit synthetic Jupiter Raydium BUY 10m lamports→{} raw",f::OWNED),
        "parent_pages":"modeled complete parent program pages; not historical HTTP facts",
        "continuity":"sparse modeled skipped-slot headers; original mixed SELL transactions retained, block rewards/partitions modeled empty because saved request rewards=false; true tonic break/HTTP recovery",
        "subscription_from_slots":*control.requests.lock().unwrap(),
        "http_recovery":http_mode,"http_progress":format!("{:?}",wire.processing.http_recovery),"http_calls":recovery_http.as_ref().map(|h|h.calls.lock().unwrap().clone()),
        "sell_boundary":"model non-DEX bundle, simulation and receipt; no execution claim",
        "source_n":n,"source_d":d,"follower_h":h,"sold_raw":raw,"remaining_raw":f::OWNED-f::SOLD,
        "unknown_before_restart":unknown,"buy_send":1,"sell_send_after_restart":server.sends(),
        "tail_parent_commits":tail,
        "cash_settlement":format!("{cash:?}"),"rpc_methods":methods}))?,
    )?;
    tonic.abort();
    relays.finish().await?;
    Ok(())
}

struct Relays {
    child: std::process::Child,
    directory: PathBuf,
    url: String,
}
impl Drop for Relays {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Relays {
    async fn start(root: &std::path::Path, port: u16) -> Result<Self> {
        let directory = root.join("two-relays");
        let child = std::process::Command::new(
            std::env::var("COPYBOT_LOCAL_PYTHON").unwrap_or_else(|_| "python3".into()),
        )
        .arg("-B")
        .arg(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../tools/tests/relay_loopback_fixture.py"),
        )
        .arg("--directory")
        .arg(&directory)
        .arg("--target-port")
        .arg(port.to_string())
        .arg("--seconds")
        .arg("180")
        .arg("--socket-path")
        .arg(std::env::temp_dir().join(format!(
            "cbr15-{}-{}.sock",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        )))
        .spawn()?;
        let ready = directory.join("ready.json");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("canonical relay startup")?;
        let v: Value = serde_json::from_slice(&std::fs::read(ready)?)?;
        Ok(Self {
            child,
            directory,
            url: format!("http://127.0.0.1:{}", v["port"].as_u64().unwrap()),
        })
    }
    async fn finish(&mut self) -> Result<()> {
        std::fs::write(self.directory.join("DONE"), b"")?;
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.child.try_wait()?.is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
}
