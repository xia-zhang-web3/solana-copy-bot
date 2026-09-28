//! Actual daemon tick with saved source, modeled follower and loopback execution.
use super::{
    run15_full_path_fixture as f, run15_full_path_frames as frames, run15_full_path_server::Server,
    run15_rpc_proof_fixture as proof,
};
use crate::{association_consumer::AssociationConsumer, execution_canary::ExecutionCanaryRunner};
use anyhow::{Context, Result};
use copybot_ingestion::{IngestionService, ReplayInput};
use copybot_storage_core::SqliteStore;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::atomic::Ordering, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit saved corpus/config and offline model boundaries"]
async fn saved_raydium_daemon_partial_sell_unknown_restart_no_second_send() -> Result<()> {
    for unknown in [false, true] {
        scenario(unknown).await?;
    }
    Ok(())
}
async fn scenario(unknown: bool) -> Result<()> {
    let corpus = PathBuf::from(std::env::var("COPYBOT_RUN15_CORPUS_DIR")?);
    let input = PathBuf::from(std::env::var("COPYBOT_RUN15_FRAMES_DIR")?);
    let config_path = PathBuf::from(std::env::var("COPYBOT_RUN15_RUNTIME_CONFIG")?);
    let evidence_root = PathBuf::from(std::env::var("COPYBOT_RUN15_EVIDENCE_DIR")?);
    let root = evidence_root.join(format!(
        "model-case-{unknown}-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    std::fs::create_dir(&root)?;
    let (payload, model) =
        proof::model_follower_with_output_and_target(f::OWNED, 985_000_000, true)?;
    let e = f::evidence(&corpus, &model)?;
    let rpc_buy = f::parsed(&model, &payload)?;
    let server = Server::new(e, rpc_buy, unknown, root.join("model.db")).await?;
    let app = f::config(&config_path, &root, &server.url, &model)?;
    let io = f::io(&app.execution, &corpus, payload, &model)?;
    let path = root.join("model.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
    copybot_storage_core::ensure_discovery_v2_schema(&store)?;
    let sql = Connection::open(&path)?;
    let runner = ExecutionCanaryRunner::new(app.execution.clone())
        .for_ingestion(&app.ingestion, &path.to_string_lossy())?
        .with_native_buy_mock(io.clone());
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let authority = crate::execution_technical_cohort::authority(&app.execution)?.unwrap();
    let scope = crate::execution_technical_cohort::admission_wallets(&authority, &app.execution)?;
    let mut service = IngestionService::with_replay_scoped(
        &app,
        receiver,
        "run15-real-source-model-follower".into(),
        Some(scope),
    )?;
    let mut consumer = AssociationConsumer::start_with_execution(
        &mut service,
        &app.ingestion,
        &app.execution,
        &path.to_string_lossy(),
    )
    .await?
    .context("actual cohort consumer")?;
    let (source, source_header) = frames::source_buy(&input)?;
    let (before, mixed, after, parent_hash) = frames::mixed(&input)?;
    let (follower, follower_header) = frames::follower(&model, &parent_hash)?;
    let sell = std::fs::read(input.join("source-sell725.pb"))?;
    let (bought_tx, bought_rx) = tokio::sync::oneshot::channel();
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
    let producer = tokio::spawn(async move {
        let mut sequence = 0;
        let mut send = |payload: Vec<u8>| {
            sequence += if frames::is_block(&payload) {
                400_000_000
            } else {
                1
            };
            ReplayInput::Update {
                offset_ns: sequence,
                payload,
            }
        };
        sender.send(send(source)).await?;
        sender.send(send(source_header)).await?;
        bought_rx.await?;
        // This gap-spanning parent chain is a model, not captured continuity.
        // Saved BUY and SELL slots/signatures/raw facts remain unchanged.
        for slot in frames::SOURCE_SLOT + 1..frames::BOT_SLOT {
            sender
                .send(send(frames::block(
                    slot,
                    frames::hash(slot),
                    frames::hash(slot - 1),
                    vec![],
                    0,
                )))
                .await?;
        }
        sender.send(send(follower)).await?;
        sender.send(send(follower_header)).await?;
        for v1 in before {
            sender.send(send(v1)).await?;
        }
        sender.send(send(sell)).await?;
        sender.send(send(mixed)).await?;
        for v1 in after {
            sender.send(send(v1)).await?;
        }
        // Parent commits continue across quote/build/simulation/send/receipt.
        let mut previous: String =
            serde_json::from_slice::<Value>(&std::fs::read(corpus.join("response-725.json"))?)?
                ["result"]["blockhash"]
                .as_str()
                .unwrap()
                .into();
        let mut tail = 0;
        loop {
            tokio::select! {
                _=&mut done_rx=>break,
                _=tokio::time::sleep(Duration::from_millis(20))=>{
                    tail+=1;let slot=frames::SELL_SLOT+tail;
                    let hash=frames::hash(slot);
                    sender.send(send(frames::block(slot,hash.clone(),previous,vec![],0))).await?;
                    previous=hash;
                }
            }
        }
        sender.send(ReplayInput::End(sequence + 1)).await?;
        Ok::<_, anyhow::Error>(tail)
    });
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
            match consumer.poll_with_transport(&store, Some(&mut fence)).await {
                Ok(()) => {}
                Err(e) if e.to_string() == "association delivery stopped" => break,
                Err(e) => return Err(e),
            }
        }
        Ok::<_, anyhow::Error>(consumer.diagnostic_snapshots())
    };
    let execute = async {
        let mut bought = Some(bought_tx);
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
                    if let Some(signal) = bought.take() {
                        signal
                            .send(())
                            .map_err(|_| anyhow::anyhow!("model follower producer ended"))?;
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
                if server.sends() == 1
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
        let _ = done_tx.send(());
        Ok::<_, anyhow::Error>(id)
    };
    let ((wire, _interval), id) = tokio::time::timeout(Duration::from_secs(120), async {
        tokio::try_join!(consume, settle)
    })
    .await
    .context("full daemon model timeout")??;
    let tail = producer.await??;
    assert!(tail > 0, "no stream commits overlapped SELL");
    assert_eq!(
        (wire.admissions, wire.selected_source, wire.selected_bot),
        (3, 2, 1),
        "{wire:?}"
    );
    // Public telemetry preserves DecodeMiss ordering; config is the final entry.
    assert_eq!(wire.decode_misses.last(), Some(&143));
    assert_eq!(wire.decode_misses.iter().sum::<u64>(), 143);
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
    drop(runner);
    drop(store);
    let reopened = SqliteStore::open(&path)?;
    let recovered = ExecutionCanaryRunner::new(app.execution.clone())
        .for_ingestion(&app.ingestion, &path.to_string_lossy())?
        .with_native_buy_mock(io.clone());
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
        output.join(format!("full-path-unknown-{unknown}.json")),
        serde_json::to_vec_pretty(&json!({
        "result":"OFFLINE_MODEL_PASS","source":"actual response727 BUY / response725 SELL; unchanged slots/signatures/raw",
        "follower_receipt":format!("explicit synthetic Jupiter Raydium BUY 10m lamports→{} raw",f::OWNED),
        "parent_pages":"modeled complete parent program pages; not historical HTTP facts",
        "continuity":"modeled 10368 parent headers; actual mixed SELL block retained",
        "sell_boundary":"model non-DEX bundle, simulation and receipt; no execution claim",
        "source_n":n,"source_d":d,"follower_h":h,"sold_raw":raw,"remaining_raw":f::OWNED-f::SOLD,
        "unknown_before_restart":unknown,"buy_send":1,"sell_send_after_restart":server.sends(),
        "v1_admissions":0,"config_misses":143,"tail_parent_commits":tail,
        "cash_settlement":format!("{cash:?}"),"rpc_methods":methods}))?,
    )?;
    Ok(())
}
