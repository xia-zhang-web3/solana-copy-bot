//! Actual loop regression: transport replay only; real startup, SQLite consumer,
//! pre-select scheduler, periodic recovery, execution timer and shutdown.
use super::{association_fixture as a, b70_fixture::Fixture, b70_hooks};
use anyhow::{ensure, Result};
use copybot_core_types::association_delivery::*;
use copybot_ingestion::{IngestionService, ReplayInput};
use copybot_storage_core::SqliteStore;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::time::Duration;

fn financial(c: &Connection) -> Result<Vec<i64>> {
    [
        "observed_swaps",
        "copy_signals",
        "orders",
        "fills",
        "shadow_lots",
        "execution_source_sell_intents",
        "execution_source_sell_promotions",
        "execution_canary_receipt_proofs",
    ]
    .iter()
    .map(|t| Ok(c.query_row(&format!("SELECT count(*) FROM {t}"), [], |r| r.get(0))?))
    .collect()
}
fn identity(c: &Connection) -> Result<Value> {
    Ok(c.query_row("SELECT admission,candidate,terminal,first_session,first_sequence,conflict,recovery,provenance FROM association_inbox_identities", [], |r| {
        Ok(json!({"admission":r.get::<_,String>(0)?,"candidate":r.get::<_,String>(1)?,
            "terminal":r.get::<_,String>(2)?,"session":r.get::<_,String>(3)?,"sequence":r.get::<_,i64>(4)?,
            "conflict":r.get::<_,bool>(5)?,"recovery":r.get::<_,bool>(6)?,"provenance":r.get::<_,String>(7)?}))
    })?)
}
fn events(c: &Connection) -> Result<Vec<Delivery>> {
    let mut query = c.prepare("SELECT delivery FROM association_inbox_events ORDER BY sequence")?;
    let rows = query.query_map([], |r| r.get::<_, String>(0))?;
    rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
}
async fn until(mut condition: impl FnMut() -> Result<bool>) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(4), async {
        while !condition()? {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
fn pending_tasks(h: &b70_hooks::Hooks) -> Vec<String> {
    h.events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|v| {
            v["stage"]
                .as_str()?
                .strip_prefix("association_pending:")
                .map(str::to_owned)
        })
        .collect()
}
fn position(c: &Connection, id: &str, token: &str) -> Result<()> {
    c.execute("INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,accounting_bucket) VALUES(?1,?2,1,1,'2026-09-09T00:00:00.000000001+00:00','open','execution_canary')",[id,token])?;
    Ok(())
}
async fn check(held: bool) -> Result<()> {
    let _serial = b70_hooks::acquire().await;
    let f = Fixture::new("http://127.0.0.1:1", false).await?;
    let meta = a::metadata("fixture.json");
    let mut config = a::config(&meta);
    config.execution = f.execution.clone();
    config.execution.enabled = false;
    config.execution.canary_tiny_submit_enabled = false;
    config.execution.quote_canary_enabled = false;
    config.execution.canary_enabled = true;
    config.execution.canary_dry_run = true;
    config.execution.canary_interval_seconds = 1;
    let kill = f.f.path.with_extension("r1-association-kill");
    ensure!(!kill.exists());
    std::fs::write(&kill, b"local timer guard; no financial actions")?;
    config.execution.canary_kill_switch_path = kill.to_string_lossy().into();
    let (input, replay) = tokio::sync::mpsc::channel(1);
    let service = IngestionService::with_replay(&config, replay, "r1-loop".into())?;
    let store = SqliteStore::open(&f.f.path)?;
    store.set_busy_timeout(Duration::from_millis(1))?;
    ensure!(
        config
            .ingestion
            .yellowstone_association
            .as_ref()
            .unwrap()
            .sqlite_busy_ms
            == 5000
    );
    let sql = f.f.conn()?;
    position(&sql, "A", meta["token"].as_str().unwrap())?;
    let before = financial(&sql)?;
    let (installed, _unused_legacy_input) = b70_hooks::Installed::new();
    let h = &installed.0;
    let daemon = crate::app_loop::run_app_loop(
        store,
        service,
        f.discovery.clone(),
        f.f.shadow.clone(),
        config.execution.clone(),
        f.risk.clone(),
        config.ingestion.clone(),
        f.shadow.clone(),
        f.f.path.to_string_lossy().into(),
        3600,
        copybot_config::HistoryRetentionConfig::default(),
        f.journal.clone(),
        3600,
        3600,
        30,
        "yellowstone_grpc".into(),
        3600,
        false,
        0,
        false,
        None,
    );
    let controller = async {
        let result: Result<()> = async {
            input.send(a::update("missing", 1)).await?;
            input.send(a::update("block", 2)).await?;
            until(|| {
                let terminal:i64=sql.query_row("SELECT count(*) FROM association_inbox_identities WHERE terminal IS NOT NULL",[],|r|r.get(0))?;
                let alive:i64=sql.query_row("SELECT count(*) FROM system_heartbeat WHERE component='copybot-app' AND status='alive'",[],|r|r.get(0))?;
                Ok(terminal == 1 && alive == 1)
            }).await?;
            let first = identity(&sql)?;
            let admission: AdmissionFacts = serde_json::from_str(first["admission"].as_str().unwrap())?;
            let candidate: CandidateGeneration = serde_json::from_str(first["candidate"].as_str().unwrap())?;
            ensure!(admission.message_time == MessageTime::Missing);
            ensure!(matches!(candidate, CandidateGeneration::AppObserved { position_id, .. } if position_id == "A"));
            ensure!(first["provenance"].as_str().unwrap().ends_with("trade_authority_none"));
            let terminal: Terminal = serde_json::from_str(first["terminal"].as_str().unwrap())?;
            ensure!(matches!(terminal, Terminal::ProviderAsserted(_)));
            let initial = events(&sql)?;
            ensure!(initial.len() == 4 && financial(&sql)? == before);
            ensure!(initial.iter().filter(|d| matches!(d.event,DeliveryEvent::Parent(_))).count()==1);
            h.wait("execution_tick", "", 1).await?;
            let required = h.count("execution_tick", "") + 2;
            let pending_before = pending_tasks(h).len();
            if held { sql.execute_batch("BEGIN IMMEDIATE")?; }
            input.send(a::update("missing", 3)).await?;
            h.wait("execution_tick", "", required).await?;
            if held {
                until(|| Ok(pending_tasks(h).len() >= pending_before + 2)).await?;
                let pending = pending_tasks(h);
                let pending = &pending[pending_before..];
                ensure!(pending.iter().all(|id| id == &pending[0]), "select cancellation must retain the same unfinished writer task");
                ensure!(events(&sql)? == initial, "held write must not receive a durable ACK");
                ensure!(identity(&sql)? == first, "pending duplicate must preserve the exact first identity");
            }
            sql.execute("UPDATE positions SET state='closed' WHERE position_id='A'", [])?;
            position(&sql, "B", meta["token"].as_str().unwrap())?;
            if held { sql.execute_batch("COMMIT")?; }
            until(|| Ok(events(&sql)?.len() == 5)).await?;
            let expected = Delivery {
                session: "r1-loop:0".into(), sequence: 4, arrival_offset_ns: 3,
                event: DeliveryEvent::Duplicate { original: admission.clone(), observed_info: admission.info.clone(),
                    observed_slot: admission.facts.slot, message_time: MessageTime::Missing },
            };
            ensure!(events(&sql)?.last() == Some(&expected), "exact durable duplicate after release");
            // Subsequent admission/terminal can pass the real consumer only after
            // the pending write's commit/readback ACK. Reconnect sees B, retains A.
            input.send(ReplayInput::Reset(4)).await?;
            input.send(a::update("missing", 5)).await?;
            input.send(a::update("block", 6)).await?;
            until(|| Ok(events(&sql)?.iter().any(|d| d.session == "r1-loop:1" && matches!(d.event, DeliveryEvent::Terminal { .. })))).await?;
            ensure!(identity(&sql)? == first, "reconnect must not replace first A with current B");
            let ids: i64 = sql.query_row("SELECT count(*) FROM association_inbox_identities", [], |r| r.get(0))?;
            let cursor: i64 = sql.query_row("SELECT count(*) FROM source_sell_handoff_cursor", [], |r| r.get(0))?;
            ensure!(ids == 1 && cursor == 0, "durable mode must leave legacy recovery cursor untouched at both call sites");
            ensure!(financial(&sql)? == before);
            eprintln!("B89_R1_ACTUAL_LOOP {}",json!({"held":held,"held_pending_tasks":&pending_tasks(h)[pending_before..],
                "ticks":h.count("execution_tick", ""),"exact_duplicate_committed":true,
                "subsequent_terminal_after_ack":true,"first_identity_preserved_after_b":true,
                "legacy_cursor_rows":cursor,"financial_delta":0,"db":f.f.path}));
            Ok(())
        }.await;
        if !sql.is_autocommit() {
            sql.execute_batch("ROLLBACK")?;
        }
        h.stop();
        result
    };
    let joined = tokio::time::timeout(Duration::from_secs(12), async {
        tokio::join!(daemon, controller)
    })
    .await;
    std::fs::remove_file(kill)?;
    let (daemon_result, control_result) = joined?;
    daemon_result?;
    control_result?;
    ensure!(h.count("checked_shutdown", "") == 1);
    ensure!(financial(&sql)? == before);
    Ok(())
}
#[tokio::test]
#[ignore = "validated local B89_FIXTURE_DIR; full loop without external calls"]
async fn b89_r1_actual_loop_no_lock_control() -> Result<()> {
    check(false).await
}
#[tokio::test]
#[ignore = "validated local B89_FIXTURE_DIR; full loop with held SQLite writer"]
async fn b89_r1_actual_loop_pending_write_keeps_ready_timer_and_first_identity() -> Result<()> {
    check(true).await
}
