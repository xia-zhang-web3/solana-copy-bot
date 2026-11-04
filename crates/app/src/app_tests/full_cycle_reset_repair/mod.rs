//! Exact saved Reset ordering through ingress bridge and actual native consumer.
pub(super) mod fixture_readiness;
mod frames;
mod source_entry;
use crate::{
    association_consumer::AssociationConsumer,
    execution_owned_sell_rpc::fractional::transport::Parsed,
};
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use copybot_core_types::association_delivery::AdmissionFacts;
use copybot_ingestion::{IngestionService, ReplayInput};
use copybot_storage_core::SqliteStore;
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::PathBuf};

fn inputs() -> Result<PathBuf> {
    Ok(std::env::var_os("RESET_REPAIR_INPUTS")
        .context("explicit saved Reset inputs")?
        .into())
}
fn count(sql: &rusqlite::Connection, table: &str) -> Result<i64> {
    Ok(sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
}

#[tokio::test]
#[ignore = "explicit immutable private real ordering fixture; no provider or signer"]
async fn saved_ready_reset_overtaken_buy_has_association_without_native_decision() -> Result<()> {
    let root = inputs()?;
    let a: AdmissionFacts = serde_json::from_slice(&std::fs::read(root.join("ADMISSION.json"))?)?;
    let events: Vec<Value> = serde_json::from_slice(&std::fs::read(root.join("EVENTS.json"))?)?;
    let mut c: copybot_config::AppConfig =
        toml::from_str(&std::fs::read_to_string(root.join("LOCAL_BASE.toml"))?)?;
    let source = a.facts.wallet.clone();
    let bot = "BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk";
    let activated = Utc::now() - Duration::seconds(5);
    let deadline = activated + Duration::seconds(600);
    let e = &mut c.execution;
    e.canary_wallet_pubkey = bot.into();
    e.execution_signer_pubkey = bot.into();
    e.execution_signer_keypair_path = "LOCAL_NEVER_READ_KEY".into();
    e.prior_bot_receipt_exit = None;
    let cohort = e.technical_cohort.as_mut().context("normal cohort")?;
    cohort.activate = true;
    cohort.wallet_ids = vec![source.clone()];
    cohort.activated_at = activated.to_rfc3339();
    cohort.deadline = deadline.to_rfc3339();
    cohort.max_wait_seconds = 600;
    let session = e.real_session.as_mut().context("normal session")?;
    session.source_wallet = source.clone();
    session.bot_wallet = bot.into();
    session.activated_at = activated.to_rfc3339();
    session.deadline = deadline.to_rfc3339();
    let scope = e
        .controlled_receipt_lot
        .as_mut()
        .context("normal custody scope")?;
    scope.source_wallet = source;
    scope.bot_wallet = bot.into();
    let path = root.join(format!("consumer-red-{}.sqlite", std::process::id()));
    anyhow::ensure!(!path.exists(), "fresh LOCAL store");
    c.sqlite.path = path.to_string_lossy().into_owned();
    c.execution
        .controlled_receipt_lot
        .as_mut()
        .unwrap()
        .source_state_db_path = path.to_string_lossy().into_owned();
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
    let sql = rusqlite::Connection::open(&path)?;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let mut ingestion = IngestionService::with_replay_scoped(
        &c,
        rx,
        "2ae11687-aa05-420e-9006-fc2134d3654b".into(),
        Some(
            [bot.to_string(), a.facts.wallet.clone()]
                .into_iter()
                .collect(),
        ),
    )?;
    let mut consumer = AssociationConsumer::start_with_execution(
        &mut ingestion,
        &c.ingestion,
        &c.execution,
        &path.to_string_lossy(),
    )
    .await?
    .context("actual consumer")?;
    let slots = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let used = slots.clone();
    let mut rpc = Parsed(move |request: Value| {
        let used = used.clone();
        async move {
            let result = match request["method"].as_str() {
                Some("getGenesisHash") => json!("5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"),
                Some("getSlot") => json!(if used.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    == 0
                {
                    465326211
                } else {
                    465326252
                }),
                _ => anyhow::bail!("unexpected external RPC operand"),
            };
            Ok(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
        }
    });
    let saved = a.clone();
    let producer = tokio::spawn(async move {
        let a = saved;
        let mut blocks = BTreeMap::new();
        for row in &events {
            let d: Value = serde_json::from_str(row["delivery"].as_str().context("delivery")?)?;
            if let Some(v) = d["event"].get("ParentCheckpoint") {
                blocks.insert(
                    v["observation"]["child"]["slot"].as_u64().unwrap(),
                    v.clone(),
                );
            }
        }
        for row in &events {
            let d: Value = serde_json::from_str(row["delivery"].as_str().unwrap())?;
            let sequence = d["sequence"].as_u64().unwrap();
            let offset = d["arrival_offset_ns"].as_u64().unwrap();
            let event = &d["event"];
            if sequence == 0
                || event
                    .get("Session")
                    .is_some_and(|v| v == "StartedContinuityUnknown")
            {
                continue;
            }
            let payload = if event.get("Session").is_some_and(|v| v == "Reset") {
                tx.send(ReplayInput::Reset(offset)).await?;
                continue;
            } else if event.get("Admission").is_some() {
                frames::admission(&a)?
            } else if let Some(v) = event.get("ParentCheckpoint") {
                frames::block(v, &a)?
            } else if let Some(v) = event.get("Parent") {
                frames::block(
                    blocks
                        .get(&v["child"]["slot"].as_u64().unwrap())
                        .context("prior saved header")?,
                    &a,
                )?
            } else {
                continue;
            };
            tx.send(ReplayInput::Update {
                offset_ns: offset,
                payload,
            })
            .await?;
        }
        let end = events
            .last()
            .and_then(|v| serde_json::from_str::<Value>(v["delivery"].as_str()?).ok())
            .and_then(|v| v["arrival_offset_ns"].as_u64())
            .unwrap()
            + 1;
        tx.send(ReplayInput::End(end)).await?;
        Ok::<_, anyhow::Error>(())
    });
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            match consumer.poll_with_transport(&store, Some(&mut rpc)).await {
                Ok(()) => {}
                Err(e) if e.to_string() == "association delivery stopped" => break,
                Err(e) => return Err(e),
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    producer.await??;
    let row: (String,i64,String,Option<String>) = sql.query_row("SELECT first_session,first_sequence,admission,terminal FROM association_inbox_identities WHERE signature=?1",[&a.facts.signature],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    assert!(row.0.ends_with(":1"));
    assert_eq!(row.1, 40);
    assert_eq!(serde_json::from_str::<AdmissionFacts>(&row.2)?, a);
    assert!(row
        .3
        .as_deref()
        .is_some_and(|v| v.contains("ProviderAsserted")));
    assert_eq!(count(&sql, "native_buy_decisions")?, 0);
    assert_eq!(count(&sql, "orders")?, 0);
    let proof = json!({"status":"RED_REPRODUCED_MISSED_COPY_WITH_REAL_ORDERING","first_session":row.0,"first_sequence":row.1,"source_slot":465326247,"initial_fence":465326211,"new_session_first_fence":465326252,"native_decisions":0,"orders":0,"actual_native_consumer":true,"historical_clock_preserved":true,"unrelated_block_transactions":"MODELED_LOCAL_VOTES","real_actions":0});
    std::fs::write(
        root.join("CONSUMER_RED.json"),
        serde_json::to_vec_pretty(&proof)?,
    )?;
    Ok(())
}
