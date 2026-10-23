//! SOURCE child fixtures: immutable saved bytes or explicit synthetic receipts, loopback only.
use super::source_recovery_fixture::{Case, Server};
use anyhow::{ensure, Context, Result};
use chrono::{Duration, Utc};
use copybot_config::{AppConfig, ExecutionConfig};
use copybot_storage_core::*;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub(super) fn child_config(c: &mut Case) -> Result<()> {
    let original = c.config.owner_exit.as_ref().unwrap().intent_id.clone();
    let p = c.config.owner_exit.as_mut().unwrap();
    p.source_recovery_parent_intent_id = Some(original);
    p.intent_id = format!(
        "source-child:{}",
        c.config.owner_technical_buy.as_ref().unwrap().intent_id
    );
    let start = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    p.activate = true;
    p.activated_at = start.to_rfc3339();
    p.expires_at = (start + Duration::seconds(299)).to_rfc3339();
    c.intent = crate::execution_owner_exit_authority::configured(&c.config)?
        .context("LOCAL child config")?;
    Ok(())
}
pub(super) async fn synthetic() -> Result<Case> {
    let mut c = Case::synthetic().await?;
    c.server.state.lock().unwrap().held = 0;
    c.runner(c.config.clone()).await?;
    ensure!(
        c.store
            .load_execution_canary_order(&owner_exit_order_id(&c.intent.intent_id))?
            .unwrap()
            .status
            == EXECUTION_STATUS_CANARY_FAILED,
        "LOCAL actual before-build failure"
    );
    c.server.state.lock().unwrap().held = 99_999_999;
    child_config(&mut c)?;
    Ok(c)
}
pub(super) fn columns(c: &rusqlite::Connection, table: &str) -> Result<Vec<String>> {
    Ok(c.prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |r| r.get(1))?
        .collect::<rusqlite::Result<_>>()?)
}
pub(super) fn table(c: &rusqlite::Connection, name: &str, cols: &[String]) -> Result<Value> {
    let mut stmt = c.prepare(&format!(
        "SELECT {} FROM {name} ORDER BY rowid",
        cols.join(",")
    ))?;
    let mut rows = stmt.query([])?;
    let mut values = vec![];
    while let Some(row) = rows.next()? {
        let mut object = serde_json::Map::new();
        for (i, col) in cols.iter().enumerate() {
            let value = match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => Value::Null,
                rusqlite::types::ValueRef::Integer(v) => json!(v),
                rusqlite::types::ValueRef::Real(v) => json!(v),
                rusqlite::types::ValueRef::Text(v) => json!(std::str::from_utf8(v)?),
                rusqlite::types::ValueRef::Blob(_) => {
                    anyhow::bail!("LOCAL financial blob unsupported")
                }
            };
            object.insert(col.clone(), value);
        }
        values.push(Value::Object(object));
    }
    Ok(json!(values))
}
pub(super) const PRESERVED: &[&str] = &[
    "owner_technical_buy_intents",
    "owner_exit_intents",
    "orders",
    "positions",
    "fills",
    "execution_canary_dispatch",
    "execution_canary_receipt_proofs",
    "execution_canary_receipt_facts",
    "execution_order_sources",
    "execution_tiny_reservations",
    "owner_exit_fee_reservations",
    "controlled_receipt_lot_scope",
];
pub(super) fn graph(c: &rusqlite::Connection) -> Result<Value> {
    let mut object = serde_json::Map::new();
    for name in PRESERVED {
        let mut cols = columns(c, name)?;
        cols.retain(|s| s != "owner_exit_child_id");
        object.insert((*name).into(), table(c, name, &cols)?);
    }
    Ok(Value::Object(object))
}
pub(super) fn copy_input(source: &Path, destination: &Path) -> Result<Vec<(PathBuf, String)>> {
    let mut pins = vec![];
    for suffix in ["", "-wal", "-shm"] {
        let input = PathBuf::from(format!("{}{suffix}", source.display()));
        if input.exists() {
            let bytes = std::fs::read(&input)?;
            pins.push((input, crate::execution_owned_sell_rpc::digest(&bytes)));
            std::fs::write(
                PathBuf::from(format!("{}{suffix}", destination.display())),
                bytes,
            )?;
        }
    }
    Ok(pins)
}
pub(super) fn unchanged(pins: &[(PathBuf, String)]) -> Result<()> {
    for (p, pin) in pins {
        ensure!(
            crate::execution_owned_sell_rpc::digest(std::fs::read(p)?) == *pin,
            "original input changed"
        );
    }
    Ok(())
}
pub(super) async fn saved() -> Result<(Case, Value, Vec<(PathBuf, String)>)> {
    let root = super::temporary_output_fixture::OutputRoot::new("SOURCE-child-saved-failure")?;
    let path = root.path().join("state.db");
    let pins = copy_input(Path::new(&std::env::var("SOURCE_CHILD_SAVED_DB")?), &path)?;
    let before = graph(&rusqlite::Connection::open(&path)?)?;
    let mut store = SqliteStore::open(&path)?;
    let n = store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    ensure!(n == 1, "LOCAL exactly additive0092 upgrade");
    ensure!(
        before == graph(&rusqlite::Connection::open(&path)?)?,
        "migration changed financial graph"
    );
    let raw = std::fs::read_to_string(std::env::var("SOURCE_CHILD_SAVED_CONFIG")?)?;
    let mut config = toml::from_str::<AppConfig>(&raw)?.execution;
    config.canary_tiny_submit_enabled = true;
    config.max_confirm_seconds = 1;
    let p = config.owner_exit.as_ref().context("LOCAL parent config")?;
    let parent = store
        .load_owner_exit_intent(&p.intent_id)?
        .context("LOCAL preserved parent")?;
    let server = Server::start(&parent.wallet, 99_999_999).await?;
    {
        let mut state = server.state.lock().unwrap();
        state.genesis = parent.genesis_hash.clone();
        state.slot = store
            .load_execution_canary_receipt_facts(&parent.buy_order_id)?
            .unwrap()
            .slot
            + 1;
    }
    config.submit_adapter_http_url = format!("{}/rpc", server.url);
    config.quote_canary_base_url = format!("{}/swap/v1", server.url);
    config.canary_kill_switch_path = root.path().join("STOP").to_string_lossy().into_owned();
    server.state.lock().unwrap().quote_snapshot = Some(path.clone());
    let mut c = Case {
        root,
        path,
        store,
        config,
        server,
        intent: parent,
    };
    child_config(&mut c)?;
    normal_loader(&raw, &c.config, c.root.path())?;
    Ok((c, before, pins))
}
fn normal_loader(raw: &str, c: &ExecutionConfig, root: &Path) -> Result<()> {
    let mut whole = toml::from_str::<toml::Value>(raw)?;
    let e = whole["execution"].as_table_mut().unwrap();
    e.insert("canary_tiny_submit_enabled".into(), true.into());
    for (key, value) in [
        ("submit_adapter_http_url", c.submit_adapter_http_url.clone()),
        ("quote_canary_base_url", c.quote_canary_base_url.clone()),
        ("canary_kill_switch_path", c.canary_kill_switch_path.clone()),
    ] {
        e.insert(key.into(), toml::Value::String(value));
    }
    let p = c.owner_exit.as_ref().unwrap();
    let t = e["owner_exit"].as_table_mut().unwrap();
    for (key, value) in [
        ("intent_id", p.intent_id.clone()),
        (
            "source_recovery_parent_intent_id",
            p.source_recovery_parent_intent_id.clone().unwrap(),
        ),
        ("activated_at", p.activated_at.clone()),
        ("expires_at", p.expires_at.clone()),
    ] {
        t.insert(key.into(), toml::Value::String(value));
    }
    t.insert("activate".into(), true.into());
    let file = root.join("LOCAL.child.startup.toml");
    std::fs::write(&file, toml::to_string(&whole)?)?;
    let loaded = copybot_config::load_from_path(&file)?;
    ensure!(
        crate::execution_owner_exit_authority::configured(&loaded.execution)?
            == crate::execution_owner_exit_authority::configured(c)?,
        "LOCAL full-loader child pairing"
    );
    Ok(())
}
