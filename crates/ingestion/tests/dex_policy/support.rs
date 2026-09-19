//! Offline oracle for an exported, stopped live capture DB; never opens a stream.
use anyhow::Result;
use copybot_config::IngestionConfig;
use rusqlite::{params, Connection};
use serde_json::Value;
use std::path::Path;

#[derive(Debug, PartialEq, Clone)]
pub struct Receipt {
    pub signature: Option<String>,
    pub wallet: String,
    pub slot: String,
    pub source_at: Option<f64>,
    pub raw: Vec<u8>,
    pub fingerprint: String,
    pub stage: String,
    pub reason: Option<String>,
    pub event: Option<Value>,
}
pub fn receipt(db: &Connection, seq: i64) -> Result<Receipt> {
    let (signature, wallet, slot, source_at, raw, fingerprint, stage, reason, event): (
        Option<String>,
        String,
        String,
        Option<f64>,
        Vec<u8>,
        String,
        String,
        Option<String>,
        Option<String>,
    ) = db.query_row(
        "SELECT signature,wallet,slot,source_at,raw,fingerprint,stage,reason,event_json
         FROM capture_events WHERE seq=?",
        [seq],
        |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
            ))
        },
    )?;
    Ok(Receipt {
        signature,
        wallet,
        slot,
        source_at,
        raw,
        fingerprint,
        stage,
        reason,
        event: event.map(|v| serde_json::from_str(&v)).transpose()?,
    })
}
pub fn create(path: &Path) -> Result<Connection> {
    let db = Connection::open(path)?;
    db.execute_batch(copybot_storage_core::capture_scope::SCHEMA)?;
    db.execute(
        "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,100000,1073741824)",
        [],
    )?;
    Ok(db)
}
pub fn request(db: &Connection, wallet: &str) -> Result<()> {
    db.execute(
        "INSERT INTO capture_requests(request_key,payload,expires)
                VALUES(hex(randomblob(8)),'{}',1e100)",
        [],
    )?;
    db.execute(
        "INSERT INTO capture_members VALUES(?,?)",
        params![db.last_insert_rowid(), wallet],
    )?;
    Ok(())
}
pub fn config(path: &Path, original: &IngestionConfig) -> IngestionConfig {
    let mut c = original.clone();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_delivery_mode = "legacy".into();
    c.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    c.yellowstone_x_token = "offline-replay".into();
    c.capture_scope_db = Some(path.to_str().unwrap().into());
    c
}
