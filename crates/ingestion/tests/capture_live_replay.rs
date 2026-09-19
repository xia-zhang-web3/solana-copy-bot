//! Offline oracle for an exported, stopped live capture DB; never opens a stream.
use anyhow::{ensure, Context, Result};
use copybot_config::IngestionConfig;
use copybot_ingestion::capture_replay::CaptureReplay;
use prost::Message;
use rusqlite::{params, Connection, OpenFlags};
use serde_json::Value;
use std::path::Path;
use yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeUpdate};

#[derive(Debug, PartialEq)]
struct Receipt {
    signature: Option<String>,
    wallet: String,
    slot: String,
    source_at: Option<f64>,
    raw: Vec<u8>,
    fingerprint: String,
    stage: String,
    reason: Option<String>,
    event: Option<Value>,
}
fn receipt(db: &Connection, seq: i64) -> Result<Receipt> {
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
fn create(path: &Path) -> Result<Connection> {
    let db = Connection::open(path)?;
    db.execute_batch(copybot_storage_core::capture_scope::SCHEMA)?;
    db.execute(
        "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,100000,1073741824)",
        [],
    )?;
    Ok(db)
}
fn request(db: &Connection, wallet: &str) -> Result<()> {
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
fn config(path: &Path, original: &IngestionConfig) -> IngestionConfig {
    let mut c = original.clone();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_delivery_mode = "legacy".into();
    c.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    c.yellowstone_x_token = "offline-replay".into();
    c.capture_scope_db = Some(path.to_str().unwrap().into());
    c
}
async fn verify(path: &Path, original: &IngestionConfig) -> Result<Value> {
    let input = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    input.execute_batch("PRAGMA query_only=ON")?;
    ensure!(
        input.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))? == "ok",
        "capture input integrity failure"
    );
    let temp = tempfile::tempdir()?;
    let target = temp.path().join("replay.db");
    let replay_db = create(&target)?;
    let replay = CaptureReplay::open(&config(&target, original)).await?;
    let sequences = input
        .prepare("SELECT seq FROM capture_events ORDER BY seq")?
        .query_map([], |r| r.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut durable = 0;
    let mut rejected = 0;
    let mut current_wallet = None;
    for (index, seq) in sequences.iter().enumerate() {
        let expected = receipt(&input, *seq)?;
        ensure!(
            matches!(expected.stage.as_str(), "DURABLE" | "REJECTED"),
            "unfinished or invalid receipt at sequence {seq}"
        );
        if current_wallet.as_deref() != Some(expected.wallet.as_str()) {
            request(&replay_db, &expected.wallet)?;
            replay.accept_pending().await?;
            current_wallet = Some(expected.wallet.clone());
        }
        // Decoder errors are valid terminal REJECTED outcomes; inspect committed rows.
        let push_result = replay.push(&expected.raw).await;
        let actual = receipt(&replay_db, index as i64 + 1)
            .with_context(|| format!("missing replay receipt {seq}; push={push_result:?}"))?;
        ensure!(
            actual == expected,
            "decoder/canonical mismatch at sequence {seq}: expected={:?}; actual={:?}",
            (
                expected.signature.as_ref(),
                &expected.stage,
                &expected.reason
            ),
            (actual.signature.as_ref(), &actual.stage, &actual.reason)
        );
        if expected.stage == "DURABLE" {
            ensure!(
                push_result?.is_some(),
                "DURABLE without supported swap at {seq}"
            );
            durable += 1;
        } else {
            ensure!(
                push_result.as_ref().map_or(true, |event| event.is_none()),
                "supported swap incorrectly rejected at {seq}"
            );
            rejected += 1;
        }
    }
    ensure!(
        replay_db.query_row("SELECT count(*) FROM capture_events", [], |r| r
            .get::<_, usize>(0))?
            == sequences.len(),
        "replay sequence/cardinality mismatch"
    );
    Ok(
        serde_json::json!({"receipts": sequences.len(), "durable": durable, "rejected": rejected,
                        "original_read_only": true, "network": false}),
    )
}
async fn fixture(path: &Path) -> Result<()> {
    let db = create(path)?;
    let replay = CaptureReplay::open(&config(path, &IngestionConfig::default())).await?;
    let paths = ["saved_buy.pb", "saved_sell.pb"];
    for name in paths {
        let raw = std::fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../app/tests/fixtures/capture")
                .join(name),
        )?;
        let update = SubscribeUpdate::decode(raw.as_slice())?;
        let Some(UpdateOneof::Transaction(tx)) = &update.update_oneof else {
            anyhow::bail!("fixture type")
        };
        let key = &tx
            .transaction
            .as_ref()
            .unwrap()
            .transaction
            .as_ref()
            .unwrap()
            .message
            .as_ref()
            .unwrap()
            .account_keys[0];
        request(&db, &bs58::encode(key).into_string())?;
        replay.accept_pending().await?;
        ensure!(replay.push(&raw).await?.is_some(), "fixture must decode");
        if name == "saved_buy.pb" {
            let mut rejected = update;
            rejected.created_at = None;
            if let Some(UpdateOneof::Transaction(tx)) = &mut rejected.update_oneof {
                // Distinct receipt signature, same supported transaction, missing time.
                tx.transaction.as_mut().unwrap().signature[0] ^= 1;
                tx.transaction
                    .as_mut()
                    .unwrap()
                    .transaction
                    .as_mut()
                    .unwrap()
                    .signatures[0][0] ^= 1;
            }
            ensure!(
                replay.push(&rejected.encode_to_vec()).await?.is_none(),
                "missing time rejects"
            );
        }
    }
    Ok(())
}
#[tokio::test]
async fn capture_live_export_replays_exact_terminal_dispositions() -> Result<()> {
    let result = if let Some(path) = std::env::var_os("CAPTURE_REPLAY_DB") {
        let config_path = std::env::var_os("CAPTURE_REPLAY_CONFIG")
            .context("CAPTURE_REPLAY_CONFIG must bind actual ingestion decoder configuration")?;
        let original: IngestionConfig = serde_json::from_slice(&std::fs::read(config_path)?)?;
        verify(Path::new(&path), &original).await?
    } else {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("fixture.db");
        fixture(&path).await?;
        let result = verify(&path, &IngestionConfig::default()).await?;
        ensure!(
            result["durable"] == 2 && result["rejected"] == 1,
            "fixture oracle counts"
        );
        db_corruption_is_detected(&path).await?;
        result
    };
    println!("CAPTURE_REPLAY_RESULT={result}");
    Ok(())
}

async fn db_corruption_is_detected(path: &Path) -> Result<()> {
    let db = Connection::open(path)?;
    db.execute(
        "UPDATE capture_events SET reason='incorrect_disposition' WHERE seq=1",
        [],
    )?;
    ensure!(
        verify(path, &IngestionConfig::default()).await.is_err(),
        "oracle accepted a changed persisted disposition"
    );
    Ok(())
}
