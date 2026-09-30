//! Actual daemon consumer and blocking Inbox writer; no manual recovery ACK.
use super::{index_recovery_fixture as f, index_recovery_transport as transport};
use crate::association_consumer::AssociationConsumer;
use anyhow::{ensure, Context, Result};
use copybot_ingestion::IngestionService;
use copybot_storage_core::SqliteStore;
use serde_json::{json, Value};
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    time::Duration,
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

fn evidence(label: &str) -> Result<PathBuf> {
    let root = PathBuf::from(std::env::var("COPYBOT_INDEX_EVIDENCE_DIR")?);
    ensure!(root.is_dir(), "index_evidence_root_required");
    let path = root.join(label);
    std::fs::create_dir(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
    Ok(path)
}
fn private(path: &Path) -> Result<()> {
    std::fs::create_dir(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}
fn cursor(path: &Path) -> Result<Value> {
    let sql =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let value: String = sql.query_row(
        "SELECT head FROM association_replay_cursor WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    Ok(serde_json::from_str(&value)?)
}
async fn consume(app: &copybot_config::AppConfig, path: &Path, target: u64) -> Result<Value> {
    let store = SqliteStore::open(path)?;
    let mut service = IngestionService::build_for_app(app)?;
    let mut consumer =
        AssociationConsumer::start(&mut service, &app.ingestion, &path.to_string_lossy())
            .await?
            .context("actual_association_consumer")?;
    let hold = consumer.http_continuity_hold().unwrap();
    ensure!(hold.load(Ordering::Acquire), "restart_initial_hold");
    tokio::time::timeout(Duration::from_secs(12),async{
        loop{
            // Cancellation resumes the same app consumer/write task. Do not
            // wait for a nonexistent delivery after the final durable ACK.
            if let Ok(result)=tokio::time::timeout(Duration::from_millis(100),consumer.poll(&store)).await {result?;}
            let (wire,_)=consumer.diagnostic_snapshots();
            if wire.last_durably_stored_parent_slot>=target && !hold.load(Ordering::Acquire){
                break Ok::<_,anyhow::Error>(json!({"durable_slot":wire.last_durably_stored_parent_slot,
                    "caught_up_to_anchor":wire.processing.http_recovery.caught_up_to_anchor,
                    "live_anchor_slot":wire.processing.http_recovery.live_anchor_slot,"hold":false}));
            }
        }
    }).await.context("actual_app_ack_timeout")?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "explicit immutable saved09 inputs; bounded loopback actual daemon path"]
async fn index_recovery_actual_app_saved09_anchor_later_ack_and_restart() -> Result<()> {
    let saved = f::load()?;
    let root = evidence("native-positive")?;
    let pairs = root.join("anchor-pairs");
    private(&pairs)?;
    let server = transport::start(&saved, saved.grpc.clone(), true).await?;
    let app = f::config(&server.grpc, &server.http, &pairs);
    copybot_config::validate_association_delivery(&app)?;
    let db = root.join("runtime.sqlite");
    f::initialize(&db, &app, &saved.grpc)?;
    let first = consume(&app, &db, f::SLOT + 1).await?;
    let stopped = cursor(&db)?;
    ensure!(
        stopped["block"]["observation"]["child"]["slot"] == f::SLOT + 1,
        "first_durable_reopen"
    );
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(pairs.join("pair-01/manifest.json"))?)?;
    ensure!(
        manifest["comparison"] == "MATCH" && manifest["first_mismatch"].is_null(),
        "saved09_indexed_pair"
    );
    ensure!(
        manifest["comparison_scope"] == "VERIFIED_EXECUTION_INDEX_RUNTIME_BLOCK_EQUIVALENT",
        "scope"
    );
    let calls = server.calls.lock().unwrap().clone();
    drop(server);
    let restart = transport::restart(&saved).await?;
    let app = f::config(&restart.grpc, &restart.http, &pairs);
    let after = consume(&app, &db, f::SLOT + 3).await?;
    let final_cursor = cursor(&db)?;
    ensure!(
        final_cursor["block"]["observation"]["child"]["slot"] == f::SLOT + 3,
        "restart_durable_reopen"
    );
    let sql =
        rusqlite::Connection::open_with_flags(&db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut statement=sql.prepare("SELECT first_observation FROM association_parent_blocks ORDER BY json_extract(first_observation,'$.child.slot')")?;
    let texts = statement
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let rows = texts
        .iter()
        .map(|s| serde_json::from_str::<Value>(s))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    for pair in rows.windows(2) {
        ensure!(pair[0]["child"] == pair[1]["parent"], "durable_parent_link");
    }
    ensure!(
        rows.iter()
            .any(|p| p["child"]["slot"] == f::SLOT && p["issue"].is_null()),
        "anchor_committed"
    );
    std::fs::write(
        root.join("RESULT.json"),
        serde_json::to_vec_pretty(&json!({"passed":true,
        "route":"IngestionService -> AssociationConsumer::poll -> spawn_blocking Inbox.persist_at -> replay_checkpoint -> receiver ACK",
        "saved09_bindings":saved.bindings,"first":first,"restart":after,"first_cursor":stopped,"final_cursor":final_cursor,
        "parents":rows,"calls":calls,"restart_calls":restart.calls.lock().unwrap().clone(),
        "modeled_context":"seed predecessor098 and empty linked100/101/102; saved099 block fields unchanged",
        "provider_calls":0,"signatures":0,"submissions":0}))?,
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "explicit immutable saved09 inputs; critical corruptions through actual daemon consumer"]
async fn index_recovery_actual_app_saved09_corruptions_cannot_ack() -> Result<()> {
    let saved = f::load()?;
    let root = evidence("native-refusals")?;
    let cases: Vec<(&str, fn(&mut SubscribeUpdateBlock))> = vec![
        ("duplicate_index", |b| {
            b.transactions[1].index = b.transactions[0].index
        }),
        ("signature_binding", |b| b.transactions[0].signature[0] ^= 1),
        ("meta_fee", |b| {
            b.transactions[0].meta.as_mut().unwrap().fee += 1
        }),
    ];
    let mut results = vec![];
    for (label, edit) in cases {
        let dir = root.join(label);
        private(&dir)?;
        let pairs = dir.join("anchor-pairs");
        private(&pairs)?;
        let mut corrupted = saved.grpc.clone();
        edit(&mut corrupted);
        let server = transport::start(&saved, corrupted, false).await?;
        let app = f::config(&server.grpc, &server.http, &pairs);
        let db = dir.join("runtime.sqlite");
        f::initialize(&db, &app, &saved.grpc)?;
        let store = SqliteStore::open(&db)?;
        let mut service = IngestionService::build_for_app(&app)?;
        let mut consumer =
            AssociationConsumer::start(&mut service, &app.ingestion, &db.to_string_lossy())
                .await?
                .unwrap();
        let hold = consumer.http_continuity_hold().unwrap();
        let error = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Err(error) = consumer.poll(&store).await {
                    break error;
                }
            }
        })
        .await
        .context("corrupt_anchor_refusal_timeout")?;
        ensure!(
            error
                .to_string()
                .contains("confirmed_http_recovery_refused"),
            "expected_refusal:{error:#}"
        );
        ensure!(hold.load(Ordering::Acquire), "corrupt_anchor_hold_released");
        drop(consumer);
        let head = cursor(&db)?;
        ensure!(
            head["block"]["observation"]["child"]["slot"] == f::SLOT - 1,
            "corrupt_anchor_acked"
        );
        let m: Value =
            serde_json::from_slice(&std::fs::read(pairs.join("pair-01/manifest.json"))?)?;
        ensure!(
            m["comparison"] == "MISMATCH"
                && m["complete"] == true
                && m["first_mismatch"]["path"].is_string(),
            "corruption_diagnostic"
        );
        results.push(
            json!({"case":label,"head":head,"first_mismatch":m["first_mismatch"],"hold":true}),
        );
    }
    std::fs::write(
        root.join("RESULT.json"),
        serde_json::to_vec_pretty(&json!({"passed":true,
        "saved09_bindings":saved.bindings,"cases":results,"provider_calls":0,"submissions":0}))?,
    )?;
    Ok(())
}
