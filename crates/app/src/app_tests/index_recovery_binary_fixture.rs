//! Thin pinned local producers for exact installed Linux daemon replay.
//! This entry prepares input and transport only; it cannot claim an ACK result.
use super::{index_recovery_fixture as f, index_recovery_transport as transport};
use anyhow::{ensure, Result};
use serde_json::json;
use std::{os::unix::fs::PermissionsExt, path::PathBuf, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "explicit external old/new Linux daemon runner, local ports only"]
async fn index_recovery_binary_fixture_loopback_only() -> Result<()> {
    let saved = f::load()?;
    let root = PathBuf::from(std::env::var("COPYBOT_INDEX_BINARY_FIXTURE_DIR")?);
    ensure!(!root.exists(), "fresh_binary_fixture_directory_required");
    std::fs::create_dir(&root)?;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    let mode = std::env::var("COPYBOT_INDEX_BINARY_FIXTURE_MODE")?;
    let mut grpc = saved.grpc.clone();
    match mode.as_str() {
        "valid" => (),
        "duplicate_index" => grpc.transactions[1].index = grpc.transactions[0].index,
        "signature_binding" => grpc.transactions[0].signature[0] ^= 1,
        "meta_fee" => grpc.transactions[0].meta.as_mut().unwrap().fee += 1,
        _ => anyhow::bail!("unknown_local_binary_fixture_mode"),
    }
    let server = transport::start(&saved, grpc, true).await?;
    let evidence = root.join("anchor-pairs");
    std::fs::create_dir(&evidence)?;
    std::fs::set_permissions(&evidence, std::fs::Permissions::from_mode(0o700))?;
    let app = f::config(&server.grpc, &server.http, &evidence);
    let seed = root.join("seed.sqlite");
    f::initialize(&seed, &app, &saved.grpc)?;
    let sql = rusqlite::Connection::open(&seed)?;
    sql.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    drop(sql);
    let ready = json!({"schema_version":1,"mode":mode,"root":root,
        "grpc_url":server.grpc,"http_url":server.http,
        "grpc_port":server.grpc.rsplit(':').next().unwrap().parse::<u16>()?,
        "http_port":server.http.rsplit(':').next().unwrap().parse::<u16>()?,
        "seed_db":seed,"anchor_evidence_dir":evidence,
        "wallet":app.ingestion.yellowstone_replay_wallets[0],
        "anchor_slot":f::SLOT,"target_slot":f::SLOT+1,
        "anchor_blockhash":saved.grpc.blockhash,"parent_blockhash":saved.grpc.parent_blockhash,
        "expected_old_first_difference":{"path":"transactions[0].signature[0]","grpc":206,"http":97},
        "saved09_bindings":saved.bindings,
        "input_provenance":"typed12.6 plus authoritative original-vector17-byte sidecars; HTTP result bytes unchanged, envelope id correlated only",
        "modeled_context":"predecessor098 seed, empty later linked100","provider_calls":0});
    std::fs::write(root.join("READY.json"), serde_json::to_vec_pretty(&ready)?)?;
    tokio::time::timeout(Duration::from_secs(180), async {
        while !root.join("STOP").exists() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    std::fs::write(
        root.join("FIXTURE_STOPPED.json"),
        serde_json::to_vec_pretty(&json!({
        "stopped":true,"mode":mode,"calls":server.calls.lock().unwrap().clone(),"provider_calls":0}))?,
    )?;
    Ok(())
}
