//! Exact saved-envelope checks for the explicit configured-family DEX policy.
#[path = "dex_policy/expected.rs"]
mod expected;
#[path = "dex_policy/support.rs"]
mod support;

use anyhow::{ensure, Context, Result};
use copybot_config::IngestionConfig;
use copybot_ingestion::capture_replay::CaptureReplay;
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;
use support::{config, create, receipt, request};

fn fixture(name: &str) -> Result<Vec<u8>> {
    Ok(std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR")).join(name),
    )?)
}
#[tokio::test]
async fn dex_policy_identical_multi_envelope_is_stable_and_single_controls_keep_labels(
) -> Result<()> {
    let original = IngestionConfig::default();
    let manifest: Value =
        serde_json::from_slice(&fixture("tests/fixtures/dex_policy/manifest.json")?)?;
    let raw = fixture("tests/fixtures/dex_policy/multi_dex.pb")?;
    ensure!(
        format!("{:x}", Sha256::digest(&raw)) == manifest["raw_sha256"],
        "fixture checksum mismatch"
    );
    ensure!(
        expected::label(&raw, &original)? == "multi_dex",
        "independent multi-DEX evidence"
    );
    let mut canonical = manifest["saved_event"].clone();
    canonical["dex"] = json!("multi_dex");
    for trial in 0..64 {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("capture.db");
        let db = create(&path)?;
        request(&db, canonical["wallet"].as_str().context("wallet")?)?;
        let replay = CaptureReplay::open(&config(&path, &original)).await?;
        replay.accept_pending().await?;
        let event = replay
            .push(&raw)
            .await?
            .context("supported multi-DEX event")?;
        ensure!(
            serde_json::from_str::<Value>(&serde_json::to_string(&event)?)? == canonical,
            "canonical variation at trial {trial}"
        );
        let stored = receipt(&db, 1)?;
        ensure!(
            stored.raw == raw
                && stored.stage == "DURABLE"
                && stored.event.as_ref() == Some(&canonical),
            "captured multi-DEX payload differs at trial {trial}"
        );
    }
    for name in ["saved_buy.pb", "saved_sell.pb"] {
        let raw = fixture(&format!("../app/tests/fixtures/capture/{name}"))?;
        ensure!(
            expected::label(&raw, &original)? == "pumpswap",
            "single-DEX control evidence"
        );
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("capture.db");
        let _db = create(&path)?;
        let replay = CaptureReplay::open(&config(&path, &original)).await?;
        // Parsing is real; empty membership does not change the returned observation.
        ensure!(
            replay.push(&raw).await?.context("single-DEX control")?.dex == "pumpswap",
            "single-family label changed"
        );
    }
    println!(
        "DEX_POLICY_IDENTICAL_ENVELOPE={}",
        json!({"multi_dex_trials":64,"single_controls":2})
    );
    Ok(())
}
async fn verify_export(path: &Path, original: &IngestionConfig) -> Result<Value> {
    let source = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    source.execute_batch("PRAGMA query_only=ON")?;
    ensure!(
        source.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))? == "ok",
        "input integrity"
    );
    let sequences = source
        .prepare("SELECT seq FROM capture_events ORDER BY seq")?
        .query_map([], |r| r.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(
        sequences.len() == 1146,
        "approved export receipt count changed"
    );
    let temp = tempfile::tempdir()?;
    let target = temp.path().join("replay.db");
    let db = create(&target)?;
    let replay = CaptureReplay::open(&config(&target, original)).await?;
    let mut current_wallet = None;
    let mut durable = 0;
    let mut rejected = 0;
    let mut changes = Vec::new();
    let mut labels = BTreeMap::<String, usize>::new();
    for (index, seq) in sequences.iter().enumerate() {
        let saved = receipt(&source, *seq)?;
        ensure!(
            matches!(saved.stage.as_str(), "DURABLE" | "REJECTED"),
            "unfinished seq{seq}"
        );
        if current_wallet.as_deref() != Some(saved.wallet.as_str()) {
            request(&db, &saved.wallet)?;
            replay.accept_pending().await?;
            current_wallet = Some(saved.wallet.clone());
        }
        let pushed = replay.push(&saved.raw).await;
        let actual =
            receipt(&db, index as i64 + 1).with_context(|| format!("replay missing seq{seq}"))?;
        let mut policy_expected = saved.clone();
        if saved.stage == "DURABLE" {
            let required = expected::label(&saved.raw, original)?;
            let old = saved.event.as_ref().context("saved canonical event")?["dex"]
                .as_str()
                .context("saved DEX label")?;
            ensure!(
                actual.event.as_ref().context("actual canonical event")?["dex"] == required,
                "independent DEX policy mismatch at seq{seq}"
            );
            if required != old {
                ensure!(
                    required == "multi_dex" && matches!(old, "raydium" | "pumpswap"),
                    "unexpected semantic transition at seq{seq}: {old}->{required}"
                );
                changes.push(json!({"seq":seq,"signature":saved.signature,"old":old,"new":required,
                    "reason":"both configured DEX families present in exact envelope program references"}));
            }
            policy_expected.event.as_mut().unwrap()["dex"] = json!(required);
            let returned = serde_json::from_str::<Value>(&serde_json::to_string(
                &pushed?.context("DURABLE must return supported swap")?,
            )?)?;
            ensure!(
                Some(&returned) == actual.event.as_ref(),
                "returned/stored canonical mismatch seq{seq}"
            );
            *labels.entry(required.to_string()).or_default() += 1;
            durable += 1;
        } else {
            ensure!(
                pushed.as_ref().map_or(true, |value| value.is_none()),
                "supported swap rejected at seq{seq}"
            );
            rejected += 1;
        }
        ensure!(
            actual == policy_expected,
            "non-policy receipt/canonical mismatch at seq{seq}"
        );
    }
    ensure!(
        durable == 896 && rejected == 250,
        "supported/rejected boundary changed"
    );
    ensure!(
        db.query_row("SELECT count(*) FROM capture_events", [], |r| r
            .get::<_, usize>(0))?
            == sequences.len(),
        "replay cardinality mismatch"
    );
    Ok(
        json!({"receipts":sequences.len(),"durable":durable,"rejected":rejected,
        "new_label_counts":labels,"intentional_dex_changes":changes,"other_mismatches":0,
        "source_open_read_only":true,"policy":"configured family presence; multi_dex means both, not venue attribution"}),
    )
}
#[tokio::test]
#[ignore = "requires stopped capture export and matching decoder configuration"]
async fn dex_policy_live_export_preserves_every_non_policy_field() -> Result<()> {
    let path =
        std::env::var_os("DEX_POLICY_REPLAY_DB").context("stopped capture export required")?;
    let config_path =
        std::env::var_os("DEX_POLICY_REPLAY_CONFIG").context("actual config required")?;
    let original: IngestionConfig = serde_json::from_slice(&std::fs::read(config_path)?)?;
    let result = verify_export(Path::new(&path), &original).await?;
    if let Some(output) = std::env::var_os("DEX_POLICY_REPLAY_REPORT") {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output)?;
        file.write_all(serde_json::to_string_pretty(&result)?.as_bytes())?;
    }
    println!(
        "DEX_POLICY_EXPORT={}",
        json!({"receipts":result["receipts"],"durable":result["durable"],
        "rejected":result["rejected"],"intentional_dex_changes":result["intentional_dex_changes"].as_array().unwrap().len(),
        "new_label_counts":result["new_label_counts"],"other_mismatches":0})
    );
    Ok(())
}
