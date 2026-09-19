use super::capture_scope_fixture as f;
use crate::source::scoped_capture as c;
use serde_json::Value;
use std::path::Path;
fn python(dir: &Path, wallet: &str, phase: &str, seq: Option<i64>) -> Value {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut command = std::process::Command::new("python3");
    command
        .arg(root.join("tools/tests/capture_virtual_bridge_fixture.py"))
        .arg(dir)
        .arg(wallet)
        .arg(phase);
    if let Some(seq) = seq {
        command.arg(seq.to_string());
    }
    let result = command.output().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}
fn fresh(fixture: &Value) -> yellowstone_grpc_proto::prelude::SubscribeUpdate {
    let mut update = f::update(fixture);
    let now = chrono::Utc::now();
    update.created_at = Some(yellowstone_grpc_proto::prost_types::Timestamp {
        seconds: now.timestamp(),
        nanos: now.timestamp_subsec_nanos() as i32,
    });
    update
}
#[tokio::test]
async fn capture_scope_actual_rust_ack_python_virtual_buy_demotion_restart_sell() {
    let tmp = tempfile::tempdir().unwrap();
    let all = f::fixtures();
    let first = all.iter().find(|x| x["expected"]["side"] == "BUY").unwrap();
    let wallet = first["expected"]["wallet"].as_str().unwrap();
    let second = all
        .iter()
        .find(|x| {
            x["expected"]["side"] == "BUY"
                && x["expected"]["wallet"] == wallet
                && x["signature"] != first["signature"]
        })
        .unwrap();
    let sell = all
        .iter()
        .find(|x| {
            x["expected"]["side"] == "SELL"
                && x["expected"]["wallet"] == wallet
                && x["expected"]["mint"] == second["expected"]["mint"]
        })
        .unwrap();
    python(tmp.path(), wallet, "init", None);
    let path = tmp.path().join("capture.db");
    let runtime = f::runtime(Some(&path));
    c::restore(runtime.clone()).await.unwrap();
    // Event between publication and consumer ACK is decoded but cannot enter portfolio.
    c::process(fresh(first), runtime.clone())
        .await
        .unwrap()
        .unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM capture_events", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    c::refresh(runtime.clone()).await.unwrap();
    let pre_admission = fresh(first);
    c::process(pre_admission.clone(), runtime.clone())
        .await
        .unwrap()
        .unwrap();
    python(tmp.path(), wallet, "admit", None);
    assert_eq!(
        python(tmp.path(), wallet, "observe", Some(1))["status"],
        "SKIPPED"
    );
    // Repeat the captured signature after admission: permanent skip, no retroactive BUY.
    c::process(pre_admission, runtime.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        python(tmp.path(), wallet, "observe", Some(1))["status"],
        "SKIPPED"
    );
    c::process(fresh(second), runtime.clone())
        .await
        .unwrap()
        .unwrap();
    let buy = python(tmp.path(), wallet, "observe", Some(2));
    assert_eq!(buy["ledger_event"]["status"], "BUY_PENDING");
    assert_eq!(
        db.query_row("SELECT count(*) FROM capture_obligations", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    python(tmp.path(), wallet, "demote", None);
    c::refresh(runtime.clone()).await.unwrap();
    drop(runtime);
    let restored = f::runtime(Some(&path));
    c::restore(restored.clone()).await.unwrap();
    c::process(fresh(sell), restored).await.unwrap().unwrap();
    let sell = python(tmp.path(), wallet, "observe", Some(3));
    assert_eq!(sell["ledger_event"]["status"], "SELL_PENDING");
    let ledger = rusqlite::Connection::open(tmp.path().join("virtual.db")).unwrap();
    assert_eq!(
        ledger
            .query_row("SELECT count(*) FROM lots", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        ledger
            .query_row("SELECT count(*) FROM jobs WHERE state='RUNNING'", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    println!("actual Rust consumer ACK -> real VirtualLedger BUY_PENDING -> demotion/restart -> SELL_PENDING; HTTP/sign/daemon=0");
}
