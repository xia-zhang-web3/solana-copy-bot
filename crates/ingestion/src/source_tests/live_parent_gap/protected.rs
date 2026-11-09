//! Existing loopback transport gap recovery, extended through the real forward.
use super::*;
use crate::protected_delivery::ProtectedDelivery;
use copybot_config::ProtectedRecoveryConfig;
use copybot_storage_core::capture_scope::CaptureStore;

fn drain(capture: &std::path::Path, ledger: &std::path::Path) -> Value {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let output = std::process::Command::new(
        std::env::var("CAPTURE_TEST_PYTHON").unwrap_or_else(|_| "python3".into()),
    )
    .arg(root.join("tools/tests/capture_recovery_consumer_fixture.py"))
    .arg(capture)
    .arg(ledger)
    .output()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protected_actual_http_gap_receipt_forward_then_checkpoint_ack() {
    let server = fixture::start("valid").await;
    let mut c = settings(&server);
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().canonicalize().unwrap();
    let capture = directory.join("capture.db");
    let sql = rusqlite::Connection::open(&capture).unwrap();
    sql.execute_batch(copybot_storage_core::capture_scope::SCHEMA)
        .unwrap();
    sql.execute(
        "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,10000,1073741824)",
        [],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO capture_requests(request_key,payload,expires) VALUES('LOCAL-gap','{}',1e100)",
        [],
    )
    .unwrap();
    let wallet = fixture::wallets().into_iter().next().unwrap();
    sql.execute("INSERT INTO capture_members VALUES(1,?)", [&wallet])
        .unwrap();
    drop(sql);
    c.capture_scope_db = Some(capture.to_str().unwrap().into());
    c.capture_recovery = Some(ProtectedRecoveryConfig {
        policy: "protected_complete_recovery_v1".into(),
        migrations_directory: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../migrations")
            .canonicalize()
            .unwrap()
            .to_str()
            .unwrap()
            .into(),
        checkpoint_directory: directory.to_str().unwrap().into(),
        max_scope_wallets: 128,
        max_scope_revisions: 128,
        provenance_policy: "historical_missed_no_execution_v1".into(),
    });
    c.yellowstone_http_recovery.as_mut().unwrap().timeout_ms = 3000;
    let mut store = CaptureStore::open(&capture).unwrap();
    store.start().unwrap();
    let scope = store.prepare_scope().unwrap();
    let mut actor = ProtectedDelivery::open(&c, store, scope).unwrap();
    let mut receiver = DeliveryReceiver::start_recovering_labeled(
        &c,
        "LOCAL-protected-gap".into(),
        fixture::wallets(),
        None,
        None,
    )
    .unwrap();
    let hold = receiver.http_continuity_hold().unwrap();
    let mut last = 0;
    let mut missed = 0;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let envelope = receiver.next().await.unwrap().unwrap();
            if let Some(head) = actor.persist(&envelope.delivery).unwrap() {
                if actor.store.recovery_outbox_pending().unwrap() {
                    assert!(!actor.commit_checkpoint(&head, 2_000_000_000.).unwrap());
                    let disposition = drain(&capture, &directory.join("ledger.db"));
                    assert_eq!(disposition["ledger_unchanged"], true);
                    assert_eq!(
                        disposition["dispositions"][0]["status"],
                        "MISSED_UNEXECUTED"
                    );
                    assert_eq!(disposition["dispositions"][0]["side"], "BUY");
                    missed += 1;
                }
                assert!(actor.commit_checkpoint(&head, 2_000_000_000.).unwrap());
                last = head.block.observation.child.slot;
                receiver.acknowledge_checkpoint(head).unwrap();
                receiver.acknowledge_parent(last);
            }
            if last == fixture::FIRST + 3 {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(missed, 1, "one historical recovered action, no fresh lot");
    assert!(server
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|r| r["method"] == "getBlock" && r["params"][0] == fixture::FIRST + 1));
    // Wait only for the existing transport's verified reader-clear, bounded by ACK timeout.
    tokio::time::timeout(Duration::from_secs(1), async {
        while hold.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    receiver.stop();
    assert!(hold.load(Ordering::Acquire));
    let sql = rusqlite::Connection::open(capture).unwrap();
    assert_eq!(
        sql.query_row(
            "SELECT count(*) FROM capture_recovery_outbox WHERE state='MISSED'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
}
