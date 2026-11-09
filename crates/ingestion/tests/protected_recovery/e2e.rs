//! Actual producer → capture/inbox → Python forward → committed checkpoint ACK.
use super::support;
use anyhow::{ensure, Context, Result};
use copybot_ingestion::{
    protected_delivery::ProtectedDelivery, DeliveryReceiver, ProtectedReplayInput,
};
use copybot_storage_core::capture_scope::CaptureStore;
use serde_json::Value;
use std::collections::HashSet;

fn drain(capture: &std::path::Path, ledger: &std::path::Path) -> Result<Value> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let result = std::process::Command::new(
        std::env::var("CAPTURE_TEST_PYTHON").unwrap_or_else(|_| "python3".into()),
    )
    .arg(root.join("tools/tests/capture_recovery_consumer_fixture.py"))
    .arg(capture)
    .arg(ledger)
    .output()?;
    ensure!(
        result.status.success(),
        "actual Python consumer failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(serde_json::from_slice(&result.stdout)?)
}

#[tokio::test]
async fn saved_four_complete_source_to_missed_disposition_and_durable_ack() -> Result<()> {
    for http in [false, true] {
        for row in support::rows() {
            let temp = tempfile::tempdir()?;
            let capture = temp.path().join("capture.db");
            let c = support::config(&capture)?;
            let sql = rusqlite::Connection::open(&capture)?;
            sql.execute("INSERT INTO capture_requests(request_key,payload,expires) VALUES('LOCAL-fixture','{}',1e100)",[])?;
            sql.execute(
                "INSERT INTO capture_members VALUES(1,?)",
                [row["wallet"].as_str().unwrap()],
            )?;
            drop(sql);
            let mut store = CaptureStore::open(&capture)?;
            store.start()?;
            let scope = store.prepare_scope()?;
            let mut actor = ProtectedDelivery::open(&c, store, scope)?;
            let (send, receive) = tokio::sync::mpsc::channel(8);
            let mut receiver = DeliveryReceiver::replay_protected(
                &c,
                "LOCAL-complete-protected".into(),
                receive,
                HashSet::from([row["wallet"].as_str().unwrap().to_owned()]),
                None,
            )?;
            let input = if http {
                ProtectedReplayInput::HttpBlock {
                    offset_ns: 1,
                    slot: row["slot"].as_u64().unwrap(),
                    raw_response: serde_json::to_vec(
                        &serde_json::json!({"jsonrpc":"2.0","id":1,"result":support::wrapper(&row)}),
                    )?,
                }
            } else {
                ProtectedReplayInput::Update {
                    offset_ns: 1,
                    payload: support::update(support::block(&row)),
                }
            };
            send.send(input).await?;
            send.send(ProtectedReplayInput::End(2)).await?;
            drop(send);
            let mut acknowledgements = 0;
            let mut dispositions = vec![];
            while let Some(envelope) =
                tokio::time::timeout(std::time::Duration::from_secs(10), receiver.next()).await??
            {
                if let Some(head) = actor.persist(&envelope.delivery)? {
                    assert!(
                        !actor.commit_checkpoint(&head, 2_000_000_000.)?,
                        "pending forward blocks ACK"
                    );
                    let result = drain(&capture, &temp.path().join("ledger.db"))?;
                    assert_eq!(result["ledger_unchanged"], true);
                    let rows = result["dispositions"].as_array().context("dispositions")?;
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0]["status"], "MISSED_UNEXECUTED");
                    assert_eq!(rows[0]["token_qty_raw"], row["outgoing_raw"]);
                    assert_eq!(rows[0]["signature"], row["signature"]);
                    dispositions = rows.clone();
                    assert!(actor.commit_checkpoint(&head, 2_000_000_000.)?);
                    receiver.acknowledge_checkpoint(head)?;
                    acknowledgements += 1;
                }
            }
            assert_eq!(acknowledgements, 1);
            assert_eq!(dispositions.len(), 1);
            // Restart after both commits: exact checkpoint survives, no pending outbox.
            let scope = actor.store.active_scope()?.context("active scope")?;
            drop(actor);
            let mut store = CaptureStore::open(&capture)?;
            store.start()?;
            let mut actor = ProtectedDelivery::open(&c, store, scope)?;
            let head = actor
                .restored_checkpoint()?
                .context("restored exact checkpoint")?;
            assert_eq!(
                head.block.observation.child.slot,
                row["slot"].as_u64().unwrap()
            );
            assert!(!actor.store.recovery_outbox_pending()?);
            let db = rusqlite::Connection::open(&capture)?;
            assert_eq!(
                db.query_row(
                    "SELECT count(*) FROM capture_recovery_outbox WHERE state='MISSED'",
                    [],
                    |r| r.get::<_, i64>(0)
                )?,
                1
            );
            assert_eq!(
                db.query_row("SELECT count(*) FROM capture_events", [], |r| r
                    .get::<_, i64>(0))?,
                0
            );
        }
    }
    Ok(())
}
