//! Positive LOCAL clock control through the actual raw stream capture and adapter.
//! The fresh timestamp here is synthetic; no historical timeliness is inferred.
use super::support;
use anyhow::{ensure, Context, Result};
use copybot_core_types::association_delivery::DeliveryEvent;
use copybot_ingestion::{
    protected_delivery::ProtectedDelivery, DeliveryReceiver, ProtectedReplayInput,
};
use copybot_storage_core::capture_scope::CaptureStore;
use prost::Message;
use serde_json::Value;
use std::collections::HashSet;
use yellowstone_grpc_proto::{
    prelude::{subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateBlock},
    prost_types::Timestamp,
};

fn forward(
    phase: &str,
    capture: &std::path::Path,
    ledger: &std::path::Path,
    wallet: &str,
) -> Result<Value> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new(
        std::env::var("CAPTURE_TEST_PYTHON").unwrap_or_else(|_| "python3".into()),
    )
    .arg(root.join("tools/tests/protected_live_consumer_fixture.py"))
    .arg(phase)
    .arg(capture)
    .arg(ledger)
    .arg("--wallet")
    .arg(wallet)
    .output()?;
    ensure!(
        output.status.success(),
        "actual live adapter {phase} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}
#[tokio::test]
async fn genuine_stream_after_published_ack_delivers_once_across_complete_block_duplicate(
) -> Result<()> {
    let mut stream = SubscribeUpdate::decode(
        include_bytes!("../../../app/tests/fixtures/capture/quote_sol_wallet_11_buy.pb").as_slice(),
    )?;
    let Some(UpdateOneof::Transaction(tx)) = stream.update_oneof.as_mut() else {
        anyhow::bail!("saved stream transaction");
    };
    let slot = tx.slot;
    let info = tx.transaction.as_mut().context("saved Info")?;
    // A controlled one-transaction complete-block fixture uses index0.
    info.index = 0;
    let wallet = bs58::encode(
        info.transaction
            .as_ref()
            .unwrap()
            .message
            .as_ref()
            .unwrap()
            .account_keys[0]
            .as_slice(),
    )
    .into_string();
    let info = info.clone();
    let bank_id = tx.bank_id;
    let temp = tempfile::tempdir()?;
    let capture = temp.path().join("capture.db");
    let ledger = temp.path().join("virtual.db");
    let c = support::config(&capture)?;
    forward("prepare", &capture, &ledger, &wallet)?;
    let mut store = CaptureStore::open(&capture)?;
    store.start()?;
    let scope = store.prepare_scope()?;
    let mut actor = ProtectedDelivery::open(&c, store, scope)?;
    let (send, receive) = tokio::sync::mpsc::channel(8);
    let mut receiver = DeliveryReceiver::replay_protected(
        &c,
        "LOCAL-positive-stream".into(),
        receive,
        HashSet::from([wallet.clone()]),
        None,
    )?;
    let baseline = SubscribeUpdateBlock {
        slot: slot - 1,
        parent_slot: slot - 2,
        blockhash: support::key(slot - 1),
        parent_blockhash: support::key(slot - 2),
        ..Default::default()
    };
    send.send(ProtectedReplayInput::Update {
        offset_ns: 1,
        payload: support::update(baseline),
    })
    .await?;
    loop {
        let envelope = tokio::time::timeout(std::time::Duration::from_secs(5), receiver.next())
            .await??
            .context("baseline delivery")?;
        if let Some(head) = actor.persist(&envelope.delivery)? {
            assert!(actor
                .commit_checkpoint(&head, chrono::Utc::now().timestamp_micros() as f64 / 1e6)?);
            receiver.acknowledge_checkpoint(head)?;
            break;
        }
    }
    let published = forward("publish", &capture, &ledger, &wallet)?;
    let available = published["result"]["available_at"]
        .as_f64()
        .context("available ACK time")?;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let local_created = chrono::Utc::now();
    assert!(local_created.timestamp_micros() as f64 / 1e6 > available);
    stream.created_at = Some(Timestamp {
        seconds: local_created.timestamp(),
        nanos: local_created.timestamp_subsec_nanos() as i32,
    });
    send.send(ProtectedReplayInput::Update {
        offset_ns: 2,
        payload: stream.encode_to_vec(),
    })
    .await?;
    loop {
        let envelope = tokio::time::timeout(std::time::Duration::from_secs(5), receiver.next())
            .await??
            .context("stream delivery")?;
        let observed = matches!(
            &envelope.delivery.event,
            DeliveryEvent::ProtectedObservation(_)
        );
        assert!(actor.persist(&envelope.delivery)?.is_none());
        if observed {
            break;
        }
    }
    let independent = rusqlite::Connection::open(&capture)?;
    let receipts: Vec<(i64, String, Option<String>, Option<String>)> = independent
        .prepare("SELECT seq,stage,reason,event_json FROM capture_events ORDER BY seq")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
        .collect::<rusqlite::Result<_>>()?;
    ensure!(
        receipts.len() == 1 && receipts[0].1 == "DURABLE",
        "LOCAL live capture before adapter: {receipts:?}"
    );
    drop(independent);
    let first = forward("observe", &capture, &ledger, &wallet)?;
    let duplicate_block = SubscribeUpdateBlock {
        slot,
        parent_slot: slot - 1,
        blockhash: support::key(slot),
        parent_blockhash: support::key(slot - 1),
        bank_id,
        executed_transaction_count: 1,
        transactions: vec![info],
        ..Default::default()
    };
    send.send(ProtectedReplayInput::Update {
        offset_ns: 3,
        payload: support::update(duplicate_block),
    })
    .await?;
    send.send(ProtectedReplayInput::End(4)).await?;
    drop(send);
    let mut committed = 0;
    while let Some(envelope) =
        tokio::time::timeout(std::time::Duration::from_secs(5), receiver.next()).await??
    {
        if let Some(head) = actor.persist(&envelope.delivery)? {
            assert!(actor
                .commit_checkpoint(&head, chrono::Utc::now().timestamp_micros() as f64 / 1e6)?);
            receiver.acknowledge_checkpoint(head)?;
            committed += 1;
        }
    }
    assert_eq!(committed, 1);
    let second = forward("repeat", &capture, &ledger, &wallet)?;
    assert_eq!(
        first["result"]["ledger_event"]["id"],
        second["result"]["ledger_event"]["id"]
    );
    assert_eq!(first["counts"], second["counts"]);
    println!(
        "LOCAL_STREAM_CONTROL counts={} duplicate_same_event=true provider_calls=0 imports={}",
        second["counts"], second["imports"]
    );
    Ok(())
}
