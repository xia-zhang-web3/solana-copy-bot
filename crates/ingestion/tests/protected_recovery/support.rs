use anyhow::{Context, Result};
use copybot_config::{
    AssociationDeliveryConfig, DeliveryBudget, HttpRecoveryConfig, IngestionConfig,
    ProtectedRecoveryConfig,
};
use copybot_core_types::association_delivery::Delivery;
use copybot_ingestion::{DeliveryReceiver, ProtectedReplayInput};
use prost::Message;
use serde_json::{json, Value};
use std::collections::HashSet;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateBlock,
};

pub fn rows() -> Vec<Value> {
    serde_json::from_str::<Value>(include_str!("../fixtures/protected_missing_four.json")).unwrap()
        ["rows"]
        .as_array()
        .unwrap()
        .clone()
}
pub fn key(slot: u64) -> String {
    bs58::encode([(slot % 251 + 1) as u8; 32]).into_string()
}
pub fn wrapper(row: &Value) -> Value {
    let slot = row["slot"].as_u64().unwrap();
    let mut transaction = row["transaction_result"].clone();
    // Local causal fixture: a one-transaction wrapper gets index0. The saved
    // transaction body is retained independently; historical index is NOT proved.
    transaction["transactionIndex"] = json!(0);
    json!({"parentSlot":slot-1,"previousBlockhash":key(slot-1),"blockhash":key(slot),
        "blockHeight":slot,"blockTime":transaction["blockTime"],"transactions":[transaction],"rewards":[]})
}
pub fn block(row: &Value) -> SubscribeUpdateBlock {
    copybot_ingestion::normalize_confirmed_http_block(row["slot"].as_u64().unwrap(), &wrapper(row))
        .unwrap()
}
pub fn update(block: SubscribeUpdateBlock) -> Vec<u8> {
    SubscribeUpdate {
        update_oneof: Some(UpdateOneof::Block(block)),
        ..Default::default()
    }
    .encode_to_vec()
}
pub fn config(path: &std::path::Path) -> Result<IngestionConfig> {
    let db = rusqlite::Connection::open(path)?;
    db.execute_batch(copybot_storage_core::capture_scope::SCHEMA)?;
    db.execute(
        "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,10000,1073741824)",
        [],
    )?;
    let mut c = IngestionConfig::default();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    c.yellowstone_x_token = "offline-local".into();
    c.yellowstone_delivery_mode = "durable_association_v1".into();
    c.capture_scope_db = Some(path.to_str().unwrap().into());
    c.capture_recovery = Some(ProtectedRecoveryConfig {
        policy: "protected_complete_recovery_v1".into(),
        migrations_directory: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../migrations")
            .canonicalize()?
            .to_str()
            .unwrap()
            .into(),
        checkpoint_directory: path
            .parent()
            .unwrap()
            .canonicalize()?
            .to_str()
            .unwrap()
            .into(),
        max_scope_wallets: 128,
        max_scope_revisions: 128,
        provenance_policy: "historical_missed_no_execution_v1".into(),
    });
    let b = |count, bytes| DeliveryBudget { count, bytes };
    c.yellowstone_association = Some(AssociationDeliveryConfig {
        pending: b(16, 8 << 20),
        blocks: b(32, 16 << 20),
        history: b(32, 8 << 20),
        outputs: b(16, 8 << 20),
        queue: b(128, 16 << 20),
        inbox: b(10000, 64 << 20),
        input_bytes: 8 << 20,
        metadata_bytes: 8 << 20,
        pending_ttl_ms: 60000,
        block_ttl_ms: 60000,
        history_ttl_ms: 120000,
        tick_ms: 1000,
        sqlite_busy_ms: 100,
    });
    c.yellowstone_http_recovery = Some(HttpRecoveryConfig {
        anchor_evidence_dir: None,
        broker_url: "http://127.0.0.1:1".into(),
        broker_token: String::new(),
        range_slots: 1024,
        max_response_bytes: 8 << 20,
        timeout_ms: 1000,
        fetch_concurrency: 1,
        raw_window_blocks: Some(1),
    });
    Ok(c)
}
pub async fn replay(
    c: &IngestionConfig,
    row: &Value,
    input: Vec<ProtectedReplayInput>,
) -> Result<(Vec<Delivery>, Option<String>)> {
    let (send, receive) = tokio::sync::mpsc::channel(16);
    let mut receiver = DeliveryReceiver::replay_protected(
        c,
        "LOCAL-SYNTHETIC-protected".into(),
        receive,
        HashSet::from([row["wallet"].as_str().context("wallet")?.to_owned()]),
        None,
    )?;
    for v in input {
        send.send(v).await?;
    }
    drop(send);
    let mut events = vec![];
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(5), receiver.next()).await? {
            Ok(Some(envelope)) => events.push(envelope.delivery),
            Ok(None) => return Ok((events, None)),
            Err(error) => return Ok((events, Some(format!("{error:#}")))),
        }
    }
}
