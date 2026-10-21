use super::*;
use crate::source::durable::{
    continuity_telemetry::AckObservation, telemetry::DurableIngressTelemetry,
};
use crate::source::YellowstoneRuntimeConfig;
pub(super) fn config() -> IngestionConfig {
    let mut c = IngestionConfig::default();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    c.yellowstone_x_token = "LOCAL_NO_PROVIDER".into();
    c.yellowstone_delivery_mode = "durable_association_v1".into();
    let b = |count, bytes| DeliveryBudget { count, bytes };
    c.yellowstone_association = Some(AssociationDeliveryConfig {
        pending: b(16, 1 << 20),
        blocks: b(32, 4 << 20),
        history: b(32, 1 << 20),
        outputs: b(16, 1 << 20),
        queue: b(32, 4 << 20),
        inbox: b(1024, 16 << 20),
        input_bytes: 1 << 20,
        metadata_bytes: 1 << 20,
        pending_ttl_ms: 60_000,
        block_ttl_ms: 60_000,
        history_ttl_ms: 120_000,
        tick_ms: 1000,
        sqlite_busy_ms: 100,
    });
    c
}
pub(super) fn wallets() -> HashSet<String> {
    HashSet::from([bs58::encode([3; 32]).into_string()])
}
pub(super) fn block(slot: u64) -> SubscribeUpdateBlock {
    SubscribeUpdateBlock {
        slot,
        parent_slot: slot - 1,
        blockhash: bs58::encode([slot as u8; 32]).into_string(),
        parent_blockhash: bs58::encode([(slot - 1) as u8; 32]).into_string(),
        executed_transaction_count: 1,
        transactions: vec![SubscribeUpdateTransactionInfo {
            signature: vec![slot as u8; 64],
            index: 0,
            transaction: Some(Transaction {
                signatures: vec![vec![slot as u8; 64]],
                message: Some(
                    yellowstone_grpc_proto::solana::storage::confirmed_block::Message {
                        header: Some(MessageHeader {
                            num_required_signatures: 1,
                            ..Default::default()
                        }),
                        account_keys: vec![vec![9; 32]],
                        ..Default::default()
                    },
                ),
            }),
            meta: Some(TransactionStatusMeta::default()),
            ..Default::default()
        }],
        ..Default::default()
    }
}
pub(super) fn update(slot: u64) -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Block(block(slot))),
        ..Default::default()
    }
}
pub(super) fn database(path: &Path, scope: &ReplayScope) -> AssociationInbox {
    SqliteStore::open(path)
        .unwrap()
        .run_migrations(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))
        .unwrap();
    let mut inbox = AssociationInbox::open(
        path,
        InboxLimits {
            count: 1024,
            bytes: 16 << 20,
            busy_ms: 100,
        },
    )
    .unwrap();
    inbox.configure_replay_scope(scope).unwrap();
    inbox
}
pub(super) fn live_reader(
    telemetry: Arc<DurableIngressTelemetry>,
    scope: Arc<CaptureScope>,
) -> Reader {
    Reader::start(
        futures_util::stream::pending(),
        futures_util::sink::drain(),
        32,
        4 << 20,
        1 << 20,
        telemetry,
        scope,
    )
    .unwrap()
}
pub(super) async fn runtime(
    config: &IngestionConfig,
    scope: &ReplayScope,
) -> (DeliveryReceiver, Bridge<'static>, Reader, Arc<CaptureScope>) {
    // Runtime config must outlive Bridge; source owns this test-only leaked tiny config.
    let mut runtime = (*crate::source::YellowstoneGrpcSource::new(config)
        .unwrap()
        .runtime_config)
        .clone();
    runtime.admission_wallets = Some(wallets());
    let runtime: &'static YellowstoneRuntimeConfig = Box::leak(Box::new(runtime));
    let limits = config.yellowstone_association.as_ref().unwrap();
    let telemetry = Arc::new(DurableIngressTelemetry::default());
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut cursor = RecoveryCursor::new(scope.clone(), None, limits.pending.clone()).unwrap();
    cursor.http_hold = Some(flag);
    cursor.observation = Some(telemetry.continuity.hold_tracker());
    let (tx, rx) = queue::channel(limits.queue.count, limits.queue.bytes);
    let mut bridge = Bridge::new(
        runtime,
        limits,
        "LOCAL-ack-source".into(),
        tx,
        None,
        telemetry.clone(),
    )
    .unwrap();
    bridge.enable_recovery(cursor.clone(), limits).unwrap();
    let capture = bridge.begin_capture().unwrap();
    let reader = live_reader(telemetry.clone(), capture.clone());
    let receiver = DeliveryReceiver {
        rx,
        task: None,
        wallet_scope: Some(wallets()),
        bot_signer: None,
        telemetry,
        recovery: Some(cursor),
    };
    bridge
        .emit(
            0,
            DeliveryEvent::Session(SessionGap::StartedContinuityUnknown),
        )
        .await
        .unwrap();
    (receiver, bridge, reader, capture)
}
pub(super) async fn persist_next(
    r: &mut DeliveryReceiver,
    db: &mut AssociationInbox,
    scope: &ReplayScope,
) {
    let envelope = r.next().await.unwrap().unwrap();
    db.persist(&envelope.delivery, &CandidateGeneration::Unknown)
        .unwrap();
    if matches!(envelope.delivery.event, DeliveryEvent::ParentCheckpoint(_)) {
        let head = db.replay_checkpoint(scope).unwrap().unwrap();
        r.acknowledge_checkpoint(head).unwrap();
    }
}
pub(super) async fn drain_sessions(r: &mut DeliveryReceiver, db: &mut AssociationInbox) {
    while let Ok(envelope) = r.rx.try_recv() {
        assert!(matches!(envelope.delivery.event, DeliveryEvent::Session(_)));
        db.persist(&envelope.delivery, &CandidateGeneration::Unknown)
            .unwrap();
    }
}
pub(super) async fn initial(
    bridge: &mut Bridge<'_>,
    r: &mut DeliveryReceiver,
    db: &mut AssociationInbox,
    scope: &ReplayScope,
    capture: &CaptureScope,
) {
    drain_sessions(r, db).await;
    for slot in [99, 100] {
        capture.frontier(slot);
        bridge.update(slot, &update(slot)).await.unwrap();
        persist_next(r, db, scope).await;
    }
}
pub(super) fn export(
    mode: &str,
    path: &Path,
    proof: &AckObservation,
    scope: &ReplayScope,
    events: &[String],
) {
    let Some(root) = std::env::var_os("COPYBOT_ACK_OBSERVATION_OUTPUT").map(PathBuf::from) else {
        return;
    };
    let root = root.join(mode);
    std::fs::create_dir_all(&root).unwrap();
    let sql = rusqlite::Connection::open(path).unwrap();
    let destination = root.join("live_runtime.db");
    assert!(!destination.exists());
    sql.execute("VACUUM INTO ?1", [destination.to_str().unwrap()])
        .unwrap();
    let emitted = events.last().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(emitted).unwrap(),
        serde_json::to_value(proof).unwrap()
    );
    std::fs::write(root.join("observation.json"), emitted.as_bytes()).unwrap();
    std::fs::write(
        root.join("ack-events.json"),
        serde_json::to_vec(events).unwrap(),
    )
    .unwrap();
    std::fs::write(
        root.join("clear.json"),
        serde_json::to_vec(&proof.eligible_clear).unwrap(),
    )
    .unwrap();
    let mut wire = vec![];
    for slot in 99..=107 {
        use sha2::{Digest, Sha256};
        let value = update(slot);
        let bytes = value.encode_to_vec();
        let file = format!("block-{slot}.pb");
        std::fs::write(root.join(&file), &bytes).unwrap();
        wire.push(json!({"slot":slot,"parent_slot":slot-1,"blockhash":block(slot).blockhash,"parent_blockhash":block(slot).parent_blockhash,"executed_transaction_count":1,"supplied_transaction_count":1,"file":file,"sha256":format!("{:x}",Sha256::digest(&bytes)),"bytes":bytes.len()}));
    }
    std::fs::write(
        root.join("wire_blocks.json"),
        serde_json::to_vec_pretty(&wire).unwrap(),
    )
    .unwrap();
    std::fs::write(root.join("fixture_manifest.json"),serde_json::to_vec_pretty(&json!({"mode":mode,"run_id":"copybot-alpenglow-recovery-readonly-20261021-01","scope":scope,"clock":"EXPLICIT_LOCAL_TEST_CLOCK_NO_LINUX_RUNTIME_PROOF","sqlite":"actualLOCALcommit/readback","events":"syntheticlocalblocks_no_market_or_provider","observation_json":"actualRustserialization_no_Python_retag","financial_actions":0})).unwrap()).unwrap();
}
