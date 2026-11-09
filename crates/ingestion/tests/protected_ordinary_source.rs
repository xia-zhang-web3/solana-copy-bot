//! Ordinary source intake shares protected recovery's one source, not its wallet admission list.
#[path = "protected_recovery/support.rs"]
mod support;
use anyhow::Result;
use copybot_core_types::association_delivery::{DeliveryEvent, MessageTime};
use copybot_ingestion::{
    protected_capture::ordinary_live_swap, DeliveryReceiver, ProtectedReplayInput,
};
use prost::Message;
use std::collections::HashSet;
use yellowstone_grpc_proto::{
    prelude::{subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateBlock},
    prost_types::Timestamp,
};

fn frame() -> SubscribeUpdate {
    // Explicit LOCAL clock; neither this timestamp nor the one-tx block wrapper
    // claims historical timeliness or historical block completeness.
    let mut frame = SubscribeUpdate::decode(
        include_bytes!("../../app/tests/fixtures/capture/quote_sol_wallet_11_buy.pb").as_slice(),
    )
    .unwrap();
    frame.created_at = Some(Timestamp {
        seconds: 1_790_000_000,
        nanos: 123,
    });
    let Some(UpdateOneof::Transaction(tx)) = frame.update_oneof.as_mut() else {
        panic!("fixture transaction")
    };
    tx.transaction.as_mut().unwrap().index = 0;
    frame
}
async fn events(
    c: &copybot_config::IngestionConfig,
    wallets: HashSet<String>,
    inputs: Vec<ProtectedReplayInput>,
) -> Result<Vec<copybot_core_types::association_delivery::Delivery>> {
    let (send, input) = tokio::sync::mpsc::channel(8);
    let mut receiver =
        DeliveryReceiver::replay_protected(c, "LOCAL-R1-ordinary".into(), input, wallets, None)?;
    for input in inputs {
        send.send(input).await?;
    }
    drop(send);
    let mut result = vec![];
    while let Some(envelope) =
        tokio::time::timeout(std::time::Duration::from_secs(5), receiver.next()).await??
    {
        result.push(envelope.delivery);
    }
    Ok(result)
}
#[tokio::test]
async fn empty_and_foreign_protected_scopes_keep_ordinary_live_without_protected_admission(
) -> Result<()> {
    for wallets in [
        HashSet::new(),
        HashSet::from([bs58::encode([2_u8; 32]).into_string()]),
    ] {
        let temp = tempfile::tempdir()?;
        let c = support::config(&temp.path().join("capture.db"))?;
        let stream = frame();
        let Some(UpdateOneof::Transaction(tx)) = stream.update_oneof.as_ref() else {
            unreachable!()
        };
        let block = SubscribeUpdateBlock {
            slot: tx.slot,
            parent_slot: tx.slot - 1,
            blockhash: support::key(tx.slot),
            parent_blockhash: support::key(tx.slot - 1),
            bank_id: tx.bank_id,
            executed_transaction_count: 1,
            transactions: vec![tx.transaction.clone().unwrap()],
            ..Default::default()
        };
        let result = events(
            &c,
            wallets,
            vec![
                ProtectedReplayInput::Update {
                    offset_ns: 1,
                    payload: stream.encode_to_vec(),
                },
                ProtectedReplayInput::Update {
                    offset_ns: 2,
                    payload: support::update(block),
                },
                ProtectedReplayInput::End(3),
            ],
        )
        .await?;
        let live: Vec<_> = result
            .iter()
            .filter_map(|d| {
                if let DeliveryEvent::OrdinaryLive(p) = &d.event {
                    Some(p)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(
            live.len(),
            1,
            "only actual transaction frame is ordinary input"
        );
        let (_, protected_identity) = copybot_ingestion::protected_capture::reconstruct_stream(
            &c,
            &stream.encode_to_vec(),
            1_790_000_000_000_000,
        )?;
        assert_eq!(live[0].info_hash, protected_identity.info_hash);
        let swap = ordinary_live_swap(&c, live[0])?.unwrap();
        assert_eq!(swap.signature, live[0].facts.signature);
        assert_eq!(swap.ts_utc.timestamp(), 1_790_000_000);
        assert!(
            !result.iter().any(|d| matches!(
                d.event,
                DeliveryEvent::ProtectedObservation(_) | DeliveryEvent::Admission(_)
            )),
            "ordinary outsider must not gain protected admission"
        );
    }
    Ok(())
}
#[tokio::test]
async fn missing_stream_time_and_http_recovery_cannot_be_ordinary_live() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let c = support::config(&temp.path().join("capture.db"))?;
    let mut stream = frame();
    stream.created_at = None;
    let row = support::rows().remove(0);
    let raw = serde_json::to_vec(
        &serde_json::json!({"jsonrpc":"2.0","id":1,"result":support::wrapper(&row)}),
    )?;
    let result = events(
        &c,
        HashSet::new(),
        vec![
            ProtectedReplayInput::Update {
                offset_ns: 1,
                payload: stream.encode_to_vec(),
            },
            ProtectedReplayInput::HttpBlock {
                offset_ns: 2,
                slot: row["slot"].as_u64().unwrap(),
                raw_response: raw,
            },
            ProtectedReplayInput::End(3),
        ],
    )
    .await?;
    assert!(!result
        .iter()
        .any(|d| matches!(d.event, DeliveryEvent::OrdinaryLive(_))));
    let packets = events(
        &c,
        HashSet::new(),
        vec![
            ProtectedReplayInput::Update {
                offset_ns: 1,
                payload: frame().encode_to_vec(),
            },
            ProtectedReplayInput::End(2),
        ],
    )
    .await?;
    let mut packet = packets
        .into_iter()
        .find_map(|d| {
            if let DeliveryEvent::OrdinaryLive(p) = d.event {
                Some(p)
            } else {
                None
            }
        })
        .unwrap();
    packet.message_time = MessageTime::RecoveredBlock {
        block_time: Some(1_790_000_000),
    };
    assert!(ordinary_live_swap(&c, &packet).is_err());
    Ok(())
}

#[tokio::test]
async fn recovery_first_suppresses_later_original_before_comparing_representation_hash(
) -> Result<()> {
    use copybot_core_types::association_delivery::OrdinaryLive;
    use copybot_ingestion::protected_delivery::ProtectedDelivery;
    use copybot_storage_core::capture_scope::CaptureStore;
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("capture.db");
    let c = support::config(&path)?;
    let row = support::rows().remove(0);
    let wallet = row["wallet"].as_str().unwrap().to_owned();
    let db = rusqlite::Connection::open(&path)?;
    db.execute("INSERT INTO capture_requests(request_key,payload,expires) VALUES('LOCAL-identity','{}',1e100)",[])?;
    db.execute("INSERT INTO capture_members VALUES(1,?)", [&wallet])?;
    let mut store = CaptureStore::open(&path)?;
    store.start()?;
    let scope = store.prepare_scope()?;
    let deliveries = events(
        &c,
        HashSet::from([wallet]),
        vec![
            ProtectedReplayInput::Update {
                offset_ns: 1,
                payload: support::update(support::block(&row)),
            },
            ProtectedReplayInput::End(2),
        ],
    )
    .await?;
    let mut prior = None;
    for d in deliveries {
        match d.event {
            DeliveryEvent::ProtectedEvidence(e) => store.commit_evidence(scope.revision, &e)?,
            DeliveryEvent::ProtectedObservation(o) => {
                let receipt = store.commit_observation(
                    scope.revision,
                    &o,
                    None,
                    copybot_ingestion::protected_capture::checked_protected_info_equivalent,
                )?;
                assert!(receipt.recovery_first);
                prior = Some(o);
            }
            _ => {}
        }
    }
    let prior = prior.unwrap();
    let packet = OrdinaryLive {
        facts: prior.facts.unwrap(),
        message_time: MessageTime::CreatedAt {
            seconds: 1_790_000_000,
            nanos: 0,
        },
        info_hash: "00".repeat(32),
    };
    let mut state = ProtectedDelivery::open(&c, store, scope)?;
    assert!(state.ordinary_live(&packet)?.is_none(),
        "a recovered identity never gets promoted; RPC/Geyser representation differences do not make it fresh");
    assert_eq!(
        db.query_row("SELECT status FROM capture_meta", [], |r| r
            .get::<_, String>(0))?,
        "running"
    );
    Ok(())
}

#[tokio::test]
async fn retained_stream_first_info_conflict_refuses_ordinary_fresh_delivery() -> Result<()> {
    use copybot_core_types::{
        association_delivery::OrdinaryLive,
        association_parent::{BlockKey, ParentObservation},
        association_recovery::{BlockCheckpoint, DurableCheckpoint},
    };
    use copybot_ingestion::protected_delivery::ProtectedDelivery;
    use copybot_storage_core::capture_scope::CaptureStore;
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("capture.db");
    let c = support::config(&path)?;
    let (evidence, observation) = copybot_ingestion::protected_capture::reconstruct_stream(
        &c,
        &frame().encode_to_vec(),
        1_790_000_000_000_000,
    )?;
    let db = rusqlite::Connection::open(&path)?;
    db.execute("INSERT INTO capture_requests(request_key,payload,expires) VALUES('LOCAL-stream-identity','{}',1e100)",[])?;
    db.execute(
        "INSERT INTO capture_members VALUES(1,?)",
        [&observation.wallet],
    )?;
    let mut store = CaptureStore::open(&path)?;
    store.start()?;
    let scope = store.prepare_scope()?;
    let replay = copybot_ingestion::replay_scope(&c, &scope.wallets.iter().cloned().collect())?;
    // Explicit controlled empty baseline for this identity-only unit. Full app
    // dispatch/ACK coverage is separate; this is not a historical block claim.
    let head = DurableCheckpoint {
        session: "LOCAL-stream-baseline".into(),
        sequence: 0,
        from_slot: 6,
        overlap: vec![],
        block: BlockCheckpoint {
            scope: replay,
            observation: ParentObservation {
                child: BlockKey {
                    slot: 7,
                    hash: support::key(7),
                },
                parent: BlockKey {
                    slot: 6,
                    hash: support::key(6),
                },
                issue: None,
            },
            executed_transaction_count: 0,
            supplied_transaction_count: 0,
            claims: vec![],
        },
    };
    store.commit_scope(&scope, &head, 1.)?;
    store.commit_evidence(scope.revision, &evidence)?;
    let source_at = match observation.message_time {
        MessageTime::CreatedAt { seconds, nanos } => seconds as f64 + nanos as f64 / 1e9,
        _ => unreachable!(),
    };
    let receipt = store
        .receive(
            observation.signature.as_deref(),
            &observation.wallet,
            observation.slot,
            &evidence.raw,
            &observation.transaction_fingerprint,
            evidence.available_at_micros as f64 / 1e6,
            Some(source_at),
        )?
        .unwrap();
    let swap = copybot_ingestion::protected_capture::protected_stream_swap(&c, &observation)?;
    assert!(swap.is_some());
    store.finish(receipt.seq, swap.as_ref(), "LOCAL stream control")?;
    let bound = store.commit_observation(
        scope.revision,
        &observation,
        Some(receipt.seq),
        copybot_ingestion::protected_capture::checked_protected_info_equivalent,
    )?;
    assert!(!bound.recovery_first);
    let mut state = ProtectedDelivery::open(&c, store, scope)?;
    let mut packet = OrdinaryLive {
        facts: observation.facts.unwrap(),
        message_time: observation.message_time,
        info_hash: observation.info_hash,
    };
    assert!(state.ordinary_live(&packet)?.is_some());
    packet.info_hash = "00".repeat(32);
    assert!(state
        .ordinary_live(&packet)
        .unwrap_err()
        .to_string()
        .contains("ordinary_live_protected_info_conflict"));
    assert_eq!(
        db.query_row("SELECT status FROM capture_meta", [], |r| r
            .get::<_, String>(0))?,
        "failed"
    );
    Ok(())
}
