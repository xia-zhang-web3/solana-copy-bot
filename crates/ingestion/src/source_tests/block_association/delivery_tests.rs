use super::*;
use crate::ReplayInput;
use copybot_config::{AppConfig, AssociationDeliveryConfig, DeliveryBudget};
use copybot_core_types::association_delivery::*;
use tokio::sync::mpsc;
pub(super) fn policy() -> AppConfig {
    let mut c = AppConfig::default();
    c.ingestion = config();
    c.ingestion.source = "yellowstone_grpc".into();
    c.execution.enabled = false;
    c.execution.canary_tiny_submit_enabled = false;
    c.ingestion.yellowstone_delivery_mode = "durable_association_v1".into();
    let b = |count, bytes| DeliveryBudget { count, bytes };
    c.ingestion.yellowstone_association = Some(AssociationDeliveryConfig {
        pending: b(1024, 16 << 20),
        blocks: b(32, 64 << 20),
        history: b(2048, 32 << 20),
        outputs: b(17, 4 << 20),
        queue: b(4, 8 << 20),
        inbox: b(20000, 128 << 20),
        input_bytes: 8 << 20,
        metadata_bytes: 32 << 20,
        pending_ttl_ms: 60_000,
        block_ttl_ms: 60_000,
        history_ttl_ms: 120_000,
        tick_ms: 1000,
        sqlite_busy_ms: 100,
    });
    c
}
pub(super) fn input(ns: u64, u: SubscribeUpdate) -> ReplayInput {
    ReplayInput::Update {
        offset_ns: ns,
        payload: u.encode_to_vec(),
    }
}
pub(super) fn block(b: SubscribeUpdateBlock) -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Block(b)),
        ..Default::default()
    }
}
pub(super) async fn run(c: AppConfig, v: Vec<ReplayInput>) -> (Vec<Delivery>, bool) {
    let (tx, rx) = mpsc::channel(2);
    let mut service =
        crate::IngestionService::with_replay(&c, rx, "fixture-session".into()).unwrap();
    let mut receiver = service.take_delivery("unused".into()).unwrap().unwrap();
    let producer = tokio::spawn(async move {
        for i in v {
            if tx.send(i).await.is_err() {
                break;
            }
        }
    });
    let mut events = vec![];
    let mut failed = false;
    loop {
        match receiver.next().await {
            Ok(Some(e)) => events.push(e.delivery.clone()),
            Ok(None) => break,
            Err(_) => {
                failed = true;
                break;
            }
        }
    }
    producer.await.unwrap();
    (events, failed)
}
#[tokio::test]
async fn b89_service_time_direction_order_and_late_matrix() -> Result<()> {
    for sell in [false, true] {
        for time in [
            None,
            Some(yellowstone_grpc_proto::prost_types::Timestamp {
                seconds: 10,
                nanos: -1,
            }),
            Some(yellowstone_grpc_proto::prost_types::Timestamp {
                seconds: 10,
                nanos: 1,
            }),
        ] {
            for block_first in [false, true] {
                let (mut tx, b) = pair(sell, false)?;
                tx.created_at = time;
                let v = if block_first {
                    vec![input(1, block(b)), input(2, tx)]
                } else {
                    vec![input(1, tx), input(2, block(b))]
                };
                let mut v = v;
                v.push(ReplayInput::End(3));
                let (out, failed) = run(policy(), v).await;
                assert!(!failed);
                let a = out
                    .iter()
                    .position(|d| matches!(d.event, DeliveryEvent::Admission(_)))
                    .unwrap();
                let t = out
                    .iter()
                    .position(|d| {
                        matches!(
                            d.event,
                            DeliveryEvent::Terminal {
                                result: Terminal::ProviderAsserted(_),
                                ..
                            }
                        )
                    })
                    .unwrap();
                assert!(a < t);
            }
        }
    }
    for resolved in [false, true] {
        let (tx, b) = pair(true, false)?;
        let mut c = policy();
        c.ingestion
            .yellowstone_association
            .as_mut()
            .unwrap()
            .pending_ttl_ms = 1;
        let mut v = vec![input(0, tx.clone())];
        if resolved {
            v.push(input(1, block(b.clone())));
        } else {
            v.push(ReplayInput::Tick(2_000_000));
        }
        let mut late = b;
        late.blockhash = bs58::encode([90; 32]).into_string();
        v.push(input(3_000_000, block(late)));
        v.push(ReplayInput::End(3_000_001));
        let (out, failed) = run(c, v).await;
        assert!(!failed);
        assert_eq!(
            out.iter()
                .filter(|d| matches!(d.event, DeliveryEvent::Terminal { .. }))
                .count(),
            1
        );
        assert!(out
            .iter()
            .any(|d| matches!(d.event, DeliveryEvent::Late { .. })));
    }
    Ok(())
}
#[tokio::test]
async fn b89_service_duplicate_conflict_reset_and_rejection() -> Result<()> {
    let (tx, b) = pair(true, false)?;
    let mut other = tx.clone();
    tx_mut(&mut other).transaction.as_mut().unwrap().index += 1;
    let (out, failed) = run(
        policy(),
        vec![
            input(1, tx.clone()),
            input(2, tx.clone()),
            input(3, other),
            ReplayInput::Reset(4),
            input(5, tx),
            input(6, block(b)),
            ReplayInput::End(7),
        ],
    )
    .await;
    assert!(!failed);
    assert_eq!(
        out.iter()
            .filter(|d| matches!(d.event, DeliveryEvent::Admission(_)))
            .count(),
        2
    );
    assert_eq!(
        out.iter()
            .filter(|d| matches!(d.event, DeliveryEvent::Duplicate { .. }))
            .count(),
        2
    );
    assert!(out.iter().any(|d| matches!(
        d.event,
        DeliveryEvent::Terminal {
            result: Terminal::Unresolved(Unresolved::ConflictingTransaction),
            ..
        }
    )));
    let (tx, b) = pair(true, false)?;
    let mut c = policy();
    c.ingestion
        .yellowstone_association
        .as_mut()
        .unwrap()
        .blocks
        .bytes = 1;
    let (out, failed) = run(
        c,
        vec![input(1, tx), input(2, block(b)), ReplayInput::End(3)],
    )
    .await;
    assert!(failed);
    assert!(out
        .iter()
        .any(|d| matches!(d.event, DeliveryEvent::Session(SessionGap::Rejected(_)))));
    assert!(out.iter().any(|d| matches!(
        d.event,
        DeliveryEvent::Terminal {
            result: Terminal::Unresolved(Unresolved::EndOfStream),
            ..
        }
    )));
    Ok(())
}
#[test]
#[ignore = "exports small checked fixtures for external app consumer tests"]
fn b89_export_app_fixtures() -> Result<()> {
    let p = std::path::PathBuf::from(std::env::var("B89_FIXTURE_DIR")?);
    std::fs::create_dir_all(&p)?;
    let (mut tx, b) = pair(true, false)?;
    tx.created_at = None;
    for (name, u) in [("missing", tx.clone()), ("block", block(b.clone()))] {
        std::fs::write(p.join(format!("{name}.pb")), u.encode_to_vec())?;
    }
    let (mut known, _) = pair(true, false)?;
    known.created_at = Some(yellowstone_grpc_proto::prost_types::Timestamp {
        seconds: 10,
        nanos: 1,
    });
    std::fs::write(p.join("plus1ns.pb"), known.encode_to_vec())?;
    let mut invalid = tx.clone();
    invalid.created_at = Some(yellowstone_grpc_proto::prost_types::Timestamp {
        seconds: 10,
        nanos: -1,
    });
    std::fs::write(p.join("invalid.pb"), invalid.encode_to_vec())?;
    let mut conflict = tx.clone();
    tx_mut(&mut conflict).transaction.as_mut().unwrap().index += 1;
    std::fs::write(p.join("conflict.pb"), conflict.encode_to_vec())?;
    let f = decode_yellowstone_swap_facts(
        transaction(&tx),
        &YellowstoneGrpcSource::new(&config())?
            .runtime_config
            .interested_program_ids,
        &YellowstoneGrpcSource::new(&config())?
            .runtime_config
            .raydium_program_ids,
        &YellowstoneGrpcSource::new(&config())?
            .runtime_config
            .pumpswap_program_ids,
    )
    .facts?
    .unwrap();
    let c = policy();
    std::fs::write(
        p.join("fixture.json"),
        serde_json::to_vec_pretty(
            &json!({"signature":f.signature,"token":f.token_in,"wallet":f.signer,"programs":c.ingestion.yellowstone_program_ids,"raydium":c.ingestion.raydium_program_ids,"pumpswap":c.ingestion.pumpswap_program_ids}),
        )?,
    )?;
    Ok(())
}

#[tokio::test]
async fn b89_service_all_unresolved_dispositions() -> Result<()> {
    for reason in [
        "expired",
        "capacity",
        "end",
        "reset",
        "association",
        "assertions",
    ] {
        let (tx, b) = pair(true, false)?;
        let mut c = policy();
        let mut v = vec![];
        let expected = match reason {
            "expired" => {
                c.ingestion
                    .yellowstone_association
                    .as_mut()
                    .unwrap()
                    .pending_ttl_ms = 1;
                v.extend([input(0, tx), ReplayInput::Tick(2_000_000)]);
                Unresolved::Expired
            }
            "capacity" => {
                c.ingestion
                    .yellowstone_association
                    .as_mut()
                    .unwrap()
                    .pending
                    .count = 1;
                let mut another = tx.clone();
                tx_mut(&mut another).transaction.as_mut().unwrap().signature = vec![89; 64];
                v.extend([input(0, tx), input(1, another)]);
                Unresolved::PendingCapacity
            }
            "end" => {
                v.push(input(0, tx));
                Unresolved::EndOfStream
            }
            "reset" => {
                v.extend([input(0, tx), ReplayInput::Reset(1)]);
                Unresolved::SessionReset
            }
            "association" => {
                let mut b = b;
                b.transactions[0].index += 1;
                v.extend([input(0, tx), input(1, block(b))]);
                Unresolved::Association("InfoMismatch".into())
            }
            _ => {
                let mut second = b.clone();
                second.blockhash = bs58::encode([90; 32]).into_string();
                v.extend([input(0, block(b)), input(1, block(second)), input(2, tx)]);
                Unresolved::ConflictingAssertions
            }
        };
        v.push(ReplayInput::End(3_000_000));
        let (out, failed) = run(c, v).await;
        assert!(!failed, "{reason}");
        assert!(out.iter().any(|d|matches!(&d.event,DeliveryEvent::Terminal{result:Terminal::Unresolved(r),..} if r==&expected)),"{reason}: {out:?}");
    }
    Ok(())
}
