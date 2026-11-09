//! Saved transaction bodies plus explicitly synthetic full-block fixtures.
//! This tests the production recovery bridge, not historical/live completeness.
#[path = "protected_recovery/e2e.rs"]
mod e2e;
#[path = "protected_recovery/live.rs"]
mod live;
#[path = "protected_recovery/support.rs"]
mod support;
use anyhow::Result;
use copybot_core_types::association_delivery::*;
use copybot_ingestion::{
    protected_capture::{checked_protected_info_equivalent, protected_stream_swap},
    ProtectedReplayInput,
};
use prost::Message;
use serde_json::json;
use yellowstone_grpc_proto::{
    prelude::{subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateTransaction},
    prost_types::Timestamp,
};

#[tokio::test]
async fn all_four_silent_omissions_have_protected_recovery_before_checkpoint() -> Result<()> {
    for row in support::rows() {
        let temp = tempfile::tempdir()?;
        let c = support::config(&temp.path().join("capture.db"))?;
        let (events, error) = support::replay(
            &c,
            &row,
            vec![
                ProtectedReplayInput::Update {
                    offset_ns: 1,
                    payload: support::update(support::block(&row)),
                },
                ProtectedReplayInput::End(2),
            ],
        )
        .await?;
        assert!(error.is_none(), "{error:?}");
        let receipt = events
            .iter()
            .position(|e| matches!(e.event, DeliveryEvent::ProtectedObservation(_)))
            .unwrap();
        let checkpoint = events
            .iter()
            .position(|e| matches!(e.event, DeliveryEvent::ParentCheckpoint(_)))
            .unwrap();
        assert!(receipt < checkpoint);
        let DeliveryEvent::ProtectedObservation(obs) = &events[receipt].event else {
            unreachable!()
        };
        assert_eq!(obs.source_kind, ProtectedSourceKind::CompleteLiveBlock);
        assert_eq!(obs.signature.as_deref(), row["signature"].as_str());
        let expected = support::block(&row);
        let restored = yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo::decode(
            obs.info.encoded.as_slice(),
        )?;
        assert_eq!(
            expected.transactions[0], restored,
            "roundtrip Info identity slot {}",
            row["slot"]
        );
        let facts = obs.facts.as_ref().unwrap_or_else(|| {
            panic!(
                "facts missing slot={} wallet={} info_hash={}",
                row["slot"], obs.wallet, obs.info_hash
            )
        });
        assert_eq!(facts.token_in, row["mint"].as_str().unwrap());
        assert_eq!(
            obs.token_quantity.as_ref().unwrap().raw,
            row["outgoing_raw"].as_str().unwrap()
        );
        assert!(matches!(
            obs.message_time,
            MessageTime::RecoveredBlock { .. }
        ));
        assert!(
            protected_stream_swap(&c, obs).is_err(),
            "historical cannot produce fresh swap"
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e.event, DeliveryEvent::ProtectedObservation(_)))
                .count(),
            1
        );
        let evidence = events
            .iter()
            .find_map(|e| {
                if let DeliveryEvent::ProtectedEvidence(v) = &e.event {
                    Some(v)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(evidence.sha256, obs.evidence_sha256);
        assert!(!evidence.raw.is_empty());
    }
    Ok(())
}
#[tokio::test]
async fn all_four_confirmed_http_bodies_keep_raw_response_and_historical_time() -> Result<()> {
    for row in support::rows() {
        let temp = tempfile::tempdir()?;
        let c = support::config(&temp.path().join("capture.db"))?;
        let raw =
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":support::wrapper(&row)}))?;
        let (events, error) = support::replay(
            &c,
            &row,
            vec![
                ProtectedReplayInput::HttpBlock {
                    offset_ns: 1,
                    slot: row["slot"].as_u64().unwrap(),
                    raw_response: raw.clone(),
                },
                ProtectedReplayInput::End(2),
            ],
        )
        .await?;
        assert!(error.is_none(), "{error:?}");
        let evidence = events
            .iter()
            .find_map(|e| {
                if let DeliveryEvent::ProtectedEvidence(v) = &e.event {
                    Some(v)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(evidence.raw, raw);
        assert_eq!(evidence.source_kind, ProtectedSourceKind::ConfirmedHttp);
        let observation = events
            .iter()
            .find_map(|e| {
                if let DeliveryEvent::ProtectedObservation(v) = &e.event {
                    Some(v)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(observation.evidence_sha256, evidence.sha256);
        assert!(matches!(
            observation.message_time,
            MessageTime::RecoveredBlock { .. }
        ));
        assert!(observation.facts.is_some());
        assert!(protected_stream_swap(&c, observation).is_err());
    }
    Ok(())
}
#[tokio::test]
async fn original_stream_and_block_share_checked_info_without_fabricated_wire_identity(
) -> Result<()> {
    let row = support::rows().remove(0);
    let temp = tempfile::tempdir()?;
    let c = support::config(&temp.path().join("capture.db"))?;
    let block = support::block(&row);
    let stream = SubscribeUpdate {
        created_at: Some(Timestamp {
            seconds: 1790000000,
            nanos: 123,
        }),
        update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            slot: block.slot,
            bank_id: block.bank_id,
            transaction: Some(block.transactions[0].clone()),
        })),
        ..Default::default()
    }
    .encode_to_vec();
    let (events, error) = support::replay(
        &c,
        &row,
        vec![
            ProtectedReplayInput::Update {
                offset_ns: 1,
                payload: stream,
            },
            ProtectedReplayInput::Update {
                offset_ns: 2,
                payload: support::update(block),
            },
            ProtectedReplayInput::End(3),
        ],
    )
    .await?;
    assert!(error.is_none(), "{error:?}");
    let observations: Vec<_> = events
        .iter()
        .filter_map(|e| {
            if let DeliveryEvent::ProtectedObservation(v) = &e.event {
                Some(v)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].info_hash, observations[1].info_hash);
    assert!(checked_protected_info_equivalent(
        &observations[0].info,
        &observations[1].info
    ));
    assert!(protected_stream_swap(&c, observations[0])?.is_some());
    assert!(protected_stream_swap(&c, observations[1]).is_err());
    assert_ne!(
        observations[0].evidence_sha256,
        observations[1].evidence_sha256
    );
    Ok(())
}
#[tokio::test]
async fn incomplete_indices_and_changed_known_info_refuse_checkpoint() -> Result<()> {
    let row = support::rows().remove(0);
    for mode in ["count", "index", "conflict"] {
        let temp = tempfile::tempdir()?;
        let c = support::config(&temp.path().join("capture.db"))?;
        let mut block = support::block(&row);
        let mut inputs = vec![];
        if mode == "count" {
            block.executed_transaction_count = 2;
        }
        if mode == "index" {
            block.transactions[0].index = 1;
        }
        if mode == "conflict" {
            let stream = SubscribeUpdate {
                created_at: Some(Timestamp {
                    seconds: 1790000000,
                    nanos: 0,
                }),
                update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    slot: block.slot,
                    bank_id: 0,
                    transaction: Some(block.transactions[0].clone()),
                })),
                ..Default::default()
            };
            inputs.push(ProtectedReplayInput::Update {
                offset_ns: 1,
                payload: stream.encode_to_vec(),
            });
            block.transactions[0].meta.as_mut().unwrap().fee += 1;
        }
        inputs.push(ProtectedReplayInput::Update {
            offset_ns: 2,
            payload: support::update(block),
        });
        inputs.push(ProtectedReplayInput::End(3));
        let (events, error) = support::replay(&c, &row, inputs).await?;
        assert!(error.is_some(), "{mode}");
        assert!(
            !events
                .iter()
                .any(|e| matches!(e.event, DeliveryEvent::ParentCheckpoint(_))),
            "{mode}"
        );
    }
    Ok(())
}
