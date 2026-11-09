//! The actual post-HTTP buffered-frame branch keeps original time, not protected admission.
use super::*;
use prost::Message;
use yellowstone_grpc_proto::{prelude::subscribe_update::UpdateOneof, prost_types::Timestamp};

#[tokio::test]
async fn original_buffered_frame_at_http_anchor_is_only_ordinary_live_with_original_clock(
) -> Result<()> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("capture.db");
    let db = rusqlite::Connection::open(&path)?;
    db.execute_batch(copybot_storage_core::capture_scope::SCHEMA)?;
    db.execute(
        "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,100,1048576)",
        [],
    )?;
    let mut c = IngestionConfig::default();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    c.yellowstone_x_token = "LOCAL-original-buffer".into();
    c.capture_scope_db = Some(path.to_str().unwrap().into());
    let mut update = SubscribeUpdate::decode(
        include_bytes!("../../../app/tests/fixtures/capture/quote_sol_wallet_11_buy.pb").as_slice(),
    )?;
    update.created_at = Some(Timestamp {
        seconds: 1_790_000_000,
        nanos: 123,
    });
    let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() else {
        unreachable!()
    };
    let slot = tx.slot;
    let wallet = bs58::encode(
        &tx.transaction
            .as_ref()
            .unwrap()
            .transaction
            .as_ref()
            .unwrap()
            .message
            .as_ref()
            .unwrap()
            .account_keys[0],
    )
    .into_string();
    let source = crate::source::YellowstoneGrpcSource::new(&c)?;
    let mut runtime = (*source.runtime_config).clone();
    runtime.admission_wallets = Some(HashSet::from([wallet]));
    let (send, mut receive) = queue::channel(16, 1 << 20);
    let mut bridge = Bridge::new(
        &runtime,
        &limits(),
        "LOCAL-buffered".into(),
        send,
        None,
        Arc::new(Default::default()),
    )?;
    bridge.buffered_original_update(1, slot, &update).await?;
    let packet = receive.try_recv().unwrap();
    let DeliveryEvent::OrdinaryLive(ordinary) = packet.delivery.event else {
        panic!("ordinary packet required")
    };
    assert_eq!(
        ordinary.message_time,
        MessageTime::CreatedAt {
            seconds: 1_790_000_000,
            nanos: 123
        }
    );
    assert!(
        receive.try_recv().is_err(),
        "old frame never re-enters protected receipt/admission"
    );
    // A block/HTTP-shaped input cannot use this branch to synthesize a stream clock.
    bridge
        .buffered_original_update(
            2,
            slot,
            &SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Block(block(slot, slot - 1))),
                ..Default::default()
            },
        )
        .await?;
    assert!(receive.try_recv().is_err());
    Ok(())
}
