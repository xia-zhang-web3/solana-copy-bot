//! Already charged, ordered envelopes survive sequential source replacement.
use super::*;
use crate::source::durable::DeliveryReceiver;
use copybot_core_types::association_delivery::{Delivery, OrdinaryLive};
use prost::Message;

async fn emitted_receiver(fail: bool) -> Result<DeliveryReceiver> {
    let mut config = IngestionConfig::default();
    config.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    config.yellowstone_x_token = "LOCAL-quiesce".into();
    let mut update = SubscribeUpdate::decode(
        include_bytes!("../../../app/tests/fixtures/capture/quote_sol_wallet_11_buy.pb").as_slice(),
    )?;
    update.created_at = Some(yellowstone_grpc_proto::prost_types::Timestamp {
        seconds: 1_790_000_000,
        nanos: 0,
    });
    let (_, observation) = crate::protected_capture::reconstruct_stream(
        &config,
        &update.encode_to_vec(),
        1_790_000_000_000_000,
    )?;
    let ordinary = OrdinaryLive {
        facts: observation.facts.unwrap(),
        message_time: observation.message_time,
        info_hash: observation.info_hash,
    };
    let (send, rx) = queue::channel(4, 1 << 20);
    let (ready, emitted) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        for (sequence, event) in [
            DeliveryEvent::Session(SessionGap::StartedContinuityUnknown),
            DeliveryEvent::OrdinaryLive(ordinary),
            DeliveryEvent::Session(SessionGap::Reset),
        ]
        .into_iter()
        .enumerate()
        {
            send.send_timed(Delivery {
                session: "LOCAL-quiesce".into(),
                sequence: sequence as u64,
                arrival_offset_ns: sequence as u64,
                event,
            })
            .await?;
        }
        ready.send(()).unwrap();
        if fail {
            anyhow::bail!("known producer failure before quiescence");
        }
        std::future::pending::<Result<()>>().await
    });
    emitted.await?;
    Ok(DeliveryReceiver {
        rx,
        task: Some(task),
        wallet_scope: None,
        bot_signer: None,
        telemetry: Arc::new(Default::default()),
        recovery: None,
    })
}

#[tokio::test]
async fn quiesce_preserves_already_emitted_ordinary_and_other_envelopes_in_order() -> Result<()> {
    let mut receiver = emitted_receiver(false).await?;
    assert!(
        receiver.try_next_buffered().is_err(),
        "producer must quiesce before drain"
    );
    receiver.quiesce().await?;
    let mut messages = vec![];
    while let Some(envelope) = receiver.try_next_buffered()? {
        messages.push(envelope);
    }
    assert_eq!(
        messages
            .iter()
            .map(|m| m.delivery.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert!(matches!(
        messages[0].delivery.event,
        DeliveryEvent::Session(_)
    ));
    assert!(matches!(
        messages[1].delivery.event,
        DeliveryEvent::OrdinaryLive(_)
    ));
    assert!(matches!(
        messages[2].delivery.event,
        DeliveryEvent::Session(SessionGap::Reset)
    ));
    let mut old_stop = emitted_receiver(false).await?;
    old_stop.stop();
    assert!(
        old_stop.try_next_buffered()?.is_none(),
        "old stop remains abort-and-drop"
    );
    Ok(())
}

#[tokio::test]
async fn quiesce_retains_an_already_completed_producer_error() -> Result<()> {
    let mut receiver = emitted_receiver(true).await?;
    while !receiver.task.as_ref().unwrap().is_finished() {
        tokio::task::yield_now().await;
    }
    let error = receiver.quiesce().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("known producer failure before quiescence"));
    Ok(())
}
