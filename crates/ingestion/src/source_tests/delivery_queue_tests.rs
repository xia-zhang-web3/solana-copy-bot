use crate::source::durable::queue;
use copybot_core_types::association_delivery::*;
use std::future::Future;
fn d() -> Delivery {
    Delivery {
        session: "q".into(),
        sequence: 0,
        arrival_offset_ns: 0,
        event: DeliveryEvent::Session(SessionGap::StartedContinuityUnknown),
    }
}
#[tokio::test]
async fn b89_queue_count_bytes_n_n_plus_one_hold_until_ack() {
    let charge = serde_json::to_vec(&d()).unwrap().len() + 512;
    for (count, bytes) in [(1, charge * 2), (2, charge)] {
        let (tx, mut rx) = queue::channel(count, bytes);
        tx.send(d()).await.unwrap();
        let envelope = rx.recv().await.unwrap();
        let mut send = Box::pin(tx.send(d()));
        std::future::poll_fn(|cx| {
            assert!(send.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(envelope);
        send.await.unwrap();
        assert!(rx.recv().await.is_some());
    }
    let (tx, _rx) = queue::channel(1, charge - 1);
    assert!(tx.send(d()).await.is_err());
}
