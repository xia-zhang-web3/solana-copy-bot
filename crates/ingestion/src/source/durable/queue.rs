use anyhow::{ensure, Result};
use copybot_core_types::association_delivery::Delivery;
use std::sync::Arc;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
/// Permits are held until the app finishes persistence, not just until dequeue.
pub struct DeliveryEnvelope {
    pub delivery: Delivery,
    _bytes: OwnedSemaphorePermit,
    _count: OwnedSemaphorePermit,
}
pub(in crate::source) struct Sender {
    tx: mpsc::Sender<DeliveryEnvelope>,
    bytes: Arc<Semaphore>,
    count: Arc<Semaphore>,
    max_bytes: usize,
}
pub(in crate::source) fn channel(
    count: usize,
    bytes: usize,
) -> (Sender, mpsc::Receiver<DeliveryEnvelope>) {
    let (tx, rx) = mpsc::channel(count);
    (
        Sender {
            tx,
            bytes: Arc::new(Semaphore::new(bytes)),
            count: Arc::new(Semaphore::new(count)),
            max_bytes: bytes,
        },
        rx,
    )
}
impl Sender {
    pub(in crate::source) async fn send(&self, delivery: Delivery) -> Result<()> {
        let charge = serde_json::to_vec(&delivery)?
            .len()
            .checked_add(512)
            .ok_or_else(|| anyhow::anyhow!("delivery byte overflow"))?;
        ensure!(
            charge <= self.max_bytes,
            "delivery queue input exceeds byte budget"
        );
        let count = self.count.clone().acquire_owned().await?;
        let bytes = self
            .bytes
            .clone()
            .acquire_many_owned(charge.try_into()?)
            .await?;
        self.tx
            .send(DeliveryEnvelope {
                delivery,
                _count: count,
                _bytes: bytes,
            })
            .await
            .map_err(|_| anyhow::anyhow!("delivery consumer closed"))
    }
}
