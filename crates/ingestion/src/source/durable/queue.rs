use anyhow::{ensure, Result};
use copybot_core_types::association_delivery::Delivery;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
/// Permits are held until the app finishes persistence, not just until dequeue.
pub struct DeliveryEnvelope {
    pub delivery: Delivery,
    created_at: Instant,
    _bytes: OwnedSemaphorePermit,
    _count: OwnedSemaphorePermit,
}
impl DeliveryEnvelope {
    /// Includes queue wait, processing and committed acknowledgement latency.
    pub fn elapsed(&self) -> Duration {
        self.created_at.elapsed()
    }
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
    /// Time waiting for charged permits and channel acceptance, after encoding.
    pub(in crate::source) async fn send_timed(&self, delivery: Delivery) -> Result<Duration> {
        let created_at = Instant::now();
        let charge = serde_json::to_vec(&delivery)?
            .len()
            .checked_add(512)
            .ok_or_else(|| anyhow::anyhow!("delivery byte overflow"))?;
        ensure!(
            charge <= self.max_bytes,
            "delivery queue input exceeds byte budget"
        );
        let start = Instant::now();
        let count = self.count.clone().acquire_owned().await?;
        let bytes = self
            .bytes
            .clone()
            .acquire_many_owned(charge.try_into()?)
            .await?;
        self.tx
            .send(DeliveryEnvelope {
                delivery,
                created_at,
                _count: count,
                _bytes: bytes,
            })
            .await
            .map_err(|_| anyhow::anyhow!("delivery consumer closed"))?;
        Ok(start.elapsed())
    }
}
