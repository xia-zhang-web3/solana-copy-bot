//! One observation actor owns the full-block receiver and both durable stores.
//! App select cancellation never replaces a dequeued, unacknowledged envelope.
use crate::app_live_ingress::LiveEnvelope;
use anyhow::{Context, Result};
use copybot_config::IngestionConfig;
use copybot_ingestion::{protected_delivery::ProtectedDelivery, DeliveryReceiver};
use copybot_storage_core::{association_inbox::AssociationInbox, capture_scope::CaptureStore};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;

struct LiveOutput {
    tx: mpsc::Sender<LiveEnvelope>,
    bytes: Arc<Semaphore>,
    bound: usize,
}
impl LiveOutput {
    async fn send(&self, swap: copybot_core_types::SwapEvent) -> Result<()> {
        let charge = serde_json::to_vec(&swap)?
            .len()
            .checked_add(512)
            .context("ordinary live charge overflow")?;
        anyhow::ensure!(
            charge <= self.bound,
            "ordinary live output exceeds byte bound"
        );
        let permit = self
            .bytes
            .clone()
            .acquire_many_owned(charge.try_into()?)
            .await?;
        self.tx
            .send(LiveEnvelope {
                swap,
                _permit: permit,
            })
            .await
            .map_err(|_| anyhow::anyhow!("ordinary live app consumer closed"))
    }
}

pub(crate) struct ProtectedConsumer {
    task: JoinHandle<Result<()>>,
    live: mpsc::Receiver<LiveEnvelope>,
}
impl ProtectedConsumer {
    pub(crate) fn start(config: &IngestionConfig) -> Result<Option<Self>> {
        if config.capture_recovery.is_none() {
            return Ok(None);
        }
        copybot_config::validate_delivery_source(config)?;
        let bytes = config
            .yellowstone_association
            .as_ref()
            .context("ordinary live limits")?
            .outputs
            .bytes;
        anyhow::ensure!(
            config.output_queue_capacity > 0 && bytes > 0 && bytes <= u32::MAX as usize,
            "ordinary live explicit count/byte bounds"
        );
        let (tx, live) = mpsc::channel(config.output_queue_capacity);
        let output = LiveOutput {
            tx,
            bytes: Arc::new(Semaphore::new(bytes)),
            bound: bytes,
        };
        let config = config.clone();
        Ok(Some(Self {
            task: tokio::spawn(run(config, output)),
            live,
        }))
    }
    pub(crate) fn parts(
        &mut self,
    ) -> (
        &mut JoinHandle<Result<()>>,
        &mut mpsc::Receiver<LiveEnvelope>,
    ) {
        (&mut self.task, &mut self.live)
    }
}
impl Drop for ProtectedConsumer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub(crate) async fn poll(task: Option<&mut JoinHandle<Result<()>>>) -> Result<()> {
    match task {
        Some(task) => {
            task.await.context("protected capture actor panicked")??;
            anyhow::bail!("protected capture actor unexpectedly stopped")
        }
        None => std::future::pending().await,
    }
}
pub(crate) fn source_quiesced(store: &mut CaptureStore, result: Result<()>) -> Result<()> {
    if let Err(error) = result {
        let _ = store.fail_closed("protected_source_quiesce_failed");
        return Err(error);
    }
    Ok(())
}
async fn blocking<T: Send + 'static>(
    state: ProtectedDelivery,
    work: impl FnOnce(&mut ProtectedDelivery) -> Result<T> + Send + 'static,
) -> Result<(ProtectedDelivery, T)> {
    let (state, result) = tokio::task::spawn_blocking(move || {
        let mut state = state;
        let result = work(&mut state);
        (state, result)
    })
    .await
    .context("protected writer task failed")?;
    Ok((state, result?))
}
async fn run(config: IngestionConfig, live: LiveOutput) -> Result<()> {
    let setup = config.clone();
    let mut store = tokio::task::spawn_blocking(move || {
        let mut store = CaptureStore::open(std::path::Path::new(
            setup
                .capture_scope_db
                .as_deref()
                .context("protected capture path")?,
        ))?;
        store.start()?;
        copybot_ingestion::protected_delivery::restore_stream_links(&setup, &mut store)?;
        Ok::<_, anyhow::Error>(store)
    })
    .await??;
    loop {
        // Durable old outbox must finish before changing its protecting revision.
        while !store.verify_forward_dispositions()? {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let planned = store.planned_scope()?;
        let scope = match planned {
            Some(scope) => scope,
            None if store.needs_transition()? => store.prepare_scope()?,
            None => store
                .active_scope()?
                .context("protected active scope missing")?,
        };
        let prior_anchor = store.latest_checkpoint()?;
        let mut state = ProtectedDelivery::open(&config, store, scope)?;
        let restored = state.restored_checkpoint()?;
        let anchor = if restored.is_none() {
            prior_anchor
        } else {
            None
        };
        let mut receiver = DeliveryReceiver::start_protected_transition(
            &config,
            AssociationInbox::new_session_id(),
            state.scope.wallets.iter().cloned().collect(),
            None,
            restored,
            anchor,
        )?;
        loop {
            let envelope = receiver.next().await?.context("protected source ended")?;
            let boundary = matches!(
                &envelope.delivery.event,
                copybot_core_types::association_delivery::DeliveryEvent::ParentCheckpoint(_)
            );
            state = consume(state, envelope, &receiver, &live).await?;
            if boundary
                && state.store.needs_transition()?
                && !state.store.recovery_outbox_pending()?
            {
                // Sequential replacement: the old source is stopped before a new
                // immutable scope opens its one provider connection.
                source_quiesced(&mut state.store, receiver.quiesce().await)?;
                while let Some(envelope) = receiver.try_next_buffered()? {
                    state = consume(state, envelope, &receiver, &live).await?;
                }
                store = state.store;
                break;
            }
        }
    }
}

async fn consume(
    mut state: ProtectedDelivery,
    envelope: copybot_ingestion::DeliveryEnvelope,
    receiver: &DeliveryReceiver,
    live: &LiveOutput,
) -> Result<ProtectedDelivery> {
    if let copybot_core_types::association_delivery::DeliveryEvent::OrdinaryLive(packet) =
        &envelope.delivery.event
    {
        let packet = packet.clone();
        let (next, swap) = blocking(state, move |s| s.ordinary_live(&packet)).await?;
        state = next;
        if let Some(swap) = swap {
            if let Err(error) = live.send(swap).await {
                let _ = state.store.fail_closed("ordinary_live_output_failed");
                return Err(error);
            }
        }
        receiver.acknowledge_delivery(envelope.elapsed());
        drop(envelope);
        return Ok(state);
    }
    let delivery = envelope.delivery.clone();
    let (next, head) = blocking(state, move |s| s.persist(&delivery)).await?;
    state = next;
    if let Some(head) = head {
        loop {
            let checkpoint = head.clone();
            let (next, committed) = blocking(state, move |s| {
                s.commit_checkpoint(
                    &checkpoint,
                    chrono::Utc::now().timestamp_micros() as f64 / 1e6,
                )
            })
            .await?;
            state = next;
            if committed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        receiver.acknowledge_checkpoint(head)?;
    }
    if let Some(parent) = envelope.delivery.event.parent_observation() {
        receiver.acknowledge_parent(parent.child.slot);
    }
    receiver.acknowledge_delivery(envelope.elapsed());
    drop(envelope);
    Ok(state)
}
