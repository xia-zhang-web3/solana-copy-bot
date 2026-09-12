//! Actual app delivery consumer. One outstanding SQLite task survives select
//! cancellation. Admission snapshots execute synchronously at first app dequeue;
//! the write then awaits off-thread without blocking maintenance branches.
use anyhow::{Context, Result};
use copybot_config::IngestionConfig;
use copybot_core_types::association_delivery::{CandidateGeneration, DeliveryEvent};
use copybot_ingestion::{DeliveryEnvelope, DeliveryReceiver, IngestionService};
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    SqliteStore,
};
use tokio::task::JoinHandle;

#[path = "association_shadow_wake.rs"]
pub(crate) mod shadow_wake;
use shadow_wake::ShadowWake;

type Pending = JoinHandle<(AssociationInbox, Option<DeliveryEnvelope>, Result<bool>)>;
pub(crate) struct AssociationConsumer {
    receiver: DeliveryReceiver,
    inbox: Option<AssociationInbox>,
    recovery_pending: bool,
    pub(crate) shadow_wake: ShadowWake,
    pub(crate) pending: Option<Pending>,
}
impl AssociationConsumer {
    pub(crate) async fn start(
        ingestion: &mut IngestionService,
        c: &IngestionConfig,
        path: &str,
    ) -> Result<Option<Self>> {
        if c.yellowstone_delivery_mode != "durable_association_v1" {
            return Ok(None);
        }
        let l = c
            .yellowstone_association
            .as_ref()
            .context("delivery limits")?;
        let limits = InboxLimits {
            count: l.inbox.count,
            bytes: l.inbox.bytes,
            busy_ms: l.sqlite_busy_ms,
        };
        let path = path.to_owned();
        // This durable mode selects provider_order_strict_v1. Schema/recovery
        // must succeed before opening a provider connection. Legacy stays separate.
        let inbox = tokio::task::spawn_blocking(move || {
            AssociationInbox::open_ordered_sell_consumer(path, limits)
        })
        .await??;
        let recovery_pending = inbox.has_sell_preparation_work()?;
        let receiver = ingestion
            .take_delivery(AssociationInbox::new_session_id())?
            .context("missing delivery receiver")?;
        Ok(Some(Self {
            receiver,
            inbox: Some(inbox),
            recovery_pending,
            shadow_wake: ShadowWake::new(limits),
            pending: None,
        }))
    }
    pub(crate) async fn poll(&mut self, store: &SqliteStore) -> Result<()> {
        if self.pending.is_none() {
            // Ready delivery has priority; otherwise resume one durable dependency
            // through the same cancellation-safe pending writer.
            let envelope = tokio::select! {
                biased;
                next = self.receiver.next() => next?,
                _ = self.shadow_wake.notified() => {
                    self.recovery_pending = true;
                    None
                },
                _ = std::future::ready(()), if self.recovery_pending => None,
            };
            if envelope.is_none() && !self.recovery_pending {
                anyhow::bail!("association delivery stopped");
            }
            let candidate = match envelope.as_ref().map(|e| &e.delivery.event) {
                Some(DeliveryEvent::Admission(a)) => store.association_candidate(&a.facts),
                _ => CandidateGeneration::Unknown,
            };
            let observed = chrono::Utc::now();
            let mut inbox = self.inbox.take().context("inbox unavailable")?;
            self.pending = Some(tokio::task::spawn_blocking(move || {
                let result = (|| {
                    match &envelope {
                        Some(e) => inbox.persist_at(&e.delivery, &candidate, observed)?,
                        None => inbox.recover_sell_preparation()?,
                    }
                    inbox.has_sell_preparation_work()
                })();
                (inbox, envelope, result)
            }));
        }
        let completed = self.pending.as_mut().expect("pending write").await;
        self.pending = None;
        let (inbox, envelope, result) = match completed {
            Ok(v) => v,
            Err(error) => {
                self.receiver.stop();
                return Err(error).context("inbox writer failed");
            }
        };
        // A resource permit is not a success ACK. On failure close/abort intake
        // before releasing the failed envelope or awaiting unrelated shutdown.
        if let Err(error) = &result {
            self.receiver.stop();
            return Err(anyhow::anyhow!(
                "association inbox failed: stop new delivery path: {error:#}"
            ));
        }
        // A stale blocking result must not consume a newer close notification.
        // Notify retains its permit across this ACK and select cancellation.
        self.recovery_pending = result?;
        self.inbox = Some(inbox);
        drop(envelope);
        Ok(())
    }
}
pub(crate) async fn poll(
    consumer: &mut Option<AssociationConsumer>,
    store: &SqliteStore,
) -> Result<()> {
    match consumer {
        Some(c) => c.poll(store).await,
        None => std::future::pending().await,
    }
}
