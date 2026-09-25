//! Actual app delivery consumer. One outstanding SQLite task survives select
//! cancellation. Admission snapshots execute synchronously at first app dequeue;
//! the write then awaits off-thread without blocking maintenance branches.
use anyhow::{Context, Result};
use copybot_config::{ExecutionConfig, IngestionConfig};
use copybot_core_types::association_delivery::{CandidateGeneration, DeliveryEvent, SessionGap};
use copybot_ingestion::{DeliveryEnvelope, DeliveryReceiver, IngestionService};
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    SqliteStore,
};
use tokio::task::JoinHandle;
use crate::execution_native_buy_rpc;
use crate::execution_technical_cohort;
use crate::execution_owned_sell_rpc::fractional::transport::{Http, Transport};
use copybot_storage_core::native_buy::NativeBuyFence;

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
    // Own the dequeued envelope before any external fence await. A cancelled
    // app-loop poll resumes this exact session without ACK or replacement.
    intake: Option<(Option<DeliveryEnvelope>, CandidateGeneration, chrono::DateTime<chrono::Utc>)>,
    native_buy: Option<ExecutionConfig>,
    native_http: Option<reqwest::Client>,
    cohort_deadline: Option<chrono::DateTime<chrono::Utc>>,
    active_session: Option<String>,
    pub(crate) next_fence_at: Option<tokio::time::Instant>,
    epoch_intake: Option<NativeBuyFence>,
    pending_epoch: bool,
}
impl AssociationConsumer {
    pub(crate) async fn start(
        ingestion: &mut IngestionService,
        c: &IngestionConfig,
        path: &str,
    ) -> Result<Option<Self>> {
        Self::start_inner(ingestion, c, None, path).await
    }
    async fn start_inner(
        ingestion: &mut IngestionService,
        c: &IngestionConfig,
        execution: Option<&ExecutionConfig>,
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
        let authority = execution.map(execution_technical_cohort::authority)
            .transpose()?.flatten();
        let deadline = authority.as_ref().map(|a| a.deadline);
        let admission_wallets = authority.as_ref()
            .map(|a| execution.context("cohort execution config")
                .and_then(|c| execution_technical_cohort::admission_wallets(a, c)))
            .transpose()?;
        let inbox = tokio::task::spawn_blocking(move || {
            let mut inbox = AssociationInbox::open_ordered_sell_consumer(path, limits)?;
            if let Some(authority) = authority.as_ref() {
                inbox.register_technical_cohort_authority(authority)?;
            }
            Ok::<_, anyhow::Error>(inbox)
        })
        .await??;
        let recovery_pending = inbox.has_sell_preparation_work()?;
        let receiver = ingestion
            .take_delivery_scoped(AssociationInbox::new_session_id(), admission_wallets)?
            .context("missing delivery receiver")?;
        Ok(Some(Self {
            receiver,
            inbox: Some(inbox),
            recovery_pending,
            shadow_wake: ShadowWake::new(limits),
            pending: None,
            intake: None,
            native_buy: None,
            native_http: None,
            cohort_deadline: deadline,
            active_session: None,
            next_fence_at: None,
            epoch_intake: None,
            pending_epoch: false,
        }))
    }
    pub(crate) async fn start_with_execution(
        ingestion: &mut IngestionService,
        c: &IngestionConfig,
        execution: &ExecutionConfig,
        path: &str,
    ) -> Result<Option<Self>> {
        let mut consumer = Self::start_inner(ingestion, c, Some(execution), path).await?;
        if let Some(value) = consumer.as_mut() {
            if execution_native_buy_rpc::enabled(execution) {
                execution_native_buy_rpc::policy_identity(execution)?;
                value.native_buy = Some(execution.clone());
                value.native_http = Some(reqwest::Client::new());
            }
        }
        Ok(consumer)
    }
    pub(crate) async fn poll(&mut self, store: &SqliteStore) -> Result<()> {
        let Some(c) = self.native_buy.clone() else {
            return self.poll_with_transport(store, None).await;
        };
        let client = self.native_http.clone().context("native buy HTTP client missing")?;
        let mut rpc = Http {
            http: &client,
            config: &c,
            url: crate::execution_owned_sell_rpc::endpoint(&c)?,
            budget: Default::default(),
        };
        self.poll_with_transport(store, Some(&mut rpc)).await
    }
    pub(crate) async fn poll_with_transport(
        &mut self,
        store: &SqliteStore,
        mut rpc: Option<&mut dyn Transport>,
    ) -> Result<()> {
        if self.pending.is_none() && self.intake.is_none() && self.epoch_intake.is_none() {
            // Ready delivery has priority; otherwise resume one durable dependency
            // through the same cancellation-safe pending writer.
            let next_fence_at = self.next_fence_at;
            let mut epoch_due = false;
            let envelope = tokio::select! {
                biased;
                next = self.receiver.next() => next?,
                _ = async {
                    if let Some(when) = next_fence_at {
                        tokio::time::sleep_until(when).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => { epoch_due = true; None },
                _ = self.shadow_wake.notified() => {
                    self.recovery_pending = true;
                    None
                },
                _ = std::future::ready(()), if self.recovery_pending => None,
            };
            if epoch_due {
                if self.cohort_deadline.is_some_and(|d| chrono::Utc::now() >= d) {
                    self.next_fence_at = None;
                    return Ok(());
                }
                let c = self.native_buy.as_ref().context("cohort_native_buy_missing")?;
                let session = self.active_session.as_deref().context("cohort_session_missing")?;
                let rpc = rpc.as_deref_mut().context("native_buy_transport_missing")?;
                let mut check = || {
                    execution_technical_cohort::before_deadline(c)?;
                    anyhow::ensure!(!std::path::Path::new(&c.canary_kill_switch_path).exists(),
                        "native_buy_kill_switch");
                    Ok(())
                };
                let evidence = execution_native_buy_rpc::fence(rpc, c, session, &mut check).await?;
                self.epoch_intake = Some(NativeBuyFence {
                    session: evidence.session, processed_slot: evidence.processed_slot,
                    sampled_at: evidence.sampled_at, genesis_hash: evidence.genesis_hash,
                    policy_identity: evidence.policy_identity,
                });
            } else if envelope.is_none() && !self.recovery_pending {
                anyhow::bail!("association delivery stopped");
            }
            if !epoch_due {
                let candidate = match envelope.as_ref().map(|e| &e.delivery.event) {
                    Some(DeliveryEvent::Admission(a)) => store.association_candidate(&a.facts),
                    _ => CandidateGeneration::Unknown,
                };
                self.intake = Some((envelope, candidate, chrono::Utc::now()));
            }
        }
        if self.pending.is_none() {
            if let Some(epoch) = self.epoch_intake.take() {
                let mut inbox = self.inbox.take().context("inbox unavailable")?;
                self.pending_epoch = true;
                self.pending = Some(tokio::task::spawn_blocking(move || {
                    let result = (|| {
                        inbox.record_native_buy_fence_epoch(&epoch)?;
                        inbox.has_sell_preparation_work()
                    })();
                    (inbox, None, result)
                }));
            } else {
            let cohort_mode = self.cohort_deadline.is_some();
            let envelope = self.intake.as_ref().and_then(|(e, _, _)| e.as_ref());
            let fence = match (self.native_buy.as_ref(), envelope) {
                (Some(_), Some(e)) if cohort_mode
                    && self.cohort_deadline.is_some_and(|d| chrono::Utc::now() >= d)
                    && matches!(&e.delivery.event, DeliveryEvent::Session(SessionGap::StartedContinuityUnknown)) => None,
                (Some(c), Some(e)) if matches!(&e.delivery.event, DeliveryEvent::Session(SessionGap::StartedContinuityUnknown)) => {
                    let rpc = rpc.as_deref_mut().context("native_buy_transport_missing")?;
                    let mut check = || {
                        if cohort_mode {
                            execution_technical_cohort::before_deadline(c)?;
                        }
                        anyhow::ensure!(
                            !std::path::Path::new(&c.canary_kill_switch_path).exists(),
                            "native_buy_kill_switch"
                        );
                        Ok(())
                    };
                    let evidence = execution_native_buy_rpc::fence(rpc, c, &e.delivery.session, &mut check).await?;
                    Some(NativeBuyFence {
                        session: evidence.session,
                        processed_slot: evidence.processed_slot,
                        sampled_at: evidence.sampled_at,
                        genesis_hash: evidence.genesis_hash,
                        policy_identity: evidence.policy_identity,
                    })
                }
                _ => None,
            };
            let (envelope, candidate, observed) = self.intake.take().context("intake missing")?;
            let mut inbox = self.inbox.take().context("inbox unavailable")?;
            self.pending = Some(tokio::task::spawn_blocking(move || {
                let result = (|| {
                    match &envelope {
                        Some(e) => {
                            inbox.persist_at(&e.delivery, &candidate, observed)?;
                            if let Some(ref fence) = fence {
                                if cohort_mode {
                                    inbox.record_native_buy_fence_epoch(fence)?;
                                } else {
                                    inbox.record_native_buy_fence(fence)?;
                                }
                            }
                        }
                        None => inbox.recover_sell_preparation()?,
                    }
                    inbox.has_sell_preparation_work()
                })();
                (inbox, envelope, result)
            }));
            }
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
        if self.pending_epoch {
            self.pending_epoch = false;
            self.next_fence_at = Some(tokio::time::Instant::now()
                + std::time::Duration::from_secs(60));
        } else if self.cohort_deadline.is_some() {
            match envelope.as_ref().map(|e| &e.delivery.event) {
                Some(DeliveryEvent::Session(SessionGap::StartedContinuityUnknown)) => {
                    self.active_session = envelope.as_ref().map(|e| e.delivery.session.clone());
                    self.next_fence_at = self.cohort_deadline
                        .filter(|d| chrono::Utc::now() < *d)
                        .map(|_| tokio::time::Instant::now()
                            + std::time::Duration::from_secs(60));
                }
                Some(DeliveryEvent::Session(_)) => {
                    self.active_session = None;
                    self.next_fence_at = None;
                }
                _ => {}
            }
        }
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
