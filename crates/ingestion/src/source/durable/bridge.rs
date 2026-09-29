use super::super::{yellowstone_association as a, YellowstoneRuntimeConfig};
use super::{convert, queue::Sender, telemetry::DurableIngressTelemetry};
use a::limits::Budget;
use anyhow::Result;
use copybot_config::AssociationDeliveryConfig;
use copybot_core_types::association_delivery::*;
use std::{sync::Arc, time::Duration};
#[path = "checkpoint.rs"]
mod checkpoint;
pub(super) struct Bridge<'a> {
    adapter: a::YellowstoneAssociation<'a>,
    session: a::Session,
    name: String,
    base_name: String,
    sequence: u64,
    tx: Sender,
    bot_signer: Option<String>,
    telemetry: Arc<DurableIngressTelemetry>,
    scoped: bool,
    recovery: Option<checkpoint::BlockRecovery>,
    capture_scope: Option<Arc<super::capture_scope::CaptureScope>>,
}
impl<'a> Bridge<'a> {
    pub(super) fn new(
        runtime: &'a YellowstoneRuntimeConfig,
        c: &AssociationDeliveryConfig,
        name: String,
        tx: Sender,
        bot_signer: Option<String>,
        telemetry: Arc<DurableIngressTelemetry>,
    ) -> Result<Self> {
        let b = |v: &copybot_config::DeliveryBudget| Budget {
            count: v.count,
            encoded_bytes: v.bytes,
        };
        let session = a::Session {
            id: [0; 16],
            generation: 0,
        };
        let mut adapter = a::YellowstoneAssociation::new(
            session,
            a::Limits {
                pending: b(&c.pending),
                blocks: b(&c.blocks),
                history: b(&c.history),
                outputs: b(&c.outputs),
                input_bytes: c.input_bytes,
                metadata_bytes: c.metadata_bytes,
                pending_ttl: Duration::from_millis(c.pending_ttl_ms),
                block_ttl: Duration::from_millis(c.block_ttl_ms),
                history_ttl: Duration::from_millis(c.history_ttl_ms),
            },
            a::Programs {
                interested: &runtime.interested_program_ids,
                raydium: &runtime.raydium_program_ids,
                pumpswap: &runtime.pumpswap_program_ids,
            },
        )
        .map_err(|e| anyhow::anyhow!("association limits {e:?}"))?;
        if let Some(wallets) = runtime.admission_wallets.as_ref() {
            anyhow::ensure!(!wallets.is_empty(), "empty admission wallet scope");
            adapter.restrict_wallets(wallets);
        }
        Ok(Self {
            adapter,
            session,
            name: format!("{name}:0"),
            base_name: name,
            sequence: 0,
            tx,
            bot_signer,
            telemetry,
            scoped: runtime.admission_wallets.is_some(),
            recovery: None,
            capture_scope: None,
        })
    }
    pub(super) async fn emit(&mut self, ns: u64, event: DeliveryEvent) -> Result<()> {
        if matches!(&event, DeliveryEvent::Session(gap)
            if !matches!(gap, SessionGap::StartedContinuityUnknown))
        {
            self.set_http_hold(true);
        }
        let parent_slot = event.parent_observation().map(|parent| parent.child.slot);
        let d = Delivery {
            session: self.name.clone(),
            sequence: self.sequence,
            arrival_offset_ns: ns,
            event,
        };
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("delivery sequence exhausted"))?;
        let wait = self.tx.send_timed(d).await?;
        self.telemetry.queue_wait(wait);
        if let Some(slot) = parent_slot {
            self.telemetry.parent(slot);
        }
        Ok(())
    }
    pub(super) async fn push(&mut self, ns: u64, input: a::Input<'_>) -> Result<()> {
        let context = a::Context {
            session: self.session,
            offset: Duration::from_nanos(ns),
        };
        let duplicate_input = match &input {
            a::Input::Transaction(tx, time) => Some((*tx, *time)),
            _ => None,
        };
        let parent_input = match &input {
            a::Input::Block(block) => Some(*block),
            _ => None,
        };
        let stage = std::time::Instant::now();
        let result = self.adapter.push(context, input);
        self.telemetry.processing.association(stage.elapsed());
        let (cached_count, cached_bytes) = self.adapter.block_cache_usage();
        self.telemetry
            .processing
            .block_cache(cached_count, cached_bytes);
        if let Some(scope) = &self.capture_scope {
            scope.replace_known(self.adapter.known_signatures_bytes());
        }
        let admission = match result {
            Ok(v) => v,
            Err(r) => {
                self.telemetry
                    .rejected(matches!(r, a::Rejection::FactsDecodeError));
                self.emit(
                    ns,
                    DeliveryEvent::Session(SessionGap::Rejected(format!("{r:?}"))),
                )
                .await?;
                // Rejected input has NOT been processed. Stop this path after recording
                // discontinuity and draining End dispositions; never retry silently.
                self.adapter
                    .push(context, a::Input::End)
                    .map_err(|e| anyhow::anyhow!("end after rejection {e:?}"))?;
                self.drain(ns).await?;
                anyhow::bail!("association input rejected: {r:?}");
            }
        };
        let bot = match &admission {
            a::Admission::Transaction(id) | a::Admission::Duplicate(id) => {
                self.adapter.admitted(*id).is_some_and(|(checked, _)| {
                    self.bot_signer.as_deref() == Some(checked.facts.signer.as_str())
                })
            }
            _ => false,
        };
        self.telemetry.admission(&admission, bot, self.scoped);
        match admission {
            a::Admission::Block => {
                let parent = super::parent::observation(parent_input.expect("admitted block"));
                if self.recovery.is_none() {
                    self.emit(ns, DeliveryEvent::Parent(parent)).await?;
                }
            }
            a::Admission::Transaction(id) => {
                let (c, i) = self
                    .adapter
                    .admitted(id)
                    .expect("successful admission retained");
                let event = DeliveryEvent::Admission(convert::admission(c, i));
                self.emit(ns, event).await?;
            }
            a::Admission::Duplicate(id) => {
                let (c, i) = self.adapter.admitted(id).expect("retained duplicate");
                let original = convert::admission(c, i);
                let (tx, time) = duplicate_input.expect("duplicate tx");
                let event = DeliveryEvent::Duplicate {
                    original,
                    observed_info: convert::info(tx.transaction.as_ref().expect("admitted Info")),
                    observed_slot: tx.slot,
                    message_time: convert::message_time(time),
                };
                self.emit(ns, event).await?;
            }
            _ => {}
        }
        self.drain(ns).await
    }
    async fn drain(&mut self, ns: u64) -> Result<()> {
        loop {
            let batch = self.adapter.drain();
            for o in batch.outcomes {
                let e = match o {
                    a::Outcome::Terminal {
                        checked,
                        resolution,
                        info,
                        ..
                    } => DeliveryEvent::Terminal {
                        signature: checked.facts.signature.clone(),
                        expected: convert::admission(&checked, &info),
                        result: convert::terminal(&resolution),
                    },
                    a::Outcome::Late {
                        signature,
                        original,
                        evidence,
                        ..
                    } => DeliveryEvent::Late {
                        signature,
                        original: convert::terminal(&original),
                        evidence: convert::late(&evidence),
                    },
                };
                self.emit(ns, e).await?;
            }
            if batch.complete {
                return Ok(());
            }
        }
    }
    pub(super) async fn update(
        &mut self,
        at: u64,
        update: &yellowstone_grpc_proto::prelude::SubscribeUpdate,
    ) -> Result<()> {
        use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;
        let time = super::super::yellowstone_message_time::YellowstoneMessageTime::from_created_at(
            update.created_at.as_ref(),
        );
        match update.update_oneof.as_ref() {
            Some(UpdateOneof::Transaction(tx)) => {
                if self.recovery.is_some() {
                    self.recovery_transaction(at, tx, time).await
                } else {
                    self.push(at, a::Input::Transaction(tx, time)).await
                }
            }
            Some(UpdateOneof::Block(b)) => {
                if self.recovery.is_some() {
                    self.recovery_block(at, b).await
                } else {
                    self.push(at, a::Input::Block(b)).await
                }
            }
            _ => Ok(()),
        }
    }
    pub(super) async fn reset(&mut self, ns: u64) -> Result<()> {
        let next = a::Session {
            id: self.session.id,
            generation: self
                .session
                .generation
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("session generation exhausted"))?,
        };
        self.push(ns, a::Input::Reset(next)).await?;
        self.emit(ns, DeliveryEvent::Session(SessionGap::Reset))
            .await?;
        self.session = next;
        self.name = format!("{}:{}", self.base_name, next.generation);
        self.emit(
            ns,
            DeliveryEvent::Session(SessionGap::StartedContinuityUnknown),
        )
        .await?;
        Ok(())
    }
}
