use super::super::{yellowstone_association as a, YellowstoneRuntimeConfig};
use super::{convert, queue::Sender};
use a::limits::Budget;
use anyhow::Result;
use copybot_config::AssociationDeliveryConfig;
use copybot_core_types::association_delivery::*;
use std::time::Duration;
pub(super) struct Bridge<'a> {
    adapter: a::YellowstoneAssociation<'a>,
    session: a::Session,
    name: String,
    base_name: String,
    sequence: u64,
    tx: Sender,
}
impl<'a> Bridge<'a> {
    pub(super) fn new(
        runtime: &'a YellowstoneRuntimeConfig,
        c: &AssociationDeliveryConfig,
        name: String,
        tx: Sender,
    ) -> Result<Self> {
        let b = |v: &copybot_config::DeliveryBudget| Budget {
            count: v.count,
            encoded_bytes: v.bytes,
        };
        let session = a::Session {
            id: [0; 16],
            generation: 0,
        };
        let adapter = a::YellowstoneAssociation::new(
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
        Ok(Self {
            adapter,
            session,
            name: format!("{name}:0"),
            base_name: name,
            sequence: 0,
            tx,
        })
    }
    pub(super) async fn emit(&mut self, ns: u64, event: DeliveryEvent) -> Result<()> {
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
        self.tx.send(d).await
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
        let admission = match self.adapter.push(context, input) {
            Ok(v) => v,
            Err(r) => {
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
        match admission {
            a::Admission::Block => {
                let parent = super::parent::observation(parent_input.expect("admitted block"));
                self.emit(ns, DeliveryEvent::Parent(parent)).await?;
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
                self.push(at, a::Input::Transaction(tx, time)).await
            }
            Some(UpdateOneof::Block(b)) => self.push(at, a::Input::Block(b)).await,
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
