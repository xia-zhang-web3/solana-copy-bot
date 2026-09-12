//! Deterministic transport seam: callers supply already validated captures in
//! arrival order. It uses the same bridge, queue, service and app consumer as tonic.
//! This does not validate capture manifests or assert provider continuity.
use super::{bridge::Bridge, queue, DeliveryReceiver};
use anyhow::{ensure, Context, Result};
use copybot_config::IngestionConfig;
use copybot_core_types::association_delivery::{DeliveryEvent, SessionGap};
use prost::Message;
use tokio::sync::mpsc;
use yellowstone_grpc_proto::prelude::SubscribeUpdate;
#[derive(Debug)]
pub enum ReplayInput {
    Update { offset_ns: u64, payload: Vec<u8> },
    Tick(u64),
    Reset(u64),
    End(u64),
}
impl DeliveryReceiver {
    #[doc(hidden)]
    pub fn replay(
        config: &IngestionConfig,
        session: String,
        mut input: mpsc::Receiver<ReplayInput>,
    ) -> Result<Self> {
        copybot_config::validate_delivery_source(config)?;
        ensure!(
            config.yellowstone_delivery_mode == "durable_association_v1",
            "replay requires durable mode"
        );
        let limits = config
            .yellowstone_association
            .clone()
            .context("association limits")?;
        let runtime = super::super::YellowstoneGrpcSource::new(config)?.runtime_config;
        let (tx, rx) = queue::channel(limits.queue.count, limits.queue.bytes);
        let task = tokio::spawn(async move {
            let mut bridge = Bridge::new(&runtime, &limits, session, tx)?;
            bridge
                .emit(
                    0,
                    DeliveryEvent::Session(SessionGap::StartedContinuityUnknown),
                )
                .await?;
            let mut ended = false;
            while let Some(v) = input.recv().await {
                use super::super::yellowstone_association::Input;
                match v {
                    ReplayInput::Update { offset_ns, payload } => {
                        ensure!(
                            payload.len() <= limits.input_bytes,
                            "replay transport input budget"
                        );
                        bridge
                            .update(offset_ns, &SubscribeUpdate::decode(payload.as_slice())?)
                            .await?;
                    }
                    ReplayInput::Tick(ns) => bridge.push(ns, Input::Tick).await?,
                    ReplayInput::Reset(ns) => bridge.reset(ns).await?,
                    ReplayInput::End(ns) => {
                        bridge.push(ns, Input::End).await?;
                        bridge
                            .emit(ns, DeliveryEvent::Session(SessionGap::End))
                            .await?;
                        ended = true;
                        break;
                    }
                }
            }
            ensure!(
                ended,
                "replay ended without explicit End: continuity unknown"
            );
            Ok(())
        });
        Ok(Self {
            rx,
            task: Some(task),
        })
    }
}
