//! Deterministic transport seam: callers supply already validated captures in
//! arrival order. It uses the same bridge, queue, service and app consumer as tonic.
//! This does not validate capture manifests or assert provider continuity.
use super::{bridge::Bridge, queue, DeliveryReceiver};
use anyhow::{ensure, Context, Result};
use copybot_config::IngestionConfig;
use copybot_core_types::association_delivery::{DeliveryEvent, SessionGap};
use prost::Message;
use std::collections::HashSet;
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
        wallet_scope: Option<HashSet<String>>,
        bot_signer: Option<String>,
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
        let mut runtime =
            (*super::super::YellowstoneGrpcSource::new(config)?.runtime_config).clone();
        runtime.admission_wallets = wallet_scope.clone();
        let runtime = std::sync::Arc::new(runtime);
        let (tx, rx) = queue::channel(limits.queue.count, limits.queue.bytes);
        let telemetry = std::sync::Arc::new(super::telemetry::DurableIngressTelemetry::default());
        let task_telemetry = std::sync::Arc::clone(&telemetry);
        let task_bot = bot_signer.clone();
        let task = tokio::spawn(async move {
            let mut bridge = Bridge::new(
                &runtime,
                &limits,
                session,
                tx,
                task_bot,
                std::sync::Arc::clone(&task_telemetry),
            )?;
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
                        let update = SubscribeUpdate::decode(payload.as_slice())?;
                        use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;
                        match update.update_oneof.as_ref() {
                            Some(UpdateOneof::Transaction(tx)) => {
                                task_telemetry.received_transaction(tx.slot)
                            }
                            Some(UpdateOneof::Block(block)) => {
                                task_telemetry.received_block(block.slot)
                            }
                            _ => {}
                        }
                        bridge.update(offset_ns, &update).await?;
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
            wallet_scope,
            bot_signer,
            telemetry,
        })
    }
}
