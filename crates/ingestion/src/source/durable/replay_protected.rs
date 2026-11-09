//! Offline raw-evidence seam: production Bridge, RecoveryGate and checkpoint ACK.
//! Synthetic wrappers supplied here are fixtures, never historical completeness.
use crate::source::durable::{bridge::Bridge, queue, recovery::RecoveryCursor, DeliveryReceiver};
use anyhow::{ensure, Context, Result};
use copybot_config::IngestionConfig;
use copybot_core_types::{
    association_delivery::{DeliveryEvent, SessionGap},
    association_recovery::DurableCheckpoint,
};
use prost::Message;
use std::{
    collections::HashSet,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::sync::mpsc;
use yellowstone_grpc_proto::prelude::SubscribeUpdate;

#[derive(Debug)]
pub enum ProtectedReplayInput {
    Update {
        offset_ns: u64,
        payload: Vec<u8>,
    },
    /// Exact JSON-RPC body accepted by the existing full-block response parser.
    HttpBlock {
        offset_ns: u64,
        slot: u64,
        raw_response: Vec<u8>,
    },
    Reset(u64),
    End(u64),
}
impl DeliveryReceiver {
    #[doc(hidden)]
    pub fn replay_protected(
        config: &IngestionConfig,
        session: String,
        mut input: mpsc::Receiver<ProtectedReplayInput>,
        wallets: HashSet<String>,
        restored: Option<DurableCheckpoint>,
    ) -> Result<Self> {
        copybot_config::validate_delivery_source(config)?;
        ensure!(
            config.yellowstone_delivery_mode == "durable_association_v1"
                && config.capture_recovery.is_some()
                && config.capture_scope_db.is_some(),
            "protected_replay_explicit_contract_required"
        );
        let limits = config
            .yellowstone_association
            .clone()
            .context("association limits")?;
        let http_config = config
            .yellowstone_http_recovery
            .clone()
            .context("HTTP recovery limits")?;
        let mut runtime =
            (*crate::source::YellowstoneGrpcSource::new(config)?.runtime_config).clone();
        runtime.admission_wallets = Some(wallets.clone());
        let scope = crate::source::durable::recovery::replay_scope(config, &wallets)?;
        let mut cursor = RecoveryCursor::new(scope, restored, limits.pending.clone())?;
        cursor.http_hold = Some(Arc::new(AtomicBool::new(true)));
        let telemetry =
            Arc::new(crate::source::durable::telemetry::DurableIngressTelemetry::default());
        cursor.observation = Some(telemetry.continuity.hold_tracker());
        let worker_cursor = cursor.clone();
        let worker_telemetry = telemetry.clone();
        let (sender, rx) = queue::channel(limits.queue.count, limits.queue.bytes);
        let task = tokio::spawn(async move {
            let mut bridge =
                Bridge::new(&runtime, &limits, session, sender, None, worker_telemetry)?;
            bridge.enable_recovery(worker_cursor, &limits)?;
            bridge.begin_replay()?;
            bridge
                .emit(
                    0,
                    DeliveryEvent::Session(SessionGap::StartedContinuityUnknown),
                )
                .await?;
            let http = crate::source::http_recovery::ConfirmedHttpRecovery::new(
                &http_config.broker_url,
                None,
                http_config.range_slots,
                http_config.max_response_bytes,
                std::time::Duration::from_millis(http_config.timeout_ms),
            )?;
            let mut ended = false;
            while let Some(value) = input.recv().await {
                match value {
                    ProtectedReplayInput::Update { offset_ns, payload } => {
                        ensure!(
                            payload.len() <= limits.input_bytes,
                            "protected_replay_input_bound"
                        );
                        bridge
                            .update(offset_ns, &SubscribeUpdate::decode(payload.as_slice())?)
                            .await?;
                    }
                    ProtectedReplayInput::HttpBlock {
                        offset_ns,
                        slot,
                        raw_response,
                    } => {
                        ensure!(
                            raw_response.len() <= http_config.max_response_bytes,
                            "protected_replay_http_bound"
                        );
                        let id = serde_json::from_slice::<serde_json::Value>(&raw_response)?["id"]
                            .as_u64()
                            .context("fixture response id")?;
                        let block = http.normalize_raw_block(
                            crate::source::http_recovery::RawRecoveredBlock::from_response(
                                slot,
                                200,
                                id,
                                raw_response,
                            ),
                        )?;
                        bridge
                            .http_block_with_evidence(
                                offset_ns,
                                &block.block,
                                &block.raw_response,
                                block.block.block_time.as_ref().map(|t| t.timestamp),
                            )
                            .await?;
                    }
                    ProtectedReplayInput::Reset(at) => {
                        bridge.reset(at).await?;
                        bridge.begin_replay()?;
                    }
                    ProtectedReplayInput::End(at) => {
                        bridge
                            .push(at, crate::source::yellowstone_association::Input::End)
                            .await?;
                        bridge
                            .emit(at, DeliveryEvent::Session(SessionGap::End))
                            .await?;
                        ended = true;
                        break;
                    }
                }
            }
            ensure!(ended, "protected replay requires explicit End");
            Ok(())
        });
        Ok(Self {
            rx,
            task: Some(task),
            wallet_scope: Some(wallets),
            bot_signer: None,
            telemetry,
            recovery: Some(cursor),
        })
    }
}
