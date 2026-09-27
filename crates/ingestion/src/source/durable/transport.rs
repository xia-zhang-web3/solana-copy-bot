use super::super::{yellowstone_association as a, YellowstoneRuntimeConfig};
use super::{
    bridge::Bridge,
    queue::Sender,
    telemetry::{DurableIngressTelemetry, TransportClass, TransportStage},
};
use anyhow::{Context, Result};
use copybot_config::AssociationDeliveryConfig;
use copybot_core_types::association_delivery::{DeliveryEvent, SessionGap};
use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::*;
fn request(c: &YellowstoneRuntimeConfig) -> SubscribeRequest {
    let mut req = super::super::yellowstone_request::build_yellowstone_subscribe_request(c);
    req.blocks.insert(
        "copybot-containing-blocks".into(),
        SubscribeRequestFilterBlocks {
            account_include: c.interested_program_ids.iter().cloned().collect(),
            include_transactions: Some(true),
            include_accounts: Some(false),
            include_entries: Some(false),
            ..Default::default()
        },
    );
    req
}
pub(super) async fn run(
    c: Arc<YellowstoneRuntimeConfig>,
    limits: AssociationDeliveryConfig,
    session: String,
    tx: Sender,
    bot_signer: Option<String>,
    telemetry: Arc<DurableIngressTelemetry>,
) -> Result<()> {
    let start = Instant::now();
    let mut bridge = Bridge::new(&c, &limits, session, tx, bot_signer, Arc::clone(&telemetry))?;
    bridge
        .emit(
            0,
            DeliveryEvent::Session(SessionGap::StartedContinuityUnknown),
        )
        .await?;
    let ns = || u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let mut backoff = c.reconnect_initial_ms;
    loop {
        let connection = async {
            let mut builder = GeyserGrpcClient::build_from_shared(c.grpc_url.clone())?
                .x_token(Some(c.x_token.as_str()))?;
            if c.grpc_url.starts_with("https://") {
                builder = builder
                    .tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())?;
            }
            let client = builder
                .connect_timeout(Duration::from_millis(c.connect_timeout_ms))
                .timeout(Duration::from_millis(c.subscribe_timeout_ms))
                .max_decoding_message_size(limits.input_bytes)
                .connect()
                .await?;
            Ok::<_, anyhow::Error>(client)
        }
        .await;
        let mut client = match connection {
            Ok(v) => v,
            Err(error) => {
                telemetry.reconnect(TransportStage::Connect, TransportClass::error(&error));
                bridge
                    .emit(ns(), DeliveryEvent::Session(SessionGap::Transport))
                    .await?;
                bridge.reset(ns()).await?;
                tokio::time::sleep(Duration::from_millis(backoff)).await;
                backoff = backoff.saturating_mul(2).min(c.reconnect_max_ms);
                continue;
            }
        };
        let (mut sink, mut stream) = match client.subscribe_with_request(Some(request(&c))).await {
            Ok(v) => v,
            Err(error) => {
                telemetry.reconnect(TransportStage::Subscribe, TransportClass::subscribe(&error));
                bridge
                    .emit(ns(), DeliveryEvent::Session(SessionGap::Transport))
                    .await?;
                bridge.reset(ns()).await?;
                tokio::time::sleep(Duration::from_millis(backoff)).await;
                backoff = backoff.saturating_mul(2).min(c.reconnect_max_ms);
                continue;
            }
        };
        backoff = c.reconnect_initial_ms;
        let mut tick = tokio::time::interval(Duration::from_millis(limits.tick_ms));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _=tick.tick()=>{
                    bridge.push(ns(),a::Input::Tick).await?;
                    telemetry.maybe_report();
                },
                v=stream.next()=>match v {
                    Some(Ok(update))=>{
                        let at=ns();
                        match update.update_oneof.as_ref() {
                            Some(subscribe_update::UpdateOneof::Transaction(tx)) => telemetry.received_transaction(tx.slot),
                            Some(subscribe_update::UpdateOneof::Block(block)) => telemetry.received_block(block.slot),
                            _ => {}
                        }
                        // Transport cap covers the entire envelope; adapter input cap
                        // covers its tx/block. Decoded allocations are still bounded by proto.
                        if update.encoded_len()>limits.input_bytes {
                            bridge.emit(at,DeliveryEvent::Session(SessionGap::Rejected("TransportInputTooLarge".into()))).await?;
                            bridge.push(at,a::Input::End).await?;
                            anyhow::bail!("delivery transport envelope exceeds budget");
                        }
                        match update.update_oneof.as_ref() {
                            Some(subscribe_update::UpdateOneof::Transaction(_)|subscribe_update::UpdateOneof::Block(_))=>bridge.update(at,&update).await?,
                            Some(subscribe_update::UpdateOneof::Ping(_))=>if sink.send(SubscribeRequest{ping:Some(SubscribeRequestPing{id:1}),..Default::default()}).await.is_err(){
                                telemetry.reconnect(TransportStage::Ping, TransportClass::Other);
                                break;
                            },
                            _=>{}
                        }
                    }
                    Some(Err(error))=>{
                        telemetry.reconnect(TransportStage::Stream, TransportClass::status(&error));
                        bridge.emit(ns(),DeliveryEvent::Session(SessionGap::Transport)).await?;
                        break;
                    }
                    None=>{
                        telemetry.reconnect(TransportStage::End, TransportClass::End);
                        bridge.push(ns(),a::Input::End).await?;
                        bridge.emit(ns(),DeliveryEvent::Session(SessionGap::End)).await?;
                        break;
                    }
                }
            }
        }
        bridge
            .reset(ns())
            .await
            .context("delivery reconnect reset")?;
        tokio::time::sleep(Duration::from_millis(backoff)).await;
    }
}
