//! Live intake continues while confirmed HTTP history reaches its first anchor.
use super::{
    super::bridge::Bridge,
    super::telemetry::{DurableIngressTelemetry, TransportStage},
    super::transport_diagnostics::{self as diagnostics, ErrorDetails},
};
use crate::source::{
    http_recovery::{identity, ConfirmedHttpRecovery},
    yellowstone_association as a, YellowstoneRuntimeConfig,
};
use anyhow::{ensure, Context, Result};
use copybot_config::{AssociationDeliveryConfig, HttpRecoveryConfig};
use copybot_core_types::association_delivery::{DeliveryEvent, SessionGap};
use futures_util::{stream, StreamExt};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::*;
#[path = "http_progress.rs"]
mod http_progress;
#[path = "reader.rs"]
mod reader;
#[cfg(test)]
pub(in crate::source) use reader::Reader as TestReader;
use reader::{Captured, End, Reader};

fn ns(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}
async fn process(
    bridge: &mut Bridge<'_>,
    at: u64,
    update: &SubscribeUpdate,
    telemetry: &DurableIngressTelemetry,
) -> Result<()> {
    let stage = Instant::now();
    let result = bridge.update(at, update).await;
    telemetry.processing.update_kind(
        matches!(
            update.update_oneof,
            Some(subscribe_update::UpdateOneof::Block(_))
        ),
        stage.elapsed(),
    );
    result
}
async fn finish(
    reader: &mut Reader,
    bridge: &mut Bridge<'_>,
    start: Instant,
    connection: Instant,
    telemetry: &DurableIngressTelemetry,
    secrets: &[&str],
) -> Result<()> {
    match reader.end().await? {
        End::Status(e) => diagnostics::report(
            telemetry,
            TransportStage::Stream,
            connection,
            ErrorDetails::status(&e, secrets),
        ),
        End::Ping(e) => diagnostics::report(
            telemetry,
            TransportStage::Ping,
            connection,
            ErrorDetails::error(&e, secrets),
        ),
        End::Eof => {
            diagnostics::report(
                telemetry,
                TransportStage::End,
                connection,
                ErrorDetails::eof(),
            );
            bridge.push(ns(start), a::Input::End).await?;
            return bridge
                .emit(ns(start), DeliveryEvent::Session(SessionGap::End))
                .await;
        }
        End::Budget(reason) => {
            bridge
                .emit(
                    ns(start),
                    DeliveryEvent::Session(SessionGap::Rejected(reason.into())),
                )
                .await?;
            anyhow::bail!("http_recovery_live_capture_refused: {reason}");
        }
    }
    bridge
        .emit(ns(start), DeliveryEvent::Session(SessionGap::Transport))
        .await
}
pub(super) async fn run(
    c: &YellowstoneRuntimeConfig,
    limits: &AssociationDeliveryConfig,
    config: &HttpRecoveryConfig,
    mut bridge: Bridge<'_>,
    start: Instant,
    telemetry: Arc<DurableIngressTelemetry>,
) -> Result<()> {
    let timeout = Duration::from_millis(config.timeout_ms);
    let header = (!config.broker_token.is_empty())
        .then_some(("X-Copybot-Broker-Token", config.broker_token.as_str()));
    let http = ConfirmedHttpRecovery::new(
        &config.broker_url,
        header,
        config.range_slots,
        config.max_response_bytes,
        timeout,
    )?;
    let secrets = [
        c.grpc_url.as_str(),
        c.x_token.as_str(),
        config.broker_token.as_str(),
    ];
    let mut backoff = c.reconnect_initial_ms;
    loop {
        // A freshly received boundary cannot replace an unacknowledged durable
        // boundary. This also closes the last-parent/next-connection race.
        bridge.wait_checkpoint(timeout).await?;
        let connection = Instant::now();
        let connected = async {
            let mut builder = GeyserGrpcClient::build_from_shared(c.grpc_url.clone())?
                .x_token(Some(c.x_token.as_str()))?;
            if c.grpc_url.starts_with("https://") {
                builder = builder
                    .tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())?;
            }
            Ok::<_, anyhow::Error>(
                builder
                    .connect_timeout(Duration::from_millis(c.connect_timeout_ms))
                    .timeout(Duration::from_millis(c.subscribe_timeout_ms))
                    .max_decoding_message_size(limits.input_bytes)
                    .connect()
                    .await?,
            )
        }
        .await;
        let mut client = match connected {
            Ok(client) => client,
            Err(error) => {
                bridge.set_http_hold(true);
                diagnostics::report(
                    &telemetry,
                    TransportStage::Connect,
                    connection,
                    ErrorDetails::error(&error, &secrets),
                );
                bridge
                    .emit(ns(start), DeliveryEvent::Session(SessionGap::Transport))
                    .await?;
                bridge.reset(ns(start)).await?;
                tokio::time::sleep(Duration::from_millis(backoff)).await;
                backoff = backoff.saturating_mul(2).min(c.reconnect_max_ms);
                continue;
            }
        };
        let from = bridge.begin_replay()?;
        if from.is_some() {
            bridge.set_http_hold(true);
        }
        // This endpoint rejects full-block from_slot. HTTP owns history;
        // gRPC is always a fresh complete-block subscription in this mode.
        let (sink, incoming) = match client
            .subscribe_with_request(Some(super::request(c, true, None)))
            .await
        {
            Ok(v) => v,
            Err(error) => {
                bridge.set_http_hold(true);
                diagnostics::report(
                    &telemetry,
                    TransportStage::Subscribe,
                    connection,
                    ErrorDetails::subscribe(&error, &secrets),
                );
                bridge
                    .emit(ns(start), DeliveryEvent::Session(SessionGap::Transport))
                    .await?;
                bridge.reset(ns(start)).await?;
                tokio::time::sleep(Duration::from_millis(backoff)).await;
                backoff = backoff.saturating_mul(2).min(c.reconnect_max_ms);
                continue;
            }
        };
        backoff = c.reconnect_initial_ms;
        let mut reader = Reader::start(
            incoming,
            sink,
            limits.blocks.count,
            limits.blocks.bytes,
            limits.input_bytes,
            telemetry.clone(),
            bridge.begin_capture()?,
        )?;
        if let Some(from) = from {
            let mut before_anchor: Vec<Captured> = Vec::new();
            let anchor = loop {
                let Some(value) = reader.next().await else {
                    break None;
                };
                if value.is_block() {
                    break Some(value);
                }
                before_anchor.push(value);
            };
            if let Some(mut anchor) = anchor {
                let Some(subscribe_update::UpdateOneof::Block(block)) =
                    anchor.update()?.update_oneof.as_ref()
                else {
                    unreachable!()
                };
                if let Err(error) =
                    catch_up(&http, config, from, block, &mut bridge, start, &telemetry).await
                {
                    http_progress::refused(&error, &secrets);
                    bridge
                        .emit(
                            ns(start),
                            DeliveryEvent::Session(SessionGap::Rejected(
                                "HttpRecoveryRefused".into(),
                            )),
                        )
                        .await?;
                    return Err(error).context("confirmed_http_recovery_refused");
                }
                let anchor_slot = block.slot;
                reader.clear_verified();
                anchor.dequeue();
                // Confirmed full blocks already cover older processed messages.
                // Re-admitting them would invent a fresh observation after HTTP.
                for mut value in before_anchor {
                    if matches!(value.update()?.update_oneof.as_ref(),
                        Some(subscribe_update::UpdateOneof::Transaction(t)) if t.slot > anchor_slot)
                    {
                        value.dequeue();
                        process(&mut bridge, ns(start), value.update()?, &telemetry).await?;
                    }
                }
            } else {
                finish(
                    &mut reader,
                    &mut bridge,
                    start,
                    connection,
                    &telemetry,
                    &secrets,
                )
                .await?;
                bridge.reset(ns(start)).await?;
                tokio::time::sleep(Duration::from_millis(backoff)).await;
                continue;
            }
        }
        let mut first_fresh_parent = from.is_none();
        let mut tick = tokio::time::interval(Duration::from_millis(limits.tick_ms));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => { bridge.push(ns(start), a::Input::Tick).await?; telemetry.maybe_report(); }
                value = reader.next() => {
                    let Some(mut value) = value else { break; };
                    let is_block = value.is_block();
                    value.dequeue();
                    if let Err(error) = process(&mut bridge, ns(start), value.update()?, &telemetry).await {
                        bridge.set_http_hold(true);
                        bridge.emit(ns(start), DeliveryEvent::Session(SessionGap::Rejected("AssociationRecoveryRefused".into()))).await?;
                        return Err(error).context("HTTP/live association refused");
                    }
                    if first_fresh_parent && is_block {
                        bridge.wait_checkpoint(timeout).await?;
                        reader.clear_verified();
                        first_fresh_parent = false;
                    }
                }
            }
        }
        finish(
            &mut reader,
            &mut bridge,
            start,
            connection,
            &telemetry,
            &secrets,
        )
        .await?;
        bridge.reset(ns(start)).await?;
        tokio::time::sleep(Duration::from_millis(backoff)).await;
    }
}
async fn catch_up(
    http: &ConfirmedHttpRecovery,
    config: &HttpRecoveryConfig,
    from: u64,
    anchor: &SubscribeUpdateBlock,
    bridge: &mut Bridge<'_>,
    start: Instant,
    telemetry: &DurableIngressTelemetry,
) -> Result<()> {
    ensure!(
        anchor.slot >= from,
        "http_recovery_live_anchor_before_cursor"
    );
    let mut progress = http_progress::Progress::new(from, anchor.slot);
    let mut lower = from;
    let mut last = None;
    loop {
        let upper = lower
            .saturating_add(config.range_slots - 1)
            .min(anchor.slot);
        let slots = http.slots(lower, upper).await?;
        // The list may omit skipped slots. The parent/hash gate, rather than
        // arithmetic adjacency, proves continuity of actual confirmed blocks.
        let blocks = stream::iter(
            slots
                .into_iter()
                .map(|slot| async move { http.block(slot).await }),
        )
        .buffered(config.fetch_concurrency);
        tokio::pin!(blocks);
        while let Some(recovered) = blocks.next().await {
            let recovered = recovered?;
            let block = &recovered.block;
            let stage = Instant::now();
            if block.slot == anchor.slot {
                ensure!(
                    identity::block_equivalent(block, anchor),
                    "http_recovery_live_anchor_conflict"
                );
                bridge.http_block(ns(start), anchor).await?;
            } else {
                bridge.http_block(ns(start), block).await?;
            }
            telemetry.processing.update_kind(true, stage.elapsed());
            last = Some(block.slot);
            progress.note(telemetry, block.slot, false);
        }
        if upper == anchor.slot {
            break;
        }
        lower = upper
            .checked_add(1)
            .context("http_recovery_range_overflow")?;
    }
    ensure!(
        last == Some(anchor.slot),
        "http_recovery_live_anchor_missing"
    );
    bridge
        .wait_checkpoint(Duration::from_millis(config.timeout_ms))
        .await?;
    ensure!(
        !bridge.replay_waiting_anchor(),
        "http_recovery_durable_anchor_missing"
    );
    progress.note(telemetry, anchor.slot, true);
    Ok(())
}
