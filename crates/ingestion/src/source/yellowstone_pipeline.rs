use futures_util::{SinkExt, StreamExt};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;
use tonic::transport::ClientTlsConfig;
use tracing::{debug, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{subscribe_update, SubscribeRequest, SubscribeRequestPing};

use super::core::{
    increment_atomic_usize, is_seen_signature, mark_seen_signature, prune_seen_signatures,
    sleep_with_backoff,
};
use super::yellowstone::{build_yellowstone_subscribe_request, parse_yellowstone_update};
use super::{
    FetchedObservation, QueueOverflowPolicy, QueuePushResult, RawObservationQueue,
    SeenSignatureEntry, YellowstoneParsedUpdate, YellowstoneRuntimeConfig, WS_IDLE_TIMEOUT_SECS,
};

pub(super) async fn yellowstone_stream_loop(
    runtime_config: Arc<YellowstoneRuntimeConfig>,
    output_queue: Arc<RawObservationQueue>,
    output_queue_depth: Arc<AtomicUsize>,
    queue_overflow_policy: QueueOverflowPolicy,
) {
    let mut next_backoff_ms = runtime_config.reconnect_initial_ms;
    let mut arrival_seq: u64 = 0;
    let mut seen_signatures_queue: VecDeque<SeenSignatureEntry> = VecDeque::new();
    let mut seen_signatures_map: HashMap<String, Instant> = HashMap::new();

    loop {
        let subscribe_request = build_yellowstone_subscribe_request(runtime_config.as_ref());
        let builder = match GeyserGrpcClient::build_from_shared(runtime_config.grpc_url.clone()) {
            Ok(builder) => builder,
            Err(error) => {
                runtime_config
                    .telemetry
                    .reconnect_count
                    .fetch_add(1, Ordering::Relaxed);
                warn!(error = %error, "invalid yellowstone endpoint");
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
                continue;
            }
        };
        let builder = match builder.x_token(Some(runtime_config.x_token.as_str())) {
            Ok(builder) => builder,
            Err(error) => {
                runtime_config
                    .telemetry
                    .reconnect_count
                    .fetch_add(1, Ordering::Relaxed);
                warn!(error = %error, "invalid yellowstone x-token metadata");
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
                continue;
            }
        };
        let use_tls = runtime_config
            .grpc_url
            .trim()
            .to_ascii_lowercase()
            .starts_with("https://");
        let builder = if use_tls {
            let tls_config = ClientTlsConfig::new().with_native_roots();
            match builder.tls_config(tls_config) {
                Ok(builder) => builder,
                Err(error) => {
                    runtime_config
                        .telemetry
                        .reconnect_count
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(error = ?error, "invalid yellowstone TLS config");
                    sleep_with_backoff(
                        &mut next_backoff_ms,
                        runtime_config.reconnect_initial_ms,
                        runtime_config.reconnect_max_ms,
                    )
                    .await;
                    continue;
                }
            }
        } else {
            builder
        };
        let mut client = match builder
            .connect_timeout(Duration::from_millis(runtime_config.connect_timeout_ms))
            .timeout(Duration::from_millis(runtime_config.subscribe_timeout_ms))
            .http2_adaptive_window(true)
            .tcp_nodelay(true)
            .connect()
            .await
        {
            Ok(client) => client,
            Err(error) => {
                runtime_config
                    .telemetry
                    .reconnect_count
                    .fetch_add(1, Ordering::Relaxed);
                warn!(error = ?error, "failed connecting yellowstone endpoint");
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
                continue;
            }
        };

        let (mut subscribe_tx, mut stream) = match client.subscribe().await {
            Ok(parts) => parts,
            Err(error) => {
                runtime_config
                    .telemetry
                    .reconnect_count
                    .fetch_add(1, Ordering::Relaxed);
                warn!(error = %error, "failed opening yellowstone subscription stream");
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
                continue;
            }
        };
        if let Err(error) = subscribe_tx.send(subscribe_request).await {
            runtime_config
                .telemetry
                .reconnect_count
                .fetch_add(1, Ordering::Relaxed);
            warn!(error = %error, "failed sending yellowstone subscribe request");
            sleep_with_backoff(
                &mut next_backoff_ms,
                runtime_config.reconnect_initial_ms,
                runtime_config.reconnect_max_ms,
            )
            .await;
            continue;
        };
        next_backoff_ms = runtime_config.reconnect_initial_ms;

        loop {
            let next_message =
                time::timeout(Duration::from_secs(WS_IDLE_TIMEOUT_SECS), stream.next()).await;
            match next_message {
                Ok(Some(Ok(update))) => {
                    let is_transaction_update = matches!(
                        update.update_oneof.as_ref(),
                        Some(subscribe_update::UpdateOneof::Transaction(_))
                    );
                    runtime_config
                        .telemetry
                        .grpc_message_total
                        .fetch_add(1, Ordering::Relaxed);
                    if is_transaction_update {
                        runtime_config
                            .telemetry
                            .grpc_transaction_updates_total
                            .fetch_add(1, Ordering::Relaxed);
                    }

                    match parse_yellowstone_update(update, runtime_config.as_ref()) {
                        Ok(Some(YellowstoneParsedUpdate::Observation(raw))) => {
                            let now = Instant::now();
                            prune_seen_signatures(
                                &mut seen_signatures_map,
                                &mut seen_signatures_queue,
                                runtime_config.seen_signatures_limit,
                                runtime_config.seen_signatures_ttl,
                                now,
                            );

                            if is_seen_signature(
                                &seen_signatures_map,
                                &raw.signature,
                                runtime_config.seen_signatures_ttl,
                                now,
                            ) {
                                continue;
                            }

                            arrival_seq = arrival_seq.saturating_add(1);
                            let signature = raw.signature.clone();
                            let fetched = FetchedObservation {
                                raw,
                                arrival_seq,
                                fetch_latency_ms: 0,
                            };

                            match output_queue.push(fetched, queue_overflow_policy).await {
                                Some(QueuePushResult::Enqueued { backpressured }) => {
                                    mark_seen_signature(
                                        &mut seen_signatures_map,
                                        &mut seen_signatures_queue,
                                        runtime_config.seen_signatures_limit,
                                        runtime_config.seen_signatures_ttl,
                                        signature,
                                        now,
                                    );
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_seen
                                        .fetch_add(1, Ordering::Relaxed);
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_enqueued
                                        .fetch_add(1, Ordering::Relaxed);
                                    if backpressured {
                                        runtime_config
                                            .telemetry
                                            .ws_notifications_backpressured
                                            .fetch_add(1, Ordering::Relaxed);
                                    }
                                    increment_atomic_usize(&output_queue_depth);
                                }
                                Some(QueuePushResult::ReplacedOldest) => {
                                    mark_seen_signature(
                                        &mut seen_signatures_map,
                                        &mut seen_signatures_queue,
                                        runtime_config.seen_signatures_limit,
                                        runtime_config.seen_signatures_ttl,
                                        signature,
                                        now,
                                    );
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_seen
                                        .fetch_add(1, Ordering::Relaxed);
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_enqueued
                                        .fetch_add(1, Ordering::Relaxed);
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_backpressured
                                        .fetch_add(1, Ordering::Relaxed);
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_replaced_oldest
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                None => {
                                    runtime_config
                                        .telemetry
                                        .ws_notifications_dropped
                                        .fetch_add(1, Ordering::Relaxed);
                                    warn!("yellowstone output queue closed; stopping stream loop");
                                    output_queue.close().await;
                                    return;
                                }
                            }
                        }
                        Ok(Some(YellowstoneParsedUpdate::Ping)) => {
                            let ping_request = SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            };
                            if let Err(error) = subscribe_tx.send(ping_request).await {
                                runtime_config
                                    .telemetry
                                    .reconnect_count
                                    .fetch_add(1, Ordering::Relaxed);
                                warn!(error = %error, "failed sending yellowstone ping response");
                                break;
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            runtime_config.telemetry.note_parse_rejected(&error);
                            debug!(error = %error, "failed parsing yellowstone transaction update");
                        }
                    }
                }
                Ok(Some(Err(error))) => {
                    runtime_config
                        .telemetry
                        .grpc_decode_errors
                        .fetch_add(1, Ordering::Relaxed);
                    runtime_config
                        .telemetry
                        .reconnect_count
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(error = %error, "yellowstone stream update error");
                    break;
                }
                Ok(None) => {
                    runtime_config
                        .telemetry
                        .reconnect_count
                        .fetch_add(1, Ordering::Relaxed);
                    warn!("yellowstone stream ended");
                    break;
                }
                Err(_) => {
                    runtime_config
                        .telemetry
                        .stream_gap_detected
                        .fetch_add(1, Ordering::Relaxed);
                    runtime_config
                        .telemetry
                        .reconnect_count
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        idle_timeout_seconds = WS_IDLE_TIMEOUT_SECS,
                        "yellowstone stream idle timeout; reconnecting"
                    );
                    break;
                }
            }
        }

        sleep_with_backoff(
            &mut next_backoff_ms,
            runtime_config.reconnect_initial_ms,
            runtime_config.reconnect_max_ms,
        )
        .await;
    }
}
