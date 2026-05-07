use super::fetch_worker::connect_ws_stream;
use super::*;

pub(in crate::source) async fn ws_reader_loop(
    runtime_config: Arc<HeliusRuntimeConfig>,
    notification_queue: Arc<NotificationQueue>,
    ws_to_fetch_depth: Arc<AtomicUsize>,
    queue_overflow_policy: QueueOverflowPolicy,
) {
    let mut request_id: u64 = 1000;
    let mut arrival_seq: u64 = 0;
    let mut ws: Option<HeliusWsStream> = None;
    let mut next_backoff_ms = runtime_config.reconnect_initial_ms;
    let mut seen_signatures_queue: VecDeque<SeenSignatureEntry> = VecDeque::new();
    let mut seen_signatures_map: HashMap<String, Instant> = HashMap::new();

    loop {
        if ws.is_none() {
            match connect_ws_stream(&runtime_config, &mut request_id).await {
                Ok(stream) => {
                    ws = Some(stream);
                    next_backoff_ms = runtime_config.reconnect_initial_ms;
                }
                Err(error) => {
                    warn!(error = ?error, "helius ws connect failed");
                    sleep_with_backoff(
                        &mut next_backoff_ms,
                        runtime_config.reconnect_initial_ms,
                        runtime_config.reconnect_max_ms,
                    )
                    .await;
                    continue;
                }
            }
        }

        let next_message = {
            let ws_stream = ws.as_mut().expect("ws stream present after connect");
            time::timeout(Duration::from_secs(WS_IDLE_TIMEOUT_SECS), ws_stream.next()).await
        };

        match next_message {
            Ok(Some(Ok(Message::Text(text)))) => {
                if let Some(mut notification) = HeliusWsSource::parse_logs_notification(&text) {
                    runtime_config
                        .telemetry
                        .ws_notifications_seen
                        .fetch_add(1, Ordering::Relaxed);
                    if notification.is_failed {
                        continue;
                    }
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
                        &notification.signature,
                        runtime_config.seen_signatures_ttl,
                        now,
                    ) {
                        continue;
                    }

                    arrival_seq = arrival_seq.saturating_add(1);
                    notification.arrival_seq = arrival_seq;
                    notification.enqueued_at = now;
                    let signature = notification.signature.clone();

                    match notification_queue
                        .push(notification, queue_overflow_policy)
                        .await
                    {
                        Some(QueuePushResult::Enqueued { backpressured }) => {
                            mark_seen_signature(
                                &mut seen_signatures_map,
                                &mut seen_signatures_queue,
                                runtime_config.seen_signatures_limit,
                                runtime_config.seen_signatures_ttl,
                                signature,
                                now,
                            );
                            if backpressured {
                                runtime_config
                                    .telemetry
                                    .ws_notifications_backpressured
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            increment_atomic_usize(&ws_to_fetch_depth);
                            runtime_config
                                .telemetry
                                .ws_notifications_enqueued
                                .fetch_add(1, Ordering::Relaxed);
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
                                .ws_notifications_backpressured
                                .fetch_add(1, Ordering::Relaxed);
                            runtime_config
                                .telemetry
                                .ws_notifications_replaced_oldest
                                .fetch_add(1, Ordering::Relaxed);
                            runtime_config
                                .telemetry
                                .ws_notifications_enqueued
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        None => {
                            runtime_config
                                .telemetry
                                .ws_notifications_dropped
                                .fetch_add(1, Ordering::Relaxed);
                            warn!(
                                policy = queue_overflow_policy.as_str(),
                                "notification queue closed; stopping ws reader"
                            );
                            break;
                        }
                    }
                }
            }
            Ok(Some(Ok(Message::Ping(payload)))) => {
                if let Some(ws_stream) = ws.as_mut() {
                    if let Err(error) = ws_stream.send(Message::Pong(payload)).await {
                        warn!(error = %error, "failed to send ws pong");
                        ws = None;
                        sleep_with_backoff(
                            &mut next_backoff_ms,
                            runtime_config.reconnect_initial_ms,
                            runtime_config.reconnect_max_ms,
                        )
                        .await;
                    }
                }
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                warn!(?frame, "helius ws closed");
                ws = None;
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(error))) => {
                warn!(error = %error, "helius ws stream error");
                ws = None;
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
            }
            Ok(None) => {
                warn!("helius ws stream ended");
                ws = None;
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
            }
            Err(_) => {
                warn!(
                    idle_timeout_seconds = WS_IDLE_TIMEOUT_SECS,
                    "helius ws idle timeout, reconnecting"
                );
                ws = None;
                sleep_with_backoff(
                    &mut next_backoff_ms,
                    runtime_config.reconnect_initial_ms,
                    runtime_config.reconnect_max_ms,
                )
                .await;
            }
        }
    }
}
