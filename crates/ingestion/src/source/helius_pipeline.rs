use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use super::{
    compute_retry_delay, decrement_atomic_usize, increment_atomic_usize, is_seen_signature,
    mark_seen_signature, normalize_program_ids_or_fallback, parse_retry_after,
    prune_seen_signatures, sleep_with_backoff, FetchedObservation, HeliusEndpoint,
    HeliusRuntimeConfig, HeliusWsSource, HeliusWsStream, LogsNotification, NotificationQueue,
    QueueOverflowPolicy, QueuePushResult, RawSwapObservation, SeenSignatureEntry,
    WS_IDLE_TIMEOUT_SECS,
};

pub(super) async fn ws_reader_loop(
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

pub(super) async fn fetch_worker_loop(
    worker_id: usize,
    runtime_config: Arc<HeliusRuntimeConfig>,
    notification_queue: Arc<NotificationQueue>,
    out_tx: mpsc::Sender<FetchedObservation>,
    ws_to_fetch_depth: Arc<AtomicUsize>,
    fetch_to_output_depth: Arc<AtomicUsize>,
) {
    loop {
        let notification = notification_queue.pop().await;

        let Some(notification) = notification else {
            debug!(
                worker_id,
                "fetch worker exiting because notification queue is closed"
            );
            return;
        };
        decrement_atomic_usize(&ws_to_fetch_depth);

        if runtime_config
            .prefetch_stale_drop
            .is_some_and(|limit| notification.enqueued_at.elapsed() > limit)
        {
            runtime_config
                .telemetry
                .prefetch_stale_dropped
                .fetch_add(1, Ordering::Relaxed);
            runtime_config
                .telemetry
                .fetch_failed
                .fetch_add(1, Ordering::Relaxed);
            continue;
        }

        match fetch_swap_with_retries(runtime_config.as_ref(), notification).await {
            Ok(Some(fetched)) => {
                if out_tx.send(fetched).await.is_err() {
                    warn!(worker_id, "output channel closed; stopping fetch worker");
                    return;
                }
                increment_atomic_usize(&fetch_to_output_depth);
            }
            Ok(None) => {
                runtime_config
                    .telemetry
                    .fetch_no_swap
                    .fetch_add(1, Ordering::Relaxed);
                runtime_config
                    .telemetry
                    .fetch_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                runtime_config
                    .telemetry
                    .fetch_failed
                    .fetch_add(1, Ordering::Relaxed);
                warn!(worker_id, error = %error, "fetch worker failed to parse transaction");
            }
        }
    }
}

async fn connect_ws_stream(
    runtime_config: &HeliusRuntimeConfig,
    request_id: &mut u64,
) -> Result<HeliusWsStream> {
    let (mut ws, _response) = connect_async(&runtime_config.ws_url)
        .await
        .with_context(|| format!("failed connecting to {}", runtime_config.ws_url))?;

    for program_id in &runtime_config.interested_program_ids {
        *request_id = request_id.saturating_add(1);
        let request = json!({
            "jsonrpc": "2.0",
            "id": *request_id,
            "method": "logsSubscribe",
            "params": [
                {"mentions": [program_id]},
                {"commitment": "confirmed"}
            ]
        });
        ws.send(Message::Text(request.to_string().into()))
            .await
            .with_context(|| format!("failed sending logsSubscribe for {program_id}"))?;
    }

    info!(
        ws_url = %runtime_config.ws_url,
        http_endpoints = runtime_config.http_endpoints.len(),
        programs = runtime_config.interested_program_ids.len(),
        "helius ws connected and subscriptions sent"
    );

    Ok(ws)
}

async fn fetch_swap_with_retries(
    runtime_config: &HeliusRuntimeConfig,
    notification: LogsNotification,
) -> Result<Option<FetchedObservation>> {
    for attempt in 0..=runtime_config.tx_fetch_retries {
        let endpoint = runtime_config.next_http_endpoint();
        let started = Instant::now();
        runtime_config
            .telemetry
            .fetch_inflight
            .fetch_add(1, Ordering::Relaxed);
        let result = fetch_swap_from_signature(
            runtime_config,
            endpoint.as_ref(),
            &notification.signature,
            notification.slot,
            &notification.logs,
        )
        .await;
        runtime_config
            .telemetry
            .fetch_inflight
            .fetch_sub(1, Ordering::Relaxed);

        match result {
            Ok(Some(raw)) => {
                runtime_config
                    .telemetry
                    .fetch_success
                    .fetch_add(1, Ordering::Relaxed);
                let fetch_latency_ms = started.elapsed().as_millis() as u64;
                return Ok(Some(FetchedObservation {
                    raw,
                    arrival_seq: notification.arrival_seq,
                    fetch_latency_ms,
                }));
            }
            Ok(None) => return Ok(None),
            Err(fetch_error) => {
                let can_retry = fetch_error.retryable && attempt < runtime_config.tx_fetch_retries;
                let error = fetch_error.error;
                if !can_retry {
                    if fetch_error.retryable {
                        runtime_config
                            .telemetry
                            .fetch_retry_exhausted
                            .fetch_add(1, Ordering::Relaxed);
                    } else {
                        runtime_config
                            .telemetry
                            .fetch_retry_terminal
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    warn!(
                        error = %error,
                        signature = %notification.signature,
                        attempt,
                        retryable = fetch_error.retryable,
                        "tx fetch attempt failed"
                    );
                    return Err(error);
                }

                runtime_config
                    .telemetry
                    .fetch_retry_attempts
                    .fetch_add(1, Ordering::Relaxed);
                let wait = compute_retry_delay(
                    runtime_config.tx_fetch_retry_base_ms,
                    runtime_config.tx_fetch_retry_max_ms,
                    runtime_config.tx_fetch_retry_jitter_ms,
                    attempt,
                    &notification.signature,
                    fetch_error.retry_after,
                );
                debug!(
                    error = %error,
                    signature = %notification.signature,
                    attempt,
                    wait_ms = wait.as_millis() as u64,
                    "retrying tx fetch attempt after backoff"
                );
                time::sleep(wait).await;
            }
        }
    }

    Ok(None)
}

#[derive(Debug)]
struct FetchAttemptError {
    error: anyhow::Error,
    retryable: bool,
    retry_after: Option<Duration>,
}

impl FetchAttemptError {
    fn retryable(error: anyhow::Error, retry_after: Option<Duration>) -> Self {
        Self {
            error,
            retryable: true,
            retry_after,
        }
    }

    fn terminal(error: anyhow::Error) -> Self {
        Self {
            error,
            retryable: false,
            retry_after: None,
        }
    }
}

async fn fetch_swap_from_signature(
    runtime_config: &HeliusRuntimeConfig,
    endpoint: &HeliusEndpoint,
    signature: &str,
    slot_hint: u64,
    logs_hint: &[String],
) -> std::result::Result<Option<RawSwapObservation>, FetchAttemptError> {
    if let Some(global_limiter) = runtime_config.global_http_limiter.as_ref() {
        global_limiter.acquire().await;
    }
    if let Some(endpoint_limiter) = endpoint.limiter.as_ref() {
        endpoint_limiter.acquire().await;
    }

    let http_url = endpoint.url.as_str();
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0
            }
        ]
    });

    let response = runtime_config
        .http_client
        .post(http_url)
        .json(&request)
        .send()
        .await
        .map_err(|error| {
            FetchAttemptError::retryable(
                anyhow!("failed getTransaction POST for {signature} via {http_url}: {error}"),
                None,
            )
        })?;

    let status = response.status();
    let retry_after = parse_retry_after(&response);
    if status.as_u16() == 429 {
        runtime_config
            .telemetry
            .rpc_429
            .fetch_add(1, Ordering::Relaxed);
    }
    if status.is_server_error() {
        runtime_config
            .telemetry
            .rpc_5xx
            .fetch_add(1, Ordering::Relaxed);
    }
    if status.as_u16() == 429 || status.is_server_error() {
        return Err(FetchAttemptError::retryable(
            anyhow!("retryable getTransaction status {status} for {signature} via {http_url}"),
            retry_after,
        ));
    }
    if !status.is_success() {
        return Err(FetchAttemptError::terminal(anyhow!(
            "non-success getTransaction status {status} for {signature} via {http_url}"
        )));
    }

    let response: Value = response.json().await.map_err(|error| {
        FetchAttemptError::terminal(anyhow!(
            "failed parsing getTransaction json for {signature} via {http_url}: {error}"
        ))
    })?;

    if response.get("error").is_some() {
        debug!(signature, error = ?response.get("error"), "rpc returned error");
        return Ok(None);
    }

    let result = match response.get("result") {
        Some(value) if !value.is_null() => value,
        _ => return Ok(None),
    };
    let meta = match result.get("meta") {
        Some(value) if !value.is_null() => value,
        _ => return Ok(None),
    };
    if meta.get("err").map(|err| !err.is_null()).unwrap_or(false) {
        return Ok(None);
    }

    let account_keys = HeliusWsSource::extract_account_keys(result);
    if account_keys.is_empty() {
        return Ok(None);
    }
    let signer_index = account_keys
        .iter()
        .position(|(_, is_signer)| *is_signer)
        .unwrap_or(0);
    let signer = account_keys
        .get(signer_index)
        .map(|(pubkey, _)| pubkey.clone())
        .ok_or_else(|| {
            FetchAttemptError::terminal(anyhow!("missing signer in parsed account keys"))
        })?;

    let program_ids = match normalize_program_ids_or_fallback(
        HeliusWsSource::extract_program_ids(result, meta, logs_hint),
        &runtime_config.interested_program_ids,
        runtime_config.telemetry.as_ref(),
        "missing program ids in helius transaction update",
    )
    .map_err(FetchAttemptError::terminal)?
    {
        Some(value) => value,
        None => return Ok(None),
    };

    let (token_in, amount_in, token_out, amount_out) =
        match HeliusWsSource::infer_swap_from_json_balances(meta, signer_index, &signer) {
            Some(value) => value,
            None => return Ok(None),
        };

    let block_time = result.get("blockTime").and_then(Value::as_i64);
    let ts_utc = block_time
        .and_then(|ts| DateTime::<Utc>::from_timestamp(ts, 0))
        .unwrap_or_else(Utc::now);
    let slot = result
        .get("slot")
        .and_then(Value::as_u64)
        .unwrap_or(slot_hint);
    let logs = HeliusWsSource::value_to_string_vec(meta.get("logMessages"))
        .unwrap_or_else(|| logs_hint.to_vec());
    let dex_hint = HeliusWsSource::detect_dex_hint(
        &program_ids,
        &logs,
        &runtime_config.raydium_program_ids,
        &runtime_config.pumpswap_program_ids,
    );

    Ok(Some(RawSwapObservation {
        signature: signature.to_string(),
        slot,
        signer,
        token_in,
        token_out,
        amount_in,
        amount_out,
        program_ids: program_ids.into_iter().collect(),
        dex_hint,
        ts_utc,
    }))
}
