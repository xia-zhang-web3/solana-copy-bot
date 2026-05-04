use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::time;
use tracing::{debug, warn};

use super::core::{compute_retry_delay, normalize_program_ids_or_fallback, parse_retry_after};
use super::{
    redacted_url_for_log, FetchedObservation, HeliusEndpoint, HeliusRuntimeConfig, HeliusWsSource,
    LogsNotification, RawSwapObservation,
};

pub(super) async fn fetch_swap_with_retries(
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
                    enqueued_at: Instant::now(),
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
    let redacted_http_url = redacted_url_for_log(http_url);
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
                anyhow!(
                    "failed getTransaction POST for {signature} via {redacted_http_url}: {error}"
                ),
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
            anyhow!(
                "retryable getTransaction status {status} for {signature} via {redacted_http_url}"
            ),
            retry_after,
        ));
    }
    if !status.is_success() {
        return Err(FetchAttemptError::terminal(anyhow!(
            "non-success getTransaction status {status} for {signature} via {redacted_http_url}"
        )));
    }

    let response: Value = response.json().await.map_err(|error| {
        FetchAttemptError::terminal(anyhow!(
            "failed parsing getTransaction json for {signature} via {redacted_http_url}: {error}"
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
        amount_in: amount_in.amount,
        amount_out: amount_out.amount,
        exact_amounts: HeliusWsSource::build_exact_swap_amounts(&amount_in, &amount_out),
        program_ids: program_ids.into_iter().collect(),
        dex_hint,
        ts_utc,
    }))
}
