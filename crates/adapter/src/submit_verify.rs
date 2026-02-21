use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

use crate::{
    http_utils::{
        classify_request_error, endpoint_identity, redacted_endpoint_label, validate_endpoint_url,
    },
    optional_non_empty_env, parse_bool_env, parse_u64_env, AppState, Reject,
    DEFAULT_SUBMIT_VERIFY_ATTEMPTS, DEFAULT_SUBMIT_VERIFY_INTERVAL_MS,
};

#[derive(Clone, Debug)]
pub(crate) struct SubmitSignatureVerifyConfig {
    pub(crate) endpoints: Vec<String>,
    pub(crate) attempts: u64,
    pub(crate) interval_ms: u64,
    pub(crate) strict: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum SubmitSignatureVerification {
    Skipped,
    Seen { confirmation_status: String },
    Unseen { reason: String },
}

pub(crate) async fn verify_submitted_signature_visibility(
    state: &AppState,
    route: &str,
    tx_signature: &str,
) -> std::result::Result<SubmitSignatureVerification, Reject> {
    let Some(config) = state.config.submit_signature_verify.as_ref() else {
        return Ok(SubmitSignatureVerification::Skipped);
    };

    let mut last_reason = String::from("signature status row is missing");
    for attempt_idx in 0..config.attempts {
        for endpoint in &config.endpoints {
            let endpoint_label = redacted_endpoint_label(endpoint.as_str());
            let payload = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignatureStatuses",
                "params": [[tx_signature], {"searchTransactionHistory": true}]
            });
            let response = match state.http.post(endpoint).json(&payload).send().await {
                Ok(value) => value,
                Err(error) => {
                    last_reason = format!(
                        "rpc send failed endpoint={} class={}",
                        endpoint_label,
                        classify_request_error(&error)
                    );
                    continue;
                }
            };
            if !response.status().is_success() {
                last_reason = format!(
                    "rpc status={} endpoint={}",
                    response.status(),
                    endpoint_label
                );
                continue;
            }
            let body: Value = match response.json().await {
                Ok(value) => value,
                Err(_) => {
                    last_reason = format!("rpc invalid_json endpoint={}", endpoint_label);
                    continue;
                }
            };
            if body
                .get("error")
                .map(Value::is_null)
                .map(|value| !value)
                .unwrap_or(false)
            {
                last_reason = format!("rpc error payload endpoint={}", endpoint_label);
                continue;
            }

            let status_row = body
                .get("result")
                .and_then(|result| result.get("value"))
                .and_then(|value| value.get(0));
            let Some(status_row) = status_row else {
                last_reason = format!("signature status missing endpoint={}", endpoint_label);
                continue;
            };
            if status_row.is_null() {
                last_reason = format!("signature status pending endpoint={}", endpoint_label);
                continue;
            }
            if let Some(err_payload) = status_row.get("err") {
                if !err_payload.is_null() {
                    return Err(Reject::terminal(
                        "upstream_submit_failed_onchain",
                        format!(
                            "tx_signature={} has on-chain err={} endpoint={}",
                            tx_signature, err_payload, endpoint_label
                        ),
                    ));
                }
            }
            let confirmation_status = status_row
                .get("confirmationStatus")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("present")
                .to_string();
            return Ok(SubmitSignatureVerification::Seen {
                confirmation_status,
            });
        }
        if attempt_idx + 1 < config.attempts {
            sleep(Duration::from_millis(config.interval_ms)).await;
        }
    }

    if config.strict {
        return Err(Reject::retryable(
            "upstream_submit_signature_unseen",
            format!(
                "tx_signature={} not visible via submit verify RPC after attempts={} route={} reason={}",
                tx_signature, config.attempts, route, last_reason
            ),
        ));
    }

    warn!(
        route = %route,
        tx_signature,
        attempts = config.attempts,
        reason = %last_reason,
        "submit signature verification could not observe tx signature; continuing because strict mode is disabled"
    );
    Ok(SubmitSignatureVerification::Unseen {
        reason: last_reason,
    })
}

pub(crate) fn submit_signature_verification_to_json(value: &SubmitSignatureVerification) -> Value {
    match value {
        SubmitSignatureVerification::Skipped => json!({
            "enabled": false,
        }),
        SubmitSignatureVerification::Seen {
            confirmation_status,
        } => json!({
            "enabled": true,
            "seen": true,
            "confirmation_status": confirmation_status,
        }),
        SubmitSignatureVerification::Unseen { reason } => json!({
            "enabled": true,
            "seen": false,
            "reason": reason,
        }),
    }
}

pub(crate) fn parse_submit_signature_verify_config() -> Result<Option<SubmitSignatureVerifyConfig>>
{
    let primary = optional_non_empty_env("COPYBOT_ADAPTER_SUBMIT_VERIFY_RPC_URL");
    let fallback = optional_non_empty_env("COPYBOT_ADAPTER_SUBMIT_VERIFY_RPC_FALLBACK_URL");
    let attempts = parse_u64_env(
        "COPYBOT_ADAPTER_SUBMIT_VERIFY_ATTEMPTS",
        DEFAULT_SUBMIT_VERIFY_ATTEMPTS,
    )?;
    let interval_ms = parse_u64_env(
        "COPYBOT_ADAPTER_SUBMIT_VERIFY_INTERVAL_MS",
        DEFAULT_SUBMIT_VERIFY_INTERVAL_MS,
    )?;
    let strict = parse_bool_env("COPYBOT_ADAPTER_SUBMIT_VERIFY_STRICT", false);
    build_submit_signature_verify_config(primary, fallback, attempts, interval_ms, strict)
}

pub(crate) fn build_submit_signature_verify_config(
    primary: Option<String>,
    fallback: Option<String>,
    attempts: u64,
    interval_ms: u64,
    strict: bool,
) -> Result<Option<SubmitSignatureVerifyConfig>> {
    if primary.is_none() && fallback.is_none() {
        return Ok(None);
    }

    let Some(primary_url) = primary else {
        return Err(anyhow!(
            "COPYBOT_ADAPTER_SUBMIT_VERIFY_RPC_FALLBACK_URL requires COPYBOT_ADAPTER_SUBMIT_VERIFY_RPC_URL"
        ));
    };
    validate_endpoint_url(primary_url.as_str())
        .map_err(|error| anyhow!("invalid COPYBOT_ADAPTER_SUBMIT_VERIFY_RPC_URL: {}", error))?;

    let mut endpoints = vec![primary_url];
    if let Some(fallback_url) = fallback {
        validate_endpoint_url(fallback_url.as_str()).map_err(|error| {
            anyhow!(
                "invalid COPYBOT_ADAPTER_SUBMIT_VERIFY_RPC_FALLBACK_URL: {}",
                error
            )
        })?;
        if endpoint_identity(fallback_url.as_str())? == endpoint_identity(endpoints[0].as_str())? {
            return Err(anyhow!(
                "COPYBOT_ADAPTER_SUBMIT_VERIFY_RPC_FALLBACK_URL must resolve to distinct endpoint"
            ));
        }
        endpoints.push(fallback_url);
    }

    if attempts == 0 || attempts > 20 {
        return Err(anyhow!(
            "COPYBOT_ADAPTER_SUBMIT_VERIFY_ATTEMPTS must be in 1..=20"
        ));
    }
    if interval_ms == 0 || interval_ms > 60_000 {
        return Err(anyhow!(
            "COPYBOT_ADAPTER_SUBMIT_VERIFY_INTERVAL_MS must be in 1..=60000"
        ));
    }

    Ok(Some(SubmitSignatureVerifyConfig {
        endpoints,
        attempts,
        interval_ms,
        strict,
    }))
}
