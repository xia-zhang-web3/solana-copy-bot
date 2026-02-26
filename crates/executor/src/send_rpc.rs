use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use serde_json::{json, Value};
use tracing::warn;

use crate::{
    http_utils::{
        body_text_was_truncated, classify_request_error, read_response_body_limited,
        redacted_endpoint_label,
        truncate_detail_chars, MAX_HTTP_ERROR_BODY_DETAIL_CHARS, MAX_HTTP_ERROR_BODY_READ_BYTES,
        MAX_HTTP_JSON_BODY_READ_BYTES,
    },
    key_validation::validate_signature_like,
    route_backend::SendRpcEndpointChainError,
    submit_deadline::SubmitDeadline,
    AppState, Reject,
};

fn validate_send_rpc_deadline_context(
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<(), Reject> {
    if submit_deadline.is_none() {
        return Err(Reject::terminal(
            "invalid_request_body",
            "submit send RPC missing deadline at send-rpc boundary",
        ));
    }
    Ok(())
}

pub(crate) async fn send_signed_transaction_via_rpc(
    state: &AppState,
    route: &str,
    signed_tx_base64: &str,
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<String, Reject> {
    validate_send_rpc_deadline_context(submit_deadline)?;
    let backend = state.config.route_backends.get(route).ok_or_else(|| {
        Reject::terminal(
            "route_not_allowed",
            format!("route={} not configured", route),
        )
    })?;

    let signed_tx_base64 = signed_tx_base64.trim();
    if signed_tx_base64.is_empty() {
        return Err(Reject::retryable(
            "submit_adapter_invalid_response",
            "signed_tx_base64 must be non-empty when present",
        ));
    }
    let signed_tx_bytes = BASE64_STANDARD.decode(signed_tx_base64).map_err(|error| {
        Reject::retryable(
            "submit_adapter_invalid_response",
            format!("signed_tx_base64 is not valid base64: {}", error),
        )
    })?;
    let expected_signature = extract_expected_signature_from_signed_tx_bytes(
        signed_tx_bytes.as_slice(),
    )
    .map_err(|error| {
        Reject::retryable(
            "submit_adapter_invalid_response",
            format!(
                "signed_tx_base64 does not contain valid transaction signature bytes: {}",
                error
            ),
        )
    })?;

    // Defense-in-depth: keep this invariant local to send path even though
    // config parsing and endpoint_chain_checked also enforce it.
    if backend.send_rpc_url.is_none() && backend.send_rpc_fallback_url.is_some() {
        return Err(reject_send_rpc_fallback_without_primary(route));
    }

    let endpoints = backend.send_rpc_endpoint_chain_checked().map_err(|error| match error {
        SendRpcEndpointChainError::FallbackWithoutPrimary => {
            reject_send_rpc_fallback_without_primary(route)
        }
    })?;
    if endpoints.is_empty() {
        return Err(Reject::terminal(
            "adapter_send_rpc_not_configured",
            format!(
                "route={} missing send RPC URL (set COPYBOT_EXECUTOR_ROUTE_{}_SEND_RPC_URL or COPYBOT_EXECUTOR_SEND_RPC_URL)",
                route,
                route.to_ascii_uppercase()
            ),
        ));
    }

    let mut last_retryable: Option<Reject> = None;
    for (attempt_idx, (url, auth_token)) in endpoints.iter().enumerate() {
        let endpoint_label = redacted_endpoint_label(url);
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                signed_tx_base64,
                {
                    "encoding": "base64",
                    "skipPreflight": false,
                    "maxRetries": 0
                }
            ]
        });
        let mut request = state.http.post(*url).json(&payload);
        if let Some(deadline) = submit_deadline {
            let remaining = deadline.remaining_timeout("send_rpc")?;
            request = request.timeout(remaining);
        }
        if let Some(token) = *auth_token {
            request = request.bearer_auth(token);
        }
        let response = match request.send().await {
            Ok(value) => value,
            Err(error) => {
                let reject = Reject::retryable(
                    "send_rpc_unavailable",
                    format!(
                        "send RPC request failed endpoint={} class={}",
                        endpoint_label,
                        classify_request_error(&error)
                    ),
                );
                if attempt_idx + 1 < endpoints.len() {
                    warn!(
                        route = %route,
                        endpoint = %endpoint_label,
                        attempt = attempt_idx + 1,
                        total = endpoints.len(),
                        "retryable send RPC failure, trying fallback endpoint"
                    );
                    last_retryable = Some(reject);
                    continue;
                }
                return Err(reject);
            }
        };
        let status = response.status();
        if !status.is_success() {
            let body_text =
                read_response_body_limited(response, MAX_HTTP_ERROR_BODY_READ_BYTES).await;
            let body_detail =
                truncate_detail_chars(body_text.as_str(), MAX_HTTP_ERROR_BODY_DETAIL_CHARS);
            let reject = if status.as_u16() == 429 || status.is_server_error() {
                Reject::retryable(
                    "send_rpc_http_unavailable",
                    format!(
                        "send RPC status={} endpoint={} body={}",
                        status, endpoint_label, body_detail
                    ),
                )
            } else {
                Reject::terminal(
                    "send_rpc_http_rejected",
                    format!(
                        "send RPC status={} endpoint={} body={}",
                        status, endpoint_label, body_detail
                    ),
                )
            };
            if reject.retryable && attempt_idx + 1 < endpoints.len() {
                warn!(
                    route = %route,
                    endpoint = %endpoint_label,
                    status = %status,
                    attempt = attempt_idx + 1,
                    total = endpoints.len(),
                    "retryable send RPC HTTP status, trying fallback endpoint"
                );
                last_retryable = Some(reject);
                continue;
            }
            return Err(reject);
        }
        let body_text = read_response_body_limited(response, MAX_HTTP_JSON_BODY_READ_BYTES).await;
        let body: Value = serde_json::from_str(body_text.as_str()).map_err(|error| {
            if body_text_was_truncated(body_text.as_str()) {
                return Reject::terminal(
                    "send_rpc_response_too_large",
                    format!(
                        "send RPC response exceeded max bytes endpoint={} max_bytes={} err={}",
                        endpoint_label, MAX_HTTP_JSON_BODY_READ_BYTES, error
                    ),
                );
            }
            Reject::terminal(
                "send_rpc_invalid_json",
                format!(
                    "send RPC response invalid JSON endpoint={} err={}",
                    endpoint_label, error
                ),
            )
        })?;
        if let Some(error_payload) = body.get("error") {
            if !error_payload.is_null() {
                let payload_detail = truncate_detail_chars(
                    error_payload.to_string().as_str(),
                    MAX_HTTP_ERROR_BODY_DETAIL_CHARS,
                );
                match classify_send_rpc_error_payload(error_payload) {
                    SendRpcErrorPayloadDisposition::AlreadyProcessed => {
                        warn!(
                            route = %route,
                            endpoint = %endpoint_label,
                            signature = %expected_signature,
                            "send RPC returned already-processed error payload; accepting expected signature"
                        );
                        return Ok(expected_signature.clone());
                    }
                    SendRpcErrorPayloadDisposition::Retryable => {
                        let reject = Reject::retryable(
                            "send_rpc_error_payload_retryable",
                            format!(
                                "send RPC returned retryable error endpoint={} payload={}",
                                endpoint_label, payload_detail
                            ),
                        );
                        if attempt_idx + 1 < endpoints.len() {
                            warn!(
                                route = %route,
                                endpoint = %endpoint_label,
                                attempt = attempt_idx + 1,
                                total = endpoints.len(),
                                "send RPC retryable error payload, trying fallback endpoint"
                            );
                            last_retryable = Some(reject);
                            continue;
                        }
                        return Err(reject);
                    }
                    SendRpcErrorPayloadDisposition::BlockhashExpired => {
                        return Err(Reject::terminal(
                            "executor_blockhash_expired",
                            format!(
                                "send RPC returned blockhash-expired error endpoint={} payload={}",
                                endpoint_label, payload_detail
                            ),
                        ));
                    }
                    SendRpcErrorPayloadDisposition::Terminal => {
                        return Err(Reject::terminal(
                            "send_rpc_error_payload_terminal",
                            format!(
                                "send RPC returned terminal error endpoint={} payload={}",
                                endpoint_label, payload_detail
                            ),
                        ));
                    }
                }
            }
        }
        let signature = body
            .get("result")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                Reject::terminal(
                    "send_rpc_invalid_response",
                    format!(
                        "send RPC response missing result signature endpoint={}",
                        endpoint_label
                    ),
                )
            })?;
        validate_signature_like(signature).map_err(|error| {
            Reject::terminal(
                "send_rpc_invalid_response",
                format!("send RPC result signature is invalid: {}", error),
            )
        })?;
        if signature != expected_signature {
            return Err(Reject::terminal(
                "send_rpc_signature_mismatch",
                format!(
                    "send RPC returned signature mismatch endpoint={} expected={} actual={}",
                    endpoint_label, expected_signature, signature
                ),
            ));
        }
        return Ok(signature.to_string());
    }

    Err(last_retryable.unwrap_or_else(|| {
        Reject::retryable(
            "send_rpc_unavailable",
            format!(
                "send RPC failed for all configured endpoints route={}",
                route
            ),
        )
    }))
}

fn reject_send_rpc_fallback_without_primary(route: &str) -> Reject {
    Reject::terminal(
        "adapter_send_rpc_not_configured",
        format!(
            "route={} has send RPC fallback URL but missing primary send RPC URL (set COPYBOT_EXECUTOR_ROUTE_{}_SEND_RPC_URL or COPYBOT_EXECUTOR_SEND_RPC_URL)",
            route,
            route.to_ascii_uppercase()
        ),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SendRpcErrorPayloadDisposition {
    AlreadyProcessed,
    BlockhashExpired,
    Retryable,
    Terminal,
}

fn classify_send_rpc_error_payload(error_payload: &Value) -> SendRpcErrorPayloadDisposition {
    let code = error_payload.get("code").and_then(Value::as_i64);
    let payload_lower = error_payload.to_string().to_ascii_lowercase();

    if code == Some(-32002)
        && (payload_lower.contains("already processed")
            || payload_lower.contains("already confirmed")
            || payload_lower.contains("already finalized"))
    {
        return SendRpcErrorPayloadDisposition::AlreadyProcessed;
    }

    if payload_lower.contains("blockhash not found")
        || payload_lower.contains("block height exceeded")
        || payload_lower.contains("transaction is too old")
        || payload_lower.contains("recent blockhash")
    {
        return SendRpcErrorPayloadDisposition::BlockhashExpired;
    }

    if code == Some(-32005)
        || payload_lower.contains("node is unhealthy")
        || payload_lower.contains("temporarily unavailable")
        || payload_lower.contains("rate limit")
        || payload_lower.contains("try again")
        || payload_lower.contains("timed out")
        || payload_lower.contains("timeout")
    {
        return SendRpcErrorPayloadDisposition::Retryable;
    }

    SendRpcErrorPayloadDisposition::Terminal
}

fn extract_expected_signature_from_signed_tx_bytes(bytes: &[u8]) -> Result<String> {
    let (signature_count, prefix_len) = parse_shortvec_len(bytes)?;
    if signature_count == 0 {
        return Err(anyhow!("transaction contains zero signatures"));
    }
    let signatures_len = signature_count
        .checked_mul(64)
        .ok_or_else(|| anyhow!("signature count overflow"))?;
    let required_len = prefix_len
        .checked_add(signatures_len)
        .ok_or_else(|| anyhow!("signature section length overflow"))?;
    if bytes.len() < required_len {
        return Err(anyhow!("transaction signature section is truncated"));
    }
    let first_signature = &bytes[prefix_len..prefix_len + 64];
    Ok(bs58::encode(first_signature).into_string())
}

fn parse_shortvec_len(bytes: &[u8]) -> Result<(usize, usize)> {
    if bytes.is_empty() {
        return Err(anyhow!("shortvec is empty"));
    }
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for (index, byte) in bytes.iter().copied().enumerate() {
        let part = u64::from(byte & 0x7f);
        let shifted = part
            .checked_shl(shift)
            .ok_or_else(|| anyhow!("shortvec shift overflow"))?;
        value = value
            .checked_add(shifted)
            .ok_or_else(|| anyhow!("shortvec value overflow"))?;
        if byte & 0x80 == 0 {
            let parsed = usize::try_from(value).map_err(|_| anyhow!("shortvec exceeds usize"))?;
            return Ok((parsed, index + 1));
        }
        shift = shift
            .checked_add(7)
            .ok_or_else(|| anyhow!("shortvec shift overflow"))?;
        if shift >= 64 {
            return Err(anyhow!("shortvec uses too many bytes"));
        }
    }
    Err(anyhow!("shortvec is truncated"))
}

#[cfg(test)]
mod tests {
    use super::validate_send_rpc_deadline_context;
    use crate::submit_deadline::SubmitDeadline;

    #[test]
    fn send_rpc_deadline_context_rejects_missing_deadline() {
        let reject = validate_send_rpc_deadline_context(None)
            .expect_err("send RPC without deadline must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[test]
    fn send_rpc_deadline_context_accepts_present_deadline() {
        let submit_deadline = SubmitDeadline::new(1_000);
        validate_send_rpc_deadline_context(Some(&submit_deadline))
            .expect("send RPC with deadline should pass");
    }
}
