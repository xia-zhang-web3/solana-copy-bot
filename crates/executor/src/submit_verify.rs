use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

use crate::{
    http_utils::{
        classify_request_error, read_response_body_limited, redacted_endpoint_label,
        truncate_detail_chars, MAX_HTTP_ERROR_BODY_DETAIL_CHARS, MAX_HTTP_ERROR_BODY_READ_BYTES,
        MAX_HTTP_JSON_BODY_READ_BYTES,
    },
    key_validation::validate_signature_like,
    submit_deadline::SubmitDeadline,
    AppState, Reject,
};

#[derive(Debug, Clone)]
pub(crate) enum SubmitSignatureVerification {
    Skipped,
    Seen { confirmation_status: String },
    Unseen { reason: String },
}

fn validate_submit_verify_deadline_context(
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<(), Reject> {
    if submit_deadline.is_none() {
        return Err(Reject::terminal(
            "invalid_request_body",
            "submit signature verify missing deadline at submit-verify boundary",
        ));
    }
    Ok(())
}

fn validate_submit_verify_signature_context(tx_signature: &str) -> std::result::Result<(), Reject> {
    validate_signature_like(tx_signature).map_err(|error| {
        Reject::terminal(
            "invalid_request_body",
            format!(
                "submit signature verify invalid tx_signature at submit-verify boundary: {}",
                error
            ),
        )
    })
}

pub(crate) async fn verify_submitted_signature_visibility(
    state: &AppState,
    route: &str,
    tx_signature: &str,
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<SubmitSignatureVerification, Reject> {
    validate_submit_verify_deadline_context(submit_deadline)?;
    validate_submit_verify_signature_context(tx_signature)?;
    let Some(config) = state.config.submit_signature_verify.as_ref() else {
        return Ok(SubmitSignatureVerification::Skipped);
    };

    let mut last_reason = String::from("signature status row is missing");
    for attempt_idx in 0..config.attempts {
        for (endpoint_idx, endpoint) in config.endpoints.iter().enumerate() {
            let endpoint_label = redacted_endpoint_label(endpoint.as_str());
            let mut set_reason_and_continue = |reason: String| {
                last_reason = reason;
                if endpoint_idx + 1 < config.endpoints.len() {
                    warn!(
                        route = %route,
                        endpoint = %endpoint_label,
                        attempt = attempt_idx + 1,
                        total_attempts = config.attempts,
                        endpoint_try = endpoint_idx + 1,
                        endpoint_total = config.endpoints.len(),
                        reason = %last_reason,
                        "submit verify endpoint failed, trying fallback endpoint"
                    );
                }
            };
            let payload = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignatureStatuses",
                "params": [[tx_signature], {"searchTransactionHistory": true}]
            });
            let mut request = state.http.post(endpoint).json(&payload);
            if let Some(deadline) = submit_deadline {
                let remaining = deadline.remaining_timeout("submit_verify")?;
                request = request.timeout(remaining);
            }
            let response = match request.send().await {
                Ok(value) => value,
                Err(error) => {
                    set_reason_and_continue(format!(
                        "rpc send failed endpoint={} class={}",
                        endpoint_label,
                        classify_request_error(&error)
                    ));
                    continue;
                }
            };
            if !response.status().is_success() {
                let status = response.status();
                let body =
                    read_response_body_limited(response, MAX_HTTP_ERROR_BODY_READ_BYTES).await;
                let body_detail =
                    truncate_detail_chars(body.text.as_str(), MAX_HTTP_ERROR_BODY_DETAIL_CHARS);
                set_reason_and_continue(format!(
                    "rpc status={} endpoint={} body={}",
                    status, endpoint_label, body_detail
                ));
                continue;
            }
            if let Some(content_length) = response.content_length() {
                if content_length > MAX_HTTP_JSON_BODY_READ_BYTES as u64 {
                    set_reason_and_continue(format!(
                        "rpc response_too_large endpoint={} declared_content_length={} max_bytes={}",
                        endpoint_label, content_length, MAX_HTTP_JSON_BODY_READ_BYTES
                    ));
                    continue;
                }
            }
            let body_read = read_response_body_limited(response, MAX_HTTP_JSON_BODY_READ_BYTES).await;
            if let Some(read_error_class) = body_read.read_error_class {
                set_reason_and_continue(format!(
                    "rpc response_read_failed endpoint={} class={}",
                    endpoint_label, read_error_class
                ));
                continue;
            }
            if body_read.was_truncated {
                set_reason_and_continue(format!(
                    "rpc response_too_large endpoint={} max_bytes={}",
                    endpoint_label, MAX_HTTP_JSON_BODY_READ_BYTES
                ));
                continue;
            }
            let body: Value = match serde_json::from_slice(body_read.bytes.as_slice()) {
                Ok(value) => value,
                Err(error) => {
                    set_reason_and_continue(format!(
                        "rpc invalid_json endpoint={} err={}",
                        endpoint_label, error
                    ));
                    continue;
                }
            };
            if body
                .get("error")
                .map(Value::is_null)
                .map(|value| !value)
                .unwrap_or(false)
            {
                set_reason_and_continue(format!("rpc error payload endpoint={}", endpoint_label));
                continue;
            }

            let status_row = body
                .get("result")
                .and_then(|result| result.get("value"))
                .and_then(|value| value.get(0));
            let Some(status_row) = status_row else {
                set_reason_and_continue(format!(
                    "signature status missing endpoint={}",
                    endpoint_label
                ));
                continue;
            };
            if status_row.is_null() {
                set_reason_and_continue(format!(
                    "signature status pending endpoint={}",
                    endpoint_label
                ));
                continue;
            }
            if let Some(err_payload) = status_row.get("err") {
                if !err_payload.is_null() {
                    let err_detail = truncate_detail_chars(
                        err_payload.to_string().as_str(),
                        MAX_HTTP_ERROR_BODY_DETAIL_CHARS,
                    );
                    return Err(Reject::terminal(
                        "upstream_submit_failed_onchain",
                        format!(
                            "tx_signature={} has on-chain err={} endpoint={}",
                            tx_signature, err_detail, endpoint_label
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
            if let Some(deadline) = submit_deadline {
                let remaining = deadline.remaining_timeout("submit_verify_sleep")?;
                let remaining_ms = remaining.as_millis().min(u128::from(u64::MAX)) as u64;
                if remaining_ms == 0 {
                    return Err(Reject::retryable(
                        "executor_submit_timeout_budget_exceeded",
                        "submit timeout budget exceeded before submit verification retry sleep",
                    ));
                }
                sleep(Duration::from_millis(config.interval_ms.min(remaining_ms))).await;
            } else {
                sleep(Duration::from_millis(config.interval_ms)).await;
            }
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

#[cfg(test)]
mod tests {
    use super::{
        validate_submit_verify_deadline_context, validate_submit_verify_signature_context,
    };
    use crate::submit_deadline::SubmitDeadline;

    #[test]
    fn submit_verify_deadline_context_rejects_missing_deadline() {
        let reject = validate_submit_verify_deadline_context(None)
            .expect_err("submit verify without deadline must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[test]
    fn submit_verify_deadline_context_accepts_present_deadline() {
        let submit_deadline = SubmitDeadline::new(1_000);
        validate_submit_verify_deadline_context(Some(&submit_deadline))
            .expect("submit verify with deadline should pass");
    }

    #[test]
    fn submit_verify_signature_context_rejects_invalid_signature() {
        let reject = validate_submit_verify_signature_context("not-base58")
            .expect_err("submit verify with invalid signature must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("invalid tx_signature"));
    }

    #[test]
    fn submit_verify_signature_context_accepts_valid_signature() {
        let tx_signature = bs58::encode([7u8; 64]).into_string();
        validate_submit_verify_signature_context(tx_signature.as_str())
            .expect("submit verify with valid signature should pass");
    }
}
