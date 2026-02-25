use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

use crate::{
    http_utils::{classify_request_error, redacted_endpoint_label},
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

pub(crate) async fn verify_submitted_signature_visibility(
    state: &AppState,
    route: &str,
    tx_signature: &str,
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<SubmitSignatureVerification, Reject> {
    validate_submit_verify_deadline_context(submit_deadline)?;
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
            let mut request = state.http.post(endpoint).json(&payload);
            if let Some(deadline) = submit_deadline {
                let remaining = deadline.remaining_timeout("submit_verify")?;
                request = request.timeout(remaining);
            }
            let response = match request.send().await {
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
    use super::validate_submit_verify_deadline_context;
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
}
