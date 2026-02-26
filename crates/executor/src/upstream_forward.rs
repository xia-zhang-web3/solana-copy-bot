use serde_json::Value;
use tracing::{debug, warn};

use crate::{
    http_utils::{
        classify_request_error, read_response_body_limited, redacted_endpoint_label,
        truncate_detail_chars, MAX_HTTP_ERROR_BODY_DETAIL_CHARS, MAX_HTTP_ERROR_BODY_READ_BYTES,
    },
    submit_deadline::SubmitDeadline,
    AppState, Reject,
};
use crate::route_backend::UpstreamAction;

fn validate_upstream_forward_deadline_context(
    action: UpstreamAction,
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<(), Reject> {
    match action {
        UpstreamAction::Submit if submit_deadline.is_none() => Err(Reject::terminal(
            "invalid_request_body",
            "submit upstream forward missing deadline at upstream-forward boundary",
        )),
        UpstreamAction::Simulate if submit_deadline.is_some() => Err(Reject::terminal(
            "invalid_request_body",
            "simulate upstream forward must not include submit deadline at upstream-forward boundary",
        )),
        _ => Ok(()),
    }
}

pub(crate) async fn forward_to_upstream(
    state: &AppState,
    route: &str,
    action: UpstreamAction,
    raw_body: &[u8],
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<Value, Reject> {
    validate_upstream_forward_deadline_context(action, submit_deadline)?;
    let backend = state.config.route_backends.get(route).ok_or_else(|| {
        Reject::terminal(
            "route_not_allowed",
            format!("route={} not configured", route),
        )
    })?;

    let endpoints = backend.endpoint_chain(action);

    let mut last_retryable: Option<Reject> = None;
    for (attempt_idx, url) in endpoints.iter().enumerate() {
        let endpoint_label = redacted_endpoint_label(url);
        debug!(
            route = %route,
            action = %action.as_str(),
            endpoint = %endpoint_label,
            attempt = attempt_idx + 1,
            total = endpoints.len(),
            "forwarding executor request route backend"
        );

        let mut request = state
            .http
            .post(*url)
            .header("content-type", "application/json")
            .body(raw_body.to_vec());
        if let Some(deadline) = submit_deadline {
            let remaining = deadline.remaining_timeout(match action {
                UpstreamAction::Simulate => "upstream_simulate",
                UpstreamAction::Submit => "upstream_submit",
            })?;
            request = request.timeout(remaining);
        }
        let selected_auth_token = backend.auth_token_for_attempt(action, attempt_idx);
        if let Some(token) = selected_auth_token {
            request = request.bearer_auth(token);
        }

        let response = match request.send().await {
            Ok(value) => value,
            Err(error) => {
                let code = if error.is_timeout() || error.is_connect() || error.is_request() {
                    "upstream_unavailable"
                } else {
                    "upstream_request_failed"
                };
                let reject = Reject::retryable(
                    code,
                    format!(
                        "upstream {} request failed endpoint={} class={}",
                        action.as_str(),
                        endpoint_label,
                        classify_request_error(&error)
                    ),
                );
                if attempt_idx + 1 < endpoints.len() {
                    warn!(
                        route = %route,
                        action = %action.as_str(),
                        endpoint = %endpoint_label,
                        attempt = attempt_idx + 1,
                        total = endpoints.len(),
                        code = %reject.code,
                        "retryable upstream request failure, trying fallback endpoint"
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
            let retryable = status.as_u16() == 429 || status.is_server_error();
            let reject = if retryable {
                Reject::retryable(
                    "upstream_http_unavailable",
                    format!(
                        "upstream {} status={} endpoint={} body={}",
                        action.as_str(), status, endpoint_label, body_detail
                    ),
                )
            } else {
                Reject::terminal(
                    "upstream_http_rejected",
                    format!(
                        "upstream {} status={} endpoint={} body={}",
                        action.as_str(), status, endpoint_label, body_detail
                    ),
                )
            };
            if reject.retryable && attempt_idx + 1 < endpoints.len() {
                warn!(
                    route = %route,
                    action = %action.as_str(),
                    endpoint = %endpoint_label,
                    status = %status,
                    attempt = attempt_idx + 1,
                    total = endpoints.len(),
                    "retryable upstream HTTP status, trying fallback endpoint"
                );
                last_retryable = Some(reject);
                continue;
            }
            return Err(reject);
        }

        let body: Value = response.json().await.map_err(|error| {
            Reject::terminal(
                "upstream_invalid_json",
                format!(
                    "upstream {} response invalid JSON endpoint={} err={}",
                    action.as_str(),
                    endpoint_label,
                    error
                ),
            )
        })?;

        return Ok(body);
    }

    Err(last_retryable.unwrap_or_else(|| {
        Reject::retryable(
            "upstream_unavailable",
            format!(
                "upstream {} failed for all configured endpoints route={}",
                action.as_str(),
                route
            ),
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::validate_upstream_forward_deadline_context;
    use crate::route_backend::UpstreamAction;
    use crate::submit_deadline::SubmitDeadline;

    #[test]
    fn upstream_forward_deadline_context_rejects_submit_without_deadline() {
        let reject = validate_upstream_forward_deadline_context(UpstreamAction::Submit, None)
            .expect_err("submit without deadline must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[test]
    fn upstream_forward_deadline_context_rejects_simulate_with_deadline() {
        let deadline = SubmitDeadline::new(1_000);
        let reject =
            validate_upstream_forward_deadline_context(UpstreamAction::Simulate, Some(&deadline))
                .expect_err("simulate with deadline must reject");
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("must not include submit deadline"));
    }

    #[test]
    fn upstream_forward_deadline_context_accepts_submit_with_deadline() {
        let deadline = SubmitDeadline::new(1_000);
        validate_upstream_forward_deadline_context(UpstreamAction::Submit, Some(&deadline))
            .expect("submit with deadline should pass");
    }

    #[test]
    fn upstream_forward_deadline_context_accepts_simulate_without_deadline() {
        validate_upstream_forward_deadline_context(UpstreamAction::Simulate, None)
            .expect("simulate without deadline should pass");
    }
}
