use serde_json::Value;
use tracing::{debug, warn};

use crate::{
    http_utils::{classify_request_error, redacted_endpoint_label},
    submit_deadline::SubmitDeadline,
    AppState, Reject,
};
use crate::route_backend::UpstreamAction;

pub(crate) async fn forward_to_upstream(
    state: &AppState,
    route: &str,
    action: UpstreamAction,
    raw_body: &[u8],
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<Value, Reject> {
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
            let body_text = response.text().await.unwrap_or_default();
            let retryable = status.as_u16() == 429 || status.is_server_error();
            let reject = if retryable {
                Reject::retryable(
                    "upstream_http_unavailable",
                    format!(
                        "upstream {} status={} endpoint={} body={}",
                        action.as_str(),
                        status,
                        endpoint_label,
                        body_text
                    ),
                )
            } else {
                Reject::terminal(
                    "upstream_http_rejected",
                    format!(
                        "upstream {} status={} endpoint={} body={}",
                        action.as_str(),
                        status,
                        endpoint_label,
                        body_text
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
