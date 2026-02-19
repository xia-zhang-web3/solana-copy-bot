use chrono::{DateTime, Utc};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fmt;
use std::time::Duration as StdDuration;
use uuid::Uuid;

use crate::intent::ExecutionIntent;

#[derive(Debug, Clone)]
pub struct SubmitResult {
    pub route: String,
    pub tx_signature: String,
    pub submitted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitErrorKind {
    Retryable,
    Terminal,
}

#[derive(Debug, Clone)]
pub struct SubmitError {
    pub kind: SubmitErrorKind,
    pub code: String,
    pub detail: String,
}

impl SubmitError {
    pub fn retryable(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            kind: SubmitErrorKind::Retryable,
            code: code.into(),
            detail: detail.into(),
        }
    }

    pub fn terminal(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            kind: SubmitErrorKind::Terminal,
            code: code.into(),
            detail: detail.into(),
        }
    }
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "submit_error kind={:?} code={} detail={}",
            self.kind, self.code, self.detail
        )
    }
}

impl std::error::Error for SubmitError {}

pub trait OrderSubmitter {
    fn submit(
        &self,
        intent: &ExecutionIntent,
        client_order_id: &str,
        route: &str,
    ) -> std::result::Result<SubmitResult, SubmitError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PaperOrderSubmitter;

impl OrderSubmitter for PaperOrderSubmitter {
    fn submit(
        &self,
        intent: &ExecutionIntent,
        client_order_id: &str,
        route: &str,
    ) -> std::result::Result<SubmitResult, SubmitError> {
        let sig = format!(
            "paper:{}:{}:{}",
            intent.side.as_str(),
            client_order_id,
            Uuid::new_v4().simple()
        );
        Ok(SubmitResult {
            route: route.to_string(),
            tx_signature: sig,
            submitted_at: Utc::now(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct FailClosedOrderSubmitter {
    code: String,
    detail: String,
}

impl FailClosedOrderSubmitter {
    pub fn new(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
        }
    }
}

impl OrderSubmitter for FailClosedOrderSubmitter {
    fn submit(
        &self,
        _intent: &ExecutionIntent,
        _client_order_id: &str,
        _route: &str,
    ) -> std::result::Result<SubmitResult, SubmitError> {
        Err(SubmitError::terminal(
            self.code.clone(),
            self.detail.clone(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct AdapterOrderSubmitter {
    endpoints: Vec<String>,
    auth_token: Option<String>,
    allowed_routes: HashSet<String>,
    slippage_bps: f64,
    client: Client,
}

impl AdapterOrderSubmitter {
    pub fn new(
        primary_url: &str,
        fallback_url: &str,
        auth_token: &str,
        allowed_routes: &[String],
        timeout_ms: u64,
        slippage_bps: f64,
    ) -> Option<Self> {
        let mut endpoints = Vec::new();
        let primary = primary_url.trim();
        if !primary.is_empty() {
            endpoints.push(primary.to_string());
        }
        let fallback = fallback_url.trim();
        if !fallback.is_empty() && fallback != primary {
            endpoints.push(fallback.to_string());
        }
        if endpoints.is_empty() || !slippage_bps.is_finite() || slippage_bps < 0.0 {
            return None;
        }

        let allowed_routes = normalize_allowed_routes(allowed_routes);
        if allowed_routes.is_empty() {
            return None;
        }

        let client = match Client::builder()
            .timeout(StdDuration::from_millis(timeout_ms.max(500)))
            .build()
        {
            Ok(value) => value,
            Err(_) => return None,
        };

        let token = auth_token.trim();
        let auth_token = if token.is_empty() {
            None
        } else {
            Some(token.to_string())
        };

        Some(Self {
            endpoints,
            auth_token,
            allowed_routes,
            slippage_bps,
            client,
        })
    }

    fn submit_via_endpoint(
        &self,
        endpoint: &str,
        payload: &Value,
        default_route: &str,
    ) -> std::result::Result<SubmitResult, SubmitError> {
        let mut request = self.client.post(endpoint).json(payload);
        if let Some(token) = self.auth_token.as_deref() {
            request = request.bearer_auth(token);
        }
        let response = request.send().map_err(|error| {
            SubmitError::retryable(
                "submit_adapter_unavailable",
                format!("endpoint={} request_error={}", endpoint, error),
            )
        })?;
        let status = response.status();
        if !status.is_success() {
            let status_code = status.as_u16();
            let body_text = response.text().unwrap_or_default();
            let detail = format!(
                "endpoint={} http_status={} body={}",
                endpoint, status_code, body_text
            );
            if status_code == 429 || status.is_server_error() {
                return Err(SubmitError::retryable(
                    "submit_adapter_http_unavailable",
                    detail,
                ));
            }
            return Err(SubmitError::terminal(
                "submit_adapter_http_rejected",
                detail,
            ));
        }
        let body: Value = response.json().map_err(|error| {
            SubmitError::retryable(
                "submit_adapter_invalid_json",
                format!("endpoint={} parse_error={}", endpoint, error),
            )
        })?;
        parse_adapter_submit_response(&body, default_route).map_err(|error| {
            if matches!(error.kind, SubmitErrorKind::Retryable) {
                SubmitError::retryable(
                    error.code,
                    format!("endpoint={} {}", endpoint, error.detail),
                )
            } else {
                SubmitError::terminal(
                    error.code,
                    format!("endpoint={} {}", endpoint, error.detail),
                )
            }
        })
    }
}

impl OrderSubmitter for AdapterOrderSubmitter {
    fn submit(
        &self,
        intent: &ExecutionIntent,
        client_order_id: &str,
        route: &str,
    ) -> std::result::Result<SubmitResult, SubmitError> {
        let route = normalize_route(route).ok_or_else(|| {
            SubmitError::terminal("route_missing", "execution route is empty for submit")
        })?;
        if !self.allowed_routes.contains(route.as_str()) {
            return Err(SubmitError::terminal(
                "route_not_allowed",
                format!(
                    "route={} is not allowed by execution.submit_allowed_routes",
                    route
                ),
            ));
        }

        let payload = json!({
            "signal_id": intent.signal_id,
            "client_order_id": client_order_id,
            "side": intent.side.as_str(),
            "token": intent.token,
            "notional_sol": intent.notional_sol,
            "signal_ts": intent.signal_ts.to_rfc3339(),
            "route": route,
            "slippage_bps": self.slippage_bps,
        });

        let mut last_retryable_error: Option<SubmitError> = None;
        for endpoint in &self.endpoints {
            match self.submit_via_endpoint(endpoint, &payload, route.as_str()) {
                Ok(result) => return Ok(result),
                Err(error) if matches!(error.kind, SubmitErrorKind::Terminal) => {
                    return Err(error);
                }
                Err(error) => last_retryable_error = Some(error),
            }
        }

        Err(last_retryable_error.unwrap_or_else(|| {
            SubmitError::retryable(
                "submit_adapter_unavailable",
                "all submit adapter endpoints failed".to_string(),
            )
        }))
    }
}

fn parse_adapter_submit_response(
    body: &Value,
    default_route: &str,
) -> std::result::Result<SubmitResult, SubmitError> {
    let status = body
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let ok_flag = body.get("ok").and_then(Value::as_bool);
    let is_reject = status == "reject" || ok_flag == Some(false);
    if is_reject {
        let retryable = body
            .get("retryable")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let code = body
            .get("code")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("submit_adapter_rejected");
        let detail = body
            .get("detail")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("submit adapter rejected order");
        return Err(if retryable {
            SubmitError::retryable(code, detail)
        } else {
            SubmitError::terminal(code, detail)
        });
    }

    let tx_signature = body
        .get("tx_signature")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            SubmitError::retryable(
                "submit_adapter_invalid_response",
                "missing tx_signature in adapter response".to_string(),
            )
        })?;
    let route = body
        .get("route")
        .and_then(Value::as_str)
        .and_then(normalize_route)
        .unwrap_or_else(|| default_route.to_string());
    let submitted_at = body
        .get("submitted_at")
        .and_then(Value::as_str)
        .and_then(parse_rfc3339_utc)
        .unwrap_or_else(Utc::now);

    Ok(SubmitResult {
        route,
        tx_signature: tx_signature.to_string(),
        submitted_at,
    })
}

fn parse_rfc3339_utc(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value.trim())
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn normalize_route(route: &str) -> Option<String> {
    let route = route.trim().to_ascii_lowercase();
    if route.is_empty() {
        None
    } else {
        Some(route)
    }
}

fn normalize_allowed_routes(routes: &[String]) -> HashSet<String> {
    routes
        .iter()
        .filter_map(|value| normalize_route(value))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent::{ExecutionIntent, ExecutionSide};
    use chrono::Utc;

    fn make_intent() -> ExecutionIntent {
        ExecutionIntent {
            signal_id: "shadow:test:wallet:buy:token".to_string(),
            leader_wallet: "leader".to_string(),
            side: ExecutionSide::Buy,
            token: "token".to_string(),
            notional_sol: 0.1,
            signal_ts: Utc::now(),
        }
    }

    #[test]
    fn parse_adapter_submit_response_parses_success_payload() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "submitted_at": "2026-02-19T12:34:56Z"
        });
        let result = parse_adapter_submit_response(&body, "paper").expect("success payload");
        assert_eq!(result.tx_signature, "5ig1ature");
        assert_eq!(result.route, "rpc");
        assert_eq!(
            result.submitted_at.to_rfc3339(),
            "2026-02-19T12:34:56+00:00"
        );
    }

    #[test]
    fn parse_adapter_submit_response_returns_retryable_on_retryable_reject() {
        let body = json!({
            "status": "reject",
            "retryable": true,
            "code": "adapter_busy",
            "detail": "backpressure"
        });
        let error =
            parse_adapter_submit_response(&body, "paper").expect_err("reject payload expected");
        assert_eq!(error.kind, SubmitErrorKind::Retryable);
        assert_eq!(error.code, "adapter_busy");
    }

    #[test]
    fn parse_adapter_submit_response_returns_terminal_on_terminal_reject() {
        let body = json!({
            "ok": false,
            "retryable": false,
            "code": "invalid_route",
            "detail": "route unsupported"
        });
        let error =
            parse_adapter_submit_response(&body, "paper").expect_err("reject payload expected");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "invalid_route");
    }

    #[test]
    fn adapter_submitter_blocks_disallowed_route_before_network_call() {
        let submitter = AdapterOrderSubmitter::new(
            "https://adapter.example/submit",
            "",
            "",
            &["rpc".to_string()],
            1_000,
            50.0,
        )
        .expect("submitter should initialize");
        let error = submitter
            .submit(&make_intent(), "cid-1", "paper")
            .expect_err("route should be rejected");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "route_not_allowed");
    }
}
