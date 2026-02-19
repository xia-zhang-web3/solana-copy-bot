use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fmt::Write as _;
use std::time::Duration as StdDuration;
use uuid::Uuid;

use crate::intent::ExecutionIntent;
type HmacSha256 = Hmac<Sha256>;

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
struct AdapterHmacAuth {
    key_id: String,
    secret: String,
    ttl_sec: u64,
}

#[derive(Debug, Clone)]
pub struct AdapterOrderSubmitter {
    endpoints: Vec<String>,
    auth_token: Option<String>,
    hmac_auth: Option<AdapterHmacAuth>,
    allowed_routes: HashSet<String>,
    route_max_slippage_bps: HashMap<String, f64>,
    route_compute_unit_limit: HashMap<String, u32>,
    route_compute_unit_price_micro_lamports: HashMap<String, u64>,
    slippage_bps: f64,
    client: Client,
}

impl AdapterOrderSubmitter {
    pub fn new(
        primary_url: &str,
        fallback_url: &str,
        auth_token: &str,
        hmac_key_id: &str,
        hmac_secret: &str,
        hmac_ttl_sec: u64,
        allowed_routes: &[String],
        route_max_slippage_bps: &BTreeMap<String, f64>,
        route_compute_unit_limit: &BTreeMap<String, u32>,
        route_compute_unit_price_micro_lamports: &BTreeMap<String, u64>,
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
        let route_max_slippage_bps = normalize_route_slippage_caps(route_max_slippage_bps);
        if route_max_slippage_bps.is_empty() {
            return None;
        }
        let route_compute_unit_limit = normalize_route_cu_limit(route_compute_unit_limit);
        if route_compute_unit_limit.is_empty() {
            return None;
        }
        let route_compute_unit_price_micro_lamports =
            normalize_route_cu_price(route_compute_unit_price_micro_lamports);
        if route_compute_unit_price_micro_lamports.is_empty() {
            return None;
        }
        if !allowed_routes
            .iter()
            .all(|route| route_max_slippage_bps.contains_key(route))
        {
            return None;
        }
        if !allowed_routes
            .iter()
            .all(|route| route_compute_unit_limit.contains_key(route))
        {
            return None;
        }
        if !allowed_routes
            .iter()
            .all(|route| route_compute_unit_price_micro_lamports.contains_key(route))
        {
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
        let hmac_key_id = hmac_key_id.trim();
        let hmac_secret = hmac_secret.trim();
        let hmac_auth = if hmac_key_id.is_empty() && hmac_secret.is_empty() {
            None
        } else if hmac_key_id.is_empty()
            || hmac_secret.is_empty()
            || !(5..=300).contains(&hmac_ttl_sec)
        {
            return None;
        } else {
            Some(AdapterHmacAuth {
                key_id: hmac_key_id.to_string(),
                secret: hmac_secret.to_string(),
                ttl_sec: hmac_ttl_sec,
            })
        };

        Some(Self {
            endpoints,
            auth_token,
            hmac_auth,
            allowed_routes,
            route_max_slippage_bps,
            route_compute_unit_limit,
            route_compute_unit_price_micro_lamports,
            slippage_bps,
            client,
        })
    }

    fn submit_via_endpoint(
        &self,
        endpoint: &str,
        payload: &Value,
        expected_route: &str,
        expected_client_order_id: &str,
    ) -> std::result::Result<SubmitResult, SubmitError> {
        let mut request = self.client.post(endpoint).json(payload);
        if let Some(token) = self.auth_token.as_deref() {
            request = request.bearer_auth(token);
        }
        if let Some(hmac_auth) = self.hmac_auth.as_ref() {
            let payload_json = serde_json::to_string(payload).map_err(|error| {
                SubmitError::terminal(
                    "submit_adapter_payload_serialize_failed",
                    format!(
                        "failed serializing submit payload for hmac signing: {}",
                        error
                    ),
                )
            })?;
            let timestamp_sec = Utc::now().timestamp();
            let nonce = Uuid::new_v4().simple().to_string();
            let signature_payload = format!(
                "{}\n{}\n{}\n{}",
                timestamp_sec, hmac_auth.ttl_sec, nonce, payload_json
            );
            let signature = compute_hmac_signature_hex(
                hmac_auth.secret.as_bytes(),
                signature_payload.as_bytes(),
            )
            .map_err(|error| {
                SubmitError::terminal(
                    "submit_adapter_hmac_signing_failed",
                    format!("failed creating submit request signature: {}", error),
                )
            })?;
            request = request
                .header("x-copybot-key-id", hmac_auth.key_id.as_str())
                .header("x-copybot-timestamp", timestamp_sec.to_string())
                .header("x-copybot-auth-ttl-sec", hmac_auth.ttl_sec.to_string())
                .header("x-copybot-nonce", nonce)
                .header("x-copybot-signature", signature)
                .header("x-copybot-signature-alg", "hmac-sha256-v1");
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
        parse_adapter_submit_response(&body, expected_route, expected_client_order_id).map_err(
            |error| {
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
            },
        )
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
        let route_cap = self
            .route_max_slippage_bps
            .get(route.as_str())
            .copied()
            .ok_or_else(|| {
                SubmitError::terminal(
                    "route_slippage_policy_missing",
                    format!("missing slippage cap for route={}", route),
                )
            })?;
        let effective_slippage_bps = self.slippage_bps.min(route_cap);
        let route_cu_limit = self
            .route_compute_unit_limit
            .get(route.as_str())
            .copied()
            .ok_or_else(|| {
                SubmitError::terminal(
                    "route_compute_budget_policy_missing",
                    format!("missing compute unit limit for route={}", route),
                )
            })?;
        let route_cu_price_micro_lamports = self
            .route_compute_unit_price_micro_lamports
            .get(route.as_str())
            .copied()
            .ok_or_else(|| {
                SubmitError::terminal(
                    "route_compute_budget_policy_missing",
                    format!(
                        "missing compute unit price (micro-lamports) for route={}",
                        route
                    ),
                )
            })?;

        let payload = json!({
            "signal_id": intent.signal_id,
            "client_order_id": client_order_id,
            "request_id": client_order_id,
            "side": intent.side.as_str(),
            "token": intent.token,
            "notional_sol": intent.notional_sol,
            "signal_ts": intent.signal_ts.to_rfc3339(),
            "route": route,
            "slippage_bps": effective_slippage_bps,
            "route_slippage_cap_bps": route_cap,
            "compute_budget": {
                "cu_limit": route_cu_limit,
                "cu_price_micro_lamports": route_cu_price_micro_lamports
            },
        });

        let mut last_retryable_error: Option<SubmitError> = None;
        for endpoint in &self.endpoints {
            match self.submit_via_endpoint(endpoint, &payload, route.as_str(), client_order_id) {
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
    expected_route: &str,
    expected_client_order_id: &str,
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
        .unwrap_or_else(|| expected_route.to_string());
    if route != expected_route {
        return Err(SubmitError::terminal(
            "submit_adapter_route_mismatch",
            format!(
                "adapter response route={} does not match requested route={}",
                route, expected_route
            ),
        ));
    }
    if let Some(client_order_id) = body
        .get("client_order_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if client_order_id != expected_client_order_id {
            return Err(SubmitError::terminal(
                "submit_adapter_client_order_id_mismatch",
                format!(
                    "adapter response client_order_id={} does not match requested client_order_id={}",
                    client_order_id, expected_client_order_id
                ),
            ));
        }
    }
    if let Some(request_id) = body
        .get("request_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if request_id != expected_client_order_id {
            return Err(SubmitError::terminal(
                "submit_adapter_request_id_mismatch",
                format!(
                    "adapter response request_id={} does not match requested client_order_id={}",
                    request_id, expected_client_order_id
                ),
            ));
        }
    }
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

fn compute_hmac_signature_hex(
    secret: &[u8],
    payload: &[u8],
) -> Result<String, hmac::digest::InvalidLength> {
    let mut mac = HmacSha256::new_from_slice(secret)?;
    mac.update(payload);
    let signature_bytes = mac.finalize().into_bytes();
    let mut hex = String::with_capacity(signature_bytes.len() * 2);
    for byte in signature_bytes {
        let _ = write!(&mut hex, "{byte:02x}");
    }
    Ok(hex)
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

fn normalize_route_slippage_caps(route_caps: &BTreeMap<String, f64>) -> HashMap<String, f64> {
    route_caps
        .iter()
        .filter_map(|(route, cap)| {
            let route = normalize_route(route)?;
            if !cap.is_finite() || *cap <= 0.0 {
                return None;
            }
            Some((route, *cap))
        })
        .collect()
}

fn normalize_route_cu_limit(route_caps: &BTreeMap<String, u32>) -> HashMap<String, u32> {
    route_caps
        .iter()
        .filter_map(|(route, value)| {
            let route = normalize_route(route)?;
            if *value == 0 || *value > 1_400_000 {
                return None;
            }
            Some((route, *value))
        })
        .collect()
}

fn normalize_route_cu_price(route_caps: &BTreeMap<String, u64>) -> HashMap<String, u64> {
    route_caps
        .iter()
        .filter_map(|(route, value)| {
            let route = normalize_route(route)?;
            if *value == 0 || *value > 10_000_000 {
                return None;
            }
            Some((route, *value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent::{ExecutionIntent, ExecutionSide};
    use chrono::Utc;
    use std::collections::BTreeMap;

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

    fn make_route_caps(route: &str, cap: f64) -> BTreeMap<String, f64> {
        BTreeMap::from([(route.to_string(), cap)])
    }

    fn make_route_cu_limits(route: &str, value: u32) -> BTreeMap<String, u32> {
        BTreeMap::from([(route.to_string(), value)])
    }

    fn make_route_cu_prices(route: &str, value: u64) -> BTreeMap<String, u64> {
        BTreeMap::from([(route.to_string(), value)])
    }

    #[test]
    fn parse_adapter_submit_response_parses_success_payload() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "submitted_at": "2026-02-19T12:34:56Z",
            "client_order_id": "cid-1",
            "request_id": "cid-1"
        });
        let result = parse_adapter_submit_response(&body, "rpc", "cid-1").expect("success payload");
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
        let error = parse_adapter_submit_response(&body, "rpc", "cid-1")
            .expect_err("reject payload expected");
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
        let error = parse_adapter_submit_response(&body, "rpc", "cid-1")
            .expect_err("reject payload expected");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "invalid_route");
    }

    #[test]
    fn parse_adapter_submit_response_rejects_route_mismatch() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc"
        });
        let error = parse_adapter_submit_response(&body, "jito", "cid-1")
            .expect_err("route mismatch must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_route_mismatch");
    }

    #[test]
    fn parse_adapter_submit_response_rejects_client_order_id_mismatch() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "client_order_id": "cid-2"
        });
        let error = parse_adapter_submit_response(&body, "rpc", "cid-1")
            .expect_err("client_order_id mismatch must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_client_order_id_mismatch");
    }

    #[test]
    fn compute_hmac_signature_hex_matches_known_vector() {
        let signature =
            compute_hmac_signature_hex(b"key", b"The quick brown fox jumps over the lazy dog")
                .expect("signature should be generated");
        assert_eq!(
            signature,
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn adapter_submitter_blocks_disallowed_route_before_network_call() {
        let submitter = AdapterOrderSubmitter::new(
            "https://adapter.example/submit",
            "",
            "",
            "",
            "",
            30,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_000),
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

    #[test]
    fn adapter_submitter_requires_complete_hmac_auth_config() {
        let missing_secret = AdapterOrderSubmitter::new(
            "https://adapter.example/submit",
            "",
            "",
            "key-1",
            "",
            30,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_000),
            1_000,
            50.0,
        );
        assert!(missing_secret.is_none());

        let missing_key = AdapterOrderSubmitter::new(
            "https://adapter.example/submit",
            "",
            "",
            "",
            "secret",
            30,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_000),
            1_000,
            50.0,
        );
        assert!(missing_key.is_none());

        let invalid_ttl = AdapterOrderSubmitter::new(
            "https://adapter.example/submit",
            "",
            "",
            "key-1",
            "secret",
            4,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_000),
            1_000,
            50.0,
        );
        assert!(invalid_ttl.is_none());
    }

    #[test]
    fn adapter_submitter_requires_route_slippage_cap_for_allowed_route() {
        let submitter = AdapterOrderSubmitter::new(
            "https://adapter.example/submit",
            "",
            "",
            "",
            "",
            30,
            &["rpc".to_string()],
            &make_route_caps("paper", 50.0),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_000),
            1_000,
            50.0,
        );
        assert!(submitter.is_none());
    }

    #[test]
    fn adapter_submitter_requires_compute_budget_policy_for_allowed_route() {
        let submitter = AdapterOrderSubmitter::new(
            "https://adapter.example/submit",
            "",
            "",
            "",
            "",
            30,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_cu_limits("paper", 300_000),
            &make_route_cu_prices("paper", 1_000),
            1_000,
            50.0,
        );
        assert!(submitter.is_none());
    }
}
