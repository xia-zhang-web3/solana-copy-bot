use chrono::{DateTime, Utc};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::time::{Duration as StdDuration, Instant};
use uuid::Uuid;

use crate::auth::compute_hmac_signature_hex;
use crate::intent::ExecutionIntent;
use crate::submitter_response::{normalize_route, parse_adapter_submit_response};
use copybot_config::{
    EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MAX, EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MIN,
    EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MAX,
    EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MIN, EXECUTION_ROUTE_TIP_LAMPORTS_MAX,
};

#[derive(Debug, Clone)]
pub struct SubmitResult {
    pub route: String,
    pub tx_signature: String,
    pub submitted_at: DateTime<Utc>,
    pub applied_tip_lamports: u64,
    pub ata_create_rent_lamports: Option<u64>,
    pub network_fee_lamports_hint: Option<u64>,
    pub base_fee_lamports_hint: Option<u64>,
    pub priority_fee_lamports_hint: Option<u64>,
    pub dynamic_cu_price_policy_enabled: bool,
    pub dynamic_cu_price_hint_used: bool,
    pub dynamic_cu_price_applied: bool,
    pub dynamic_tip_policy_enabled: bool,
    pub dynamic_tip_applied: bool,
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
            applied_tip_lamports: 0,
            ata_create_rent_lamports: None,
            network_fee_lamports_hint: None,
            base_fee_lamports_hint: None,
            priority_fee_lamports_hint: None,
            dynamic_cu_price_policy_enabled: false,
            dynamic_cu_price_hint_used: false,
            dynamic_cu_price_applied: false,
            dynamic_tip_policy_enabled: false,
            dynamic_tip_applied: false,
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
struct DynamicCuPricePolicy {
    rpc_endpoints: Vec<String>,
    percentile: u8,
    max_micro_lamports: u64,
    timeout_ms: u64,
}

#[derive(Debug, Clone)]
struct DynamicTipLamportsPolicy {
    multiplier_bps: u32,
}

const PRIORITY_FEE_HINT_TIMEOUT_MS_CAP: u64 = 1_000;
const PRIORITY_FEE_HINT_TIMEOUT_MS_FLOOR: u64 = 250;

#[derive(Debug, Clone)]
pub struct AdapterOrderSubmitter {
    endpoints: Vec<String>,
    auth_token: Option<String>,
    hmac_auth: Option<AdapterHmacAuth>,
    dynamic_cu_price_policy: Option<DynamicCuPricePolicy>,
    dynamic_tip_lamports_policy: Option<DynamicTipLamportsPolicy>,
    dynamic_cu_price_client: Option<Client>,
    contract_version: String,
    require_policy_echo: bool,
    allowed_routes: HashSet<String>,
    route_max_slippage_bps: HashMap<String, f64>,
    route_tip_lamports: HashMap<String, u64>,
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
        contract_version: &str,
        require_policy_echo: bool,
        allowed_routes: &[String],
        route_max_slippage_bps: &BTreeMap<String, f64>,
        route_tip_lamports: &BTreeMap<String, u64>,
        route_compute_unit_limit: &BTreeMap<String, u32>,
        route_compute_unit_price_micro_lamports: &BTreeMap<String, u64>,
        timeout_ms: u64,
        slippage_bps: f64,
    ) -> Option<Self> {
        Self::new_with_dynamic_and_tip(
            primary_url,
            fallback_url,
            auth_token,
            hmac_key_id,
            hmac_secret,
            hmac_ttl_sec,
            contract_version,
            require_policy_echo,
            allowed_routes,
            route_max_slippage_bps,
            route_tip_lamports,
            route_compute_unit_limit,
            route_compute_unit_price_micro_lamports,
            "",
            "",
            false,
            75,
            0,
            false,
            10_000,
            timeout_ms,
            slippage_bps,
        )
    }

    pub fn new_with_dynamic(
        primary_url: &str,
        fallback_url: &str,
        auth_token: &str,
        hmac_key_id: &str,
        hmac_secret: &str,
        hmac_ttl_sec: u64,
        contract_version: &str,
        require_policy_echo: bool,
        allowed_routes: &[String],
        route_max_slippage_bps: &BTreeMap<String, f64>,
        route_tip_lamports: &BTreeMap<String, u64>,
        route_compute_unit_limit: &BTreeMap<String, u32>,
        route_compute_unit_price_micro_lamports: &BTreeMap<String, u64>,
        rpc_primary_url: &str,
        rpc_fallback_url: &str,
        submit_dynamic_cu_price_enabled: bool,
        submit_dynamic_cu_price_percentile: u8,
        pretrade_max_priority_fee_micro_lamports: u64,
        timeout_ms: u64,
        slippage_bps: f64,
    ) -> Option<Self> {
        Self::new_with_dynamic_and_tip(
            primary_url,
            fallback_url,
            auth_token,
            hmac_key_id,
            hmac_secret,
            hmac_ttl_sec,
            contract_version,
            require_policy_echo,
            allowed_routes,
            route_max_slippage_bps,
            route_tip_lamports,
            route_compute_unit_limit,
            route_compute_unit_price_micro_lamports,
            rpc_primary_url,
            rpc_fallback_url,
            submit_dynamic_cu_price_enabled,
            submit_dynamic_cu_price_percentile,
            pretrade_max_priority_fee_micro_lamports,
            false,
            10_000,
            timeout_ms,
            slippage_bps,
        )
    }

    pub fn new_with_dynamic_and_tip(
        primary_url: &str,
        fallback_url: &str,
        auth_token: &str,
        hmac_key_id: &str,
        hmac_secret: &str,
        hmac_ttl_sec: u64,
        contract_version: &str,
        require_policy_echo: bool,
        allowed_routes: &[String],
        route_max_slippage_bps: &BTreeMap<String, f64>,
        route_tip_lamports: &BTreeMap<String, u64>,
        route_compute_unit_limit: &BTreeMap<String, u32>,
        route_compute_unit_price_micro_lamports: &BTreeMap<String, u64>,
        rpc_primary_url: &str,
        rpc_fallback_url: &str,
        submit_dynamic_cu_price_enabled: bool,
        submit_dynamic_cu_price_percentile: u8,
        pretrade_max_priority_fee_micro_lamports: u64,
        submit_dynamic_tip_lamports_enabled: bool,
        submit_dynamic_tip_lamports_multiplier_bps: u32,
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
        let route_tip_lamports = normalize_route_tip_lamports(route_tip_lamports);
        if route_tip_lamports.is_empty() {
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
            .all(|route| route_tip_lamports.contains_key(route))
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
        let dynamic_cu_price_policy = if submit_dynamic_cu_price_enabled {
            if submit_dynamic_cu_price_percentile == 0 || submit_dynamic_cu_price_percentile > 100 {
                return None;
            }
            if pretrade_max_priority_fee_micro_lamports == 0 {
                return None;
            }
            let hint_timeout_ms = priority_fee_hint_timeout_ms(timeout_ms);
            if route_compute_unit_price_micro_lamports
                .values()
                .any(|value| *value > pretrade_max_priority_fee_micro_lamports)
            {
                return None;
            }
            let rpc_endpoints =
                normalize_distinct_non_empty_endpoints(rpc_primary_url, rpc_fallback_url);
            if rpc_endpoints.is_empty() {
                return None;
            }
            Some(DynamicCuPricePolicy {
                rpc_endpoints,
                percentile: submit_dynamic_cu_price_percentile,
                max_micro_lamports: pretrade_max_priority_fee_micro_lamports,
                timeout_ms: hint_timeout_ms,
            })
        } else {
            None
        };
        let dynamic_tip_lamports_policy = if submit_dynamic_tip_lamports_enabled {
            if !submit_dynamic_cu_price_enabled {
                return None;
            }
            if submit_dynamic_tip_lamports_multiplier_bps == 0
                || submit_dynamic_tip_lamports_multiplier_bps > 100_000
            {
                return None;
            }
            Some(DynamicTipLamportsPolicy {
                multiplier_bps: submit_dynamic_tip_lamports_multiplier_bps,
            })
        } else {
            None
        };

        let client = match Client::builder()
            .timeout(StdDuration::from_millis(timeout_ms.max(500)))
            .build()
        {
            Ok(value) => value,
            Err(_) => return None,
        };
        let dynamic_cu_price_client = if dynamic_cu_price_policy.is_some() {
            let hint_timeout_ms = dynamic_cu_price_policy
                .as_ref()
                .map(|policy| policy.timeout_ms)
                .unwrap_or_else(|| priority_fee_hint_timeout_ms(timeout_ms));
            match Client::builder()
                .timeout(StdDuration::from_millis(hint_timeout_ms))
                .build()
            {
                Ok(value) => Some(value),
                Err(_) => return None,
            }
        } else {
            None
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
        let contract_version = contract_version.trim();
        if contract_version.is_empty()
            || contract_version.len() > 64
            || !is_valid_contract_version_token(contract_version)
        {
            return None;
        }

        Some(Self {
            endpoints,
            auth_token,
            hmac_auth,
            dynamic_cu_price_policy,
            dynamic_tip_lamports_policy,
            dynamic_cu_price_client,
            contract_version: contract_version.to_string(),
            require_policy_echo,
            allowed_routes,
            route_max_slippage_bps,
            route_tip_lamports,
            route_compute_unit_limit,
            route_compute_unit_price_micro_lamports,
            slippage_bps,
            client,
        })
    }

    fn resolve_route_cu_price_micro_lamports(
        &self,
        static_route_cu_price: u64,
    ) -> (u64, bool, bool) {
        let Some(policy) = self.dynamic_cu_price_policy.as_ref() else {
            return (static_route_cu_price, false, false);
        };
        let hinted = self
            .query_recent_priority_fee_micro_lamports(policy)
            .map(|dynamic| {
                dynamic
                    .max(static_route_cu_price)
                    .min(policy.max_micro_lamports)
                    .clamp(
                        EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MIN,
                        EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MAX,
                    )
            });
        let resolved = hinted
            .unwrap_or(static_route_cu_price)
            .max(static_route_cu_price)
            .min(policy.max_micro_lamports)
            .clamp(
                EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MIN,
                EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MAX,
            );
        let hint_used = hinted.is_some();
        let applied = resolved > static_route_cu_price;
        (resolved, hint_used, applied)
    }

    fn resolve_route_tip_lamports(
        &self,
        static_route_tip_lamports: u64,
        route_cu_limit: u32,
        route_cu_price_micro_lamports: u64,
    ) -> (u64, bool) {
        let Some(policy) = self.dynamic_tip_lamports_policy.as_ref() else {
            return (static_route_tip_lamports, false);
        };
        let dynamic_tip = dynamic_tip_lamports_from_compute_budget(
            route_cu_limit,
            route_cu_price_micro_lamports,
            policy.multiplier_bps,
        );
        let resolved = static_route_tip_lamports
            .max(dynamic_tip)
            .min(EXECUTION_ROUTE_TIP_LAMPORTS_MAX);
        let applied = resolved > static_route_tip_lamports;
        (resolved, applied)
    }

    fn query_recent_priority_fee_micro_lamports(
        &self,
        policy: &DynamicCuPricePolicy,
    ) -> Option<u64> {
        let client = self.dynamic_cu_price_client.as_ref()?;
        let started = Instant::now();
        let total_timeout = StdDuration::from_millis(policy.timeout_ms);
        for endpoint in &policy.rpc_endpoints {
            let elapsed = started.elapsed();
            if elapsed >= total_timeout {
                break;
            }
            let remaining_timeout = total_timeout.saturating_sub(elapsed);
            let response = match client
                .post(endpoint)
                .header("content-type", "application/json")
                .timeout(remaining_timeout)
                .json(&json!({
                    "jsonrpc": "2.0",
                    "id": "copybot-priority-fee",
                    "method": "getRecentPrioritizationFees",
                    "params": []
                }))
                .send()
            {
                Ok(value) => value,
                Err(_) => continue,
            };
            if !response.status().is_success() {
                continue;
            }
            let body: Value = match response.json() {
                Ok(value) => value,
                Err(_) => continue,
            };
            if let Some(fee) =
                parse_recent_priority_fee_percentile_from_rpc_body(&body, policy.percentile)
            {
                return Some(fee);
            }
        }
        None
    }

    fn submit_via_endpoint(
        &self,
        endpoint: &str,
        payload: &Value,
        expected_route: &str,
        expected_client_order_id: &str,
        expected_contract_version: &str,
        require_policy_echo: bool,
        expected_slippage_bps: f64,
        expected_tip_lamports: u64,
        expected_cu_limit: u32,
        expected_cu_price_micro_lamports: u64,
    ) -> std::result::Result<SubmitResult, SubmitError> {
        let payload_json = serde_json::to_string(payload).map_err(|error| {
            SubmitError::terminal(
                "submit_adapter_payload_serialize_failed",
                format!("failed serializing submit payload: {}", error),
            )
        })?;
        let mut request = self
            .client
            .post(endpoint)
            .header("content-type", "application/json")
            .body(payload_json.clone());
        if let Some(token) = self.auth_token.as_deref() {
            request = request.bearer_auth(token);
        }
        if let Some(hmac_auth) = self.hmac_auth.as_ref() {
            let timestamp_sec = Utc::now().timestamp();
            let nonce = Uuid::new_v4().simple().to_string();
            // Signature must cover the exact UTF-8 JSON bytes sent in this HTTP request body.
            // Verifier should use raw body bytes instead of re-serialized JSON.
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
        parse_adapter_submit_response(
            &body,
            expected_route,
            expected_client_order_id,
            expected_contract_version,
            require_policy_echo,
            expected_slippage_bps,
            expected_tip_lamports,
            expected_cu_limit,
            expected_cu_price_micro_lamports,
        )
        .map_err(|error| {
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
        let static_route_tip_lamports = self
            .route_tip_lamports
            .get(route.as_str())
            .copied()
            .ok_or_else(|| {
                SubmitError::terminal(
                    "route_tip_policy_missing",
                    format!("missing tip_lamports policy for route={}", route),
                )
            })?;
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
        let static_route_cu_price_micro_lamports = self
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
        let dynamic_cu_price_policy_enabled = self.dynamic_cu_price_policy.is_some();
        let (route_cu_price_micro_lamports, dynamic_cu_price_hint_used, dynamic_cu_price_applied) =
            self.resolve_route_cu_price_micro_lamports(static_route_cu_price_micro_lamports);
        let dynamic_tip_policy_enabled = self.dynamic_tip_lamports_policy.is_some();
        let (route_tip_lamports, dynamic_tip_applied) = self.resolve_route_tip_lamports(
            static_route_tip_lamports,
            route_cu_limit,
            route_cu_price_micro_lamports,
        );

        let payload = json!({
            "contract_version": self.contract_version,
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
            "tip_lamports": route_tip_lamports,
            "compute_budget": {
                "cu_limit": route_cu_limit,
                "cu_price_micro_lamports": route_cu_price_micro_lamports
            },
        });

        let mut last_retryable_error: Option<SubmitError> = None;
        for endpoint in &self.endpoints {
            match self.submit_via_endpoint(
                endpoint,
                &payload,
                route.as_str(),
                client_order_id,
                self.contract_version.as_str(),
                self.require_policy_echo,
                effective_slippage_bps,
                route_tip_lamports,
                route_cu_limit,
                route_cu_price_micro_lamports,
            ) {
                Ok(mut result) => {
                    result.dynamic_cu_price_policy_enabled = dynamic_cu_price_policy_enabled;
                    result.dynamic_cu_price_hint_used = dynamic_cu_price_hint_used;
                    result.dynamic_cu_price_applied = dynamic_cu_price_applied;
                    result.dynamic_tip_policy_enabled = dynamic_tip_policy_enabled;
                    result.dynamic_tip_applied = dynamic_tip_applied;
                    return Ok(result);
                }
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

fn is_valid_contract_version_token(value: &str) -> bool {
    value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_'))
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

fn normalize_route_tip_lamports(route_tips: &BTreeMap<String, u64>) -> HashMap<String, u64> {
    route_tips
        .iter()
        .filter_map(|(route, value)| {
            let route = normalize_route(route)?;
            if *value > EXECUTION_ROUTE_TIP_LAMPORTS_MAX {
                return None;
            }
            Some((route, *value))
        })
        .collect()
}

fn normalize_route_cu_limit(route_caps: &BTreeMap<String, u32>) -> HashMap<String, u32> {
    route_caps
        .iter()
        .filter_map(|(route, value)| {
            let route = normalize_route(route)?;
            if *value < EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MIN
                || *value > EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MAX
            {
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
            if *value < EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MIN
                || *value > EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MAX
            {
                return None;
            }
            Some((route, *value))
        })
        .collect()
}

fn dynamic_tip_lamports_from_compute_budget(
    cu_limit: u32,
    cu_price_micro_lamports: u64,
    multiplier_bps: u32,
) -> u64 {
    let priority_fee = priority_fee_lamports_from_compute_budget(cu_limit, cu_price_micro_lamports);
    let scaled = (priority_fee as u128).saturating_mul(multiplier_bps as u128) / 10_000u128;
    scaled
        .min(EXECUTION_ROUTE_TIP_LAMPORTS_MAX as u128)
        .try_into()
        .unwrap_or(EXECUTION_ROUTE_TIP_LAMPORTS_MAX)
}

fn priority_fee_lamports_from_compute_budget(cu_limit: u32, cu_price_micro_lamports: u64) -> u64 {
    let numerator = (cu_limit as u128)
        .saturating_mul(cu_price_micro_lamports as u128)
        .saturating_add(999_999);
    let lamports = numerator / 1_000_000u128;
    lamports
        .min(u64::MAX as u128)
        .try_into()
        .unwrap_or(u64::MAX)
}

fn parse_recent_priority_fee_percentile_from_rpc_body(body: &Value, percentile: u8) -> Option<u64> {
    if percentile == 0 || percentile > 100 {
        return None;
    }
    let rows = body.get("result")?.as_array()?;
    let mut fees = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(value) = row.get("prioritizationFee").and_then(Value::as_u64) {
            fees.push(value);
        }
    }
    if fees.is_empty() {
        return None;
    }
    fees.sort_unstable();
    let len = fees.len();
    let rank = (((percentile as usize) * len).saturating_add(99) / 100).saturating_sub(1);
    fees.get(rank.min(len.saturating_sub(1))).copied()
}

fn normalize_distinct_non_empty_endpoints(primary_url: &str, fallback_url: &str) -> Vec<String> {
    let mut endpoints = Vec::new();
    let primary = primary_url.trim();
    if !primary.is_empty() {
        endpoints.push(primary.to_string());
    }
    let fallback = fallback_url.trim();
    if !fallback.is_empty() && !fallback.eq_ignore_ascii_case(primary) {
        endpoints.push(fallback.to_string());
    }
    endpoints
}

fn priority_fee_hint_timeout_ms(submit_timeout_ms: u64) -> u64 {
    submit_timeout_ms
        .min(PRIORITY_FEE_HINT_TIMEOUT_MS_CAP)
        .max(PRIORITY_FEE_HINT_TIMEOUT_MS_FLOOR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent::{ExecutionIntent, ExecutionSide};
    use chrono::Utc;
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::io::ErrorKind;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::{Duration as StdDuration, Instant};

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

    fn make_route_tips(route: &str, value: u64) -> BTreeMap<String, u64> {
        BTreeMap::from([(route.to_string(), value)])
    }

    fn make_route_cu_limits(route: &str, value: u32) -> BTreeMap<String, u32> {
        BTreeMap::from([(route.to_string(), value)])
    }

    fn make_route_cu_prices(route: &str, value: u64) -> BTreeMap<String, u64> {
        BTreeMap::from([(route.to_string(), value)])
    }

    #[derive(Debug)]
    struct CapturedHttpRequest {
        path: String,
        headers: HashMap<String, String>,
        body: String,
    }

    fn find_header_end(buffer: &[u8]) -> Option<usize> {
        buffer.windows(4).position(|window| window == b"\r\n\r\n")
    }

    fn spawn_one_shot_adapter(
        status: u16,
        response_body: Value,
    ) -> Option<(String, thread::JoinHandle<CapturedHttpRequest>)> {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(error) => {
                eprintln!(
                    "skipping adapter HTTP integration test: failed to bind 127.0.0.1:0: {}",
                    error
                );
                return None;
            }
        };
        if let Err(error) = listener.set_nonblocking(false) {
            eprintln!(
                "skipping adapter HTTP integration test: failed to set blocking mode: {}",
                error
            );
            return None;
        }
        let addr = match listener.local_addr() {
            Ok(addr) => addr,
            Err(error) => {
                eprintln!(
                    "skipping adapter HTTP integration test: failed to read listener addr: {}",
                    error
                );
                return None;
            }
        };
        let response_body = response_body.to_string();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept adapter client");
            stream
                .set_read_timeout(Some(StdDuration::from_secs(5)))
                .expect("set read timeout");
            let mut buffer = Vec::new();
            let mut chunk = [0_u8; 1024];
            let mut header_end = None;
            while header_end.is_none() {
                let read = stream.read(&mut chunk).expect("read request headers");
                if read == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..read]);
                header_end = find_header_end(&buffer).map(|offset| offset + 4);
            }

            let header_end = header_end.expect("request headers must be present");
            let header_text = String::from_utf8_lossy(&buffer[..header_end]).to_string();
            let mut lines = header_text.split("\r\n");
            let request_line = lines.next().unwrap_or_default().to_string();
            let path = request_line
                .split_whitespace()
                .nth(1)
                .unwrap_or_default()
                .to_string();
            let mut headers = HashMap::new();
            let mut content_length = 0usize;
            for line in lines {
                if line.trim().is_empty() {
                    continue;
                }
                if let Some((name, value)) = line.split_once(':') {
                    let key = name.trim().to_ascii_lowercase();
                    let value = value.trim().to_string();
                    if key == "content-length" {
                        content_length = value.parse::<usize>().unwrap_or(0);
                    }
                    headers.insert(key, value);
                }
            }

            while buffer.len() < header_end.saturating_add(content_length) {
                let read = stream.read(&mut chunk).expect("read request body");
                if read == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..read]);
            }
            let body_end = header_end
                .saturating_add(content_length)
                .min(buffer.len())
                .max(header_end);
            let body = String::from_utf8_lossy(&buffer[header_end..body_end]).to_string();

            let reason = if status == 200 { "OK" } else { "ERR" };
            let response = format!(
                "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                status,
                reason,
                response_body.len(),
                response_body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write adapter response");
            stream.flush().expect("flush adapter response");

            CapturedHttpRequest {
                path,
                headers,
                body,
            }
        });
        Some((format!("http://{}/submit", addr), handle))
    }

    fn spawn_probe_server_with_optional_capture(
        path: &str,
        status: u16,
        response_body: Value,
        response_delay_ms: u64,
        accept_timeout_ms: u64,
    ) -> Option<(String, thread::JoinHandle<Option<CapturedHttpRequest>>)> {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(error) => {
                eprintln!(
                    "skipping probe server test: failed to bind 127.0.0.1:0: {}",
                    error
                );
                return None;
            }
        };
        if let Err(error) = listener.set_nonblocking(true) {
            eprintln!(
                "skipping probe server test: failed to set nonblocking mode: {}",
                error
            );
            return None;
        }
        let addr = match listener.local_addr() {
            Ok(addr) => addr,
            Err(error) => {
                eprintln!(
                    "skipping probe server test: failed to read listener addr: {}",
                    error
                );
                return None;
            }
        };
        let endpoint = format!("http://{}/{}", addr, path.trim_start_matches('/'));
        let response_body = response_body.to_string();
        let handle = thread::spawn(move || -> Option<CapturedHttpRequest> {
            let deadline = Instant::now() + StdDuration::from_millis(accept_timeout_ms.max(100));
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        if Instant::now() >= deadline {
                            return None;
                        }
                        thread::sleep(StdDuration::from_millis(5));
                    }
                    Err(_) => return None,
                }
            };

            if stream
                .set_read_timeout(Some(StdDuration::from_secs(5)))
                .is_err()
            {
                return None;
            }
            let mut buffer = Vec::new();
            let mut chunk = [0_u8; 1024];
            let mut header_end = None;
            while header_end.is_none() {
                let read = match stream.read(&mut chunk) {
                    Ok(value) => value,
                    Err(_) => return None,
                };
                if read == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..read]);
                header_end = find_header_end(&buffer).map(|offset| offset + 4);
            }
            let header_end = header_end?;
            let header_text = String::from_utf8_lossy(&buffer[..header_end]).to_string();
            let mut lines = header_text.split("\r\n");
            let request_line = lines.next().unwrap_or_default().to_string();
            let path = request_line
                .split_whitespace()
                .nth(1)
                .unwrap_or_default()
                .to_string();
            let mut headers = HashMap::new();
            let mut content_length = 0usize;
            for line in lines {
                if line.trim().is_empty() {
                    continue;
                }
                if let Some((name, value)) = line.split_once(':') {
                    let key = name.trim().to_ascii_lowercase();
                    let value = value.trim().to_string();
                    if key == "content-length" {
                        content_length = value.parse::<usize>().unwrap_or(0);
                    }
                    headers.insert(key, value);
                }
            }
            while buffer.len() < header_end.saturating_add(content_length) {
                let read = match stream.read(&mut chunk) {
                    Ok(value) => value,
                    Err(_) => break,
                };
                if read == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..read]);
            }
            let body_end = header_end
                .saturating_add(content_length)
                .min(buffer.len())
                .max(header_end);
            let body = String::from_utf8_lossy(&buffer[header_end..body_end]).to_string();

            if response_delay_ms > 0 {
                thread::sleep(StdDuration::from_millis(response_delay_ms));
            }
            let reason = if status == 200 { "OK" } else { "ERR" };
            let response = format!(
                "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                status,
                reason,
                response_body.len(),
                response_body
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();

            Some(CapturedHttpRequest {
                path,
                headers,
                body,
            })
        });
        Some((endpoint, handle))
    }

    fn parse_response(
        body: &Value,
        expected_route: &str,
        expected_client_order_id: &str,
    ) -> std::result::Result<SubmitResult, SubmitError> {
        parse_adapter_submit_response(
            body,
            expected_route,
            expected_client_order_id,
            "v1",
            false,
            50.0,
            0,
            300_000,
            1_000,
        )
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
        let result = parse_response(&body, "rpc", "cid-1").expect("success payload");
        assert_eq!(result.tx_signature, "5ig1ature");
        assert_eq!(result.route, "rpc");
        assert_eq!(
            result.submitted_at.to_rfc3339(),
            "2026-02-19T12:34:56+00:00"
        );
        assert_eq!(result.applied_tip_lamports, 0);
        assert_eq!(result.ata_create_rent_lamports, None);
        assert_eq!(result.network_fee_lamports_hint, None);
        assert_eq!(result.base_fee_lamports_hint, None);
        assert_eq!(result.priority_fee_lamports_hint, None);
    }

    #[test]
    fn parse_adapter_submit_response_parses_ata_rent_lamports() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "tip_lamports": 777,
            "ata_create_rent_lamports": 2_039_280
        });
        let result = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", false, 50.0, 777, 300_000, 1_000,
        )
        .expect("success payload");
        assert_eq!(result.applied_tip_lamports, 777);
        assert_eq!(result.ata_create_rent_lamports, Some(2_039_280));
        assert_eq!(result.network_fee_lamports_hint, None);
        assert_eq!(result.base_fee_lamports_hint, None);
        assert_eq!(result.priority_fee_lamports_hint, None);
    }

    #[test]
    fn parse_adapter_submit_response_rejects_ata_rent_above_i64_max() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "tip_lamports": 777,
            "ata_create_rent_lamports": (i64::MAX as u64).saturating_add(1)
        });
        let error = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", false, 50.0, 777, 300_000, 1_000,
        )
        .expect_err("ata rent above i64 max must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_invalid_response");
    }

    #[test]
    fn parse_adapter_submit_response_parses_fee_hints_and_derives_network_fee() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "tip_lamports": 777,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 12000,
            "ata_create_rent_lamports": 2_039_280
        });
        let result = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", false, 50.0, 777, 300_000, 1_000,
        )
        .expect("success payload");
        assert_eq!(result.applied_tip_lamports, 777);
        assert_eq!(result.ata_create_rent_lamports, Some(2_039_280));
        assert_eq!(result.network_fee_lamports_hint, Some(17_000));
        assert_eq!(result.base_fee_lamports_hint, Some(5_000));
        assert_eq!(result.priority_fee_lamports_hint, Some(12_000));
    }

    #[test]
    fn parse_adapter_submit_response_rejects_network_fee_hint_above_i64_max() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "network_fee_lamports": (i64::MAX as u64).saturating_add(1)
        });
        let error = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", false, 50.0, 0, 300_000, 1_000,
        )
        .expect_err("network fee hint above i64 max must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_invalid_response");
    }

    #[test]
    fn parse_adapter_submit_response_rejects_negative_fee_hint() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "network_fee_lamports": -1
        });
        let error = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", false, 50.0, 0, 300_000, 1_000,
        )
        .expect_err("negative fee hint must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_invalid_response");
    }

    #[test]
    fn parse_adapter_submit_response_rejects_non_numeric_fee_hint() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "base_fee_lamports": "5000"
        });
        let error = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", false, 50.0, 0, 300_000, 1_000,
        )
        .expect_err("non-numeric fee hint must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_invalid_response");
    }

    #[test]
    fn parse_adapter_submit_response_rejects_missing_fee_breakdown_in_strict_mode() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 50.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let error = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", true, 50.0, 0, 300_000, 1_000,
        )
        .expect_err("strict mode must require fee breakdown echo");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_policy_echo_missing");
        assert!(
            error.detail.contains("base_fee_lamports"),
            "unexpected detail: {}",
            error.detail
        );
    }

    #[test]
    fn parse_adapter_submit_response_accepts_fee_breakdown_in_strict_mode() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 50.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            },
            "network_fee_lamports": 17000,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 12000
        });
        let result = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", true, 50.0, 0, 300_000, 1_000,
        )
        .expect("strict mode should accept valid fee breakdown echo");
        assert_eq!(result.network_fee_lamports_hint, Some(17_000));
        assert_eq!(result.base_fee_lamports_hint, Some(5_000));
        assert_eq!(result.priority_fee_lamports_hint, Some(12_000));
    }

    #[test]
    fn parse_adapter_submit_response_rejects_missing_network_fee_hint_in_strict_mode() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 50.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            },
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 12000
        });
        let error = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", true, 50.0, 0, 300_000, 1_000,
        )
        .expect_err("strict mode must require network_fee_lamports echo");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_policy_echo_missing");
        assert!(
            error.detail.contains("network_fee_lamports"),
            "unexpected detail: {}",
            error.detail
        );
    }

    #[test]
    fn parse_adapter_submit_response_rejects_network_fee_mismatch_with_base_priority() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "network_fee_lamports": 1000,
            "base_fee_lamports": 600,
            "priority_fee_lamports": 500
        });
        let error = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", false, 50.0, 0, 300_000, 1_000,
        )
        .expect_err("mismatched network/base+priority must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_invalid_response");
    }

    #[test]
    fn parse_adapter_submit_response_returns_retryable_on_retryable_reject() {
        let body = json!({
            "status": "reject",
            "retryable": true,
            "code": "adapter_busy",
            "detail": "backpressure"
        });
        let error = parse_response(&body, "rpc", "cid-1").expect_err("reject payload expected");
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
        let error = parse_response(&body, "rpc", "cid-1").expect_err("reject payload expected");
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
        let error = parse_response(&body, "jito", "cid-1").expect_err("route mismatch must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_route_mismatch");
    }

    #[test]
    fn parse_adapter_submit_response_rejects_missing_route_echo() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature"
        });
        let error = parse_response(&body, "rpc", "cid-1").expect_err("missing route must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_policy_echo_missing");
    }

    #[test]
    fn parse_adapter_submit_response_rejects_unknown_status_even_with_ok_true() {
        let body = json!({
            "status": "pending",
            "ok": true,
            "tx_signature": "5ig1ature",
            "route": "rpc"
        });
        let error = parse_response(&body, "rpc", "cid-1").expect_err("unknown status must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_invalid_status");
    }

    #[test]
    fn parse_adapter_submit_response_rejects_unknown_status_even_with_accepted_true() {
        let body = json!({
            "status": "pending",
            "accepted": true,
            "tx_signature": "5ig1ature",
            "route": "rpc"
        });
        let error = parse_response(&body, "rpc", "cid-1").expect_err("unknown status must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_invalid_status");
    }

    #[test]
    fn parse_adapter_submit_response_rejects_invalid_submitted_at_timestamp() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "submitted_at": "not-a-timestamp"
        });
        let error =
            parse_response(&body, "rpc", "cid-1").expect_err("invalid submitted_at must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_invalid_response");
        assert!(error.detail.contains("submitted_at"));
    }

    #[test]
    fn parse_adapter_submit_response_rejects_client_order_id_mismatch() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "client_order_id": "cid-2"
        });
        let error =
            parse_response(&body, "rpc", "cid-1").expect_err("client_order_id mismatch must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_client_order_id_mismatch");
    }

    #[test]
    fn parse_adapter_submit_response_rejects_missing_required_policy_echo() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc"
        });
        let error = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", true, 50.0, 0, 300_000, 1_000,
        )
        .expect_err("missing policy echo must fail in strict mode");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_policy_echo_missing");
    }

    #[test]
    fn parse_adapter_submit_response_rejects_contract_version_mismatch() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "contract_version": "v2",
            "slippage_bps": 50.0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let error = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", true, 50.0, 0, 300_000, 1_000,
        )
        .expect_err("contract version mismatch must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_contract_version_mismatch");
    }

    #[test]
    fn parse_adapter_submit_response_rejects_compute_budget_mismatch() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 50.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 310000,
                "cu_price_micro_lamports": 1000
            }
        });
        let error = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", true, 50.0, 0, 300_000, 1_000,
        )
        .expect_err("compute budget mismatch must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_policy_mismatch");
    }

    #[test]
    fn parse_adapter_submit_response_accepts_small_slippage_rounding_delta() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 50.0000004,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            },
            "network_fee_lamports": 17000,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 12000
        });
        let result = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", true, 50.0, 0, 300_000, 1_000,
        )
        .expect("small floating point noise in slippage_bps should be tolerated");
        assert_eq!(result.route, "rpc");
    }

    #[test]
    fn parse_adapter_submit_response_rejects_tip_mismatch() {
        let body = json!({
            "status": "ok",
            "tx_signature": "5ig1ature",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 50.0,
            "tip_lamports": 1000,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let error = parse_adapter_submit_response(
            &body, "rpc", "cid-1", "v1", true, 50.0, 500, 300_000, 1_000,
        )
        .expect_err("tip mismatch must fail");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_policy_mismatch");
    }

    #[test]
    fn parse_recent_priority_fee_percentile_from_rpc_body_selects_nearest_rank() {
        let body = json!({
            "result": [
                { "prioritizationFee": 100 },
                { "prioritizationFee": 500 },
                { "prioritizationFee": 200 },
                { "prioritizationFee": 900 }
            ]
        });
        assert_eq!(
            parse_recent_priority_fee_percentile_from_rpc_body(&body, 50),
            Some(200)
        );
        assert_eq!(
            parse_recent_priority_fee_percentile_from_rpc_body(&body, 90),
            Some(900)
        );
    }

    #[test]
    fn parse_recent_priority_fee_percentile_from_rpc_body_ignores_non_numeric_rows() {
        let body = json!({
            "result": [
                { "prioritizationFee": "bad" },
                { "prioritizationFee": 2000 },
                { "foo": 1 }
            ]
        });
        assert_eq!(
            parse_recent_priority_fee_percentile_from_rpc_body(&body, 80),
            Some(2000)
        );
    }

    #[test]
    fn priority_fee_lamports_from_compute_budget_rounds_up_micro_lamports() {
        assert_eq!(
            priority_fee_lamports_from_compute_budget(300_000, 1_500),
            450
        );
        assert_eq!(priority_fee_lamports_from_compute_budget(1, 1), 1);
    }

    #[test]
    fn dynamic_tip_lamports_from_compute_budget_applies_multiplier_bps() {
        let tip = dynamic_tip_lamports_from_compute_budget(300_000, 3_000, 20_000);
        assert_eq!(tip, 1_800);
    }

    #[test]
    fn dynamic_tip_lamports_from_compute_budget_saturates_at_global_tip_cap() {
        let tip = dynamic_tip_lamports_from_compute_budget(u32::MAX, u64::MAX, u32::MAX);
        assert_eq!(tip, EXECUTION_ROUTE_TIP_LAMPORTS_MAX);
    }

    #[test]
    fn priority_fee_hint_timeout_ms_caps_high_submit_timeout() {
        assert_eq!(priority_fee_hint_timeout_ms(3_000), 1_000);
    }

    #[test]
    fn priority_fee_hint_timeout_ms_applies_floor_for_small_submit_timeout() {
        assert_eq!(priority_fee_hint_timeout_ms(100), 250);
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
            "v1",
            false,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_tips("rpc", 0),
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
            "v1",
            false,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_tips("rpc", 0),
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
            "v1",
            false,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_tips("rpc", 0),
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
            "v1",
            false,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_tips("rpc", 0),
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
            "v1",
            false,
            &["rpc".to_string()],
            &make_route_caps("paper", 50.0),
            &make_route_tips("rpc", 0),
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
            "v1",
            false,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_tips("rpc", 0),
            &make_route_cu_limits("paper", 300_000),
            &make_route_cu_prices("paper", 1_000),
            1_000,
            50.0,
        );
        assert!(submitter.is_none());
    }

    #[test]
    fn adapter_submitter_requires_tip_policy_for_allowed_route() {
        let submitter = AdapterOrderSubmitter::new(
            "https://adapter.example/submit",
            "",
            "",
            "",
            "",
            30,
            "v1",
            false,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_tips("paper", 0),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_000),
            1_000,
            50.0,
        );
        assert!(submitter.is_none());
    }

    #[test]
    fn adapter_submitter_rejects_tip_above_guardrail() {
        let submitter = AdapterOrderSubmitter::new(
            "https://adapter.example/submit",
            "",
            "",
            "",
            "",
            30,
            "v1",
            false,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_tips("rpc", EXECUTION_ROUTE_TIP_LAMPORTS_MAX.saturating_add(1)),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_000),
            1_000,
            50.0,
        );
        assert!(submitter.is_none());
    }

    #[test]
    fn adapter_submitter_requires_non_empty_contract_version() {
        let submitter = AdapterOrderSubmitter::new(
            "https://adapter.example/submit",
            "",
            "",
            "",
            "",
            30,
            "",
            false,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_tips("rpc", 0),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_000),
            1_000,
            50.0,
        );
        assert!(submitter.is_none());

        let invalid_token = AdapterOrderSubmitter::new(
            "https://adapter.example/submit",
            "",
            "",
            "",
            "",
            30,
            "v1 beta",
            false,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_tips("rpc", 0),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_000),
            1_000,
            50.0,
        );
        assert!(invalid_token.is_none());
    }

    #[test]
    fn adapter_submitter_dynamic_cu_price_requires_non_zero_cap() {
        let submitter = AdapterOrderSubmitter::new_with_dynamic(
            "https://adapter.example/submit",
            "",
            "",
            "",
            "",
            30,
            "v1",
            false,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_tips("rpc", 0),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_000),
            "http://rpc.primary",
            "",
            true,
            75,
            0,
            1_000,
            50.0,
        );
        assert!(submitter.is_none());
    }

    #[test]
    fn adapter_submitter_dynamic_cu_price_uses_capped_hint_timeout_budget() {
        let submitter = AdapterOrderSubmitter::new_with_dynamic(
            "https://adapter.example/submit",
            "",
            "",
            "",
            "",
            30,
            "v1",
            false,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_tips("rpc", 0),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_000),
            "http://rpc.primary",
            "http://rpc.fallback",
            true,
            75,
            5_000,
            3_000,
            50.0,
        )
        .expect("submitter should initialize");
        assert_eq!(
            submitter
                .dynamic_cu_price_policy
                .as_ref()
                .map(|value| value.timeout_ms),
            Some(1_000)
        );
    }

    #[test]
    fn adapter_submitter_dynamic_cu_price_rejects_route_policy_above_cap() {
        let submitter = AdapterOrderSubmitter::new_with_dynamic(
            "https://adapter.example/submit",
            "",
            "",
            "",
            "",
            30,
            "v1",
            false,
            &["rpc".to_string()],
            &make_route_caps("rpc", 50.0),
            &make_route_tips("rpc", 0),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_500),
            "http://rpc.primary",
            "",
            true,
            75,
            1_000,
            1_000,
            50.0,
        );
        assert!(submitter.is_none());
    }

    #[test]
    fn adapter_submitter_posts_route_tip_and_budget_policy() {
        let response = json!({
            "status": "ok",
            "tx_signature": "sig-123",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 45.0,
            "tip_lamports": 777,
            "network_fee_lamports": 17000,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 12000,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let Some((endpoint, handle)) = spawn_one_shot_adapter(200, response) else {
            return;
        };
        let submitter = AdapterOrderSubmitter::new(
            &endpoint,
            "",
            "",
            "",
            "",
            30,
            "v1",
            true,
            &["rpc".to_string()],
            &make_route_caps("rpc", 45.0),
            &make_route_tips("rpc", 777),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_500),
            2_000,
            50.0,
        )
        .expect("submitter should initialize");
        let result = submitter
            .submit(&make_intent(), "cid-integration-1", "rpc")
            .expect("submit call should succeed");
        assert_eq!(result.route, "rpc");
        assert_eq!(result.tx_signature, "sig-123");

        let captured = handle.join().expect("join adapter server thread");
        assert_eq!(captured.path, "/submit");
        assert_eq!(
            captured
                .headers
                .get("content-type")
                .map(String::as_str)
                .unwrap_or_default(),
            "application/json"
        );
        let payload: Value = serde_json::from_str(&captured.body).expect("parse captured payload");
        assert_eq!(
            payload
                .get("route")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            "rpc"
        );
        assert_eq!(
            payload
                .get("tip_lamports")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            777
        );
        assert_eq!(
            payload
                .get("compute_budget")
                .and_then(|value| value.get("cu_limit"))
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            300_000
        );
        assert_eq!(
            payload
                .get("compute_budget")
                .and_then(|value| value.get("cu_price_micro_lamports"))
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            1_500
        );
    }

    #[test]
    fn adapter_submitter_rejects_missing_tip_echo_in_strict_mode() {
        let response = json!({
            "status": "ok",
            "tx_signature": "sig-123",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 45.0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let Some((endpoint, handle)) = spawn_one_shot_adapter(200, response) else {
            return;
        };
        let submitter = AdapterOrderSubmitter::new(
            &endpoint,
            "",
            "",
            "",
            "",
            30,
            "v1",
            true,
            &["rpc".to_string()],
            &make_route_caps("rpc", 45.0),
            &make_route_tips("rpc", 777),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_500),
            2_000,
            50.0,
        )
        .expect("submitter should initialize");
        let error = submitter
            .submit(&make_intent(), "cid-integration-2", "rpc")
            .expect_err("missing tip echo should fail in strict mode");
        assert_eq!(error.kind, SubmitErrorKind::Terminal);
        assert_eq!(error.code, "submit_adapter_policy_echo_missing");
        let _ = handle.join().expect("join adapter server thread");
    }

    #[test]
    fn adapter_submitter_hmac_signature_matches_raw_http_body() {
        let response = json!({
            "status": "ok",
            "tx_signature": "sig-123",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 45.0,
            "tip_lamports": 777,
            "network_fee_lamports": 17000,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 12000,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let Some((endpoint, handle)) = spawn_one_shot_adapter(200, response) else {
            return;
        };
        let hmac_secret = "super-secret";
        let submitter = AdapterOrderSubmitter::new(
            &endpoint,
            "",
            "",
            "key-123",
            hmac_secret,
            30,
            "v1",
            true,
            &["rpc".to_string()],
            &make_route_caps("rpc", 45.0),
            &make_route_tips("rpc", 777),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_500),
            2_000,
            50.0,
        )
        .expect("submitter should initialize");
        submitter
            .submit(&make_intent(), "cid-integration-3", "rpc")
            .expect("submit call should succeed");

        let captured = handle.join().expect("join adapter server thread");
        let headers = &captured.headers;
        let key_id = headers
            .get("x-copybot-key-id")
            .map(String::as_str)
            .unwrap_or_default();
        let timestamp = headers
            .get("x-copybot-timestamp")
            .map(String::as_str)
            .unwrap_or_default();
        let ttl = headers
            .get("x-copybot-auth-ttl-sec")
            .map(String::as_str)
            .unwrap_or_default();
        let nonce = headers
            .get("x-copybot-nonce")
            .map(String::as_str)
            .unwrap_or_default();
        let signature = headers
            .get("x-copybot-signature")
            .map(String::as_str)
            .unwrap_or_default();
        let alg = headers
            .get("x-copybot-signature-alg")
            .map(String::as_str)
            .unwrap_or_default();

        assert_eq!(key_id, "key-123");
        assert_eq!(ttl, "30");
        assert_eq!(alg, "hmac-sha256-v1");
        assert!(!timestamp.is_empty(), "timestamp header must be present");
        assert!(!nonce.is_empty(), "nonce header must be present");
        assert!(!signature.is_empty(), "signature header must be present");

        let signed_payload = format!("{}\n{}\n{}\n{}", timestamp, ttl, nonce, captured.body);
        let expected_signature =
            compute_hmac_signature_hex(hmac_secret.as_bytes(), signed_payload.as_bytes())
                .expect("compute expected signature");
        assert_eq!(signature, expected_signature);
    }

    #[test]
    fn adapter_submitter_dynamic_cu_price_uses_recent_priority_fee_with_cap() {
        let rpc_response = json!({
            "jsonrpc": "2.0",
            "result": [
                { "prioritizationFee": 1200 },
                { "prioritizationFee": 2600 },
                { "prioritizationFee": 4400 }
            ]
        });
        let Some((rpc_endpoint, rpc_handle)) = spawn_one_shot_adapter(200, rpc_response) else {
            return;
        };
        let adapter_response = json!({
            "status": "ok",
            "tx_signature": "sig-dynamic-1",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 45.0,
            "tip_lamports": 777,
            "network_fee_lamports": 17000,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 12000,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 3500
            }
        });
        let Some((adapter_endpoint, adapter_handle)) =
            spawn_one_shot_adapter(200, adapter_response)
        else {
            let _ = rpc_handle.join();
            return;
        };
        let submitter = AdapterOrderSubmitter::new_with_dynamic(
            &adapter_endpoint,
            "",
            "",
            "",
            "",
            30,
            "v1",
            true,
            &["rpc".to_string()],
            &make_route_caps("rpc", 45.0),
            &make_route_tips("rpc", 777),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_500),
            &rpc_endpoint,
            "",
            true,
            90,
            3_500,
            2_000,
            50.0,
        )
        .expect("submitter should initialize");
        let result = submitter
            .submit(&make_intent(), "cid-dynamic-1", "rpc")
            .expect("submit call should succeed");
        assert_eq!(result.tx_signature, "sig-dynamic-1");
        assert!(result.dynamic_cu_price_policy_enabled);
        assert!(result.dynamic_cu_price_hint_used);
        assert!(result.dynamic_cu_price_applied);
        assert!(!result.dynamic_tip_policy_enabled);
        assert!(!result.dynamic_tip_applied);

        let rpc_captured = rpc_handle.join().expect("join rpc server thread");
        let rpc_payload: Value =
            serde_json::from_str(&rpc_captured.body).expect("parse captured rpc payload");
        assert_eq!(
            rpc_payload
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            "getRecentPrioritizationFees"
        );

        let adapter_captured = adapter_handle.join().expect("join adapter server thread");
        let adapter_payload: Value =
            serde_json::from_str(&adapter_captured.body).expect("parse captured adapter payload");
        assert_eq!(
            adapter_payload
                .get("compute_budget")
                .and_then(|value| value.get("cu_price_micro_lamports"))
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            3_500
        );
    }

    #[test]
    fn adapter_submitter_dynamic_cu_price_falls_back_to_static_route_price_when_rpc_unavailable() {
        let adapter_response = json!({
            "status": "ok",
            "tx_signature": "sig-dynamic-2",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 45.0,
            "tip_lamports": 777,
            "network_fee_lamports": 17000,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 12000,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let Some((adapter_endpoint, adapter_handle)) =
            spawn_one_shot_adapter(200, adapter_response)
        else {
            return;
        };
        let submitter = AdapterOrderSubmitter::new_with_dynamic(
            &adapter_endpoint,
            "",
            "",
            "",
            "",
            30,
            "v1",
            true,
            &["rpc".to_string()],
            &make_route_caps("rpc", 45.0),
            &make_route_tips("rpc", 777),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_500),
            "http://127.0.0.1:1",
            "",
            true,
            90,
            3_500,
            2_000,
            50.0,
        )
        .expect("submitter should initialize");
        let result = submitter
            .submit(&make_intent(), "cid-dynamic-2", "rpc")
            .expect("submit call should succeed with static fallback");
        assert!(result.dynamic_cu_price_policy_enabled);
        assert!(!result.dynamic_cu_price_hint_used);
        assert!(!result.dynamic_cu_price_applied);
        assert!(!result.dynamic_tip_policy_enabled);
        assert!(!result.dynamic_tip_applied);

        let adapter_captured = adapter_handle.join().expect("join adapter server thread");
        let adapter_payload: Value =
            serde_json::from_str(&adapter_captured.body).expect("parse captured adapter payload");
        assert_eq!(
            adapter_payload
                .get("compute_budget")
                .and_then(|value| value.get("cu_price_micro_lamports"))
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            1_500
        );
    }

    #[test]
    fn adapter_submitter_dynamic_cu_price_marks_hint_used_when_hint_below_static_floor() {
        let rpc_response = json!({
            "jsonrpc": "2.0",
            "result": [
                { "prioritizationFee": 1200 }
            ]
        });
        let Some((rpc_endpoint, rpc_handle)) = spawn_one_shot_adapter(200, rpc_response) else {
            return;
        };
        let adapter_response = json!({
            "status": "ok",
            "tx_signature": "sig-dynamic-floor",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 45.0,
            "tip_lamports": 777,
            "network_fee_lamports": 17000,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 12000,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let Some((adapter_endpoint, adapter_handle)) =
            spawn_one_shot_adapter(200, adapter_response)
        else {
            let _ = rpc_handle.join();
            return;
        };
        let submitter = AdapterOrderSubmitter::new_with_dynamic(
            &adapter_endpoint,
            "",
            "",
            "",
            "",
            30,
            "v1",
            true,
            &["rpc".to_string()],
            &make_route_caps("rpc", 45.0),
            &make_route_tips("rpc", 777),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_500),
            &rpc_endpoint,
            "",
            true,
            90,
            3_500,
            2_000,
            50.0,
        )
        .expect("submitter should initialize");
        let result = submitter
            .submit(&make_intent(), "cid-dynamic-floor", "rpc")
            .expect("submit call should succeed with static floor");
        assert!(result.dynamic_cu_price_policy_enabled);
        assert!(result.dynamic_cu_price_hint_used);
        assert!(!result.dynamic_cu_price_applied);

        let adapter_captured = adapter_handle.join().expect("join adapter server thread");
        let adapter_payload: Value =
            serde_json::from_str(&adapter_captured.body).expect("parse captured adapter payload");
        assert_eq!(
            adapter_payload
                .get("compute_budget")
                .and_then(|value| value.get("cu_price_micro_lamports"))
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            1_500
        );
        let _ = rpc_handle.join();
    }

    #[test]
    fn adapter_submitter_dynamic_cu_price_total_timeout_budget_prevents_additive_endpoint_delay() {
        let slow_rpc_response = json!({
            "jsonrpc": "2.0",
            "result": [{ "prioritizationFee": 3000 }]
        });
        let Some((slow_rpc_endpoint, slow_rpc_handle)) =
            spawn_probe_server_with_optional_capture("rpc", 200, slow_rpc_response, 1_500, 2_500)
        else {
            return;
        };
        let fast_rpc_response = json!({
            "jsonrpc": "2.0",
            "result": [{ "prioritizationFee": 4500 }]
        });
        let Some((fast_rpc_endpoint, fast_rpc_handle)) =
            spawn_probe_server_with_optional_capture("rpc", 200, fast_rpc_response, 600, 1_500)
        else {
            let _ = slow_rpc_handle.join();
            return;
        };
        let adapter_response = json!({
            "status": "ok",
            "tx_signature": "sig-dynamic-budget",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 45.0,
            "tip_lamports": 777,
            "network_fee_lamports": 17000,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 12000,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let Some((adapter_endpoint, adapter_handle)) =
            spawn_one_shot_adapter(200, adapter_response)
        else {
            let _ = slow_rpc_handle.join();
            let _ = fast_rpc_handle.join();
            return;
        };

        let submitter = AdapterOrderSubmitter::new_with_dynamic(
            &adapter_endpoint,
            "",
            "",
            "",
            "",
            30,
            "v1",
            true,
            &["rpc".to_string()],
            &make_route_caps("rpc", 45.0),
            &make_route_tips("rpc", 777),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_500),
            &slow_rpc_endpoint,
            &fast_rpc_endpoint,
            true,
            90,
            3_500,
            3_000,
            50.0,
        )
        .expect("submitter should initialize");

        let started = Instant::now();
        submitter
            .submit(&make_intent(), "cid-dynamic-budget", "rpc")
            .expect("submit should succeed with static fallback after hint timeout budget");
        let elapsed = started.elapsed();
        assert!(
            elapsed < StdDuration::from_millis(1_500),
            "hint phase should be capped by total timeout budget, got elapsed={:?}",
            elapsed
        );

        let slow_rpc_captured = slow_rpc_handle.join().expect("join slow rpc server");
        assert!(
            slow_rpc_captured.is_some(),
            "primary hint endpoint should be attempted"
        );
        let _ = fast_rpc_handle.join().expect("join fast rpc server");

        let adapter_captured = adapter_handle.join().expect("join adapter server thread");
        let adapter_payload: Value =
            serde_json::from_str(&adapter_captured.body).expect("parse captured adapter payload");
        assert_eq!(
            adapter_payload
                .get("compute_budget")
                .and_then(|value| value.get("cu_price_micro_lamports"))
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            1_500
        );
    }

    #[test]
    fn adapter_submitter_dynamic_tip_policy_raises_tip_from_dynamic_compute_budget() {
        let rpc_response = json!({
            "jsonrpc": "2.0",
            "result": [
                { "prioritizationFee": 3000 }
            ]
        });
        let Some((rpc_endpoint, rpc_handle)) = spawn_one_shot_adapter(200, rpc_response) else {
            return;
        };
        let adapter_response = json!({
            "status": "ok",
            "tx_signature": "sig-dynamic-tip",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 45.0,
            "tip_lamports": 1800,
            "network_fee_lamports": 17000,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 12000,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 3000
            }
        });
        let Some((adapter_endpoint, adapter_handle)) =
            spawn_one_shot_adapter(200, adapter_response)
        else {
            let _ = rpc_handle.join();
            return;
        };
        let submitter = AdapterOrderSubmitter::new_with_dynamic_and_tip(
            &adapter_endpoint,
            "",
            "",
            "",
            "",
            30,
            "v1",
            true,
            &["rpc".to_string()],
            &make_route_caps("rpc", 45.0),
            &make_route_tips("rpc", 1_000),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_500),
            &rpc_endpoint,
            "",
            true,
            90,
            3_500,
            true,
            20_000,
            2_000,
            50.0,
        )
        .expect("submitter should initialize");
        let result = submitter
            .submit(&make_intent(), "cid-dynamic-tip", "rpc")
            .expect("submit call should succeed");
        assert!(result.dynamic_cu_price_policy_enabled);
        assert!(result.dynamic_cu_price_hint_used);
        assert!(result.dynamic_cu_price_applied);
        assert!(result.dynamic_tip_policy_enabled);
        assert!(result.dynamic_tip_applied);

        let adapter_captured = adapter_handle.join().expect("join adapter server thread");
        let adapter_payload: Value =
            serde_json::from_str(&adapter_captured.body).expect("parse captured adapter payload");
        assert_eq!(
            adapter_payload
                .get("tip_lamports")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            1_800
        );
        let _ = rpc_handle.join();
    }

    #[test]
    fn adapter_submitter_dynamic_tip_policy_keeps_static_floor_when_dynamic_lower() {
        let rpc_response = json!({
            "jsonrpc": "2.0",
            "result": [
                { "prioritizationFee": 3000 }
            ]
        });
        let Some((rpc_endpoint, rpc_handle)) = spawn_one_shot_adapter(200, rpc_response) else {
            return;
        };
        let adapter_response = json!({
            "status": "ok",
            "tx_signature": "sig-dynamic-tip-floor",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 45.0,
            "tip_lamports": 2500,
            "network_fee_lamports": 17000,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 12000,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 3000
            }
        });
        let Some((adapter_endpoint, adapter_handle)) =
            spawn_one_shot_adapter(200, adapter_response)
        else {
            let _ = rpc_handle.join();
            return;
        };
        let submitter = AdapterOrderSubmitter::new_with_dynamic_and_tip(
            &adapter_endpoint,
            "",
            "",
            "",
            "",
            30,
            "v1",
            true,
            &["rpc".to_string()],
            &make_route_caps("rpc", 45.0),
            &make_route_tips("rpc", 2_500),
            &make_route_cu_limits("rpc", 300_000),
            &make_route_cu_prices("rpc", 1_500),
            &rpc_endpoint,
            "",
            true,
            90,
            3_500,
            true,
            20_000,
            2_000,
            50.0,
        )
        .expect("submitter should initialize");
        let result = submitter
            .submit(&make_intent(), "cid-dynamic-tip-floor", "rpc")
            .expect("submit call should succeed");
        assert!(result.dynamic_cu_price_policy_enabled);
        assert!(result.dynamic_cu_price_hint_used);
        assert!(result.dynamic_cu_price_applied);
        assert!(result.dynamic_tip_policy_enabled);
        assert!(!result.dynamic_tip_applied);

        let adapter_captured = adapter_handle.join().expect("join adapter server thread");
        let adapter_payload: Value =
            serde_json::from_str(&adapter_captured.body).expect("parse captured adapter payload");
        assert_eq!(
            adapter_payload
                .get("tip_lamports")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            2_500
        );
        let _ = rpc_handle.join();
    }

    #[test]
    fn adapter_submitter_dynamic_tip_policy_saturates_at_global_tip_cap() {
        let rpc_response = json!({
            "jsonrpc": "2.0",
            "result": [
                { "prioritizationFee": 10000000 }
            ]
        });
        let Some((rpc_endpoint, rpc_handle)) = spawn_one_shot_adapter(200, rpc_response) else {
            return;
        };
        let adapter_response = json!({
            "status": "ok",
            "tx_signature": "sig-dynamic-tip-cap",
            "route": "rpc",
            "contract_version": "v1",
            "slippage_bps": 45.0,
            "tip_lamports": EXECUTION_ROUTE_TIP_LAMPORTS_MAX,
            "network_fee_lamports": 17000,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 12000,
            "compute_budget": {
                "cu_limit": EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MAX,
                "cu_price_micro_lamports": EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MAX
            }
        });
        let Some((adapter_endpoint, adapter_handle)) =
            spawn_one_shot_adapter(200, adapter_response)
        else {
            let _ = rpc_handle.join();
            return;
        };
        let submitter = AdapterOrderSubmitter::new_with_dynamic_and_tip(
            &adapter_endpoint,
            "",
            "",
            "",
            "",
            30,
            "v1",
            true,
            &["rpc".to_string()],
            &make_route_caps("rpc", 45.0),
            &make_route_tips("rpc", 1_000),
            &make_route_cu_limits("rpc", EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MAX),
            &make_route_cu_prices("rpc", 1_500),
            &rpc_endpoint,
            "",
            true,
            90,
            EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MAX,
            true,
            100_000,
            2_000,
            50.0,
        )
        .expect("submitter should initialize");
        let result = submitter
            .submit(&make_intent(), "cid-dynamic-tip-cap", "rpc")
            .expect("submit call should succeed");
        assert!(result.dynamic_cu_price_policy_enabled);
        assert!(result.dynamic_cu_price_hint_used);
        assert!(result.dynamic_cu_price_applied);
        assert!(result.dynamic_tip_policy_enabled);
        assert!(result.dynamic_tip_applied);
        assert_eq!(
            result.applied_tip_lamports,
            EXECUTION_ROUTE_TIP_LAMPORTS_MAX
        );

        let adapter_captured = adapter_handle.join().expect("join adapter server thread");
        let adapter_payload: Value =
            serde_json::from_str(&adapter_captured.body).expect("parse captured adapter payload");
        assert_eq!(
            adapter_payload
                .get("compute_budget")
                .and_then(|value| value.get("cu_price_micro_lamports"))
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MAX
        );
        assert_eq!(
            adapter_payload
                .get("tip_lamports")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            EXECUTION_ROUTE_TIP_LAMPORTS_MAX
        );
        let _ = rpc_handle.join();
    }
}
