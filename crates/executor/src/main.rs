use anyhow::{anyhow, Context, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

mod http_utils;
mod fee_hints;
mod idempotency;
mod route_backend;
mod route_policy;
mod send_rpc;
mod submit_verify;

use crate::fee_hints::{resolve_fee_hints, FeeHintError, FeeHintInputs};
use crate::http_utils::{
    classify_request_error, endpoint_identity, redacted_endpoint_label, validate_endpoint_url,
};
use crate::idempotency::{SubmitClaimOutcome, SubmitIdempotencyStore};
use crate::route_backend::{RouteBackend, UpstreamAction};
use crate::route_policy::{apply_submit_tip_policy, requires_submit_fastlane_enabled};
use crate::send_rpc::send_signed_transaction_via_rpc;
#[cfg(test)]
use crate::submit_verify::{build_submit_signature_verify_config, SubmitSignatureVerification};
use crate::submit_verify::{
    parse_submit_signature_verify_config, submit_signature_verification_to_json,
    verify_submitted_signature_visibility, SubmitSignatureVerifyConfig,
};
#[cfg(test)]
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};

const TIP_MAX_LAMPORTS: u64 = 100_000_000;
const CU_LIMIT_MIN: u32 = 1;
const CU_LIMIT_MAX: u32 = 1_400_000;
const CU_PRICE_MIN: u64 = 1;
const CU_PRICE_MAX: u64 = 10_000_000;
const POLICY_FLOAT_EPSILON: f64 = 1e-6;
const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8090";
const DEFAULT_TIMEOUT_MS: u64 = 8_000;
const DEFAULT_MAX_NOTIONAL_SOL: f64 = 10.0;
const DEFAULT_BASE_FEE_LAMPORTS: u64 = 5_000;
const DEFAULT_SUBMIT_VERIFY_ATTEMPTS: u64 = 3;
const DEFAULT_SUBMIT_VERIFY_INTERVAL_MS: u64 = 250;
const DEFAULT_SUBMIT_TOTAL_BUDGET_MS: u64 = 7_000;
const DEFAULT_IDEMPOTENCY_CLAIM_TTL_SEC: u64 = 60;
const CLAIM_TTL_SAFETY_PADDING_MS: u64 = 1_000;
const KNOWN_ROUTES: &[&str] = &["paper", "rpc", "jito", "fastlane"];

#[derive(Clone)]
struct AppState {
    config: ExecutorConfig,
    http: Client,
    auth: Arc<AuthVerifier>,
    idempotency: Arc<SubmitIdempotencyStore>,
}

#[derive(Clone)]
struct ExecutorConfig {
    bind_addr: SocketAddr,
    contract_version: String,
    signer_pubkey: String,
    signer_source: SignerSource,
    signer_keypair_file: Option<String>,
    signer_kms_key_id: Option<String>,
    submit_fastlane_enabled: bool,
    route_allowlist: HashSet<String>,
    route_backends: HashMap<String, RouteBackend>,
    bearer_token: Option<String>,
    hmac_key_id: Option<String>,
    hmac_secret: Option<String>,
    hmac_ttl_sec: u64,
    request_timeout_ms: u64,
    submit_total_budget_ms: u64,
    idempotency_db_path: String,
    idempotency_claim_ttl_sec: u64,
    max_notional_sol: f64,
    allow_nonzero_tip: bool,
    submit_signature_verify: Option<SubmitSignatureVerifyConfig>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SignerSource {
    File,
    Kms,
}

impl SignerSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Kms => "kms",
        }
    }
}

impl ExecutorConfig {
    fn from_env() -> Result<Self> {
        let bind_addr = parse_socket_addr(
            env::var("COPYBOT_EXECUTOR_BIND_ADDR")
                .unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string()),
        )?;

        let contract_version = env::var("COPYBOT_EXECUTOR_CONTRACT_VERSION")
            .unwrap_or_else(|_| "v1".to_string())
            .trim()
            .to_string();
        if contract_version.is_empty()
            || contract_version.len() > 64
            || !is_valid_contract_version_token(&contract_version)
        {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_CONTRACT_VERSION must be non-empty token [A-Za-z0-9._-], len<=64"
            ));
        }

        let signer_pubkey = non_empty_env("COPYBOT_EXECUTOR_SIGNER_PUBKEY")?;
        validate_pubkey_like(signer_pubkey.as_str()).map_err(|error| {
            anyhow!(
                "COPYBOT_EXECUTOR_SIGNER_PUBKEY must be valid base58 pubkey-like value: {}",
                error
            )
        })?;
        let signer_source = resolve_signer_source_config(
            optional_non_empty_env("COPYBOT_EXECUTOR_SIGNER_SOURCE").as_deref(),
            optional_non_empty_env("COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE").as_deref(),
            optional_non_empty_env("COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID").as_deref(),
            signer_pubkey.as_str(),
        )?;
        let submit_fastlane_enabled =
            parse_bool_env("COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED", false);

        let route_allowlist = parse_route_allowlist(
            env::var("COPYBOT_EXECUTOR_ROUTE_ALLOWLIST")
                .unwrap_or_else(|_| "paper,rpc,jito".to_string()),
        )?;
        if route_allowlist.is_empty() {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_ROUTE_ALLOWLIST must contain at least one route"
            ));
        }
        validate_fastlane_route_policy(&route_allowlist, submit_fastlane_enabled)?;

        let default_submit = optional_non_empty_env("COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL");
        let default_submit_fallback =
            optional_non_empty_env("COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_FALLBACK_URL");
        let default_simulate = optional_non_empty_env("COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL");
        let default_simulate_fallback =
            optional_non_empty_env("COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_FALLBACK_URL");
        let default_send_rpc = optional_non_empty_env("COPYBOT_EXECUTOR_SEND_RPC_URL");
        let default_send_rpc_fallback =
            optional_non_empty_env("COPYBOT_EXECUTOR_SEND_RPC_FALLBACK_URL");
        let default_auth_token = resolve_secret_source(
            "COPYBOT_EXECUTOR_UPSTREAM_AUTH_TOKEN",
            optional_non_empty_env("COPYBOT_EXECUTOR_UPSTREAM_AUTH_TOKEN").as_deref(),
            "COPYBOT_EXECUTOR_UPSTREAM_AUTH_TOKEN_FILE",
            optional_non_empty_env("COPYBOT_EXECUTOR_UPSTREAM_AUTH_TOKEN_FILE").as_deref(),
        )?;
        let default_fallback_auth_token = resolve_secret_source(
            "COPYBOT_EXECUTOR_UPSTREAM_FALLBACK_AUTH_TOKEN",
            optional_non_empty_env("COPYBOT_EXECUTOR_UPSTREAM_FALLBACK_AUTH_TOKEN").as_deref(),
            "COPYBOT_EXECUTOR_UPSTREAM_FALLBACK_AUTH_TOKEN_FILE",
            optional_non_empty_env("COPYBOT_EXECUTOR_UPSTREAM_FALLBACK_AUTH_TOKEN_FILE").as_deref(),
        )?;
        let default_send_rpc_auth_token = resolve_secret_source(
            "COPYBOT_EXECUTOR_SEND_RPC_AUTH_TOKEN",
            optional_non_empty_env("COPYBOT_EXECUTOR_SEND_RPC_AUTH_TOKEN").as_deref(),
            "COPYBOT_EXECUTOR_SEND_RPC_AUTH_TOKEN_FILE",
            optional_non_empty_env("COPYBOT_EXECUTOR_SEND_RPC_AUTH_TOKEN_FILE").as_deref(),
        )?;
        let default_send_rpc_fallback_auth_token = resolve_secret_source(
            "COPYBOT_EXECUTOR_SEND_RPC_FALLBACK_AUTH_TOKEN",
            optional_non_empty_env("COPYBOT_EXECUTOR_SEND_RPC_FALLBACK_AUTH_TOKEN").as_deref(),
            "COPYBOT_EXECUTOR_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE",
            optional_non_empty_env("COPYBOT_EXECUTOR_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE").as_deref(),
        )?;

        let mut route_backends = HashMap::new();
        for route in &route_allowlist {
            let route_upper = route.to_ascii_uppercase();
            let submit_key = format!("COPYBOT_EXECUTOR_ROUTE_{}_SUBMIT_URL", route_upper);
            let submit_fallback_key =
                format!("COPYBOT_EXECUTOR_ROUTE_{}_SUBMIT_FALLBACK_URL", route_upper);
            let simulate_key = format!("COPYBOT_EXECUTOR_ROUTE_{}_SIMULATE_URL", route_upper);
            let simulate_fallback_key = format!(
                "COPYBOT_EXECUTOR_ROUTE_{}_SIMULATE_FALLBACK_URL",
                route_upper
            );
            let send_rpc_key = format!("COPYBOT_EXECUTOR_ROUTE_{}_SEND_RPC_URL", route_upper);
            let send_rpc_fallback_key = format!(
                "COPYBOT_EXECUTOR_ROUTE_{}_SEND_RPC_FALLBACK_URL",
                route_upper
            );
            let auth_key = format!("COPYBOT_EXECUTOR_ROUTE_{}_AUTH_TOKEN", route_upper);
            let auth_file_key = format!("COPYBOT_EXECUTOR_ROUTE_{}_AUTH_TOKEN_FILE", route_upper);
            let fallback_auth_key =
                format!("COPYBOT_EXECUTOR_ROUTE_{}_FALLBACK_AUTH_TOKEN", route_upper);
            let fallback_auth_file_key = format!(
                "COPYBOT_EXECUTOR_ROUTE_{}_FALLBACK_AUTH_TOKEN_FILE",
                route_upper
            );
            let send_rpc_auth_key =
                format!("COPYBOT_EXECUTOR_ROUTE_{}_SEND_RPC_AUTH_TOKEN", route_upper);
            let send_rpc_auth_file_key = format!(
                "COPYBOT_EXECUTOR_ROUTE_{}_SEND_RPC_AUTH_TOKEN_FILE",
                route_upper
            );
            let send_rpc_fallback_auth_key = format!(
                "COPYBOT_EXECUTOR_ROUTE_{}_SEND_RPC_FALLBACK_AUTH_TOKEN",
                route_upper
            );
            let send_rpc_fallback_auth_file_key = format!(
                "COPYBOT_EXECUTOR_ROUTE_{}_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE",
                route_upper
            );

            let submit_url = optional_non_empty_env(submit_key.as_str())
                .or_else(|| default_submit.clone())
                .ok_or_else(|| {
                    anyhow!(
                        "missing submit backend URL for route={} (set {} or COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL)",
                        route,
                        submit_key
                    )
                })?;
            let simulate_url = optional_non_empty_env(simulate_key.as_str())
                .or_else(|| default_simulate.clone())
                .ok_or_else(|| {
                    anyhow!(
                        "missing simulate backend URL for route={} (set {} or COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL)",
                        route,
                        simulate_key
                    )
                })?;
            let submit_fallback_url = optional_non_empty_env(submit_fallback_key.as_str())
                .or_else(|| default_submit_fallback.clone());
            let simulate_fallback_url = optional_non_empty_env(simulate_fallback_key.as_str())
                .or_else(|| default_simulate_fallback.clone());
            let send_rpc_url =
                optional_non_empty_env(send_rpc_key.as_str()).or_else(|| default_send_rpc.clone());
            let send_rpc_fallback_url = optional_non_empty_env(send_rpc_fallback_key.as_str())
                .or_else(|| default_send_rpc_fallback.clone());
            validate_endpoint_url(submit_url.as_str())
                .map_err(|error| anyhow!("invalid submit URL for route={}: {}", route, error))?;
            validate_endpoint_url(simulate_url.as_str())
                .map_err(|error| anyhow!("invalid simulate URL for route={}: {}", route, error))?;
            if let Some(url) = submit_fallback_url.as_deref() {
                validate_endpoint_url(url).map_err(|error| {
                    anyhow!("invalid submit fallback URL for route={}: {}", route, error)
                })?;
                if endpoint_identity(url)? == endpoint_identity(submit_url.as_str())? {
                    return Err(anyhow!(
                        "submit fallback URL for route={} must resolve to distinct endpoint",
                        route
                    ));
                }
            }
            if let Some(url) = simulate_fallback_url.as_deref() {
                validate_endpoint_url(url).map_err(|error| {
                    anyhow!(
                        "invalid simulate fallback URL for route={}: {}",
                        route,
                        error
                    )
                })?;
                if endpoint_identity(url)? == endpoint_identity(simulate_url.as_str())? {
                    return Err(anyhow!(
                        "simulate fallback URL for route={} must resolve to distinct endpoint",
                        route
                    ));
                }
            }
            if send_rpc_url.is_none() && send_rpc_fallback_url.is_some() {
                return Err(anyhow!(
                    "send RPC fallback URL for route={} requires primary send RPC URL",
                    route
                ));
            }
            if let Some(url) = send_rpc_url.as_deref() {
                validate_endpoint_url(url).map_err(|error| {
                    anyhow!("invalid send RPC URL for route={}: {}", route, error)
                })?;
            }
            if let Some(url) = send_rpc_fallback_url.as_deref() {
                validate_endpoint_url(url).map_err(|error| {
                    anyhow!(
                        "invalid send RPC fallback URL for route={}: {}",
                        route,
                        error
                    )
                })?;
                if endpoint_identity(url)?
                    == endpoint_identity(
                        send_rpc_url
                            .as_deref()
                            .expect("checked send_rpc_url before fallback"),
                    )?
                {
                    return Err(anyhow!(
                        "send RPC fallback URL for route={} must resolve to distinct endpoint",
                        route
                    ));
                }
            }

            let primary_auth_token = resolve_secret_source(
                auth_key.as_str(),
                optional_non_empty_env(auth_key.as_str()).as_deref(),
                auth_file_key.as_str(),
                optional_non_empty_env(auth_file_key.as_str()).as_deref(),
            )?
            .or_else(|| default_auth_token.clone());
            let fallback_auth_token = resolve_secret_source(
                fallback_auth_key.as_str(),
                optional_non_empty_env(fallback_auth_key.as_str()).as_deref(),
                fallback_auth_file_key.as_str(),
                optional_non_empty_env(fallback_auth_file_key.as_str()).as_deref(),
            )?
            .or_else(|| default_fallback_auth_token.clone())
            .or_else(|| primary_auth_token.clone());
            let send_rpc_primary_auth_token = resolve_secret_source(
                send_rpc_auth_key.as_str(),
                optional_non_empty_env(send_rpc_auth_key.as_str()).as_deref(),
                send_rpc_auth_file_key.as_str(),
                optional_non_empty_env(send_rpc_auth_file_key.as_str()).as_deref(),
            )?
            .or_else(|| default_send_rpc_auth_token.clone());
            let send_rpc_fallback_auth_token = resolve_secret_source(
                send_rpc_fallback_auth_key.as_str(),
                optional_non_empty_env(send_rpc_fallback_auth_key.as_str()).as_deref(),
                send_rpc_fallback_auth_file_key.as_str(),
                optional_non_empty_env(send_rpc_fallback_auth_file_key.as_str()).as_deref(),
            )?
            .or_else(|| default_send_rpc_fallback_auth_token.clone())
            .or_else(|| send_rpc_primary_auth_token.clone());

            route_backends.insert(
                route.clone(),
                RouteBackend {
                    submit_url,
                    submit_fallback_url,
                    simulate_url,
                    simulate_fallback_url,
                    primary_auth_token,
                    fallback_auth_token,
                    send_rpc_url,
                    send_rpc_fallback_url,
                    send_rpc_primary_auth_token,
                    send_rpc_fallback_auth_token,
                },
            );
        }

        let bearer_token = resolve_secret_source(
            "COPYBOT_EXECUTOR_BEARER_TOKEN",
            optional_non_empty_env("COPYBOT_EXECUTOR_BEARER_TOKEN").as_deref(),
            "COPYBOT_EXECUTOR_BEARER_TOKEN_FILE",
            optional_non_empty_env("COPYBOT_EXECUTOR_BEARER_TOKEN_FILE").as_deref(),
        )?;
        let hmac_key_id = optional_non_empty_env("COPYBOT_EXECUTOR_HMAC_KEY_ID");
        let hmac_secret = resolve_secret_source(
            "COPYBOT_EXECUTOR_HMAC_SECRET",
            optional_non_empty_env("COPYBOT_EXECUTOR_HMAC_SECRET").as_deref(),
            "COPYBOT_EXECUTOR_HMAC_SECRET_FILE",
            optional_non_empty_env("COPYBOT_EXECUTOR_HMAC_SECRET_FILE").as_deref(),
        )?;
        let hmac_ttl_sec = parse_u64_env("COPYBOT_EXECUTOR_HMAC_TTL_SEC", 30)?;
        let allow_unauthenticated = parse_bool_env("COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED", false);
        if (hmac_key_id.is_some() && hmac_secret.is_none())
            || (hmac_key_id.is_none() && hmac_secret.is_some())
        {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_HMAC_KEY_ID and COPYBOT_EXECUTOR_HMAC_SECRET must be set together"
            ));
        }
        if hmac_key_id.is_some() && !(5..=300).contains(&hmac_ttl_sec) {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_HMAC_TTL_SEC must be in 5..=300 when HMAC auth is enabled"
            ));
        }
        require_authenticated_mode(bearer_token.as_deref(), allow_unauthenticated)?;

        let request_timeout_ms =
            parse_u64_env("COPYBOT_EXECUTOR_REQUEST_TIMEOUT_MS", DEFAULT_TIMEOUT_MS)?;
        let submit_total_budget_ms = parse_u64_env(
            "COPYBOT_EXECUTOR_SUBMIT_TOTAL_BUDGET_MS",
            default_submit_total_budget_ms(request_timeout_ms),
        )?;
        let min_submit_budget_ms = request_timeout_ms.max(500);
        if submit_total_budget_ms < min_submit_budget_ms {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_SUBMIT_TOTAL_BUDGET_MS must be >= {} (effective request timeout floor)",
                min_submit_budget_ms
            ));
        }
        let idempotency_db_path = env::var("COPYBOT_EXECUTOR_IDEMPOTENCY_DB_PATH")
            .unwrap_or_else(|_| "state/executor_idempotency.sqlite3".to_string())
            .trim()
            .to_string();
        if idempotency_db_path.is_empty() {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_IDEMPOTENCY_DB_PATH must be non-empty"
            ));
        }
        let idempotency_claim_ttl_sec = parse_u64_env(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_CLAIM_TTL_SEC",
            DEFAULT_IDEMPOTENCY_CLAIM_TTL_SEC,
        )?;
        if idempotency_claim_ttl_sec == 0 {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_IDEMPOTENCY_CLAIM_TTL_SEC must be > 0"
            ));
        }
        let max_notional_sol = parse_f64_env(
            "COPYBOT_EXECUTOR_MAX_NOTIONAL_SOL",
            DEFAULT_MAX_NOTIONAL_SOL,
        )?;
        if !max_notional_sol.is_finite() || max_notional_sol <= 0.0 {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_MAX_NOTIONAL_SOL must be finite and > 0"
            ));
        }
        let allow_nonzero_tip = parse_bool_env("COPYBOT_EXECUTOR_ALLOW_NONZERO_TIP", true);
        let submit_signature_verify = parse_submit_signature_verify_config()?;
        let min_claim_ttl_sec = min_claim_ttl_sec_for_submit_path(
            request_timeout_ms,
            &route_backends,
            submit_signature_verify.as_ref(),
        );
        if idempotency_claim_ttl_sec < min_claim_ttl_sec {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_IDEMPOTENCY_CLAIM_TTL_SEC must be >= {} (derived from request_timeout_ms={}, route fallback topology, send_rpc topology, and submit signature verify settings)",
                min_claim_ttl_sec,
                request_timeout_ms.max(500)
            ));
        }

        Ok(Self {
            bind_addr,
            contract_version,
            signer_pubkey,
            signer_source,
            signer_keypair_file: optional_non_empty_env("COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE"),
            signer_kms_key_id: optional_non_empty_env("COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID"),
            submit_fastlane_enabled,
            route_allowlist,
            route_backends,
            bearer_token,
            hmac_key_id,
            hmac_secret,
            hmac_ttl_sec,
            request_timeout_ms,
            submit_total_budget_ms,
            idempotency_db_path,
            idempotency_claim_ttl_sec,
            max_notional_sol,
            allow_nonzero_tip,
            submit_signature_verify,
        })
    }
}

fn min_claim_ttl_sec_for_submit_path(
    request_timeout_ms: u64,
    route_backends: &HashMap<String, RouteBackend>,
    submit_signature_verify: Option<&SubmitSignatureVerifyConfig>,
) -> u64 {
    let effective_request_timeout_ms = request_timeout_ms.max(500);
    let submit_hops = route_backends
        .values()
        .map(|backend| backend.endpoint_chain(UpstreamAction::Submit).len() as u64)
        .max()
        .unwrap_or(1)
        .max(1);
    let send_rpc_hops = route_backends
        .values()
        .map(|backend| backend.send_rpc_endpoint_chain().len() as u64)
        .max()
        .unwrap_or(0);
    let verify_hops = submit_signature_verify
        .map(|config| {
            config
                .attempts
                .saturating_mul(config.endpoints.len() as u64)
                .max(1)
        })
        .unwrap_or(0);
    let verify_wait_ms = submit_signature_verify
        .map(|config| {
            config
                .interval_ms
                .saturating_mul(config.attempts.saturating_sub(1))
        })
        .unwrap_or(0);
    let total_hops = submit_hops
        .saturating_add(send_rpc_hops)
        .saturating_add(verify_hops)
        .max(1);
    let budget_ms = effective_request_timeout_ms
        .saturating_mul(total_hops)
        .saturating_add(verify_wait_ms)
        .saturating_add(CLAIM_TTL_SAFETY_PADDING_MS);
    (budget_ms.saturating_add(999) / 1000).max(1)
}

fn default_submit_total_budget_ms(request_timeout_ms: u64) -> u64 {
    request_timeout_ms
        .max(500)
        .saturating_mul(3)
        .saturating_add(1_000)
        .max(DEFAULT_SUBMIT_TOTAL_BUDGET_MS)
}

#[derive(Clone, Copy, Debug)]
struct SubmitDeadline {
    started_at: Instant,
    total_budget_ms: u64,
}

impl SubmitDeadline {
    fn new(total_budget_ms: u64) -> Self {
        Self {
            started_at: Instant::now(),
            total_budget_ms: total_budget_ms.max(1),
        }
    }

    fn elapsed_ms(&self) -> u64 {
        let millis = self.started_at.elapsed().as_millis();
        if millis > u128::from(u64::MAX) {
            u64::MAX
        } else {
            millis as u64
        }
    }

    fn remaining_timeout(&self, stage: &str) -> std::result::Result<Duration, Reject> {
        let elapsed_ms = self.elapsed_ms();
        if elapsed_ms >= self.total_budget_ms {
            return Err(Reject::retryable(
                "executor_submit_timeout_budget_exceeded",
                format!(
                    "submit timeout budget exceeded before stage={} elapsed_ms={} budget_ms={}",
                    stage, elapsed_ms, self.total_budget_ms
                ),
            ));
        }
        Ok(Duration::from_millis(
            self.total_budget_ms.saturating_sub(elapsed_ms).max(1),
        ))
    }
}

#[derive(Clone)]
struct AuthVerifier {
    bearer_token: Option<String>,
    hmac: Option<HmacConfig>,
    nonce_seen_until_epoch: Arc<Mutex<HashMap<String, i64>>>,
}

#[derive(Clone)]
struct HmacConfig {
    key_id: String,
    secret: String,
    ttl_sec: u64,
}

impl AuthVerifier {
    fn new(config: &ExecutorConfig) -> Self {
        let hmac = match (&config.hmac_key_id, &config.hmac_secret) {
            (Some(key_id), Some(secret)) => Some(HmacConfig {
                key_id: key_id.clone(),
                secret: secret.clone(),
                ttl_sec: config.hmac_ttl_sec,
            }),
            _ => None,
        };
        Self {
            bearer_token: config.bearer_token.clone(),
            hmac,
            nonce_seen_until_epoch: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn verify(
        &self,
        headers: &HeaderMap,
        raw_body: &[u8],
    ) -> std::result::Result<(), Reject> {
        if let Some(expected) = self.bearer_token.as_deref() {
            let auth_header = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .ok_or_else(|| Reject::terminal("auth_missing", "missing Authorization header"))?;
            let provided = auth_header.strip_prefix("Bearer ").ok_or_else(|| {
                Reject::terminal("auth_invalid", "Authorization header must use Bearer token")
            })?;
            if !constant_time_eq(provided.trim().as_bytes(), expected.as_bytes()) {
                return Err(Reject::terminal("auth_invalid", "Bearer token mismatch"));
            }
        }

        if let Some(hmac) = self.hmac.as_ref() {
            let key_id = get_required_header(headers, "x-copybot-key-id", "hmac_missing")?;
            if key_id != hmac.key_id {
                return Err(Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-key-id mismatch",
                ));
            }
            let alg = get_required_header(headers, "x-copybot-signature-alg", "hmac_missing")?;
            if alg != "hmac-sha256-v1" {
                return Err(Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-signature-alg must be hmac-sha256-v1",
                ));
            }
            let timestamp_raw =
                get_required_header(headers, "x-copybot-timestamp", "hmac_missing")?;
            let timestamp = timestamp_raw.parse::<i64>().map_err(|_| {
                Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-timestamp must be integer seconds",
                )
            })?;
            let ttl_raw = get_required_header(headers, "x-copybot-auth-ttl-sec", "hmac_missing")?;
            let ttl = ttl_raw.parse::<u64>().map_err(|_| {
                Reject::terminal("hmac_invalid", "x-copybot-auth-ttl-sec must be integer")
            })?;
            if ttl != hmac.ttl_sec {
                return Err(Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-auth-ttl-sec mismatch",
                ));
            }
            let nonce = get_required_header(headers, "x-copybot-nonce", "hmac_missing")?;
            if nonce.is_empty() || nonce.len() > 128 {
                return Err(Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-nonce must be 1..=128 chars",
                ));
            }
            let signature = get_required_header(headers, "x-copybot-signature", "hmac_missing")?;
            let now = Utc::now().timestamp();
            let max_skew = hmac.ttl_sec as i64;
            if (now - timestamp).abs() > max_skew {
                return Err(Reject::terminal(
                    "hmac_expired",
                    "HMAC timestamp outside TTL window",
                ));
            }

            {
                let mut seen = self.nonce_seen_until_epoch.lock().await;
                seen.retain(|_, expires_at| *expires_at >= now);
                let nonce_key = format!("{}:{}", key_id, nonce);
                if seen.contains_key(&nonce_key) {
                    return Err(Reject::terminal(
                        "hmac_replay",
                        "HMAC nonce replay detected",
                    ));
                }
                seen.insert(nonce_key, now + max_skew);
            }

            let payload = format!(
                "{}\n{}\n{}\n{}",
                timestamp,
                ttl,
                nonce,
                String::from_utf8_lossy(raw_body)
            );
            let expected_signature =
                compute_hmac_signature_hex(hmac.secret.as_bytes(), payload.as_bytes()).map_err(
                    |_| Reject::terminal("hmac_invalid", "failed computing HMAC signature"),
                )?;
            if !constant_time_eq(signature.as_bytes(), expected_signature.as_bytes()) {
                return Err(Reject::terminal("hmac_invalid", "HMAC signature mismatch"));
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
enum Side {
    Buy,
    Sell,
}

impl Side {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "buy" => Some(Self::Buy),
            "sell" => Some(Self::Sell),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct SimulateRequest {
    action: Option<String>,
    contract_version: Option<String>,
    request_id: String,
    signal_id: String,
    side: String,
    token: String,
    notional_sol: f64,
    signal_ts: String,
    route: String,
    dry_run: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ComputeBudgetRequest {
    cu_limit: u32,
    cu_price_micro_lamports: u64,
}

#[derive(Debug, Deserialize)]
struct SubmitRequest {
    contract_version: Option<String>,
    signal_id: String,
    client_order_id: String,
    request_id: String,
    side: String,
    token: String,
    notional_sol: f64,
    signal_ts: String,
    route: String,
    slippage_bps: f64,
    route_slippage_cap_bps: f64,
    tip_lamports: u64,
    compute_budget: ComputeBudgetRequest,
}

#[derive(Debug, Clone)]
struct Reject {
    retryable: bool,
    code: String,
    detail: String,
}

struct SubmitClaimGuard {
    idempotency: Arc<SubmitIdempotencyStore>,
    client_order_id: String,
    request_id: String,
}

impl SubmitClaimGuard {
    fn new(idempotency: Arc<SubmitIdempotencyStore>, client_order_id: &str, request_id: &str) -> Self {
        Self {
            idempotency,
            client_order_id: client_order_id.trim().to_string(),
            request_id: request_id.trim().to_string(),
        }
    }
}

impl Drop for SubmitClaimGuard {
    fn drop(&mut self) {
        match self
            .idempotency
            .release_submit_claim(self.client_order_id.as_str(), self.request_id.as_str())
        {
            Ok(true) => {}
            Ok(false) => {
                warn!(
                    client_order_id = %self.client_order_id,
                    request_id = %self.request_id,
                    "idempotency submit claim release had no owner-match row"
                );
            }
            Err(error) => {
                warn!(
                    client_order_id = %self.client_order_id,
                    request_id = %self.request_id,
                    error = %error,
                    "failed to release idempotency submit claim"
                );
            }
        }
    }
}

impl Reject {
    fn terminal(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            retryable: false,
            code: code.into(),
            detail: detail.into(),
        }
    }

    fn retryable(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            retryable: true,
            code: code.into(),
            detail: detail.into(),
        }
    }
}

#[derive(Debug)]
enum UpstreamOutcome {
    Success,
    Reject(Reject),
}

#[tokio::main]
async fn main() -> Result<()> {
    let log_json = parse_bool_env("COPYBOT_EXECUTOR_LOG_JSON", true);
    let log_filter = env::var("COPYBOT_EXECUTOR_LOG_FILTER")
        .unwrap_or_else(|_| "info,reqwest=warn,hyper=warn,h2=warn".to_string());
    let env_filter = EnvFilter::try_new(log_filter).unwrap_or_else(|_| EnvFilter::new("info"));
    if log_json {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }

    let config = ExecutorConfig::from_env()?;
    let http = Client::builder()
        .timeout(Duration::from_millis(config.request_timeout_ms.max(500)))
        .build()
        .context("failed to build reqwest client")?;
    let auth = Arc::new(AuthVerifier::new(&config));
    let idempotency = Arc::new(
        SubmitIdempotencyStore::open(config.idempotency_db_path.as_str())
            .context("failed to open idempotency store")?,
    );

    let state = AppState {
        config,
        http,
        auth,
        idempotency,
    };

    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/simulate", post(simulate))
        .route("/submit", post(submit))
        .with_state(state.clone());

    info!(
        bind_addr = %state.config.bind_addr,
        signer_pubkey = %state.config.signer_pubkey,
        signer_source = %state.config.signer_source.as_str(),
        signer_kms_key_id_configured = state.config.signer_kms_key_id.is_some(),
        signer_keypair_file_configured = state.config.signer_keypair_file.is_some(),
        contract_version = %state.config.contract_version,
        routes = ?state.config.route_allowlist,
        submit_fastlane_enabled = state.config.submit_fastlane_enabled,
        idempotency_db_path = %state.config.idempotency_db_path,
        idempotency_claim_ttl_sec = state.config.idempotency_claim_ttl_sec,
        submit_total_budget_ms = state.config.submit_total_budget_ms,
        submit_signature_verify_enabled = state.config.submit_signature_verify.is_some(),
        submit_signature_verify_strict = state
            .config
            .submit_signature_verify
            .as_ref()
            .map(|value| value.strict)
            .unwrap_or(false),
        "copybot executor started"
    );
    let listener = tokio::net::TcpListener::bind(state.config.bind_addr)
        .await
        .context("failed binding executor listener")?;
    axum::serve(listener, router)
        .await
        .context("executor server crashed")
}

async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    let idempotency_store_status = match state.idempotency.probe() {
        Ok(()) => "ok",
        Err(error) => {
            warn!(
                error = %error,
                "idempotency store probe failed in healthz"
            );
            "degraded"
        }
    };
    let status = if idempotency_store_status == "ok" {
        "ok"
    } else {
        "degraded"
    };
    Json(json!({
        "status": status,
        "contract_version": state.config.contract_version,
        "enabled_routes": state.config.route_allowlist,
        "signer_source": state.config.signer_source.as_str(),
        "signer_kms_key_id_configured": state.config.signer_kms_key_id.is_some(),
        "signer_keypair_file_configured": state.config.signer_keypair_file.is_some(),
        "submit_fastlane_enabled": state.config.submit_fastlane_enabled,
        "idempotency_store_status": idempotency_store_status,
        "signer_pubkey": state.config.signer_pubkey,
        "routes": state.config.route_allowlist,
    }))
}

async fn simulate(
    State(state): State<AppState>,
    headers: HeaderMap,
    raw_body: Bytes,
) -> impl IntoResponse {
    if let Err(reject) = state.auth.verify(&headers, raw_body.as_ref()).await {
        return (
            StatusCode::OK,
            Json(reject_to_json(
                &reject,
                None,
                &state.config.contract_version,
            )),
        );
    }

    let request: SimulateRequest = match serde_json::from_slice(raw_body.as_ref()) {
        Ok(value) => value,
        Err(error) => {
            return (
                StatusCode::OK,
                Json(reject_to_json(
                    &Reject::terminal(
                        "invalid_json",
                        format!("request body is not valid JSON: {}", error),
                    ),
                    None,
                    &state.config.contract_version,
                )),
            );
        }
    };

    match handle_simulate(&state, &request, raw_body.as_ref()).await {
        Ok(value) => (StatusCode::OK, Json(value)),
        Err(reject) => (
            StatusCode::OK,
            Json(reject_to_json(
                &reject,
                None,
                &state.config.contract_version,
            )),
        ),
    }
}

async fn submit(
    State(state): State<AppState>,
    headers: HeaderMap,
    raw_body: Bytes,
) -> impl IntoResponse {
    if let Err(reject) = state.auth.verify(&headers, raw_body.as_ref()).await {
        return (
            StatusCode::OK,
            Json(reject_to_json(
                &reject,
                None,
                &state.config.contract_version,
            )),
        );
    }

    let request: SubmitRequest = match serde_json::from_slice(raw_body.as_ref()) {
        Ok(value) => value,
        Err(error) => {
            return (
                StatusCode::OK,
                Json(reject_to_json(
                    &Reject::terminal(
                        "invalid_json",
                        format!("request body is not valid JSON: {}", error),
                    ),
                    None,
                    &state.config.contract_version,
                )),
            );
        }
    };

    let client_order_id = request.client_order_id.clone();
    match handle_submit(&state, &request, raw_body.as_ref()).await {
        Ok(value) => (StatusCode::OK, Json(value)),
        Err(reject) => (
            StatusCode::OK,
            Json(reject_to_json(
                &reject,
                Some(client_order_id.as_str()),
                &state.config.contract_version,
            )),
        ),
    }
}

async fn handle_simulate(
    state: &AppState,
    request: &SimulateRequest,
    raw_body: &[u8],
) -> std::result::Result<Value, Reject> {
    validate_common_contract(
        state,
        request.contract_version.as_deref(),
        request.route.as_str(),
        request.side.as_str(),
        request.token.as_str(),
        request.notional_sol,
    )?;
    if request
        .action
        .as_deref()
        .map(|value| value.trim().eq_ignore_ascii_case("simulate"))
        != Some(true)
    {
        return Err(Reject::terminal(
            "invalid_action",
            "simulate endpoint requires action=simulate",
        ));
    }
    if request.dry_run != Some(true) {
        return Err(Reject::terminal(
            "invalid_dry_run",
            "simulate endpoint requires dry_run=true",
        ));
    }
    parse_rfc3339_utc(request.signal_ts.as_str())
        .ok_or_else(|| Reject::terminal("invalid_signal_ts", "signal_ts must be RFC3339"))?;
    if request.request_id.trim().is_empty() {
        return Err(Reject::terminal(
            "invalid_request_id",
            "request_id must be non-empty",
        ));
    }
    if request.signal_id.trim().is_empty() {
        return Err(Reject::terminal(
            "invalid_signal_id",
            "signal_id must be non-empty",
        ));
    }

    let route = normalize_route(request.route.as_str());
    debug!(
        route = %route,
        signal_id = %request.signal_id,
        "handling simulate request"
    );
    let backend_response = forward_to_upstream(
        state,
        route.as_str(),
        UpstreamAction::Simulate,
        raw_body,
        None,
    )
    .await?;
    match parse_upstream_outcome(&backend_response, "simulation_rejected") {
        UpstreamOutcome::Reject(reject) => return Err(reject),
        UpstreamOutcome::Success => {}
    }

    // route echo can come from upstream; mismatch remains fail-closed.
    if let Some(response_route) = backend_response
        .get("route")
        .and_then(Value::as_str)
        .map(normalize_route)
    {
        if response_route != route {
            return Err(Reject::terminal(
                "simulation_route_mismatch",
                format!(
                    "upstream route={} does not match requested route={}",
                    response_route, route
                ),
            ));
        }
    }

    if let Some(version) = backend_response
        .get("contract_version")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if version != state.config.contract_version {
            return Err(Reject::terminal(
                "simulation_contract_version_mismatch",
                format!(
                    "upstream contract_version={} does not match expected={}",
                    version, state.config.contract_version
                ),
            ));
        }
    }

    let detail = backend_response
        .get("detail")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("adapter_simulation_ok")
        .to_string();

    Ok(json!({
        "status": "ok",
        "ok": true,
        "accepted": true,
        "route": route,
        "contract_version": state.config.contract_version,
        "request_id": request.request_id,
        "detail": detail
    }))
}

async fn handle_submit(
    state: &AppState,
    request: &SubmitRequest,
    raw_body: &[u8],
) -> std::result::Result<Value, Reject> {
    validate_common_contract(
        state,
        request.contract_version.as_deref(),
        request.route.as_str(),
        request.side.as_str(),
        request.token.as_str(),
        request.notional_sol,
    )?;
    parse_rfc3339_utc(request.signal_ts.as_str())
        .ok_or_else(|| Reject::terminal("invalid_signal_ts", "signal_ts must be RFC3339"))?;

    if request.client_order_id.trim().is_empty() {
        return Err(Reject::terminal(
            "invalid_client_order_id",
            "client_order_id must be non-empty",
        ));
    }
    if request.request_id.trim().is_empty() {
        return Err(Reject::terminal(
            "invalid_request_id",
            "request_id must be non-empty",
        ));
    }
    if request.signal_id.trim().is_empty() {
        return Err(Reject::terminal(
            "invalid_signal_id",
            "signal_id must be non-empty",
        ));
    }
    if !request.slippage_bps.is_finite() || request.slippage_bps <= 0.0 {
        return Err(Reject::terminal(
            "invalid_slippage_bps",
            "slippage_bps must be finite and > 0",
        ));
    }
    if !request.route_slippage_cap_bps.is_finite() || request.route_slippage_cap_bps <= 0.0 {
        return Err(Reject::terminal(
            "invalid_route_slippage_cap_bps",
            "route_slippage_cap_bps must be finite and > 0",
        ));
    }
    // Keep same tolerance as execution submitter echo checks to avoid float drift false rejects.
    // This checks only that caller did not exceed route cap.
    if request.slippage_bps - request.route_slippage_cap_bps > POLICY_FLOAT_EPSILON {
        return Err(Reject::terminal(
            "slippage_exceeds_route_cap",
            format!(
                "slippage_bps={} exceeds route_slippage_cap_bps={}",
                request.slippage_bps, request.route_slippage_cap_bps
            ),
        ));
    }
    let route = normalize_route(request.route.as_str());
    let (effective_tip_lamports, tip_policy_code) =
        apply_submit_tip_policy(route.as_str(), request.tip_lamports);

    if effective_tip_lamports > TIP_MAX_LAMPORTS {
        return Err(Reject::terminal(
            "invalid_tip_lamports",
            format!("tip_lamports exceeds max {}", TIP_MAX_LAMPORTS),
        ));
    }
    if effective_tip_lamports > 0 && !state.config.allow_nonzero_tip {
        return Err(Reject::terminal(
            "tip_not_supported",
            "non-zero tip_lamports is disabled in executor config",
        ));
    }
    if !(CU_LIMIT_MIN..=CU_LIMIT_MAX).contains(&request.compute_budget.cu_limit) {
        return Err(Reject::terminal(
            "invalid_compute_budget",
            format!(
                "compute_budget.cu_limit must be in {}..={}",
                CU_LIMIT_MIN, CU_LIMIT_MAX
            ),
        ));
    }
    if !(CU_PRICE_MIN..=CU_PRICE_MAX).contains(&request.compute_budget.cu_price_micro_lamports) {
        return Err(Reject::terminal(
            "invalid_compute_budget",
            format!(
                "compute_budget.cu_price_micro_lamports must be in {}..={}",
                CU_PRICE_MIN, CU_PRICE_MAX
            ),
        ));
    }

    let forward_body = build_submit_forward_payload(
        raw_body,
        request.tip_lamports,
        effective_tip_lamports,
    )?;
    if let Some(policy_code) = tip_policy_code {
        debug!(
            route = %route,
            policy_code = %policy_code,
            requested_tip_lamports = request.tip_lamports,
            effective_tip_lamports = effective_tip_lamports,
            "applied submit tip policy"
        );
    }
    debug!(
        route = %route,
        signal_id = %request.signal_id,
        client_order_id = %request.client_order_id,
        "handling submit request"
    );
    let submit_deadline = SubmitDeadline::new(state.config.submit_total_budget_ms);
    let _submit_claim_guard = match state.idempotency.load_cached_or_claim_submit(
        request.client_order_id.as_str(),
        request.request_id.as_str(),
        state.config.idempotency_claim_ttl_sec,
    ) {
        Ok(SubmitClaimOutcome::Cached(cached_response)) => {
            debug!(
                route = %route,
                signal_id = %request.signal_id,
                client_order_id = %request.client_order_id,
                "serving cached idempotent submit response"
            );
            return Ok(cached_response);
        }
        Ok(SubmitClaimOutcome::Claimed) => {
            SubmitClaimGuard::new(
                state.idempotency.clone(),
                request.client_order_id.as_str(),
                request.request_id.as_str(),
            )
        }
        Ok(SubmitClaimOutcome::InFlight) => {
            return Err(Reject::retryable(
                "submit_in_flight",
                format!(
                    "client_order_id={} is already being processed",
                    request.client_order_id
                ),
            ));
        }
        Err(error) => return Err(map_idempotency_error_to_reject(error)),
    };
    let backend_response = forward_to_upstream(
        state,
        route.as_str(),
        UpstreamAction::Submit,
        forward_body.as_slice(),
        Some(&submit_deadline),
    )
    .await?;
    match parse_upstream_outcome(&backend_response, "submit_adapter_rejected") {
        UpstreamOutcome::Reject(reject) => return Err(reject),
        UpstreamOutcome::Success => {}
    }

    if let Some(response_route) = backend_response
        .get("route")
        .and_then(Value::as_str)
        .map(normalize_route)
    {
        if response_route != route {
            return Err(Reject::terminal(
                "submit_adapter_route_mismatch",
                format!(
                    "upstream route={} does not match requested route={}",
                    response_route, route
                ),
            ));
        }
    }

    if let Some(version) = backend_response
        .get("contract_version")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if version != state.config.contract_version {
            return Err(Reject::terminal(
                "submit_adapter_contract_version_mismatch",
                format!(
                    "upstream contract_version={} does not match expected={}",
                    version, state.config.contract_version
                ),
            ));
        }
    }

    let upstream_tx_signature = backend_response
        .get("tx_signature")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let signed_tx_base64 = backend_response
        .get("signed_tx_base64")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let (tx_signature, submit_transport) = if let Some(value) = upstream_tx_signature {
        validate_signature_like(value).map_err(|error| {
            Reject::retryable(
                "submit_adapter_invalid_response",
                format!(
                    "upstream tx_signature is not valid base58 signature: {}",
                    error
                ),
            )
        })?;
        (value.to_string(), "upstream_signature")
    } else if let Some(value) = signed_tx_base64 {
        let signature =
            send_signed_transaction_via_rpc(state, route.as_str(), value, Some(&submit_deadline))
                .await?;
        (signature, "adapter_send_rpc")
    } else {
        return Err(Reject::retryable(
            "submit_adapter_invalid_response",
            "upstream response missing tx_signature and signed_tx_base64",
        ));
    };

    let submit_signature_verify = verify_submitted_signature_visibility(
        state,
        route.as_str(),
        tx_signature.as_str(),
        Some(&submit_deadline),
    )
    .await?;

    if let Some(response_client_order_id) = backend_response
        .get("client_order_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if response_client_order_id != request.client_order_id {
            return Err(Reject::terminal(
                "submit_adapter_client_order_id_mismatch",
                format!(
                    "upstream client_order_id={} does not match expected client_order_id={}",
                    response_client_order_id, request.client_order_id
                ),
            ));
        }
    }

    if let Some(response_request_id) = backend_response
        .get("request_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if response_request_id != request.request_id {
            return Err(Reject::terminal(
                "submit_adapter_request_id_mismatch",
                format!(
                    "upstream request_id={} does not match expected request_id={}",
                    response_request_id, request.request_id
                ),
            ));
        }
    }

    let submitted_at = match backend_response.get("submitted_at") {
        Some(value) => {
            let raw = value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    Reject::terminal(
                        "submit_adapter_invalid_response",
                        "submitted_at must be non-empty RFC3339 string",
                    )
                })?;
            parse_rfc3339_utc(raw).ok_or_else(|| {
                Reject::terminal(
                    "submit_adapter_invalid_response",
                    format!("submitted_at is not valid RFC3339: {}", raw),
                )
            })?
        }
        None => Utc::now(),
    };

    let ata_create_rent_lamports =
        parse_optional_non_negative_u64_field(&backend_response, "ata_create_rent_lamports")?;
    let response_network_fee =
        parse_optional_non_negative_u64_field(&backend_response, "network_fee_lamports")?;
    let response_base_fee =
        parse_optional_non_negative_u64_field(&backend_response, "base_fee_lamports")?;
    let response_priority_fee =
        parse_optional_non_negative_u64_field(&backend_response, "priority_fee_lamports")?;

    let resolved_fee_hints = resolve_fee_hints(FeeHintInputs {
        response_network_fee_lamports: response_network_fee,
        response_base_fee_lamports: response_base_fee,
        response_priority_fee_lamports: response_priority_fee,
        response_ata_create_rent_lamports: ata_create_rent_lamports,
        request_cu_limit: request.compute_budget.cu_limit,
        request_cu_price_micro_lamports: request.compute_budget.cu_price_micro_lamports,
        default_base_fee_lamports: DEFAULT_BASE_FEE_LAMPORTS,
    })
    .map_err(map_fee_hint_error_to_reject)?;

    let mut response = json!({
        "status": "ok",
        "ok": true,
        "accepted": true,
        "route": route,
        "contract_version": state.config.contract_version,
        "client_order_id": request.client_order_id,
        "request_id": request.request_id,
        "tx_signature": tx_signature,
        "submit_transport": submit_transport,
        "submitted_at": submitted_at.to_rfc3339(),
        "slippage_bps": request.slippage_bps,
        "tip_lamports": effective_tip_lamports,
        "compute_budget": {
            "cu_limit": request.compute_budget.cu_limit,
            "cu_price_micro_lamports": request.compute_budget.cu_price_micro_lamports,
        },
        "network_fee_lamports": resolved_fee_hints.network_fee_lamports,
        "base_fee_lamports": resolved_fee_hints.base_fee_lamports,
        "priority_fee_lamports": resolved_fee_hints.priority_fee_lamports,
        "ata_create_rent_lamports": resolved_fee_hints.ata_create_rent_lamports,
    });
    if let Some(policy_code) = tip_policy_code {
        response["tip_policy"] = json!({
            "policy_code": policy_code,
            "requested_tip_lamports": request.tip_lamports,
            "effective_tip_lamports": effective_tip_lamports,
        });
    }
    response["submit_signature_verify"] =
        submit_signature_verification_to_json(&submit_signature_verify);
    let inserted = state
        .idempotency
        .store_submit_response(
            request.client_order_id.as_str(),
            request.request_id.as_str(),
            &response,
        )
        .map_err(|error| {
            warn!(
                route = %route,
                signal_id = %request.signal_id,
                client_order_id = %request.client_order_id,
                error = %error,
                "failed to persist submit idempotency record"
            );
            map_idempotency_error_to_reject(error)
        })?;
    if !inserted {
        warn!(
            route = %route,
            signal_id = %request.signal_id,
            client_order_id = %request.client_order_id,
            "idempotency row already exists; keeping first stored response"
        );
        let canonical = state
            .idempotency
            .load_submit_response(request.client_order_id.as_str())
            .map_err(map_idempotency_error_to_reject)?
            .ok_or_else(|| {
                Reject::retryable(
                    "idempotency_store_unavailable",
                    "idempotency conflict detected but canonical response missing",
                )
            })?;
        return Ok(canonical);
    }
    Ok(response)
}

async fn forward_to_upstream(
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

fn parse_upstream_outcome(body: &Value, default_reject_code: &str) -> UpstreamOutcome {
    let status = body
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let ok_flag = body.get("ok").and_then(Value::as_bool);
    let accepted_flag = body.get("accepted").and_then(Value::as_bool);
    let retryable = body
        .get("retryable")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let is_reject = matches!(
        status.as_str(),
        "reject" | "rejected" | "error" | "failed" | "failure"
    ) || ok_flag == Some(false)
        || accepted_flag == Some(false);
    if is_reject {
        let code = body
            .get("code")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(default_reject_code)
            .to_string();
        let detail = body
            .get("detail")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("upstream rejected request")
            .to_string();
        return UpstreamOutcome::Reject(if retryable {
            Reject::retryable(code, detail)
        } else {
            Reject::terminal(code, detail)
        });
    }

    let is_known_success_status = matches!(status.as_str(), "ok" | "accepted" | "success");
    let is_known_status = is_known_success_status
        || matches!(
            status.as_str(),
            "reject" | "rejected" | "error" | "failed" | "failure"
        );

    if !status.is_empty() && !is_known_status {
        return UpstreamOutcome::Reject(Reject::terminal(
            "upstream_invalid_status",
            format!("unknown upstream status={}", status),
        ));
    }

    let success = accepted_flag.or(ok_flag).unwrap_or(is_known_success_status);
    if !success {
        return UpstreamOutcome::Reject(Reject::terminal(
            "upstream_invalid_response",
            "upstream did not explicitly confirm success",
        ));
    }

    UpstreamOutcome::Success
}

fn validate_common_contract(
    state: &AppState,
    request_contract_version: Option<&str>,
    route: &str,
    side: &str,
    token: &str,
    notional_sol: f64,
) -> std::result::Result<(), Reject> {
    let contract_version = request_contract_version
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Reject::terminal(
                "contract_version_missing",
                "contract_version must be provided",
            )
        })?;
    if contract_version != state.config.contract_version {
        return Err(Reject::terminal(
            "contract_version_mismatch",
            format!(
                "contract_version={} does not match expected={}",
                contract_version, state.config.contract_version
            ),
        ));
    }

    let route = normalize_route(route);
    if !state.config.route_allowlist.contains(route.as_str()) {
        return Err(Reject::terminal(
            "route_not_allowed",
            format!("route={} is not allowed", route),
        ));
    }
    if requires_submit_fastlane_enabled(route.as_str()) && !state.config.submit_fastlane_enabled {
        return Err(Reject::terminal(
            "fastlane_not_enabled",
            "route=fastlane requires COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED=true",
        ));
    }

    if Side::parse(side).is_none() {
        return Err(Reject::terminal("invalid_side", "side must be buy|sell"));
    }
    if token.trim().is_empty() {
        return Err(Reject::terminal("invalid_token", "token must be non-empty"));
    }
    validate_pubkey_like(token.trim())
        .map_err(|error| Reject::terminal("invalid_token", error.to_string()))?;

    if !notional_sol.is_finite() || notional_sol <= 0.0 {
        return Err(Reject::terminal(
            "invalid_notional_sol",
            "notional_sol must be finite and > 0",
        ));
    }
    if notional_sol > state.config.max_notional_sol {
        return Err(Reject::terminal(
            "notional_too_high",
            format!(
                "notional_sol={} exceeds executor max_notional_sol={}",
                notional_sol, state.config.max_notional_sol
            ),
        ));
    }

    Ok(())
}

fn parse_optional_non_negative_u64_field(
    body: &Value,
    field: &str,
) -> std::result::Result<Option<u64>, Reject> {
    let Some(value) = body.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    if let Some(parsed) = value.as_u64() {
        return Ok(Some(parsed));
    }
    Err(Reject::terminal(
        "submit_adapter_invalid_response",
        format!("{} must be non-negative integer when present", field),
    ))
}

fn map_fee_hint_error_to_reject(error: FeeHintError) -> Reject {
    match error {
        FeeHintError::DerivedPriorityFeeExceedsU64 { .. } => Reject::terminal(
            "fee_overflow",
            "derived priority fee exceeds u64 range",
        ),
        FeeHintError::OverflowBasePlusPriority => {
            Reject::terminal("fee_overflow", "base+priority fee overflow")
        }
        FeeHintError::NetworkFeeMismatch {
            network_fee_lamports,
            derived_network_fee_lamports,
        } => Reject::terminal(
            "submit_adapter_invalid_response",
            format!(
                "network_fee_lamports={} does not match base+priority={}",
                network_fee_lamports, derived_network_fee_lamports
            ),
        ),
        FeeHintError::FieldExceedsI64 { field, value } => Reject::terminal(
            "submit_adapter_invalid_response",
            format!("{}={} exceeds i64::MAX", field, value),
        ),
    }
}

fn map_idempotency_error_to_reject(error: anyhow::Error) -> Reject {
    Reject::retryable(
        "idempotency_store_unavailable",
        format!("idempotency store unavailable: {}", error),
    )
}

fn build_submit_forward_payload(
    raw_body: &[u8],
    requested_tip_lamports: u64,
    effective_tip_lamports: u64,
) -> std::result::Result<Vec<u8>, Reject> {
    if requested_tip_lamports == effective_tip_lamports {
        return Ok(raw_body.to_vec());
    }

    let mut payload: Value = serde_json::from_slice(raw_body).map_err(|error| {
        Reject::terminal(
            "invalid_request_body",
            format!("submit request body is not valid JSON object: {}", error),
        )
    })?;
    let object = payload.as_object_mut().ok_or_else(|| {
        Reject::terminal(
            "invalid_request_body",
            "submit request body must be JSON object",
        )
    })?;
    object.insert(
        "tip_lamports".to_string(),
        Value::Number(serde_json::Number::from(effective_tip_lamports)),
    );
    serde_json::to_vec(&payload).map_err(|error| {
        Reject::terminal(
            "invalid_request_body",
            format!("failed to encode submit request body: {}", error),
        )
    })
}

fn reject_to_json(reject: &Reject, client_order_id: Option<&str>, contract_version: &str) -> Value {
    let mut payload = json!({
        "status": "reject",
        "ok": false,
        "accepted": false,
        "retryable": reject.retryable,
        "code": reject.code,
        "detail": reject.detail,
        "contract_version": contract_version,
    });
    if let Some(client_order_id) = client_order_id {
        payload["client_order_id"] = Value::String(client_order_id.to_string());
    }
    payload
}

fn simulate_http_status_for_reject(_reject: &Reject) -> StatusCode {
    StatusCode::OK
}

fn parse_socket_addr(value: String) -> Result<SocketAddr> {
    value
        .trim()
        .parse::<SocketAddr>()
        .map_err(|error| anyhow!("invalid COPYBOT_EXECUTOR_BIND_ADDR: {}", error))
}

fn non_empty_env(name: &str) -> Result<String> {
    env::var(name)
        .map_err(|_| anyhow!("{} must be set", name))
        .map(|value| value.trim().to_string())
        .and_then(|value| {
            if value.is_empty() {
                Err(anyhow!("{} must be non-empty", name))
            } else {
                Ok(value)
            }
        })
}

fn optional_non_empty_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn resolve_secret_source(
    inline_name: &str,
    inline_value: Option<&str>,
    file_name: &str,
    file_value: Option<&str>,
) -> Result<Option<String>> {
    let inline_secret = inline_value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let file_path = file_value.map(str::trim).filter(|value| !value.is_empty());

    if inline_secret.is_some() && file_path.is_some() {
        return Err(anyhow!(
            "{} and {} cannot both be set",
            inline_name,
            file_name
        ));
    }

    if let Some(path) = file_path {
        let secret = read_trimmed_secret_file(path)
            .with_context(|| format!("{} invalid file source path={}", file_name, path))?;
        return Ok(Some(secret));
    }

    Ok(inline_secret)
}

fn read_trimmed_secret_file(path: &str) -> Result<String> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("secret file not found/readable path={}", path))?;
    match secret_file_has_restrictive_permissions(path) {
        Ok(false) => {
            warn!(
                path = %path,
                "secret file permissions are broader than recommended; expected owner-only access (e.g. 0600/0400)"
            );
        }
        Ok(true) => {}
        Err(error) => {
            warn!(
                path = %path,
                error = %error,
                "unable to inspect secret file permissions"
            );
        }
    }
    let secret = raw.trim().to_string();
    if secret.is_empty() {
        return Err(anyhow!("secret file is empty path={}", path));
    }
    Ok(secret)
}

#[cfg(unix)]
fn secret_file_has_restrictive_permissions(path: &str) -> Result<bool> {
    use std::os::unix::fs::PermissionsExt;
    let metadata =
        fs::metadata(path).with_context(|| format!("secret file stat failed path={}", path))?;
    Ok((metadata.permissions().mode() & 0o077) == 0)
}

#[cfg(not(unix))]
fn secret_file_has_restrictive_permissions(_path: &str) -> Result<bool> {
    Ok(true)
}

fn parse_u64_env(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .map_err(|error| anyhow!("{} must be u64: {}", name, error)),
        Err(_) => Ok(default),
    }
}

fn parse_f64_env(name: &str, default: f64) -> Result<f64> {
    match env::var(name) {
        Ok(raw) => raw
            .trim()
            .parse::<f64>()
            .map_err(|error| anyhow!("{} must be f64: {}", name, error)),
        Err(_) => Ok(default),
    }
}

fn parse_bool_env(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(raw) => matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

fn parse_route_allowlist(csv: String) -> Result<HashSet<String>> {
    let mut routes = HashSet::new();
    for raw in csv.split(',') {
        let route = normalize_route(raw);
        if route.is_empty() {
            continue;
        }
        if !KNOWN_ROUTES.iter().any(|known| *known == route) {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_ROUTE_ALLOWLIST contains unsupported route={} (supported: paper,rpc,jito,fastlane)",
                route
            ));
        }
        routes.insert(route);
    }
    Ok(routes)
}

fn validate_fastlane_route_policy(
    route_allowlist: &HashSet<String>,
    submit_fastlane_enabled: bool,
) -> Result<()> {
    if !submit_fastlane_enabled {
        for route in route_allowlist {
            if requires_submit_fastlane_enabled(route.as_str()) {
                return Err(anyhow!(
                    "COPYBOT_EXECUTOR_ROUTE_ALLOWLIST includes fastlane but COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED is false"
                ));
            }
        }
    }
    Ok(())
}

fn normalize_route(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn get_required_header<'a>(
    headers: &'a HeaderMap,
    key: &str,
    err_code: &str,
) -> std::result::Result<&'a str, Reject> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Reject::terminal(err_code, format!("missing header {}", key)))
}

fn parse_rfc3339_utc(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value.trim())
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn is_valid_contract_version_token(value: &str) -> bool {
    value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_'))
}

fn compute_hmac_signature_hex(key: &[u8], payload: &[u8]) -> Result<String> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).context("invalid HMAC key")?;
    mac.update(payload);
    Ok(to_hex_lower(mac.finalize().into_bytes().as_slice()))
}

fn to_hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut mismatch = 0u8;
    for (l, r) in left.iter().zip(right.iter()) {
        mismatch |= l ^ r;
    }
    mismatch == 0
}

fn validate_pubkey_like(value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("value is empty"));
    }
    let decoded = bs58::decode(trimmed)
        .into_vec()
        .map_err(|error| anyhow!("invalid base58: {}", error))?;
    if decoded.len() != 32 {
        return Err(anyhow!("decoded pubkey length must be 32 bytes"));
    }
    Ok(())
}

fn validate_signature_like(value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("value is empty"));
    }
    let decoded = bs58::decode(trimmed)
        .into_vec()
        .map_err(|error| anyhow!("invalid base58: {}", error))?;
    if decoded.len() != 64 {
        return Err(anyhow!("decoded signature length must be 64 bytes"));
    }
    Ok(())
}

fn resolve_signer_source_config(
    source_raw: Option<&str>,
    keypair_file_raw: Option<&str>,
    kms_key_id_raw: Option<&str>,
    signer_pubkey: &str,
) -> Result<SignerSource> {
    let source = source_raw
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("file")
        .to_ascii_lowercase();
    let keypair_file = keypair_file_raw
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let kms_key_id = kms_key_id_raw
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match source.as_str() {
        "file" => {
            if kms_key_id.is_some() {
                return Err(anyhow!(
                    "COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be empty when COPYBOT_EXECUTOR_SIGNER_SOURCE=file"
                ));
            }
            let keypair_file = keypair_file.ok_or_else(|| {
                anyhow!(
                    "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must be set when COPYBOT_EXECUTOR_SIGNER_SOURCE=file"
                )
            })?;
            validate_signer_keypair_file(keypair_file, signer_pubkey)?;
            Ok(SignerSource::File)
        }
        "kms" => {
            if keypair_file.is_some() {
                return Err(anyhow!(
                    "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must be empty when COPYBOT_EXECUTOR_SIGNER_SOURCE=kms"
                ));
            }
            let kms_key_id = kms_key_id.ok_or_else(|| {
                anyhow!(
                    "COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be set when COPYBOT_EXECUTOR_SIGNER_SOURCE=kms"
                )
            })?;
            if kms_key_id.len() > 256 {
                return Err(anyhow!(
                    "COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be <= 256 chars"
                ));
            }
            Ok(SignerSource::Kms)
        }
        _ => Err(anyhow!(
            "COPYBOT_EXECUTOR_SIGNER_SOURCE must be one of: file,kms"
        )),
    }
}

fn validate_signer_keypair_file(path: &str, signer_pubkey: &str) -> Result<()> {
    let raw = fs::read_to_string(path).with_context(|| {
        format!(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE not found/readable path={}",
            path
        )
    })?;
    if !secret_file_has_restrictive_permissions(path).with_context(|| {
        format!(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE permission check failed path={}",
            path
        )
    })? {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must use owner-only permissions (0600/0400) path={}",
            path
        ));
    }
    if raw.trim().is_empty() {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE is empty after trim path={}",
            path
        ));
    }
    let keypair_bytes: Vec<u8> = serde_json::from_str(raw.trim()).with_context(|| {
        format!(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must be JSON array with 64 u8 values path={}",
            path
        )
    })?;
    if keypair_bytes.len() != 64 {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must contain 64-byte keypair, got {} path={}",
            keypair_bytes.len(),
            path
        ));
    }
    let derived_pubkey = bs58::encode(&keypair_bytes[32..64]).into_string();
    if derived_pubkey != signer_pubkey.trim() {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE pubkey mismatch: file_pubkey={} expected_pubkey={}",
            derived_pubkey,
            signer_pubkey.trim()
        ));
    }
    Ok(())
}

fn require_authenticated_mode(
    bearer_token: Option<&str>,
    allow_unauthenticated: bool,
) -> Result<()> {
    if allow_unauthenticated {
        return Ok(());
    }
    if bearer_token.is_none() {
        return Err(anyhow!(
            "executor auth is required: set COPYBOT_EXECUTOR_BEARER_TOKEN (or *_FILE); optionally add HMAC pair for dual auth. For controlled local setups only, set COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED=true"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use std::{
        fs as stdfs,
        io::{Read, Write},
        net::TcpListener,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };

    static TEMP_SECRET_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn contract_version_token_validation() {
        assert!(is_valid_contract_version_token("v1"));
        assert!(is_valid_contract_version_token("v1.2.3-prod"));
        assert!(!is_valid_contract_version_token("v1 beta"));
        assert!(!is_valid_contract_version_token("v1/beta"));
    }

    #[test]
    fn constant_time_eq_checks_content() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }

    #[test]
    fn parse_route_allowlist_normalizes() {
        let routes = parse_route_allowlist("RPC, jito ,fastlane".to_string()).unwrap();
        assert!(routes.contains("rpc"));
        assert!(routes.contains("jito"));
        assert!(routes.contains("fastlane"));
        assert_eq!(routes.len(), 3);
    }

    #[test]
    fn parse_route_allowlist_rejects_unknown_route() {
        let error = parse_route_allowlist("rpc,unknown_route".to_string())
            .expect_err("unknown route must fail closed");
        assert!(
            error
                .to_string()
                .contains("unsupported route=unknown_route"),
            "error={}",
            error
        );
    }

    #[test]
    fn validate_fastlane_route_policy_enforces_feature_gate() {
        let routes = parse_route_allowlist("rpc,fastlane".to_string()).unwrap();
        assert!(validate_fastlane_route_policy(&routes, false).is_err());
        assert!(validate_fastlane_route_policy(&routes, true).is_ok());
    }

    #[test]
    fn min_claim_ttl_sec_for_submit_path_accounts_for_verify_and_fallback_hops() {
        let mut route_backends = HashMap::new();
        route_backends.insert(
            "rpc".to_string(),
            RouteBackend {
                submit_url: "https://submit.primary".to_string(),
                submit_fallback_url: Some("https://submit.fallback".to_string()),
                simulate_url: "https://simulate.primary".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: Some("https://send-rpc.primary".to_string()),
                send_rpc_fallback_url: Some("https://send-rpc.fallback".to_string()),
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let verify = SubmitSignatureVerifyConfig {
            endpoints: vec![
                "https://verify.primary".to_string(),
                "https://verify.fallback".to_string(),
            ],
            attempts: 3,
            interval_ms: 250,
            strict: false,
        };
        let ttl = min_claim_ttl_sec_for_submit_path(2_000, &route_backends, Some(&verify));
        assert_eq!(ttl, 22);
    }

    #[test]
    fn min_claim_ttl_sec_for_submit_path_applies_500ms_runtime_floor() {
        let mut route_backends = HashMap::new();
        route_backends.insert(
            "rpc".to_string(),
            RouteBackend {
                submit_url: "https://submit.primary".to_string(),
                submit_fallback_url: None,
                simulate_url: "https://simulate.primary".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let ttl = min_claim_ttl_sec_for_submit_path(100, &route_backends, None);
        assert_eq!(ttl, 2);
    }

    #[test]
    fn validate_common_contract_rejects_fastlane_when_feature_disabled() {
        let mut state = test_state("http://127.0.0.1:1/upstream");
        state.config.route_allowlist.insert("fastlane".to_string());
        let reject = validate_common_contract(
            &state,
            Some("v1"),
            "fastlane",
            "buy",
            "11111111111111111111111111111111",
            1.0,
        )
        .expect_err("fastlane must be rejected when feature flag is disabled");
        assert_eq!(reject.code, "fastlane_not_enabled");
    }

    #[test]
    fn apply_submit_tip_policy_forces_rpc_tip_to_zero() {
        let (effective, policy_code) = apply_submit_tip_policy("rpc", 12_345);
        assert_eq!(effective, 0);
        assert_eq!(policy_code, Some("rpc_tip_forced_zero"));

        let (effective, policy_code) = apply_submit_tip_policy("jito", 12_345);
        assert_eq!(effective, 12_345);
        assert_eq!(policy_code, None);
    }

    #[test]
    fn to_hex_lower_matches_expected() {
        assert_eq!(to_hex_lower(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    #[test]
    fn parse_upstream_outcome_rejects_unknown_status() {
        let payload = json!({
            "status": "pending",
            "ok": true
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => assert_eq!(reject.code, "upstream_invalid_status"),
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn parse_upstream_outcome_rejects_explicit_reject() {
        let payload = json!({
            "status": "reject",
            "retryable": true,
            "code": "busy",
            "detail": "backpressure"
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(reject.retryable);
                assert_eq!(reject.code, "busy");
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn validate_pubkey_like_requires_32_bytes() {
        let ok = "11111111111111111111111111111111";
        assert!(validate_pubkey_like(ok).is_ok());
        assert!(validate_pubkey_like("not-base58").is_err());
    }

    #[test]
    fn validate_signature_like_requires_64_bytes() {
        let valid = bs58::encode([7u8; 64]).into_string();
        assert!(validate_signature_like(valid.as_str()).is_ok());
        assert!(validate_signature_like("not-base58").is_err());
        let short = bs58::encode([7u8; 32]).into_string();
        assert!(validate_signature_like(short.as_str()).is_err());
    }

    #[test]
    fn resolve_signer_source_config_requires_file_source_materials() {
        let error = resolve_signer_source_config(
            Some("file"),
            None,
            None,
            "11111111111111111111111111111111",
        )
        .expect_err("file source requires keypair file");
        assert!(error
            .to_string()
            .contains("COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must be set"));
    }

    #[test]
    fn resolve_signer_source_config_rejects_kms_key_for_file_source() {
        let path = write_temp_signer_keypair_file([0u8; 32]);
        let error = resolve_signer_source_config(
            Some("file"),
            Some(path.to_str().expect("utf8 path")),
            Some("kms-key-id"),
            "11111111111111111111111111111111",
        )
        .expect_err("kms key id must be empty for file source");
        assert!(error
            .to_string()
            .contains("COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be empty"));
        cleanup_temp_secret_file(path);
    }

    #[test]
    fn resolve_signer_source_config_accepts_kms_source() {
        let source = resolve_signer_source_config(
            Some("kms"),
            None,
            Some("projects/p/keys/k"),
            "11111111111111111111111111111111",
        )
        .expect("kms source should validate");
        assert_eq!(source, SignerSource::Kms);
    }

    #[test]
    fn resolve_signer_source_config_rejects_kms_without_key_id() {
        let error = resolve_signer_source_config(
            Some("kms"),
            None,
            None,
            "11111111111111111111111111111111",
        )
        .expect_err("kms source requires key id");
        assert!(error
            .to_string()
            .contains("COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be set"));
    }

    #[test]
    fn resolve_signer_source_config_rejects_unknown_source() {
        let error = resolve_signer_source_config(
            Some("vault"),
            None,
            None,
            "11111111111111111111111111111111",
        )
        .expect_err("unknown source must fail");
        assert!(error
            .to_string()
            .contains("COPYBOT_EXECUTOR_SIGNER_SOURCE must be one of: file,kms"));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_signer_source_config_rejects_non_restrictive_keypair_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let path = write_temp_signer_keypair_file([0u8; 32]);
        let mut perms = stdfs::metadata(&path)
            .expect("stat temp secret")
            .permissions();
        perms.set_mode(0o644);
        stdfs::set_permissions(&path, perms).expect("set relaxed mode");
        let error = resolve_signer_source_config(
            Some("file"),
            Some(path.to_str().expect("utf8 path")),
            None,
            "11111111111111111111111111111111",
        )
        .expect_err("broad keypair file permissions must fail");
        assert!(error
            .to_string()
            .contains("must use owner-only permissions"));
        cleanup_temp_secret_file(path);
    }

    #[test]
    fn resolve_signer_source_config_rejects_keypair_pubkey_mismatch() {
        let path = write_temp_signer_keypair_file([1u8; 32]);
        let error = resolve_signer_source_config(
            Some("file"),
            Some(path.to_str().expect("utf8 path")),
            None,
            "11111111111111111111111111111111",
        )
        .expect_err("keypair pubkey mismatch must fail");
        assert!(error.to_string().contains("pubkey mismatch"));
        cleanup_temp_secret_file(path);
    }

    #[test]
    fn build_submit_signature_verify_config_rejects_fallback_without_primary() {
        let error = build_submit_signature_verify_config(
            None,
            Some("https://rpc-fallback.example.com".to_string()),
            3,
            250,
            false,
        )
        .expect_err("fallback without primary must fail");
        assert!(error
            .to_string()
            .contains("requires COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL"));
    }

    #[test]
    fn build_submit_signature_verify_config_rejects_same_endpoint_identity() {
        let error = build_submit_signature_verify_config(
            Some("https://RPC.example.com".to_string()),
            Some("https://rpc.example.com:443/".to_string()),
            3,
            250,
            false,
        )
        .expect_err("same primary/fallback identity must fail");
        assert!(error
            .to_string()
            .contains("must resolve to distinct endpoint"));
    }

    #[test]
    fn build_submit_signature_verify_config_rejects_attempts_out_of_range() {
        let error = build_submit_signature_verify_config(
            Some("https://rpc.example.com".to_string()),
            None,
            0,
            250,
            false,
        )
        .expect_err("attempts=0 must fail");
        assert!(error.to_string().contains("ATTEMPTS must be in 1..=20"));
    }

    #[test]
    fn build_submit_signature_verify_config_rejects_interval_out_of_range() {
        let error = build_submit_signature_verify_config(
            Some("https://rpc.example.com".to_string()),
            None,
            3,
            0,
            false,
        )
        .expect_err("interval=0 must fail");
        assert!(error
            .to_string()
            .contains("INTERVAL_MS must be in 1..=60000"));
    }

    #[test]
    fn resolve_secret_source_rejects_inline_and_file_conflict() {
        let error = resolve_secret_source(
            "COPYBOT_EXECUTOR_BEARER_TOKEN",
            Some("inline"),
            "COPYBOT_EXECUTOR_BEARER_TOKEN_FILE",
            Some("/tmp/adapter-bearer.secret"),
        )
        .expect_err("inline + file must fail-closed");
        let message = error.to_string();
        assert!(
            message.contains("COPYBOT_EXECUTOR_BEARER_TOKEN")
                && message.contains("COPYBOT_EXECUTOR_BEARER_TOKEN_FILE")
        );
    }

    #[test]
    fn resolve_secret_source_reads_trimmed_file() {
        let path = write_temp_secret_file(" \nsecret-value\n");
        let resolved = resolve_secret_source(
            "COPYBOT_EXECUTOR_BEARER_TOKEN",
            None,
            "COPYBOT_EXECUTOR_BEARER_TOKEN_FILE",
            Some(path.to_str().expect("utf8 path")),
        )
        .expect("file-backed secret must resolve");
        assert_eq!(resolved.as_deref(), Some("secret-value"));
        cleanup_temp_secret_file(path);
    }

    #[test]
    fn resolve_secret_source_rejects_empty_file() {
        let path = write_temp_secret_file(" \n\t ");
        let error = resolve_secret_source(
            "COPYBOT_EXECUTOR_HMAC_SECRET",
            None,
            "COPYBOT_EXECUTOR_HMAC_SECRET_FILE",
            Some(path.to_str().expect("utf8 path")),
        )
        .expect_err("empty secret file must fail");
        let message = format!("{:#}", error);
        assert!(message.contains("COPYBOT_EXECUTOR_HMAC_SECRET_FILE"));
        assert!(message.contains("secret file is empty"));
        cleanup_temp_secret_file(path);
    }

    #[test]
    fn resolve_secret_source_rejects_missing_file() {
        let path = temp_secret_path("missing");
        let error = resolve_secret_source(
            "COPYBOT_EXECUTOR_HMAC_SECRET",
            None,
            "COPYBOT_EXECUTOR_HMAC_SECRET_FILE",
            Some(path.to_str().expect("utf8 path")),
        )
        .expect_err("missing secret file must fail");
        let message = format!("{:#}", error);
        assert!(message.contains("COPYBOT_EXECUTOR_HMAC_SECRET_FILE"));
        assert!(message.contains("secret file not found/readable"));
    }

    #[cfg(unix)]
    #[test]
    fn secret_file_permissions_check_detects_relaxed_mode() {
        use std::os::unix::fs::PermissionsExt;

        let path = write_temp_secret_file("secret");
        let mut perms = stdfs::metadata(&path)
            .expect("stat temp secret")
            .permissions();
        perms.set_mode(0o644);
        stdfs::set_permissions(&path, perms).expect("set relaxed mode");
        assert!(
            !secret_file_has_restrictive_permissions(path.to_str().expect("utf8 path"))
                .expect("permission check"),
            "0644 should be flagged as broad permissions"
        );

        let mut perms = stdfs::metadata(&path)
            .expect("stat temp secret")
            .permissions();
        perms.set_mode(0o600);
        stdfs::set_permissions(&path, perms).expect("set strict mode");
        assert!(
            secret_file_has_restrictive_permissions(path.to_str().expect("utf8 path"))
                .expect("permission check"),
            "0600 should pass restrictive permission check"
        );

        cleanup_temp_secret_file(path);
    }

    #[test]
    fn require_authenticated_mode_fails_closed_by_default() {
        assert!(require_authenticated_mode(None, false).is_err());
        assert!(require_authenticated_mode(Some("token"), false).is_ok());
        assert!(require_authenticated_mode(None, true).is_ok());
    }

    #[tokio::test]
    async fn auth_verifier_rejects_wrong_bearer_token() {
        let verifier = AuthVerifier {
            bearer_token: Some("correct-token".to_string()),
            hmac: None,
            nonce_seen_until_epoch: Arc::new(Mutex::new(HashMap::new())),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer wrong-token"),
        );
        let reject = verifier
            .verify(&headers, b"{}")
            .await
            .expect_err("wrong bearer token must fail");
        assert_eq!(reject.code, "auth_invalid");
    }

    #[tokio::test]
    async fn auth_verifier_accepts_correct_bearer_token() {
        let verifier = AuthVerifier {
            bearer_token: Some("correct-token".to_string()),
            hmac: None,
            nonce_seen_until_epoch: Arc::new(Mutex::new(HashMap::new())),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer correct-token"),
        );
        verifier
            .verify(&headers, b"{\"ping\":true}")
            .await
            .expect("correct bearer token should pass");
    }

    #[tokio::test]
    async fn handle_simulate_rejects_empty_request_id() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "   ".to_string(),
            signal_id: "signal-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let reject = handle_simulate(&state, &request, b"{}")
            .await
            .expect_err("empty request_id must fail");
        assert_eq!(reject.code, "invalid_request_id");
    }

    #[tokio::test]
    async fn handle_simulate_rejects_empty_signal_id() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-1".to_string(),
            signal_id: " ".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let reject = handle_simulate(&state, &request, b"{}")
            .await
            .expect_err("empty signal_id must fail");
        assert_eq!(reject.code, "invalid_signal_id");
    }

    #[tokio::test]
    async fn handle_submit_rejects_empty_signal_id() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            signal_id: " ".to_string(),
            client_order_id: "client-1".to_string(),
            request_id: "request-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 50.0,
            route_slippage_cap_bps: 50.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let reject = handle_submit(&state, &request, b"{}")
            .await
            .expect_err("empty signal_id must fail");
        assert_eq!(reject.code, "invalid_signal_id");
    }

    #[test]
    fn simulate_reject_status_is_http_200_for_retryable_and_terminal() {
        let reject = Reject::retryable("busy", "upstream temporary issue");
        assert_eq!(simulate_http_status_for_reject(&reject), StatusCode::OK);
        let reject = Reject::terminal("invalid", "bad request");
        assert_eq!(simulate_http_status_for_reject(&reject), StatusCode::OK);
    }

    #[tokio::test]
    async fn forward_to_upstream_treats_plaintext_503_as_retryable() {
        let Some((url, handle)) =
            spawn_one_shot_upstream_raw(503, "text/plain", "temporary upstream outage")
        else {
            return;
        };
        let state = test_state(url.as_str());
        let reject = forward_to_upstream(&state, "rpc", UpstreamAction::Simulate, b"{}", None)
            .await
            .expect_err("503 upstream should be retryable");
        assert!(reject.retryable);
        assert_eq!(reject.code, "upstream_http_unavailable");
        assert!(
            reject.detail.contains("temporary upstream outage"),
            "detail={}",
            reject.detail
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_uses_fallback_after_primary_send_error() {
        let Some((fallback_url, handle)) = spawn_one_shot_upstream_raw(
            200,
            "application/json",
            "{\"status\":\"ok\",\"accepted\":true}",
        ) else {
            return;
        };
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            Some(fallback_url.as_str()),
            "http://127.0.0.1:1/upstream",
            Some(fallback_url.as_str()),
        );
        let body = forward_to_upstream(&state, "rpc", UpstreamAction::Simulate, b"{}", None)
            .await
            .expect("fallback should succeed after primary send error");
        assert_eq!(body.get("status").and_then(Value::as_str), Some("ok"));
        let _ = handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_uses_fallback_after_primary_retryable_status() {
        let Some((primary_url, primary_handle)) =
            spawn_one_shot_upstream_raw(503, "text/plain", "temporary outage")
        else {
            return;
        };
        let Some((fallback_url, fallback_handle)) = spawn_one_shot_upstream_raw(
            200,
            "application/json",
            "{\"status\":\"ok\",\"accepted\":true}",
        ) else {
            return;
        };

        let state = test_state_with_backends(
            primary_url.as_str(),
            Some(fallback_url.as_str()),
            primary_url.as_str(),
            Some(fallback_url.as_str()),
        );
        let body = forward_to_upstream(&state, "rpc", UpstreamAction::Submit, b"{}", None)
            .await
            .expect("fallback should succeed after retryable status");
        assert_eq!(body.get("status").and_then(Value::as_str), Some("ok"));
        let _ = primary_handle.join();
        let _ = fallback_handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_does_not_fallback_after_primary_terminal_status() {
        let Some((primary_url, primary_handle)) =
            spawn_one_shot_upstream_raw(401, "text/plain", "unauthorized")
        else {
            return;
        };

        let state = test_state_with_backends(
            primary_url.as_str(),
            Some("http://127.0.0.1:1/upstream"),
            primary_url.as_str(),
            Some("http://127.0.0.1:1/upstream"),
        );
        let reject = forward_to_upstream(&state, "rpc", UpstreamAction::Submit, b"{}", None)
            .await
            .expect_err("terminal status should short-circuit");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_http_rejected");
        let _ = primary_handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_uses_fallback_auth_token_when_retrying() {
        let fallback_token = "FallBack-Token-123";
        let Some((primary_url, primary_handle)) =
            spawn_one_shot_upstream_raw(503, "text/plain", "temporary outage")
        else {
            return;
        };
        let Some((fallback_url, fallback_handle)) =
            spawn_one_shot_upstream_expect_bearer(fallback_token)
        else {
            return;
        };

        let mut state = test_state_with_backends(
            primary_url.as_str(),
            Some(fallback_url.as_str()),
            primary_url.as_str(),
            Some(fallback_url.as_str()),
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.primary_auth_token = Some("primary-token".to_string());
            backend.fallback_auth_token = Some(fallback_token.to_string());
        } else {
            panic!("rpc backend must exist");
        }

        let body = forward_to_upstream(&state, "rpc", UpstreamAction::Submit, b"{}", None)
            .await
            .expect("fallback with dedicated token should succeed");
        assert_eq!(body.get("status").and_then(Value::as_str), Some("ok"));
        let _ = primary_handle.join();
        let _ = fallback_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_returns_signature_result() {
        let (signed_tx_base64, rpc_signature) = test_signed_tx_base64_with_signature([13u8; 64]);
        let rpc_body = format!(r#"{{"jsonrpc":"2.0","result":"{}"}}"#, rpc_signature);
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", rpc_body.as_str())
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url.clone());
        } else {
            panic!("rpc backend must exist");
        }

        let signature =
            send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str(), None)
            .await
            .expect("send RPC should return tx signature");
        assert_eq!(signature, rpc_signature);
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_signature_mismatch() {
        let (signed_tx_base64, expected_signature) =
            test_signed_tx_base64_with_signature([31u8; 64]);
        let rpc_signature = bs58::encode([32u8; 64]).into_string();
        assert_ne!(expected_signature, rpc_signature);
        let rpc_body = format!(r#"{{"jsonrpc":"2.0","result":"{}"}}"#, rpc_signature);
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", rpc_body.as_str())
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let reject =
            send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str(), None)
            .await
            .expect_err("mismatched send RPC signature must fail");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "send_rpc_signature_mismatch");
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_uses_fallback_auth_token_when_retrying() {
        let (signed_tx_base64, expected_signature) =
            test_signed_tx_base64_with_signature([33u8; 64]);
        let fallback_token = "Send-Rpc-Fallback-Token";
        let Some((primary_url, primary_handle)) =
            spawn_one_shot_upstream_raw(503, "text/plain", "send rpc primary unavailable")
        else {
            return;
        };
        let Some((fallback_url, fallback_handle)) =
            spawn_one_shot_send_rpc_expect_bearer(fallback_token, expected_signature.as_str())
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(primary_url);
            backend.send_rpc_fallback_url = Some(fallback_url);
            backend.send_rpc_primary_auth_token = Some("send-rpc-primary-token".to_string());
            backend.send_rpc_fallback_auth_token = Some(fallback_token.to_string());
        } else {
            panic!("rpc backend must exist");
        }

        let signature =
            send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str(), None)
            .await
            .expect("fallback send RPC with dedicated auth token should succeed");
        assert_eq!(signature, expected_signature);
        let _ = primary_handle.join();
        let _ = fallback_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_fallback_without_primary_url() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([36u8; 64]);

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = None;
            backend.send_rpc_fallback_url =
                Some("http://127.0.0.1:1/send-rpc-fallback".to_string());
        } else {
            panic!("rpc backend must exist");
        }

        let reject =
            send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str(), None)
            .await
            .expect_err("fallback-only send RPC topology must fail closed");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "adapter_send_rpc_not_configured");
        assert!(
            reject
                .detail
                .contains("fallback URL but missing primary send RPC URL"),
            "detail={}",
            reject.detail
        );
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_accepts_already_processed_error() {
        let (signed_tx_base64, expected_signature) =
            test_signed_tx_base64_with_signature([34u8; 64]);
        let rpc_body = r#"{"jsonrpc":"2.0","error":{"code":-32002,"message":"Transaction already processed"}}"#;
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", rpc_body)
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let signature =
            send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str(), None)
            .await
            .expect("already processed error should resolve to expected signature");
        assert_eq!(signature, expected_signature);
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_treats_unknown_error_payload_as_terminal() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([35u8; 64]);
        let Some((primary_url, primary_handle)) = spawn_one_shot_upstream_raw(
            200,
            "application/json",
            r#"{"jsonrpc":"2.0","error":{"code":-32002,"message":"Blockhash not found"}}"#,
        ) else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(primary_url);
            backend.send_rpc_fallback_url =
                Some("http://127.0.0.1:1/send-rpc-fallback".to_string());
        } else {
            panic!("rpc backend must exist");
        }

        let reject =
            send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str(), None)
            .await
            .expect_err("unknown send RPC error payload should be terminal");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "send_rpc_error_payload_terminal");
        let _ = primary_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_uses_send_rpc_when_upstream_returns_signed_tx_base64() {
        let (signed_tx_base64, rpc_signature) = test_signed_tx_base64_with_signature([14u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":"{}"}}"#,
            signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };
        let rpc_body = format!(r#"{{"jsonrpc":"2.0","result":"{}"}}"#, rpc_signature);
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", rpc_body.as_str())
        else {
            return;
        };

        let mut state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-1",
            "client_order_id": "client-order-1",
            "request_id": "request-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let response = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("submit should succeed via send RPC");
        assert_eq!(
            response.get("tx_signature").and_then(Value::as_str),
            Some(rpc_signature.as_str())
        );
        assert_eq!(
            response.get("submit_transport").and_then(Value::as_str),
            Some("adapter_send_rpc")
        );
        let _ = upstream_handle.join();
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_returns_cached_response_for_duplicate_client_order_id() {
        let signature = bs58::encode([17u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}"}}"#,
            signature
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-idempotent-1",
            "client_order_id": "client-order-idempotent-1",
            "request_id": "request-idempotent-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.3,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let first = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("first submit should succeed");
        let first_submitted_at = first
            .get("submitted_at")
            .and_then(Value::as_str)
            .expect("submitted_at");

        let second = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("second submit should return cached response");
        assert_eq!(
            second.get("tx_signature").and_then(Value::as_str),
            Some(signature.as_str())
        );
        assert_eq!(
            second.get("submitted_at").and_then(Value::as_str),
            Some(first_submitted_at)
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_parallel_duplicate_client_order_id_in_flight() {
        let signature = bs58::encode([19u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}"}}"#,
            signature
        );
        let Some((upstream_url, upstream_handle)) = spawn_one_shot_upstream_raw_with_delay(
            200,
            "application/json",
            upstream_body.as_str(),
            200,
        ) else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-idempotent-inflight-1",
            "client_order_id": "client-order-idempotent-inflight-1",
            "request_id": "request-idempotent-inflight-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let first_submit = async {
            handle_submit(&state, &request, raw_body_bytes.as_slice()).await
        };
        let second_submit = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            handle_submit(&state, &request, raw_body_bytes.as_slice()).await
        };

        let (first_result, second_result) = tokio::join!(first_submit, second_submit);
        let first_response = first_result.expect("first submit should succeed");
        assert_eq!(
            first_response.get("tx_signature").and_then(Value::as_str),
            Some(signature.as_str())
        );
        let second_reject =
            second_result.expect_err("second submit should reject while first in flight");
        assert!(second_reject.retryable);
        assert_eq!(second_reject.code, "submit_in_flight");
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_returns_canonical_cached_response_when_store_conflicts() {
        let upstream_signature = bs58::encode([21u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}"}}"#,
            upstream_signature
        );
        let Some((upstream_url, upstream_handle)) = spawn_one_shot_upstream_raw_with_delay(
            200,
            "application/json",
            upstream_body.as_str(),
            250,
        ) else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let state_inject = state.clone();
        let client_order_id = "client-order-idempotent-overlap-1".to_string();
        let canonical = json!({
            "status": "ok",
            "ok": true,
            "accepted": true,
            "route": "rpc",
            "contract_version": "v1",
            "client_order_id": client_order_id,
            "request_id": "request-canonical-1",
            "tx_signature": bs58::encode([22u8; 64]).into_string(),
            "submit_transport": "upstream_signature",
            "submitted_at": "2026-02-24T00:00:00Z",
            "network_fee_lamports": 5300,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 300,
            "ata_create_rent_lamports": 0
        });

        let inject_handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(60));
            state_inject
                .idempotency
                .store_submit_response(
                    "client-order-idempotent-overlap-1",
                    "request-canonical-1",
                    &canonical,
                )
                .expect("inject canonical response");
        });

        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-idempotent-overlap-1",
            "client_order_id": "client-order-idempotent-overlap-1",
            "request_id": "request-idempotent-overlap-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let response = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("submit should return canonical cached response");
        assert_eq!(
            response.get("request_id").and_then(Value::as_str),
            Some("request-canonical-1")
        );
        let _ = inject_handle.join();
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_forces_rpc_tip_to_zero_and_emits_trace() {
        let signature = bs58::encode([15u8; 64]).into_string();
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_expect_tip_lamports(0, signature.as_str())
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-1",
            "client_order_id": "client-order-rpc-tip",
            "request_id": "request-rpc-tip",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 15.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 2500,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let response = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("submit should coerce rpc tip to zero and succeed");
        assert_eq!(
            response.get("tx_signature").and_then(Value::as_str),
            Some(signature.as_str())
        );
        assert_eq!(response.get("tip_lamports").and_then(Value::as_u64), Some(0));
        assert_eq!(
            response
                .get("tip_policy")
                .and_then(|value| value.get("policy_code"))
                .and_then(Value::as_str),
            Some("rpc_tip_forced_zero")
        );
        assert_eq!(
            response
                .get("tip_policy")
                .and_then(|value| value.get("requested_tip_lamports"))
                .and_then(Value::as_u64),
            Some(2500)
        );
        assert_eq!(
            response
                .get("tip_policy")
                .and_then(|value| value.get("effective_tip_lamports"))
                .and_then(Value::as_u64),
            Some(0)
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_allows_rpc_tip_when_nonzero_tip_disabled() {
        let signature = bs58::encode([16u8; 64]).into_string();
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_expect_tip_lamports(0, signature.as_str())
        else {
            return;
        };

        let mut state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        state.config.allow_nonzero_tip = false;

        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-2",
            "client_order_id": "client-order-rpc-tip-disabled",
            "request_id": "request-rpc-tip-disabled",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 15.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 7000,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let response = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("rpc route should coerce tip and bypass nonzero-tip reject");
        assert_eq!(
            response.get("tx_signature").and_then(Value::as_str),
            Some(signature.as_str())
        );
        assert_eq!(response.get("tip_lamports").and_then(Value::as_u64), Some(0));
        assert_eq!(
            response
                .get("tip_policy")
                .and_then(|value| value.get("policy_code"))
                .and_then(Value::as_str),
            Some("rpc_tip_forced_zero")
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_nonzero_tip_for_jito_when_disabled() {
        let state = {
            let mut state = test_state("http://127.0.0.1:1/upstream");
            state.config.allow_nonzero_tip = false;
            state.config.route_allowlist.insert("jito".to_string());
            state.config.route_backends.insert(
                "jito".to_string(),
                RouteBackend {
                    submit_url: "http://127.0.0.1:1/upstream".to_string(),
                    submit_fallback_url: None,
                    simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                    simulate_fallback_url: None,
                    primary_auth_token: None,
                    fallback_auth_token: None,
                    send_rpc_url: None,
                    send_rpc_fallback_url: None,
                    send_rpc_primary_auth_token: None,
                    send_rpc_fallback_auth_token: None,
                },
            );
            state
        };

        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-jito-1",
            "client_order_id": "client-order-jito-tip",
            "request_id": "request-jito-tip",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "jito",
            "slippage_bps": 15.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 1000,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("non-rpc route must still reject non-zero tip when disabled");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "tip_not_supported");
    }

    #[tokio::test]
    async fn verify_submit_signature_seen_when_rpc_reports_confirmation() {
        let signature = bs58::encode([9u8; 64]).into_string();
        let body = r#"{"jsonrpc":"2.0","result":{"value":[{"err":null,"confirmationStatus":"confirmed"}]}}"#;
        let Some((verify_url, handle)) = spawn_one_shot_upstream_raw(200, "application/json", body)
        else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            true,
        );
        let result =
            verify_submitted_signature_visibility(&state, "rpc", signature.as_str(), None).await;
        let Ok(SubmitSignatureVerification::Seen {
            confirmation_status,
        }) = result
        else {
            panic!("expected seen verification result");
        };
        assert_eq!(confirmation_status, "confirmed");
        let _ = handle.join();
    }

    #[tokio::test]
    async fn verify_submit_signature_returns_unseen_when_not_strict() {
        let signature = bs58::encode([10u8; 64]).into_string();
        let body = r#"{"jsonrpc":"2.0","result":{"value":[null]}}"#;
        let Some((verify_url, handle)) = spawn_one_shot_upstream_raw(200, "application/json", body)
        else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            false,
        );
        let result =
            verify_submitted_signature_visibility(&state, "rpc", signature.as_str(), None).await;
        let Ok(SubmitSignatureVerification::Unseen { reason }) = result else {
            panic!("expected unseen verification result");
        };
        assert!(
            reason.contains("pending") || reason.contains("missing"),
            "reason={}",
            reason
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn verify_submit_signature_rejects_when_pending_and_strict() {
        let signature = bs58::encode([11u8; 64]).into_string();
        let body = r#"{"jsonrpc":"2.0","result":{"value":[null]}}"#;
        let Some((verify_url, handle)) = spawn_one_shot_upstream_raw(200, "application/json", body)
        else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            true,
        );
        let reject = verify_submitted_signature_visibility(&state, "rpc", signature.as_str(), None)
            .await
            .expect_err("strict mode must reject unseen signature");
        assert!(reject.retryable);
        assert_eq!(reject.code, "upstream_submit_signature_unseen");
        let _ = handle.join();
    }

    #[tokio::test]
    async fn verify_submit_signature_rejects_when_onchain_error_seen() {
        let signature = bs58::encode([12u8; 64]).into_string();
        let body =
            r#"{"jsonrpc":"2.0","result":{"value":[{"err":{"InstructionError":[0,"Custom"]}}]}}"#;
        let Some((verify_url, handle)) = spawn_one_shot_upstream_raw(200, "application/json", body)
        else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            false,
        );
        let reject = verify_submitted_signature_visibility(&state, "rpc", signature.as_str(), None)
            .await
            .expect_err("on-chain err must be terminal reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_submit_failed_onchain");
        let _ = handle.join();
    }

    fn test_state(endpoint: &str) -> AppState {
        test_state_with_backends(endpoint, None, endpoint, None)
    }

    fn test_state_with_backends_and_verify(
        submit_primary: &str,
        submit_fallback: Option<&str>,
        simulate_primary: &str,
        simulate_fallback: Option<&str>,
        verify_endpoints: Vec<&str>,
        verify_strict: bool,
    ) -> AppState {
        let mut state = test_state_with_backends(
            submit_primary,
            submit_fallback,
            simulate_primary,
            simulate_fallback,
        );
        if !verify_endpoints.is_empty() {
            state.config.submit_signature_verify = Some(SubmitSignatureVerifyConfig {
                endpoints: verify_endpoints
                    .into_iter()
                    .map(|value| value.to_string())
                    .collect(),
                attempts: 1,
                interval_ms: 1,
                strict: verify_strict,
            });
        }
        state
    }

    fn test_state_with_backends(
        submit_primary: &str,
        submit_fallback: Option<&str>,
        simulate_primary: &str,
        simulate_fallback: Option<&str>,
    ) -> AppState {
        let mut route_allowlist = HashSet::new();
        route_allowlist.insert("rpc".to_string());
        let mut route_backends = HashMap::new();
        route_backends.insert(
            "rpc".to_string(),
            RouteBackend {
                submit_url: submit_primary.to_string(),
                submit_fallback_url: submit_fallback.map(|value| value.to_string()),
                simulate_url: simulate_primary.to_string(),
                simulate_fallback_url: simulate_fallback.map(|value| value.to_string()),
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let config = ExecutorConfig {
            bind_addr: "127.0.0.1:8080".parse().expect("valid bind"),
            contract_version: "v1".to_string(),
            signer_pubkey: "11111111111111111111111111111111".to_string(),
            signer_source: SignerSource::File,
            signer_keypair_file: Some("/tmp/copybot-executor-test-keypair.json".to_string()),
            signer_kms_key_id: None,
            submit_fastlane_enabled: false,
            route_allowlist,
            route_backends,
            bearer_token: None,
            hmac_key_id: None,
            hmac_secret: None,
            hmac_ttl_sec: 30,
            request_timeout_ms: 2_000,
            submit_total_budget_ms: default_submit_total_budget_ms(2_000),
            idempotency_db_path: ":memory:".to_string(),
            idempotency_claim_ttl_sec: DEFAULT_IDEMPOTENCY_CLAIM_TTL_SEC,
            max_notional_sol: 10.0,
            allow_nonzero_tip: true,
            submit_signature_verify: None,
        };
        let auth = Arc::new(AuthVerifier::new(&config));
        let idempotency = Arc::new(
            SubmitIdempotencyStore::open(config.idempotency_db_path.as_str())
                .expect("idempotency store"),
        );
        let http = Client::builder()
            .timeout(Duration::from_millis(2_000))
            .build()
            .expect("http client");
        AppState {
            config,
            http,
            auth,
            idempotency,
        }
    }

    fn spawn_one_shot_upstream_raw(
        status: u16,
        content_type: &str,
        body: &str,
    ) -> Option<(String, thread::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let response_body = body.to_string();
        let content_type = content_type.to_string();
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            _ => "Unknown",
        }
        .to_string();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request_buf = [0u8; 8192];
                let _ = stream.read(&mut request_buf);
                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    reason,
                    content_type,
                    response_body.len(),
                    response_body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        Some((format!("http://{}/upstream", addr), handle))
    }

    fn spawn_one_shot_upstream_raw_with_delay(
        status: u16,
        content_type: &str,
        body: &str,
        delay_ms: u64,
    ) -> Option<(String, thread::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let response_body = body.to_string();
        let content_type = content_type.to_string();
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            _ => "Unknown",
        }
        .to_string();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request_buf = [0u8; 8192];
                let _ = stream.read(&mut request_buf);
                thread::sleep(Duration::from_millis(delay_ms));
                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    reason,
                    content_type,
                    response_body.len(),
                    response_body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        Some((format!("http://{}/upstream", addr), handle))
    }

    fn spawn_one_shot_upstream_expect_bearer(
        expected_token: &str,
    ) -> Option<(String, thread::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let expected_token = expected_token.to_string();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request_buf = [0u8; 8192];
                let bytes_read = stream.read(&mut request_buf).unwrap_or(0);
                let request_raw = String::from_utf8_lossy(&request_buf[..bytes_read]);
                let authorized = request_raw.lines().any(|line| {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        return false;
                    }
                    let Some((name, value)) = trimmed.split_once(':') else {
                        return false;
                    };
                    if !name.trim().eq_ignore_ascii_case("authorization") {
                        return false;
                    }
                    let value = value.trim();
                    if value.len() < "bearer ".len()
                        || !value[.."bearer ".len()].eq_ignore_ascii_case("bearer ")
                    {
                        return false;
                    }
                    let provided_token = &value["bearer ".len()..];
                    provided_token == expected_token
                });

                let (status, reason, body) = if authorized {
                    (
                        200u16,
                        "OK",
                        "{\"status\":\"ok\",\"ok\":true,\"accepted\":true}",
                    )
                } else {
                    (401u16, "Unauthorized", "missing or invalid bearer token")
                };
                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    reason,
                    if status == 200 { "application/json" } else { "text/plain" },
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        Some((format!("http://{}/upstream", addr), handle))
    }

    fn spawn_one_shot_upstream_expect_tip_lamports(
        expected_tip_lamports: u64,
        signature: &str,
    ) -> Option<(String, thread::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let signature = signature.to_string();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request_buf = [0u8; 8192];
                let bytes_read = stream.read(&mut request_buf).unwrap_or(0);
                let request_raw = String::from_utf8_lossy(&request_buf[..bytes_read]);
                let request_body = request_raw
                    .split_once("\r\n\r\n")
                    .map(|(_, body)| body)
                    .unwrap_or_default();
                let tip_matches = serde_json::from_str::<Value>(request_body)
                    .ok()
                    .and_then(|value| value.get("tip_lamports").and_then(Value::as_u64))
                    .map(|value| value == expected_tip_lamports)
                    .unwrap_or(false);

                let (status, reason, body) = if tip_matches {
                    (
                        200u16,
                        "OK",
                        format!(
                            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}"}}"#,
                            signature
                        ),
                    )
                } else {
                    (
                        400u16,
                        "Bad Request",
                        "tip_lamports mismatch for rpc route".to_string(),
                    )
                };

                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    reason,
                    if status == 200 { "application/json" } else { "text/plain" },
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        Some((format!("http://{}/upstream", addr), handle))
    }

    fn spawn_one_shot_send_rpc_expect_bearer(
        expected_token: &str,
        signature: &str,
    ) -> Option<(String, thread::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let expected_token = expected_token.to_string();
        let signature = signature.to_string();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request_buf = [0u8; 8192];
                let bytes_read = stream.read(&mut request_buf).unwrap_or(0);
                let request_raw = String::from_utf8_lossy(&request_buf[..bytes_read]);
                let authorized = request_raw.lines().any(|line| {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        return false;
                    }
                    let Some((name, value)) = trimmed.split_once(':') else {
                        return false;
                    };
                    if !name.trim().eq_ignore_ascii_case("authorization") {
                        return false;
                    }
                    let value = value.trim();
                    if value.len() < "bearer ".len()
                        || !value[.."bearer ".len()].eq_ignore_ascii_case("bearer ")
                    {
                        return false;
                    }
                    let provided_token = &value["bearer ".len()..];
                    provided_token == expected_token
                });

                let (status, reason, body) = if authorized {
                    (
                        200u16,
                        "OK",
                        format!(r#"{{"jsonrpc":"2.0","result":"{}"}}"#, signature),
                    )
                } else {
                    (
                        401u16,
                        "Unauthorized",
                        "missing or invalid bearer token".to_string(),
                    )
                };
                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    reason,
                    if status == 200 { "application/json" } else { "text/plain" },
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        Some((format!("http://{}/upstream", addr), handle))
    }

    fn test_signed_tx_base64_with_signature(signature: [u8; 64]) -> (String, String) {
        let mut tx_bytes = Vec::with_capacity(1 + signature.len() + 1);
        tx_bytes.push(1u8);
        tx_bytes.extend_from_slice(&signature);
        tx_bytes.push(0u8);
        (
            BASE64_STANDARD.encode(tx_bytes),
            bs58::encode(signature).into_string(),
        )
    }

    fn temp_secret_path(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("monotonic time")
            .as_nanos();
        let seq = TEMP_SECRET_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "copybot_adapter_secret_{}_{}_{}_{}.tmp",
            prefix,
            std::process::id(),
            nanos,
            seq
        ))
    }

    fn write_temp_secret_file(contents: &str) -> PathBuf {
        let path = temp_secret_path("value");
        stdfs::write(&path, contents).expect("write temp secret");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = stdfs::metadata(&path)
                .expect("stat temp secret")
                .permissions();
            perms.set_mode(0o600);
            stdfs::set_permissions(&path, perms).expect("set temp secret perms");
        }
        path
    }

    fn write_temp_signer_keypair_file(pubkey_bytes: [u8; 32]) -> PathBuf {
        let mut bytes = vec![0u8; 64];
        bytes[32..64].copy_from_slice(&pubkey_bytes);
        let json = serde_json::to_string(&bytes).expect("serialize keypair json");
        write_temp_secret_file(json.as_str())
    }

    fn cleanup_temp_secret_file(path: PathBuf) {
        let _ = stdfs::remove_file(path);
    }
}
