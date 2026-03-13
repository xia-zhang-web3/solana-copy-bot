use anyhow::{anyhow, Context, Result};
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
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
    env, fs, future,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

mod http_utils;
mod send_rpc;
mod submit_verify;

use crate::http_utils::{
    classify_request_error, endpoint_identity, read_response_body_limited, redacted_endpoint_label,
    validate_endpoint_url, MAX_HTTP_ERROR_BODY_READ_BYTES, MAX_HTTP_JSON_BODY_READ_BYTES,
};
use crate::send_rpc::send_signed_transaction_via_rpc;
#[cfg(test)]
use crate::submit_verify::{build_submit_signature_verify_config, SubmitSignatureVerification};
use crate::submit_verify::{
    parse_submit_signature_verify_config, submit_signature_verification_to_json,
    verify_submitted_signature_visibility, SubmitSignatureVerifyConfig,
};
#[cfg(test)]
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
#[cfg(test)]
use solana_keypair::{Keypair, Signature, Signer};
#[cfg(test)]
use solana_message::{
    compiled_instruction::CompiledInstruction, legacy::Message as LegacyMessage, Address, Hash,
    MessageHeader, VersionedMessage,
};
#[cfg(test)]
use solana_transaction::versioned::VersionedTransaction;

const TIP_MAX_LAMPORTS: u64 = 100_000_000;
const CU_LIMIT_MIN: u32 = 1;
const CU_LIMIT_MAX: u32 = 1_400_000;
const CU_PRICE_MIN: u64 = 1;
const CU_PRICE_MAX: u64 = 10_000_000;
const POLICY_FLOAT_EPSILON: f64 = 1e-6;
const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8080";
const DEFAULT_TIMEOUT_MS: u64 = 8_000;
const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_NOTIONAL_SOL: f64 = 10.0;
const DEFAULT_HMAC_NONCE_CACHE_MAX_ENTRIES: usize = 100_000;
const DEFAULT_BASE_FEE_LAMPORTS: u64 = 5_000;
const DEFAULT_SUBMIT_VERIFY_ATTEMPTS: u64 = 3;
const DEFAULT_SUBMIT_VERIFY_INTERVAL_MS: u64 = 250;
static UPSTREAM_HMAC_NONCE_COUNTER: AtomicU64 = AtomicU64::new(1);
#[cfg(test)]
const TEST_ADAPTER_SIGNER_SECRET: [u8; 32] = [11u8; 32];

#[derive(Clone)]
struct AppState {
    config: AdapterConfig,
    http: Client,
    auth: Arc<AuthVerifier>,
}

#[derive(Clone)]
struct AdapterConfig {
    bind_addr: SocketAddr,
    contract_version: String,
    signer_pubkey: String,
    route_allowlist: HashSet<String>,
    route_backends: HashMap<String, RouteBackend>,
    bearer_token: Option<String>,
    hmac_key_id: Option<String>,
    hmac_secret: Option<String>,
    hmac_ttl_sec: u64,
    upstream_hmac: Option<UpstreamHmacConfig>,
    request_timeout_ms: u64,
    max_notional_sol: f64,
    allow_nonzero_tip: bool,
    submit_signature_verify: Option<SubmitSignatureVerifyConfig>,
}

#[derive(Clone, Debug)]
struct RouteBackend {
    submit_url: String,
    submit_fallback_url: Option<String>,
    simulate_url: String,
    simulate_fallback_url: Option<String>,
    primary_auth_token: Option<String>,
    fallback_auth_token: Option<String>,
    send_rpc_url: Option<String>,
    send_rpc_fallback_url: Option<String>,
    send_rpc_primary_auth_token: Option<String>,
    send_rpc_fallback_auth_token: Option<String>,
}

impl AdapterConfig {
    fn from_env() -> Result<Self> {
        let bind_addr = parse_socket_addr(
            env::var("COPYBOT_ADAPTER_BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string()),
        )?;

        let contract_version = env::var("COPYBOT_ADAPTER_CONTRACT_VERSION")
            .unwrap_or_else(|_| "v1".to_string())
            .trim()
            .to_string();
        if contract_version.is_empty()
            || contract_version.len() > 64
            || !is_valid_contract_version_token(&contract_version)
        {
            return Err(anyhow!(
                "COPYBOT_ADAPTER_CONTRACT_VERSION must be non-empty token [A-Za-z0-9._-], len<=64"
            ));
        }

        let signer_pubkey = non_empty_env("COPYBOT_ADAPTER_SIGNER_PUBKEY")?;
        validate_pubkey_like(signer_pubkey.as_str()).map_err(|error| {
            anyhow!(
                "COPYBOT_ADAPTER_SIGNER_PUBKEY must be valid base58 pubkey-like value: {}",
                error
            )
        })?;

        let route_allowlist = parse_route_allowlist(
            env::var("COPYBOT_ADAPTER_ROUTE_ALLOWLIST")
                .unwrap_or_else(|_| "paper,rpc,fastlane,jito".to_string()),
        )?;
        if route_allowlist.is_empty() {
            return Err(anyhow!(
                "COPYBOT_ADAPTER_ROUTE_ALLOWLIST must contain at least one route"
            ));
        }

        let default_submit = optional_non_empty_env("COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL");
        let default_submit_fallback =
            optional_non_empty_env("COPYBOT_ADAPTER_UPSTREAM_SUBMIT_FALLBACK_URL");
        let default_simulate = optional_non_empty_env("COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL");
        let default_simulate_fallback =
            optional_non_empty_env("COPYBOT_ADAPTER_UPSTREAM_SIMULATE_FALLBACK_URL");
        let default_send_rpc = optional_non_empty_env("COPYBOT_ADAPTER_SEND_RPC_URL");
        let default_send_rpc_fallback =
            optional_non_empty_env("COPYBOT_ADAPTER_SEND_RPC_FALLBACK_URL");
        let default_auth_token = resolve_secret_source(
            "COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN",
            optional_non_empty_env("COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN").as_deref(),
            "COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE",
            optional_non_empty_env("COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE").as_deref(),
        )?;
        let default_fallback_auth_token = resolve_secret_source(
            "COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN",
            optional_non_empty_env("COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN").as_deref(),
            "COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN_FILE",
            optional_non_empty_env("COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN_FILE").as_deref(),
        )?;
        let default_send_rpc_auth_token = resolve_secret_source(
            "COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN",
            optional_non_empty_env("COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN").as_deref(),
            "COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN_FILE",
            optional_non_empty_env("COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN_FILE").as_deref(),
        )?;
        let default_send_rpc_fallback_auth_token = resolve_secret_source(
            "COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN",
            optional_non_empty_env("COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN").as_deref(),
            "COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE",
            optional_non_empty_env("COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE").as_deref(),
        )?;

        let mut route_backends = HashMap::new();
        for route in &route_allowlist {
            let route_upper = route.to_ascii_uppercase();
            let submit_key = format!("COPYBOT_ADAPTER_ROUTE_{}_SUBMIT_URL", route_upper);
            let submit_fallback_key =
                format!("COPYBOT_ADAPTER_ROUTE_{}_SUBMIT_FALLBACK_URL", route_upper);
            let simulate_key = format!("COPYBOT_ADAPTER_ROUTE_{}_SIMULATE_URL", route_upper);
            let simulate_fallback_key = format!(
                "COPYBOT_ADAPTER_ROUTE_{}_SIMULATE_FALLBACK_URL",
                route_upper
            );
            let send_rpc_key = format!("COPYBOT_ADAPTER_ROUTE_{}_SEND_RPC_URL", route_upper);
            let send_rpc_fallback_key = format!(
                "COPYBOT_ADAPTER_ROUTE_{}_SEND_RPC_FALLBACK_URL",
                route_upper
            );
            let auth_key = format!("COPYBOT_ADAPTER_ROUTE_{}_AUTH_TOKEN", route_upper);
            let auth_file_key = format!("COPYBOT_ADAPTER_ROUTE_{}_AUTH_TOKEN_FILE", route_upper);
            let fallback_auth_key =
                format!("COPYBOT_ADAPTER_ROUTE_{}_FALLBACK_AUTH_TOKEN", route_upper);
            let fallback_auth_file_key = format!(
                "COPYBOT_ADAPTER_ROUTE_{}_FALLBACK_AUTH_TOKEN_FILE",
                route_upper
            );
            let send_rpc_auth_key =
                format!("COPYBOT_ADAPTER_ROUTE_{}_SEND_RPC_AUTH_TOKEN", route_upper);
            let send_rpc_auth_file_key = format!(
                "COPYBOT_ADAPTER_ROUTE_{}_SEND_RPC_AUTH_TOKEN_FILE",
                route_upper
            );
            let send_rpc_fallback_auth_key = format!(
                "COPYBOT_ADAPTER_ROUTE_{}_SEND_RPC_FALLBACK_AUTH_TOKEN",
                route_upper
            );
            let send_rpc_fallback_auth_file_key = format!(
                "COPYBOT_ADAPTER_ROUTE_{}_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE",
                route_upper
            );

            let submit_url = optional_non_empty_env(submit_key.as_str())
                .or_else(|| default_submit.clone())
                .ok_or_else(|| {
                    anyhow!(
                        "missing submit backend URL for route={} (set {} or COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL)",
                        route,
                        submit_key
                    )
                })?;
            let simulate_url = optional_non_empty_env(simulate_key.as_str())
                .or_else(|| default_simulate.clone())
                .ok_or_else(|| {
                    anyhow!(
                        "missing simulate backend URL for route={} (set {} or COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL)",
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
            "COPYBOT_ADAPTER_BEARER_TOKEN",
            optional_non_empty_env("COPYBOT_ADAPTER_BEARER_TOKEN").as_deref(),
            "COPYBOT_ADAPTER_BEARER_TOKEN_FILE",
            optional_non_empty_env("COPYBOT_ADAPTER_BEARER_TOKEN_FILE").as_deref(),
        )?;
        let hmac_key_id = optional_non_empty_env("COPYBOT_ADAPTER_HMAC_KEY_ID");
        let hmac_secret = resolve_secret_source(
            "COPYBOT_ADAPTER_HMAC_SECRET",
            optional_non_empty_env("COPYBOT_ADAPTER_HMAC_SECRET").as_deref(),
            "COPYBOT_ADAPTER_HMAC_SECRET_FILE",
            optional_non_empty_env("COPYBOT_ADAPTER_HMAC_SECRET_FILE").as_deref(),
        )?;
        let hmac_ttl_sec = parse_u64_env("COPYBOT_ADAPTER_HMAC_TTL_SEC", 30)?;
        let upstream_hmac_key_id = optional_non_empty_env("COPYBOT_ADAPTER_UPSTREAM_HMAC_KEY_ID");
        let upstream_hmac_secret = resolve_secret_source(
            "COPYBOT_ADAPTER_UPSTREAM_HMAC_SECRET",
            optional_non_empty_env("COPYBOT_ADAPTER_UPSTREAM_HMAC_SECRET").as_deref(),
            "COPYBOT_ADAPTER_UPSTREAM_HMAC_SECRET_FILE",
            optional_non_empty_env("COPYBOT_ADAPTER_UPSTREAM_HMAC_SECRET_FILE").as_deref(),
        )?;
        let upstream_hmac_ttl_sec = parse_u64_env("COPYBOT_ADAPTER_UPSTREAM_HMAC_TTL_SEC", 30)?;
        let allow_unauthenticated = parse_bool_env("COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED", false);
        if (hmac_key_id.is_some() && hmac_secret.is_none())
            || (hmac_key_id.is_none() && hmac_secret.is_some())
        {
            return Err(anyhow!(
                "COPYBOT_ADAPTER_HMAC_KEY_ID and COPYBOT_ADAPTER_HMAC_SECRET must be set together"
            ));
        }
        if hmac_key_id.is_some() && !(5..=300).contains(&hmac_ttl_sec) {
            return Err(anyhow!(
                "COPYBOT_ADAPTER_HMAC_TTL_SEC must be in 5..=300 when HMAC auth is enabled"
            ));
        }
        if (upstream_hmac_key_id.is_some() && upstream_hmac_secret.is_none())
            || (upstream_hmac_key_id.is_none() && upstream_hmac_secret.is_some())
        {
            return Err(anyhow!(
                "COPYBOT_ADAPTER_UPSTREAM_HMAC_KEY_ID and COPYBOT_ADAPTER_UPSTREAM_HMAC_SECRET must be set together"
            ));
        }
        if upstream_hmac_key_id.is_some() && !(5..=300).contains(&upstream_hmac_ttl_sec) {
            return Err(anyhow!(
                "COPYBOT_ADAPTER_UPSTREAM_HMAC_TTL_SEC must be in 5..=300 when upstream HMAC auth is enabled"
            ));
        }
        require_authenticated_mode(
            bearer_token.as_deref(),
            hmac_key_id.as_deref(),
            allow_unauthenticated,
        )?;
        let upstream_hmac = match (&upstream_hmac_key_id, &upstream_hmac_secret) {
            (Some(key_id), Some(secret)) => Some(UpstreamHmacConfig {
                key_id: key_id.clone(),
                secret: secret.clone(),
                ttl_sec: upstream_hmac_ttl_sec,
            }),
            _ => None,
        };

        let request_timeout_ms =
            parse_u64_env("COPYBOT_ADAPTER_REQUEST_TIMEOUT_MS", DEFAULT_TIMEOUT_MS)?;
        let max_notional_sol =
            parse_f64_env("COPYBOT_ADAPTER_MAX_NOTIONAL_SOL", DEFAULT_MAX_NOTIONAL_SOL)?;
        if !max_notional_sol.is_finite() || max_notional_sol <= 0.0 {
            return Err(anyhow!(
                "COPYBOT_ADAPTER_MAX_NOTIONAL_SOL must be finite and > 0"
            ));
        }
        let allow_nonzero_tip = parse_bool_env("COPYBOT_ADAPTER_ALLOW_NONZERO_TIP", true);
        let submit_signature_verify = parse_submit_signature_verify_config()?;

        Ok(Self {
            bind_addr,
            contract_version,
            signer_pubkey,
            route_allowlist,
            route_backends,
            bearer_token,
            hmac_key_id,
            hmac_secret,
            hmac_ttl_sec,
            upstream_hmac,
            request_timeout_ms,
            max_notional_sol,
            allow_nonzero_tip,
            submit_signature_verify,
        })
    }
}

#[derive(Clone)]
struct AuthVerifier {
    bearer_token: Option<String>,
    hmac: Option<HmacConfig>,
    nonce_seen_until_epoch: Arc<Mutex<HashMap<String, i64>>>,
    nonce_cache_max_entries: usize,
}

#[derive(Clone)]
struct HmacConfig {
    key_id: String,
    secret: String,
    ttl_sec: u64,
}

#[derive(Clone)]
struct UpstreamHmacConfig {
    key_id: String,
    secret: String,
    ttl_sec: u64,
}

impl AuthVerifier {
    fn new(config: &AdapterConfig) -> Self {
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
            nonce_cache_max_entries: DEFAULT_HMAC_NONCE_CACHE_MAX_ENTRIES,
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

            let payload = hmac_payload_bytes(timestamp, ttl, nonce, raw_body);
            let expected_signature =
                compute_hmac_signature_hex(hmac.secret.as_bytes(), payload.as_slice()).map_err(
                    |_| Reject::terminal("hmac_invalid", "failed computing HMAC signature"),
                )?;
            if !constant_time_eq(signature.as_bytes(), expected_signature.as_bytes()) {
                return Err(Reject::terminal("hmac_invalid", "HMAC signature mismatch"));
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
                if seen.len() >= self.nonce_cache_max_entries {
                    return Err(Reject::retryable(
                        "hmac_replay_cache_overflow",
                        format!(
                            "HMAC nonce replay cache capacity reached (max_entries={})",
                            self.nonce_cache_max_entries
                        ),
                    ));
                }
                seen.insert(nonce_key, timestamp.saturating_add(max_skew));
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
    request_id: Option<String>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum SubmitTransportArtifact {
    UpstreamSignature(String),
    SignedTransactionBase64(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SubmitTransportArtifactError {
    InvalidSubmitArtifactType { field_name: String },
    InvalidUpstreamSignature { error: String },
    ConflictingSubmitArtifacts,
    MissingSubmitArtifact,
}

#[derive(Clone, Debug, PartialEq)]
enum SubmitResponseValidationError {
    FieldMustBeNonEmptyStringWhenPresent {
        field_name: String,
    },
    FieldMustBeFiniteNumberWhenPresent {
        field_name: String,
    },
    FieldMustBeNonNegativeIntegerWhenPresent {
        field_name: String,
    },
    FieldMustBeObjectWhenPresent {
        field_name: String,
    },
    RouteMismatch {
        response_route: String,
        expected_route: String,
    },
    ContractVersionMismatch {
        response_contract_version: String,
        expected_contract_version: String,
    },
    ClientOrderIdMismatch {
        response_client_order_id: String,
        expected_client_order_id: String,
    },
    RequestIdMismatch {
        response_request_id: String,
        expected_request_id: String,
    },
    SignalIdMismatch {
        response_signal_id: String,
        expected_signal_id: String,
    },
    SideMismatch {
        response_side: String,
        expected_side: String,
    },
    TokenMismatch {
        response_token: String,
        expected_token: String,
    },
    SubmittedAtMustBeNonEmptyRfc3339,
    SubmittedAtInvalidRfc3339 {
        raw: String,
    },
    SlippageBpsMismatch {
        response_slippage_bps: f64,
        expected_slippage_bps: f64,
    },
    TipLamportsMismatch {
        response_tip_lamports: u64,
        expected_tip_lamports: u64,
    },
    ComputeBudgetCuLimitMismatch {
        response_cu_limit: u64,
        expected_cu_limit: u64,
    },
    ComputeBudgetCuPriceMicroLamportsMismatch {
        response_cu_price_micro_lamports: u64,
        expected_cu_price_micro_lamports: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SimulateResponseValidationError {
    FieldMustBeNonEmptyStringWhenPresent {
        field_name: String,
    },
    RouteMismatch {
        response_route: String,
        expected_route: String,
    },
    ContractVersionMismatch {
        response_contract_version: String,
        expected_contract_version: String,
    },
    RequestIdMismatch {
        response_request_id: String,
        expected_request_id: String,
    },
    SignalIdMismatch {
        response_signal_id: String,
        expected_signal_id: String,
    },
    SideMismatch {
        response_side: String,
        expected_side: String,
    },
    TokenMismatch {
        response_token: String,
        expected_token: String,
    },
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
    let log_json = parse_bool_env("COPYBOT_ADAPTER_LOG_JSON", true);
    let log_filter = env::var("COPYBOT_ADAPTER_LOG_FILTER")
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

    let config = AdapterConfig::from_env()?;
    let http = Client::builder()
        .timeout(Duration::from_millis(config.request_timeout_ms.max(500)))
        .build()
        .context("failed to build reqwest client")?;
    let auth = Arc::new(AuthVerifier::new(&config));

    let state = AppState { config, http, auth };

    let router = build_router(state.clone());

    info!(
        bind_addr = %state.config.bind_addr,
        signer_pubkey = %state.config.signer_pubkey,
        contract_version = %state.config.contract_version,
        routes = ?state.config.route_allowlist,
        submit_signature_verify_enabled = state.config.submit_signature_verify.is_some(),
        submit_signature_verify_strict = state
            .config
            .submit_signature_verify
            .as_ref()
            .map(|value| value.strict)
            .unwrap_or(false),
        "copybot adapter started"
    );
    let listener = tokio::net::TcpListener::bind(state.config.bind_addr)
        .await
        .context("failed binding adapter listener")?;
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("adapter server crashed")
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/simulate", post(simulate))
        .route("/submit", post(submit))
        .layer(DefaultBodyLimit::max(DEFAULT_MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {}
            Err(error) => {
                warn!(error = %error, "failed to install Ctrl+C signal handler");
                future::pending::<()>().await;
            }
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(error) => {
                warn!(error = %error, "failed to install SIGTERM signal handler");
                future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received, terminating adapter server");
}

async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "contract_version": state.config.contract_version,
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
            simulate_http_status_for_reject(&reject),
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
            submit_http_status_for_reject(&reject),
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
            let reject = Reject::terminal(
                "invalid_json",
                format!("request body is not valid JSON: {}", error),
            );
            return (
                submit_http_status_for_reject(&reject),
                Json(reject_to_json(
                    &reject,
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
            submit_http_status_for_reject(&reject),
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

    let route = normalize_route(request.route.as_str());
    debug!(
        route = %route,
        signal_id = %request.signal_id,
        "handling simulate request"
    );
    let backend_response =
        forward_to_upstream(state, route.as_str(), UpstreamAction::Simulate, raw_body).await?;
    match parse_upstream_outcome(&backend_response, "simulation_rejected") {
        UpstreamOutcome::Reject(reject) => return Err(reject),
        UpstreamOutcome::Success => {}
    }

    validate_simulate_response_route_and_contract(
        &backend_response,
        route.as_str(),
        state.config.contract_version.as_str(),
    )
    .map_err(map_simulate_response_validation_error_to_reject)?;
    validate_simulate_response_identity(
        &backend_response,
        request.request_id.as_deref().unwrap_or_default(),
        request.signal_id.as_str(),
        request.side.as_str(),
        request.token.as_str(),
    )
    .map_err(map_simulate_response_validation_error_to_reject)?;

    let detail = resolve_simulate_response_detail(&backend_response, "adapter_simulation_ok")
        .map_err(map_simulate_response_validation_error_to_reject)?;

    Ok(json!({
        "status": "ok",
        "ok": true,
        "accepted": true,
        "route": route,
        "contract_version": state.config.contract_version,
        "request_id": request.request_id.as_deref().map(str::trim).unwrap_or_default(),
        "signal_id": request.signal_id.trim(),
        "side": request.side.trim().to_ascii_lowercase(),
        "token": request.token.trim(),
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
    if request.tip_lamports > TIP_MAX_LAMPORTS {
        return Err(Reject::terminal(
            "invalid_tip_lamports",
            format!("tip_lamports exceeds max {}", TIP_MAX_LAMPORTS),
        ));
    }
    if request.tip_lamports > 0 && !state.config.allow_nonzero_tip {
        return Err(Reject::terminal(
            "tip_not_supported",
            "non-zero tip_lamports is disabled in adapter config",
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

    let route = normalize_route(request.route.as_str());
    debug!(
        route = %route,
        signal_id = %request.signal_id,
        client_order_id = %request.client_order_id,
        "handling submit request"
    );
    let backend_response =
        forward_to_upstream(state, route.as_str(), UpstreamAction::Submit, raw_body).await?;
    match parse_upstream_outcome(&backend_response, "submit_adapter_rejected") {
        UpstreamOutcome::Reject(reject) => return Err(reject),
        UpstreamOutcome::Success => {}
    }

    validate_submit_response_route_and_contract(
        &backend_response,
        route.as_str(),
        state.config.contract_version.as_str(),
    )
    .map_err(map_submit_response_validation_error_to_reject)?;
    validate_submit_response_request_identity(
        &backend_response,
        request.client_order_id.as_str(),
        request.request_id.as_str(),
    )
    .map_err(map_submit_response_validation_error_to_reject)?;
    validate_submit_response_extended_identity(
        &backend_response,
        request.signal_id.as_str(),
        request.side.as_str(),
        request.token.as_str(),
    )
    .map_err(map_submit_response_validation_error_to_reject)?;
    validate_submit_response_policy_echoes(
        &backend_response,
        request.slippage_bps,
        request.tip_lamports,
        request.compute_budget.cu_limit,
        request.compute_budget.cu_price_micro_lamports,
    )
    .map_err(map_submit_response_validation_error_to_reject)?;

    let submitted_at = resolve_submit_response_submitted_at(&backend_response, Utc::now())
        .map_err(map_submit_response_validation_error_to_reject)?;

    let ata_create_rent_lamports =
        parse_optional_non_negative_u64_field(&backend_response, "ata_create_rent_lamports")?;
    let response_network_fee =
        parse_optional_non_negative_u64_field(&backend_response, "network_fee_lamports")?;
    let response_base_fee =
        parse_optional_non_negative_u64_field(&backend_response, "base_fee_lamports")?;
    let response_priority_fee =
        parse_optional_non_negative_u64_field(&backend_response, "priority_fee_lamports")?;

    let default_priority_fee = ((request.compute_budget.cu_limit as u128)
        .saturating_mul(request.compute_budget.cu_price_micro_lamports as u128)
        / 1_000_000u128) as u64;
    let base_fee_lamports = response_base_fee.unwrap_or(DEFAULT_BASE_FEE_LAMPORTS);
    let priority_fee_lamports = response_priority_fee.unwrap_or(default_priority_fee);
    let derived_network_fee_lamports = base_fee_lamports
        .checked_add(priority_fee_lamports)
        .ok_or_else(|| Reject::terminal("fee_overflow", "base+priority fee overflow"))?;

    let network_fee_lamports = response_network_fee.unwrap_or(derived_network_fee_lamports);
    if network_fee_lamports != derived_network_fee_lamports {
        return Err(Reject::terminal(
            "submit_adapter_invalid_response",
            format!(
                "network_fee_lamports={} does not match base+priority={}",
                network_fee_lamports, derived_network_fee_lamports
            ),
        ));
    }

    for (field, value) in [
        ("network_fee_lamports", network_fee_lamports),
        ("base_fee_lamports", base_fee_lamports),
        ("priority_fee_lamports", priority_fee_lamports),
    ] {
        if value > i64::MAX as u64 {
            return Err(Reject::terminal(
                "submit_adapter_invalid_response",
                format!("{}={} exceeds i64::MAX", field, value),
            ));
        }
    }
    if let Some(value) = ata_create_rent_lamports {
        if value > i64::MAX as u64 {
            return Err(Reject::terminal(
                "submit_adapter_invalid_response",
                format!("ata_create_rent_lamports={} exceeds i64::MAX", value),
            ));
        }
    }

    let submit_transport_artifact =
        extract_submit_transport_artifact(&backend_response).map_err(|error| match error {
            SubmitTransportArtifactError::InvalidSubmitArtifactType { field_name } => {
                Reject::retryable(
                    "submit_adapter_invalid_response",
                    format!(
                        "upstream {} must be a non-empty string when present",
                        field_name
                    ),
                )
            }
            SubmitTransportArtifactError::InvalidUpstreamSignature { error } => Reject::retryable(
                "submit_adapter_invalid_response",
                format!(
                    "upstream tx_signature is not valid base58 signature: {}",
                    error
                ),
            ),
            SubmitTransportArtifactError::ConflictingSubmitArtifacts => Reject::retryable(
                "submit_adapter_invalid_response",
                "upstream response must not include both tx_signature and signed_tx_base64",
            ),
            SubmitTransportArtifactError::MissingSubmitArtifact => Reject::retryable(
                "submit_adapter_invalid_response",
                "upstream response missing tx_signature and signed_tx_base64",
            ),
        })?;
    let (tx_signature, submit_transport) = match submit_transport_artifact {
        SubmitTransportArtifact::UpstreamSignature(value) => (value, "upstream_signature"),
        SubmitTransportArtifact::SignedTransactionBase64(value) => {
            let signature =
                send_signed_transaction_via_rpc(state, route.as_str(), value.as_str()).await?;
            (signature, "adapter_send_rpc")
        }
    };

    let submit_signature_verify =
        verify_submitted_signature_visibility(state, route.as_str(), tx_signature.as_str()).await?;

    let mut response = json!({
        "status": "ok",
        "ok": true,
        "accepted": true,
        "route": route,
        "contract_version": state.config.contract_version,
        "signal_id": request.signal_id.trim(),
        "client_order_id": request.client_order_id.trim(),
        "request_id": request.request_id.trim(),
        "side": request.side.trim().to_ascii_lowercase(),
        "token": request.token.trim(),
        "tx_signature": tx_signature,
        "submit_transport": submit_transport,
        "submitted_at": submitted_at.to_rfc3339(),
        "slippage_bps": request.slippage_bps,
        "tip_lamports": request.tip_lamports,
        "compute_budget": {
            "cu_limit": request.compute_budget.cu_limit,
            "cu_price_micro_lamports": request.compute_budget.cu_price_micro_lamports,
        },
        "network_fee_lamports": network_fee_lamports,
        "base_fee_lamports": base_fee_lamports,
        "priority_fee_lamports": priority_fee_lamports,
        "ata_create_rent_lamports": ata_create_rent_lamports,
    });
    response["submit_signature_verify"] =
        submit_signature_verification_to_json(&submit_signature_verify);
    Ok(response)
}

#[derive(Clone, Copy)]
enum UpstreamAction {
    Simulate,
    Submit,
}

impl UpstreamAction {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Simulate => "simulate",
            Self::Submit => "submit",
        }
    }
}

async fn forward_to_upstream(
    state: &AppState,
    route: &str,
    action: UpstreamAction,
    raw_body: &[u8],
) -> std::result::Result<Value, Reject> {
    let backend = state.config.route_backends.get(route).ok_or_else(|| {
        Reject::terminal(
            "route_not_allowed",
            format!("route={} not configured", route),
        )
    })?;

    let mut endpoints = Vec::with_capacity(2);
    match action {
        UpstreamAction::Simulate => {
            endpoints.push(backend.simulate_url.as_str());
            if let Some(url) = backend.simulate_fallback_url.as_deref() {
                endpoints.push(url);
            }
        }
        UpstreamAction::Submit => {
            endpoints.push(backend.submit_url.as_str());
            if let Some(url) = backend.submit_fallback_url.as_deref() {
                endpoints.push(url);
            }
        }
    }

    let mut last_retryable: Option<Reject> = None;
    for (attempt_idx, url) in endpoints.iter().enumerate() {
        let endpoint_label = redacted_endpoint_label(url);
        debug!(
            route = %route,
            action = %action.as_str(),
            endpoint = %endpoint_label,
            attempt = attempt_idx + 1,
            total = endpoints.len(),
            "forwarding adapter request upstream"
        );

        let mut request = state
            .http
            .post(*url)
            .header("content-type", "application/json")
            .body(raw_body.to_vec());
        let selected_auth_token = if attempt_idx == 0 {
            backend.primary_auth_token.as_deref()
        } else {
            backend.fallback_auth_token.as_deref()
        };
        if let Some(token) = selected_auth_token {
            request = request.bearer_auth(token);
        }
        if let Some(upstream_hmac) = state.config.upstream_hmac.as_ref() {
            let timestamp = Utc::now().timestamp();
            let nonce = build_upstream_hmac_nonce(route, action, attempt_idx);
            let payload =
                hmac_payload_bytes(timestamp, upstream_hmac.ttl_sec, nonce.as_str(), raw_body);
            let signature =
                compute_hmac_signature_hex(upstream_hmac.secret.as_bytes(), payload.as_slice())
                    .map_err(|error| {
                        Reject::terminal(
                            "upstream_hmac_signing_failed",
                            format!(
                                "upstream {} hmac signing failed endpoint={} err={}",
                                action.as_str(),
                                endpoint_label,
                                error
                            ),
                        )
                    })?;
            request = request
                .header("x-copybot-key-id", upstream_hmac.key_id.as_str())
                .header("x-copybot-signature-alg", "hmac-sha256-v1")
                .header("x-copybot-timestamp", timestamp.to_string())
                .header("x-copybot-auth-ttl-sec", upstream_hmac.ttl_sec.to_string())
                .header("x-copybot-nonce", nonce)
                .header("x-copybot-signature", signature);
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
            let body_text = read_response_body_limited(response, MAX_HTTP_ERROR_BODY_READ_BYTES)
                .await
                .text;
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

        let body_read = read_response_body_limited(response, MAX_HTTP_JSON_BODY_READ_BYTES).await;
        if let Some(read_error_class) = body_read.read_error_class {
            return Err(Reject::terminal(
                "upstream_body_read_failed",
                format!(
                    "upstream {} body read failed endpoint={} class={}",
                    action.as_str(),
                    endpoint_label,
                    read_error_class
                ),
            ));
        }
        if body_read.was_truncated {
            return Err(Reject::terminal(
                "upstream_response_too_large",
                format!(
                    "upstream {} response exceeded {} bytes endpoint={}",
                    action.as_str(),
                    MAX_HTTP_JSON_BODY_READ_BYTES,
                    endpoint_label
                ),
            ));
        }
        let body: Value = serde_json::from_slice(body_read.bytes.as_slice()).map_err(|error| {
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
    let status = match parse_optional_upstream_status_field(body) {
        Ok(Some(value)) => value,
        Ok(None) => String::new(),
        Err(detail) => {
            return UpstreamOutcome::Reject(Reject::terminal("upstream_invalid_response", detail))
        }
    };

    let ok_flag = match parse_optional_upstream_bool_field(
        body,
        "ok",
        "upstream ok must be boolean when present",
    ) {
        Ok(value) => value,
        Err(detail) => {
            return UpstreamOutcome::Reject(Reject::terminal("upstream_invalid_response", detail))
        }
    };
    let accepted_flag = match parse_optional_upstream_bool_field(
        body,
        "accepted",
        "upstream accepted must be boolean when present",
    ) {
        Ok(value) => value,
        Err(detail) => {
            return UpstreamOutcome::Reject(Reject::terminal("upstream_invalid_response", detail))
        }
    };

    let is_known_success_status = matches!(status.as_str(), "ok" | "accepted" | "success");
    let is_known_reject_status = matches!(
        status.as_str(),
        "reject" | "rejected" | "error" | "failed" | "failure"
    );
    let is_known_status = is_known_success_status || is_known_reject_status;

    if !status.is_empty() && !is_known_status {
        return UpstreamOutcome::Reject(Reject::terminal(
            "upstream_invalid_status",
            format!("unknown upstream status={}", status),
        ));
    }

    if is_known_success_status && (ok_flag == Some(false) || accepted_flag == Some(false)) {
        return UpstreamOutcome::Reject(Reject::terminal(
            "upstream_invalid_response",
            "upstream status=ok conflicts with reject flags",
        ));
    }
    if is_known_reject_status && (ok_flag == Some(true) || accepted_flag == Some(true)) {
        return UpstreamOutcome::Reject(Reject::terminal(
            "upstream_invalid_response",
            "upstream status=reject conflicts with success flags",
        ));
    }
    if status.is_empty() && ok_flag.is_some() && accepted_flag.is_some() && ok_flag != accepted_flag
    {
        return UpstreamOutcome::Reject(Reject::terminal(
            "upstream_invalid_response",
            "upstream ok/accepted flags conflict when status is missing",
        ));
    }

    let retryable = match parse_optional_upstream_bool_field(
        body,
        "retryable",
        "upstream retryable must be boolean when present",
    ) {
        Ok(Some(value)) => value,
        Ok(None) => false,
        Err(detail) => {
            return UpstreamOutcome::Reject(Reject::terminal("upstream_invalid_response", detail))
        }
    };
    let response_code = match parse_optional_non_empty_upstream_string_field(
        body,
        "code",
        "upstream code must be non-empty string when present",
    ) {
        Ok(value) => value,
        Err(detail) => {
            return UpstreamOutcome::Reject(Reject::terminal("upstream_invalid_response", detail))
        }
    };
    let response_detail = match parse_optional_non_empty_upstream_string_field(
        body,
        "detail",
        "upstream detail must be non-empty string when present",
    ) {
        Ok(value) => value,
        Err(detail) => {
            return UpstreamOutcome::Reject(Reject::terminal("upstream_invalid_response", detail))
        }
    };

    let is_reject = matches!(
        status.as_str(),
        "reject" | "rejected" | "error" | "failed" | "failure"
    ) || ok_flag == Some(false)
        || accepted_flag == Some(false);
    if is_reject {
        let code = response_code.unwrap_or_else(|| default_reject_code.to_string());
        let detail = response_detail.unwrap_or_else(|| "upstream rejected request".to_string());
        return UpstreamOutcome::Reject(if retryable {
            Reject::retryable(code, detail)
        } else {
            Reject::terminal(code, detail)
        });
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

fn parse_optional_non_empty_upstream_string_field(
    payload: &Value,
    field_name: &str,
    invalid_detail: &str,
) -> Result<Option<String>, String> {
    let Some(field_value) = payload.get(field_name) else {
        return Ok(None);
    };
    let Some(raw_value) = field_value.as_str() else {
        return Err(invalid_detail.to_string());
    };
    let normalized = raw_value.trim();
    if normalized.is_empty() {
        return Err(invalid_detail.to_string());
    }
    Ok(Some(normalized.to_string()))
}

fn parse_optional_upstream_bool_field(
    payload: &Value,
    field_name: &str,
    invalid_detail: &str,
) -> Result<Option<bool>, String> {
    let Some(field_value) = payload.get(field_name) else {
        return Ok(None);
    };
    let Some(normalized) = field_value.as_bool() else {
        return Err(invalid_detail.to_string());
    };
    Ok(Some(normalized))
}

fn parse_optional_upstream_status_field(payload: &Value) -> Result<Option<String>, String> {
    let Some(field_value) = payload.get("status") else {
        return Ok(None);
    };
    let Some(raw_status) = field_value.as_str() else {
        return Err("upstream status must be non-empty string when present".to_string());
    };
    let normalized = raw_status.trim();
    if normalized.is_empty() {
        return Err("upstream status must be non-empty string when present".to_string());
    }
    Ok(Some(normalized.to_ascii_lowercase()))
}

fn map_simulate_response_validation_error_to_reject(
    error: SimulateResponseValidationError,
) -> Reject {
    match error {
        SimulateResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name } => {
            Reject::terminal(
                "simulation_invalid_response",
                format!(
                    "upstream simulate field {} must be a non-empty string when present",
                    field_name
                ),
            )
        }
        SimulateResponseValidationError::RouteMismatch {
            response_route,
            expected_route,
        } => Reject::terminal(
            "simulation_route_mismatch",
            format!(
                "upstream route={} does not match requested route={}",
                response_route, expected_route
            ),
        ),
        SimulateResponseValidationError::ContractVersionMismatch {
            response_contract_version,
            expected_contract_version,
        } => Reject::terminal(
            "simulation_contract_version_mismatch",
            format!(
                "upstream contract_version={} does not match expected={}",
                response_contract_version, expected_contract_version
            ),
        ),
        SimulateResponseValidationError::RequestIdMismatch {
            response_request_id,
            expected_request_id,
        } => Reject::terminal(
            "simulation_request_id_mismatch",
            format!(
                "upstream request_id={} does not match expected request_id={}",
                response_request_id, expected_request_id
            ),
        ),
        SimulateResponseValidationError::SignalIdMismatch {
            response_signal_id,
            expected_signal_id,
        } => Reject::terminal(
            "simulation_signal_id_mismatch",
            format!(
                "upstream signal_id={} does not match expected signal_id={}",
                response_signal_id, expected_signal_id
            ),
        ),
        SimulateResponseValidationError::SideMismatch {
            response_side,
            expected_side,
        } => Reject::terminal(
            "simulation_side_mismatch",
            format!(
                "upstream side={} does not match expected side={}",
                response_side, expected_side
            ),
        ),
        SimulateResponseValidationError::TokenMismatch {
            response_token,
            expected_token,
        } => Reject::terminal(
            "simulation_token_mismatch",
            format!(
                "upstream token={} does not match expected token={}",
                response_token, expected_token
            ),
        ),
    }
}

fn validate_simulate_response_route_and_contract(
    backend_response: &Value,
    expected_route: &str,
    expected_contract_version: &str,
) -> Result<(), SimulateResponseValidationError> {
    if let Some(response_route_raw) =
        parse_optional_non_empty_simulate_response_field(backend_response, "route")?
    {
        let response_route = normalize_route(response_route_raw.as_str());
        if response_route != expected_route {
            return Err(SimulateResponseValidationError::RouteMismatch {
                response_route,
                expected_route: expected_route.to_string(),
            });
        }
    }

    if let Some(response_contract_version) =
        parse_optional_non_empty_simulate_response_field(backend_response, "contract_version")?
    {
        if response_contract_version.as_str() != expected_contract_version {
            return Err(SimulateResponseValidationError::ContractVersionMismatch {
                response_contract_version,
                expected_contract_version: expected_contract_version.to_string(),
            });
        }
    }

    Ok(())
}

fn validate_simulate_response_identity(
    backend_response: &Value,
    expected_request_id: &str,
    expected_signal_id: &str,
    expected_side: &str,
    expected_token: &str,
) -> Result<(), SimulateResponseValidationError> {
    let normalized_expected_request_id = expected_request_id.trim();
    let normalized_expected_signal_id = expected_signal_id.trim();
    let normalized_expected_side = expected_side.trim().to_ascii_lowercase();
    let normalized_expected_token = expected_token.trim();

    if let Some(response_request_id) =
        parse_optional_non_empty_simulate_response_field(backend_response, "request_id")?
    {
        if response_request_id.as_str() != normalized_expected_request_id {
            return Err(SimulateResponseValidationError::RequestIdMismatch {
                response_request_id,
                expected_request_id: normalized_expected_request_id.to_string(),
            });
        }
    }

    if let Some(response_signal_id) =
        parse_optional_non_empty_simulate_response_field(backend_response, "signal_id")?
    {
        if response_signal_id.as_str() != normalized_expected_signal_id {
            return Err(SimulateResponseValidationError::SignalIdMismatch {
                response_signal_id,
                expected_signal_id: normalized_expected_signal_id.to_string(),
            });
        }
    }

    if let Some(response_side) =
        parse_optional_non_empty_simulate_response_field(backend_response, "side")?
    {
        let normalized_response_side = response_side.to_ascii_lowercase();
        if normalized_response_side != normalized_expected_side {
            return Err(SimulateResponseValidationError::SideMismatch {
                response_side,
                expected_side: normalized_expected_side,
            });
        }
    }

    if let Some(response_token) =
        parse_optional_non_empty_simulate_response_field(backend_response, "token")?
    {
        if response_token.as_str() != normalized_expected_token {
            return Err(SimulateResponseValidationError::TokenMismatch {
                response_token,
                expected_token: normalized_expected_token.to_string(),
            });
        }
    }

    Ok(())
}

fn resolve_simulate_response_detail(
    backend_response: &Value,
    default_detail: &str,
) -> Result<String, SimulateResponseValidationError> {
    Ok(
        parse_optional_non_empty_simulate_response_field(backend_response, "detail")?
            .unwrap_or_else(|| default_detail.to_string()),
    )
}

fn parse_optional_non_empty_simulate_response_field(
    backend_response: &Value,
    field_name: &str,
) -> Result<Option<String>, SimulateResponseValidationError> {
    let Some(field_value) = backend_response.get(field_name) else {
        return Ok(None);
    };
    let Some(raw_value) = field_value.as_str() else {
        return Err(
            SimulateResponseValidationError::FieldMustBeNonEmptyStringWhenPresent {
                field_name: field_name.to_string(),
            },
        );
    };
    let normalized = raw_value.trim();
    if normalized.is_empty() {
        return Err(
            SimulateResponseValidationError::FieldMustBeNonEmptyStringWhenPresent {
                field_name: field_name.to_string(),
            },
        );
    }
    Ok(Some(normalized.to_string()))
}

fn map_submit_response_validation_error_to_reject(error: SubmitResponseValidationError) -> Reject {
    match error {
        SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { field_name } => {
            Reject::terminal(
                "submit_adapter_invalid_response",
                format!(
                    "upstream {} must be non-empty string when present",
                    field_name
                ),
            )
        }
        SubmitResponseValidationError::FieldMustBeFiniteNumberWhenPresent { field_name } => {
            Reject::terminal(
                "submit_adapter_invalid_response",
                format!(
                    "upstream {} must be finite number when present",
                    field_name
                ),
            )
        }
        SubmitResponseValidationError::FieldMustBeNonNegativeIntegerWhenPresent { field_name } => {
            Reject::terminal(
                "submit_adapter_invalid_response",
                format!(
                    "upstream {} must be non-negative integer when present",
                    field_name
                ),
            )
        }
        SubmitResponseValidationError::FieldMustBeObjectWhenPresent { field_name } => {
            Reject::terminal(
                "submit_adapter_invalid_response",
                format!("upstream {} must be object when present", field_name),
            )
        }
        SubmitResponseValidationError::RouteMismatch {
            response_route,
            expected_route,
        } => Reject::terminal(
            "submit_adapter_route_mismatch",
            format!(
                "upstream route={} does not match requested route={}",
                response_route, expected_route
            ),
        ),
        SubmitResponseValidationError::ContractVersionMismatch {
            response_contract_version,
            expected_contract_version,
        } => Reject::terminal(
            "submit_adapter_contract_version_mismatch",
            format!(
                "upstream contract_version={} does not match expected={}",
                response_contract_version, expected_contract_version
            ),
        ),
        SubmitResponseValidationError::ClientOrderIdMismatch {
            response_client_order_id,
            expected_client_order_id,
        } => Reject::terminal(
            "submit_adapter_client_order_id_mismatch",
            format!(
                "upstream client_order_id={} does not match expected client_order_id={}",
                response_client_order_id, expected_client_order_id
            ),
        ),
        SubmitResponseValidationError::RequestIdMismatch {
            response_request_id,
            expected_request_id,
        } => Reject::terminal(
            "submit_adapter_request_id_mismatch",
            format!(
                "upstream request_id={} does not match expected request_id={}",
                response_request_id, expected_request_id
            ),
        ),
        SubmitResponseValidationError::SignalIdMismatch {
            response_signal_id,
            expected_signal_id,
        } => Reject::terminal(
            "submit_adapter_signal_id_mismatch",
            format!(
                "upstream signal_id={} does not match expected signal_id={}",
                response_signal_id, expected_signal_id
            ),
        ),
        SubmitResponseValidationError::SideMismatch {
            response_side,
            expected_side,
        } => Reject::terminal(
            "submit_adapter_side_mismatch",
            format!(
                "upstream side={} does not match expected side={}",
                response_side, expected_side
            ),
        ),
        SubmitResponseValidationError::TokenMismatch {
            response_token,
            expected_token,
        } => Reject::terminal(
            "submit_adapter_token_mismatch",
            format!(
                "upstream token={} does not match expected token={}",
                response_token, expected_token
            ),
        ),
        SubmitResponseValidationError::SubmittedAtMustBeNonEmptyRfc3339 => Reject::terminal(
            "submit_adapter_invalid_response",
            "submitted_at must be non-empty RFC3339 string",
        ),
        SubmitResponseValidationError::SubmittedAtInvalidRfc3339 { raw } => Reject::terminal(
            "submit_adapter_invalid_response",
            format!("submitted_at is not valid RFC3339: {}", raw),
        ),
        SubmitResponseValidationError::SlippageBpsMismatch {
            response_slippage_bps,
            expected_slippage_bps,
        } => Reject::terminal(
            "submit_adapter_slippage_bps_mismatch",
            format!(
                "upstream slippage_bps={} does not match expected slippage_bps={}",
                response_slippage_bps, expected_slippage_bps
            ),
        ),
        SubmitResponseValidationError::TipLamportsMismatch {
            response_tip_lamports,
            expected_tip_lamports,
        } => Reject::terminal(
            "submit_adapter_tip_lamports_mismatch",
            format!(
                "upstream tip_lamports={} does not match expected tip_lamports={}",
                response_tip_lamports, expected_tip_lamports
            ),
        ),
        SubmitResponseValidationError::ComputeBudgetCuLimitMismatch {
            response_cu_limit,
            expected_cu_limit,
        } => Reject::terminal(
            "submit_adapter_compute_budget_mismatch",
            format!(
                "upstream compute_budget.cu_limit={} does not match expected compute_budget.cu_limit={}",
                response_cu_limit, expected_cu_limit
            ),
        ),
        SubmitResponseValidationError::ComputeBudgetCuPriceMicroLamportsMismatch {
            response_cu_price_micro_lamports,
            expected_cu_price_micro_lamports,
        } => Reject::terminal(
            "submit_adapter_compute_budget_mismatch",
            format!(
                "upstream compute_budget.cu_price_micro_lamports={} does not match expected compute_budget.cu_price_micro_lamports={}",
                response_cu_price_micro_lamports, expected_cu_price_micro_lamports
            ),
        ),
    }
}

fn validate_submit_response_route_and_contract(
    backend_response: &Value,
    expected_route: &str,
    expected_contract_version: &str,
) -> Result<(), SubmitResponseValidationError> {
    if let Some(response_route_raw) =
        parse_optional_non_empty_submit_response_field(backend_response, "route")?
    {
        let response_route = normalize_route(response_route_raw.as_str());
        if response_route != expected_route {
            return Err(SubmitResponseValidationError::RouteMismatch {
                response_route,
                expected_route: expected_route.to_string(),
            });
        }
    }

    if let Some(response_contract_version) =
        parse_optional_non_empty_submit_response_field(backend_response, "contract_version")?
    {
        if response_contract_version.as_str() != expected_contract_version {
            return Err(SubmitResponseValidationError::ContractVersionMismatch {
                response_contract_version,
                expected_contract_version: expected_contract_version.to_string(),
            });
        }
    }

    Ok(())
}

fn validate_submit_response_request_identity(
    backend_response: &Value,
    expected_client_order_id: &str,
    expected_request_id: &str,
) -> Result<(), SubmitResponseValidationError> {
    let normalized_expected_client_order_id = expected_client_order_id.trim();
    let normalized_expected_request_id = expected_request_id.trim();

    if let Some(response_client_order_id) =
        parse_optional_non_empty_submit_response_field(backend_response, "client_order_id")?
    {
        if response_client_order_id.as_str() != normalized_expected_client_order_id {
            return Err(SubmitResponseValidationError::ClientOrderIdMismatch {
                response_client_order_id,
                expected_client_order_id: normalized_expected_client_order_id.to_string(),
            });
        }
    }

    if let Some(response_request_id) =
        parse_optional_non_empty_submit_response_field(backend_response, "request_id")?
    {
        if response_request_id.as_str() != normalized_expected_request_id {
            return Err(SubmitResponseValidationError::RequestIdMismatch {
                response_request_id,
                expected_request_id: normalized_expected_request_id.to_string(),
            });
        }
    }

    Ok(())
}

fn validate_submit_response_extended_identity(
    backend_response: &Value,
    expected_signal_id: &str,
    expected_side: &str,
    expected_token: &str,
) -> Result<(), SubmitResponseValidationError> {
    let normalized_expected_signal_id = expected_signal_id.trim();
    let normalized_expected_side = expected_side.trim().to_ascii_lowercase();
    let normalized_expected_token = expected_token.trim();

    if let Some(response_signal_id) =
        parse_optional_non_empty_submit_response_field(backend_response, "signal_id")?
    {
        if response_signal_id.as_str() != normalized_expected_signal_id {
            return Err(SubmitResponseValidationError::SignalIdMismatch {
                response_signal_id,
                expected_signal_id: normalized_expected_signal_id.to_string(),
            });
        }
    }

    if let Some(response_side) =
        parse_optional_non_empty_submit_response_field(backend_response, "side")?
    {
        let normalized_response_side = response_side.to_ascii_lowercase();
        if normalized_response_side != normalized_expected_side {
            return Err(SubmitResponseValidationError::SideMismatch {
                response_side,
                expected_side: normalized_expected_side,
            });
        }
    }

    if let Some(response_token) =
        parse_optional_non_empty_submit_response_field(backend_response, "token")?
    {
        if response_token.as_str() != normalized_expected_token {
            return Err(SubmitResponseValidationError::TokenMismatch {
                response_token,
                expected_token: normalized_expected_token.to_string(),
            });
        }
    }

    Ok(())
}

fn validate_submit_response_policy_echoes(
    backend_response: &Value,
    expected_slippage_bps: f64,
    expected_tip_lamports: u64,
    expected_cu_limit: u32,
    expected_cu_price_micro_lamports: u64,
) -> Result<(), SubmitResponseValidationError> {
    if let Some(response_slippage_bps) =
        parse_optional_finite_submit_response_f64_field(backend_response, "slippage_bps")?
    {
        if (response_slippage_bps - expected_slippage_bps).abs() > POLICY_FLOAT_EPSILON {
            return Err(SubmitResponseValidationError::SlippageBpsMismatch {
                response_slippage_bps,
                expected_slippage_bps,
            });
        }
    }

    if let Some(response_tip_lamports) =
        parse_optional_non_negative_submit_response_u64_field(backend_response, "tip_lamports")?
    {
        if response_tip_lamports != expected_tip_lamports {
            return Err(SubmitResponseValidationError::TipLamportsMismatch {
                response_tip_lamports,
                expected_tip_lamports,
            });
        }
    }

    if let Some(compute_budget) = backend_response.get("compute_budget") {
        let Some(compute_budget) = compute_budget.as_object() else {
            return Err(SubmitResponseValidationError::FieldMustBeObjectWhenPresent {
                field_name: "compute_budget".to_string(),
            });
        };
        let response_cu_limit = parse_required_non_negative_submit_response_u64_field(
            compute_budget.get("cu_limit"),
            "compute_budget.cu_limit",
        )?;
        let expected_cu_limit = u64::from(expected_cu_limit);
        if response_cu_limit != expected_cu_limit {
            return Err(SubmitResponseValidationError::ComputeBudgetCuLimitMismatch {
                response_cu_limit,
                expected_cu_limit,
            });
        }

        let response_cu_price_micro_lamports =
            parse_required_non_negative_submit_response_u64_field(
                compute_budget.get("cu_price_micro_lamports"),
                "compute_budget.cu_price_micro_lamports",
            )?;
        if response_cu_price_micro_lamports != expected_cu_price_micro_lamports {
            return Err(
                SubmitResponseValidationError::ComputeBudgetCuPriceMicroLamportsMismatch {
                    response_cu_price_micro_lamports,
                    expected_cu_price_micro_lamports,
                },
            );
        }
    }

    Ok(())
}

fn parse_optional_non_empty_submit_response_field(
    backend_response: &Value,
    field_name: &str,
) -> Result<Option<String>, SubmitResponseValidationError> {
    let Some(field_value) = backend_response.get(field_name) else {
        return Ok(None);
    };
    let Some(raw_value) = field_value.as_str() else {
        return Err(
            SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent {
                field_name: field_name.to_string(),
            },
        );
    };
    let normalized = raw_value.trim();
    if normalized.is_empty() {
        return Err(
            SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent {
                field_name: field_name.to_string(),
            },
        );
    }
    Ok(Some(normalized.to_string()))
}

fn parse_optional_finite_submit_response_f64_field(
    backend_response: &Value,
    field_name: &str,
) -> Result<Option<f64>, SubmitResponseValidationError> {
    let Some(field_value) = backend_response.get(field_name) else {
        return Ok(None);
    };
    let Some(parsed) = field_value.as_f64() else {
        return Err(
            SubmitResponseValidationError::FieldMustBeFiniteNumberWhenPresent {
                field_name: field_name.to_string(),
            },
        );
    };
    if !parsed.is_finite() {
        return Err(
            SubmitResponseValidationError::FieldMustBeFiniteNumberWhenPresent {
                field_name: field_name.to_string(),
            },
        );
    }
    Ok(Some(parsed))
}

fn parse_optional_non_negative_submit_response_u64_field(
    backend_response: &Value,
    field_name: &str,
) -> Result<Option<u64>, SubmitResponseValidationError> {
    let Some(field_value) = backend_response.get(field_name) else {
        return Ok(None);
    };
    let Some(parsed) = field_value.as_u64() else {
        return Err(
            SubmitResponseValidationError::FieldMustBeNonNegativeIntegerWhenPresent {
                field_name: field_name.to_string(),
            },
        );
    };
    Ok(Some(parsed))
}

fn parse_required_non_negative_submit_response_u64_field(
    field_value: Option<&Value>,
    field_name: &str,
) -> Result<u64, SubmitResponseValidationError> {
    let Some(field_value) = field_value else {
        return Err(
            SubmitResponseValidationError::FieldMustBeNonNegativeIntegerWhenPresent {
                field_name: field_name.to_string(),
            },
        );
    };
    let Some(parsed) = field_value.as_u64() else {
        return Err(
            SubmitResponseValidationError::FieldMustBeNonNegativeIntegerWhenPresent {
                field_name: field_name.to_string(),
            },
        );
    };
    Ok(parsed)
}

fn resolve_submit_response_submitted_at(
    backend_response: &Value,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>, SubmitResponseValidationError> {
    let Some(value) = backend_response.get("submitted_at") else {
        return Ok(now);
    };
    let raw = value
        .as_str()
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty())
        .ok_or(SubmitResponseValidationError::SubmittedAtMustBeNonEmptyRfc3339)?;
    parse_rfc3339_utc(raw).ok_or_else(
        || SubmitResponseValidationError::SubmittedAtInvalidRfc3339 {
            raw: raw.to_string(),
        },
    )
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
                "notional_sol={} exceeds adapter max_notional_sol={}",
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
    if let Some(parsed) = value.as_u64() {
        return Ok(Some(parsed));
    }
    Err(Reject::terminal(
        "submit_adapter_invalid_response",
        format!("{} must be non-negative integer when present", field),
    ))
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

fn simulate_http_status_for_reject(reject: &Reject) -> StatusCode {
    if reject.retryable {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

fn submit_http_status_for_reject(reject: &Reject) -> StatusCode {
    if reject.retryable {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

fn parse_socket_addr(value: String) -> Result<SocketAddr> {
    value
        .trim()
        .parse::<SocketAddr>()
        .map_err(|error| anyhow!("invalid COPYBOT_ADAPTER_BIND_ADDR: {}", error))
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
    ensure_secret_file_has_restrictive_permissions(path)?;
    let secret = raw.trim().to_string();
    if secret.is_empty() {
        return Err(anyhow!("secret file is empty path={}", path));
    }
    Ok(secret)
}

fn ensure_secret_file_has_restrictive_permissions(path: &str) -> Result<()> {
    if !secret_file_has_restrictive_permissions(path)? {
        return Err(anyhow!(
            "secret file must use owner-only permissions (0600/0400) path={}",
            path
        ));
    }
    Ok(())
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
        routes.insert(route);
    }
    Ok(routes)
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

fn hmac_payload_bytes(timestamp: i64, ttl_sec: u64, nonce: &str, raw_body: &[u8]) -> Vec<u8> {
    let mut payload = format!("{}\n{}\n{}\n", timestamp, ttl_sec, nonce).into_bytes();
    payload.extend_from_slice(raw_body);
    payload
}

fn build_upstream_hmac_nonce(route: &str, action: UpstreamAction, attempt_idx: usize) -> String {
    let sequence = UPSTREAM_HMAC_NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let timestamp_nanos = Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000);
    format!(
        "upstream:{}:{}:{}:{}:{}",
        route,
        action.as_str(),
        attempt_idx + 1,
        timestamp_nanos,
        sequence
    )
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

fn extract_submit_transport_artifact(
    backend_response: &Value,
) -> Result<SubmitTransportArtifact, SubmitTransportArtifactError> {
    let upstream_tx_signature =
        parse_optional_non_empty_submit_transport_field(backend_response, "tx_signature")?;
    let signed_tx_base64 =
        parse_optional_non_empty_submit_transport_field(backend_response, "signed_tx_base64")?;

    if upstream_tx_signature.is_some() && signed_tx_base64.is_some() {
        return Err(SubmitTransportArtifactError::ConflictingSubmitArtifacts);
    }

    if let Some(value) = upstream_tx_signature {
        validate_signature_like(value.as_str()).map_err(|error| {
            SubmitTransportArtifactError::InvalidUpstreamSignature {
                error: error.to_string(),
            }
        })?;
        return Ok(SubmitTransportArtifact::UpstreamSignature(value));
    }
    if let Some(value) = signed_tx_base64 {
        return Ok(SubmitTransportArtifact::SignedTransactionBase64(value));
    }

    Err(SubmitTransportArtifactError::MissingSubmitArtifact)
}

fn parse_optional_non_empty_submit_transport_field(
    backend_response: &Value,
    field_name: &str,
) -> Result<Option<String>, SubmitTransportArtifactError> {
    let Some(field_value) = backend_response.get(field_name) else {
        return Ok(None);
    };
    let Some(raw_value) = field_value.as_str() else {
        return Err(SubmitTransportArtifactError::InvalidSubmitArtifactType {
            field_name: field_name.to_string(),
        });
    };
    let normalized = raw_value.trim();
    if normalized.is_empty() {
        return Err(SubmitTransportArtifactError::InvalidSubmitArtifactType {
            field_name: field_name.to_string(),
        });
    }
    Ok(Some(normalized.to_string()))
}

fn require_authenticated_mode(
    bearer_token: Option<&str>,
    hmac_key_id: Option<&str>,
    allow_unauthenticated: bool,
) -> Result<()> {
    if allow_unauthenticated {
        return Ok(());
    }
    if bearer_token.is_none() && hmac_key_id.is_none() {
        return Err(anyhow!(
            "adapter auth is required: set COPYBOT_ADAPTER_BEARER_TOKEN or HMAC pair (COPYBOT_ADAPTER_HMAC_KEY_ID/COPYBOT_ADAPTER_HMAC_SECRET); or explicitly set COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED=true for controlled non-production setups"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
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
    use tower::ServiceExt;

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
    fn parse_upstream_outcome_rejects_non_boolean_ok_flag() {
        let payload = json!({
            "status": "ok",
            "ok": "yes",
            "accepted": true
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(!reject.retryable);
                assert_eq!(reject.code, "upstream_invalid_response");
                assert!(reject.detail.contains("upstream ok must be boolean when present"));
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn parse_upstream_outcome_rejects_conflicting_success_and_reject_flags() {
        let payload = json!({
            "status": "ok",
            "accepted": false
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(!reject.retryable);
                assert_eq!(reject.code, "upstream_invalid_response");
                assert!(reject.detail.contains("status=ok conflicts with reject flags"));
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn parse_upstream_outcome_rejects_non_string_code_in_success_envelope() {
        let payload = json!({
            "status": "ok",
            "ok": true,
            "accepted": true,
            "code": 123
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(!reject.retryable);
                assert_eq!(reject.code, "upstream_invalid_response");
                assert!(reject.detail.contains("upstream code must be non-empty string when present"));
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
    fn extract_submit_transport_artifact_rejects_conflicting_fields() {
        let signature = bs58::encode([9u8; 64]).into_string();
        let backend = json!({
            "tx_signature": signature,
            "signed_tx_base64": "AQID"
        });
        let error = extract_submit_transport_artifact(&backend)
            .expect_err("conflicting artifacts must reject");
        assert_eq!(
            error,
            SubmitTransportArtifactError::ConflictingSubmitArtifacts
        );
    }

    #[test]
    fn extract_submit_transport_artifact_rejects_non_string_signed_tx_type() {
        let backend = json!({
            "signed_tx_base64": 123
        });
        let error = extract_submit_transport_artifact(&backend)
            .expect_err("non-string signed_tx_base64 must reject");
        assert!(matches!(
            error,
            SubmitTransportArtifactError::InvalidSubmitArtifactType { field_name }
            if field_name == "signed_tx_base64"
        ));
    }

    #[test]
    fn simulate_response_validation_rejects_signal_id_mismatch() {
        let backend = json!({
            "signal_id": "signal-2"
        });
        let error = validate_simulate_response_identity(
            &backend,
            "request-1",
            "signal-1",
            "buy",
            "Token111",
        )
        .expect_err("signal mismatch must reject");
        assert!(matches!(
            error,
            SimulateResponseValidationError::SignalIdMismatch { .. }
        ));
    }

    #[test]
    fn simulate_response_validation_accepts_case_insensitive_side_and_trimmed_strings() {
        let backend = json!({
            "request_id": " request-1 ",
            "signal_id": " signal-1 ",
            "side": "BUY",
            "token": " Token111 "
        });
        validate_simulate_response_identity(&backend, "request-1", "signal-1", "buy", "Token111")
            .expect("canonical identity should pass");
    }

    #[test]
    fn submit_response_validation_rejects_signal_id_mismatch() {
        let backend = json!({
            "signal_id": "signal-2"
        });
        let error =
            validate_submit_response_extended_identity(&backend, "signal-1", "buy", "Token111")
                .expect_err("signal mismatch must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::SignalIdMismatch { .. }
        ));
    }

    #[test]
    fn submit_response_validation_rejects_non_string_request_id() {
        let backend = json!({
            "request_id": 123
        });
        let error = validate_submit_response_request_identity(&backend, "client-1", "request-1")
            .expect_err("non-string request id must reject");
        assert!(matches!(
            error,
            SubmitResponseValidationError::FieldMustBeNonEmptyStringWhenPresent { .. }
        ));
    }

    #[test]
    fn submit_response_validation_accepts_case_insensitive_side_and_trimmed_strings() {
        let backend = json!({
            "client_order_id": " client-1 ",
            "request_id": " request-1 ",
            "signal_id": " signal-1 ",
            "side": "BUY",
            "token": " Token111 "
        });
        validate_submit_response_request_identity(&backend, "client-1", "request-1")
            .expect("trimmed request identity should pass");
        validate_submit_response_extended_identity(&backend, "signal-1", "buy", "Token111")
            .expect("canonical extended identity should pass");
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
            .contains("requires COPYBOT_ADAPTER_SUBMIT_VERIFY_RPC_URL"));
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
            "COPYBOT_ADAPTER_BEARER_TOKEN",
            Some("inline"),
            "COPYBOT_ADAPTER_BEARER_TOKEN_FILE",
            Some("/tmp/adapter-bearer.secret"),
        )
        .expect_err("inline + file must fail-closed");
        let message = error.to_string();
        assert!(
            message.contains("COPYBOT_ADAPTER_BEARER_TOKEN")
                && message.contains("COPYBOT_ADAPTER_BEARER_TOKEN_FILE")
        );
    }

    #[test]
    fn resolve_secret_source_reads_trimmed_file() {
        let path = write_temp_secret_file(" \nsecret-value\n");
        let resolved = resolve_secret_source(
            "COPYBOT_ADAPTER_BEARER_TOKEN",
            None,
            "COPYBOT_ADAPTER_BEARER_TOKEN_FILE",
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
            "COPYBOT_ADAPTER_HMAC_SECRET",
            None,
            "COPYBOT_ADAPTER_HMAC_SECRET_FILE",
            Some(path.to_str().expect("utf8 path")),
        )
        .expect_err("empty secret file must fail");
        let message = format!("{:#}", error);
        assert!(message.contains("COPYBOT_ADAPTER_HMAC_SECRET_FILE"));
        assert!(message.contains("secret file is empty"));
        cleanup_temp_secret_file(path);
    }

    #[test]
    fn resolve_secret_source_rejects_missing_file() {
        let path = temp_secret_path("missing");
        let error = resolve_secret_source(
            "COPYBOT_ADAPTER_HMAC_SECRET",
            None,
            "COPYBOT_ADAPTER_HMAC_SECRET_FILE",
            Some(path.to_str().expect("utf8 path")),
        )
        .expect_err("missing secret file must fail");
        let message = format!("{:#}", error);
        assert!(message.contains("COPYBOT_ADAPTER_HMAC_SECRET_FILE"));
        assert!(message.contains("secret file not found/readable"));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_secret_source_rejects_broad_permissions_file() {
        use std::os::unix::fs::PermissionsExt;

        let path = write_temp_secret_file("secret");
        let mut perms = stdfs::metadata(&path)
            .expect("stat temp secret")
            .permissions();
        perms.set_mode(0o644);
        stdfs::set_permissions(&path, perms).expect("set relaxed mode");

        let error = resolve_secret_source(
            "COPYBOT_ADAPTER_BEARER_TOKEN",
            None,
            "COPYBOT_ADAPTER_BEARER_TOKEN_FILE",
            Some(path.to_str().expect("utf8 path")),
        )
        .expect_err("broad secret file permissions must fail");
        let message = format!("{:#}", error);
        assert!(message.contains("COPYBOT_ADAPTER_BEARER_TOKEN_FILE"));

        cleanup_temp_secret_file(path);
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
        assert!(require_authenticated_mode(None, None, false).is_err());
        assert!(require_authenticated_mode(Some("token"), None, false).is_ok());
        assert!(require_authenticated_mode(None, Some("kid"), false).is_ok());
        assert!(require_authenticated_mode(None, None, true).is_ok());
    }

    #[tokio::test]
    async fn auth_verifier_rejects_wrong_bearer_token() {
        let verifier = AuthVerifier {
            bearer_token: Some("correct-token".to_string()),
            hmac: None,
            nonce_seen_until_epoch: Arc::new(Mutex::new(HashMap::new())),
            nonce_cache_max_entries: DEFAULT_HMAC_NONCE_CACHE_MAX_ENTRIES,
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
            nonce_cache_max_entries: DEFAULT_HMAC_NONCE_CACHE_MAX_ENTRIES,
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
    async fn auth_verifier_accepts_hmac_signature_over_raw_non_utf8_body() {
        let verifier = AuthVerifier {
            bearer_token: None,
            hmac: Some(HmacConfig {
                key_id: "kid-1".to_string(),
                secret: "test-secret".to_string(),
                ttl_sec: 30,
            }),
            nonce_seen_until_epoch: Arc::new(Mutex::new(HashMap::new())),
            nonce_cache_max_entries: DEFAULT_HMAC_NONCE_CACHE_MAX_ENTRIES,
        };
        let timestamp = Utc::now().timestamp();
        let ttl = 30u64;
        let nonce = "nonce-raw-bytes";
        let raw_body = [0xff, 0x00, 0x41, 0x42];

        let mut payload = format!("{}\n{}\n{}\n", timestamp, ttl, nonce).into_bytes();
        payload.extend_from_slice(&raw_body);
        let signature = compute_hmac_signature_hex(b"test-secret", payload.as_slice())
            .expect("hmac signature must compute");

        let mut headers = HeaderMap::new();
        headers.insert("x-copybot-key-id", HeaderValue::from_static("kid-1"));
        headers.insert(
            "x-copybot-signature-alg",
            HeaderValue::from_static("hmac-sha256-v1"),
        );
        headers.insert(
            "x-copybot-timestamp",
            HeaderValue::from_str(timestamp.to_string().as_str()).expect("timestamp header"),
        );
        headers.insert("x-copybot-auth-ttl-sec", HeaderValue::from_static("30"));
        headers.insert(
            "x-copybot-nonce",
            HeaderValue::from_static("nonce-raw-bytes"),
        );
        headers.insert(
            "x-copybot-signature",
            HeaderValue::from_str(signature.as_str()).expect("signature header"),
        );

        verifier
            .verify(&headers, &raw_body)
            .await
            .expect("raw-byte hmac signature should pass");
    }

    #[tokio::test]
    async fn auth_verifier_rejects_hmac_signature_computed_over_lossy_body() {
        let verifier = AuthVerifier {
            bearer_token: None,
            hmac: Some(HmacConfig {
                key_id: "kid-1".to_string(),
                secret: "test-secret".to_string(),
                ttl_sec: 30,
            }),
            nonce_seen_until_epoch: Arc::new(Mutex::new(HashMap::new())),
            nonce_cache_max_entries: DEFAULT_HMAC_NONCE_CACHE_MAX_ENTRIES,
        };
        let timestamp = Utc::now().timestamp();
        let ttl = 30u64;
        let nonce = "nonce-lossy-body";
        let raw_body = [0xff, 0x00, 0x41, 0x42];

        let lossy_payload = format!(
            "{}\n{}\n{}\n{}",
            timestamp,
            ttl,
            nonce,
            String::from_utf8_lossy(&raw_body)
        );
        let lossy_signature = compute_hmac_signature_hex(b"test-secret", lossy_payload.as_bytes())
            .expect("lossy hmac signature must compute");

        let mut headers = HeaderMap::new();
        headers.insert("x-copybot-key-id", HeaderValue::from_static("kid-1"));
        headers.insert(
            "x-copybot-signature-alg",
            HeaderValue::from_static("hmac-sha256-v1"),
        );
        headers.insert(
            "x-copybot-timestamp",
            HeaderValue::from_str(timestamp.to_string().as_str()).expect("timestamp header"),
        );
        headers.insert("x-copybot-auth-ttl-sec", HeaderValue::from_static("30"));
        headers.insert(
            "x-copybot-nonce",
            HeaderValue::from_static("nonce-lossy-body"),
        );
        headers.insert(
            "x-copybot-signature",
            HeaderValue::from_str(lossy_signature.as_str()).expect("signature header"),
        );

        let reject = verifier
            .verify(&headers, &raw_body)
            .await
            .expect_err("lossy-body signature must fail against raw-byte verifier");
        assert_eq!(reject.code, "hmac_invalid");
        assert!(reject.detail.contains("HMAC signature mismatch"));
    }

    #[tokio::test]
    async fn auth_verifier_does_not_poison_nonce_cache_after_bad_signature() {
        let verifier = AuthVerifier {
            bearer_token: None,
            hmac: Some(HmacConfig {
                key_id: "kid-1".to_string(),
                secret: "test-secret".to_string(),
                ttl_sec: 30,
            }),
            nonce_seen_until_epoch: Arc::new(Mutex::new(HashMap::new())),
            nonce_cache_max_entries: DEFAULT_HMAC_NONCE_CACHE_MAX_ENTRIES,
        };
        let timestamp = Utc::now().timestamp();
        let ttl = 30u64;
        let nonce = "nonce-retry-after-bad-signature";
        let raw_body = br#"{"ping":true}"#;

        let mut payload = format!("{}\n{}\n{}\n", timestamp, ttl, nonce).into_bytes();
        payload.extend_from_slice(raw_body);
        let valid_signature = compute_hmac_signature_hex(b"test-secret", payload.as_slice())
            .expect("hmac signature must compute");

        let mut bad_headers = HeaderMap::new();
        bad_headers.insert("x-copybot-key-id", HeaderValue::from_static("kid-1"));
        bad_headers.insert(
            "x-copybot-signature-alg",
            HeaderValue::from_static("hmac-sha256-v1"),
        );
        bad_headers.insert(
            "x-copybot-timestamp",
            HeaderValue::from_str(timestamp.to_string().as_str()).expect("timestamp header"),
        );
        bad_headers.insert("x-copybot-auth-ttl-sec", HeaderValue::from_static("30"));
        bad_headers.insert(
            "x-copybot-nonce",
            HeaderValue::from_static("nonce-retry-after-bad-signature"),
        );
        bad_headers.insert("x-copybot-signature", HeaderValue::from_static("deadbeef"));

        let reject = verifier
            .verify(&bad_headers, raw_body)
            .await
            .expect_err("bad signature must fail");
        assert_eq!(reject.code, "hmac_invalid");

        let mut good_headers = HeaderMap::new();
        good_headers.insert("x-copybot-key-id", HeaderValue::from_static("kid-1"));
        good_headers.insert(
            "x-copybot-signature-alg",
            HeaderValue::from_static("hmac-sha256-v1"),
        );
        good_headers.insert(
            "x-copybot-timestamp",
            HeaderValue::from_str(timestamp.to_string().as_str()).expect("timestamp header"),
        );
        good_headers.insert("x-copybot-auth-ttl-sec", HeaderValue::from_static("30"));
        good_headers.insert(
            "x-copybot-nonce",
            HeaderValue::from_static("nonce-retry-after-bad-signature"),
        );
        good_headers.insert(
            "x-copybot-signature",
            HeaderValue::from_str(valid_signature.as_str()).expect("signature header"),
        );

        verifier
            .verify(&good_headers, raw_body)
            .await
            .expect("valid retry should succeed because bad signature must not poison nonce cache");
    }

    #[tokio::test]
    async fn auth_verifier_rejects_when_nonce_cache_capacity_is_reached() {
        let verifier = AuthVerifier {
            bearer_token: None,
            hmac: Some(HmacConfig {
                key_id: "kid-1".to_string(),
                secret: "test-secret".to_string(),
                ttl_sec: 30,
            }),
            nonce_seen_until_epoch: Arc::new(Mutex::new(HashMap::new())),
            nonce_cache_max_entries: 1,
        };
        let timestamp = Utc::now().timestamp();
        let raw_body = br#"{"ping":true}"#;

        for (nonce, should_pass) in [("nonce-cap-1", true), ("nonce-cap-2", false)] {
            let mut payload = format!("{}\n{}\n{}\n", timestamp, 30u64, nonce).into_bytes();
            payload.extend_from_slice(raw_body);
            let signature = compute_hmac_signature_hex(b"test-secret", payload.as_slice())
                .expect("hmac signature must compute");
            let mut headers = HeaderMap::new();
            headers.insert("x-copybot-key-id", HeaderValue::from_static("kid-1"));
            headers.insert(
                "x-copybot-signature-alg",
                HeaderValue::from_static("hmac-sha256-v1"),
            );
            headers.insert(
                "x-copybot-timestamp",
                HeaderValue::from_str(timestamp.to_string().as_str()).expect("timestamp header"),
            );
            headers.insert("x-copybot-auth-ttl-sec", HeaderValue::from_static("30"));
            headers.insert(
                "x-copybot-nonce",
                HeaderValue::from_str(nonce).expect("nonce header"),
            );
            headers.insert(
                "x-copybot-signature",
                HeaderValue::from_str(signature.as_str()).expect("signature header"),
            );

            if should_pass {
                verifier
                    .verify(&headers, raw_body)
                    .await
                    .expect("first nonce should fit into cache");
            } else {
                let reject = verifier
                    .verify(&headers, raw_body)
                    .await
                    .expect_err("second nonce should overflow capped cache");
                assert!(reject.retryable);
                assert_eq!(reject.code, "hmac_replay_cache_overflow");
            }
        }
    }

    #[tokio::test]
    async fn router_rejects_oversized_request_body_before_handler() {
        let app = build_router(test_state("http://127.0.0.1:1/upstream"));
        let oversized_payload = format!(
            r#"{{"padding":"{}"}}"#,
            "x".repeat(DEFAULT_MAX_REQUEST_BODY_BYTES + 1024)
        );
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/simulate")
            .header("content-type", "application/json")
            .body(Body::from(oversized_payload))
            .expect("request");
        let response = app
            .oneshot(request)
            .await
            .expect("router should produce response");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn simulate_status_is_503_for_retryable_reject() {
        let reject = Reject::retryable("busy", "upstream temporary issue");
        assert_eq!(
            simulate_http_status_for_reject(&reject),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let reject = Reject::terminal("invalid", "bad request");
        assert_eq!(simulate_http_status_for_reject(&reject), StatusCode::OK);
    }

    #[test]
    fn submit_status_is_503_for_retryable_reject() {
        let reject = Reject::retryable("busy", "upstream temporary issue");
        assert_eq!(
            submit_http_status_for_reject(&reject),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let reject = Reject::terminal("invalid", "bad request");
        assert_eq!(submit_http_status_for_reject(&reject), StatusCode::OK);
    }

    #[tokio::test]
    async fn forward_to_upstream_treats_plaintext_503_as_retryable() {
        let Some((url, handle)) =
            spawn_one_shot_upstream_raw(503, "text/plain", "temporary upstream outage")
        else {
            return;
        };
        let state = test_state(url.as_str());
        let reject = forward_to_upstream(&state, "rpc", UpstreamAction::Simulate, b"{}")
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
        let body = forward_to_upstream(&state, "rpc", UpstreamAction::Simulate, b"{}")
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
        let body = forward_to_upstream(&state, "rpc", UpstreamAction::Submit, b"{}")
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
        let reject = forward_to_upstream(&state, "rpc", UpstreamAction::Submit, b"{}")
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

        let body = forward_to_upstream(&state, "rpc", UpstreamAction::Submit, b"{}")
            .await
            .expect("fallback with dedicated token should succeed");
        assert_eq!(body.get("status").and_then(Value::as_str), Some("ok"));
        let _ = primary_handle.join();
        let _ = fallback_handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_rejects_oversized_success_body() {
        let oversized_body = format!(
            r#"{{"status":"ok","accepted":true,"payload":"{}"}}"#,
            "x".repeat(70_000)
        );
        let Some((primary_url, primary_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", oversized_body.as_str())
        else {
            return;
        };

        let state =
            test_state_with_backends(primary_url.as_str(), None, primary_url.as_str(), None);
        let reject = forward_to_upstream(&state, "rpc", UpstreamAction::Submit, b"{}")
            .await
            .expect_err("oversized upstream body must fail closed");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_response_too_large");
        let _ = primary_handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_adds_upstream_hmac_headers() {
        let key_id = "executor-hmac-k1";
        let secret = "executor-hmac-secret";
        let ttl_sec = 30u64;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_expect_hmac(key_id, secret, ttl_sec, None)
        else {
            return;
        };

        let mut state = test_state(upstream_url.as_str());
        state.config.upstream_hmac = Some(UpstreamHmacConfig {
            key_id: key_id.to_string(),
            secret: secret.to_string(),
            ttl_sec,
        });

        let body = forward_to_upstream(
            &state,
            "rpc",
            UpstreamAction::Submit,
            b"{\"action\":\"submit\"}",
        )
        .await
        .expect("upstream hmac headers should be accepted");
        assert_eq!(body.get("status").and_then(Value::as_str), Some("ok"));
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_signs_hmac_over_raw_non_utf8_body() {
        let key_id = "executor-hmac-k1";
        let secret = "executor-hmac-secret";
        let ttl_sec = 30u64;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_expect_hmac(key_id, secret, ttl_sec, None)
        else {
            return;
        };

        let mut state = test_state(upstream_url.as_str());
        state.config.upstream_hmac = Some(UpstreamHmacConfig {
            key_id: key_id.to_string(),
            secret: secret.to_string(),
            ttl_sec,
        });
        let raw_body = [0xff, 0x00, 0x41, 0x42];
        let body = forward_to_upstream(&state, "rpc", UpstreamAction::Simulate, &raw_body)
            .await
            .expect("raw-byte hmac payload should be accepted");
        assert_eq!(body.get("status").and_then(Value::as_str), Some("ok"));
        let _ = upstream_handle.join();
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

        let signature = send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str())
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

        let reject = send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str())
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

        let signature = send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str())
            .await
            .expect("fallback send RPC with dedicated auth token should succeed");
        assert_eq!(signature, expected_signature);
        let _ = primary_handle.join();
        let _ = fallback_handle.join();
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

        let signature = send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str())
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

        let reject = send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str())
            .await
            .expect_err("unknown send RPC error payload should be terminal");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "send_rpc_error_payload_terminal");
        let _ = primary_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_oversized_success_body() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([36u8; 64]);
        let oversized_body = format!(r#"{{"jsonrpc":"2.0","result":"{}"}}"#, "x".repeat(70_000));
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", oversized_body.as_str())
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

        let reject = send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str())
            .await
            .expect_err("oversized send-rpc body must fail closed");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "send_rpc_response_too_large");
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_canonical_transaction_with_trailing_garbage() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([37u8; 64]);
        let mut tx_bytes = BASE64_STANDARD
            .decode(signed_tx_base64.as_bytes())
            .expect("decode valid canonical signed tx");
        tx_bytes.extend_from_slice(b"TRAILING_GARBAGE");
        let tampered_base64 = BASE64_STANDARD.encode(tx_bytes);

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some("http://127.0.0.1:1/send-rpc".to_string());
        } else {
            panic!("rpc backend must exist");
        }

        let reject = send_signed_transaction_via_rpc(&state, "rpc", tampered_base64.as_str())
            .await
            .expect_err("trailing garbage must reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject.detail.contains("transaction bincode decode failed"),
            "detail={}",
            reject.detail
        );
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_canonical_transaction_with_invalid_signature_bytes(
    ) {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([38u8; 64]);
        let tx_bytes = BASE64_STANDARD
            .decode(signed_tx_base64.as_bytes())
            .expect("decode valid canonical signed tx");
        let mut transaction: VersionedTransaction =
            bincode::deserialize(tx_bytes.as_slice()).expect("decode valid canonical tx");
        transaction.signatures[0] = Signature::from([99u8; 64]);
        let tampered_base64 = BASE64_STANDARD
            .encode(bincode::serialize(&transaction).expect("serialize tampered canonical tx"));

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some("http://127.0.0.1:1/send-rpc".to_string());
        } else {
            panic!("rpc backend must exist");
        }

        let reject = send_signed_transaction_via_rpc(&state, "rpc", tampered_base64.as_str())
            .await
            .expect_err("invalid signature bytes must reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("contains invalid required signature bytes"),
            "detail={}",
            reject.detail
        );
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_signed_transaction_without_configured_signer()
    {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature_and_signer([39u8; 64], [8u8; 32]);

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some("http://127.0.0.1:1/send-rpc".to_string());
        } else {
            panic!("rpc backend must exist");
        }

        let reject = send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str())
            .await
            .expect_err("missing configured signer must reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("does not require configured adapter signer"),
            "detail={}",
            reject.detail
        );
    }

    #[tokio::test]
    async fn handle_simulate_returns_canonical_identity_echoes_on_success() {
        let upstream_body = r#"{"status":"ok","ok":true,"accepted":true,"request_id":" request-1 ","signal_id":" signal-1 ","side":"BUY","token":" 11111111111111111111111111111111 "}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "action": "simulate",
            "dry_run": true,
            "contract_version": "v1",
            "request_id": " request-1 ",
            "signal_id": " signal-1 ",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");
        let request: SimulateRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize simulate request");

        let response = handle_simulate(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("simulate success should preserve canonical identity echoes");
        assert_eq!(response.get("status").and_then(Value::as_str), Some("ok"));
        assert_eq!(
            response.get("request_id").and_then(Value::as_str),
            Some("request-1")
        );
        assert_eq!(
            response.get("signal_id").and_then(Value::as_str),
            Some("signal-1")
        );
        assert_eq!(response.get("side").and_then(Value::as_str), Some("buy"));
        assert_eq!(
            response.get("token").and_then(Value::as_str),
            Some("11111111111111111111111111111111")
        );
        let _ = upstream_handle.join();
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
    async fn handle_submit_rejects_conflicting_submit_transport_artifacts() {
        let signature = bs58::encode([41u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}","signed_tx_base64":"AQID"}}"#,
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

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("conflicting submit artifacts must reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("must not include both tx_signature and signed_tx_base64"),
            "detail={}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_non_string_signed_tx_base64_field() {
        let upstream_body = r#"{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":123}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
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

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("invalid signed_tx_base64 type must reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("upstream signed_tx_base64 must be a non-empty string when present"),
            "detail={}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_upstream_signal_id_mismatch() {
        let signature = bs58::encode([42u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}","signal_id":"signal-2"}}"#,
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

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("mismatched upstream signal_id should reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_signal_id_mismatch");
        assert!(
            reject
                .detail
                .contains("signal_id=signal-2 does not match expected signal_id=signal-1"),
            "detail={}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_signed_tx_base64_signal_id_mismatch_before_send_rpc() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([45u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":"{}","signal_id":"signal-2"}}"#,
            signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let mut state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some("http://127.0.0.1:1/send-rpc-should-not-run".to_string());
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

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("mismatched upstream signal_id should reject before send_rpc");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_signal_id_mismatch");
        assert!(
            reject
                .detail
                .contains("signal_id=signal-2 does not match expected signal_id=signal-1"),
            "detail={}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_accepts_case_insensitive_upstream_side_echo() {
        let signature = bs58::encode([43u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}","request_id":" request-1 ","client_order_id":" client-order-1 ","signal_id":" signal-1 ","side":"BUY","token":" 11111111111111111111111111111111 "}}"#,
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
            .expect("case-insensitive side echo should pass");
        assert_eq!(response.get("status").and_then(Value::as_str), Some("ok"));
        assert_eq!(
            response.get("signal_id").and_then(Value::as_str),
            Some("signal-1")
        );
        assert_eq!(response.get("side").and_then(Value::as_str), Some("buy"));
        assert_eq!(
            response.get("token").and_then(Value::as_str),
            Some("11111111111111111111111111111111")
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_non_string_request_id_field() {
        let signature = bs58::encode([44u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}","request_id":123}}"#,
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

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("non-string request_id should reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("upstream request_id must be non-empty string when present"),
            "detail={}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_non_boolean_upstream_ok_before_send_rpc() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([45u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":"yes","accepted":true,"signed_tx_base64":"{}"}}"#,
            signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let mut state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some("http://127.0.0.1:1/send-rpc-should-not-run".to_string());
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

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("non-boolean upstream ok must reject before send_rpc");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_response");
        assert!(
            reject
                .detail
                .contains("upstream ok must be boolean when present"),
            "detail={}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_non_boolean_upstream_retryable_before_send_rpc() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([58u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"retryable":"yes","signed_tx_base64":"{}"}}"#,
            signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let mut state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some("http://127.0.0.1:1/send-rpc-should-not-run".to_string());
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

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("non-boolean upstream retryable must reject before send_rpc");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_response");
        assert!(
            reject
                .detail
                .contains("upstream retryable must be boolean when present"),
            "detail={}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_network_fee_mismatch_before_send_rpc() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([46u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":"{}","base_fee_lamports":5000,"priority_fee_lamports":300,"network_fee_lamports":9999}}"#,
            signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let mut state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some("http://127.0.0.1:1/send-rpc-should-not-run".to_string());
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

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("network_fee mismatch must reject before send_rpc");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("network_fee_lamports=9999 does not match base+priority=5300"),
            "detail={}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_null_network_fee_before_send_rpc() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([47u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":"{}","network_fee_lamports":null}}"#,
            signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let mut state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some("http://127.0.0.1:1/send-rpc-should-not-run".to_string());
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

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("null fee field must reject before send_rpc");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("network_fee_lamports must be non-negative integer when present"),
            "detail={}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_slippage_bps_mismatch_before_send_rpc() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([48u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":"{}","slippage_bps":12.0}}"#,
            signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let mut state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some("http://127.0.0.1:1/send-rpc-should-not-run".to_string());
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

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("slippage mismatch must reject before send_rpc");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_slippage_bps_mismatch");
        assert!(
            reject
                .detail
                .contains("upstream slippage_bps=12 does not match expected slippage_bps=10"),
            "detail={}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_compute_budget_mismatch_before_send_rpc() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([49u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":"{}","compute_budget":{{"cu_limit":350000,"cu_price_micro_lamports":1000}}}}"#,
            signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let mut state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some("http://127.0.0.1:1/send-rpc-should-not-run".to_string());
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

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("compute_budget mismatch must reject before send_rpc");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_compute_budget_mismatch");
        assert!(
            reject
                .detail
                .contains("upstream compute_budget.cu_limit=350000 does not match expected compute_budget.cu_limit=300000"),
            "detail={}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_tip_lamports_mismatch_before_send_rpc() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([50u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":"{}","tip_lamports":1}}"#,
            signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let mut state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some("http://127.0.0.1:1/send-rpc-should-not-run".to_string());
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

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("tip_lamports mismatch must reject before send_rpc");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_tip_lamports_mismatch");
        assert!(
            reject
                .detail
                .contains("upstream tip_lamports=1 does not match expected tip_lamports=0"),
            "detail={}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_accepts_matching_upstream_policy_echoes() {
        let signature = bs58::encode([51u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}","slippage_bps":10.0,"tip_lamports":0,"compute_budget":{{"cu_limit":300000,"cu_price_micro_lamports":1000}}}}"#,
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
            .expect("matching policy echoes should pass");
        assert_eq!(response.get("status").and_then(Value::as_str), Some("ok"));
        assert_eq!(
            response.get("slippage_bps").and_then(Value::as_f64),
            Some(10.0)
        );
        assert_eq!(
            response.get("tip_lamports").and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(
            response
                .get("compute_budget")
                .and_then(|value| value.get("cu_limit"))
                .and_then(Value::as_u64),
            Some(300_000)
        );
        assert_eq!(
            response
                .get("compute_budget")
                .and_then(|value| value.get("cu_price_micro_lamports"))
                .and_then(Value::as_u64),
            Some(1_000)
        );
        let _ = upstream_handle.join();
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
        let result = verify_submitted_signature_visibility(&state, "rpc", signature.as_str()).await;
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
        let result = verify_submitted_signature_visibility(&state, "rpc", signature.as_str()).await;
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
        let reject = verify_submitted_signature_visibility(&state, "rpc", signature.as_str())
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
        let reject = verify_submitted_signature_visibility(&state, "rpc", signature.as_str())
            .await
            .expect_err("on-chain err must be terminal reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_submit_failed_onchain");
        let _ = handle.join();
    }

    #[tokio::test]
    async fn verify_submit_signature_returns_unseen_when_response_body_too_large() {
        let signature = bs58::encode([13u8; 64]).into_string();
        let oversized_body = format!(
            r#"{{"jsonrpc":"2.0","result":{{"value":[{{"confirmationStatus":"{}","padding":"{}"}}]}}}}"#,
            "confirmed",
            "x".repeat(70_000)
        );
        let Some((verify_url, handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", oversized_body.as_str())
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
        let result = verify_submitted_signature_visibility(&state, "rpc", signature.as_str()).await;
        let Ok(SubmitSignatureVerification::Unseen { reason }) = result else {
            panic!("expected unseen verification result");
        };
        assert!(reason.contains("response_too_large"), "reason={}", reason);
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
        let config = AdapterConfig {
            bind_addr: "127.0.0.1:8080".parse().expect("valid bind"),
            contract_version: "v1".to_string(),
            signer_pubkey: test_adapter_signer_keypair().pubkey().to_string(),
            route_allowlist,
            route_backends,
            bearer_token: None,
            hmac_key_id: None,
            hmac_secret: None,
            hmac_ttl_sec: 30,
            upstream_hmac: None,
            request_timeout_ms: 2_000,
            max_notional_sol: 10.0,
            allow_nonzero_tip: true,
            submit_signature_verify: None,
        };
        let auth = Arc::new(AuthVerifier::new(&config));
        let http = Client::builder()
            .timeout(Duration::from_millis(2_000))
            .build()
            .expect("http client");
        AppState { config, http, auth }
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
                    if status == 200 {
                        "application/json"
                    } else {
                        "text/plain"
                    },
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        Some((format!("http://{}/upstream", addr), handle))
    }

    fn spawn_one_shot_upstream_expect_hmac(
        expected_key_id: &str,
        expected_secret: &str,
        expected_ttl_sec: u64,
        expected_bearer: Option<&str>,
    ) -> Option<(String, thread::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let expected_key_id = expected_key_id.to_string();
        let expected_secret = expected_secret.to_string();
        let expected_bearer = expected_bearer.map(ToString::to_string);
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request_buf = [0u8; 8192];
                let bytes_read = stream.read(&mut request_buf).unwrap_or(0);
                let request_bytes = &request_buf[..bytes_read];

                let header_end = request_bytes
                    .windows(4)
                    .position(|chunk| chunk == b"\r\n\r\n");
                let mut headers = HashMap::new();
                let mut body_bytes: Vec<u8> = Vec::new();

                if let Some(header_end) = header_end {
                    let header_text = String::from_utf8_lossy(&request_bytes[..header_end]);
                    for line in header_text.lines().skip(1) {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        let Some((name, value)) = trimmed.split_once(':') else {
                            continue;
                        };
                        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
                    }

                    let body_start = header_end + 4;
                    let declared_len = headers
                        .get("content-length")
                        .and_then(|raw| raw.parse::<usize>().ok())
                        .unwrap_or(0);
                    let available_len = request_bytes.len().saturating_sub(body_start);
                    let body_len = available_len.min(declared_len);
                    body_bytes = request_bytes[body_start..body_start + body_len].to_vec();
                }

                let bearer_ok = match expected_bearer.as_deref() {
                    Some(expected_token) => headers
                        .get("authorization")
                        .and_then(|value| {
                            if value.len() < "bearer ".len()
                                || !value[.."bearer ".len()].eq_ignore_ascii_case("bearer ")
                            {
                                return None;
                            }
                            Some(value["bearer ".len()..].to_string())
                        })
                        .map(|provided| provided == expected_token)
                        .unwrap_or(false),
                    None => true,
                };

                let hmac_ok = headers
                    .get("x-copybot-key-id")
                    .map(|value| value.as_str() == expected_key_id.as_str())
                    .unwrap_or(false)
                    && headers
                        .get("x-copybot-signature-alg")
                        .map(|value| value == "hmac-sha256-v1")
                        .unwrap_or(false)
                    && headers
                        .get("x-copybot-auth-ttl-sec")
                        .and_then(|value| value.parse::<u64>().ok())
                        .map(|value| value == expected_ttl_sec)
                        .unwrap_or(false)
                    && headers
                        .get("x-copybot-timestamp")
                        .and_then(|value| value.parse::<i64>().ok())
                        .and_then(|timestamp| {
                            let nonce = headers.get("x-copybot-nonce")?;
                            if nonce.is_empty() || nonce.len() > 128 {
                                return Some(false);
                            }
                            let provided_signature = headers.get("x-copybot-signature")?;
                            let payload = hmac_payload_bytes(
                                timestamp,
                                expected_ttl_sec,
                                nonce.as_str(),
                                body_bytes.as_slice(),
                            );
                            let expected_signature = compute_hmac_signature_hex(
                                expected_secret.as_bytes(),
                                payload.as_slice(),
                            )
                            .ok()?;
                            Some(constant_time_eq(
                                provided_signature.as_bytes(),
                                expected_signature.as_bytes(),
                            ))
                        })
                        .unwrap_or(false);

                let authorized = bearer_ok && hmac_ok;
                let (status, reason, body) = if authorized {
                    (
                        200u16,
                        "OK",
                        "{\"status\":\"ok\",\"ok\":true,\"accepted\":true}",
                    )
                } else {
                    (401u16, "Unauthorized", "missing or invalid auth headers")
                };
                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    reason,
                    if status == 200 {
                        "application/json"
                    } else {
                        "text/plain"
                    },
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
                    if status == 200 {
                        "application/json"
                    } else {
                        "text/plain"
                    },
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
        test_signed_tx_base64_with_signature_and_signer(signature, TEST_ADAPTER_SIGNER_SECRET)
    }

    fn test_signed_tx_base64_with_signature_and_signer(
        signature_seed: [u8; 64],
        signer_secret: [u8; 32],
    ) -> (String, String) {
        let signer = Keypair::new_from_array(signer_secret);
        let message = LegacyMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![signer.pubkey(), Address::from([9u8; 32])],
            recent_blockhash: Hash::new_from_array(
                signature_seed[32..64]
                    .try_into()
                    .expect("recent blockhash seed slice"),
            ),
            instructions: vec![CompiledInstruction::new_from_raw_parts(
                1,
                signature_seed[0..8].to_vec(),
                vec![0],
            )],
        };
        let versioned_message = VersionedMessage::Legacy(message);
        let signature: Signature = signer.sign_message(&versioned_message.serialize());
        let tx_bytes = bincode::serialize(&VersionedTransaction {
            signatures: vec![signature],
            message: versioned_message,
        })
        .expect("serialize test signed transaction");
        (BASE64_STANDARD.encode(tx_bytes), signature.to_string())
    }

    fn test_adapter_signer_keypair() -> Keypair {
        Keypair::new_from_array(TEST_ADAPTER_SIGNER_SECRET)
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

    fn cleanup_temp_secret_file(path: PathBuf) {
        let _ = stdfs::remove_file(path);
    }
}
