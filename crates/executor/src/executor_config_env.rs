use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use std::{collections::HashMap, env};

use crate::auth_mode::{require_authenticated_mode, validate_hmac_auth_config};
use crate::contract_version::parse_contract_version;
use crate::env_parsing::{
    non_empty_env, optional_non_empty_env, parse_bool_env, parse_f64_env, parse_socket_addr_str,
    parse_u64_env,
};
use crate::http_utils::{endpoint_identity, validate_endpoint_url};
use crate::key_validation::validate_pubkey_like;
use crate::route_allowlist::{parse_route_allowlist, validate_fastlane_route_policy};
use crate::route_backend::RouteBackend;
use crate::secret_source::resolve_secret_source;
use crate::signer_source::resolve_signer_source_config;
use crate::submit_budget::{default_submit_total_budget_ms, min_claim_ttl_sec_for_submit_path};
use crate::submit_verify_config::parse_submit_signature_verify_config;
use crate::{
    ExecutorConfig, DEFAULT_BIND_ADDR, DEFAULT_HMAC_NONCE_CACHE_MAX_ENTRIES,
    DEFAULT_IDEMPOTENCY_CLAIM_TTL_SEC, DEFAULT_IDEMPOTENCY_RESPONSE_RETENTION_SEC,
    DEFAULT_MAX_NOTIONAL_SOL, DEFAULT_TIMEOUT_MS,
};
use crate::idempotency::{
    response_cleanup_interval_sec_for_retention, DEFAULT_RESPONSE_CLEANUP_DELETE_BATCH_SIZE,
    DEFAULT_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN,
};
use crate::idempotency_cleanup_worker::{
    response_cleanup_worker_tick_sec, MAX_RESPONSE_CLEANUP_WORKER_TICK_SEC,
    MIN_RESPONSE_CLEANUP_WORKER_TICK_SEC,
};

const MAX_RESPONSE_CLEANUP_BATCH_SIZE: u64 = 1_000_000;
const MAX_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN: u64 = 10_000;
const MAX_RESPONSE_CLEANUP_ROWS_PER_RUN: u64 = 200_000;

impl ExecutorConfig {
    pub(crate) fn from_env() -> Result<Self> {
        let bind_addr_raw =
            env::var("COPYBOT_EXECUTOR_BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string());
        let bind_addr =
            parse_socket_addr_str("COPYBOT_EXECUTOR_BIND_ADDR", bind_addr_raw.as_str())?;

        let contract_version_raw =
            env::var("COPYBOT_EXECUTOR_CONTRACT_VERSION").unwrap_or_else(|_| "v1".to_string());
        let contract_version = parse_contract_version(contract_version_raw.as_str())?;

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
            parse_bool_env("COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED", false)?;

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
        let hmac_nonce_cache_max_entries = parse_u64_env(
            "COPYBOT_EXECUTOR_HMAC_NONCE_CACHE_MAX_ENTRIES",
            DEFAULT_HMAC_NONCE_CACHE_MAX_ENTRIES,
        )?;
        if hmac_nonce_cache_max_entries == 0 {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_HMAC_NONCE_CACHE_MAX_ENTRIES must be > 0"
            ));
        }
        let allow_unauthenticated =
            parse_bool_env("COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED", false)?;
        validate_hmac_auth_config(
            hmac_key_id.as_deref(),
            hmac_secret.as_deref(),
            hmac_ttl_sec,
        )?;
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
        let idempotency_response_retention_sec = parse_u64_env(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_RETENTION_SEC",
            DEFAULT_IDEMPOTENCY_RESPONSE_RETENTION_SEC,
        )?;
        if idempotency_response_retention_sec == 0 {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_RETENTION_SEC must be > 0"
            ));
        }
        validate_response_retention_cutoff(idempotency_response_retention_sec)?;
        let idempotency_response_cleanup_batch_size = parse_u64_env(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_BATCH_SIZE",
            DEFAULT_RESPONSE_CLEANUP_DELETE_BATCH_SIZE,
        )?;
        if idempotency_response_cleanup_batch_size == 0 {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_BATCH_SIZE must be > 0"
            ));
        }
        let idempotency_response_cleanup_max_batches_per_run = parse_u64_env(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN",
            DEFAULT_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN,
        )?;
        if idempotency_response_cleanup_max_batches_per_run == 0 {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN must be > 0"
            ));
        }
        validate_response_cleanup_tuning(
            idempotency_response_cleanup_batch_size,
            idempotency_response_cleanup_max_batches_per_run,
        )?;
        let idempotency_response_cleanup_worker_tick_sec = parse_u64_env(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_WORKER_TICK_SEC",
            response_cleanup_worker_tick_sec(idempotency_response_retention_sec),
        )?;
        validate_response_cleanup_worker_tick_sec(idempotency_response_cleanup_worker_tick_sec)?;
        validate_response_cleanup_worker_cadence(
            idempotency_response_cleanup_worker_tick_sec,
            idempotency_response_retention_sec,
        )?;
        let max_notional_sol = parse_f64_env(
            "COPYBOT_EXECUTOR_MAX_NOTIONAL_SOL",
            DEFAULT_MAX_NOTIONAL_SOL,
        )?;
        if !max_notional_sol.is_finite() || max_notional_sol <= 0.0 {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_MAX_NOTIONAL_SOL must be finite and > 0"
            ));
        }
        let allow_nonzero_tip = parse_bool_env("COPYBOT_EXECUTOR_ALLOW_NONZERO_TIP", true)?;
        let submit_signature_verify = parse_submit_signature_verify_config()?;
        let min_claim_ttl_sec = min_claim_ttl_sec_for_submit_path(
            request_timeout_ms,
            submit_total_budget_ms,
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
            hmac_nonce_cache_max_entries,
            request_timeout_ms,
            submit_total_budget_ms,
            idempotency_db_path,
            idempotency_claim_ttl_sec,
            idempotency_response_retention_sec,
            idempotency_response_cleanup_batch_size,
            idempotency_response_cleanup_max_batches_per_run,
            idempotency_response_cleanup_worker_tick_sec,
            max_notional_sol,
            allow_nonzero_tip,
            submit_signature_verify,
        })
    }
}

fn validate_response_retention_cutoff(idempotency_response_retention_sec: u64) -> Result<()> {
    let retention_i64 = i64::try_from(idempotency_response_retention_sec).map_err(|_| {
        anyhow!(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_RETENTION_SEC must fit into signed 64-bit seconds"
        )
    })?;
    let cutoff_unix = Utc::now().timestamp().saturating_sub(retention_i64.max(1));
    if DateTime::<Utc>::from_timestamp(cutoff_unix, 0).is_none() {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_RETENTION_SEC is out of supported timestamp range"
        ));
    }
    Ok(())
}

fn validate_response_cleanup_tuning(
    idempotency_response_cleanup_batch_size: u64,
    idempotency_response_cleanup_max_batches_per_run: u64,
) -> Result<()> {
    if idempotency_response_cleanup_batch_size > MAX_RESPONSE_CLEANUP_BATCH_SIZE {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_BATCH_SIZE must be <= {}",
            MAX_RESPONSE_CLEANUP_BATCH_SIZE
        ));
    }
    i64::try_from(idempotency_response_cleanup_batch_size).map_err(|_| {
        anyhow!(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_BATCH_SIZE must fit into signed 64-bit range"
        )
    })?;
    if idempotency_response_cleanup_max_batches_per_run > MAX_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN
    {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN must be <= {}",
            MAX_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN
        ));
    }
    usize::try_from(idempotency_response_cleanup_max_batches_per_run).map_err(|_| {
        anyhow!(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN must fit into platform usize range"
        )
    })?;
    let rows_per_run = idempotency_response_cleanup_batch_size
        .checked_mul(idempotency_response_cleanup_max_batches_per_run)
        .ok_or_else(|| {
            anyhow!(
                "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_BATCH_SIZE * COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN overflowed u64"
            )
        })?;
    if rows_per_run > MAX_RESPONSE_CLEANUP_ROWS_PER_RUN {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_BATCH_SIZE * COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN must be <= {}",
            MAX_RESPONSE_CLEANUP_ROWS_PER_RUN
        ));
    }
    Ok(())
}

fn validate_response_cleanup_worker_tick_sec(
    idempotency_response_cleanup_worker_tick_sec: u64,
) -> Result<()> {
    if !(MIN_RESPONSE_CLEANUP_WORKER_TICK_SEC..=MAX_RESPONSE_CLEANUP_WORKER_TICK_SEC)
        .contains(&idempotency_response_cleanup_worker_tick_sec)
    {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_WORKER_TICK_SEC must be in range [{}..={}]",
            MIN_RESPONSE_CLEANUP_WORKER_TICK_SEC,
            MAX_RESPONSE_CLEANUP_WORKER_TICK_SEC
        ));
    }
    Ok(())
}

fn validate_response_cleanup_worker_cadence(
    idempotency_response_cleanup_worker_tick_sec: u64,
    idempotency_response_retention_sec: u64,
) -> Result<()> {
    let max_tick_sec = response_cleanup_interval_sec_for_retention(idempotency_response_retention_sec);
    if idempotency_response_cleanup_worker_tick_sec > max_tick_sec {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_WORKER_TICK_SEC must be <= {} (derived from COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_RETENTION_SEC={})",
            max_tick_sec,
            idempotency_response_retention_sec
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::ffi::OsString;
    use std::fs;
    use std::path::Path;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        validate_response_cleanup_tuning, validate_response_cleanup_worker_cadence,
        validate_response_cleanup_worker_tick_sec,
        validate_response_retention_cutoff,
    };
    use crate::idempotency_cleanup_worker::response_cleanup_worker_tick_sec;

    static EXECUTOR_ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_copybot_executor_env() {
        let current: Vec<OsString> = env::vars_os()
            .filter(|(key, _)| key.to_string_lossy().starts_with("COPYBOT_EXECUTOR_"))
            .map(|(key, _)| key)
            .collect();
        for key in current {
            env::remove_var(key);
        }
    }

    fn with_clean_executor_env<T>(run: impl FnOnce() -> T) -> T {
        let _guard = EXECUTOR_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let saved: Vec<(OsString, OsString)> = env::vars_os()
            .filter(|(key, _)| key.to_string_lossy().starts_with("COPYBOT_EXECUTOR_"))
            .collect();
        clear_copybot_executor_env();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
        clear_copybot_executor_env();
        for (key, value) in saved {
            env::set_var(key, value);
        }
        match outcome {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn with_temp_signer_keypair_file<T>(run: impl FnOnce(&Path) -> T) -> T {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "copybot-executor-config-test-keypair-{}-{nanos}.json",
            std::process::id()
        ));
        let bytes = vec![0u8; 64];
        let json = serde_json::to_string(&bytes).expect("serialize keypair fixture");
        fs::write(&path, json).expect("write keypair fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&path)
                .expect("stat keypair fixture")
                .permissions();
            perms.set_mode(0o600);
            fs::set_permissions(&path, perms).expect("set restrictive keypair permissions");
        }
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&path)));
        let _ = fs::remove_file(&path);
        match outcome {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn set_minimal_executor_env_for_from_env(keypair_path: &Path) {
        env::set_var(
            "COPYBOT_EXECUTOR_SIGNER_PUBKEY",
            "11111111111111111111111111111111",
        );
        env::set_var("COPYBOT_EXECUTOR_SIGNER_SOURCE", "file");
        env::set_var(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE",
            keypair_path.to_str().expect("utf8 path"),
        );
        env::set_var("COPYBOT_EXECUTOR_ROUTE_ALLOWLIST", "rpc");
        env::set_var(
            "COPYBOT_EXECUTOR_ROUTE_RPC_SUBMIT_URL",
            "https://submit.example.com",
        );
        env::set_var(
            "COPYBOT_EXECUTOR_ROUTE_RPC_SIMULATE_URL",
            "https://simulate.example.com",
        );
        env::set_var("COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED", "true");
    }

    #[test]
    fn validate_response_retention_cutoff_accepts_default_range() {
        validate_response_retention_cutoff(604_800).expect("default retention should be valid");
    }

    #[test]
    fn validate_response_retention_cutoff_rejects_out_of_range_value() {
        let error = validate_response_retention_cutoff(u64::MAX).expect_err("must reject");
        assert!(
            error
                .to_string()
                .contains("COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_RETENTION_SEC"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_response_cleanup_tuning_accepts_default_range() {
        validate_response_cleanup_tuning(500, 4).expect("default cleanup tuning should be valid");
        validate_response_cleanup_tuning(50_000, 4).expect("boundary rows-per-run should be valid");
    }

    #[test]
    fn validate_response_cleanup_tuning_rejects_out_of_range_values() {
        let batch_error = validate_response_cleanup_tuning(u64::MAX, 4).expect_err("must reject");
        assert!(
            batch_error
                .to_string()
                .contains("COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_BATCH_SIZE"),
            "unexpected error: {}",
            batch_error
        );

        let batch_count_error =
            validate_response_cleanup_tuning(500, u64::MAX).expect_err("must reject");
        assert!(
            batch_count_error
                .to_string()
                .contains("COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN"),
            "unexpected error: {}",
            batch_count_error
        );
    }

    #[test]
    fn validate_response_cleanup_tuning_rejects_excessive_rows_per_run() {
        let error = validate_response_cleanup_tuning(50_000, 5).expect_err("must reject");
        assert!(
            error
                .to_string()
                .contains("COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_BATCH_SIZE * COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_response_cleanup_worker_tick_sec_accepts_default_bounds() {
        validate_response_cleanup_worker_tick_sec(15).expect("lower bound should be valid");
        validate_response_cleanup_worker_tick_sec(300).expect("upper bound should be valid");
    }

    #[test]
    fn validate_response_cleanup_worker_tick_sec_rejects_out_of_range_values() {
        let low = validate_response_cleanup_worker_tick_sec(14).expect_err("must reject");
        assert!(
            low.to_string()
                .contains("COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_WORKER_TICK_SEC"),
            "unexpected error: {}",
            low
        );
        let high = validate_response_cleanup_worker_tick_sec(301).expect_err("must reject");
        assert!(
            high.to_string()
                .contains("COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_WORKER_TICK_SEC"),
            "unexpected error: {}",
            high
        );
    }

    #[test]
    fn validate_response_cleanup_worker_cadence_rejects_tick_slower_than_cleanup_interval() {
        let error = validate_response_cleanup_worker_cadence(300, 120).expect_err("must reject");
        assert!(
            error
                .to_string()
                .contains("COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_WORKER_TICK_SEC"),
            "unexpected error: {}",
            error
        );
        assert!(
            error
                .to_string()
                .contains("COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_RETENTION_SEC"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn executor_config_from_env_wires_response_cleanup_worker_tick_override() {
        with_clean_executor_env(|| {
            with_temp_signer_keypair_file(|keypair_path| {
                set_minimal_executor_env_for_from_env(keypair_path);
                env::set_var(
                    "COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_WORKER_TICK_SEC",
                    "42",
                );

                let config =
                    crate::ExecutorConfig::from_env().expect("config should parse from env");
                assert_eq!(config.idempotency_response_cleanup_worker_tick_sec, 42);
                assert_ne!(
                    config.idempotency_response_cleanup_worker_tick_sec,
                    response_cleanup_worker_tick_sec(config.idempotency_response_retention_sec),
                    "explicit tick override must take precedence over retention-derived default"
                );
            });
        });
    }

    #[test]
    fn executor_config_from_env_rejects_invalid_submit_fastlane_enabled_token() {
        with_clean_executor_env(|| {
            with_temp_signer_keypair_file(|keypair_path| {
                set_minimal_executor_env_for_from_env(keypair_path);
                env::set_var("COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED", "sometimes");

                let error = match crate::ExecutorConfig::from_env() {
                    Ok(_) => panic!("invalid bool token must reject"),
                    Err(error) => error,
                };
                assert!(
                    error
                        .to_string()
                        .contains("COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED"),
                    "unexpected error: {}",
                    error
                );
            });
        });
    }

    #[test]
    fn executor_config_from_env_rejects_invalid_allow_nonzero_tip_token() {
        with_clean_executor_env(|| {
            with_temp_signer_keypair_file(|keypair_path| {
                set_minimal_executor_env_for_from_env(keypair_path);
                env::set_var("COPYBOT_EXECUTOR_ALLOW_NONZERO_TIP", "maybe");

                let error = match crate::ExecutorConfig::from_env() {
                    Ok(_) => panic!("invalid bool token must reject"),
                    Err(error) => error,
                };
                assert!(
                    error
                        .to_string()
                        .contains("COPYBOT_EXECUTOR_ALLOW_NONZERO_TIP"),
                    "unexpected error: {}",
                    error
                );
            });
        });
    }

    #[test]
    fn with_clean_executor_env_removes_newly_added_keys_after_scope() {
        const LEAK_KEY: &str = "COPYBOT_EXECUTOR_TEST_LEAK_KEY";
        env::remove_var(LEAK_KEY);
        with_clean_executor_env(|| {
            env::set_var(LEAK_KEY, "value");
            assert_eq!(env::var(LEAK_KEY).as_deref(), Ok("value"));
        });
        assert!(
            env::var_os(LEAK_KEY).is_none(),
            "helper must remove keys created inside isolated scope"
        );
    }
}
