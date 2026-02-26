use anyhow::{anyhow, Result};

use crate::{
    env_parsing::{optional_non_empty_env, parse_bool_env, parse_u64_env},
    http_utils::{endpoint_identity, validate_endpoint_url},
    DEFAULT_SUBMIT_VERIFY_ATTEMPTS, DEFAULT_SUBMIT_VERIFY_INTERVAL_MS,
};

#[derive(Clone, Debug)]
pub(crate) struct SubmitSignatureVerifyConfig {
    pub(crate) endpoints: Vec<String>,
    pub(crate) attempts: u64,
    pub(crate) interval_ms: u64,
    pub(crate) strict: bool,
}

pub(crate) fn parse_submit_signature_verify_config() -> Result<Option<SubmitSignatureVerifyConfig>>
{
    let primary = optional_non_empty_env("COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL");
    let fallback = optional_non_empty_env("COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_FALLBACK_URL");
    let attempts = parse_u64_env(
        "COPYBOT_EXECUTOR_SUBMIT_VERIFY_ATTEMPTS",
        DEFAULT_SUBMIT_VERIFY_ATTEMPTS,
    )?;
    let interval_ms = parse_u64_env(
        "COPYBOT_EXECUTOR_SUBMIT_VERIFY_INTERVAL_MS",
        DEFAULT_SUBMIT_VERIFY_INTERVAL_MS,
    )?;
    let strict = parse_bool_env("COPYBOT_EXECUTOR_SUBMIT_VERIFY_STRICT", false)?;
    build_submit_signature_verify_config(primary, fallback, attempts, interval_ms, strict)
}

pub(crate) fn build_submit_signature_verify_config(
    primary: Option<String>,
    fallback: Option<String>,
    attempts: u64,
    interval_ms: u64,
    strict: bool,
) -> Result<Option<SubmitSignatureVerifyConfig>> {
    if primary.is_none() && fallback.is_none() {
        return Ok(None);
    }

    let Some(primary_url) = primary else {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_FALLBACK_URL requires COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL"
        ));
    };
    validate_endpoint_url(primary_url.as_str())
        .map_err(|error| anyhow!("invalid COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL: {}", error))?;

    let mut endpoints = vec![primary_url];
    if let Some(fallback_url) = fallback {
        validate_endpoint_url(fallback_url.as_str()).map_err(|error| {
            anyhow!(
                "invalid COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_FALLBACK_URL: {}",
                error
            )
        })?;
        if endpoint_identity(fallback_url.as_str())? == endpoint_identity(endpoints[0].as_str())? {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_FALLBACK_URL must resolve to distinct endpoint"
            ));
        }
        endpoints.push(fallback_url);
    }

    if attempts == 0 || attempts > 20 {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_SUBMIT_VERIFY_ATTEMPTS must be in 1..=20"
        ));
    }
    if interval_ms == 0 || interval_ms > 60_000 {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_SUBMIT_VERIFY_INTERVAL_MS must be in 1..=60000"
        ));
    }

    Ok(Some(SubmitSignatureVerifyConfig {
        endpoints,
        attempts,
        interval_ms,
        strict,
    }))
}
