use anyhow::{anyhow, Context, Result};
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use std::time::Duration;

pub(crate) async fn resolve_spl_token_decimals(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    mint: &str,
    known: Option<u8>,
) -> Option<u8> {
    if known.is_some() {
        return known;
    }
    fetch_spl_token_decimals(
        http,
        config.priority_fee_canary_rpc_url.trim(),
        mint,
        config.quote_canary_timeout_ms,
    )
    .await
    .ok()
    .flatten()
}

pub(crate) async fn fetch_spl_token_decimals(
    http: &reqwest::Client,
    rpc_url: &str,
    mint: &str,
    timeout_ms: u64,
) -> Result<Option<u8>> {
    let rpc_url = rpc_url.trim();
    if rpc_url.is_empty() {
        return Ok(None);
    }
    let response = http
        .post(rpc_url)
        .timeout(Duration::from_millis(timeout_ms.max(1)))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": "execution-quote-canary-token-decimals",
            "method": "getTokenSupply",
            "params": [mint],
        }))
        .send()
        .await
        .context("token decimals RPC request failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("token decimals RPC body read failed")?;
    if !status.is_success() {
        return Err(anyhow!("token decimals RPC returned HTTP {status}: {body}"));
    }
    token_decimals_from_rpc_json(
        serde_json::from_str(&body).context("token decimals RPC JSON decode failed")?,
    )
}

pub(crate) fn token_decimals_from_rpc_json(value: Value) -> Result<Option<u8>> {
    if let Some(error) = value.get("error") {
        return Err(anyhow!("token decimals RPC error: {error}"));
    }
    let Some(raw) = value
        .get("result")
        .and_then(|result| result.get("value"))
        .and_then(|value| value.get("decimals"))
        .and_then(Value::as_u64)
    else {
        return Ok(None);
    };
    let decimals = u8::try_from(raw).context("token decimals do not fit u8")?;
    Ok(Some(decimals))
}
