use anyhow::{anyhow, Context, Result};
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WalletTokenBalance {
    pub(super) raw: u64,
    pub(super) decimals: u8,
}

pub(super) async fn fetch_wallet_token_balance(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    token: &str,
) -> Result<Option<WalletTokenBalance>> {
    let rpc_url = config.submit_adapter_http_url.trim();
    if rpc_url.is_empty() {
        return Ok(None);
    }
    let response = http
        .post(rpc_url)
        .timeout(Duration::from_millis(config.quote_canary_timeout_ms.max(1)))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": "execution-canary-owned-sell-balance",
            "method": "getTokenAccountsByOwner",
            "params": [
                config.canary_wallet_pubkey.as_str(),
                {"mint": token},
                {"encoding": "jsonParsed", "commitment": "confirmed"}
            ]
        }))
        .send()
        .await
        .context("owned sell wallet token account RPC request failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("owned sell wallet token account RPC body read failed")?;
    if !status.is_success() {
        return Err(anyhow!(
            "owned sell wallet token account RPC returned HTTP {status}: {body}"
        ));
    }
    parse_wallet_token_balance(
        serde_json::from_str(&body)
            .context("owned sell wallet token account RPC JSON decode failed")?,
    )
}

fn parse_wallet_token_balance(value: Value) -> Result<Option<WalletTokenBalance>> {
    if let Some(error) = value.get("error") {
        return Err(anyhow!(
            "owned sell wallet token account RPC error: {error}"
        ));
    }
    let accounts = value
        .get("result")
        .and_then(|result| result.get("value"))
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("owned sell wallet token account response missing result.value"))?;
    let mut total_raw = 0_u64;
    let mut token_decimals = None;
    let mut account_seen = false;
    for account in accounts {
        let Some(amount) = account.pointer("/account/data/parsed/info/tokenAmount") else {
            continue;
        };
        let Some(raw) = amount
            .get("amount")
            .and_then(Value::as_str)
            .and_then(|raw| raw.parse::<u64>().ok())
        else {
            continue;
        };
        let Some(decimals) = amount
            .get("decimals")
            .and_then(Value::as_u64)
            .and_then(|raw| u8::try_from(raw).ok())
        else {
            continue;
        };
        if let Some(existing) = token_decimals {
            if existing != decimals {
                return Err(anyhow!(
                    "owned sell wallet token account decimals mismatch: {existing} != {decimals}"
                ));
            }
        }
        token_decimals = Some(decimals);
        total_raw = total_raw
            .checked_add(raw)
            .ok_or_else(|| anyhow!("owned sell wallet token balance overflow"))?;
        account_seen = true;
    }
    Ok(account_seen.then(|| WalletTokenBalance {
        raw: total_raw,
        decimals: token_decimals.expect("account_seen requires decimals"),
    }))
}
