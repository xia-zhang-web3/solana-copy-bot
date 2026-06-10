use crate::execution_pumpswap_error::annotate_pumpswap_custom_errors;
use crate::execution_quote_canary_helpers::truncate_for_log;
use anyhow::{anyhow, Result};
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use std::time::Duration as StdDuration;

pub(crate) async fn verify_serialized_transaction_rpc_simulation(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    serialized_transaction_base64: &str,
    source: &str,
    timeout: StdDuration,
) -> Result<()> {
    if !config.canary_tiny_submit_enabled {
        return Ok(());
    }
    let rpc_url = config.submit_adapter_http_url.trim();
    if rpc_url.is_empty() {
        return Ok(());
    }
    let request = json!({
        "jsonrpc": "2.0",
        "id": "execution-swap-transaction-simulate",
        "method": "simulateTransaction",
        "params": [
            serialized_transaction_base64,
            {
                "encoding": "base64",
                "sigVerify": false,
                "replaceRecentBlockhash": true,
                "commitment": "confirmed",
            }
        ],
    });
    let response = http
        .post(rpc_url)
        .timeout(timeout)
        .json(&request)
        .send()
        .await
        .map_err(|error| anyhow!("swap transaction RPC simulation request failed: {error}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| anyhow!("swap transaction RPC simulation body read failed: {error}"))?;
    if !status.is_success() {
        return Err(anyhow!(
            "swap transaction RPC simulation returned HTTP {status}: {}",
            truncate_for_log(&body, 240)
        ));
    }
    let value: Value = serde_json::from_str(&body)
        .map_err(|error| anyhow!("swap transaction RPC simulation JSON decode failed: {error}"))?;
    if let Some(error) = value.get("error") {
        return Err(anyhow!(
            "swap transaction RPC simulation error source={}: {}",
            source,
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    let simulation = value.pointer("/result/value");
    if let Some(error) = simulation
        .and_then(|item| item.get("err"))
        .filter(|item| !item.is_null())
    {
        let logs = simulation
            .and_then(|item| item.get("logs"))
            .map(|logs| truncate_for_log(&logs.to_string(), 260))
            .unwrap_or_else(|| "[]".to_string());
        let error_text = if source == "pumpswap_direct" {
            annotate_pumpswap_custom_errors(&error.to_string())
        } else {
            error.to_string()
        };
        return Err(anyhow!(
            "swap transaction RPC simulation failed source={} err={} logs={}",
            source,
            truncate_for_log(&error_text, 220),
            logs
        ));
    }
    Ok(())
}
