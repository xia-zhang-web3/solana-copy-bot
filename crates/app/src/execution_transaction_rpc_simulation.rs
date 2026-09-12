use crate::execution_pumpswap_error::annotate_pumpswap_custom_errors;
use crate::execution_quote_canary_helpers::truncate_for_log;
use anyhow::{anyhow, Result};
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use std::time::Duration as StdDuration;

const SIMULATION_REQUEST_ID: &str = "execution-swap-transaction-simulate";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RpcSimulationOutcome {
    Passed { slot: u64 },
    Skipped,
}

impl RpcSimulationOutcome {
    pub(crate) fn with_summary(self, summary: &str) -> String {
        let status = match self {
            Self::Passed { .. } => "rpc_simulation=passed",
            Self::Skipped => "rpc_simulation=skipped",
        };
        // Reserve both the separator and truncate_for_log's possible ellipsis.
        format!(
            "{} {status}",
            truncate_for_log(summary, 500 - status.len() - 4)
        )
    }
}

pub(crate) async fn verify_serialized_transaction_rpc_simulation(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    serialized_transaction_base64: &str,
    source: &str,
    timeout: StdDuration,
) -> Result<RpcSimulationOutcome> {
    if !config.canary_tiny_submit_enabled {
        return Ok(RpcSimulationOutcome::Skipped);
    }
    let rpc_url = config.submit_adapter_http_url.trim();
    if rpc_url.is_empty() {
        return Err(anyhow!(
            "swap transaction RPC simulation requires nonempty submit_adapter_http_url"
        ));
    }
    let request = json!({
        "jsonrpc": "2.0",
        "id": SIMULATION_REQUEST_ID,
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
    parse_rpc_simulation_response(&value, source)
}

pub(crate) fn parse_rpc_simulation_response(
    value: &Value,
    source: &str,
) -> Result<RpcSimulationOutcome> {
    let invalid = |field: &str, expected: &str| {
        anyhow!(
            "swap transaction RPC simulation invalid source={} field={field} expected={expected}",
            truncate_for_log(source, 80)
        )
    };
    if !value.is_object() {
        return Err(invalid("response", "object"));
    }
    if let Some(error) = value.get("error") {
        return Err(anyhow!(
            "swap transaction RPC simulation error source={}: {}",
            source,
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(invalid("jsonrpc", "2.0"));
    }
    if value.get("id").and_then(Value::as_str) != Some(SIMULATION_REQUEST_ID) {
        return Err(invalid("id", SIMULATION_REQUEST_ID));
    }
    let result = value
        .get("result")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("result", "object"))?;
    let context = result
        .get("context")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("result.context", "object"))?;
    let slot = context
        .get("slot")
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid("result.context.slot", "u64"))?;
    let simulation = result
        .get("value")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("result.value", "object"))?;
    let error = simulation
        .get("err")
        .ok_or_else(|| invalid("result.value.err", "explicit null"))?;
    if !error.is_null() {
        let logs = simulation
            .get("logs")
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
    Ok(RpcSimulationOutcome::Passed { slot })
}
