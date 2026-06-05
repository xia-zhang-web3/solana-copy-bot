use crate::execution_quote_canary_helpers::{elapsed_ms, truncate_for_log};
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use crate::execution_swap_http_request::{swap_endpoint_url, swap_request_body};
use anyhow::{anyhow, Context, Result};
use copybot_config::ExecutionConfig;
use serde_json::Value;
use std::time::{Duration as StdDuration, Instant};

pub(crate) async fn fetch_swap_transaction_dry_run(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<Option<String>> {
    if !config.swap_transaction_dry_run_enabled {
        return Ok(None);
    }
    let Some(blueprint) = plan.swap_blueprint.as_ref() else {
        return Err(anyhow!(
            "missing swap blueprint for swap transaction dry-run"
        ));
    };
    let user_pubkey = blueprint
        .wallet_pubkey
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing user public key for swap transaction dry-run"))?;
    let body = swap_request_body(plan, user_pubkey, "swap transaction")?;
    let url = swap_endpoint_url(&config.quote_canary_base_url, "swap", "swap transaction")?;
    let timeout = StdDuration::from_millis(config.quote_canary_timeout_ms.max(1));
    let started = Instant::now();
    let mut request = http.post(url).json(&body).timeout(timeout);
    let api_key = config.quote_canary_api_key.trim();
    if !api_key.is_empty() {
        request = request.header("x-api-key", api_key);
    }
    let response = request
        .send()
        .await
        .context("swap transaction dry-run request failed")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "swap transaction dry-run returned HTTP {status}: {}",
            truncate_for_log(&body, 240)
        ));
    }
    let value: Value = response
        .json()
        .await
        .context("swap transaction dry-run response JSON decode failed")?;
    Ok(Some(swap_transaction_response_summary(value, started)?))
}

fn swap_transaction_response_summary(value: Value, started: Instant) -> Result<String> {
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        return Err(anyhow!(
            "swap transaction dry-run error: {}",
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    let swap_transaction = value
        .get("swapTransaction")
        .and_then(Value::as_str)
        .filter(|item| !item.trim().is_empty())
        .ok_or_else(|| anyhow!("swap transaction dry-run missing swapTransaction"))?;
    let simulation_error = value
        .get("simulationError")
        .filter(|item| !item.is_null())
        .map(|item| truncate_for_log(&item.to_string(), 180));
    let summary = format!(
        "metis_swap_transaction_ok base64_len={} latency_ms={} simulation_error={}",
        swap_transaction.len(),
        elapsed_ms(started),
        simulation_error.unwrap_or_else(|| "none".to_string())
    );
    Ok(truncate_for_log(&summary, 500))
}
