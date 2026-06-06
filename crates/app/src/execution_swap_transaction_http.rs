use crate::execution_quote_canary_helpers::truncate_for_log;
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use crate::execution_swap_http_request::{swap_endpoint_url, swap_request_body};
use crate::execution_swap_http_retry::post_swap_json_with_retry;
use anyhow::{anyhow, Result};
use copybot_config::ExecutionConfig;
use serde_json::Value;
use std::time::Duration as StdDuration;

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
    let api_key = config.quote_canary_api_key.trim();
    let response = post_swap_json_with_retry(
        http,
        url,
        api_key,
        &body,
        timeout,
        "swap transaction dry-run",
    )
    .await?;
    Ok(Some(swap_transaction_response_summary(
        response.value,
        response.elapsed_ms,
        response.attempts,
    )?))
}

fn swap_transaction_response_summary(
    value: Value,
    elapsed_ms: u64,
    attempts: usize,
) -> Result<String> {
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
        "metis_swap_transaction_ok base64_len={} latency_ms={} attempts={} simulation_error={}",
        swap_transaction.len(),
        elapsed_ms,
        attempts,
        simulation_error.unwrap_or_else(|| "none".to_string())
    );
    Ok(truncate_for_log(&summary, 500))
}
