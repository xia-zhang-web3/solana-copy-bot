use crate::execution_quote_canary_helpers::truncate_for_log;
use crate::execution_signing_envelope::validate_serialized_transaction_base64;
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use crate::execution_swap_http_retry::post_swap_json_with_retry;
use crate::execution_swap_transaction_http::SwapTransactionDryRunResult;
use anyhow::{anyhow, Result};
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use std::time::Duration as StdDuration;

const PUMP_FUN_PRIORITY_FEE_LEVEL_DRY_RUN: &str = "high";

pub(crate) async fn fetch_pump_fun_swap_transaction_dry_run(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<Option<SwapTransactionDryRunResult>> {
    if !config.swap_transaction_dry_run_enabled {
        return Ok(None);
    }
    let Some(blueprint) = plan.swap_blueprint.as_ref() else {
        return Err(anyhow!(
            "missing swap blueprint for pump.fun swap transaction dry-run"
        ));
    };
    let user_pubkey = blueprint
        .wallet_pubkey
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing user public key for pump.fun swap transaction dry-run"))?;
    let body = json!({
        "wallet": user_pubkey,
        "type": plan.side.to_ascii_uppercase(),
        "mint": plan.token,
        "inAmount": blueprint.input_amount_raw,
        "priorityFeeLevel": PUMP_FUN_PRIORITY_FEE_LEVEL_DRY_RUN,
        "slippageBps": blueprint.slippage_bps.round().to_string(),
    });
    let url = pump_fun_swap_url(&config.quote_canary_base_url)?;
    let timeout = StdDuration::from_millis(config.quote_canary_timeout_ms.max(1));
    let response = post_swap_json_with_retry(
        http,
        url,
        config.quote_canary_api_key.trim(),
        &body,
        timeout,
        "pump.fun swap transaction dry-run",
    )
    .await?;
    Ok(Some(pump_fun_swap_transaction_summary(
        response.value,
        response.elapsed_ms,
        response.attempts,
    )?))
}

fn pump_fun_swap_transaction_summary(
    value: Value,
    elapsed_ms: u64,
    attempts: usize,
) -> Result<SwapTransactionDryRunResult> {
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        return Err(anyhow!(
            "pump.fun swap transaction dry-run error: {}",
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    if let Some(error) = value
        .get("simulationError")
        .filter(|error| !error.is_null())
    {
        return Err(anyhow!(
            "pump.fun swap transaction dry-run simulation error: {}",
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    let tx = value
        .get("tx")
        .and_then(Value::as_str)
        .filter(|item| !item.trim().is_empty())
        .ok_or_else(|| anyhow!("pump.fun swap transaction dry-run missing tx"))?;
    validate_serialized_transaction_base64(tx)?;
    let summary = format!(
        "pump_fun_swap_transaction_ok base64_len={} serialized_transaction_base64_ready=true latency_ms={} attempts={} priority_fee_level={}",
        tx.len(),
        elapsed_ms,
        attempts,
        PUMP_FUN_PRIORITY_FEE_LEVEL_DRY_RUN
    );
    Ok(SwapTransactionDryRunResult {
        summary: truncate_for_log(&summary, 500),
        serialized_transaction_base64: tx.to_string(),
        source: "pump_fun_paid".to_string(),
    })
}

fn pump_fun_swap_url(base_url: &str) -> Result<String> {
    let trimmed = base_url.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("pump.fun swap base URL is empty"));
    }
    let without_slash = trimmed.trim_end_matches('/');
    if without_slash.ends_with("/pump-fun/swap") {
        Ok(without_slash.to_string())
    } else {
        Ok(format!("{without_slash}/pump-fun/swap"))
    }
}
