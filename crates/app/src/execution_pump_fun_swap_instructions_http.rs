use crate::execution_quote_canary_helpers::truncate_for_log;
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use crate::execution_swap_http_retry::post_swap_json_with_retry;
use anyhow::{anyhow, Result};
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use std::time::Duration as StdDuration;

const PUMP_FUN_PRIORITY_FEE_LEVEL_DRY_RUN: &str = "high";

pub(crate) async fn fetch_pump_fun_swap_instructions_dry_run(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<Option<String>> {
    if !config.swap_instructions_dry_run_enabled {
        return Ok(None);
    }
    let Some(blueprint) = plan.swap_blueprint.as_ref() else {
        return Err(anyhow!(
            "missing swap blueprint for pump.fun swap-instructions dry-run"
        ));
    };
    let user_pubkey = blueprint
        .wallet_pubkey
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing user public key for pump.fun swap-instructions dry-run"))?;
    let body = json!({
        "wallet": user_pubkey,
        "type": plan.side.to_ascii_uppercase(),
        "mint": plan.token,
        "inAmount": blueprint.input_amount_raw,
        "priorityFeeLevel": PUMP_FUN_PRIORITY_FEE_LEVEL_DRY_RUN,
        "slippageBps": blueprint.slippage_bps.round().to_string(),
    });
    let url = pump_fun_swap_instructions_url(&config.quote_canary_base_url)?;
    let timeout = StdDuration::from_millis(config.quote_canary_timeout_ms.max(1));
    let api_key = config.quote_canary_api_key.trim();
    let response = post_swap_json_with_retry(
        http,
        url,
        api_key,
        &body,
        timeout,
        "pump.fun swap-instructions dry-run",
    )
    .await?;
    Ok(Some(pump_fun_swap_instructions_summary(
        response.value,
        response.elapsed_ms,
        response.attempts,
    )?))
}

fn pump_fun_swap_instructions_summary(
    value: Value,
    elapsed_ms: u64,
    attempts: usize,
) -> Result<String> {
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        return Err(anyhow!(
            "pump.fun swap-instructions dry-run error: {}",
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    let instructions = value
        .get("instructions")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("pump.fun swap-instructions dry-run missing instructions"))?;
    if instructions.is_empty() {
        return Err(anyhow!(
            "pump.fun swap-instructions dry-run returned no instructions"
        ));
    }
    let compute_budget = instructions
        .iter()
        .filter(|item| {
            item.get("programId")
                .and_then(Value::as_str)
                .is_some_and(|program| program == "ComputeBudget111111111111111111111111111111")
        })
        .count();
    let summary = format!(
        "pump_fun_swap_instructions_ok instructions={} compute_budget={} latency_ms={} attempts={} priority_fee_level={}",
        instructions.len(),
        compute_budget,
        elapsed_ms,
        attempts,
        PUMP_FUN_PRIORITY_FEE_LEVEL_DRY_RUN
    );
    Ok(truncate_for_log(&summary, 500))
}

fn pump_fun_swap_instructions_url(base_url: &str) -> Result<String> {
    let trimmed = base_url.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("pump.fun swap-instructions base URL is empty"));
    }
    let without_slash = trimmed.trim_end_matches('/');
    if without_slash.ends_with("/pump-fun/swap-instructions") {
        Ok(without_slash.to_string())
    } else {
        Ok(format!("{without_slash}/pump-fun/swap-instructions"))
    }
}
