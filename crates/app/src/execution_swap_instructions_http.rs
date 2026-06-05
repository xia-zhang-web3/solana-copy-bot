use crate::execution_quote_canary_helpers::{elapsed_ms, truncate_for_log};
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use anyhow::{anyhow, Context, Result};
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use std::time::{Duration as StdDuration, Instant};

pub(crate) async fn fetch_swap_instructions_dry_run(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<Option<String>> {
    if !config.swap_instructions_dry_run_enabled {
        return Ok(None);
    }
    let Some(blueprint) = plan.swap_blueprint.as_ref() else {
        return Err(anyhow!(
            "missing swap blueprint for swap-instructions dry-run"
        ));
    };
    let user_pubkey = blueprint
        .wallet_pubkey
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing user public key for swap-instructions dry-run"))?;
    let body = swap_instructions_request_body(plan, user_pubkey)?;
    let url = swap_instructions_url(&config.quote_canary_base_url)?;
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
        .context("swap-instructions dry-run request failed")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "swap-instructions dry-run returned HTTP {status}: {}",
            truncate_for_log(&body, 240)
        ));
    }
    let value: Value = response
        .json()
        .await
        .context("swap-instructions dry-run response JSON decode failed")?;
    Ok(Some(swap_instructions_response_summary(value, started)?))
}

fn swap_instructions_request_body(
    plan: &ExecutionTransactionPlan,
    user_pubkey: &str,
) -> Result<Value> {
    let blueprint = plan
        .swap_blueprint
        .as_ref()
        .ok_or_else(|| anyhow!("missing swap blueprint for swap-instructions body"))?;
    let route_plan_json = plan
        .metadata
        .route_plan_json
        .as_deref()
        .ok_or_else(|| anyhow!("missing route_plan_json for swap-instructions body"))?;
    let route_plan: Value = serde_json::from_str(route_plan_json)
        .map_err(|error| anyhow!("invalid route_plan_json for swap-instructions body: {error}"))?;
    if !route_plan.is_array() {
        return Err(anyhow!(
            "route_plan_json must be an array for swap-instructions body"
        ));
    }
    let slippage_bps = blueprint.slippage_bps.round() as u64;
    let quote_response = json!({
        "inputMint": blueprint.input_mint,
        "inAmount": blueprint.input_amount_raw,
        "outputMint": blueprint.output_mint,
        "outAmount": blueprint.output_amount_raw,
        "otherAmountThreshold": other_amount_threshold(
            &blueprint.output_amount_raw,
            slippage_bps,
        )?,
        "swapMode": "ExactIn",
        "slippageBps": slippage_bps,
        "platformFee": null,
        "priceImpactPct": price_impact_pct_string(plan.metadata.price_impact_pct),
        "routePlan": route_plan,
    });
    Ok(json!({
        "userPublicKey": user_pubkey,
        "quoteResponse": quote_response,
        "dynamicComputeUnitLimit": true,
        "prioritizationFeeLamports": blueprint.priority_fee_lamports,
    }))
}

fn swap_instructions_response_summary(value: Value, started: Instant) -> Result<String> {
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        return Err(anyhow!(
            "swap-instructions dry-run error: {}",
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    if value
        .get("swapInstruction")
        .filter(|item| item.is_object())
        .is_none()
    {
        return Err(anyhow!("swap-instructions dry-run missing swapInstruction"));
    }
    let simulation_error = value
        .get("simulationError")
        .filter(|item| !item.is_null())
        .map(|item| truncate_for_log(&item.to_string(), 180));
    let summary = format!(
        "metis_swap_instructions_ok compute={} setup={} other={} alt={} cleanup={} latency_ms={} simulation_error={}",
        array_len(value.get("computeBudgetInstructions")),
        array_len(value.get("setupInstructions")),
        array_len(value.get("otherInstructions")),
        array_len(value.get("addressLookupTableAddresses")),
        value.get("cleanupInstruction")
            .filter(|item| !item.is_null())
            .is_some(),
        elapsed_ms(started),
        simulation_error.unwrap_or_else(|| "none".to_string())
    );
    Ok(truncate_for_log(&summary, 500))
}

fn swap_instructions_url(base_url: &str) -> Result<String> {
    let trimmed = base_url.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("swap-instructions base URL is empty"));
    }
    let without_slash = trimmed.trim_end_matches('/');
    if without_slash.ends_with("/swap-instructions") {
        Ok(without_slash.to_string())
    } else if without_slash.ends_with("/quote") {
        Ok(format!(
            "{}/swap-instructions",
            without_slash.trim_end_matches("/quote")
        ))
    } else {
        Ok(format!("{without_slash}/swap-instructions"))
    }
}

fn other_amount_threshold(out_amount_raw: &str, slippage_bps: u64) -> Result<String> {
    let out_amount = out_amount_raw
        .parse::<u128>()
        .map_err(|error| anyhow!("invalid outAmount for swap-instructions threshold: {error}"))?;
    let keep_bps = 10_000_u128.saturating_sub(u128::from(slippage_bps.min(10_000)));
    Ok(((out_amount.saturating_mul(keep_bps)) / 10_000).to_string())
}

fn price_impact_pct_string(value: Option<f64>) -> String {
    let Some(value) = value.filter(|value| value.is_finite()) else {
        return "0".to_string();
    };
    value.to_string()
}

fn array_len(value: Option<&Value>) -> usize {
    value.and_then(Value::as_array).map(Vec::len).unwrap_or(0)
}
