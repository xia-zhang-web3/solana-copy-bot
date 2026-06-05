use crate::execution_submit_adapter::ExecutionTransactionPlan;
use anyhow::{anyhow, Result};
use serde_json::{json, Value};

pub(crate) fn swap_request_body(
    plan: &ExecutionTransactionPlan,
    user_pubkey: &str,
    context: &str,
) -> Result<Value> {
    let blueprint = plan
        .swap_blueprint
        .as_ref()
        .ok_or_else(|| anyhow!("missing swap blueprint for {context} body"))?;
    let route_plan_json = plan
        .metadata
        .route_plan_json
        .as_deref()
        .ok_or_else(|| anyhow!("missing route_plan_json for {context} body"))?;
    let route_plan: Value = serde_json::from_str(route_plan_json)
        .map_err(|error| anyhow!("invalid route_plan_json for {context} body: {error}"))?;
    if !route_plan.is_array() {
        return Err(anyhow!(
            "route_plan_json must be an array for {context} body"
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
            context,
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

pub(crate) fn swap_endpoint_url(base_url: &str, endpoint: &str, context: &str) -> Result<String> {
    let trimmed = base_url.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{context} base URL is empty"));
    }
    let without_slash = trimmed.trim_end_matches('/');
    let root = without_slash
        .strip_suffix("/swap-instructions")
        .or_else(|| without_slash.strip_suffix("/quote"))
        .or_else(|| without_slash.strip_suffix("/swap"))
        .unwrap_or(without_slash);
    Ok(format!("{root}/{endpoint}"))
}

fn other_amount_threshold(
    out_amount_raw: &str,
    slippage_bps: u64,
    context: &str,
) -> Result<String> {
    let out_amount = out_amount_raw
        .parse::<u128>()
        .map_err(|error| anyhow!("invalid outAmount for {context} threshold: {error}"))?;
    let keep_bps = 10_000_u128.saturating_sub(u128::from(slippage_bps.min(10_000)));
    Ok(((out_amount.saturating_mul(keep_bps)) / 10_000).to_string())
}

fn price_impact_pct_string(value: Option<f64>) -> String {
    let Some(value) = value.filter(|value| value.is_finite()) else {
        return "0".to_string();
    };
    value.to_string()
}
