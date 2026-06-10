use crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_PUBLIC;
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use crate::execution_swap_http_retry::{post_swap_json_with_retry, SwapHttpJsonResponse};
use anyhow::{anyhow, Result};
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use std::time::Duration as StdDuration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SwapBuilderSource {
    Metis,
    MetisFallback,
    PublicSelected,
    PublicFallback,
}

impl SwapBuilderSource {
    pub(crate) fn summary_tag(self, shared_accounts_disabled: bool) -> &'static str {
        match (self, shared_accounts_disabled) {
            (Self::Metis, false) => "ok",
            (Self::Metis, true) => "no_shared_accounts_ok",
            (Self::MetisFallback, false) => "metis_fallback_ok",
            (Self::MetisFallback, true) => "metis_fallback_no_shared_accounts_ok",
            (Self::PublicSelected, false) => "public_selected_ok",
            (Self::PublicSelected, true) => "public_selected_no_shared_accounts_ok",
            (Self::PublicFallback, false) => "public_fallback_ok",
            (Self::PublicFallback, true) => "public_fallback_no_shared_accounts_ok",
        }
    }

    pub(crate) fn payload_source(self, shared_accounts_disabled: bool) -> &'static str {
        match (self, shared_accounts_disabled) {
            (Self::Metis, false) => "metis",
            (Self::Metis, true) => "metis_no_shared_accounts",
            (Self::MetisFallback, false) => "metis_fallback",
            (Self::MetisFallback, true) => "metis_fallback_no_shared_accounts",
            (Self::PublicSelected, _) => "public_selected",
            (Self::PublicFallback, _) => "public_fallback",
        }
    }
}

pub(crate) struct SwapBuilderEndpoint {
    pub(crate) url: String,
    pub(crate) api_key: String,
    pub(crate) source: SwapBuilderSource,
}

pub(crate) fn swap_request_body(
    plan: &ExecutionTransactionPlan,
    user_pubkey: &str,
    context: &str,
) -> Result<Value> {
    let blueprint = plan
        .swap_blueprint
        .as_ref()
        .ok_or_else(|| anyhow!("missing swap blueprint for {context} body"))?;
    let quote_response = quote_response_body(plan, blueprint.slippage_bps, context)?;
    Ok(json!({
        "userPublicKey": user_pubkey,
        "quoteResponse": quote_response,
        "dynamicComputeUnitLimit": true,
        "prioritizationFeeLamports": blueprint.priority_fee_lamports,
    }))
}

pub(crate) fn disable_shared_accounts(body: &mut Value) {
    if let Some(object) = body.as_object_mut() {
        object.insert("useSharedAccounts".to_string(), Value::Bool(false));
    }
}

pub(crate) fn enable_skip_user_accounts_rpc_calls(body: &mut Value) {
    if let Some(object) = body.as_object_mut() {
        object.insert("skipUserAccountsRpcCalls".to_string(), Value::Bool(true));
    }
}

pub(crate) fn disable_dynamic_compute_unit_limit(body: &mut Value) {
    if let Some(object) = body.as_object_mut() {
        object.insert("dynamicComputeUnitLimit".to_string(), Value::Bool(false));
    }
}

pub(crate) async fn post_no_shared_skip_user_accounts_json_with_retry(
    http: &reqwest::Client,
    url: &str,
    api_key: &str,
    body: &Value,
    timeout: StdDuration,
    context: &str,
) -> Result<SwapHttpJsonResponse> {
    let mut retry_body = body.clone();
    disable_shared_accounts(&mut retry_body);
    enable_skip_user_accounts_rpc_calls(&mut retry_body);
    post_swap_json_with_retry(
        http,
        url.to_string(),
        api_key,
        &retry_body,
        timeout,
        context,
    )
    .await
}

pub(crate) async fn post_no_shared_skip_user_accounts_static_cu_json_with_retry(
    http: &reqwest::Client,
    url: &str,
    api_key: &str,
    body: &Value,
    timeout: StdDuration,
    context: &str,
) -> Result<SwapHttpJsonResponse> {
    let mut retry_body = body.clone();
    disable_shared_accounts(&mut retry_body);
    enable_skip_user_accounts_rpc_calls(&mut retry_body);
    disable_dynamic_compute_unit_limit(&mut retry_body);
    post_swap_json_with_retry(
        http,
        url.to_string(),
        api_key,
        &retry_body,
        timeout,
        context,
    )
    .await
}

pub(crate) fn is_missing_account_simulation_error(value: &Value) -> bool {
    let Some(error) = simulation_error_text(value) else {
        return false;
    };
    is_missing_account_error_text(&error)
}

pub(crate) fn is_missing_account_error_text(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("account required") && lower.contains("missing")
        || lower.contains("not enough account")
        || lower.contains("missing account")
}

pub(crate) fn simulation_error_text(value: &Value) -> Option<String> {
    value
        .get("simulationError")
        .filter(|item| !item.is_null())
        .map(Value::to_string)
}

fn quote_response_body(
    plan: &ExecutionTransactionPlan,
    slippage_bps: f64,
    context: &str,
) -> Result<Value> {
    if let Some(raw) = plan
        .metadata
        .quote_response_json
        .as_deref()
        .map(str::trim)
        .filter(|raw| !raw.is_empty())
    {
        let mut value: Value = serde_json::from_str(raw)
            .map_err(|error| anyhow!("invalid quote_response_json for {context} body: {error}"))?;
        if !value.is_object() {
            return Err(anyhow!(
                "quote_response_json must be an object for {context} body"
            ));
        }
        if let Some(object) = value.as_object_mut() {
            object.remove("_copybot");
        }
        return Ok(value);
    }
    let blueprint = plan
        .swap_blueprint
        .as_ref()
        .ok_or_else(|| anyhow!("missing swap blueprint for {context} quote response"))?;
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
    let slippage_bps = slippage_bps.round() as u64;
    Ok(json!({
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

pub(crate) fn primary_swap_builder_endpoint(
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
    endpoint: &str,
    context: &str,
) -> Result<SwapBuilderEndpoint> {
    if should_use_selected_public_builder(config, plan) {
        return Ok(SwapBuilderEndpoint {
            url: swap_endpoint_url(&config.quote_canary_public_base_url, endpoint, context)?,
            api_key: String::new(),
            source: SwapBuilderSource::PublicSelected,
        });
    }
    Ok(SwapBuilderEndpoint {
        url: swap_endpoint_url(&config.quote_canary_base_url, endpoint, context)?,
        api_key: config.quote_canary_api_key.trim().to_string(),
        source: SwapBuilderSource::Metis,
    })
}

pub(crate) fn public_fallback_swap_builder_endpoint(
    config: &ExecutionConfig,
    endpoint: &str,
    context: &str,
) -> Result<SwapBuilderEndpoint> {
    Ok(SwapBuilderEndpoint {
        url: swap_endpoint_url(&config.quote_canary_public_base_url, endpoint, context)?,
        api_key: String::new(),
        source: SwapBuilderSource::PublicFallback,
    })
}

pub(crate) fn metis_fallback_swap_builder_endpoint(
    config: &ExecutionConfig,
    endpoint: &str,
    context: &str,
) -> Result<SwapBuilderEndpoint> {
    Ok(SwapBuilderEndpoint {
        url: swap_endpoint_url(&config.quote_canary_base_url, endpoint, context)?,
        api_key: config.quote_canary_api_key.trim().to_string(),
        source: SwapBuilderSource::MetisFallback,
    })
}

fn should_use_selected_public_builder(
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> bool {
    plan.metadata.quote_source.as_deref() == Some(QUOTE_SOURCE_GENERIC_PUBLIC)
        && config.quote_canary_public_parallel_enabled
        && !config.quote_canary_public_base_url.trim().is_empty()
        && config.quote_canary_public_base_url.trim() != config.quote_canary_base_url.trim()
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
