use crate::execution_pump_fun_quote_http::fetch_pump_fun_quote_sample;
use crate::execution_quote_canary_helpers::{
    quote_canary_slippage_limit_bps, quote_slippage_bps_for_buy, sol_to_lamports_raw,
    DECISION_UNKNOWN, DECISION_WOULD_EXECUTE, DECISION_WOULD_SKIP, QUOTE_STATUS_ERROR,
    QUOTE_STATUS_OK, SIDE_BUY, SOL_MINT,
};
use crate::execution_quote_http::fetch_quote_sample_from_base_url;
use crate::execution_quote_provider_selection::{
    QUOTE_SOURCE_GENERIC_METIS, QUOTE_SOURCE_PUMP_FUN_PAID,
};
use crate::execution_submit_adapter::ExecutionBuildPlanMetadata;
use anyhow::Result;
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use serde_json::{Map, Number, Value};

pub(crate) const FRESH_SUBMIT_QUOTE_ERROR: &str = "fresh_submit_quote_error";
pub(crate) const FRESH_SUBMIT_QUOTE_INVALID_PRICE: &str = "fresh_submit_quote_invalid_price";
pub(crate) const FRESH_SUBMIT_QUOTE_SLIPPAGE_ABOVE_LIMIT: &str =
    "fresh_submit_quote_slippage_above_limit";
pub(crate) const FRESH_SUBMIT_QUOTE_WITHIN_SLIPPAGE: &str =
    "fresh_submit_quote_within_slippage_limit";
const COPYBOT_QUOTE_SIDECAR: &str = "_copybot";

pub(crate) async fn refresh_tiny_buy_build_plan_metadata(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    signal: &CopySignalRow,
    metadata: ExecutionBuildPlanMetadata,
) -> Result<ExecutionBuildPlanMetadata> {
    if !signal.side.eq_ignore_ascii_case(SIDE_BUY) {
        return Ok(metadata);
    }
    if metadata.quote_event_id.as_deref().is_none_or(str::is_empty) {
        return Ok(metadata);
    }

    let amount_raw = sol_to_lamports_raw(config.canary_buy_size_sol)?;
    let max_slippage_bps = quote_canary_slippage_limit_bps(config, SIDE_BUY);
    if metadata.quote_source.as_deref() == Some(QUOTE_SOURCE_PUMP_FUN_PAID) {
        match fetch_pump_fun_quote_sample(http, config, SIDE_BUY, &signal.token, &amount_raw).await
        {
            Ok(quote) if pump_fun_quote_is_completed(&quote.response_json) != Some(true) => {
                return Ok(apply_fresh_quote(
                    metadata,
                    quote,
                    max_slippage_bps,
                    QUOTE_SOURCE_PUMP_FUN_PAID,
                ));
            }
            Ok(_) => {}
            Err(_) => {
                return Ok(fresh_quote_error_metadata(
                    metadata,
                    QUOTE_SOURCE_PUMP_FUN_PAID,
                ));
            }
        }
    }

    let (quote_source, base_url, api_key) = generic_refresh_source(config, &metadata);
    match fetch_quote_sample_from_base_url(
        http,
        base_url,
        api_key,
        config.quote_canary_timeout_ms,
        SOL_MINT,
        &signal.token,
        &amount_raw,
        max_slippage_bps,
    )
    .await
    {
        Ok(quote) => Ok(apply_fresh_quote(
            metadata,
            quote,
            max_slippage_bps,
            quote_source,
        )),
        Err(_) => {
            retry_fresh_pump_fun_quote_or_error(
                http,
                config,
                signal,
                metadata,
                amount_raw,
                max_slippage_bps,
                quote_source,
            )
            .await
        }
    }
}

fn generic_refresh_source<'a>(
    config: &'a ExecutionConfig,
    _metadata: &ExecutionBuildPlanMetadata,
) -> (&'static str, &'a str, &'a str) {
    (
        QUOTE_SOURCE_GENERIC_METIS,
        &config.quote_canary_base_url,
        &config.quote_canary_api_key,
    )
}

pub(crate) fn fresh_submit_gate_reason(
    metadata: &ExecutionBuildPlanMetadata,
    default: &'static str,
) -> &'static str {
    match metadata.decision_reason.as_deref() {
        Some(FRESH_SUBMIT_QUOTE_ERROR) => FRESH_SUBMIT_QUOTE_ERROR,
        Some(FRESH_SUBMIT_QUOTE_INVALID_PRICE) => FRESH_SUBMIT_QUOTE_INVALID_PRICE,
        Some(FRESH_SUBMIT_QUOTE_SLIPPAGE_ABOVE_LIMIT) => FRESH_SUBMIT_QUOTE_SLIPPAGE_ABOVE_LIMIT,
        _ => default,
    }
}

fn fresh_quote_error_metadata(
    mut metadata: ExecutionBuildPlanMetadata,
    quote_source: &str,
) -> ExecutionBuildPlanMetadata {
    metadata.quote_source = Some(quote_source.to_string());
    metadata.quote_status = Some(QUOTE_STATUS_ERROR.to_string());
    metadata.decision_status = Some(DECISION_UNKNOWN.to_string());
    metadata.decision_reason = Some(FRESH_SUBMIT_QUOTE_ERROR.to_string());
    metadata
}

async fn retry_fresh_pump_fun_quote_or_error(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    signal: &CopySignalRow,
    metadata: ExecutionBuildPlanMetadata,
    amount_raw: String,
    max_slippage_bps: u64,
    quote_source: &str,
) -> Result<ExecutionBuildPlanMetadata> {
    if !should_retry_fresh_pump_fun_quote(config, &metadata) {
        return Ok(fresh_quote_error_metadata(metadata, quote_source));
    }
    match fetch_pump_fun_quote_sample(http, config, SIDE_BUY, &signal.token, &amount_raw).await {
        Ok(quote) if pump_fun_quote_is_completed(&quote.response_json) != Some(true) => {
            Ok(apply_fresh_quote(
                metadata,
                quote,
                max_slippage_bps,
                QUOTE_SOURCE_PUMP_FUN_PAID,
            ))
        }
        _ => Ok(fresh_quote_error_metadata(metadata, quote_source)),
    }
}

fn should_retry_fresh_pump_fun_quote(
    config: &ExecutionConfig,
    metadata: &ExecutionBuildPlanMetadata,
) -> bool {
    config.quote_canary_pump_fun_parallel_enabled
        && route_plan_has_pump_fun_amm(metadata.route_plan_json.as_deref())
}

fn route_plan_has_pump_fun_amm(route_plan_json: Option<&str>) -> bool {
    let Some(raw) = route_plan_json.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        return false;
    };
    value.as_array().is_some_and(|items| {
        items.iter().any(|item| {
            item.pointer("/swapInfo/label")
                .and_then(Value::as_str)
                .is_some_and(|label| label.eq_ignore_ascii_case("Pump.fun Amm"))
        })
    })
}

fn apply_fresh_quote(
    mut metadata: ExecutionBuildPlanMetadata,
    quote: crate::execution_quote_canary_helpers::QuoteSample,
    max_slippage_bps: u64,
    quote_source: &str,
) -> ExecutionBuildPlanMetadata {
    let price_and_slippage = fresh_buy_price_and_slippage(&metadata, &quote.out_amount);
    let quote_response_json = quote_response_with_copybot_sidecar(
        quote.response_json,
        metadata.quote_response_json.as_deref(),
    );

    metadata.quote_source = Some(quote_source.to_string());
    metadata.quote_request_ts = Some(Utc::now());
    metadata.quote_status = Some(QUOTE_STATUS_OK.to_string());
    metadata.quote_in_amount_raw = Some(quote.in_amount);
    metadata.quote_out_amount_raw = Some(quote.out_amount);
    metadata.quote_response_json = Some(quote_response_json);
    metadata.price_impact_pct = quote.price_impact_pct;
    metadata.route_plan_json = quote.route_plan_json;

    let Some((quote_price, slippage_bps)) = price_and_slippage else {
        metadata.quote_price_sol = None;
        metadata.slippage_bps = None;
        metadata.decision_status = Some(DECISION_UNKNOWN.to_string());
        metadata.decision_reason = Some(FRESH_SUBMIT_QUOTE_INVALID_PRICE.to_string());
        return metadata;
    };
    metadata.quote_price_sol = Some(quote_price);
    metadata.slippage_bps = Some(slippage_bps);
    if slippage_bps <= max_slippage_bps as f64 {
        metadata.decision_status = Some(DECISION_WOULD_EXECUTE.to_string());
        metadata.decision_reason = Some(FRESH_SUBMIT_QUOTE_WITHIN_SLIPPAGE.to_string());
    } else {
        metadata.decision_status = Some(DECISION_WOULD_SKIP.to_string());
        metadata.decision_reason = Some(FRESH_SUBMIT_QUOTE_SLIPPAGE_ABOVE_LIMIT.to_string());
    }
    metadata
}

fn pump_fun_quote_is_completed(raw: &str) -> Option<bool> {
    serde_json::from_str::<Value>(raw).ok().and_then(|value| {
        value
            .pointer("/quote/meta/isCompleted")
            .and_then(Value::as_bool)
    })
}

fn fresh_buy_price_and_slippage(
    metadata: &ExecutionBuildPlanMetadata,
    fresh_out_amount_raw: &str,
) -> Option<(f64, f64)> {
    let old_quote_price = metadata.quote_price_sol?;
    let old_slippage_bps = metadata.slippage_bps?;
    let old_out_amount = metadata
        .quote_out_amount_raw
        .as_deref()?
        .parse::<f64>()
        .ok()?;
    let fresh_out_amount = fresh_out_amount_raw.parse::<f64>().ok()?;
    if !old_quote_price.is_finite()
        || old_quote_price <= 0.0
        || !old_slippage_bps.is_finite()
        || old_out_amount <= 0.0
        || fresh_out_amount <= 0.0
    {
        return None;
    }
    let shadow_denominator = 1.0 + old_slippage_bps / 10_000.0;
    if !shadow_denominator.is_finite() || shadow_denominator <= 0.0 {
        return None;
    }
    let fresh_quote_price = old_quote_price * old_out_amount / fresh_out_amount;
    let shadow_price = old_quote_price / shadow_denominator;
    let fresh_slippage = quote_slippage_bps_for_buy(Some(fresh_quote_price), Some(shadow_price))?;
    Some((fresh_quote_price, fresh_slippage))
}

fn quote_response_with_copybot_sidecar(fresh_raw: String, old_raw: Option<&str>) -> String {
    let Some(old_value) = old_raw.and_then(|raw| serde_json::from_str::<Value>(raw).ok()) else {
        return fresh_raw;
    };
    let Some(out_decimals) = quote_output_decimals(&old_value) else {
        return fresh_raw;
    };
    let Ok(mut fresh_value) = serde_json::from_str::<Value>(&fresh_raw) else {
        return fresh_raw;
    };
    let Some(object) = fresh_value.as_object_mut() else {
        return fresh_raw;
    };
    let mut sidecar = Map::new();
    sidecar.insert(
        "outDecimals".to_string(),
        Value::Number(Number::from(u64::from(out_decimals))),
    );
    object.insert(COPYBOT_QUOTE_SIDECAR.to_string(), Value::Object(sidecar));
    fresh_value.to_string()
}

fn quote_output_decimals(value: &Value) -> Option<u8> {
    for path in [
        "/quote/meta/outDecimals",
        "/meta/outDecimals",
        "/quote/outDecimals",
        "/outDecimals",
    ] {
        if let Some(raw) = value.pointer(path).and_then(Value::as_u64) {
            if let Ok(decimals) = u8::try_from(raw) {
                return Some(decimals);
            }
        }
    }
    None
}
