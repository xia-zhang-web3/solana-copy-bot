use crate::execution_quote_canary_helpers::{
    elapsed_ms, numeric_field, string_field, truncate_for_log, QuoteSample,
};
use anyhow::{anyhow, Context, Result};
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use std::time::{Duration as StdDuration, Instant};

pub(crate) async fn fetch_pump_fun_quote_sample(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    side: &str,
    mint: &str,
    amount_raw: &str,
) -> Result<QuoteSample> {
    let url = pump_fun_quote_url(&config.quote_canary_base_url)?;
    let timeout = StdDuration::from_millis(config.quote_canary_timeout_ms.max(1));
    let side = side.to_ascii_uppercase();
    let started = Instant::now();
    let mut request = http
        .get(url)
        .query(&[
            ("mint", mint),
            ("type", side.as_str()),
            ("amount", amount_raw),
        ])
        .timeout(timeout);
    let api_key = config.quote_canary_api_key.trim();
    if !api_key.is_empty() {
        request = request.header("x-api-key", api_key);
    }
    let response = request
        .send()
        .await
        .context("pump.fun paid quote request failed")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "pump.fun paid quote returned HTTP {status}: {}",
            truncate_for_log(&body, 240)
        ));
    }
    let value = response
        .json()
        .await
        .context("pump.fun paid quote response JSON decode failed")?;
    pump_fun_quote_sample_from_json(value, started)
}

fn pump_fun_quote_sample_from_json(value: Value, started: Instant) -> Result<QuoteSample> {
    let quote = value
        .get("quote")
        .ok_or_else(|| anyhow!("pump.fun paid quote response missing quote"))?;
    let in_amount = string_field(quote, "inAmount")
        .ok_or_else(|| anyhow!("pump.fun paid quote response missing quote.inAmount"))?;
    let out_amount = string_field(quote, "outAmount")
        .ok_or_else(|| anyhow!("pump.fun paid quote response missing quote.outAmount"))?;
    Ok(QuoteSample {
        in_amount,
        out_amount,
        response_json: value.to_string(),
        price_impact_pct: numeric_field(quote, "priceImpactPct"),
        route_plan_json: Some(pump_fun_paid_route_plan_json()),
        in_decimals: decimal_field(quote.pointer("/meta/inDecimals")),
        out_decimals: decimal_field(quote.pointer("/meta/outDecimals")),
        latency_ms: elapsed_ms(started),
    })
}

fn pump_fun_quote_url(base_url: &str) -> Result<String> {
    let trimmed = base_url.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("quote canary base URL is empty"));
    }
    let without_slash = trimmed.trim_end_matches('/');
    if without_slash.ends_with("/pump-fun/quote") {
        Ok(without_slash.to_string())
    } else {
        Ok(format!("{without_slash}/pump-fun/quote"))
    }
}

fn decimal_field(value: Option<&Value>) -> Option<u8> {
    value
        .and_then(Value::as_u64)
        .and_then(|raw| u8::try_from(raw).ok())
}

fn pump_fun_paid_route_plan_json() -> String {
    json!([{"swapInfo":{"label":"Pump.fun Paid"}}]).to_string()
}
