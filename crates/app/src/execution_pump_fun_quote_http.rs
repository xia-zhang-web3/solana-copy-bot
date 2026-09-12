use crate::execution_quote_canary_helpers::{
    numeric_field, string_field, truncate_for_log, QuoteSample,
};
use crate::execution_quote_timing::{complete_attempt, QuoteAttemptClock, QuoteAttemptResult};
use anyhow::{anyhow, Context, Result};
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use std::time::Duration as StdDuration;

pub(crate) async fn fetch_pump_fun_quote_sample(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    side: &str,
    mint: &str,
    amount_raw: &str,
) -> QuoteAttemptResult {
    let mut clock = None;
    let result = async {
        let url = pump_fun_quote_url(&config.quote_canary_base_url)?;
        let timeout = StdDuration::from_millis(config.quote_canary_timeout_ms.max(1));
        let side = side.to_ascii_uppercase();
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
        let request = request
            .build()
            .context("pump.fun paid quote request failed")?;
        clock = Some(QuoteAttemptClock::start());
        let response = http
            .execute(request)
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
        pump_fun_quote_sample_from_json(value)
            .map(crate::execution_quote_timing::response_available)
    }
    .await;
    complete_attempt(result, clock)
}

fn pump_fun_quote_sample_from_json(value: Value) -> Result<QuoteSample> {
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
        http_request_started_ts: None,
        quote_response_available_ts: None,
        latency_ms: 0,
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
