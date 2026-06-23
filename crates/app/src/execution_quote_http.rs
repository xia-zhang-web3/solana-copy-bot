use crate::execution_quote_canary_helpers::{
    elapsed_ms, numeric_field, quote_url, string_field, truncate_for_log, QuoteSample,
};
use anyhow::{anyhow, Result};
use copybot_config::ExecutionConfig;
use serde_json::Value;
use std::time::{Duration as StdDuration, Instant};

const TOKEN_NOT_TRADABLE: &str = "TOKEN_NOT_TRADABLE";
const TOKEN_NOT_TRADABLE_RETRY_DELAYS_MS: [u64; 3] = [100, 300, 700];

pub(crate) async fn fetch_quote_sample(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    input_mint: &str,
    output_mint: &str,
    amount_raw: &str,
    slippage_bps: u64,
) -> Result<QuoteSample> {
    fetch_quote_sample_from_base_url(
        http,
        &config.quote_canary_base_url,
        &config.quote_canary_api_key,
        config.quote_canary_timeout_ms,
        input_mint,
        output_mint,
        amount_raw,
        slippage_bps,
    )
    .await
}

pub(crate) async fn fetch_quote_sample_from_base_url(
    http: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    timeout_ms: u64,
    input_mint: &str,
    output_mint: &str,
    amount_raw: &str,
    slippage_bps: u64,
) -> Result<QuoteSample> {
    let url = quote_url(base_url)?;
    let timeout = StdDuration::from_millis(timeout_ms.max(1));
    let slippage_bps = slippage_bps.to_string();
    let started = Instant::now();
    let api_key = api_key.trim().to_string();
    for attempt in 0..=TOKEN_NOT_TRADABLE_RETRY_DELAYS_MS.len() {
        let mut request = http
            .get(&url)
            .query(&[
                ("inputMint", input_mint),
                ("outputMint", output_mint),
                ("amount", amount_raw),
                ("slippageBps", slippage_bps.as_str()),
                ("swapMode", "ExactIn"),
                ("instructionVersion", "V2"),
            ])
            .timeout(timeout);
        if !api_key.is_empty() {
            request = request.header("x-api-key", api_key.as_str());
        }
        match fetch_quote_json_once(request).await {
            Ok(value) => return quote_sample_from_json(value, started),
            Err(error)
                if error.retryable_token_not_tradable
                    && attempt < TOKEN_NOT_TRADABLE_RETRY_DELAYS_MS.len() =>
            {
                tokio::time::sleep(StdDuration::from_millis(
                    TOKEN_NOT_TRADABLE_RETRY_DELAYS_MS[attempt],
                ))
                .await;
            }
            Err(error) => return Err(anyhow!(error.message)),
        }
    }
    Err(anyhow!("quote canary retry loop exhausted"))
}

struct QuoteHttpError {
    message: String,
    retryable_token_not_tradable: bool,
}

async fn fetch_quote_json_once(request: reqwest::RequestBuilder) -> Result<Value, QuoteHttpError> {
    let response = request.send().await.map_err(|error| QuoteHttpError {
        message: format!("quote canary request failed: {error}"),
        retryable_token_not_tradable: false,
    })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let retryable_token_not_tradable = body.contains(TOKEN_NOT_TRADABLE);
        return Err(QuoteHttpError {
            message: format!(
                "quote canary returned HTTP {status}: {}",
                truncate_for_log(&body, 240)
            ),
            retryable_token_not_tradable,
        });
    }
    response.json().await.map_err(|error| QuoteHttpError {
        message: format!("quote canary response JSON decode failed: {error}"),
        retryable_token_not_tradable: false,
    })
}

fn quote_sample_from_json(value: Value, started: Instant) -> Result<QuoteSample> {
    let in_amount = string_field(&value, "inAmount")
        .ok_or_else(|| anyhow!("quote canary response missing inAmount"))?;
    let out_amount = string_field(&value, "outAmount")
        .ok_or_else(|| anyhow!("quote canary response missing outAmount"))?;
    Ok(QuoteSample {
        in_amount,
        out_amount,
        response_json: value.to_string(),
        price_impact_pct: numeric_field(&value, "priceImpactPct"),
        route_plan_json: value
            .get("routePlan")
            .map(|route| route.to_string())
            .filter(|raw| !raw.is_empty()),
        in_decimals: decimal_field(value.get("inDecimals"))
            .or_else(|| decimal_field(value.get("inputDecimals")))
            .or_else(|| decimal_field(value.pointer("/meta/inDecimals")))
            .or_else(|| decimal_field(value.pointer("/inputToken/decimals"))),
        out_decimals: decimal_field(value.get("outDecimals"))
            .or_else(|| decimal_field(value.get("outputDecimals")))
            .or_else(|| decimal_field(value.pointer("/meta/outDecimals")))
            .or_else(|| decimal_field(value.pointer("/outputToken/decimals"))),
        latency_ms: elapsed_ms(started),
    })
}

fn decimal_field(value: Option<&Value>) -> Option<u8> {
    match value? {
        Value::Number(number) => number.as_u64().and_then(|raw| u8::try_from(raw).ok()),
        Value::String(raw) => raw.parse::<u8>().ok(),
        _ => None,
    }
}
