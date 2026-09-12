use crate::execution_quote_canary_helpers::{
    numeric_field, quote_url, string_field, truncate_for_log, QuoteSample,
};
use crate::execution_quote_timing::{complete_attempt, QuoteAttemptClock, QuoteAttemptResult};
use anyhow::{anyhow, Result};
use copybot_config::ExecutionConfig;
use serde_json::Value;
use std::time::Duration as StdDuration;

const TOKEN_NOT_TRADABLE: &str = "TOKEN_NOT_TRADABLE";
const TOKEN_NOT_TRADABLE_RETRY_DELAYS_MS: [u64; 3] = [100, 300, 700];

pub(crate) async fn fetch_quote_sample(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    input_mint: &str,
    output_mint: &str,
    amount_raw: &str,
    slippage_bps: u64,
) -> QuoteAttemptResult {
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
) -> QuoteAttemptResult {
    let mut clock = None;
    let result = async {
        let api_key = api_key.trim().to_string();
        for attempt in 0..=TOKEN_NOT_TRADABLE_RETRY_DELAYS_MS.len() {
            let request = build_quote_request(
                http,
                base_url,
                &api_key,
                timeout_ms,
                input_mint,
                output_mint,
                amount_raw,
                slippage_bps,
            )?;
            clock.get_or_insert_with(QuoteAttemptClock::start);
            match fetch_quote_json_once(http, request).await {
                Ok(value) => {
                    return quote_sample_from_json(value)
                        .map(crate::execution_quote_timing::response_available)
                }
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
    .await;
    complete_attempt(result, clock)
}

struct QuoteHttpError {
    message: String,
    retryable_token_not_tradable: bool,
}

async fn fetch_quote_json_once(
    http: &reqwest::Client,
    request: reqwest::Request,
) -> Result<Value, QuoteHttpError> {
    let response = http
        .execute(request)
        .await
        .map_err(|error| QuoteHttpError {
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

pub(crate) fn quote_sample_from_json(value: Value) -> Result<QuoteSample> {
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
        http_request_started_ts: None,
        quote_response_available_ts: None,
        latency_ms: 0,
    })
}

fn decimal_field(value: Option<&Value>) -> Option<u8> {
    match value? {
        Value::Number(number) => number.as_u64().and_then(|raw| u8::try_from(raw).ok()),
        Value::String(raw) => raw.parse::<u8>().ok(),
        _ => None,
    }
}

// Shared request construction for legacy retries and one-attempt strict jobs.
pub(crate) fn build_quote_request(
    http: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    timeout_ms: u64,
    input_mint: &str,
    output_mint: &str,
    amount_raw: &str,
    slippage_bps: u64,
) -> Result<reqwest::Request> {
    let url = quote_url(base_url)?;
    let slippage_bps = slippage_bps.to_string();
    let mut request = http
        .get(url)
        .query(&[
            ("inputMint", input_mint),
            ("outputMint", output_mint),
            ("amount", amount_raw),
            ("slippageBps", slippage_bps.as_str()),
            ("swapMode", "ExactIn"),
            ("instructionVersion", "V2"),
        ])
        .timeout(StdDuration::from_millis(timeout_ms.max(1)));
    if !api_key.trim().is_empty() {
        request = request.header("x-api-key", api_key.trim());
    }
    request
        .build()
        .map_err(|error| anyhow!("quote canary request failed: {error}"))
}
