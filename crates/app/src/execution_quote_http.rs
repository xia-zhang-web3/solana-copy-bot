use crate::execution_quote_canary_helpers::{
    elapsed_ms, numeric_field, quote_url, string_field, truncate_for_log, QuoteSample,
};
use anyhow::{anyhow, Context, Result};
use copybot_config::ExecutionConfig;
use serde_json::Value;
use std::time::{Duration as StdDuration, Instant};

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
    let mut request = http
        .get(url)
        .query(&[
            ("inputMint", input_mint),
            ("outputMint", output_mint),
            ("amount", amount_raw),
            ("slippageBps", slippage_bps.as_str()),
            ("swapMode", "ExactIn"),
        ])
        .timeout(timeout);
    let api_key = api_key.trim();
    if !api_key.is_empty() {
        request = request.header("x-api-key", api_key);
    }
    let response = request
        .send()
        .await
        .context("quote canary request failed")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "quote canary returned HTTP {status}: {}",
            truncate_for_log(&body, 240)
        ));
    }
    let value = response
        .json()
        .await
        .context("quote canary response JSON decode failed")?;
    quote_sample_from_json(value, started)
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
        in_decimals: None,
        out_decimals: None,
        latency_ms: elapsed_ms(started),
    })
}
