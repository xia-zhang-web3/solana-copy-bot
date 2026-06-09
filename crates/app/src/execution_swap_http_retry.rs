use crate::execution_quote_canary_helpers::{elapsed_ms, truncate_for_log};
use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use std::time::{Duration as StdDuration, Instant};

pub(crate) const MISSING_TOKEN_PROGRAM: &str = "Missing token program";
const RETRY_DELAYS_MS: [u64; 2] = [250, 750];

#[derive(Debug)]
pub(crate) struct SwapHttpJsonResponse {
    pub(crate) value: Value,
    pub(crate) elapsed_ms: u64,
    pub(crate) attempts: usize,
}

pub(crate) async fn post_swap_json_with_retry(
    http: &reqwest::Client,
    url: String,
    api_key: &str,
    body: &Value,
    timeout: StdDuration,
    context: &str,
) -> Result<SwapHttpJsonResponse> {
    let started = Instant::now();
    for attempt in 0..=RETRY_DELAYS_MS.len() {
        let mut request = http.post(&url).json(body).timeout(timeout);
        if !api_key.trim().is_empty() {
            request = request.header("x-api-key", api_key.trim());
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("{context} request failed"))?;
        let status = response.status();
        if status.is_success() {
            let value = response
                .json()
                .await
                .with_context(|| format!("{context} response JSON decode failed"))?;
            return Ok(SwapHttpJsonResponse {
                value,
                elapsed_ms: elapsed_ms(started),
                attempts: attempt + 1,
            });
        }
        let response_body = response.text().await.unwrap_or_default();
        if should_retry_missing_token_program(&response_body, attempt) {
            tokio::time::sleep(StdDuration::from_millis(RETRY_DELAYS_MS[attempt])).await;
            continue;
        }
        return Err(anyhow!(
            "{context} returned HTTP {status}: {}",
            truncate_for_log(&response_body, 240)
        ));
    }
    Err(anyhow!("{context} retry loop exhausted"))
}

fn should_retry_missing_token_program(response_body: &str, attempt: usize) -> bool {
    attempt < RETRY_DELAYS_MS.len() && response_body.contains(MISSING_TOKEN_PROGRAM)
}

pub(crate) fn is_missing_token_program_error(error: &anyhow::Error) -> bool {
    error.to_string().contains(MISSING_TOKEN_PROGRAM)
}

pub(crate) fn is_market_not_found_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("market_not_found")
        || (message.contains("market") && message.contains("not found"))
}
