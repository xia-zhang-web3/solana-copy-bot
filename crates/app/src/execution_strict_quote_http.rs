//! One bounded generic HTTP observation, reusing the existing request/parser/timing.
//! Checks run after headers and every body await; no hidden provider retries.
use crate::execution_quote_http::{build_quote_request, quote_sample_from_json};
use crate::execution_quote_timing::{complete_attempt, QuoteAttemptClock};
use anyhow::{ensure, Result};
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_storage_core::ordered_sell_quote::{QuoteBinding, QuoteObservation, QuoteOutcome};
use sha2::{Digest, Sha256};

pub(super) async fn fetch(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    binding: &QuoteBinding,
    mut check: impl FnMut() -> Result<bool>,
) -> QuoteObservation {
    let mut clock = None;
    let mut response_ts = None;
    let mut digest = None;
    let mut stale = false;
    let mut recheck = || -> Result<()> {
        if !check()? {
            stale = true;
            anyhow::bail!("strict binding changed during HTTP");
        }
        Ok(())
    };
    let outcome = async {
        recheck()?;
        let request = build_quote_request(
            http,
            &config.quote_canary_base_url,
            &config.quote_canary_api_key,
            config.quote_canary_timeout_ms.min(10_000),
            &binding.mint,
            &binding.output_mint,
            &binding.raw.to_string(),
            crate::execution_quote_canary_helpers::quote_canary_slippage_limit_bps(config, "sell"),
        )?;
        clock = Some(QuoteAttemptClock::start());
        let mut response = http.execute(request).await;
        response_ts = response.is_ok().then(Utc::now);
        recheck()?;
        let response = response
            .as_mut()
            .map_err(|e| anyhow::anyhow!("strict quote request: {e}"))?;
        ensure!(
            response.status().is_success(),
            "strict quote HTTP status {}",
            response.status()
        );
        ensure!(
            response.content_length().unwrap_or(0) <= 65_536,
            "strict quote response budget"
        );
        let mut bytes = vec![];
        loop {
            let chunk = response.chunk().await;
            recheck()?;
            let Some(chunk) = chunk? else {
                break;
            };
            ensure!(
                bytes.len() + chunk.len() <= 65_536,
                "strict quote response budget"
            );
            bytes.extend_from_slice(&chunk);
        }
        digest = Some(format!("{:x}", Sha256::digest(&bytes)));
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        ensure!(
            value["inputMint"].as_str() == Some(&binding.mint)
                && value["outputMint"].as_str() == Some(&binding.output_mint)
                && value["swapMode"].as_str() == Some("ExactIn"),
            "strict quote response mint/side conflict"
        );
        let mut q = quote_sample_from_json(value)?;
        ensure!(
            q.in_amount == binding.raw.to_string()
                && q.in_decimals.is_none_or(|d| d == binding.decimals)
                && q.out_decimals.is_none_or(|d| d == 9),
            "strict quote response amount/decimals conflict"
        );
        let out_raw = q.out_amount.parse::<u64>()?;
        ensure!(out_raw > 0, "strict quote output raw unknown");
        q.out_amount = out_raw.to_string(); // exact raw, bounded decimal representation
        Ok(crate::execution_quote_timing::response_available(q))
    }
    .await;
    let ended = Utc::now();
    let result = complete_attempt(outcome, clock);
    let (outcome, reason, started, available, input, output) = match result {
        Ok(q) => (
            QuoteOutcome::Current,
            None,
            q.http_request_started_ts,
            q.quote_response_available_ts,
            Some(q.in_amount),
            Some(q.out_amount),
        ),
        Err(e) => (
            if stale {
                QuoteOutcome::Stale
            } else {
                QuoteOutcome::Unknown
            },
            Some(e.to_string().chars().take(512).collect()),
            e.timing.map(|t| t.started_ts),
            None,
            None,
            None,
        ),
    };
    QuoteObservation {
        version: 1,
        binding: Some(binding.clone()),
        outcome,
        reason,
        http_started: started,
        http_response: response_ts,
        quote_response_available_ts: available,
        http_ended: ended,
        response_in_raw: input,
        response_out_raw: output,
        response_sha256: digest,
        event_time: None,
        event_delay_ns: None,
    }
}
