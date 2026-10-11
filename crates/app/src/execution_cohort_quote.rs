//! Quote selection for the existing closed technical-cohort execution profile.
//! This selects a provider request, never grants admission, signing or retry authority.
use crate::execution_quote_canary_helpers::{QuoteSample, SOL_MINT};
use crate::execution_quote_timing::{complete_attempt, QuoteAttemptResult, QuoteHttpTiming};
use anyhow::{ensure, Context, Result};
use copybot_config::ExecutionConfig;
use serde_json::Value;

pub(crate) fn applies(c: &ExecutionConfig, id: &str, wallet: &str, side: &str) -> bool {
    copybot_config::native_first_buy_activation(c)
        && c.technical_cohort.as_ref().is_some_and(|p| p.activate
            && p.policy == copybot_config::TECHNICAL_COHORT_V1
            && c.tiny_experiment.id.as_deref() == Some(p.run_id.as_str())
            && p.wallet_ids.iter().any(|w| w == wallet))
        && ((side.eq_ignore_ascii_case("buy") && id.starts_with("native-buy-v1:"))
            || (side.eq_ignore_ascii_case("sell") && id.starts_with("source-sell:")))
}

pub(crate) fn validate(value: &Value, input: &str, output: &str, amount: &str,
    slippage: u64) -> Result<()> {
    let out = value["outAmount"].as_str().context("cohort_quote_output")?.parse::<u64>()?;
    let threshold = value["otherAmountThreshold"].as_str()
        .context("cohort_quote_threshold")?.parse::<u64>()?;
    ensure!(out > 0 && threshold > 0 && threshold <= out && slippage <= 50
        && u128::from(threshold) >= u128::from(out) * u128::from(10_000-slippage) / 10_000
        && value["inputMint"] == input && value["outputMint"] == output
        && value["inAmount"] == amount && value["swapMode"] == "ExactIn"
        && value["slippageBps"].as_u64() == Some(slippage)
        && value.get("instructionVersion").is_none_or(|v| v == "V1")
        && value.get("platformFee").is_none_or(|v| v.is_null() || v["feeBps"] == 0),
        "cohort_quote_profile_binding");
    let route = value["routePlan"].as_array().context("cohort_quote_route")?;
    ensure!(route.len() == 1 && (route[0]["percent"] == 100 || route[0]["bps"] == 10_000)
        && route[0].get("percent").is_none_or(|v| v.is_null() || v == 100)
        && route[0].get("bps").is_none_or(|v| v.is_null() || v == 10_000),
        "cohort_quote_route_shape");
    let swap = &route[0]["swapInfo"];
    ensure!(swap["label"] == "Raydium" && swap["inputMint"] == input
        && swap["outputMint"] == output && swap["inAmount"] == amount
        && swap["outAmount"] == value["outAmount"], "cohort_quote_route_binding");
    crate::execution_pumpswap_accounts::parse_pubkey(
        swap["ammKey"].as_str().context("cohort_quote_amm")?, "cohort_quote_amm")?;
    Ok(())
}

pub(crate) async fn fetch(http: &reqwest::Client, c: &ExecutionConfig,
    input: &str, output: &str, amount: &str, slippage: u64) -> QuoteAttemptResult {
    let quote = if input == SOL_MINT && output != SOL_MINT {
        super::fetch_owner_quote_sample(http, c, output, amount, slippage).await
    } else if output == SOL_MINT && input != SOL_MINT {
        super::fetch_owner_exit_quote_sample(http, c, input, amount, slippage).await
    } else {
        return complete_attempt(Err(anyhow::anyhow!("cohort_quote_direction")), None);
    }?;
    checked(quote, input, output, amount, slippage)
}

fn checked(quote: QuoteSample, input: &str, output: &str, amount: &str,
    slippage: u64) -> QuoteAttemptResult {
    let validation = serde_json::from_str(&quote.response_json).map_err(anyhow::Error::from)
        .and_then(|v| validate(&v, input, output, amount, slippage));
    if let Err(error) = validation {
        let mut rejected = complete_attempt(Err(error), None).unwrap_err();
        rejected.timing = quote.http_request_started_ts.map(|started_ts|
            QuoteHttpTiming { started_ts, elapsed_ms: quote.latency_ms });
        return Err(rejected);
    }
    Ok(quote)
}
