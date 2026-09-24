//! A single current ExactIn quote bound to the approved owner mint and amount.
use crate::execution_quote_canary_helpers::{
    DECISION_WOULD_EXECUTE, QUOTE_STATUS_OK, SOL_MINT,
};
use crate::execution_submit_adapter::ExecutionBuildPlanMetadata;
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_storage_core::OwnerTechnicalBuyIntent;
use serde_json::{json, Value};

pub(crate) async fn fetch(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    intent: &OwnerTechnicalBuyIntent,
    mint_decimals: u8,
) -> Result<ExecutionBuildPlanMetadata> {
    let quote = if crate::execution_native_floor_policy::protected::enabled(config) {
        crate::execution_quote_http::fetch_owner_quote_sample(http, config, &intent.mint,
            &intent.amount_lamports.to_string(), u64::from(intent.max_slippage_bps)).await
    } else {
        crate::execution_quote_http::fetch_quote_sample_from_base_url(
        http,
        &config.quote_canary_base_url,
        &config.quote_canary_api_key,
        config.quote_canary_timeout_ms,
        SOL_MINT,
        &intent.mint,
        &intent.amount_lamports.to_string(),
        u64::from(intent.max_slippage_bps),
        ).await
    }.map_err(|e| anyhow::anyhow!("owner_buy_fresh_quote: {e}"))?;
    let mut metadata = metadata(intent, mint_decimals, quote)?;
    let fee = crate::execution_quote_canary_priority_fee::PriorityFeeSampler::new(
        config.clone(), http.clone(),
    ).sample_if_enabled().await;
    match fee {
        Some(sample) => {
            ensure!(sample.status == QUOTE_STATUS_OK, "owner_buy_priority_sample");
            metadata.priority_fee_source = Some("owner_buy_live_sample".into());
            metadata.priority_fee_status = Some(sample.status);
            metadata.priority_fee_lamports = sample.lamports;
            metadata.priority_fee_json = sample.json;
        }
        None => {
            metadata.priority_fee_source = Some("owner_buy_zero_priority".into());
            metadata.priority_fee_status = Some(QUOTE_STATUS_OK.into());
            metadata.priority_fee_lamports = Some(0);
            metadata.priority_fee_json = Some(json!({"version":1,
                "source":"owner_buy_zero_priority","unit":"total_priority_fee_lamports",
                "value":0}).to_string());
        }
    }
    let metadata = crate::execution_priority_fee::cap_metadata_total(
        intent.max_priority_fee_lamports, metadata,
    );
    crate::execution_priority_fee::metadata_fee(&metadata)?;
    Ok(metadata)
}

pub(crate) fn metadata(
    intent: &OwnerTechnicalBuyIntent,
    mint_decimals: u8,
    quote: crate::execution_quote_canary_helpers::QuoteSample,
) -> Result<ExecutionBuildPlanMetadata> {
    ensure!(mint_decimals <= 18, "owner_buy_mint_decimals");
    let raw: Value = serde_json::from_str(&quote.response_json)
        .context("owner_buy_quote_json")?;
    ensure!(raw["inputMint"] == SOL_MINT && raw["outputMint"] == intent.mint,
        "owner_buy_quote_mint");
    ensure!(raw["inAmount"] == intent.amount_lamports.to_string()
        && quote.in_amount == intent.amount_lamports.to_string(),
        "owner_buy_quote_amount");
    ensure!(raw["outAmount"] == quote.out_amount, "owner_buy_quote_output");
    ensure!(raw.get("swapMode").is_none_or(|v| v == "ExactIn"),
        "owner_buy_quote_mode");
    let slippage_bps = raw["slippageBps"].as_u64().context("owner_buy_quote_slippage_missing")?;
    ensure!(slippage_bps == u64::from(intent.max_slippage_bps) && slippage_bps <= 10_000,
        "owner_buy_quote_slippage_binding");
    ensure!(quote.out_decimals.is_none_or(|v| v == mint_decimals),
        "owner_buy_quote_decimals");
    let output = quote.out_amount.parse::<u64>()?;
    let threshold = raw["otherAmountThreshold"].as_str()
        .context("owner_buy_quote_threshold")?.parse::<u64>()?;
    ensure!(output > 0 && threshold > 0 && threshold <= output,
        "owner_buy_quote_threshold");
    // Jupiter integer minimum rounds down; reverse-ceiling its ratio invents an
    // extra basis point for non-divisible output. Keep the validated bps unchanged.
    let minimum = u128::from(output) * u128::from(10_000 - slippage_bps) / 10_000;
    ensure!(u128::from(threshold) >= minimum, "owner_buy_quote_slippage");
    let qty = output as f64 / 10_f64.powi(i32::from(mint_decimals));
    let quote_price_sol = (intent.amount_lamports as f64 / 1_000_000_000.0) / qty;
    ensure!(quote_price_sol.is_finite() && quote_price_sol > 0.0,
        "owner_buy_quote_price");
    ensure!(quote.route_plan_json.as_deref().is_some_and(|v| !v.is_empty()),
        "owner_buy_route_plan");
    let response_ts = quote.quote_response_available_ts.context("owner_buy_quote_time")?;
    ensure!(Utc::now().signed_duration_since(response_ts).num_seconds() <= 30,
        "owner_buy_quote_stale");
    let event_id = format!("owner-buy:{}:quote:{}", intent.intent_id,
        crate::execution_owned_sell_rpc::digest(quote.response_json.as_bytes()));
    Ok(ExecutionBuildPlanMetadata {
        quote_event_id: Some(event_id),
        quote_request_ts: quote.http_request_started_ts,
        quote_response_available_ts: Some(response_ts),
        http_request_started_ts: quote.http_request_started_ts,
        quote_source: Some("owner_buy_generic_metis".into()),
        quote_status: Some(QUOTE_STATUS_OK.into()),
        quote_in_amount_raw: Some(quote.in_amount),
        quote_out_amount_raw: Some(quote.out_amount),
        quote_response_json: Some(quote.response_json),
        quote_price_sol: Some(quote_price_sol),
        price_impact_pct: quote.price_impact_pct,
        route_plan_json: quote.route_plan_json,
        slippage_bps: Some(slippage_bps as f64),
        decision_status: Some(DECISION_WOULD_EXECUTE.into()),
        decision_reason: Some("owner_buy_fresh_quote_within_limit".into()),
        ..Default::default()
    })
}
