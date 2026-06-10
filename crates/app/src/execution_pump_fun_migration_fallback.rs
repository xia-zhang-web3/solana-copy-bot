use crate::execution_quote_canary_helpers::{
    quote_canary_slippage_limit_bps, DECISION_WOULD_EXECUTE, QUOTE_STATUS_OK, SIDE_SELL, SOL_MINT,
};
use crate::execution_quote_http::fetch_quote_sample_from_base_url;
use crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS;
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use copybot_config::ExecutionConfig;

pub(crate) async fn refresh_pump_fun_paid_sell_to_generic_pumpswap_plan(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<ExecutionTransactionPlan> {
    if !plan.side.eq_ignore_ascii_case(SIDE_SELL) {
        return Err(anyhow!("Pump.fun migration fallback only supports sells"));
    }
    let blueprint = plan
        .swap_blueprint
        .as_ref()
        .ok_or_else(|| anyhow!("missing swap blueprint for Pump.fun migration fallback"))?;
    let quote = fetch_quote_sample_from_base_url(
        http,
        &config.quote_canary_base_url,
        &config.quote_canary_api_key,
        config.quote_canary_timeout_ms,
        &blueprint.input_mint,
        SOL_MINT,
        &blueprint.input_amount_raw,
        quote_canary_slippage_limit_bps(config, SIDE_SELL),
    )
    .await
    .context("Pump.fun migration fallback generic quote failed")?;

    let mut metadata = plan.metadata.clone();
    metadata.quote_source = Some(QUOTE_SOURCE_GENERIC_METIS.to_string());
    metadata.quote_request_ts = Some(Utc::now());
    metadata.quote_status = Some(QUOTE_STATUS_OK.to_string());
    metadata.quote_in_amount_raw = Some(quote.in_amount);
    metadata.quote_out_amount_raw = Some(quote.out_amount.clone());
    metadata.quote_response_json = Some(quote.response_json);
    metadata.price_impact_pct = quote.price_impact_pct;
    metadata.route_plan_json = quote.route_plan_json;
    metadata.decision_status = Some(DECISION_WOULD_EXECUTE.to_string());
    metadata.decision_reason = Some("pump_fun_bonding_curve_miss_generic_requote".to_string());
    metadata.quote_price_sol = None;

    let mut fallback = plan.clone();
    fallback.metadata = metadata;
    Ok(fallback)
}
