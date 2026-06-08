use crate::execution_quote_canary_helpers::{
    quote_canary_slippage_limit_bps, DECISION_WOULD_EXECUTE, QUOTE_STATUS_OK, SIDE_BUY,
};
use crate::execution_quote_provider_selection::quote_response_requires_fee_account;
use crate::execution_submit_adapter::ExecutionBuildPlanMetadata;
use copybot_config::ExecutionConfig;

pub(crate) const ENTRY_GATE_MISSING_QUOTE_METADATA: &str = "missing_quote_metadata";
pub(crate) const ENTRY_GATE_QUOTE_STATUS_NOT_OK: &str = "quote_status_not_ok";
pub(crate) const ENTRY_GATE_DECISION_NOT_EXECUTE: &str = "entry_decision_not_execute";
pub(crate) const ENTRY_GATE_MISSING_QUOTE_AMOUNTS: &str = "missing_quote_amounts";
pub(crate) const ENTRY_GATE_QUOTE_AMOUNT_MISMATCH: &str = "quote_amount_mismatch";
pub(crate) const ENTRY_GATE_INVALID_QUOTE_PRICE: &str = "invalid_quote_price";
pub(crate) const ENTRY_GATE_MISSING_ROUTE_PLAN: &str = "missing_route_plan";
pub(crate) const ENTRY_GATE_MISSING_SLIPPAGE: &str = "missing_entry_slippage";
pub(crate) const ENTRY_GATE_SLIPPAGE_ABOVE_LIMIT: &str = "entry_slippage_above_limit";
pub(crate) const ENTRY_GATE_MISSING_PRIORITY_FEE: &str = "missing_priority_fee";
pub(crate) const ENTRY_GATE_PRIORITY_FEE_NOT_OK: &str = "priority_fee_not_ok";
pub(crate) const ENTRY_GATE_PRIORITY_FEE_ABOVE_CAP: &str = "priority_fee_above_cap";
pub(crate) const ENTRY_GATE_PLATFORM_FEE_REQUIRES_FEE_ACCOUNT: &str =
    "platform_fee_requires_fee_account";

pub(crate) fn validate_execution_canary_entry_metadata(
    config: &ExecutionConfig,
    metadata: &ExecutionBuildPlanMetadata,
) -> Option<&'static str> {
    if metadata.quote_event_id.as_deref().is_none_or(str::is_empty) {
        return Some(ENTRY_GATE_MISSING_QUOTE_METADATA);
    }
    if metadata.quote_status.as_deref() != Some(QUOTE_STATUS_OK) {
        return Some(ENTRY_GATE_QUOTE_STATUS_NOT_OK);
    }
    if metadata.decision_status.as_deref() != Some(DECISION_WOULD_EXECUTE) {
        return Some(ENTRY_GATE_DECISION_NOT_EXECUTE);
    }
    if metadata
        .quote_in_amount_raw
        .as_deref()
        .is_none_or(str::is_empty)
        || metadata
            .quote_out_amount_raw
            .as_deref()
            .is_none_or(str::is_empty)
    {
        return Some(ENTRY_GATE_MISSING_QUOTE_AMOUNTS);
    }
    if !quote_input_matches_canary_buy_size(config, metadata.quote_in_amount_raw.as_deref()) {
        return Some(ENTRY_GATE_QUOTE_AMOUNT_MISMATCH);
    }
    if metadata
        .quote_price_sol
        .is_none_or(|price| !price.is_finite() || price <= 0.0)
    {
        return Some(ENTRY_GATE_INVALID_QUOTE_PRICE);
    }
    if metadata
        .route_plan_json
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Some(ENTRY_GATE_MISSING_ROUTE_PLAN);
    }
    if quote_response_requires_fee_account(metadata.quote_response_json.as_deref()) {
        return Some(ENTRY_GATE_PLATFORM_FEE_REQUIRES_FEE_ACCOUNT);
    }
    let Some(slippage_bps) = metadata.slippage_bps else {
        return Some(ENTRY_GATE_MISSING_SLIPPAGE);
    };
    let max_slippage_bps = quote_canary_slippage_limit_bps(config, SIDE_BUY) as f64;
    if !slippage_bps.is_finite() || slippage_bps > max_slippage_bps {
        return Some(ENTRY_GATE_SLIPPAGE_ABOVE_LIMIT);
    }
    if metadata.priority_fee_status.is_none() || metadata.priority_fee_lamports.is_none() {
        return Some(ENTRY_GATE_MISSING_PRIORITY_FEE);
    }
    if metadata.priority_fee_status.as_deref() != Some(QUOTE_STATUS_OK) {
        return Some(ENTRY_GATE_PRIORITY_FEE_NOT_OK);
    }
    if config.pretrade_max_priority_fee_lamports > 0
        && metadata
            .priority_fee_lamports
            .is_some_and(|fee| fee > config.pretrade_max_priority_fee_lamports)
    {
        return Some(ENTRY_GATE_PRIORITY_FEE_ABOVE_CAP);
    }
    None
}

fn quote_input_matches_canary_buy_size(
    config: &ExecutionConfig,
    quote_in_raw: Option<&str>,
) -> bool {
    let Some(quote_in_raw) = quote_in_raw else {
        return false;
    };
    let Ok(actual_lamports) = quote_in_raw.parse::<u128>() else {
        return false;
    };
    if !config.canary_buy_size_sol.is_finite() || config.canary_buy_size_sol <= 0.0 {
        return false;
    }
    let expected = (config.canary_buy_size_sol * 1_000_000_000.0).round();
    if !expected.is_finite() || expected <= 0.0 {
        return false;
    }
    actual_lamports == expected as u128
}
