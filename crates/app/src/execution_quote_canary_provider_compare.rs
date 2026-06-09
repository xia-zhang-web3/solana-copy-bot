use crate::execution_quote_canary_helpers::{
    price_sol_per_token, quote_slippage_bps_for_buy, quote_slippage_bps_for_sell, raw_amount_to_ui,
    short_error, DECISION_UNKNOWN, DECISION_WOULD_EXECUTE, DECISION_WOULD_FORCE_EXIT,
    DECISION_WOULD_SKIP, QUOTE_STATUS_ERROR, QUOTE_STATUS_OK, SIDE_BUY, SIDE_SELL,
};
use crate::execution_quote_provider_selection::quote_response_requires_fee_account;
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, ExecutionQuoteCanaryProviderSampleInsert,
    PROVIDER_GENERIC_METIS, PROVIDER_GENERIC_PUBLIC, PROVIDER_PUMP_FUN_PAID,
};
use serde_json::Value;

pub(crate) struct QuoteEventBundle {
    pub(crate) event: ExecutionQuoteCanaryEventInsert,
    pub(crate) provider_samples: Vec<ExecutionQuoteCanaryProviderSampleInsert>,
}

impl QuoteEventBundle {
    pub(crate) fn event_only(event: ExecutionQuoteCanaryEventInsert) -> Self {
        Self {
            event,
            provider_samples: Vec::new(),
        }
    }
}

pub(super) fn generic_provider_sample(
    event: &ExecutionQuoteCanaryEventInsert,
    max_slippage_bps: u64,
) -> ExecutionQuoteCanaryProviderSampleInsert {
    provider_sample_from_event(event, PROVIDER_GENERIC_METIS, max_slippage_bps)
}

pub(super) fn provider_sample_from_event(
    event: &ExecutionQuoteCanaryEventInsert,
    provider: &str,
    max_slippage_bps: u64,
) -> ExecutionQuoteCanaryProviderSampleInsert {
    let (decision_status, decision_reason) = quote_only_decision(
        &event.side,
        &event.quote_status,
        event.slippage_bps,
        max_slippage_bps,
    );
    ExecutionQuoteCanaryProviderSampleInsert {
        event_id: event.event_id.clone(),
        provider: provider.to_string(),
        side: event.side.clone(),
        quote_status: event.quote_status.clone(),
        request_ts: event.request_ts,
        quote_latency_ms: event.quote_latency_ms,
        quote_in_amount_raw: event.quote_in_amount_raw.clone(),
        quote_out_amount_raw: event.quote_out_amount_raw.clone(),
        quote_response_json: event.quote_response_json.clone(),
        quote_price_sol: event.quote_price_sol,
        shadow_price_sol: event.shadow_price_sol,
        slippage_bps: event.slippage_bps,
        price_impact_pct: event.price_impact_pct,
        route_plan_json: event.route_plan_json.clone(),
        decision_status: Some(decision_status),
        decision_reason: Some(decision_reason),
        error: (event.quote_status == QUOTE_STATUS_ERROR)
            .then(|| event.error.clone())
            .flatten(),
    }
}

pub(super) fn provider_error_sample(
    event: &ExecutionQuoteCanaryEventInsert,
    provider: &str,
    error: &anyhow::Error,
    max_slippage_bps: u64,
) -> ExecutionQuoteCanaryProviderSampleInsert {
    let mut sample = provider_sample_from_event(event, provider, max_slippage_bps);
    sample.quote_status = QUOTE_STATUS_ERROR.to_string();
    sample.quote_latency_ms = None;
    sample.quote_in_amount_raw = None;
    sample.quote_out_amount_raw = None;
    sample.quote_response_json = None;
    sample.quote_price_sol = None;
    sample.slippage_bps = None;
    sample.price_impact_pct = None;
    sample.route_plan_json = None;
    sample.decision_status = Some(DECISION_UNKNOWN.to_string());
    sample.decision_reason = Some("quote_error".to_string());
    sample.error = Some(short_error(error));
    sample
}

pub(super) fn select_usable_provider_for_event(
    event: &mut ExecutionQuoteCanaryEventInsert,
    provider_samples: &[ExecutionQuoteCanaryProviderSampleInsert],
) {
    if let Some(sample) = provider_samples.iter().find(|sample| {
        sample.provider == PROVIDER_PUMP_FUN_PAID
            && provider_quote_is_ok(sample)
            && pump_fun_quote_is_completed(sample) == Some(false)
    }) {
        apply_provider_sample_to_event(event, sample);
        return;
    }
    if let Some(sample) = best_generic_provider_sample(provider_samples) {
        apply_provider_sample_to_event(event, sample);
        return;
    }
    if let Some(sample) = provider_samples.iter().find(|sample| {
        sample.provider == PROVIDER_GENERIC_PUBLIC
            && provider_quote_is_buildable_without_fee_account(sample)
    }) {
        apply_provider_sample_to_event(event, sample);
    }
}

fn best_generic_provider_sample(
    provider_samples: &[ExecutionQuoteCanaryProviderSampleInsert],
) -> Option<&ExecutionQuoteCanaryProviderSampleInsert> {
    provider_samples
        .iter()
        .filter(|sample| {
            matches!(
                sample.provider.as_str(),
                PROVIDER_GENERIC_METIS | PROVIDER_GENERIC_PUBLIC
            ) && provider_quote_has_finite_slippage(sample)
        })
        .min_by(|left, right| {
            let left_slippage = left.slippage_bps.expect("filtered finite slippage");
            let right_slippage = right.slippage_bps.expect("filtered finite slippage");
            left_slippage.total_cmp(&right_slippage).then(
                provider_priority(left.provider.as_str())
                    .cmp(&provider_priority(right.provider.as_str())),
            )
        })
}

fn provider_priority(provider: &str) -> u8 {
    match provider {
        PROVIDER_GENERIC_METIS => 0,
        PROVIDER_GENERIC_PUBLIC => 1,
        _ => 2,
    }
}

fn apply_provider_sample_to_event(
    event: &mut ExecutionQuoteCanaryEventInsert,
    sample: &ExecutionQuoteCanaryProviderSampleInsert,
) {
    event.quote_status = sample.quote_status.clone();
    event.quote_latency_ms = sample.quote_latency_ms;
    event.quote_in_amount_raw = sample.quote_in_amount_raw.clone();
    event.quote_out_amount_raw = sample.quote_out_amount_raw.clone();
    event.quote_response_json = sample.quote_response_json.clone();
    event.quote_price_sol = sample.quote_price_sol;
    event.slippage_bps = sample.slippage_bps;
    event.price_impact_pct = sample.price_impact_pct;
    event.route_plan_json = sample.route_plan_json.clone();
    event.decision_status = sample.decision_status.clone();
    event.decision_reason = sample.decision_reason.clone();
    event.error = if sample.quote_status == QUOTE_STATUS_ERROR {
        sample.error.clone()
    } else {
        None
    };
}

fn provider_quote_is_ok(sample: &ExecutionQuoteCanaryProviderSampleInsert) -> bool {
    sample.quote_status == QUOTE_STATUS_OK
}

fn provider_quote_has_finite_slippage(sample: &ExecutionQuoteCanaryProviderSampleInsert) -> bool {
    provider_quote_is_buildable_without_fee_account(sample)
        && sample.slippage_bps.is_some_and(f64::is_finite)
}

fn provider_quote_is_buildable_without_fee_account(
    sample: &ExecutionQuoteCanaryProviderSampleInsert,
) -> bool {
    provider_quote_is_ok(sample)
        && !quote_response_requires_fee_account(sample.quote_response_json.as_deref())
}

fn pump_fun_quote_is_completed(sample: &ExecutionQuoteCanaryProviderSampleInsert) -> Option<bool> {
    let raw = sample.quote_response_json.as_deref()?;
    let value: Value = serde_json::from_str(raw).ok()?;
    value
        .pointer("/quote/meta/isCompleted")
        .and_then(Value::as_bool)
}

pub(super) fn buy_quote_price_and_slippage(
    event: &ExecutionQuoteCanaryEventInsert,
    token_decimals: u8,
) -> (Option<f64>, Option<f64>) {
    let quote_in_sol = raw_amount_to_ui(event.quote_in_amount_raw.as_deref(), 9);
    let quote_out_tokens = raw_amount_to_ui(event.quote_out_amount_raw.as_deref(), token_decimals);
    let price = quote_in_sol
        .and_then(|input| quote_out_tokens.and_then(|out| price_sol_per_token(input, out)));
    (
        price,
        quote_slippage_bps_for_buy(price, event.shadow_price_sol),
    )
}

pub(crate) fn sell_quote_price_and_slippage(
    event: &ExecutionQuoteCanaryEventInsert,
    token_decimals: u8,
) -> (Option<f64>, Option<f64>) {
    let quote_out_sol = raw_amount_to_ui(event.quote_out_amount_raw.as_deref(), 9);
    let quote_in_tokens = raw_amount_to_ui(event.quote_in_amount_raw.as_deref(), token_decimals);
    let price = quote_out_sol
        .and_then(|out| quote_in_tokens.and_then(|input| price_sol_per_token(out, input)));
    (
        price,
        quote_slippage_bps_for_sell(price, event.shadow_price_sol),
    )
}

fn quote_only_decision(
    side: &str,
    quote_status: &str,
    slippage_bps: Option<f64>,
    max_slippage_bps: u64,
) -> (String, String) {
    if quote_status != "ok" {
        return (DECISION_UNKNOWN.to_string(), "quote_not_ok".to_string());
    }
    let Some(slippage_bps) = slippage_bps.filter(|value| value.is_finite()) else {
        return (
            DECISION_UNKNOWN.to_string(),
            "missing_slippage_bps".to_string(),
        );
    };
    if slippage_bps <= max_slippage_bps as f64 {
        (
            DECISION_WOULD_EXECUTE.to_string(),
            "within_slippage_limit".to_string(),
        )
    } else if side.eq_ignore_ascii_case(SIDE_SELL) {
        (
            DECISION_WOULD_FORCE_EXIT.to_string(),
            "exit_slippage_above_soft_limit".to_string(),
        )
    } else if side.eq_ignore_ascii_case(SIDE_BUY) {
        (
            DECISION_WOULD_SKIP.to_string(),
            "slippage_above_limit".to_string(),
        )
    } else {
        (DECISION_UNKNOWN.to_string(), "unknown_side".to_string())
    }
}
