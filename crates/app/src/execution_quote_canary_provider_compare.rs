use crate::execution_quote_canary_helpers::{
    price_sol_per_token, quote_slippage_bps_for_buy, quote_slippage_bps_for_sell, raw_amount_to_ui,
    short_error, DECISION_UNKNOWN, DECISION_WOULD_EXECUTE, DECISION_WOULD_FORCE_EXIT,
    DECISION_WOULD_SKIP, QUOTE_STATUS_ERROR, SIDE_BUY, SIDE_SELL,
};
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, ExecutionQuoteCanaryProviderSampleInsert,
    PROVIDER_GENERIC_METIS,
};

pub(crate) struct QuoteEventBundle {
    pub(crate) event: ExecutionQuoteCanaryEventInsert,
    pub(crate) provider_samples: Vec<ExecutionQuoteCanaryProviderSampleInsert>,
}

impl QuoteEventBundle {
    pub(super) fn event_only(event: ExecutionQuoteCanaryEventInsert) -> Self {
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

pub(super) fn sell_quote_price_and_slippage(
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
