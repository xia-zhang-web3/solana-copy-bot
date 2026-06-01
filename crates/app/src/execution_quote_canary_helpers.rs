use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{
    ExecutionCanaryCloseCandidate, ExecutionCanaryObservedLeg, ExecutionQuoteCanaryEventInsert,
    SqliteStore,
};
use serde_json::Value;
use std::time::Instant;

pub(crate) const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub(crate) const QUOTE_STATUS_OK: &str = "ok";
pub(crate) const QUOTE_STATUS_ERROR: &str = "error";
pub(crate) const QUOTE_STATUS_SKIPPED: &str = "skipped";
pub(crate) const SIDE_BUY: &str = "buy";
pub(crate) const SIDE_SELL: &str = "sell";
pub(crate) const DECISION_WOULD_EXECUTE: &str = "would_execute";
pub(crate) const DECISION_WOULD_SKIP: &str = "would_skip";
pub(crate) const DECISION_UNKNOWN: &str = "unknown";

#[derive(Debug, Clone)]
pub(crate) struct QuoteSample {
    pub(crate) in_amount: String,
    pub(crate) out_amount: String,
    pub(crate) price_impact_pct: Option<f64>,
    pub(crate) route_plan_json: Option<String>,
    pub(crate) latency_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct PriorityFeeSample {
    pub(crate) status: String,
    pub(crate) lamports: Option<u64>,
    pub(crate) json: Option<String>,
    pub(crate) error: Option<String>,
}

pub(crate) fn apply_quote_sample_to_event(
    event: &mut ExecutionQuoteCanaryEventInsert,
    quote: QuoteSample,
) {
    event.quote_status = QUOTE_STATUS_OK.to_string();
    event.quote_in_amount_raw = Some(quote.in_amount);
    event.quote_out_amount_raw = Some(quote.out_amount);
    event.price_impact_pct = quote.price_impact_pct;
    event.route_plan_json = quote.route_plan_json;
    event.quote_latency_ms = Some(quote.latency_ms);
}

pub(crate) fn attach_priority_fee(
    event: &mut ExecutionQuoteCanaryEventInsert,
    sample: Option<&PriorityFeeSample>,
) {
    let Some(sample) = sample else {
        return;
    };
    event.priority_fee_status = Some(sample.status.clone());
    event.priority_fee_lamports = sample.lamports;
    event.priority_fee_json = sample.json.clone();
    if let Some(error) = &sample.error {
        append_event_error(event, format!("priority_fee: {error}"));
    }
}

pub(crate) fn finalize_quote_decision(
    event: &mut ExecutionQuoteCanaryEventInsert,
    max_slippage_bps: u64,
) {
    if event.quote_status == QUOTE_STATUS_ERROR {
        set_decision(event, DECISION_UNKNOWN, "quote_error");
        return;
    }
    if event.quote_status == QUOTE_STATUS_SKIPPED {
        set_decision(event, DECISION_UNKNOWN, "quote_skipped");
        return;
    }
    if event.priority_fee_status.as_deref() == Some(QUOTE_STATUS_ERROR) {
        set_decision(event, DECISION_UNKNOWN, "priority_fee_error");
        return;
    }
    let Some(slippage_bps) = event.slippage_bps else {
        set_decision(event, DECISION_UNKNOWN, "missing_slippage_bps");
        return;
    };
    if !slippage_bps.is_finite() {
        set_decision(event, DECISION_UNKNOWN, "invalid_slippage_bps");
        return;
    }
    if slippage_bps > max_slippage_bps as f64 {
        set_decision(event, DECISION_WOULD_SKIP, "slippage_above_limit");
    } else {
        set_decision(event, DECISION_WOULD_EXECUTE, "within_slippage_limit");
    }
}

pub(crate) fn quote_canary_slippage_limit_bps(config: &ExecutionConfig, side: &str) -> u64 {
    let directional = match side {
        SIDE_BUY => config.quote_canary_buy_slippage_bps,
        SIDE_SELL => config.quote_canary_sell_slippage_bps,
        _ => 0,
    };
    if directional > 0 {
        directional
    } else {
        config.quote_canary_slippage_bps
    }
}

pub(crate) fn load_matching_observed_entry_leg(
    store: &SqliteStore,
    signal: &CopySignalRow,
) -> Result<Option<ExecutionCanaryObservedLeg>> {
    let Some(signature) = signal_signature_for_canary(&signal.signal_id) else {
        return Ok(None);
    };
    let observed = store.load_execution_canary_observed_leg_by_signature(signature)?;
    Ok(observed.filter(|leg| leg.is_buy && leg.token_mint == signal.token))
}

pub(crate) fn load_matching_observed_leg_for_signal(
    store: &SqliteStore,
    signal_id: &str,
    token: &str,
) -> Result<Option<ExecutionCanaryObservedLeg>> {
    let Some(signature) = signal_signature_for_canary(signal_id) else {
        return Ok(None);
    };
    let observed = store.load_execution_canary_observed_leg_by_signature(signature)?;
    Ok(observed.filter(|leg| leg.token_mint == token))
}

pub(crate) fn entry_quote_event_id(signal_id: &str) -> String {
    format!("quote:entry:{signal_id}")
}

pub(crate) fn close_quote_event_id(close_id: i64) -> String {
    format!("quote:close:{close_id}")
}

pub(crate) fn entry_error_event(
    signal: &CopySignalRow,
    now: DateTime<Utc>,
    error: &anyhow::Error,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: entry_quote_event_id(&signal.signal_id),
        signal_id: Some(signal.signal_id.clone()),
        shadow_closed_trade_id: None,
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: SIDE_BUY.to_string(),
        quote_status: QUOTE_STATUS_ERROR.to_string(),
        request_ts: now,
        signal_ts: Some(signal.ts),
        decision_delay_ms: duration_ms_between(signal.ts, now),
        quote_latency_ms: None,
        leader_notional_sol: Some(signal.notional_sol),
        quote_in_amount_raw: None,
        quote_out_amount_raw: None,
        quote_price_sol: None,
        shadow_price_sol: None,
        slippage_bps: None,
        price_impact_pct: None,
        route_plan_json: None,
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        decision_status: Some(DECISION_UNKNOWN.to_string()),
        decision_reason: Some("quote_error".to_string()),
        error: Some(short_error(error)),
    }
}

pub(crate) fn close_error_event(
    close: &ExecutionCanaryCloseCandidate,
    now: DateTime<Utc>,
    error: &anyhow::Error,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: close_quote_event_id(close.id),
        signal_id: Some(close.signal_id.clone()),
        shadow_closed_trade_id: Some(close.id),
        wallet_id: close.wallet_id.clone(),
        token: close.token.clone(),
        side: SIDE_SELL.to_string(),
        quote_status: QUOTE_STATUS_ERROR.to_string(),
        request_ts: now,
        signal_ts: Some(close.closed_ts),
        decision_delay_ms: duration_ms_between(close.closed_ts, now),
        quote_latency_ms: None,
        leader_notional_sol: Some(close.exit_value_sol),
        quote_in_amount_raw: close.qty_raw.clone(),
        quote_out_amount_raw: None,
        quote_price_sol: None,
        shadow_price_sol: price_sol_per_token(close.exit_value_sol, close.qty),
        slippage_bps: None,
        price_impact_pct: None,
        route_plan_json: None,
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        decision_status: Some(DECISION_UNKNOWN.to_string()),
        decision_reason: Some("quote_error".to_string()),
        error: Some(short_error(error)),
    }
}

pub(crate) fn quote_url(base_url: &str) -> Result<String> {
    let trimmed = base_url.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("quote canary base URL is empty"));
    }
    let without_slash = trimmed.trim_end_matches('/');
    if without_slash.ends_with("/quote") {
        Ok(without_slash.to_string())
    } else {
        Ok(format!("{without_slash}/quote"))
    }
}

pub(crate) fn sol_to_lamports_raw(sol: f64) -> Result<String> {
    if !sol.is_finite() || sol <= 0.0 {
        return Err(anyhow!("invalid SOL amount for quote canary: {sol}"));
    }
    let lamports = (sol * 1_000_000_000.0).round();
    if lamports < 1.0 || lamports > u64::MAX as f64 {
        return Err(anyhow!(
            "SOL amount cannot be represented in lamports: {sol}"
        ));
    }
    Ok((lamports as u64).to_string())
}

pub(crate) fn price_sol_per_token(sol: f64, tokens: f64) -> Option<f64> {
    if sol.is_finite() && tokens.is_finite() && sol >= 0.0 && tokens > 0.0 {
        Some(sol / tokens)
    } else {
        None
    }
}

pub(crate) fn quote_slippage_bps_for_buy(
    quote_price: Option<f64>,
    shadow_price: Option<f64>,
) -> Option<f64> {
    let (Some(quote_price), Some(shadow_price)) = (quote_price, shadow_price) else {
        return None;
    };
    (shadow_price > 0.0).then_some((quote_price - shadow_price) / shadow_price * 10_000.0)
}

pub(crate) fn quote_slippage_bps_for_sell(
    quote_price: Option<f64>,
    shadow_price: Option<f64>,
) -> Option<f64> {
    let (Some(quote_price), Some(shadow_price)) = (quote_price, shadow_price) else {
        return None;
    };
    (shadow_price > 0.0).then_some((shadow_price - quote_price) / shadow_price * 10_000.0)
}

pub(crate) fn raw_amount_to_ui(raw: Option<&str>, decimals: u8) -> Option<f64> {
    let value = raw?.parse::<f64>().ok()?;
    let divisor = 10f64.powi(i32::from(decimals));
    (value.is_finite() && divisor.is_finite() && divisor > 0.0).then_some(value / divisor)
}

pub(crate) fn ui_amount_to_raw_string(ui_amount: f64, decimals: u8) -> Option<String> {
    if !ui_amount.is_finite() || ui_amount <= 0.0 {
        return None;
    }
    let multiplier = 10f64.powi(i32::from(decimals));
    let raw = (ui_amount * multiplier).round();
    if !raw.is_finite() || raw < 1.0 || raw > u64::MAX as f64 {
        return None;
    }
    Some(format!("{raw:.0}"))
}

pub(crate) fn observed_token_decimals(observed: &ExecutionCanaryObservedLeg) -> Option<u8> {
    observed.token_decimals.or_else(|| {
        let raw = observed.token_raw_amount.as_deref()?;
        infer_decimals_from_raw_and_ui(raw, observed.token_qty)
    })
}

pub(crate) fn infer_decimals_from_raw_and_ui(raw: &str, ui_amount: f64) -> Option<u8> {
    if !ui_amount.is_finite() || ui_amount <= 0.0 {
        return None;
    }
    let raw_value = raw.parse::<f64>().ok()?;
    if !raw_value.is_finite() || raw_value <= 0.0 {
        return None;
    }
    for decimals in 0..=18u8 {
        let scaled = ui_amount * 10f64.powi(i32::from(decimals));
        let tolerance = 1.0_f64.max(raw_value.abs() * 1e-9);
        if (scaled.round() - raw_value).abs() <= tolerance {
            return Some(decimals);
        }
    }
    None
}

pub(crate) fn duration_ms_between(from: DateTime<Utc>, to: DateTime<Utc>) -> Option<u64> {
    let millis = to.signed_duration_since(from).num_milliseconds();
    u64::try_from(millis).ok()
}

pub(crate) fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

pub(crate) fn string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

pub(crate) fn numeric_field(value: &Value, field: &str) -> Option<f64> {
    match value.get(field)? {
        Value::Number(number) => number.as_f64(),
        Value::String(raw) => raw.parse::<f64>().ok(),
        _ => None,
    }
}

pub(crate) fn priority_fee_lamports(result: &Value) -> Option<u64> {
    json_u64(result.get("recommended"))
        .or_else(|| nested_json_u64(result, "per_compute_unit", "high"))
        .or_else(|| nested_json_u64(result, "per_compute_unit", "medium"))
        .or_else(|| nested_json_u64(result, "per_transaction", "high"))
        .or_else(|| nested_json_u64(result, "per_transaction", "medium"))
}

pub(crate) fn short_error(error: &anyhow::Error) -> String {
    truncate_for_log(&format!("{error:#}"), 500)
}

pub(crate) fn truncate_for_log(value: &str, max_chars: usize) -> String {
    let mut out = value.chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        out.push_str("...");
    }
    out
}

pub(crate) fn signal_signature_for_canary(signal_id: &str) -> Option<&str> {
    if signal_id.trim().is_empty() {
        return None;
    }
    let mut parts = signal_id.split(':');
    if matches!(parts.next(), Some("shadow")) {
        parts.next().filter(|value| !value.trim().is_empty())
    } else {
        Some(signal_id)
    }
}

fn nested_json_u64(value: &Value, parent: &str, child: &str) -> Option<u64> {
    value.get(parent).and_then(|node| json_u64(node.get(child)))
}

fn json_u64(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Number(number) => number.as_u64(),
        Value::String(raw) => raw.parse::<u64>().ok(),
        _ => None,
    }
}

fn append_event_error(event: &mut ExecutionQuoteCanaryEventInsert, message: String) {
    event.error = Some(match event.error.take() {
        Some(current) if !current.is_empty() => format!("{current}; {message}"),
        _ => message,
    });
}

fn set_decision(event: &mut ExecutionQuoteCanaryEventInsert, status: &str, reason: &str) {
    event.decision_status = Some(status.to_string());
    event.decision_reason = Some(reason.to_string());
}
