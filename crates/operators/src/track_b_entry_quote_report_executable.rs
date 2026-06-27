use crate::track_b_entry_quote_report_db::{CloseOutcome, EntryQuoteOutcome, MarketExitQuote};

const MARKET_EXIT_OK: &str = "ok";
const MARKET_EXIT_RATIO_MIN: f64 = 0.1;
const MARKET_EXIT_RATIO_MAX: f64 = 10.0;

#[derive(Debug, Default)]
pub(crate) struct FullyExecutablePnl {
    pub(crate) pnl_sol: Option<f64>,
    pub(crate) market_quote_events: u64,
    pub(crate) market_error_events: u64,
    pub(crate) market_dead_error_events: u64,
    pub(crate) market_transient_error_events: u64,
    pub(crate) market_missing_events: u64,
    pub(crate) market_ratio_outlier_events: u64,
    pub(crate) market_zero_exit_events: u64,
    pub(crate) market_quote_shadow_ratios: Vec<f64>,
    pub(crate) market_decision_delay_ms: Vec<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum MarketExitQuoteRatio {
    Sane(f64),
    Missing,
    Outlier,
}

pub(crate) fn fully_executable_pnl(
    outcome: &EntryQuoteOutcome,
    entry_qty_factor: f64,
    entry_cost_sol: f64,
    max_market_exit_delay_ms: Option<i64>,
) -> FullyExecutablePnl {
    let mut out = FullyExecutablePnl {
        pnl_sol: Some(-entry_cost_sol),
        ..FullyExecutablePnl::default()
    };
    for close in &outcome.closes {
        let exit_value =
            executable_exit_value(close, entry_qty_factor, max_market_exit_delay_ms, &mut out);
        out.pnl_sol = match (out.pnl_sol, exit_value) {
            (Some(current), Some(value)) => Some(current + value),
            _ => None,
        };
    }
    out
}

fn executable_exit_value(
    close: &CloseOutcome,
    entry_qty_factor: f64,
    max_market_exit_delay_ms: Option<i64>,
    state: &mut FullyExecutablePnl,
) -> Option<f64> {
    match close.close_context.as_str() {
        "market" => {
            let Some(quote) = close.market_exit_quote.as_ref() else {
                state.market_missing_events += 1;
                return None;
            };
            if quote_exceeds_delay(quote, max_market_exit_delay_ms) {
                state.market_missing_events += 1;
                return None;
            }
            if quote.quote_status != MARKET_EXIT_OK {
                state.market_error_events += 1;
                if is_terminal_market_exit_error(quote.error.as_deref()) {
                    state.market_dead_error_events += 1;
                    state.market_zero_exit_events += 1;
                    return Some(0.0);
                }
                state.market_transient_error_events += 1;
                state.market_missing_events += 1;
                return None;
            }
            let ratio = match market_exit_quote_ratio(quote) {
                MarketExitQuoteRatio::Sane(ratio) => ratio,
                MarketExitQuoteRatio::Missing => {
                    state.market_missing_events += 1;
                    return None;
                }
                MarketExitQuoteRatio::Outlier => {
                    state.market_ratio_outlier_events += 1;
                    state.market_missing_events += 1;
                    return None;
                }
            };
            state.market_quote_events += 1;
            state.market_quote_shadow_ratios.push(ratio);
            if let Some(delay) = quote.decision_delay_ms {
                state.market_decision_delay_ms.push(delay as f64);
            }
            Some(close.exit_value_sol * entry_qty_factor * ratio)
        }
        "stale_quote_price" | "stale_terminal_zero_price" | "recovery_terminal_zero_price" => {
            Some(close.exit_value_sol * entry_qty_factor)
        }
        _ => None,
    }
}

fn quote_exceeds_delay(quote: &MarketExitQuote, max_delay_ms: Option<i64>) -> bool {
    max_delay_ms
        .zip(quote.decision_delay_ms)
        .is_some_and(|(max, actual)| actual > max)
}

fn market_exit_quote_ratio(quote: &MarketExitQuote) -> MarketExitQuoteRatio {
    if quote.quote_status != MARKET_EXIT_OK {
        return MarketExitQuoteRatio::Missing;
    }
    let Some(quote_price) = quote.quote_price_sol.and_then(positive) else {
        return MarketExitQuoteRatio::Missing;
    };
    let Some(shadow_price) = quote.shadow_price_sol.and_then(positive) else {
        return MarketExitQuoteRatio::Missing;
    };
    let ratio = quote_price / shadow_price;
    if !(MARKET_EXIT_RATIO_MIN..=MARKET_EXIT_RATIO_MAX).contains(&ratio) {
        return MarketExitQuoteRatio::Outlier;
    }
    MarketExitQuoteRatio::Sane(ratio)
}

fn is_terminal_market_exit_error(error: Option<&str>) -> bool {
    let Some(error) = error else {
        return false;
    };
    let lower = error.to_ascii_lowercase();
    lower.contains("token_not_tradable")
        || lower.contains("not tradable")
        || lower.contains("no_routes")
        || lower.contains("no routes")
        || lower.contains("no_route")
        || lower.contains("no route")
        || lower.contains("no route found")
}

fn positive(value: f64) -> Option<f64> {
    (value.is_finite() && value > 0.0).then_some(value)
}
