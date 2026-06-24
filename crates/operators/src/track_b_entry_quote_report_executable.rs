use crate::track_b_entry_quote_report_db::{CloseOutcome, EntryQuoteOutcome, MarketExitQuote};

const MARKET_EXIT_OK: &str = "ok";

#[derive(Debug, Default)]
pub(crate) struct FullyExecutablePnl {
    pub(crate) pnl_sol: Option<f64>,
    pub(crate) market_quote_events: u64,
    pub(crate) market_missing_events: u64,
    pub(crate) market_quote_shadow_ratios: Vec<f64>,
    pub(crate) market_decision_delay_ms: Vec<f64>,
}

pub(crate) fn fully_executable_pnl(
    outcome: &EntryQuoteOutcome,
    entry_qty_factor: f64,
    entry_cost_sol: f64,
) -> FullyExecutablePnl {
    let mut out = FullyExecutablePnl {
        pnl_sol: Some(-entry_cost_sol),
        ..FullyExecutablePnl::default()
    };
    for close in &outcome.closes {
        let exit_value = executable_exit_value(close, entry_qty_factor, &mut out);
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
    state: &mut FullyExecutablePnl,
) -> Option<f64> {
    match close.close_context.as_str() {
        "market" => {
            let Some(ratio) = market_exit_quote_ratio(close.market_exit_quote.as_ref()) else {
                state.market_missing_events += 1;
                return None;
            };
            state.market_quote_events += 1;
            state.market_quote_shadow_ratios.push(ratio);
            if let Some(delay) = close
                .market_exit_quote
                .as_ref()
                .and_then(|quote| quote.decision_delay_ms)
            {
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

fn market_exit_quote_ratio(quote: Option<&MarketExitQuote>) -> Option<f64> {
    let quote = quote?;
    if quote.quote_status != MARKET_EXIT_OK {
        return None;
    }
    let quote_price = positive(quote.quote_price_sol?)?;
    let shadow_price = positive(quote.shadow_price_sol?)?;
    Some(quote_price / shadow_price)
}

fn positive(value: f64) -> Option<f64> {
    (value.is_finite() && value > 0.0).then_some(value)
}
