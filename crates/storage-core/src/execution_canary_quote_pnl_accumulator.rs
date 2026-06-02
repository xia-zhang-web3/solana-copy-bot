use crate::{
    ExecutionCanaryQuotePnlSummary, ExecutionCanaryQuotePnlTrade,
    EXECUTION_CANARY_QUOTE_PNL_STATUS_COUNTED, EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED,
};
use chrono::{DateTime, Utc};

pub(crate) fn summarize_quote_pnl(
    as_of: DateTime<Utc>,
    since: DateTime<Utc>,
    limit: u32,
    trades: Vec<ExecutionCanaryQuotePnlTrade>,
) -> ExecutionCanaryQuotePnlSummary {
    let mut summary = empty_summary(as_of, since, limit);
    for trade in trades {
        record_trade(&mut summary, trade);
    }
    summary
}

fn empty_summary(
    as_of: DateTime<Utc>,
    since: DateTime<Utc>,
    limit: u32,
) -> ExecutionCanaryQuotePnlSummary {
    ExecutionCanaryQuotePnlSummary {
        as_of,
        since,
        limit,
        total_closed_trades: 0,
        matched_quote_trades: 0,
        pnl_counted_trades: 0,
        skipped_trades: 0,
        unknown_trades: 0,
        missing_entry_quote_trades: 0,
        missing_exit_quote_trades: 0,
        invalid_quote_amount_trades: 0,
        shadow_win_count: 0,
        shadow_loss_count: 0,
        quote_win_count: 0,
        quote_loss_count: 0,
        shadow_pnl_sol: 0.0,
        quote_adjusted_pnl_sol: 0.0,
        quote_adjusted_pnl_after_priority_fee_sol: 0.0,
        quote_vs_shadow_delta_sol: 0.0,
        priority_fee_lamports_sum: 0,
        trades: Vec::new(),
    }
}

fn record_trade(summary: &mut ExecutionCanaryQuotePnlSummary, trade: ExecutionCanaryQuotePnlTrade) {
    summary.total_closed_trades += 1;
    summary.shadow_pnl_sol += trade.shadow_pnl_sol;
    if trade.shadow_pnl_sol > 0.0 {
        summary.shadow_win_count += 1;
    } else if trade.shadow_pnl_sol < 0.0 {
        summary.shadow_loss_count += 1;
    }
    if trade.entry_quote_event_id.is_some() && trade.exit_quote_event_id.is_some() {
        summary.matched_quote_trades += 1;
    }
    match trade.status.as_str() {
        EXECUTION_CANARY_QUOTE_PNL_STATUS_COUNTED => record_counted(summary, &trade),
        EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED => summary.skipped_trades += 1,
        _ => record_unknown(summary, &trade),
    }
    summary.trades.push(trade);
}

fn record_counted(
    summary: &mut ExecutionCanaryQuotePnlSummary,
    trade: &ExecutionCanaryQuotePnlTrade,
) {
    summary.pnl_counted_trades += 1;
    let quote_pnl = trade.quote_adjusted_pnl_sol.unwrap_or(0.0);
    let quote_pnl_after_fee = trade
        .quote_adjusted_pnl_after_priority_fee_sol
        .unwrap_or(quote_pnl);
    summary.quote_adjusted_pnl_sol += quote_pnl;
    summary.quote_adjusted_pnl_after_priority_fee_sol += quote_pnl_after_fee;
    summary.quote_vs_shadow_delta_sol += trade
        .quote_vs_shadow_delta_sol
        .unwrap_or(quote_pnl - trade.shadow_pnl_sol);
    summary.priority_fee_lamports_sum += trade.priority_fee_lamports_total.unwrap_or(0);
    if quote_pnl > 0.0 {
        summary.quote_win_count += 1;
    } else if quote_pnl < 0.0 {
        summary.quote_loss_count += 1;
    }
}

fn record_unknown(
    summary: &mut ExecutionCanaryQuotePnlSummary,
    trade: &ExecutionCanaryQuotePnlTrade,
) {
    summary.unknown_trades += 1;
    match trade.reason.as_str() {
        "missing_entry_quote" => summary.missing_entry_quote_trades += 1,
        "missing_exit_quote" => summary.missing_exit_quote_trades += 1,
        "invalid_quote_amount" | "close_qty_exceeds_entry_quote" => {
            summary.invalid_quote_amount_trades += 1;
        }
        _ => {}
    }
}
