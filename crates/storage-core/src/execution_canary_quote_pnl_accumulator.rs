use crate::{
    execution_canary_quote_pnl_buckets::record_quote_pnl_buckets,
    execution_canary_quote_pnl_diagnostics::{empty_quote_diagnostics, record_quote_diagnostics},
    ExecutionCanaryQuotePnlSummary, ExecutionCanaryQuotePnlTrade,
    ExecutionCanaryShadowCloseBreakdown, EXECUTION_CANARY_QUOTE_PNL_STATUS_COUNTED,
    EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED,
};
use chrono::{DateTime, Utc};

const DECISION_WOULD_FORCE_EXIT: &str = "would_force_exit";

pub(crate) fn summarize_quote_pnl(
    as_of: DateTime<Utc>,
    since: DateTime<Utc>,
    limit: u32,
    trades: Vec<ExecutionCanaryQuotePnlTrade>,
    shadow_close_breakdown: ExecutionCanaryShadowCloseBreakdown,
) -> ExecutionCanaryQuotePnlSummary {
    let mut summary = empty_summary(as_of, since, limit, shadow_close_breakdown);
    for trade in trades {
        record_trade(&mut summary, trade);
    }
    record_quote_pnl_buckets(&mut summary);
    summary
}

fn empty_summary(
    as_of: DateTime<Utc>,
    since: DateTime<Utc>,
    limit: u32,
    shadow_close_breakdown: ExecutionCanaryShadowCloseBreakdown,
) -> ExecutionCanaryQuotePnlSummary {
    ExecutionCanaryQuotePnlSummary {
        as_of,
        since,
        limit,
        shadow_close_breakdown,
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
        quote_after_fee_vs_shadow_delta_sol: 0.0,
        skipped_shadow_pnl_sol: 0.0,
        skipped_counterfactual_pnl_sol: 0.0,
        skipped_counterfactual_pnl_after_priority_fee_sol: 0.0,
        skipped_counterfactual_after_fee_vs_shadow_delta_sol: 0.0,
        force_exit_counted_trades: 0,
        force_exit_skipped_entry_trades: 0,
        quote_diagnostics: empty_quote_diagnostics(),
        threshold_summaries: Vec::new(),
        buy_slippage_buckets: Vec::new(),
        entry_decision_delay_buckets: Vec::new(),
        buy_leader_notional_buckets: Vec::new(),
        route_counts: Vec::new(),
        priority_fee_status_counts: Vec::new(),
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
        EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED => record_skipped(summary, &trade),
        _ => record_unknown(summary, &trade),
    }
    record_quote_diagnostics(summary, &trade);
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
    summary.quote_after_fee_vs_shadow_delta_sol += trade
        .quote_after_fee_vs_shadow_delta_sol
        .unwrap_or(quote_pnl_after_fee - trade.shadow_pnl_sol);
    summary.priority_fee_lamports_sum += trade.priority_fee_lamports_total.unwrap_or(0);
    if is_force_exit(trade) {
        summary.force_exit_counted_trades += 1;
    }
    if quote_pnl > 0.0 {
        summary.quote_win_count += 1;
    } else if quote_pnl < 0.0 {
        summary.quote_loss_count += 1;
    }
}

fn record_skipped(
    summary: &mut ExecutionCanaryQuotePnlSummary,
    trade: &ExecutionCanaryQuotePnlTrade,
) {
    summary.skipped_trades += 1;
    summary.skipped_shadow_pnl_sol += trade.shadow_pnl_sol;
    summary.skipped_counterfactual_pnl_sol += trade.skipped_counterfactual_pnl_sol.unwrap_or(0.0);
    summary.skipped_counterfactual_pnl_after_priority_fee_sol += trade
        .skipped_counterfactual_pnl_after_priority_fee_sol
        .unwrap_or(0.0);
    summary.skipped_counterfactual_after_fee_vs_shadow_delta_sol += trade
        .skipped_counterfactual_after_fee_vs_shadow_delta_sol
        .unwrap_or(0.0);
    if is_force_exit(trade) {
        summary.force_exit_skipped_entry_trades += 1;
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

fn is_force_exit(trade: &ExecutionCanaryQuotePnlTrade) -> bool {
    trade.exit_decision_status.as_deref() == Some(DECISION_WOULD_FORCE_EXIT)
}
