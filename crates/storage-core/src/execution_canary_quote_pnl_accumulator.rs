use crate::{
    execution_canary_quote_pnl_buckets::record_quote_pnl_buckets,
    execution_canary_quote_pnl_diagnostics::{empty_quote_diagnostics, record_quote_diagnostics},
    execution_canary_quote_pnl_gate::build_quote_readiness_gate,
    ExecutionCanaryQuotePnlSummary, ExecutionCanaryQuotePnlTrade,
    ExecutionCanaryQuoteShadowGateSummary, ExecutionCanaryShadowCloseBreakdown,
    EXECUTION_CANARY_QUOTE_PNL_STATUS_COUNTED, EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED,
};
use chrono::{DateTime, Utc};

const DECISION_WOULD_FORCE_EXIT: &str = "would_force_exit";

pub(crate) fn summarize_quote_pnl(
    as_of: DateTime<Utc>,
    since: DateTime<Utc>,
    limit: u32,
    trades: Vec<ExecutionCanaryQuotePnlTrade>,
    window_total_closed_trades: u64,
    shadow_close_breakdown: ExecutionCanaryShadowCloseBreakdown,
    open_position_count: u64,
    buy_shadow_gate: ExecutionCanaryQuoteShadowGateSummary,
) -> ExecutionCanaryQuotePnlSummary {
    let mut summary = empty_summary(as_of, since, limit, shadow_close_breakdown, buy_shadow_gate);
    for trade in trades {
        record_trade(&mut summary, trade);
    }
    summary.window_total_closed_trades = Some(window_total_closed_trades);
    summary.sampled_closed_trades = summary.total_closed_trades;
    summary.omitted_closed_trades =
        window_total_closed_trades.checked_sub(summary.sampled_closed_trades);
    record_quote_pnl_buckets(&mut summary);
    summary.readiness_gate = build_quote_readiness_gate(&summary, open_position_count);
    summary
}

fn empty_summary(
    as_of: DateTime<Utc>,
    since: DateTime<Utc>,
    limit: u32,
    shadow_close_breakdown: ExecutionCanaryShadowCloseBreakdown,
    buy_shadow_gate: ExecutionCanaryQuoteShadowGateSummary,
) -> ExecutionCanaryQuotePnlSummary {
    ExecutionCanaryQuotePnlSummary {
        as_of,
        since,
        limit,
        shadow_close_breakdown,
        financial_totals_scope: "selected_market_closes",
        window_total_closed_trades: None,
        sampled_closed_trades: 0,
        omitted_closed_trades: None,
        total_closed_trades: 0,
        matched_quote_trades: 0,
        pnl_counted_trades: 0,
        gross_pnl_counted_trades: 0,
        unknown_priority_fee_trades: 0,
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
        quote_adjusted_pnl_after_priority_fee_sol: Some(0.0),
        quote_vs_shadow_delta_sol: 0.0,
        quote_after_fee_vs_shadow_delta_sol: Some(0.0),
        skipped_shadow_pnl_sol: 0.0,
        skipped_counterfactual_pnl_sol: Some(0.0),
        skipped_counterfactual_gross_known_trades: 0,
        skipped_counterfactual_gross_unknown_trades: 0,
        skipped_counterfactual_net_known_trades: 0,
        skipped_counterfactual_net_unknown_trades: 0,
        skipped_counterfactual_pnl_after_priority_fee_sol: Some(0.0),
        skipped_counterfactual_after_fee_vs_shadow_delta_sol: Some(0.0),
        force_exit_counted_trades: 0,
        force_exit_skipped_entry_trades: 0,
        quote_diagnostics: empty_quote_diagnostics(),
        threshold_summaries: Vec::new(),
        buy_slippage_buckets: Vec::new(),
        entry_decision_delay_buckets: Vec::new(),
        buy_leader_notional_buckets: Vec::new(),
        route_counts: Vec::new(),
        priority_fee_status_counts: Vec::new(),
        priority_fee_lamports_sum: Some(0),
        buy_shadow_gate,
        readiness_gate: Default::default(),
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
    if trade.priority_fee_lamports_total.is_none() {
        summary.unknown_priority_fee_trades += 1;
        summary.quote_adjusted_pnl_after_priority_fee_sol = None;
        summary.quote_after_fee_vs_shadow_delta_sol = None;
        summary.priority_fee_lamports_sum = None;
    }
    if trade.status != EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED {
        if let Some(gross) = trade.quote_adjusted_pnl_sol {
            summary.gross_pnl_counted_trades += 1;
            summary.quote_adjusted_pnl_sol += gross;
            summary.quote_vs_shadow_delta_sol += gross - trade.shadow_pnl_sol;
        }
        add_net(
            &mut summary.quote_adjusted_pnl_after_priority_fee_sol,
            trade.quote_adjusted_pnl_after_priority_fee_sol,
        );
        add_net(
            &mut summary.quote_after_fee_vs_shadow_delta_sol,
            trade.quote_after_fee_vs_shadow_delta_sol,
        );
        summary.priority_fee_lamports_sum = summary
            .priority_fee_lamports_sum
            .zip(trade.priority_fee_lamports_total)
            .and_then(|(sum, fee)| sum.checked_add(fee));
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
    let quote_pnl = trade.quote_adjusted_pnl_sol;
    if is_force_exit(trade) {
        summary.force_exit_counted_trades += 1;
    }
    if quote_pnl.is_some_and(|pnl| pnl > 0.0) {
        summary.quote_win_count += 1;
    } else if quote_pnl.is_some_and(|pnl| pnl < 0.0) {
        summary.quote_loss_count += 1;
    }
}

fn record_skipped(
    summary: &mut ExecutionCanaryQuotePnlSummary,
    trade: &ExecutionCanaryQuotePnlTrade,
) {
    summary.skipped_trades += 1;
    summary.skipped_shadow_pnl_sol += trade.shadow_pnl_sol;
    if trade.skipped_counterfactual_pnl_sol.is_some() {
        summary.skipped_counterfactual_gross_known_trades += 1;
    } else {
        summary.skipped_counterfactual_gross_unknown_trades += 1;
    }
    if trade
        .skipped_counterfactual_pnl_after_priority_fee_sol
        .is_some()
    {
        summary.skipped_counterfactual_net_known_trades += 1;
    } else {
        summary.skipped_counterfactual_net_unknown_trades += 1;
    }
    if trade.skipped_counterfactual_pnl_sol.is_none() {
        record_unknown_reason(summary, trade.skipped_counterfactual_reason.as_deref());
    }
    add_net(
        &mut summary.skipped_counterfactual_pnl_sol,
        trade.skipped_counterfactual_pnl_sol,
    );
    add_net(
        &mut summary.skipped_counterfactual_pnl_after_priority_fee_sol,
        trade.skipped_counterfactual_pnl_after_priority_fee_sol,
    );
    add_net(
        &mut summary.skipped_counterfactual_after_fee_vs_shadow_delta_sol,
        trade.skipped_counterfactual_after_fee_vs_shadow_delta_sol,
    );
    if is_force_exit(trade) {
        summary.force_exit_skipped_entry_trades += 1;
    }
}

fn record_unknown(
    summary: &mut ExecutionCanaryQuotePnlSummary,
    trade: &ExecutionCanaryQuotePnlTrade,
) {
    summary.unknown_trades += 1;
    record_unknown_reason(summary, Some(&trade.reason));
}

fn record_unknown_reason(summary: &mut ExecutionCanaryQuotePnlSummary, reason: Option<&str>) {
    match reason.unwrap_or_default() {
        "missing_entry_quote" => summary.missing_entry_quote_trades += 1,
        "missing_exit_quote" => summary.missing_exit_quote_trades += 1,
        "invalid_quote_amount" | "close_qty_exceeds_entry_quote" => {
            summary.invalid_quote_amount_trades += 1;
        }
        _ => {}
    }
}

fn add_net(sum: &mut Option<f64>, value: Option<f64>) {
    *sum = sum.zip(value).map(|(sum, value)| sum + value);
}

fn is_force_exit(trade: &ExecutionCanaryQuotePnlTrade) -> bool {
    trade.exit_decision_status.as_deref() == Some(DECISION_WOULD_FORCE_EXIT)
}
