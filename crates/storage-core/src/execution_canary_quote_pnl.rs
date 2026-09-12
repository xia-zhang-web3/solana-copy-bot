use crate::{
    execution_canary_quote_pnl_accumulator::summarize_quote_pnl,
    execution_canary_quote_pnl_compute::compute_quote_pnl,
    execution_canary_quote_pnl_diagnostics::route_labels,
    execution_canary_quote_pnl_rows::QuotePnlRow,
    execution_quote_canary::ensure_execution_quote_canary_tables, ExecutionCanaryQuotePnlSummary,
    ExecutionCanaryQuotePnlTrade, SqliteDiscoveryStore, EXECUTION_CANARY_QUOTE_PNL_STATUS_COUNTED,
    EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED, EXECUTION_CANARY_QUOTE_PNL_STATUS_UNKNOWN,
};
use anyhow::Result;
use chrono::{DateTime, Utc};

const QUOTE_STATUS_OK: &str = "ok";
const DECISION_WOULD_EXECUTE: &str = "would_execute";
const DECISION_WOULD_SKIP: &str = "would_skip";
const DECISION_WOULD_FORCE_EXIT: &str = "would_force_exit";

impl SqliteDiscoveryStore {
    pub fn execution_canary_quote_pnl_summary(
        &self,
        as_of: DateTime<Utc>,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<ExecutionCanaryQuotePnlSummary> {
        crate::quote_http_timing::quote_http_timing_available(
            &self.conn,
            "execution_quote_canary_events",
        )?;
        ensure_execution_quote_canary_tables(self)?;
        let shadow_close_breakdown = self.execution_canary_shadow_close_breakdown(since)?;
        let open_position_count = self.execution_canary_open_position_count()?;
        let buy_shadow_gate = self.execution_quote_canary_shadow_gate_summary(since, limit)?;
        let cohort = self.quote_fee_cohort(since, Some(as_of), Some(limit))?;
        let trades = cohort
            .rows
            .into_iter()
            .map(classify_trade)
            .collect::<Result<Vec<_>>>()?;
        Ok(summarize_quote_pnl(
            as_of,
            since,
            limit,
            trades,
            cohort.window_total_closed_trades,
            shadow_close_breakdown,
            open_position_count,
            buy_shadow_gate,
        ))
    }
}

fn classify_trade(row: QuotePnlRow) -> Result<ExecutionCanaryQuotePnlTrade> {
    let mut trade = base_trade(&row);
    trade.entry_quote_event_id = row.buy.event_id.clone();
    trade.exit_quote_event_id = row.sell.event_id.clone();
    trade.entry_quote_status = row.buy.quote_status.clone();
    trade.exit_quote_status = row.sell.quote_status.clone();
    trade.entry_priority_fee_status = row.buy.priority_fee_status.clone();
    trade.exit_priority_fee_status = row.sell.priority_fee_status.clone();
    trade.priority_fee_lamports_total = row
        .amounts()
        .buy_priority_fee_lamports
        .zip(row.sell.priority_fee_lamports)
        .and_then(|(buy, sell)| buy.checked_add(sell));
    trade.entry_decision_status = row.buy.decision_status.clone();
    trade.exit_decision_status = row.sell.decision_status.clone();
    trade.buy_leader_notional_sol = row.buy.leader_notional_sol;
    trade.sell_leader_notional_sol = row.sell.leader_notional_sol;
    trade.buy_slippage_bps = row.buy.slippage_bps;
    trade.sell_slippage_bps = row.sell.slippage_bps;
    trade.buy_price_impact_pct = row.buy.price_impact_pct;
    trade.sell_price_impact_pct = row.sell.price_impact_pct;
    trade.entry_decision_delay_ms = row.buy.decision_delay_ms;
    trade.exit_decision_delay_ms = row.sell.decision_delay_ms;
    trade.entry_http_request_started_ts = row.buy.http_request_started_ts;
    trade.exit_http_request_started_ts = row.sell.http_request_started_ts;
    trade.entry_quote_latency_ms = row.buy.quote_latency_ms;
    trade.exit_quote_latency_ms = row.sell.quote_latency_ms;
    trade.entry_quote_price_sol = row.buy.quote_price_sol;
    trade.exit_quote_price_sol = row.sell.quote_price_sol;
    trade.entry_shadow_price_sol = row.buy.shadow_price_sol;
    trade.exit_shadow_price_sol = row.sell.shadow_price_sol;
    trade.entry_route_labels = route_labels(row.buy.route_plan_json.as_deref());
    trade.exit_route_labels = route_labels(row.sell.route_plan_json.as_deref());

    // Entry identity is proven separately by bind_entry; an exit/history error
    // cannot erase a known decision not to enter. Never infer this from the
    // decision string alone (ambiguous or conflicting BUYs remain unknown).
    if row.entry_attributed
        && row.buy.quote_status.as_deref() == Some(QUOTE_STATUS_OK)
        && row.buy.decision_status.as_deref() == Some(DECISION_WOULD_SKIP)
    {
        record_skipped_counterfactual(&mut trade, &row)?;
        return Ok(mark_skipped(trade, entry_reason(&row)));
    }

    if let Some(reason) = row.binding_error {
        return Ok(mark_unknown(trade, reason));
    }
    let Some(buy_status) = row.buy.quote_status.as_deref() else {
        return Ok(mark_unknown(trade, "missing_entry_quote"));
    };
    let Some(sell_status) = row.sell.quote_status.as_deref() else {
        return Ok(mark_unknown(trade, "missing_exit_quote"));
    };
    if buy_status != QUOTE_STATUS_OK {
        return Ok(mark_unknown(
            trade,
            &format!("entry_quote_status:{buy_status}"),
        ));
    }
    if sell_status != QUOTE_STATUS_OK {
        return Ok(mark_unknown(
            trade,
            &format!("exit_quote_status:{sell_status}"),
        ));
    }

    match row.buy.decision_status.as_deref() {
        Some(DECISION_WOULD_EXECUTE) => {}
        Some(status) => return Ok(mark_unknown(trade, &format!("entry_decision:{status}"))),
        None => return Ok(mark_unknown(trade, "entry_decision_missing")),
    }
    match row.sell.decision_status.as_deref() {
        Some(DECISION_WOULD_EXECUTE | DECISION_WOULD_FORCE_EXIT) => {}
        Some(status) => return Ok(mark_unknown(trade, &format!("exit_decision:{status}"))),
        None => return Ok(mark_unknown(trade, "exit_decision_missing")),
    }

    let Some(pnl) = compute_quote_pnl(row.amounts())? else {
        return Ok(mark_unknown(trade, "invalid_quote_amount"));
    };
    trade.status = EXECUTION_CANARY_QUOTE_PNL_STATUS_COUNTED.to_string();
    trade.reason = if pnl.scaled_exit_to_entry_qty {
        "ok_scaled_to_entry_qty".to_string()
    } else {
        "ok".to_string()
    };
    trade.quote_adjusted_pnl_sol = Some(pnl.quote_adjusted_pnl_sol);
    trade.quote_adjusted_pnl_after_priority_fee_sol = pnl.quote_adjusted_pnl_after_priority_fee_sol;
    trade.quote_vs_shadow_delta_sol = Some(pnl.quote_adjusted_pnl_sol - row.shadow_pnl_sol);
    trade.quote_after_fee_vs_shadow_delta_sol = pnl
        .quote_adjusted_pnl_after_priority_fee_sol
        .map(|net| net - row.shadow_pnl_sol);
    trade.entry_cost_sol = Some(pnl.entry_cost_sol);
    trade.exit_quote_sol = Some(pnl.exit_quote_sol);
    trade.closed_qty_ratio = Some(pnl.closed_qty_ratio);
    trade.priority_fee_lamports_total = pnl.priority_fee_lamports_total;
    if pnl.priority_fee_lamports_total.is_none() {
        trade.status = EXECUTION_CANARY_QUOTE_PNL_STATUS_UNKNOWN.to_string();
        trade.reason = row.allocation.reason.clone();
    }
    Ok(trade)
}

/// Only called for an independently proven, successful skipped BUY quote.
/// Exit validation and fee allocation still govern hypothetical money coverage.
fn record_skipped_counterfactual(
    trade: &mut ExecutionCanaryQuotePnlTrade,
    row: &QuotePnlRow,
) -> Result<()> {
    let exit_error = row.binding_error.or_else(|| {
        if row.sell.quote_status.is_none() {
            Some("missing_exit_quote")
        } else if row.sell.quote_status.as_deref() != Some(QUOTE_STATUS_OK) {
            Some("exit_quote_not_ok")
        } else if !matches!(
            row.sell.decision_status.as_deref(),
            Some(DECISION_WOULD_EXECUTE | DECISION_WOULD_FORCE_EXIT)
        ) {
            Some("exit_decision_unproven")
        } else {
            None
        }
    });
    let reason = if let Some(reason) = exit_error {
        reason
    } else if let Some(pnl) = compute_quote_pnl(row.amounts())? {
        trade.skipped_counterfactual_pnl_sol = Some(pnl.quote_adjusted_pnl_sol);
        trade.skipped_counterfactual_pnl_after_priority_fee_sol =
            pnl.quote_adjusted_pnl_after_priority_fee_sol;
        trade.skipped_counterfactual_after_fee_vs_shadow_delta_sol = pnl
            .quote_adjusted_pnl_after_priority_fee_sol
            .map(|net| net - row.shadow_pnl_sol);
        // The allocation reason is bounded and retains missing prior history,
        // unknown fees and partial allocation without inventing another budget.
        &row.allocation.reason
    } else {
        "invalid_quote_amount"
    };
    trade.skipped_counterfactual_reason = Some(reason.to_string());
    Ok(())
}

fn base_trade(row: &QuotePnlRow) -> ExecutionCanaryQuotePnlTrade {
    ExecutionCanaryQuotePnlTrade {
        fee_allocation: row.allocation.clone(),
        shadow_closed_trade_id: row.id,
        signal_id: row.signal_id.clone(),
        wallet_id: row.wallet_id.clone(),
        token: row.token.clone(),
        opened_ts: row.opened_ts,
        closed_ts: row.closed_ts,
        status: EXECUTION_CANARY_QUOTE_PNL_STATUS_UNKNOWN.to_string(),
        reason: "unclassified".to_string(),
        shadow_pnl_sol: row.shadow_pnl_sol,
        quote_adjusted_pnl_sol: None,
        quote_adjusted_pnl_after_priority_fee_sol: None,
        quote_vs_shadow_delta_sol: None,
        quote_after_fee_vs_shadow_delta_sol: None,
        skipped_counterfactual_reason: None,
        skipped_counterfactual_pnl_sol: None,
        skipped_counterfactual_pnl_after_priority_fee_sol: None,
        skipped_counterfactual_after_fee_vs_shadow_delta_sol: None,
        entry_quote_event_id: None,
        exit_quote_event_id: None,
        entry_decision_status: None,
        exit_decision_status: None,
        entry_quote_status: None,
        exit_quote_status: None,
        entry_priority_fee_status: None,
        exit_priority_fee_status: None,
        entry_cost_sol: None,
        exit_quote_sol: None,
        closed_qty_ratio: None,
        buy_leader_notional_sol: None,
        sell_leader_notional_sol: None,
        buy_slippage_bps: None,
        sell_slippage_bps: None,
        buy_price_impact_pct: None,
        sell_price_impact_pct: None,
        entry_decision_delay_ms: None,
        exit_decision_delay_ms: None,
        entry_http_request_started_ts: None,
        exit_http_request_started_ts: None,
        entry_quote_latency_ms: None,
        exit_quote_latency_ms: None,
        entry_quote_price_sol: None,
        exit_quote_price_sol: None,
        entry_shadow_price_sol: None,
        exit_shadow_price_sol: None,
        entry_route_labels: Vec::new(),
        exit_route_labels: Vec::new(),
        priority_fee_lamports_total: None,
    }
}

fn entry_reason(row: &QuotePnlRow) -> String {
    row.buy
        .decision_reason
        .clone()
        .unwrap_or_else(|| "entry_decision:would_skip".to_string())
}

fn mark_unknown(
    mut trade: ExecutionCanaryQuotePnlTrade,
    reason: &str,
) -> ExecutionCanaryQuotePnlTrade {
    if trade.reason == "unclassified" {
        trade.reason = reason.to_string();
    }
    trade.status = EXECUTION_CANARY_QUOTE_PNL_STATUS_UNKNOWN.to_string();
    trade
}

fn mark_skipped(
    mut trade: ExecutionCanaryQuotePnlTrade,
    reason: String,
) -> ExecutionCanaryQuotePnlTrade {
    trade.status = EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED.to_string();
    trade.reason = reason;
    trade
}
