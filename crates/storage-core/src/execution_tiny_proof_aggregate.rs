use crate::{
    execution_tiny_order_failures::order_failure_counts, execution_tiny_proof_rows::ProofRow,
    ExecutionTinyEntryFunnel, ExecutionTinyProofLatencyStats, ExecutionTinyProofLatencySummary,
    ExecutionTinyProofOpenPosition, ExecutionTinyProofOrder, ExecutionTinyProofReasonCount,
    ExecutionTinyProofReport, ExecutionTinyProofSummary, ExecutionTinyProofTrade,
    EXECUTION_CANARY_POSITION_STATE_CLOSED, EXECUTION_CANARY_POSITION_STATE_OPEN,
    EXECUTION_STATUS_CANARY_CONFIRMED,
};
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;

const QUOTE_STATUS_OK: &str = "ok";
const DECISION_WOULD_EXECUTE: &str = "would_execute";
const DECISION_WOULD_FORCE_EXIT: &str = "would_force_exit";
const PROOF_STATUS_CLOSED: &str = "tiny_closed";

pub(crate) fn build_report(
    as_of: DateTime<Utc>,
    since: DateTime<Utc>,
    limit: u32,
    entry_funnel: ExecutionTinyEntryFunnel,
    rows: Vec<ProofRow>,
    recent_orders: Vec<ExecutionTinyProofOrder>,
    open_positions: Vec<ExecutionTinyProofOpenPosition>,
) -> ExecutionTinyProofReport {
    let mut acc = ProofAccumulator::default();
    let trades = rows
        .into_iter()
        .map(|row| {
            let trade = classify_trade(row);
            acc.record(&trade);
            trade
        })
        .collect();
    let reason_counts = acc.reason_counts();
    let order_failure_counts = order_failure_counts(&recent_orders);
    let latency = acc.latency.finish();
    let mut summary = acc.summary;
    summary.tiny_open_positions = open_positions.len() as u64;
    summary.tiny_vs_shadow_delta_sol = summary.tiny_realized_pnl_sol - summary.shadow_pnl_sol;
    ExecutionTinyProofReport {
        as_of,
        since,
        limit,
        summary,
        entry_funnel,
        latency,
        reason_counts,
        order_failure_counts,
        trades,
        recent_orders,
        open_positions,
    }
}

#[derive(Default)]
struct ProofAccumulator {
    summary: ExecutionTinyProofSummary,
    latency: LatencyAccumulator,
    reasons: BTreeMap<(String, String), u64>,
}

impl ProofAccumulator {
    fn record(&mut self, trade: &ExecutionTinyProofTrade) {
        self.summary.shadow_market_closed_trades += 1;
        self.summary.shadow_pnl_sol += trade.shadow_pnl_sol;
        if trade.entry_decision_status.as_deref() == Some(DECISION_WOULD_EXECUTE) {
            self.summary.canary_entry_would_execute_trades += 1;
        }
        if matches!(
            trade.exit_decision_status.as_deref(),
            Some(DECISION_WOULD_EXECUTE | DECISION_WOULD_FORCE_EXIT)
        ) {
            self.summary.canary_exit_would_execute_trades += 1;
        }
        if trade.tiny_buy_order.is_some() {
            self.summary.tiny_entry_ordered_trades += 1;
        }
        if trade
            .tiny_buy_order
            .as_ref()
            .is_some_and(|order| order.status == EXECUTION_STATUS_CANARY_CONFIRMED)
        {
            self.summary.tiny_entry_confirmed_trades += 1;
        }
        if trade.tiny_sell_order.is_some() {
            self.summary.tiny_exit_ordered_trades += 1;
        }
        if trade
            .tiny_sell_order
            .as_ref()
            .is_some_and(|order| order.status == EXECUTION_STATUS_CANARY_CONFIRMED)
        {
            self.summary.tiny_exit_confirmed_trades += 1;
        }
        if trade.tiny_position_state.as_deref() == Some(EXECUTION_CANARY_POSITION_STATE_CLOSED) {
            self.summary.tiny_closed_positions += 1;
            self.summary.tiny_realized_pnl_sol += trade.tiny_realized_pnl_sol.unwrap_or(0.0);
        }
        self.latency.record(trade);
        *self
            .reasons
            .entry((trade.proof_stage.clone(), trade.proof_reason.clone()))
            .or_insert(0) += 1;
    }

    fn reason_counts(&self) -> Vec<ExecutionTinyProofReasonCount> {
        self.reasons
            .iter()
            .map(|((stage, reason), trades)| ExecutionTinyProofReasonCount {
                stage: stage.clone(),
                reason: reason.clone(),
                trades: *trades,
            })
            .collect()
    }
}

#[derive(Default)]
struct LatencyAccumulator {
    entry_quote_latency_ms: Sample,
    exit_quote_latency_ms: Sample,
    entry_decision_delay_ms: Sample,
    exit_decision_delay_ms: Sample,
    entry_signal_to_submit_ms: Sample,
    exit_signal_to_submit_ms: Sample,
    entry_quote_to_submit_ms: Sample,
    exit_quote_to_submit_ms: Sample,
    entry_submit_to_confirm_ms: Sample,
    exit_submit_to_confirm_ms: Sample,
}

impl LatencyAccumulator {
    fn record(&mut self, trade: &ExecutionTinyProofTrade) {
        self.entry_quote_latency_ms
            .record(trade.entry_quote_latency_ms);
        self.exit_quote_latency_ms
            .record(trade.exit_quote_latency_ms);
        self.entry_decision_delay_ms
            .record(trade.entry_decision_delay_ms);
        self.exit_decision_delay_ms
            .record(trade.exit_decision_delay_ms);
        if let Some(order) = &trade.tiny_buy_order {
            self.entry_signal_to_submit_ms
                .record_i64(order.signal_to_submit_ms);
            self.entry_quote_to_submit_ms
                .record_i64(order.quote_to_submit_ms);
            self.entry_submit_to_confirm_ms
                .record_i64(order.submit_to_confirm_ms);
        }
        if let Some(order) = &trade.tiny_sell_order {
            self.exit_signal_to_submit_ms
                .record_i64(order.signal_to_submit_ms);
            self.exit_quote_to_submit_ms
                .record_i64(order.quote_to_submit_ms);
            self.exit_submit_to_confirm_ms
                .record_i64(order.submit_to_confirm_ms);
        }
    }

    fn finish(self) -> ExecutionTinyProofLatencySummary {
        ExecutionTinyProofLatencySummary {
            entry_quote_latency_ms: self.entry_quote_latency_ms.finish(),
            exit_quote_latency_ms: self.exit_quote_latency_ms.finish(),
            entry_decision_delay_ms: self.entry_decision_delay_ms.finish(),
            exit_decision_delay_ms: self.exit_decision_delay_ms.finish(),
            entry_signal_to_submit_ms: self.entry_signal_to_submit_ms.finish(),
            exit_signal_to_submit_ms: self.exit_signal_to_submit_ms.finish(),
            entry_quote_to_submit_ms: self.entry_quote_to_submit_ms.finish(),
            exit_quote_to_submit_ms: self.exit_quote_to_submit_ms.finish(),
            entry_submit_to_confirm_ms: self.entry_submit_to_confirm_ms.finish(),
            exit_submit_to_confirm_ms: self.exit_submit_to_confirm_ms.finish(),
        }
    }
}

#[derive(Default)]
struct Sample {
    samples: u64,
    sum: u64,
    max: u64,
}

impl Sample {
    fn record(&mut self, value: Option<u64>) {
        if let Some(value) = value {
            self.samples += 1;
            self.sum = self.sum.saturating_add(value);
            self.max = self.max.max(value);
        }
    }

    fn record_i64(&mut self, value: Option<i64>) {
        if let Some(value) = value.and_then(|value| u64::try_from(value).ok()) {
            self.record(Some(value));
        }
    }

    fn finish(self) -> ExecutionTinyProofLatencyStats {
        ExecutionTinyProofLatencyStats {
            samples: self.samples,
            avg_ms: if self.samples == 0 {
                0.0
            } else {
                self.sum as f64 / self.samples as f64
            },
            max_ms: self.max,
        }
    }
}

fn classify_trade(row: ProofRow) -> ExecutionTinyProofTrade {
    let (proof_status, proof_stage, proof_reason) = classify_reason(&row);
    let tiny_pnl = row.position.pnl_sol;
    ExecutionTinyProofTrade {
        shadow_closed_trade_id: row.shadow_closed_trade_id,
        signal_id: row.signal_id,
        wallet_id: row.wallet_id,
        token: row.token,
        opened_ts: row.opened_ts,
        closed_ts: row.closed_ts,
        proof_status,
        proof_stage,
        proof_reason,
        shadow_pnl_sol: row.shadow_pnl_sol,
        tiny_position_state: row.position.state,
        tiny_position_opened_ts: row.position.opened_ts,
        tiny_position_closed_ts: row.position.closed_ts,
        tiny_position_cost_sol: row.position.cost_sol,
        tiny_realized_pnl_sol: tiny_pnl,
        tiny_vs_shadow_delta_sol: tiny_pnl.map(|pnl| pnl - row.shadow_pnl_sol),
        entry_quote_event_id: row.buy_quote.event_id,
        entry_quote_status: row.buy_quote.quote_status,
        entry_decision_status: row.buy_quote.decision_status,
        entry_decision_reason: row.buy_quote.decision_reason,
        entry_quote_latency_ms: row.buy_quote.quote_latency_ms,
        entry_decision_delay_ms: row.buy_quote.decision_delay_ms,
        entry_priority_fee_lamports: row.buy_quote.priority_fee_lamports,
        exit_quote_event_id: row.sell_quote.event_id,
        exit_quote_status: row.sell_quote.quote_status,
        exit_decision_status: row.sell_quote.decision_status,
        exit_decision_reason: row.sell_quote.decision_reason,
        exit_quote_latency_ms: row.sell_quote.quote_latency_ms,
        exit_decision_delay_ms: row.sell_quote.decision_delay_ms,
        exit_priority_fee_lamports: row.sell_quote.priority_fee_lamports,
        tiny_buy_order: row.buy_order,
        tiny_sell_order: row.sell_order,
    }
}

fn classify_reason(row: &ProofRow) -> (String, String, String) {
    if row.buy_quote.event_id.is_none() {
        return reason("not_opened", "entry_quote", "missing_entry_quote");
    }
    if row.buy_quote.quote_status.as_deref() != Some(QUOTE_STATUS_OK) {
        return reason_status(
            "not_opened",
            "entry_quote",
            "entry_quote_status",
            row.buy_quote.quote_status.as_deref(),
        );
    }
    if row.buy_quote.decision_status.as_deref() != Some(DECISION_WOULD_EXECUTE) {
        return reason_status(
            "not_opened",
            "entry_decision",
            "entry_decision",
            row.buy_quote.decision_status.as_deref(),
        );
    }
    let Some(buy_order) = row.buy_order.as_ref() else {
        return reason("not_opened", "entry_order", "entry_order_missing");
    };
    if buy_order.status != EXECUTION_STATUS_CANARY_CONFIRMED {
        return reason(
            "not_opened",
            "entry_order",
            &order_reason("entry_order", buy_order),
        );
    }
    if row.sell_quote.event_id.is_none() {
        return reason("open_unmatched", "exit_quote", "missing_exit_quote");
    }
    if row.sell_quote.quote_status.as_deref() != Some(QUOTE_STATUS_OK) {
        return reason_status(
            "open_unmatched",
            "exit_quote",
            "exit_quote_status",
            row.sell_quote.quote_status.as_deref(),
        );
    }
    if !matches!(
        row.sell_quote.decision_status.as_deref(),
        Some(DECISION_WOULD_EXECUTE | DECISION_WOULD_FORCE_EXIT)
    ) {
        return reason_status(
            "open_unmatched",
            "exit_decision",
            "exit_decision",
            row.sell_quote.decision_status.as_deref(),
        );
    }
    let Some(sell_order) = row.sell_order.as_ref() else {
        return reason("open_unmatched", "exit_order", "exit_order_missing");
    };
    if sell_order.status != EXECUTION_STATUS_CANARY_CONFIRMED {
        if row.position.state.as_deref() == Some(EXECUTION_CANARY_POSITION_STATE_CLOSED) {
            return reason(
                PROOF_STATUS_CLOSED,
                "position",
                "closed_after_terminal_write_off",
            );
        }
        return reason(
            "open_unmatched",
            "exit_order",
            &order_reason("exit_order", sell_order),
        );
    }
    match row.position.state.as_deref() {
        Some(EXECUTION_CANARY_POSITION_STATE_CLOSED) => {
            reason(PROOF_STATUS_CLOSED, "position", PROOF_STATUS_CLOSED)
        }
        Some(EXECUTION_CANARY_POSITION_STATE_OPEN) => {
            reason("open_unmatched", "position", "tiny_open_after_shadow_close")
        }
        Some(other) => reason("unknown", "position", &format!("position_state:{other}")),
        None => reason("unknown", "position", "tiny_position_missing"),
    }
}

fn order_reason(prefix: &str, order: &ExecutionTinyProofOrder) -> String {
    match order.err_code.as_deref() {
        Some(code) => format!("{prefix}:{}:{code}", order.status),
        None => format!("{prefix}:{}", order.status),
    }
}

fn reason(status: &str, stage: &str, reason: &str) -> (String, String, String) {
    (status.to_string(), stage.to_string(), reason.to_string())
}

fn reason_status(
    status: &str,
    stage: &str,
    prefix: &str,
    value: Option<&str>,
) -> (String, String, String) {
    reason(
        status,
        stage,
        &format!("{prefix}:{}", value.unwrap_or("missing")),
    )
}
