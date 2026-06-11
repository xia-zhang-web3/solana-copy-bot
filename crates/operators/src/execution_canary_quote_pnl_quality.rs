use chrono::{DateTime, Utc};
use copybot_storage_core::{
    ExecutionCanaryQuotePnlSummary, ExecutionTinyProofLatencyStats, ExecutionTinyProofOrder,
    ExecutionTinyProofReport,
};
use serde::Serialize;

use crate::execution_canary_quote_pnl_error_class::simulation_error_class;

const CONFIRMED: &str = "execution_canary_confirmed";
const DECISION_WOULD_EXECUTE: &str = "would_execute";
const DECISION_WOULD_FORCE_EXIT: &str = "would_force_exit";
const MIN_SAMPLE_TRADES: u64 = 30;
const FAILED_ORDER_SAMPLE_LIMIT: usize = 5;
const PROOF_STATUS_CLOSED: &str = "tiny_closed";

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TinyExecutionQualityReport {
    pub verdict: String,
    pub sample_status: String,
    pub shadow_market_closed_trades: u64,
    pub quote_canary_entry_would_execute_trades: u64,
    pub quote_canary_exit_would_execute_trades: u64,
    pub shadow_gate_dropped_would_execute_events: u64,
    pub actionable_entry_would_execute_events: u64,
    pub actionable_entry_ordered_events: u64,
    pub actionable_entry_confirmed_events: u64,
    pub actionable_entry_missing_order_events: u64,
    pub actionable_entry_confirmed_coverage_pct: f64,
    pub shadow_pending_entry_would_execute_events: u64,
    pub tiny_entry_ordered_trades: u64,
    pub tiny_entry_confirmed_trades: u64,
    pub tiny_entry_failed_orders: u64,
    pub tiny_entry_missing_orders: u64,
    pub entry_confirmed_coverage_pct: f64,
    pub tiny_exit_ordered_trades: u64,
    pub tiny_exit_confirmed_trades: u64,
    pub tiny_exit_failed_orders: u64,
    pub tiny_exit_missing_orders: u64,
    pub exit_confirmed_coverage_pct: f64,
    pub tiny_closed_positions: u64,
    pub tiny_unique_closed_positions: u64,
    pub tiny_closed_shadow_match_rows: u64,
    pub tiny_duplicate_closed_position_matches: u64,
    pub tiny_open_positions: u64,
    pub tiny_position_close_coverage_pct: f64,
    pub tiny_unique_position_close_coverage_pct: f64,
    pub shadow_pnl_sol: f64,
    pub quote_adjusted_pnl_after_priority_fee_sol: f64,
    pub tiny_realized_pnl_sol: f64,
    pub tiny_vs_shadow_delta_sol: f64,
    pub entry_signal_to_submit_ms: ExecutionTinyProofLatencyStats,
    pub exit_signal_to_submit_ms: ExecutionTinyProofLatencyStats,
    pub entry_submit_to_confirm_ms: ExecutionTinyProofLatencyStats,
    pub exit_submit_to_confirm_ms: ExecutionTinyProofLatencyStats,
    pub top_flow_blockers: Vec<TinyExecutionQualityBlocker>,
    pub failed_entry_order_samples: Vec<TinyExecutionQualityOrderSample>,
    pub failed_exit_order_samples: Vec<TinyExecutionQualityOrderSample>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TinyExecutionQualityBlocker {
    pub stage: String,
    pub reason: String,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TinyExecutionQualityOrderSample {
    pub order_id: String,
    pub signal_id: String,
    pub token: Option<String>,
    pub status: String,
    pub err_code: Option<String>,
    pub simulation_status: Option<String>,
    pub simulation_error_class: String,
    pub decision_reason: Option<String>,
    pub route: String,
    pub quote_source: Option<String>,
    pub quote_event_id: Option<String>,
    pub priority_fee_lamports: Option<u64>,
    pub attempt: u32,
    pub signal_ts: Option<DateTime<Utc>>,
    pub quote_request_ts: Option<DateTime<Utc>>,
    pub build_recorded_ts: Option<DateTime<Utc>>,
    pub submit_ts: DateTime<Utc>,
    pub confirm_ts: Option<DateTime<Utc>>,
    pub signal_to_quote_ms: Option<i64>,
    pub quote_to_build_ms: Option<i64>,
    pub build_to_submit_ms: Option<i64>,
    pub signal_to_submit_ms: Option<i64>,
    pub quote_to_submit_ms: Option<i64>,
    pub submit_to_confirm_ms: Option<i64>,
}

pub fn build_tiny_execution_quality(
    summary: &ExecutionCanaryQuotePnlSummary,
    proof: &ExecutionTinyProofReport,
) -> TinyExecutionQualityReport {
    let proof_summary = &proof.summary;
    let entry_failed = failed_orders(proof, "buy");
    let exit_failed = failed_orders(proof, "sell");
    let entry_missing = missing_entry_orders(proof);
    let exit_missing = missing_exit_orders(proof);
    let actionable_entry_missing = proof.entry_funnel.actionable_tiny_missing_order_events;
    let sample_status = sample_status(proof_summary.shadow_market_closed_trades);
    let top_flow_blockers = top_flow_blockers(
        proof,
        entry_failed,
        exit_failed,
        entry_missing,
        exit_missing,
        actionable_entry_missing,
    );
    let verdict = verdict(
        &sample_status,
        entry_failed,
        exit_failed,
        entry_missing,
        exit_missing,
        actionable_entry_missing,
        proof.entry_funnel.quote_would_execute_shadow_dropped_events,
        proof_summary.tiny_open_positions,
        proof_summary.tiny_vs_shadow_delta_sol,
        summary.quote_adjusted_pnl_after_priority_fee_sol,
    );

    TinyExecutionQualityReport {
        verdict,
        sample_status,
        shadow_market_closed_trades: proof_summary.shadow_market_closed_trades,
        quote_canary_entry_would_execute_trades: proof_summary.canary_entry_would_execute_trades,
        quote_canary_exit_would_execute_trades: proof_summary.canary_exit_would_execute_trades,
        shadow_gate_dropped_would_execute_events: proof
            .entry_funnel
            .quote_would_execute_shadow_dropped_events,
        actionable_entry_would_execute_events: proof
            .entry_funnel
            .actionable_quote_would_execute_events,
        actionable_entry_ordered_events: proof.entry_funnel.actionable_tiny_ordered_events,
        actionable_entry_confirmed_events: proof.entry_funnel.actionable_tiny_confirmed_events,
        actionable_entry_missing_order_events: actionable_entry_missing,
        actionable_entry_confirmed_coverage_pct: pct(
            proof.entry_funnel.actionable_tiny_confirmed_events,
            proof.entry_funnel.actionable_quote_would_execute_events,
        ),
        shadow_pending_entry_would_execute_events: proof
            .entry_funnel
            .quote_would_execute_shadow_pending_events,
        tiny_entry_ordered_trades: proof_summary.tiny_entry_ordered_trades,
        tiny_entry_confirmed_trades: proof_summary.tiny_entry_confirmed_trades,
        tiny_entry_failed_orders: entry_failed,
        tiny_entry_missing_orders: entry_missing,
        entry_confirmed_coverage_pct: pct(
            proof_summary.tiny_entry_confirmed_trades,
            proof_summary.canary_entry_would_execute_trades,
        ),
        tiny_exit_ordered_trades: proof_summary.tiny_exit_ordered_trades,
        tiny_exit_confirmed_trades: proof_summary.tiny_exit_confirmed_trades,
        tiny_exit_failed_orders: exit_failed,
        tiny_exit_missing_orders: exit_missing,
        exit_confirmed_coverage_pct: pct(
            proof_summary.tiny_exit_confirmed_trades,
            proof_summary.canary_exit_would_execute_trades,
        ),
        tiny_closed_positions: proof_summary.tiny_closed_positions,
        tiny_unique_closed_positions: proof_summary.tiny_unique_closed_positions,
        tiny_closed_shadow_match_rows: proof_summary.tiny_closed_shadow_match_rows,
        tiny_duplicate_closed_position_matches: proof_summary
            .tiny_duplicate_closed_position_matches,
        tiny_open_positions: proof_summary.tiny_open_positions,
        tiny_position_close_coverage_pct: pct(
            proof_summary.tiny_closed_positions,
            proof_summary.tiny_entry_confirmed_trades,
        ),
        tiny_unique_position_close_coverage_pct: pct(
            proof_summary.tiny_unique_closed_positions,
            proof_summary.tiny_entry_confirmed_trades,
        ),
        shadow_pnl_sol: proof_summary.shadow_pnl_sol,
        quote_adjusted_pnl_after_priority_fee_sol: summary
            .quote_adjusted_pnl_after_priority_fee_sol,
        tiny_realized_pnl_sol: proof_summary.tiny_realized_pnl_sol,
        tiny_vs_shadow_delta_sol: proof_summary.tiny_vs_shadow_delta_sol,
        entry_signal_to_submit_ms: proof.latency.entry_signal_to_submit_ms.clone(),
        exit_signal_to_submit_ms: proof.latency.exit_signal_to_submit_ms.clone(),
        entry_submit_to_confirm_ms: proof.latency.entry_submit_to_confirm_ms.clone(),
        exit_submit_to_confirm_ms: proof.latency.exit_submit_to_confirm_ms.clone(),
        top_flow_blockers,
        failed_entry_order_samples: failed_order_samples(proof, "buy"),
        failed_exit_order_samples: failed_order_samples(proof, "sell"),
    }
}

fn failed_orders(proof: &ExecutionTinyProofReport, side: &str) -> u64 {
    proof
        .recent_orders
        .iter()
        .filter(|order| order.side.as_deref() == Some(side) && order.status != CONFIRMED)
        .count() as u64
}

fn missing_entry_orders(proof: &ExecutionTinyProofReport) -> u64 {
    proof
        .trades
        .iter()
        .filter(|trade| {
            trade.entry_decision_status.as_deref() == Some(DECISION_WOULD_EXECUTE)
                && trade.tiny_buy_order.is_none()
        })
        .count() as u64
}

fn missing_exit_orders(proof: &ExecutionTinyProofReport) -> u64 {
    proof
        .trades
        .iter()
        .filter(|trade| {
            matches!(
                trade.exit_decision_status.as_deref(),
                Some(DECISION_WOULD_EXECUTE | DECISION_WOULD_FORCE_EXIT)
            ) && trade
                .tiny_buy_order
                .as_ref()
                .is_some_and(|order| order.status == CONFIRMED)
                && trade.tiny_sell_order.is_none()
                && trade.proof_status != PROOF_STATUS_CLOSED
        })
        .count() as u64
}

fn sample_status(shadow_market_closed_trades: u64) -> String {
    if shadow_market_closed_trades == 0 {
        "no_sample".to_string()
    } else if shadow_market_closed_trades < MIN_SAMPLE_TRADES {
        "thin_sample".to_string()
    } else {
        "sampled".to_string()
    }
}

fn verdict(
    sample_status: &str,
    entry_failed: u64,
    exit_failed: u64,
    entry_missing: u64,
    exit_missing: u64,
    actionable_entry_missing: u64,
    shadow_gate_dropped_would_execute: u64,
    open_positions: u64,
    tiny_vs_shadow_delta_sol: f64,
    quote_after_fee_sol: f64,
) -> String {
    if exit_failed > 0 || exit_missing > 0 {
        "sell_flow_loss".to_string()
    } else if entry_failed > 0 || entry_missing > 0 || actionable_entry_missing > 0 {
        "entry_flow_loss".to_string()
    } else if open_positions > 0 {
        "open_positions_present".to_string()
    } else if tiny_vs_shadow_delta_sol < 0.0 || quote_after_fee_sol < 0.0 {
        "execution_cost_negative".to_string()
    } else if shadow_gate_dropped_would_execute > 0 {
        "shadow_gate_filtered".to_string()
    } else if sample_status != "sampled" {
        sample_status.to_string()
    } else {
        "healthy".to_string()
    }
}

fn top_flow_blockers(
    proof: &ExecutionTinyProofReport,
    entry_failed: u64,
    exit_failed: u64,
    entry_missing: u64,
    exit_missing: u64,
    actionable_entry_missing: u64,
) -> Vec<TinyExecutionQualityBlocker> {
    let mut blockers = Vec::new();
    if actionable_entry_missing > 0 {
        blockers.push(TinyExecutionQualityBlocker {
            stage: "entry_order".to_string(),
            reason: "missing_tiny_order_after_actionable_would_execute".to_string(),
            count: actionable_entry_missing,
        });
    }
    if entry_missing > 0 {
        blockers.push(TinyExecutionQualityBlocker {
            stage: "entry_order".to_string(),
            reason: "missing_tiny_order_after_would_execute".to_string(),
            count: entry_missing,
        });
    }
    if exit_missing > 0 {
        blockers.push(TinyExecutionQualityBlocker {
            stage: "exit_order".to_string(),
            reason: "missing_tiny_sell_order_after_would_execute".to_string(),
            count: exit_missing,
        });
    }
    for count in &proof
        .entry_funnel
        .quote_would_execute_shadow_drop_reason_counts
    {
        if count.events == 0 {
            continue;
        }
        blockers.push(TinyExecutionQualityBlocker {
            stage: "shadow_gate".to_string(),
            reason: format!("shadow_dropped:{}", count.reason),
            count: count.events,
        });
    }
    for count in &proof.order_failure_counts {
        if count.orders == 0 {
            continue;
        }
        if count.side == "buy" && entry_failed > 0 {
            blockers.push(TinyExecutionQualityBlocker {
                stage: "entry_order".to_string(),
                reason: format!("{}:{}", count.err_code, count.simulation_error_class),
                count: count.orders,
            });
        } else if count.side == "sell" && exit_failed > 0 {
            blockers.push(TinyExecutionQualityBlocker {
                stage: "exit_order".to_string(),
                reason: format!("{}:{}", count.err_code, count.simulation_error_class),
                count: count.orders,
            });
        }
    }
    blockers
}

fn failed_order_samples(
    proof: &ExecutionTinyProofReport,
    side: &str,
) -> Vec<TinyExecutionQualityOrderSample> {
    proof
        .recent_orders
        .iter()
        .filter(|order| order.side.as_deref() == Some(side) && order.status != CONFIRMED)
        .take(FAILED_ORDER_SAMPLE_LIMIT)
        .map(order_sample)
        .collect()
}

fn order_sample(order: &ExecutionTinyProofOrder) -> TinyExecutionQualityOrderSample {
    TinyExecutionQualityOrderSample {
        order_id: order.order_id.clone(),
        signal_id: order.signal_id.clone(),
        token: order.token.clone(),
        status: order.status.clone(),
        err_code: order.err_code.clone(),
        simulation_status: order.simulation_status.clone(),
        simulation_error_class: simulation_error_class(order.simulation_error.as_deref()),
        decision_reason: order.decision_reason.clone(),
        route: order.route.clone(),
        quote_source: order.quote_source.clone(),
        quote_event_id: order.quote_event_id.clone(),
        priority_fee_lamports: order.priority_fee_lamports,
        attempt: order.attempt,
        signal_ts: order.signal_ts,
        quote_request_ts: order.quote_request_ts,
        build_recorded_ts: order.build_recorded_ts,
        submit_ts: order.submit_ts,
        confirm_ts: order.confirm_ts,
        signal_to_quote_ms: order.signal_to_quote_ms,
        quote_to_build_ms: order.quote_to_build_ms,
        build_to_submit_ms: order.build_to_submit_ms,
        signal_to_submit_ms: order.signal_to_submit_ms,
        quote_to_submit_ms: order.quote_to_submit_ms,
        submit_to_confirm_ms: order.submit_to_confirm_ms,
    }
}

fn pct(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        (numerator as f64 / denominator as f64) * 100.0
    }
}
