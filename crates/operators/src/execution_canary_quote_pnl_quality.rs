use copybot_storage_core::{
    ExecutionCanaryQuotePnlSummary, ExecutionTinyProofLatencyStats, ExecutionTinyProofReport,
};
use serde::Serialize;

const CONFIRMED: &str = "execution_canary_confirmed";
const DECISION_WOULD_EXECUTE: &str = "would_execute";
const DECISION_WOULD_FORCE_EXIT: &str = "would_force_exit";
const MIN_SAMPLE_TRADES: u64 = 30;
const PROOF_STATUS_CLOSED: &str = "tiny_closed";

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TinyExecutionQualityReport {
    pub verdict: String,
    pub sample_status: String,
    pub shadow_market_closed_trades: u64,
    pub quote_canary_entry_would_execute_trades: u64,
    pub quote_canary_exit_would_execute_trades: u64,
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
    pub tiny_open_positions: u64,
    pub tiny_position_close_coverage_pct: f64,
    pub shadow_pnl_sol: f64,
    pub quote_adjusted_pnl_after_priority_fee_sol: f64,
    pub tiny_realized_pnl_sol: f64,
    pub tiny_vs_shadow_delta_sol: f64,
    pub entry_signal_to_submit_ms: ExecutionTinyProofLatencyStats,
    pub exit_signal_to_submit_ms: ExecutionTinyProofLatencyStats,
    pub entry_submit_to_confirm_ms: ExecutionTinyProofLatencyStats,
    pub exit_submit_to_confirm_ms: ExecutionTinyProofLatencyStats,
    pub top_flow_blockers: Vec<TinyExecutionQualityBlocker>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TinyExecutionQualityBlocker {
    pub stage: String,
    pub reason: String,
    pub count: u64,
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
    let sample_status = sample_status(proof_summary.shadow_market_closed_trades);
    let top_flow_blockers = top_flow_blockers(
        proof,
        entry_failed,
        exit_failed,
        entry_missing,
        exit_missing,
    );
    let verdict = verdict(
        &sample_status,
        entry_failed,
        exit_failed,
        entry_missing,
        exit_missing,
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
        tiny_open_positions: proof_summary.tiny_open_positions,
        tiny_position_close_coverage_pct: pct(
            proof_summary.tiny_closed_positions,
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
            ) && trade.tiny_sell_order.is_none()
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
    open_positions: u64,
    tiny_vs_shadow_delta_sol: f64,
    quote_after_fee_sol: f64,
) -> String {
    if exit_failed > 0 || exit_missing > 0 || open_positions > 0 {
        "sell_flow_loss".to_string()
    } else if entry_failed > 0 || entry_missing > 0 {
        "entry_flow_loss".to_string()
    } else if tiny_vs_shadow_delta_sol < 0.0 || quote_after_fee_sol < 0.0 {
        "execution_cost_negative".to_string()
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
) -> Vec<TinyExecutionQualityBlocker> {
    let mut blockers = Vec::new();
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

fn pct(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        (numerator as f64 / denominator as f64) * 100.0
    }
}
