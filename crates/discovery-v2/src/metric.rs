use crate::accumulator::{
    WalletAccumulator, REJECT_SUSPICIOUS_ACTIVITY, REJECT_TOKEN_QUALITY_EVIDENCE_MISSING,
};
use chrono::{DateTime, Duration, Utc};
use copybot_config::DiscoveryConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
struct RugEvaluation {
    ratio: f64,
    evaluated: u32,
    unevaluated: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2WalletMetric {
    pub wallet_id: String,
    pub trades: u32,
    pub active_days: u32,
    pub buys: u32,
    pub sells: u32,
    pub max_buy_notional_sol: f64,
    pub pnl_sol: f64,
    pub win_rate: f64,
    pub closed_trades: u32,
    pub hold_median_seconds: i64,
    pub buy_total: u32,
    pub tradable_ratio: f64,
    pub missing_quality_evidence_buys: u32,
    pub rug_ratio: f64,
    pub rug_lookahead_evaluated: u32,
    pub rug_lookahead_unevaluated: u32,
    pub live_sol_balance: Option<f64>,
    pub live_token_value_sol: Option<f64>,
    pub live_token_positions: Option<u32>,
    pub live_tradable_token_positions: Option<u32>,
    pub shadow_closed_trades_24h: Option<u32>,
    pub shadow_pnl_sol_24h: Option<f64>,
    pub shadow_roi_24h: Option<f64>,
    #[serde(default)]
    pub shadow_worst_trade_roi_24h: Option<f64>,
    #[serde(default)]
    pub shadow_fast_loss_roi_24h: Option<f64>,
    #[serde(default)]
    pub shadow_stale_copy_loss_roi_24h: Option<f64>,
    #[serde(default)]
    pub executable_feedback_samples: Option<u32>,
    #[serde(default)]
    pub executable_feedback_pnl_after_fee_sol: Option<f64>,
    #[serde(default)]
    pub executable_feedback_flip_rate: Option<f64>,
    #[serde(default)]
    pub rug_feedback_closed_trades: Option<u32>,
    #[serde(default)]
    pub rug_feedback_stale_terminal_closes: Option<u32>,
    #[serde(default)]
    pub rug_feedback_stale_terminal_rate: Option<f64>,
    #[serde(default)]
    pub rug_feedback_stale_terminal_pnl_sol: Option<f64>,
    pub eligible: bool,
    pub reject_reasons: Vec<String>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub score: f64,
    pub maturity_window_days: u32,
    pub maturity_active_days: u32,
    pub maturity_trades: u32,
    pub maturity_preferred: bool,
    pub selection_score: f64,
}

pub(crate) fn wallet_metric_from_accumulator(
    wallet_id: String,
    acc: WalletAccumulator,
    discovery: &DiscoveryConfig,
    now: DateTime<Utc>,
) -> DiscoveryV2WalletMetric {
    let active_days = acc.active_days.len() as u32;
    let hold_median_seconds = median_i64(&acc.hold_samples_sec).unwrap_or(0);
    let win_rate = if acc.closed_trades > 0 {
        acc.wins as f64 / acc.closed_trades as f64
    } else {
        0.0
    };
    let consistency_ratio = positive_day_ratio(&acc.realized_pnl_by_day);
    let roi = if acc.spent_sol > 1e-9 {
        acc.realized_pnl_sol / acc.spent_sol
    } else {
        0.0
    };
    let buy_total = acc.buy_total;
    let tradable_ratio = if buy_total > 0 {
        acc.tradable_buys as f64 / buy_total as f64
    } else {
        0.0
    };
    let missing_quality_evidence_buys = acc.missing_quality_evidence_buys;
    let mut reject_reasons = pre_rug_reject_reasons(
        &acc,
        active_days,
        buy_total,
        tradable_ratio,
        missing_quality_evidence_buys,
        discovery,
        now,
    );
    let rug = rug_evaluation_from_accumulator(&acc);
    if reject_reasons.is_empty() {
        push_rug_reject_reasons(&mut reject_reasons, rug.ratio, rug.unevaluated, discovery);
    }
    let mut score = if reject_reasons.is_empty() {
        let base_score = (0.35 * tanh01(acc.realized_pnl_sol / 2.0))
            + (0.20 * tanh01(roi * 3.0))
            + (0.15 * (win_rate * (acc.closed_trades as f64 / 8.0).min(1.0)).clamp(0.0, 1.0))
            + (0.15 * hold_time_quality_score(hold_median_seconds))
            + (0.10 * consistency_ratio.clamp(0.0, 1.0))
            + (0.05 * if acc.suspicious { 0.0 } else { 1.0 });
        let rug_penalty = if discovery.max_rug_ratio >= 1.0 {
            1.0
        } else {
            (1.0 - rug.ratio).clamp(0.0, 1.0).powi(2)
        };
        (base_score * tradable_ratio.powf(1.5) * rug_penalty).clamp(0.0, 1.0)
    } else {
        0.0
    };
    if !reject_reasons.is_empty() {
        score = 0.0;
    } else if score < discovery.min_score {
        reject_reasons.push("below_min_score".to_string());
    }
    DiscoveryV2WalletMetric {
        wallet_id,
        trades: acc.trades,
        active_days,
        buys: buy_total,
        sells: acc.sells,
        max_buy_notional_sol: acc.max_buy_notional_sol,
        pnl_sol: acc.realized_pnl_sol,
        win_rate,
        closed_trades: acc.closed_trades,
        hold_median_seconds,
        buy_total,
        tradable_ratio,
        missing_quality_evidence_buys,
        rug_ratio: rug.ratio,
        rug_lookahead_evaluated: rug.evaluated,
        rug_lookahead_unevaluated: rug.unevaluated,
        live_sol_balance: None,
        live_token_value_sol: None,
        live_token_positions: None,
        live_tradable_token_positions: None,
        shadow_closed_trades_24h: None,
        shadow_pnl_sol_24h: None,
        shadow_roi_24h: None,
        shadow_worst_trade_roi_24h: None,
        shadow_fast_loss_roi_24h: None,
        shadow_stale_copy_loss_roi_24h: None,
        executable_feedback_samples: None,
        executable_feedback_pnl_after_fee_sol: None,
        executable_feedback_flip_rate: None,
        rug_feedback_closed_trades: None,
        rug_feedback_stale_terminal_closes: None,
        rug_feedback_stale_terminal_rate: None,
        rug_feedback_stale_terminal_pnl_sol: None,
        eligible: reject_reasons.is_empty(),
        reject_reasons,
        first_seen: acc.first_seen,
        last_seen: acc.last_seen,
        score,
        maturity_window_days: 0,
        maturity_active_days: active_days,
        maturity_trades: acc.trades,
        maturity_preferred: false,
        selection_score: score,
    }
}

pub(crate) fn reject_wallet_metric(metric: &mut DiscoveryV2WalletMetric, reason: &str) {
    metric.eligible = false;
    metric.score = 0.0;
    metric.selection_score = 0.0;
    if !metric
        .reject_reasons
        .iter()
        .any(|existing| existing == reason)
    {
        metric.reject_reasons.push(reason.to_string());
    }
}

fn rug_evaluation_from_accumulator(acc: &WalletAccumulator) -> RugEvaluation {
    let total = acc.buy_total;
    let evaluated = acc.rug_lookahead_evaluated.min(total);
    let rugged = acc.rug_lookahead_rugged.min(evaluated);
    let unevaluated = total.saturating_sub(evaluated);
    let risky = rugged.saturating_add(unevaluated);
    RugEvaluation {
        ratio: if total == 0 {
            0.0
        } else {
            risky as f64 / total as f64
        },
        evaluated,
        unevaluated,
    }
}

fn pre_rug_reject_reasons(
    acc: &WalletAccumulator,
    active_days: u32,
    buy_total: u32,
    tradable_ratio: f64,
    missing_quality_evidence_buys: u32,
    discovery: &DiscoveryConfig,
    now: DateTime<Utc>,
) -> Vec<String> {
    let mut reasons = Vec::new();
    let decay_cutoff = now - Duration::days(discovery.decay_window_days.max(1) as i64);
    push_if(
        &mut reasons,
        acc.trades < discovery.min_trades,
        "insufficient_trades",
    );
    push_if(
        &mut reasons,
        active_days < discovery.min_active_days,
        "insufficient_active_days",
    );
    push_if(&mut reasons, acc.suspicious, REJECT_SUSPICIOUS_ACTIVITY);
    push_if(
        &mut reasons,
        acc.max_buy_notional_sol < discovery.min_leader_notional_sol,
        "low_notional",
    );
    push_if(
        &mut reasons,
        acc.last_seen < decay_cutoff,
        "stale_last_seen",
    );
    push_if(
        &mut reasons,
        buy_total < discovery.min_buy_count,
        "insufficient_buy_count",
    );
    push_if(
        &mut reasons,
        tradable_ratio < discovery.min_tradable_ratio,
        "low_tradable_ratio",
    );
    push_if(
        &mut reasons,
        missing_quality_evidence_buys > 0,
        REJECT_TOKEN_QUALITY_EVIDENCE_MISSING,
    );
    push_if(
        &mut reasons,
        discovery.require_open_positions_for_publication
            && !acc.has_actionable_open_positions(now, discovery.metric_snapshot_interval_seconds),
        "open_position_required_missing",
    );
    reasons
}

fn push_rug_reject_reasons(
    reasons: &mut Vec<String>,
    rug_ratio: f64,
    unevaluated_rugs: u32,
    discovery: &DiscoveryConfig,
) {
    let rug_enabled = discovery.max_rug_ratio < 1.0;
    push_if(
        reasons,
        rug_enabled && unevaluated_rugs > 0,
        "rug_lookahead_unevaluated",
    );
    push_if(
        reasons,
        rug_enabled && rug_ratio > discovery.max_rug_ratio,
        "rug_gate",
    );
}

fn push_if(reasons: &mut Vec<String>, condition: bool, reason: &str) {
    if condition {
        reasons.push(reason.to_string());
    }
}

fn positive_day_ratio(values: &HashMap<chrono::NaiveDate, f64>) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.values().filter(|value| **value > 0.0).count() as f64 / values.len() as f64
    }
}

fn tanh01(value: f64) -> f64 {
    ((value.tanh() + 1.0) * 0.5).clamp(0.0, 1.0)
}

fn hold_time_quality_score(median_seconds: i64) -> f64 {
    if median_seconds <= 0 {
        0.0
    } else if median_seconds < 45 {
        0.2
    } else if median_seconds < 120 {
        0.5
    } else if median_seconds <= 6 * 60 * 60 {
        1.0
    } else if median_seconds <= 24 * 60 * 60 {
        0.75
    } else {
        0.4
    }
}

fn median_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        Some(sorted[mid])
    } else {
        Some((sorted[mid - 1] + sorted[mid]) / 2)
    }
}
