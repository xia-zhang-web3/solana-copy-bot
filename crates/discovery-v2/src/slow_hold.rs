use crate::metric::DiscoveryV2WalletMetric;
use chrono::{DateTime, Duration, Utc};
use copybot_config::DiscoveryConfig;

pub(crate) const SOURCE_BASELINE: &str = "baseline";
pub(crate) const SOURCE_SLOW_HOLD: &str = "slow_hold";

const RELAXED_REJECT_REASONS: &[&str] = &[
    "below_min_score",
    "insufficient_trades",
    "insufficient_active_days",
    "insufficient_buy_count",
    "stale_last_seen",
    "open_position_required_missing",
];

pub(crate) fn slow_hold_score(
    metric: &DiscoveryV2WalletMetric,
    discovery: &DiscoveryConfig,
    now: DateTime<Utc>,
) -> Option<f64> {
    if !discovery.slow_hold_wallets_enabled {
        return None;
    }
    if metric.hold_median_seconds < discovery.slow_hold_min_hold_median_seconds as i64 {
        return None;
    }
    if metric.trades < discovery.slow_hold_min_trades
        || metric.buy_total < discovery.slow_hold_min_buy_count
        || metric.active_days < discovery.slow_hold_min_active_days
    {
        return None;
    }
    if metric.max_buy_notional_sol < discovery.min_leader_notional_sol
        || metric.tradable_ratio < discovery.min_tradable_ratio
        || metric.missing_quality_evidence_buys > 0
    {
        return None;
    }
    let stale_cutoff = now - Duration::days(discovery.slow_hold_max_stale_days.max(1) as i64);
    if metric.last_seen < stale_cutoff {
        return None;
    }
    if discovery.max_rug_ratio < 1.0
        && (metric.rug_lookahead_unevaluated > 0 || metric.rug_ratio > discovery.max_rug_ratio)
    {
        return None;
    }
    if metric
        .reject_reasons
        .iter()
        .any(|reason| !RELAXED_REJECT_REASONS.contains(&reason.as_str()))
    {
        return None;
    }
    let score = (0.40 * tanh01(metric.pnl_sol / 2.0))
        + (0.20
            * (metric.win_rate
                * (metric.closed_trades as f64 / discovery.slow_hold_min_trades.max(1) as f64)
                    .min(1.0))
            .clamp(0.0, 1.0))
        + (0.25 * slow_hold_quality(metric.hold_median_seconds))
        + (0.10 * metric.tradable_ratio.clamp(0.0, 1.0))
        + (0.05
            * (metric.active_days as f64 / discovery.slow_hold_min_active_days.max(1) as f64)
                .min(1.0));
    let rug_penalty = if discovery.max_rug_ratio >= 1.0 {
        1.0
    } else {
        (1.0 - metric.rug_ratio).clamp(0.0, 1.0).powi(2)
    };
    let score = (score * rug_penalty).clamp(0.0, 1.0);
    (score >= discovery.slow_hold_min_score).then_some(score)
}

fn tanh01(value: f64) -> f64 {
    ((value.tanh() + 1.0) * 0.5).clamp(0.0, 1.0)
}

fn slow_hold_quality(median_seconds: i64) -> f64 {
    if median_seconds <= 0 {
        0.0
    } else if median_seconds < 30 * 60 {
        0.4
    } else if median_seconds <= 6 * 60 * 60 {
        1.0
    } else if median_seconds <= 24 * 60 * 60 {
        0.9
    } else {
        0.8
    }
}
