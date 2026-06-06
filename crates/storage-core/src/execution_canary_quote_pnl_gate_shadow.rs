use crate::{ExecutionCanaryQuotePnlSummary, ExecutionCanaryQuoteReadinessCheck};

const MAX_ENTRY_SHADOW_GATE_DROP_RATE_PCT: f64 = 20.0;
const WARN_ENTRY_SHADOW_GATE_DROP_RATE_PCT: f64 = 0.0;
const EXPECTED_STRATEGY_DROP_REASONS: &[&str] = &[
    "below_notional",
    "low_holders",
    "low_liquidity",
    "low_volume",
    "not_followed",
    "thin_market",
    "too_new",
];

const CHECK_PASS: &str = "pass";
const CHECK_WARN: &str = "warn";
const CHECK_BLOCK: &str = "block";

pub(crate) fn entry_shadow_gate_check(
    summary: &ExecutionCanaryQuotePnlSummary,
    drop_rate_pct: f64,
) -> ExecutionCanaryQuoteReadinessCheck {
    let gate = &summary.buy_shadow_gate;
    if gate.total_buy_quote_events > 0 && gate.shadow_gate_sampled_events == 0 {
        return check(
            "entry_shadow_gate",
            CHECK_WARN,
            format!(
                "quote_would_execute={} shadow_samples=0",
                gate.quote_would_execute_events
            ),
            "shadow_samples > 0",
            "shadow gate outcome is not available for this window, usually pre-rollout data",
        );
    }

    let (strategy_filtered, unexpected_dropped) = split_drop_reasons(gate);
    let unexpected_drop_rate_pct = percent(unexpected_dropped, gate.quote_would_execute_events);
    let status = if unexpected_drop_rate_pct > MAX_ENTRY_SHADOW_GATE_DROP_RATE_PCT {
        CHECK_BLOCK
    } else if unexpected_drop_rate_pct > WARN_ENTRY_SHADOW_GATE_DROP_RATE_PCT
        || strategy_filtered > 0
        || drop_rate_pct > WARN_ENTRY_SHADOW_GATE_DROP_RATE_PCT
    {
        CHECK_WARN
    } else {
        CHECK_PASS
    };
    check(
        "entry_shadow_gate",
        status,
        format!(
            "quote_would_execute={} recorded={} dropped={} strategy_filtered={} unexpected_dropped={} pending={} drop={} unexpected_drop={}",
            gate.quote_would_execute_events,
            gate.quote_would_execute_shadow_recorded_events,
            gate.quote_would_execute_shadow_dropped_events,
            strategy_filtered,
            unexpected_dropped,
            gate.quote_would_execute_shadow_pending_events,
            pct_value(drop_rate_pct),
            pct_value(unexpected_drop_rate_pct)
        ),
        format!("unexpected_drop <= {MAX_ENTRY_SHADOW_GATE_DROP_RATE_PCT:.1}%"),
        "strategy filters are not execution blockers; unexpected shadow drops must stay low",
    )
}

fn split_drop_reasons(summary: &crate::ExecutionCanaryQuoteShadowGateSummary) -> (u64, u64) {
    let mut strategy_filtered = 0;
    let mut unexpected_dropped = 0;
    for count in &summary.quote_would_execute_drop_reason_counts {
        if EXPECTED_STRATEGY_DROP_REASONS.contains(&count.reason.as_str()) {
            strategy_filtered += count.events;
        } else {
            unexpected_dropped += count.events;
        }
    }
    (strategy_filtered, unexpected_dropped)
}

fn percent(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 * 100.0 / denominator as f64
    }
}

fn check(
    name: &str,
    status: &str,
    value: impl Into<String>,
    threshold: impl Into<String>,
    reason: &str,
) -> ExecutionCanaryQuoteReadinessCheck {
    ExecutionCanaryQuoteReadinessCheck {
        name: name.to_string(),
        status: status.to_string(),
        value: value.into(),
        threshold: threshold.into(),
        reason: reason.to_string(),
    }
}

fn pct_value(value: f64) -> String {
    format!("{value:.2}%")
}
