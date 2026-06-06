use crate::{ExecutionCanaryQuotePnlSummary, ExecutionCanaryQuoteReadinessCheck};

const MAX_ENTRY_SHADOW_GATE_DROP_RATE_PCT: f64 = 20.0;
const WARN_ENTRY_SHADOW_GATE_DROP_RATE_PCT: f64 = 0.0;

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

    let status = if drop_rate_pct > MAX_ENTRY_SHADOW_GATE_DROP_RATE_PCT {
        CHECK_BLOCK
    } else if drop_rate_pct > WARN_ENTRY_SHADOW_GATE_DROP_RATE_PCT {
        CHECK_WARN
    } else {
        CHECK_PASS
    };
    check(
        "entry_shadow_gate",
        status,
        format!(
            "quote_would_execute={} recorded={} dropped={} pending={} drop={}",
            gate.quote_would_execute_events,
            gate.quote_would_execute_shadow_recorded_events,
            gate.quote_would_execute_shadow_dropped_events,
            gate.quote_would_execute_shadow_pending_events,
            pct_value(drop_rate_pct)
        ),
        format!("drop <= {MAX_ENTRY_SHADOW_GATE_DROP_RATE_PCT:.1}%"),
        "quote-approved BUYs must also pass shadow gates and become recorded shadow entries",
    )
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
