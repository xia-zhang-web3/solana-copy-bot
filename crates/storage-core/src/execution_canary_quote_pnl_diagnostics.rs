use crate::{
    ExecutionCanaryQuoteDiagnosticsSummary, ExecutionCanaryQuotePnlSummary,
    ExecutionCanaryQuotePnlTrade, ExecutionCanaryQuoteSideDiagnostics,
    EXECUTION_CANARY_QUOTE_PNL_STATUS_COUNTED, EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED,
};
use serde_json::Value;

pub(crate) fn record_quote_diagnostics(
    summary: &mut ExecutionCanaryQuotePnlSummary,
    trade: &ExecutionCanaryQuotePnlTrade,
) {
    record_side(
        &mut summary.quote_diagnostics.entry_all,
        trade,
        QuoteSide::Entry,
    );
    record_side(
        &mut summary.quote_diagnostics.exit_all,
        trade,
        QuoteSide::Exit,
    );
    match trade.status.as_str() {
        EXECUTION_CANARY_QUOTE_PNL_STATUS_COUNTED => {
            record_side(
                &mut summary.quote_diagnostics.entry_counted,
                trade,
                QuoteSide::Entry,
            );
            record_side(
                &mut summary.quote_diagnostics.exit_counted,
                trade,
                QuoteSide::Exit,
            );
        }
        EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED => {
            record_side(
                &mut summary.quote_diagnostics.entry_skipped,
                trade,
                QuoteSide::Entry,
            );
            record_side(
                &mut summary.quote_diagnostics.exit_skipped_entry,
                trade,
                QuoteSide::Exit,
            );
        }
        _ => {}
    }
}

pub(crate) fn route_labels(route_plan_json: Option<&str>) -> Vec<String> {
    let Some(raw) = route_plan_json else {
        return Vec::new();
    };
    let Ok(Value::Array(steps)) = serde_json::from_str::<Value>(raw) else {
        return Vec::new();
    };
    steps
        .iter()
        .filter_map(|step| step.pointer("/swapInfo/label")?.as_str())
        .map(ToString::to_string)
        .collect()
}

fn record_side(
    diagnostics: &mut ExecutionCanaryQuoteSideDiagnostics,
    trade: &ExecutionCanaryQuotePnlTrade,
    side: QuoteSide,
) {
    diagnostics.events += 1;
    let (delay, latency, slippage, impact) = match side {
        QuoteSide::Entry => (
            trade.entry_decision_delay_ms,
            trade.entry_quote_latency_ms,
            trade.buy_slippage_bps,
            trade.buy_price_impact_pct,
        ),
        QuoteSide::Exit => (
            trade.exit_decision_delay_ms,
            trade.exit_quote_latency_ms,
            trade.sell_slippage_bps,
            trade.sell_price_impact_pct,
        ),
    };
    record_u64_sample(
        &mut diagnostics.decision_delay_ms_samples,
        &mut diagnostics.decision_delay_ms_avg,
        &mut diagnostics.decision_delay_ms_max,
        delay,
    );
    record_u64_sample(
        &mut diagnostics.quote_latency_ms_samples,
        &mut diagnostics.quote_latency_ms_avg,
        &mut diagnostics.quote_latency_ms_max,
        latency,
    );
    record_f64_sample(
        &mut diagnostics.slippage_bps_samples,
        &mut diagnostics.slippage_bps_avg,
        &mut diagnostics.slippage_bps_max,
        slippage,
    );
    record_f64_sample(
        &mut diagnostics.price_impact_pct_samples,
        &mut diagnostics.price_impact_pct_avg,
        &mut diagnostics.price_impact_pct_max,
        impact,
    );
}

fn record_u64_sample(samples: &mut u64, avg: &mut f64, max: &mut u64, sample: Option<u64>) {
    let Some(value) = sample else {
        return;
    };
    *samples += 1;
    *avg = running_avg(*avg, *samples, value as f64);
    *max = (*max).max(value);
}

fn record_f64_sample(samples: &mut u64, avg: &mut f64, max: &mut f64, sample: Option<f64>) {
    let Some(value) = sample.filter(|value| value.is_finite()) else {
        return;
    };
    *samples += 1;
    *avg = running_avg(*avg, *samples, value);
    *max = (*max).max(value);
}

fn running_avg(current: f64, samples: u64, next: f64) -> f64 {
    current + (next - current) / samples as f64
}

#[derive(Debug, Clone, Copy)]
enum QuoteSide {
    Entry,
    Exit,
}

pub(crate) fn empty_quote_diagnostics() -> ExecutionCanaryQuoteDiagnosticsSummary {
    ExecutionCanaryQuoteDiagnosticsSummary::default()
}
