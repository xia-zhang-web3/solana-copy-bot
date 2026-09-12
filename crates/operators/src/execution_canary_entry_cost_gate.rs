use crate::execution_canary_quote_pnl_gate::TinyExecutionGateCheck;
use copybot_config::ExecutionConfig;
use copybot_storage_core::ExecutionCanaryEntryCost;

pub(crate) fn push_current_entry_cost_checks(
    checks: &mut Vec<TinyExecutionGateCheck>,
    cost: &ExecutionCanaryEntryCost,
    config: Option<&ExecutionConfig>,
) {
    let result = config
        .ok_or_else(|| anyhow::anyhow!("config unavailable"))
        .and_then(|c| cost.check_cap(c.canary_max_daily_loss_sol));
    let (status, threshold, reason) = match result {
        Ok(cap) => (
            if cap.exhausted { "block" } else { "pass" },
            format!("<{} lamports ({} SOL)", cap.cap_lamports_ceiling, cap.cap_sol_decimal),
            "conservative entry cost: original CLOSED floor plus uncovered per-position day gross cash loss plus known failed fees once; not net/economic daily loss; unknown fees below cap retain runtime policy".into(),
        ),
        Err(error) => ("block", "known valid cap and readable cost schema".into(), error.to_string()),
    };
    checks.push(TinyExecutionGateCheck {
        name: "current_entry_cost_cap".into(),
        status: status.into(),
        value: cost.known_total_lamports.clone().unwrap_or_else(|| {
            format!(
                "unavailable; partial CLOSED+failed={} lamports",
                cost.partial_known_subtotal_lamports
            )
        }),
        threshold,
        reason,
    });
    checks.push(TinyExecutionGateCheck {
        name: "current_entry_cost_coverage".into(),
        status: if cost.selected_cost_complete() {
            "pass"
        } else {
            "warn"
        }
        .into(),
        value: format!(
            "cash={}, failed={}, unknown={}, legacy_closed={}, null_closed={}",
            cost.cash_loss.coverage,
            cost.failed_expenses.coverage,
            cost.failed_expenses.unknown_orders,
            cost.closed_loss.legacy_f64_positions,
            cost.closed_loss.legacy_null_positions
        ),
        threshold: "exact selected cost components; historical completeness remains unverified"
            .into(),
        reason:
            "known subtotal is not full economic loss; existing readiness blockers remain in force"
                .into(),
    });
}
