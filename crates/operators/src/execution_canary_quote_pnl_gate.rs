use chrono::{DateTime, Utc};
use copybot_storage_core::{
    ExecutionCanaryQuotePnlSummary, ExecutionCanaryReadinessSummary, ExecutionCanaryStatusReport,
    EXECUTION_SIMULATION_STATUS_PASSED,
};
use serde::Serialize;

const TINY_MAX_RECENT_LOSS_SOL_24H: f64 = 0.05;
const TINY_MAX_LATEST_METADATA_AGE_SECONDS: i64 = 6 * 60 * 60;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TinyExecutionGate {
    pub status: String,
    pub can_start_tiny_execution: bool,
    pub blocker_count: u64,
    pub warning_count: u64,
    pub quote_gate_status: String,
    pub latest_order_id: Option<String>,
    pub latest_order_status: Option<String>,
    pub latest_simulation_status: Option<String>,
    pub latest_metadata_age_seconds: Option<i64>,
    pub recent_realized_loss_sol_24h: f64,
    pub checks: Vec<TinyExecutionGateCheck>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TinyExecutionGateCheck {
    pub name: String,
    pub status: String,
    pub value: String,
    pub threshold: String,
    pub reason: String,
}

pub(crate) fn build_tiny_execution_gate(
    summary: &ExecutionCanaryQuotePnlSummary,
    canary_status: &ExecutionCanaryStatusReport,
    canary_readiness: &ExecutionCanaryReadinessSummary,
    recent_loss_sol_24h: f64,
    as_of: DateTime<Utc>,
) -> TinyExecutionGate {
    let latest_order = canary_status.latest_order.as_ref();
    let latest = canary_readiness.latest.as_ref();
    let metadata_age_seconds =
        latest
            .and_then(|order| order.metadata_recorded_ts)
            .map(|recorded_ts| {
                as_of
                    .signed_duration_since(recorded_ts)
                    .num_seconds()
                    .max(0)
            });
    let latest_simulation_status = latest_order.and_then(|order| order.simulation_status.clone());
    let mut checks = Vec::new();

    push_check(
        &mut checks,
        "quote_readiness_gate",
        summary.readiness_gate.can_start_tiny_execution,
        summary.readiness_gate.status.clone(),
        "ready".to_string(),
        "quote PnL, skip-rate, latency, priority-fee and stale checks must pass",
    );
    push_check(
        &mut checks,
        "latest_canary_order",
        latest_order.is_some(),
        latest_order
            .map(|order| order.order_id.clone())
            .unwrap_or_else(|| "missing".to_string()),
        "present".to_string(),
        "latest BUY path must produce a canary order before tiny execution",
    );
    push_check(
        &mut checks,
        "latest_entry_decision",
        canary_readiness.readiness_status == "would_enter",
        canary_readiness.readiness_status.clone(),
        "would_enter".to_string(),
        canary_readiness.readiness_reason.clone(),
    );
    push_check(
        &mut checks,
        "latest_metis_simulation",
        latest_simulation_status.as_deref() == Some(EXECUTION_SIMULATION_STATUS_PASSED),
        latest_simulation_status
            .clone()
            .unwrap_or_else(|| "missing".to_string()),
        EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
        "latest canary order must have a passed swap-instructions dry-run simulation",
    );
    push_check(
        &mut checks,
        "latest_metadata_freshness",
        metadata_age_seconds
            .map(|age| age <= TINY_MAX_LATEST_METADATA_AGE_SECONDS)
            .unwrap_or(false),
        metadata_age_seconds
            .map(|age| age.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        format!("<={TINY_MAX_LATEST_METADATA_AGE_SECONDS}s"),
        "latest quote/build metadata must be recent enough for the gate",
    );
    push_check(
        &mut checks,
        "open_canary_positions",
        summary.readiness_gate.open_position_count == 0,
        summary.readiness_gate.open_position_count.to_string(),
        "0".to_string(),
        "tiny execution should start from a flat canary position book",
    );
    push_check(
        &mut checks,
        "recent_realized_loss_24h",
        recent_loss_sol_24h <= TINY_MAX_RECENT_LOSS_SOL_24H,
        format!("{recent_loss_sol_24h:.6}"),
        format!("<={TINY_MAX_RECENT_LOSS_SOL_24H:.6} SOL"),
        "recent canary accounting losses must stay under the tiny-test cap",
    );
    push_warning(
        &mut checks,
        "stale_rug_like_closes",
        summary.shadow_close_breakdown.stale_rug_like_closed_trades == 0,
        summary
            .shadow_close_breakdown
            .stale_rug_like_closed_trades
            .to_string(),
        "0".to_string(),
        "rug-like stale closes are not an automatic blocker, but must be watched",
    );

    finish_gate(
        summary,
        latest_order,
        latest_simulation_status,
        metadata_age_seconds,
        recent_loss_sol_24h,
        checks,
    )
}

fn finish_gate(
    summary: &ExecutionCanaryQuotePnlSummary,
    latest_order: Option<&copybot_storage_core::ExecutionCanaryOrder>,
    latest_simulation_status: Option<String>,
    metadata_age_seconds: Option<i64>,
    recent_loss_sol_24h: f64,
    checks: Vec<TinyExecutionGateCheck>,
) -> TinyExecutionGate {
    let blocker_count = checks
        .iter()
        .filter(|check| check.status == "block")
        .count() as u64;
    let warning_count = checks.iter().filter(|check| check.status == "warn").count() as u64;
    let status = if blocker_count > 0 {
        "blocked"
    } else if warning_count > 0 {
        "ready_with_warnings"
    } else {
        "ready"
    };

    TinyExecutionGate {
        status: status.to_string(),
        can_start_tiny_execution: blocker_count == 0,
        blocker_count,
        warning_count,
        quote_gate_status: summary.readiness_gate.status.clone(),
        latest_order_id: latest_order.map(|order| order.order_id.clone()),
        latest_order_status: latest_order.map(|order| order.status.clone()),
        latest_simulation_status,
        latest_metadata_age_seconds: metadata_age_seconds,
        recent_realized_loss_sol_24h: recent_loss_sol_24h,
        checks,
    }
}

fn push_check(
    checks: &mut Vec<TinyExecutionGateCheck>,
    name: &str,
    ok: bool,
    value: String,
    threshold: String,
    reason: impl Into<String>,
) {
    checks.push(TinyExecutionGateCheck {
        name: name.to_string(),
        status: if ok { "pass" } else { "block" }.to_string(),
        value,
        threshold,
        reason: reason.into(),
    });
}

fn push_warning(
    checks: &mut Vec<TinyExecutionGateCheck>,
    name: &str,
    ok: bool,
    value: String,
    threshold: String,
    reason: impl Into<String>,
) {
    checks.push(TinyExecutionGateCheck {
        name: name.to_string(),
        status: if ok { "pass" } else { "warn" }.to_string(),
        value,
        threshold,
        reason: reason.into(),
    });
}
