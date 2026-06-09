use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ExecutionCanaryQuotePnlSummary, ExecutionCanaryReadinessSummary, ExecutionCanaryStatusReport,
    ExecutionCanarySubmitRiskSummary, EXECUTION_SIMULATION_STATUS_PASSED,
};
use serde::Serialize;
use std::path::Path;

use crate::execution_canary_tiny_config_checks::push_config_checks;

const DEFAULT_TINY_MAX_RECENT_LOSS_SOL_24H: f64 = 0.05;
const TINY_MAX_LATEST_METADATA_AGE_SECONDS: i64 = 6 * 60 * 60;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TinyExecutionGate {
    pub status: String,
    pub can_start_tiny_execution: bool,
    pub can_continue_tiny_execution: bool,
    pub blocker_count: u64,
    pub runtime_blocker_count: u64,
    pub warning_count: u64,
    pub quote_gate_status: String,
    pub latest_order_id: Option<String>,
    pub latest_order_status: Option<String>,
    pub latest_simulation_status: Option<String>,
    pub latest_metadata_age_seconds: Option<i64>,
    pub active_submit_orders: u64,
    pub pending_submit_orders: u64,
    pub retry_ready_orders: u64,
    pub retry_budget_blocked_orders: u64,
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
    submit_risk: &ExecutionCanarySubmitRiskSummary,
    recent_loss_sol_24h: f64,
    as_of: DateTime<Utc>,
    config: Option<&ExecutionConfig>,
    runtime_root: Option<&Path>,
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

    push_config_checks(&mut checks, config, runtime_root);
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
    let open_position_limit = config.map(|config| u64::from(config.canary_max_open_positions));
    let open_position_ok = open_position_limit
        .map(|limit| summary.readiness_gate.open_position_count <= limit)
        .unwrap_or(summary.readiness_gate.open_position_count == 0);
    push_check(
        &mut checks,
        "open_canary_positions",
        open_position_ok,
        summary.readiness_gate.open_position_count.to_string(),
        open_position_limit
            .map(|limit| format!("<={limit}"))
            .unwrap_or_else(|| "0".to_string()),
        "open tiny positions must stay within the configured live cap",
    );
    push_submit_risk_checks(&mut checks, submit_risk);
    let recent_loss_cap_sol = config
        .map(|config| config.canary_max_daily_loss_sol)
        .filter(|cap| cap.is_finite() && *cap > 0.0)
        .unwrap_or(DEFAULT_TINY_MAX_RECENT_LOSS_SOL_24H);
    push_check(
        &mut checks,
        "recent_realized_loss_24h",
        recent_loss_sol_24h <= recent_loss_cap_sol,
        format!("{recent_loss_sol_24h:.6}"),
        format!("<={recent_loss_cap_sol:.6} SOL"),
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
        submit_risk,
        recent_loss_sol_24h,
        checks,
    )
}

fn push_submit_risk_checks(
    checks: &mut Vec<TinyExecutionGateCheck>,
    submit_risk: &ExecutionCanarySubmitRiskSummary,
) {
    push_check(
        checks,
        "pending_signed_submit_orders",
        submit_risk.submitted_with_signature_orders == 0,
        submit_risk.submitted_with_signature_orders.to_string(),
        "0".to_string(),
        "signed pending submit must confirm or expire before tiny execution starts",
    );
    push_check(
        checks,
        "pending_unknown_submit_orders",
        submit_risk.submitted_without_signature_orders == 0,
        submit_risk.submitted_without_signature_orders.to_string(),
        "0".to_string(),
        "unknown submit without signature must retry or expire before starting another tiny run",
    );
    push_check(
        checks,
        "retry_ready_orders",
        submit_risk.retry_ready_orders == 0,
        submit_risk.retry_ready_orders.to_string(),
        "0".to_string(),
        "retry-ready orders must be replayed or expired before tiny execution starts",
    );
    push_check(
        checks,
        "retry_budget_blocked_orders",
        submit_risk.retry_budget_blocked_orders == 0,
        submit_risk.retry_budget_blocked_orders.to_string(),
        "0".to_string(),
        "orders at retry budget must not be submitted again",
    );
}

fn finish_gate(
    summary: &ExecutionCanaryQuotePnlSummary,
    latest_order: Option<&copybot_storage_core::ExecutionCanaryOrder>,
    latest_simulation_status: Option<String>,
    metadata_age_seconds: Option<i64>,
    submit_risk: &ExecutionCanarySubmitRiskSummary,
    recent_loss_sol_24h: f64,
    checks: Vec<TinyExecutionGateCheck>,
) -> TinyExecutionGate {
    let blocker_count = checks
        .iter()
        .filter(|check| check.status == "block")
        .count() as u64;
    let runtime_blocker_count = checks
        .iter()
        .filter(|check| check.status == "block" && is_tiny_runtime_blocker(&check.name))
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
        can_continue_tiny_execution: runtime_blocker_count == 0,
        blocker_count,
        runtime_blocker_count,
        warning_count,
        quote_gate_status: summary.readiness_gate.status.clone(),
        latest_order_id: latest_order.map(|order| order.order_id.clone()),
        latest_order_status: latest_order.map(|order| order.status.clone()),
        latest_simulation_status,
        latest_metadata_age_seconds: metadata_age_seconds,
        active_submit_orders: submit_risk.active_orders,
        pending_submit_orders: submit_risk.submitted_orders,
        retry_ready_orders: submit_risk.retry_ready_orders,
        retry_budget_blocked_orders: submit_risk.retry_budget_blocked_orders,
        recent_realized_loss_sol_24h: recent_loss_sol_24h,
        checks,
    }
}

fn is_tiny_runtime_blocker(name: &str) -> bool {
    matches!(
        name,
        "execution_disabled"
            | "canary_enabled"
            | "canary_dry_run"
            | "canary_tiny_submit_enabled"
            | "canary_wallet_pubkey"
            | "canary_buy_size"
            | "canary_max_open_positions"
            | "canary_daily_loss_cap"
            | "canary_kill_switch_inactive"
            | "execution_signer_pubkey"
            | "execution_signer_matches_canary_wallet"
            | "execution_signer_keypair_path"
            | "execution_signer_keypair_file"
            | "execution_signer_keypair_format"
            | "execution_signer_keypair_pubkey_match"
            | "execution_signer_can_sign_preflight"
            | "submit_adapter_http_url"
            | "submit_timeout_ms"
            | "max_confirm_seconds"
            | "max_submit_attempts"
            | "simulate_before_submit"
            | "pretrade_min_sol_reserve"
            | "pretrade_max_priority_fee_lamports"
            | "quote_canary_enabled"
            | "swap_instructions_dry_run_enabled"
            | "swap_transaction_dry_run_enabled"
            | "priority_fee_canary_enabled"
            | "open_canary_positions"
            | "recent_realized_loss_24h"
    )
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
