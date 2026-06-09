use crate::execution_canary_quote_pnl_gate::TinyExecutionGateCheck;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TinyRuntimeGateSections {
    pub(crate) runtime_status: String,
    pub(crate) runtime_mode: String,
    pub(crate) can_open_new_tiny_entries: bool,
    pub(crate) can_process_tiny_sells: bool,
    pub(crate) runtime_blocker_count: u64,
    pub(crate) entry_runtime_blocker_count: u64,
    pub(crate) sell_runtime_blocker_count: u64,
    pub(crate) runtime_blockers: Vec<TinyExecutionGateCheck>,
    pub(crate) entry_runtime_blockers: Vec<TinyExecutionGateCheck>,
    pub(crate) sell_runtime_blockers: Vec<TinyExecutionGateCheck>,
    pub(crate) why_not_trading_now: Vec<String>,
}

pub(crate) fn classify_runtime_gate(
    checks: &[TinyExecutionGateCheck],
    warning_count: u64,
    open_position_count: u64,
) -> TinyRuntimeGateSections {
    let entry_runtime_blockers = runtime_blockers(checks, is_entry_runtime_blocker);
    let sell_runtime_blockers = runtime_blockers(checks, is_sell_runtime_blocker);
    let runtime_blockers = runtime_blockers(checks, |name| {
        is_entry_runtime_blocker(name) || is_sell_runtime_blocker(name)
    });
    let can_open_new_tiny_entries = entry_runtime_blockers.is_empty();
    let can_process_tiny_sells = sell_runtime_blockers.is_empty();
    let runtime_mode = runtime_mode(
        can_open_new_tiny_entries,
        can_process_tiny_sells,
        open_position_count,
    );
    let runtime_status = runtime_status(
        can_open_new_tiny_entries,
        can_process_tiny_sells,
        warning_count,
    );
    let why_not_trading_now = runtime_blockers
        .iter()
        .map(runtime_blocker_reason)
        .collect();

    TinyRuntimeGateSections {
        runtime_status,
        runtime_mode,
        can_open_new_tiny_entries,
        can_process_tiny_sells,
        runtime_blocker_count: runtime_blockers.len() as u64,
        entry_runtime_blocker_count: entry_runtime_blockers.len() as u64,
        sell_runtime_blocker_count: sell_runtime_blockers.len() as u64,
        runtime_blockers,
        entry_runtime_blockers,
        sell_runtime_blockers,
        why_not_trading_now,
    }
}

fn runtime_blockers(
    checks: &[TinyExecutionGateCheck],
    predicate: fn(&str) -> bool,
) -> Vec<TinyExecutionGateCheck> {
    checks
        .iter()
        .filter(|check| check.status == "block" && predicate(&check.name))
        .cloned()
        .collect()
}

fn runtime_status(can_open_entries: bool, can_process_sells: bool, warning_count: u64) -> String {
    if can_open_entries && can_process_sells {
        return if warning_count > 0 {
            "ready_with_warnings"
        } else {
            "ready"
        }
        .to_string();
    }
    if !can_open_entries && can_process_sells {
        return "entries_paused".to_string();
    }
    "blocked".to_string()
}

fn runtime_mode(can_open_entries: bool, can_process_sells: bool, open_positions: u64) -> String {
    match (can_open_entries, can_process_sells, open_positions == 0) {
        (true, true, _) => "entries_and_sells_enabled",
        (false, true, true) => "entries_paused_sell_standby_no_open_positions",
        (false, true, false) => "entries_paused_sell_enabled",
        (true, false, _) => "entries_enabled_sell_blocked",
        (false, false, _) => "entries_and_sells_blocked",
    }
    .to_string()
}

fn is_sell_runtime_blocker(name: &str) -> bool {
    if matches!(
        name,
        "canary_entry_submit_enabled"
            | "canary_buy_size"
            | "canary_max_open_positions"
            | "canary_daily_loss_cap"
            | "open_canary_positions"
            | "recent_realized_loss_24h"
    ) {
        return false;
    }
    is_entry_runtime_blocker(name)
}

fn is_entry_runtime_blocker(name: &str) -> bool {
    matches!(
        name,
        "execution_disabled"
            | "canary_enabled"
            | "canary_dry_run"
            | "canary_tiny_submit_enabled"
            | "canary_entry_submit_enabled"
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
            | "pending_signed_submit_orders"
            | "pending_unknown_submit_orders"
            | "retry_ready_orders"
            | "retry_budget_blocked_orders"
    )
}

fn runtime_blocker_reason(check: &TinyExecutionGateCheck) -> String {
    format!(
        "{}={} blocks runtime: {}",
        check.name, check.value, check.reason
    )
}
