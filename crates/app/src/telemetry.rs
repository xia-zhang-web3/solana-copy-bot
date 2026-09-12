use copybot_core_types::SwapEvent;
use copybot_shadow::ShadowDropReason;
use std::collections::BTreeMap;
use tracing::{info, warn};

pub(crate) fn reason_to_key(reason: ShadowDropReason) -> &'static str {
    reason.as_str()
}

pub(crate) fn reason_to_stage(reason: ShadowDropReason) -> &'static str {
    match reason {
        ShadowDropReason::Disabled => "disabled",
        ShadowDropReason::NotFollowed => "follow",
        ShadowDropReason::NotSolLeg => "pair",
        ShadowDropReason::BelowNotional => "notional",
        ShadowDropReason::LagExceeded => "lag",
        ShadowDropReason::TooNew
        | ShadowDropReason::LowHolders
        | ShadowDropReason::LowLiquidity
        | ShadowDropReason::LowVolume
        | ShadowDropReason::ThinMarket => "quality",
        ShadowDropReason::RecentSellCooldown => "cooldown",
        ShadowDropReason::InvalidSizing => "sizing",
        ShadowDropReason::DuplicateSignal => "dedupe",
        ShadowDropReason::UnsupportedSide => "side",
    }
}

pub(crate) fn format_error_chain(error: &anyhow::Error) -> String {
    let mut chain = String::new();
    for (idx, cause) in error.chain().enumerate() {
        if idx > 0 {
            chain.push_str(" | ");
        }
        chain.push_str(&cause.to_string());
    }
    chain
}

pub(crate) fn record_shadow_queue_full_buy_drop(
    swap: &SwapEvent,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
) {
    let reason = "queue_full_buy_drop";
    *shadow_drop_reason_counts.entry(reason).or_insert(0) += 1;
    *shadow_drop_stage_counts.entry("scheduler").or_insert(0) += 1;
    *shadow_queue_full_outcome_counts.entry(reason).or_insert(0) += 1;
    warn!(
        stage = "scheduler",
        reason,
        side = "buy",
        wallet = %swap.wallet,
        token = %swap.token_out,
        signature = %swap.signature,
        "shadow gate dropped"
    );
}

pub(crate) fn record_shadow_queue_full_sell_outcome(
    swap: &SwapEvent,
    kept: bool,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
) {
    let reason = "queue_full_sell_kept_or_dropped";
    let outcome_key = if kept {
        "queue_full_sell_kept"
    } else {
        "queue_full_sell_dropped"
    };
    *shadow_queue_full_outcome_counts
        .entry(outcome_key)
        .or_insert(0) += 1;
    if kept {
        info!(
            stage = "scheduler",
            reason,
            outcome = "kept",
            side = "sell",
            wallet = %swap.wallet,
            token = %swap.token_in,
            signature = %swap.signature,
            "shadow queue_full sell outcome"
        );
    } else {
        *shadow_drop_reason_counts
            .entry("queue_full_sell_dropped")
            .or_insert(0) += 1;
        *shadow_drop_stage_counts.entry("scheduler").or_insert(0) += 1;
        warn!(
            stage = "scheduler",
            reason,
            outcome = "dropped",
            side = "sell",
            wallet = %swap.wallet,
            token = %swap.token_in,
            signature = %swap.signature,
            "shadow gate dropped"
        );
    }
}

/// Keep a candidate-local failure paired with its own signal, independently of
/// the last successful quote/order and other error fields in the tick summary.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OwnedSellRecoveryError {
    signal_id: String,
    reason: String,
}

impl OwnedSellRecoveryError {
    pub(crate) fn from_bounded_error(signal_id: &str, reason: &str) -> Self {
        Self {
            signal_id: signal_id.to_owned(),
            reason: reason.to_owned(),
        }
    }
}

/// Emit at the failure boundary so later successful orders cannot replace its ID.
pub(crate) fn record_sell_accounting_write_failure(order_id: &str) {
    warn!(
        target: "copybot_app::app_loop",
        order_id,
        reason = "receipt_accounting_write_failed",
        accounting_status = "pending",
        "SELL accounting deferred after local write rejection"
    );
}

/// Periodic tick event consumed by the application loop.
pub(crate) fn record_execution_canary_tick(
    summary: &crate::execution_canary::ExecutionCanaryTickSummary,
) {
    let recovery_error = summary.last_owned_sell_recovery_error.as_ref();
    let producer = &summary.source_sell_production;
    info!(
        target: "copybot_app::app_loop",
        enabled = summary.enabled,
        dry_run = summary.dry_run,
        route = %summary.route,
        wallet_pubkey = %summary.wallet_pubkey,
        candidates = summary.candidates,
        inserted = summary.inserted,
        existing = summary.existing,
        skipped_reason = summary.skipped_reason.unwrap_or("none"),
        last_signal_id = summary.last_signal_id.as_deref().unwrap_or("none"),
        last_order_id = summary.last_order_id.as_deref().unwrap_or("none"),
        quote_entry_candidates = summary.quote_entry_candidates,
        quote_entry_inserted = summary.quote_entry_inserted,
        quote_entry_existing = summary.quote_entry_existing,
        quote_entry_errors = summary.quote_entry_errors,
        quote_close_candidates = summary.quote_close_candidates,
        quote_close_inserted = summary.quote_close_inserted,
        quote_close_existing = summary.quote_close_existing,
        quote_close_errors = summary.quote_close_errors,
        owned_sell_recovery_signal_id = recovery_error.map(|error| error.signal_id.as_str()).unwrap_or("none"),
        owned_sell_recovery_error = recovery_error.map(|error| error.reason.as_str()).unwrap_or("none"),
        quote_would_execute = summary.quote_would_execute,
        quote_would_force_exit = summary.quote_would_force_exit,
        quote_would_skip = summary.quote_would_skip,
        quote_decision_unknown = summary.quote_decision_unknown,
        last_quote_event_id = summary.last_quote_event_id.as_deref().unwrap_or("none"),
        pre_submit_refusals = summary.pre_submit_refusals.count(),
        pre_submit_refusal_order_id = summary.pre_submit_refusals.order_id(),
        pre_submit_refusal_reason = summary.pre_submit_refusals.reason(),
        source_sell_refusals = summary.source_sell_refusals.count(),
        source_sell_staging_visits = producer.visits,
        source_sell_promoted = producer.inserted,
        source_sell_promotion_existing = producer.existing,
        source_sell_promotion_rejected = producer.rejected,
        source_sell_promotion_malformed = producer.malformed,
        source_sell_staging_wrapped = producer.wrapped,
        source_sell_promotion_refusal_id = producer.refusals.order_id(),
        source_sell_promotion_refusal_reason = producer.refusals.reason(),
        source_sell_refusal_id = summary.source_sell_refusals.order_id(),
        source_sell_refusal_reason = summary.source_sell_refusals.reason(),
        source_sell_write_off_refusals = summary.source_sell_write_off_refusals.count(),
        source_sell_write_off_refusal_order_id = summary.source_sell_write_off_refusals.order_id(),
        source_sell_write_off_refusal_reason = summary.source_sell_write_off_refusals.reason(),
        state_machine_reserved = summary.state_machine_reserved,
        state_machine_existing = summary.state_machine_existing,
        state_machine_built = summary.state_machine_built,
        state_machine_simulated = summary.state_machine_simulated,
        state_machine_submit_disabled = summary.state_machine_submit_disabled,
        state_machine_failed = summary.state_machine_failed,
        state_machine_safety_blocked = summary.state_machine_safety_blocked,
        state_machine_entry_gate_blocked = summary.state_machine_entry_gate_blocked,
        orphan_recovery_checked = summary.orphan_recovery_checked,
        orphan_recovery_recovered = summary.orphan_recovery_recovered,
        orphan_recovery_reconciled = summary.orphan_recovery_reconciled,
        orphan_recovery_skipped_no_history = summary.orphan_recovery_skipped_no_history,
        orphan_recovery_errors = summary.orphan_recovery_errors,
        last_orphan_recovery_token = summary.last_orphan_recovery_token.as_deref().unwrap_or("none"),
        last_state_machine_order_id = summary.last_state_machine_order_id.as_deref().unwrap_or("none"),
        state_machine_skipped_reason = summary.state_machine_skipped_reason.unwrap_or("none"),
        buy_blocker_order_id = summary.buy_blocker.order_id(),
        buy_blocker_reason = summary.buy_blocker.reason(),
        "execution canary dry-run tick"
    );
}

/// One bounded task-local expense write failure; recovery never submits a trade.
pub(crate) fn record_failed_expense_write_failure(order_id: &str) {
    warn!(target: "copybot_app::app_loop", order_id, reason="failed_expense_write_rejected",
        "failed transaction expense remains pending");
}

/// Hot shadow-signal event; same fields/target as the former app-loop log.
pub(crate) fn record_execution_canary_shadow_signal(
    summary: &crate::execution_canary::ExecutionCanaryTickSummary,
    signal: &copybot_shadow::ShadowSignalResult,
) {
    info!(
        target: "copybot_app::app_loop",
        signal_id = %signal.signal_id,
        side = %signal.side,
        token = %signal.token,
        inserted = summary.inserted,
        existing = summary.existing,
        quote_entry_inserted = summary.quote_entry_inserted,
        quote_entry_existing = summary.quote_entry_existing,
        quote_entry_errors = summary.quote_entry_errors,
        quote_close_inserted = summary.quote_close_inserted,
        quote_close_existing = summary.quote_close_existing,
        quote_close_errors = summary.quote_close_errors,
        quote_would_execute = summary.quote_would_execute,
        quote_would_force_exit = summary.quote_would_force_exit,
        quote_would_skip = summary.quote_would_skip,
        quote_decision_unknown = summary.quote_decision_unknown,
        last_quote_event_id = summary.last_quote_event_id.as_deref().unwrap_or("none"),
        pre_submit_refusals = summary.pre_submit_refusals.count(),
        pre_submit_refusal_order_id = summary.pre_submit_refusals.order_id(),
        pre_submit_refusal_reason = summary.pre_submit_refusals.reason(),
        source_sell_refusals = summary.source_sell_refusals.count(),
        source_sell_refusal_id = summary.source_sell_refusals.order_id(),
        source_sell_refusal_reason = summary.source_sell_refusals.reason(),
        source_sell_write_off_refusals = summary.source_sell_write_off_refusals.count(),
        source_sell_write_off_refusal_order_id = summary.source_sell_write_off_refusals.order_id(),
        source_sell_write_off_refusal_reason = summary.source_sell_write_off_refusals.reason(),
        state_machine_reserved = summary.state_machine_reserved,
        state_machine_existing = summary.state_machine_existing,
        state_machine_built = summary.state_machine_built,
        state_machine_simulated = summary.state_machine_simulated,
        state_machine_submit_disabled = summary.state_machine_submit_disabled,
        state_machine_failed = summary.state_machine_failed,
        state_machine_safety_blocked = summary.state_machine_safety_blocked,
        state_machine_entry_gate_blocked = summary.state_machine_entry_gate_blocked,
        orphan_recovery_checked = summary.orphan_recovery_checked,
        orphan_recovery_recovered = summary.orphan_recovery_recovered,
        orphan_recovery_reconciled = summary.orphan_recovery_reconciled,
        orphan_recovery_skipped_no_history = summary.orphan_recovery_skipped_no_history,
        orphan_recovery_errors = summary.orphan_recovery_errors,
        last_orphan_recovery_token = summary.last_orphan_recovery_token.as_deref().unwrap_or("none"),
        state_machine_skipped_reason = summary.state_machine_skipped_reason.as_deref().unwrap_or("none"),
        buy_blocker_order_id = summary.buy_blocker.order_id(),
        buy_blocker_reason = summary.buy_blocker.reason(),
        state_machine_open_positions = summary.state_machine_open_positions,
        state_machine_daily_loss_sol = summary.state_machine_daily_loss_sol,
        state_machine_entry_cost = ?summary.state_machine_entry_cost,
        last_state_machine_order_id = summary.last_state_machine_order_id.as_deref().unwrap_or("none"),
        "execution canary shadow-signal task"
    );
}

pub(crate) mod hot_quote;
