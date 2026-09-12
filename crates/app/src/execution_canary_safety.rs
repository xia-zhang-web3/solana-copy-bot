use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::SqliteStore;
use std::path::Path;

#[path = "execution_canary_risk_clock.rs"]
pub(crate) mod risk_clock;

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionCanarySafetySnapshot {
    pub(crate) blocked_reason: Option<&'static str>,
    pub(crate) buy_blocker: crate::execution_canary_summary::BuyBlocker,
    pub(crate) open_positions: u64,
    pub(crate) daily_loss_sol: f64,
    pub(crate) entry_cost: Option<copybot_storage_core::ExecutionCanaryEntryCost>,
}

pub(crate) fn pre_submit_safety_snapshot(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<ExecutionCanarySafetySnapshot> {
    safety_with_decision_time(config, store, || Some(now))
}

pub(crate) fn live_pre_submit_safety_snapshot(
    config: &ExecutionConfig,
    store: &SqliteStore,
    tick_at: DateTime<Utc>,
) -> Result<ExecutionCanarySafetySnapshot> {
    safety_with_decision_time(config, store, || risk_clock::decision_time(tick_at))
}

fn safety_with_decision_time(
    config: &ExecutionConfig,
    store: &SqliteStore,
    decision_time: impl FnOnce() -> Option<DateTime<Utc>>,
) -> Result<ExecutionCanarySafetySnapshot> {
    if Path::new(&config.canary_kill_switch_path).exists() {
        return Ok(ExecutionCanarySafetySnapshot {
            blocked_reason: Some("kill_switch_active"),
            ..ExecutionCanarySafetySnapshot::default()
        });
    }

    if !config.canary_entry_submit_enabled {
        return Ok(ExecutionCanarySafetySnapshot {
            blocked_reason: Some("entry_submit_disabled"),
            ..ExecutionCanarySafetySnapshot::default()
        });
    }

    if store.execution_canary_accounting_pending()? {
        return Ok(ExecutionCanarySafetySnapshot {
            blocked_reason: Some(copybot_storage_core::EXECUTION_ACCOUNTING_PENDING_REASON),
            ..ExecutionCanarySafetySnapshot::default()
        });
    }
    if let Some(id) = store.execution_canary_unresolved_buy_order_id()? {
        return Ok(ExecutionCanarySafetySnapshot {
            blocked_reason: Some(copybot_storage_core::EXECUTION_UNRESOLVED_BUY_REASON),
            buy_blocker: crate::execution_canary_summary::BuyBlocker::unresolved(id),
            ..Default::default()
        });
    }
    let open_positions = store.execution_canary_open_position_count()?;
    if open_positions >= u64::from(config.canary_max_open_positions) {
        return Ok(ExecutionCanarySafetySnapshot {
            blocked_reason: Some("max_open_positions"),
            open_positions,
            ..ExecutionCanarySafetySnapshot::default()
        });
    }

    if config.canary_max_daily_loss_sol <= 0.0 {
        return Ok(ExecutionCanarySafetySnapshot {
            blocked_reason: Some("daily_loss_cap_zero"),
            open_positions,
            ..ExecutionCanarySafetySnapshot::default()
        });
    }

    let Some(as_of) = decision_time() else {
        return Ok(ExecutionCanarySafetySnapshot {
            blocked_reason: Some("risk_decision_clock_unordered"),
            open_positions,
            ..ExecutionCanarySafetySnapshot::default()
        });
    };
    let entry_cost = store.execution_canary_entry_cost(as_of)?;
    let daily_loss_sol = entry_cost.approximate_known_loss_sol()?;
    if entry_cost.cash_loss.unavailable_reason.is_some() {
        return Ok(ExecutionCanarySafetySnapshot {
            blocked_reason: Some("cash_loss_unavailable"),
            open_positions,
            daily_loss_sol,
            entry_cost: Some(entry_cost),
            ..Default::default()
        });
    }
    if entry_cost
        .check_cap(config.canary_max_daily_loss_sol)?
        .exhausted
    {
        return Ok(ExecutionCanarySafetySnapshot {
            blocked_reason: Some("max_daily_loss"),
            open_positions,
            daily_loss_sol,
            entry_cost: Some(entry_cost),
            ..Default::default()
        });
    }

    Ok(ExecutionCanarySafetySnapshot {
        blocked_reason: None,
        open_positions,
        daily_loss_sol,
        entry_cost: Some(entry_cost),
        ..Default::default()
    })
}
