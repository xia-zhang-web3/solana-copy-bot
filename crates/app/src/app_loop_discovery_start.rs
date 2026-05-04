use anyhow::Result;

use super::*;

pub(crate) fn watch_running_discovery_safety(
    store: &SqliteStore,
    observed_swap_writer: &ObservedSwapWriter,
    recent_raw_journal_path: &str,
    discovery_handle: &mut Option<JoinHandle<Result<DiscoveryTaskOutput>>>,
    discovery_recent_raw_journal_abort_reason: &mut Option<&'static str>,
    discovery_runtime_memory_abort_reason: &mut Option<&'static str>,
    discovery_running_trigger: Option<&'static str>,
    discovery_recent_raw_journal_safety_settle: &mut DiscoveryRecentRawJournalSafetySettleState,
) -> Result<()> {
    let observed_swap_writer_snapshot = observed_swap_writer.snapshot();
    let persisted_lag_gate =
        load_discovery_recent_raw_journal_persisted_lag_gate(store, recent_raw_journal_path);
    if let Some(reason) = discovery_recent_raw_journal_abort_reason_from_gates(
        &observed_swap_writer_snapshot,
        &persisted_lag_gate,
    ) {
        discovery_recent_raw_journal_safety_settle.reset();
        *discovery_recent_raw_journal_abort_reason = Some(reason);
        if let Some(handle) = discovery_handle.as_ref() {
            handle.abort();
        }
        if reason == DISCOVERY_CYCLE_ABORTED_DUE_TO_RECENT_RAW_JOURNAL_BACKLOG {
            warn_discovery_aborted_due_to_recent_raw_journal_backlog(
                discovery_running_trigger.unwrap_or("unknown"),
                &observed_swap_writer_snapshot,
            );
        } else {
            warn_discovery_aborted_due_to_recent_raw_journal_persisted_lag(
                discovery_running_trigger.unwrap_or("unknown"),
                &persisted_lag_gate,
            );
        }
        return Ok(());
    }
    let memory_gate = load_discovery_runtime_memory_pressure_gate();
    if let Some(reason) = discovery_runtime_memory_pressure_abort_reason(&memory_gate) {
        *discovery_runtime_memory_abort_reason = Some(reason);
        if let Some(handle) = discovery_handle.as_ref() {
            handle.abort();
        }
        warn_discovery_aborted_due_to_runtime_memory_pressure(
            discovery_running_trigger.unwrap_or("unknown"),
            &memory_gate,
        );
    }
    Ok(())
}

pub(crate) fn try_start_discovery_catch_up(
    store: &SqliteStore,
    observed_swap_writer: &ObservedSwapWriter,
    sqlite_path: &str,
    recent_raw_journal_path: &str,
    recent_raw_journal_replay_batch_size: usize,
    discovery: &DiscoveryService,
    discovery_handle: &mut Option<JoinHandle<Result<DiscoveryTaskOutput>>>,
    discovery_catch_up_pending: &mut bool,
    discovery_catch_up_recent_raw_journal_defer_retry_at: &mut Option<StdInstant>,
    discovery_recent_raw_journal_abort_reason: &mut Option<&'static str>,
    discovery_runtime_memory_abort_reason: &mut Option<&'static str>,
    discovery_running_trigger: &mut Option<&'static str>,
    discovery_recent_raw_journal_safety_settle: &mut DiscoveryRecentRawJournalSafetySettleState,
) -> Result<()> {
    let observed_swap_writer_snapshot = observed_swap_writer.snapshot();
    let catch_up_gate = discovery_catch_up_retrigger_recent_raw_journal_backlog_gate(
        &observed_swap_writer_snapshot,
        *discovery_catch_up_recent_raw_journal_defer_retry_at,
        StdInstant::now(),
    );
    if catch_up_gate.reason.is_some() {
        discovery_recent_raw_journal_safety_settle.reset();
        *discovery_catch_up_pending = catch_up_gate.keep_pending;
        *discovery_catch_up_recent_raw_journal_defer_retry_at = catch_up_gate.next_retry_at;
        warn_discovery_deferred_due_to_recent_raw_journal_backlog(
            "catch_up_retrigger",
            &observed_swap_writer_snapshot,
        );
        return Ok(());
    }
    if !catch_up_gate.should_start {
        *discovery_catch_up_pending = catch_up_gate.keep_pending;
        *discovery_catch_up_recent_raw_journal_defer_retry_at = catch_up_gate.next_retry_at;
        return Ok(());
    }
    let persisted_lag_gate =
        load_discovery_recent_raw_journal_persisted_lag_gate(store, recent_raw_journal_path);
    if let Some(reason) = persisted_lag_gate.reason {
        discovery_recent_raw_journal_safety_settle.reset();
        let catch_up_gate =
            discovery_catch_up_retrigger_recent_raw_journal_defer_gate(reason, StdInstant::now());
        *discovery_catch_up_pending = catch_up_gate.keep_pending;
        *discovery_catch_up_recent_raw_journal_defer_retry_at = catch_up_gate.next_retry_at;
        warn_discovery_deferred_due_to_recent_raw_journal_persisted_lag(
            "catch_up_retrigger",
            &persisted_lag_gate,
        );
        return Ok(());
    }
    let memory_gate = load_discovery_runtime_memory_pressure_gate();
    if let Some(reason) = discovery_runtime_memory_pressure_defer_reason(&memory_gate) {
        let catch_up_gate =
            discovery_catch_up_retrigger_recent_raw_journal_defer_gate(reason, StdInstant::now());
        *discovery_catch_up_pending = catch_up_gate.keep_pending;
        *discovery_catch_up_recent_raw_journal_defer_retry_at = catch_up_gate.next_retry_at;
        warn_discovery_deferred_due_to_runtime_memory_pressure("catch_up_retrigger", &memory_gate);
        return Ok(());
    }
    let settle_gate = discovery_recent_raw_journal_safety_settle_start_gate(
        discovery_recent_raw_journal_safety_settle,
        StdInstant::now(),
    );
    if !settle_gate.should_start {
        let catch_up_gate = discovery_catch_up_retrigger_recent_raw_journal_defer_gate(
            settle_gate.reason.expect("settle deferral reason"),
            StdInstant::now(),
        );
        *discovery_catch_up_pending = catch_up_gate.keep_pending;
        *discovery_catch_up_recent_raw_journal_defer_retry_at = catch_up_gate.next_retry_at;
        warn_discovery_deferred_due_to_recent_raw_journal_settle_window(
            "catch_up_retrigger",
            &settle_gate,
        );
        return Ok(());
    }
    *discovery_catch_up_pending = false;
    *discovery_catch_up_recent_raw_journal_defer_retry_at = None;
    *discovery_recent_raw_journal_abort_reason = None;
    *discovery_runtime_memory_abort_reason = None;
    *discovery_running_trigger = Some("catch_up_retrigger");
    *discovery_handle = Some(spawn_logged_discovery_task(
        sqlite_path.to_string(),
        recent_raw_journal_path.to_string(),
        recent_raw_journal_replay_batch_size,
        discovery.clone(),
        Utc::now(),
        "catch_up_retrigger",
    ));
    Ok(())
}

pub(crate) fn try_start_scheduled_discovery(
    store: &SqliteStore,
    observed_swap_writer: &ObservedSwapWriter,
    sqlite_path: &str,
    recent_raw_journal_path: &str,
    recent_raw_journal_replay_batch_size: usize,
    discovery: &DiscoveryService,
    discovery_handle: &mut Option<JoinHandle<Result<DiscoveryTaskOutput>>>,
    discovery_catch_up_pending: &mut bool,
    discovery_catch_up_recent_raw_journal_defer_retry_at: &mut Option<StdInstant>,
    discovery_recent_raw_journal_abort_reason: &mut Option<&'static str>,
    discovery_runtime_memory_abort_reason: &mut Option<&'static str>,
    discovery_running_trigger: &mut Option<&'static str>,
    discovery_recent_raw_journal_safety_settle: &mut DiscoveryRecentRawJournalSafetySettleState,
) -> Result<()> {
    if discovery_handle.is_some() {
        warn!("discovery cycle still running, skipping scheduled trigger");
        return Ok(());
    }
    let observed_swap_writer_snapshot = observed_swap_writer.snapshot();
    if discovery_recent_raw_journal_backlog_defer_reason(&observed_swap_writer_snapshot).is_some() {
        discovery_recent_raw_journal_safety_settle.reset();
        warn_discovery_deferred_due_to_recent_raw_journal_backlog(
            "scheduled_tick",
            &observed_swap_writer_snapshot,
        );
        return Ok(());
    }
    let persisted_lag_gate =
        load_discovery_recent_raw_journal_persisted_lag_gate(store, recent_raw_journal_path);
    if persisted_lag_gate.reason.is_some() {
        discovery_recent_raw_journal_safety_settle.reset();
        warn_discovery_deferred_due_to_recent_raw_journal_persisted_lag(
            "scheduled_tick",
            &persisted_lag_gate,
        );
        return Ok(());
    }
    let memory_gate = load_discovery_runtime_memory_pressure_gate();
    if discovery_runtime_memory_pressure_defer_reason(&memory_gate).is_some() {
        warn_discovery_deferred_due_to_runtime_memory_pressure("scheduled_tick", &memory_gate);
        return Ok(());
    }
    let settle_gate = discovery_recent_raw_journal_safety_settle_start_gate(
        discovery_recent_raw_journal_safety_settle,
        StdInstant::now(),
    );
    if !settle_gate.should_start {
        warn_discovery_deferred_due_to_recent_raw_journal_settle_window(
            "scheduled_tick",
            &settle_gate,
        );
        return Ok(());
    }
    *discovery_catch_up_pending = false;
    *discovery_catch_up_recent_raw_journal_defer_retry_at = None;
    *discovery_recent_raw_journal_abort_reason = None;
    *discovery_runtime_memory_abort_reason = None;
    *discovery_running_trigger = Some("scheduled_tick");
    *discovery_handle = Some(spawn_logged_discovery_task(
        sqlite_path.to_string(),
        recent_raw_journal_path.to_string(),
        recent_raw_journal_replay_batch_size,
        discovery.clone(),
        Utc::now(),
        "scheduled_tick",
    ));
    Ok(())
}
