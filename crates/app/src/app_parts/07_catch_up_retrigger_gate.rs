#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DiscoveryCatchUpRetriggerBacklogGate {
    should_start: bool,
    keep_pending: bool,
    next_retry_at: Option<StdInstant>,
    reason: Option<&'static str>,
}

fn discovery_catch_up_retrigger_recent_raw_journal_defer_gate(
    reason: &'static str,
    now: StdInstant,
) -> DiscoveryCatchUpRetriggerBacklogGate {
    DiscoveryCatchUpRetriggerBacklogGate {
        should_start: false,
        keep_pending: true,
        next_retry_at: Some(now + DISCOVERY_CATCH_UP_RECENT_RAW_JOURNAL_BACKLOG_RETRY_INTERVAL),
        reason: Some(reason),
    }
}

fn discovery_catch_up_retrigger_recent_raw_journal_backlog_gate(
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
    retry_at: Option<StdInstant>,
    now: StdInstant,
) -> DiscoveryCatchUpRetriggerBacklogGate {
    if let Some(retry_at) = retry_at {
        if now < retry_at {
            return DiscoveryCatchUpRetriggerBacklogGate {
                should_start: false,
                keep_pending: true,
                next_retry_at: Some(retry_at),
                reason: None,
            };
        }
    }

    if let Some(reason) =
        discovery_recent_raw_journal_backlog_defer_reason(observed_swap_writer_snapshot)
    {
        return discovery_catch_up_retrigger_recent_raw_journal_defer_gate(reason, now);
    }

    DiscoveryCatchUpRetriggerBacklogGate {
        should_start: true,
        keep_pending: false,
        next_retry_at: None,
        reason: None,
    }
}
