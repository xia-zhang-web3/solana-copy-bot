fn discovery_recent_raw_journal_backlog_defer_reason(
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
) -> Option<&'static str> {
    if observed_swap_writer_snapshot.journal_queue_depth_batches > 0
        || observed_swap_writer_snapshot.journal_overflow_depth_batches > 0
        || observed_swap_writer_snapshot.journal_queue_row_debt > 0
        || observed_swap_writer_snapshot.journal_overflow_row_debt > 0
        || observed_swap_writer_snapshot.journal_writer_inflight_rows > 0
    {
        return Some(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_BACKLOG);
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscoveryRecentRawJournalPersistedLagGate {
    reason: Option<&'static str>,
    observed_tail_cursor: Option<DiscoveryRuntimeCursor>,
    journal_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    lag_seconds: Option<i64>,
    error: Option<String>,
}

fn discovery_observed_tail_cursor_cmp_for_recent_raw_gate(
    left: &DiscoveryRuntimeCursor,
    right: &DiscoveryRuntimeCursor,
) -> std::cmp::Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

fn discovery_recent_raw_journal_persisted_lag_gate_from_cursors(
    observed_tail_cursor: Option<DiscoveryRuntimeCursor>,
    journal_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
) -> DiscoveryRecentRawJournalPersistedLagGate {
    let lag_seconds = match (&observed_tail_cursor, &journal_covered_through_cursor) {
        (Some(observed_tail_cursor), Some(journal_cursor)) => Some(
            observed_tail_cursor
                .ts_utc
                .signed_duration_since(journal_cursor.ts_utc)
                .num_seconds()
                .max(0),
        ),
        _ => None,
    };
    let reason = match (&observed_tail_cursor, &journal_covered_through_cursor) {
        (None, _) => None,
        (Some(_), None) => Some(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_PERSISTED_LAG),
        (Some(observed_tail_cursor), Some(journal_cursor))
            if discovery_observed_tail_cursor_cmp_for_recent_raw_gate(
                journal_cursor,
                observed_tail_cursor,
            ) == std::cmp::Ordering::Less =>
        {
            Some(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_PERSISTED_LAG)
        }
        (Some(_), Some(_)) => None,
    };

    DiscoveryRecentRawJournalPersistedLagGate {
        reason,
        observed_tail_cursor,
        journal_covered_through_cursor,
        lag_seconds,
        error: None,
    }
}

fn discovery_recent_raw_journal_persisted_lag_unproven_gate(
    error: impl Into<String>,
) -> DiscoveryRecentRawJournalPersistedLagGate {
    DiscoveryRecentRawJournalPersistedLagGate {
        reason: Some(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_LAG_UNPROVEN),
        observed_tail_cursor: None,
        journal_covered_through_cursor: None,
        lag_seconds: None,
        error: Some(error.into()),
    }
}

fn load_discovery_recent_raw_journal_persisted_lag_gate(
    runtime_store: &SqliteStore,
    recent_raw_journal_path: &str,
) -> DiscoveryRecentRawJournalPersistedLagGate {
    let load_result =
        (|| -> Result<(Option<DiscoveryRuntimeCursor>, Option<DiscoveryRuntimeCursor>)> {
            let observed_tail_cursor = runtime_store
                .observed_swaps_tail_cursor_read_only()
                .context("failed loading bounded observed_swaps tail cursor")?;
            if observed_tail_cursor.is_none() {
                return Ok((None, None));
            }
            let journal_store = SqliteStore::open_read_only(Path::new(recent_raw_journal_path))
                .with_context(|| {
                    format!(
                        "failed opening recent_raw journal db read-only: {recent_raw_journal_path}"
                    )
                })?;
            let journal_state = journal_store
                .recent_raw_journal_state_cached_read_only_required()
                .context("failed loading cached recent_raw journal state")?;
            Ok((observed_tail_cursor, journal_state.covered_through_cursor))
        })();

    match load_result {
        Ok((observed_tail_cursor, journal_covered_through_cursor)) => {
            discovery_recent_raw_journal_persisted_lag_gate_from_cursors(
                observed_tail_cursor,
                journal_covered_through_cursor,
            )
        }
        Err(error) => {
            discovery_recent_raw_journal_persisted_lag_unproven_gate(format!("{error:#}"))
        }
    }
}
