fn record_shadow_queue_backpressure_risk_event(
    store: &SqliteStore,
    pending_shadow_task_count: usize,
    held_shadow_task_count: usize,
    shadow_buffered_task_count: usize,
    shadow_pending_capacity: usize,
    now: DateTime<Utc>,
) -> Result<()> {
    let details_json = format!(
        "{{\"reason\":\"queue_backpressure\",\"pending\":{},\"held\":{},\"buffered\":{},\"capacity\":{}}}",
        pending_shadow_task_count,
        held_shadow_task_count,
        shadow_buffered_task_count,
        shadow_pending_capacity
    );
    persist_runtime_risk_event_or_warn(
        store,
        "shadow_queue_saturated",
        "warn",
        now,
        Some(&details_json),
        "failed to persist shadow queue backpressure risk event",
        "failed to persist shadow queue backpressure risk event with fatal sqlite I/O",
    )
}

fn refresh_shadow_open_lot_index_or_warn(
    store: &SqliteStore,
    open_shadow_lots: &mut HashSet<(String, String)>,
) -> Result<()> {
    match store.list_shadow_open_pairs() {
        Ok(pairs) => {
            *open_shadow_lots = pairs;
            Ok(())
        }
        Err(error) => {
            if shadow_open_lot_refresh_error_requires_restart(&error) {
                return Err(error)
                    .context("failed to refresh open shadow lot index with fatal sqlite I/O");
            }
            warn!(error = %error, "failed to refresh open shadow lot index");
            Ok(())
        }
    }
}

fn record_shadow_risk_state_event_or_warn(
    store: &SqliteStore,
    event_type: &str,
    severity: &str,
    ts: DateTime<Utc>,
    details_json: &str,
    fatal_context: &'static str,
) -> Result<()> {
    if let Err(error) = store.insert_risk_event(event_type, severity, ts, Some(details_json)) {
        if shadow_risk_state_event_error_requires_abort(&error) {
            return Err(error).context(fatal_context);
        }
        warn!(
            error = %error,
            event_type,
            severity,
            "failed to persist shadow risk event"
        );
    }
    Ok(())
}
