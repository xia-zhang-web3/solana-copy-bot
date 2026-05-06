use anyhow::{Context, Result};
use chrono::Utc;
use copybot_storage_core::SqliteStore;
use tempfile::tempdir;

#[test]
fn cursor_query_returns_warn_error_rows_after_cursor() -> Result<()> {
    let temp = tempdir()?;
    let store = SqliteStore::open(temp.path().join("system-events.db"))?;
    store.ensure_system_event_tables()?;
    let now = Utc::now();
    store.insert_risk_event("info_event", "info", now, None)?;
    store.insert_risk_event("warn_event_a", "warn", now, Some("{\"seq\":1}"))?;
    store.insert_risk_event("error_event", "error", now, Some("{\"seq\":2}"))?;

    let first = store
        .list_risk_events_after_cursor(None, 10)?
        .into_iter()
        .next()
        .context("first deliverable event missing")?;
    assert_eq!(first.event_type, "warn_event_a");
    store.upsert_alert_delivery_cursor("webhook", first.rowid)?;

    let cursor = store
        .load_alert_delivery_cursor("webhook")?
        .context("cursor missing")?;
    let remaining = store.list_risk_events_after_cursor(Some(cursor), 10)?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].event_type, "error_event");
    Ok(())
}

#[test]
fn latest_and_count_by_type_match_inserted_events() -> Result<()> {
    let temp = tempdir()?;
    let store = SqliteStore::open(temp.path().join("system-events.db"))?;
    store.ensure_system_event_tables()?;
    let now = Utc::now();
    store.insert_risk_event("shadow_risk_pause", "warn", now, Some("{\"seq\":1}"))?;
    store.insert_risk_event("other_event", "warn", now, None)?;
    store.insert_risk_event("shadow_risk_pause", "error", now, Some("{\"seq\":2}"))?;

    assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 2);
    let latest = store
        .latest_risk_event_by_type("shadow_risk_pause")?
        .context("latest row missing")?;
    assert_eq!(latest.severity, "error");
    assert_eq!(
        store
            .list_risk_events_by_type_desc("shadow_risk_pause")?
            .len(),
        2
    );
    Ok(())
}
