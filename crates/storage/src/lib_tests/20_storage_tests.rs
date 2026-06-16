use super::*;

#[test]
fn apply_history_retention_preserves_undelivered_warn_events_after_cursor() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("risk-events-retention.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let fresh_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    for (event_id, event_type, severity, ts) in [
        ("info-old", "info_event", "info", stale_ts),
        ("warn-delivered", "warn_event", "warn", stale_ts),
        ("warn-pending", "warn_event", "warn", stale_ts),
        ("warn-fresh", "warn_event", "warn", fresh_ts),
    ] {
        store.conn.execute(
            "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
                 VALUES (?1, ?2, ?3, ?4, NULL)",
            params![event_id, event_type, severity, ts.to_rfc3339()],
        )?;
    }

    let delivered_rowid: i64 = store.conn.query_row(
        "SELECT rowid FROM risk_events WHERE event_id = 'warn-delivered'",
        [],
        |row| row.get(0),
    )?;
    store.upsert_alert_delivery_cursor("webhook", delivered_rowid)?;

    let summary = store.apply_history_retention(
        HistoryRetentionCutoffs {
            risk_events_before: fresh_ts - Duration::days(1),
            copy_signals_before: fresh_ts - Duration::days(1),
            orders_before: fresh_ts - Duration::days(1),
            shadow_closed_trades_before: fresh_ts - Duration::days(1),
            execution_quote_canary_before: fresh_ts - Duration::days(1),
        },
        true,
    )?;
    assert_eq!(summary.risk_events_deleted, 2);
    assert_eq!(summary.risk_events_batches, 1);

    let mut stmt = store.conn.prepare(
        "SELECT event_id
             FROM risk_events
             ORDER BY rowid ASC",
    )?;
    let remaining = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert_eq!(remaining, vec!["warn-pending", "warn-fresh"]);
    Ok(())
}

#[test]
fn exact_money_cutover_state_round_trips_and_applies_activation_boundary() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("exact-money-cutover-state.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    assert_eq!(store.exact_money_cutover_ts()?, None);

    let cutover_ts = DateTime::parse_from_rfc3339("2026-03-10T08:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    store.upsert_exact_money_cutover_state(cutover_ts, Some("test-cutover"))?;

    assert_eq!(store.exact_money_cutover_ts()?, Some(cutover_ts));
    assert!(!store.exact_money_cutover_active_at(cutover_ts - Duration::seconds(1))?);
    assert!(store.exact_money_cutover_active_at(cutover_ts)?);
    assert!(store.exact_money_cutover_active_at(cutover_ts + Duration::seconds(1))?);
    Ok(())
}

#[test]
fn apply_history_retention_preserves_warn_events_before_first_alert_delivery() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("risk-events-retention-no-cursor.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let fresh_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    for (event_id, severity, ts) in [
        ("info-old", "info", stale_ts),
        ("warn-old", "warn", stale_ts),
        ("error-old", "error", stale_ts),
        ("warn-fresh", "warn", fresh_ts),
    ] {
        store.conn.execute(
            "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
                 VALUES (?1, 'risk_event', ?2, ?3, NULL)",
            params![event_id, severity, ts.to_rfc3339()],
        )?;
    }

    let summary = store.apply_history_retention(
        HistoryRetentionCutoffs {
            risk_events_before: fresh_ts - Duration::days(1),
            copy_signals_before: fresh_ts - Duration::days(1),
            orders_before: fresh_ts - Duration::days(1),
            shadow_closed_trades_before: fresh_ts - Duration::days(1),
            execution_quote_canary_before: fresh_ts - Duration::days(1),
        },
        true,
    )?;
    assert_eq!(summary.risk_events_deleted, 1);
    assert_eq!(summary.risk_events_batches, 1);

    let mut stmt = store.conn.prepare(
        "SELECT event_id
             FROM risk_events
             ORDER BY rowid ASC",
    )?;
    let remaining = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert_eq!(remaining, vec!["warn-old", "error-old", "warn-fresh"]);
    Ok(())
}

#[test]
fn latest_risk_event_by_type_returns_latest_row() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("risk-events-latest.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let first_ts = DateTime::parse_from_rfc3339("2026-03-10T08:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let second_ts = DateTime::parse_from_rfc3339("2026-03-10T08:05:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let third_ts = DateTime::parse_from_rfc3339("2026-03-10T08:06:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    store.insert_risk_event("shadow_risk_pause", "warn", first_ts, Some("{\"seq\":1}"))?;
    store.insert_risk_event("other_event", "warn", second_ts, Some("{\"seq\":2}"))?;
    store.insert_risk_event("shadow_risk_pause", "warn", third_ts, Some("{\"seq\":3}"))?;

    let latest = store
        .latest_risk_event_by_type("shadow_risk_pause")?
        .expect("latest event");
    assert_eq!(latest.event_type, "shadow_risk_pause");
    assert_eq!(latest.ts, third_ts.to_rfc3339());
    assert_eq!(latest.details_json.as_deref(), Some("{\"seq\":3}"));
    assert!(latest.rowid > 0);
    Ok(())
}
