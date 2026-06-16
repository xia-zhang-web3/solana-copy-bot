use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::{HistoryRetentionCutoffs, SqliteStore};
use rusqlite::{params, Connection};
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

fn open_test_store(name: &str) -> Result<(SqliteStore, Connection)> {
    let temp = tempdir()?;
    let path = temp.keep().join(format!("{name}.db"));
    let store = SqliteStore::open(&path)?;
    store.ensure_history_retention_tables()?;
    let conn = Connection::open(&path)?;
    Ok((store, conn))
}

fn cutoffs(reference: DateTime<Utc>) -> HistoryRetentionCutoffs {
    HistoryRetentionCutoffs {
        risk_events_before: reference - Duration::days(1),
        copy_signals_before: reference - Duration::days(1),
        orders_before: reference - Duration::days(1),
        shadow_closed_trades_before: reference - Duration::days(1),
        execution_quote_canary_before: reference - Duration::days(1),
    }
}

#[test]
fn preserves_undelivered_warn_events_after_cursor() -> Result<()> {
    let (store, conn) = open_test_store("risk-events-retention")?;
    let stale = ts("2026-03-01T12:00:00Z");
    let fresh = ts("2026-03-06T12:00:00Z");
    for (event_id, severity, event_ts) in [
        ("info-old", "info", stale),
        ("warn-delivered", "warn", stale),
        ("warn-pending", "warn", stale),
        ("warn-fresh", "warn", fresh),
    ] {
        conn.execute(
            "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
             VALUES (?1, 'risk_event', ?2, ?3, NULL)",
            params![event_id, severity, event_ts.to_rfc3339()],
        )?;
    }
    let delivered_rowid: i64 = conn.query_row(
        "SELECT rowid FROM risk_events WHERE event_id = 'warn-delivered'",
        [],
        |row| row.get(0),
    )?;
    store.upsert_alert_delivery_cursor("webhook", delivered_rowid)?;

    let summary = store.apply_history_retention(cutoffs(fresh), true)?;
    assert_eq!(summary.risk_events_deleted, 2);
    let remaining = event_ids(&conn, "risk_events", "event_id")?;
    assert_eq!(remaining, vec!["warn-pending", "warn-fresh"]);
    Ok(())
}

#[test]
fn deletes_terminal_execution_history_child_first() -> Result<()> {
    let (store, conn) = open_test_store("execution-history-retention")?;
    let stale = ts("2026-03-01T12:00:00Z");
    let fresh = ts("2026-03-06T12:00:00Z");
    seed_signal(&conn, "sig-old", stale, "execution_confirmed")?;
    seed_signal(&conn, "sig-pending", stale, "execution_submitted")?;
    seed_signal(&conn, "sig-fresh", fresh, "execution_confirmed")?;
    seed_order(
        &conn,
        "ord-old",
        "sig-old",
        stale,
        Some(stale),
        "execution_confirmed",
    )?;
    seed_order(
        &conn,
        "ord-pending",
        "sig-pending",
        stale,
        None,
        "execution_submitted",
    )?;
    seed_order(
        &conn,
        "ord-fresh",
        "sig-fresh",
        fresh,
        Some(fresh),
        "execution_confirmed",
    )?;
    seed_fill(&conn, "ord-old")?;
    seed_fill(&conn, "ord-fresh")?;

    let summary = store.apply_history_retention(cutoffs(fresh), false)?;
    assert_eq!(summary.fills_deleted, 1);
    assert_eq!(summary.orders_deleted, 1);
    assert_eq!(summary.copy_signals_deleted, 1);
    assert_eq!(count_rows(&conn, "fills")?, 1);
    assert_eq!(count_rows(&conn, "orders")?, 2);
    assert_eq!(count_rows(&conn, "copy_signals")?, 2);
    Ok(())
}

#[test]
fn bounded_run_stops_before_full_sweep_when_budget_exhausts() -> Result<()> {
    let (store, conn) = open_test_store("history-retention-bounded")?;
    let stale = ts("2026-03-01T12:00:00Z");
    let fresh = ts("2026-03-06T12:00:00Z");
    for idx in 0..501 {
        conn.execute(
            "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
             VALUES (?1, 'risk_event', 'info', ?2, NULL)",
            params![format!("old-{idx}"), stale.to_rfc3339()],
        )?;
    }

    let summary = store.apply_history_retention_bounded(cutoffs(fresh), false, 1, 1, 1, 1, 1)?;
    assert_eq!(summary.risk_events_deleted, 500);
    assert_eq!(summary.risk_events_batches, 1);
    assert!(!summary.completed_full_sweep);
    assert_eq!(count_rows(&conn, "risk_events")?, 1);
    Ok(())
}

#[test]
fn deletes_shadow_closed_trades_in_batches() -> Result<()> {
    let (store, conn) = open_test_store("shadow-closed-retention")?;
    let stale_open = ts("2026-03-01T10:00:00Z");
    let stale_close = ts("2026-03-01T12:00:00Z");
    let fresh_close = ts("2026-03-06T12:00:00Z");
    for idx in 0..501 {
        seed_shadow_closed_trade(&conn, &format!("sig-old-{idx}"), stale_open, stale_close)?;
    }
    seed_shadow_closed_trade(&conn, "sig-fresh", stale_open, fresh_close)?;

    let summary = store.apply_history_retention(cutoffs(fresh_close), false)?;
    assert_eq!(summary.shadow_closed_trades_deleted, 501);
    assert_eq!(summary.shadow_closed_trades_batches, 2);
    assert_eq!(count_rows(&conn, "shadow_closed_trades")?, 1);
    Ok(())
}

#[test]
fn deletes_execution_quote_canary_history_in_batches() -> Result<()> {
    let (store, conn) = open_test_store("quote-canary-retention")?;
    let stale = ts("2026-03-01T12:00:00Z");
    let fresh = ts("2026-03-06T12:00:00Z");
    seed_quote_canary_event(&conn, "event-old", "sig-old", stale)?;
    seed_quote_canary_event(&conn, "event-fresh", "sig-fresh", fresh)?;
    seed_quote_canary_provider_sample(&conn, "event-old", "generic", stale)?;
    seed_quote_canary_provider_sample(&conn, "event-fresh", "generic", fresh)?;
    seed_quote_canary_shadow_gate(&conn, "sig-old", stale)?;
    seed_quote_canary_shadow_gate(&conn, "sig-fresh", fresh)?;

    let summary = store.apply_history_retention(cutoffs(fresh), false)?;
    assert_eq!(summary.execution_quote_canary_events_deleted, 1);
    assert_eq!(summary.execution_quote_canary_provider_samples_deleted, 1);
    assert_eq!(summary.execution_quote_canary_shadow_gate_events_deleted, 1);
    assert_eq!(count_rows(&conn, "execution_quote_canary_events")?, 1);
    assert_eq!(
        count_rows(&conn, "execution_quote_canary_provider_samples")?,
        1
    );
    assert_eq!(
        count_rows(&conn, "execution_quote_canary_shadow_gate_events")?,
        1
    );
    Ok(())
}

fn seed_signal(conn: &Connection, signal_id: &str, ts: DateTime<Utc>, status: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO copy_signals(signal_id, wallet_id, side, token, notional_sol, ts, status)
         VALUES (?1, 'wallet-1', 'buy', 'token-1', 0.5, ?2, ?3)",
        params![signal_id, ts.to_rfc3339(), status],
    )?;
    Ok(())
}

fn seed_order(
    conn: &Connection,
    order_id: &str,
    signal_id: &str,
    submit_ts: DateTime<Utc>,
    confirm_ts: Option<DateTime<Utc>>,
    status: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO orders(
            order_id, signal_id, route, submit_ts, confirm_ts, status,
            err_code, client_order_id, tx_signature, simulation_status,
            simulation_error, attempt
         ) VALUES (?1, ?2, 'rpc', ?3, ?4, ?5, NULL, ?6, 'sig', NULL, NULL, 1)",
        params![
            order_id,
            signal_id,
            submit_ts.to_rfc3339(),
            confirm_ts.map(|value| value.to_rfc3339()),
            status,
            format!("client-{order_id}"),
        ],
    )?;
    Ok(())
}

fn seed_fill(conn: &Connection, order_id: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO fills(order_id, token, qty, avg_price, fee, slippage_bps)
         VALUES (?1, 'token-1', 1.0, 0.05, 0.001, 10.0)",
        params![order_id],
    )?;
    Ok(())
}

fn seed_shadow_closed_trade(
    conn: &Connection,
    signal_id: &str,
    opened: DateTime<Utc>,
    closed: DateTime<Utc>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO shadow_closed_trades(
            signal_id, wallet_id, token, qty, entry_cost_sol, exit_value_sol,
            pnl_sol, opened_ts, closed_ts
         ) VALUES (?1, 'wallet-1', 'token-1', 10.0, 0.10, 0.12, 0.02, ?2, ?3)",
        params![signal_id, opened.to_rfc3339(), closed.to_rfc3339()],
    )?;
    Ok(())
}

fn seed_quote_canary_event(
    conn: &Connection,
    event_id: &str,
    signal_id: &str,
    request_ts: DateTime<Utc>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO execution_quote_canary_events(
            event_id, signal_id, wallet_id, token, side, quote_status, request_ts
         ) VALUES (?1, ?2, 'wallet-1', 'token-1', 'buy', 'ok', ?3)",
        params![event_id, signal_id, request_ts.to_rfc3339()],
    )?;
    Ok(())
}

fn seed_quote_canary_provider_sample(
    conn: &Connection,
    event_id: &str,
    provider: &str,
    request_ts: DateTime<Utc>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO execution_quote_canary_provider_samples(
            event_id, provider, side, quote_status, request_ts
         ) VALUES (?1, ?2, 'buy', 'ok', ?3)",
        params![event_id, provider, request_ts.to_rfc3339()],
    )?;
    Ok(())
}

fn seed_quote_canary_shadow_gate(
    conn: &Connection,
    signal_id: &str,
    recorded_ts: DateTime<Utc>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO execution_quote_canary_shadow_gate_events(
            signal_id, wallet_id, token, side, status, recorded_ts
         ) VALUES (?1, 'wallet-1', 'token-1', 'buy', 'shadow_recorded', ?2)",
        params![signal_id, recorded_ts.to_rfc3339()],
    )?;
    Ok(())
}

fn count_rows(conn: &Connection, table: &str) -> Result<i64> {
    Ok(
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })?,
    )
}

fn event_ids(conn: &Connection, table: &str, column: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("SELECT {column} FROM {table} ORDER BY rowid ASC"))?;
    let values = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(values)
}
