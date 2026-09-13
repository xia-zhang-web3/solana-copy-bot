#[path = "common/historical_migration_fixture.rs"]
mod historical;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE};
use copybot_storage_core::{SqliteStore, EXECUTION_SELL_INTENT_STATUS};
use std::path::{Path, PathBuf};
use tempfile::tempdir;

fn migrations() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations")
}
fn seed(s: &SqliteStore) -> Result<DateTime<Utc>> {
    let now: DateTime<Utc> = "2026-09-05T12:00:00Z".parse()?;
    s.record_execution_canary_open_position(
        "owned",
        "token",
        7.0,
        None,
        0.07,
        now - Duration::minutes(1),
    )?;
    for id in ["a1", "a2", "a3", "b"] {
        s.insert_copy_signal(&CopySignalRow {
            signal_id: id.into(),
            wallet_id: "leader".into(),
            side: "sell".into(),
            token: "token".into(),
            notional_sol: 0.1,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.into(),
            ts: now,
            status: EXECUTION_SELL_INTENT_STATUS.into(),
        })?;
    }
    Ok(now)
}
fn page(s: &SqliteStore, limit: u32) -> Result<Vec<String>> {
    s.list_execution_quote_canary_owned_sell_signal_candidate_ids(
        "shadow_recorded",
        Utc::now(),
        limit,
    )
}

#[test]
fn owned_sell_queue_cursor_wraps_ties_and_survives_crash_before_quote() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("queue.db");
    let mut s = SqliteStore::open(&path)?;
    s.run_migrations(&migrations())?;
    let now = seed(&s)?;
    assert_eq!(page(&s, 1)?, vec!["a1"]);
    s.advance_execution_owned_sell_cursor("a1")?;
    // Crash before even loading the selected signal/quote; no order or attempt outcome.
    drop(s);
    let s = SqliteStore::open(&path)?;
    assert_eq!(page(&s, 2)?, vec!["a2", "a3"]);
    assert_eq!(page(&s, 2)?, vec!["a2", "a3"], "lookup itself is read-only");
    s.advance_execution_owned_sell_cursor("a2")?;
    assert_eq!(page(&s, 3)?, vec!["a3", "b", "a1"]);
    s.advance_execution_owned_sell_cursor("b")?;
    s.reserve_execution_canary_order("b", "test", now)?;
    assert_eq!(
        page(&s, 10)?,
        vec!["a1", "a2", "a3"],
        "removed cursor candidate still wraps"
    );
    let c = rusqlite::Connection::open(&path)?;
    assert_eq!(
        c.query_row(
            "SELECT COUNT(*) FROM execution_quote_canary_events",
            [],
            |r| r.get::<_, u32>(0)
        )?,
        0
    );
    for id in ["a1", "a2", "a3", "b"] {
        let signal = s.load_copy_signal_by_signal_id(id)?.unwrap();
        assert_eq!(signal.ts, now);
        assert_eq!(signal.status, EXECUTION_SELL_INTENT_STATUS);
    }
    assert_eq!(s.shadow_open_lots_count()?, 0);
    Ok(())
}

#[test]
fn owned_sell_queue_0053_upgrade_preserves_rows_and_cursor_on_reopen() -> Result<()> {
    let dir = tempdir()?;
    let before = dir.path().join("pre-0053");
    std::fs::create_dir(&before)?;
    for entry in std::fs::read_dir(migrations())? {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_string_lossy().ends_with(".sql") && name.to_string_lossy().as_ref() < "0053" {
            std::fs::copy(entry.path(), before.join(name))?;
        }
    }
    let path = dir.path().join("upgrade.db");
    let mut s = SqliteStore::open(&path)?;
    s.run_migrations(&before)?;
    seed(&s)?;
    let c = rusqlite::Connection::open(&path)?;
    let original_signals = (0..4)
        .map(|i| s.load_copy_signal_by_signal_id(["a1", "a2", "a3", "b"][i]))
        .collect::<Result<Vec<_>>>()?;
    let original_position = s.load_execution_canary_open_position("token")?;
    assert_eq!(
        c.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='execution_owned_sell_cursor'",
            [],
            |r| r.get::<_, u32>(0)
        )?,
        0
    );
    // Freeze the historical 0053..0061 contract; current-schema tests remain above.
    let through61 = dir.path().join("through61");
    historical::prefix(&through61, "0062")?;
    assert_eq!(s.run_migrations(&through61)?, 9);
    assert_eq!(
        c.query_row(
            "SELECT COUNT(*) FROM execution_owned_sell_cursor",
            [],
            |r| r.get::<_, u32>(0)
        )?,
        0
    );
    assert_eq!(s.run_migrations(&through61)?, 0);
    s.advance_execution_owned_sell_cursor("a2")?;
    drop(s);
    let mut s = SqliteStore::open(&path)?;
    assert_eq!(s.run_migrations(&through61)?, 0);
    assert_eq!(page(&s, 2)?, vec!["a3", "b"]);
    let after = (0..4)
        .map(|i| s.load_copy_signal_by_signal_id(["a1", "a2", "a3", "b"][i]))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(format!("{after:?}"), format!("{original_signals:?}"));
    assert_eq!(
        s.load_execution_canary_open_position("token")?,
        original_position
    );
    assert_eq!(
        c.query_row("SELECT COUNT(*) FROM orders", [], |r| r.get::<_, u32>(0))?,
        0
    );
    Ok(())
}

#[test]
fn owned_sell_queue_cursor_write_failure_does_not_claim_progress() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("failure.db");
    let mut s = SqliteStore::open(&path)?;
    s.run_migrations(&migrations())?;
    seed(&s)?;
    s.advance_execution_owned_sell_cursor("a1")?;
    let c = rusqlite::Connection::open(&path)?;
    c.execute_batch("CREATE TRIGGER fail_cursor BEFORE UPDATE ON execution_owned_sell_cursor BEGIN SELECT RAISE(ABORT, 'cursor_write_failure'); END;")?;
    assert!(s.advance_execution_owned_sell_cursor("a2").is_err());
    drop(s);
    let s = SqliteStore::open(&path)?;
    assert_eq!(page(&s, 1)?, vec!["a2"]);
    c.execute_batch("DROP TRIGGER fail_cursor")?;
    s.advance_execution_owned_sell_cursor("a2")?;
    assert_eq!(page(&s, 1)?, vec!["a3"]);
    Ok(())
}
