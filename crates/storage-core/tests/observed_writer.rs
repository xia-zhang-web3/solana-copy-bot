use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage_core::SqliteDiscoveryStore;
use rusqlite::Connection;
use tempfile::tempdir;

fn ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

fn swap(signature: &str, wallet: &str, ts_utc: DateTime<Utc>) -> SwapEvent {
    SwapEvent {
        signature: signature.to_string(),
        wallet: wallet.to_string(),
        dex: "raydium".to_string(),
        token_in: "SOL".to_string(),
        token_out: "TOKEN".to_string(),
        amount_in: 1.0,
        amount_out: 2.0,
        slot: 42,
        ts_utc,
        exact_amounts: None,
    }
}

#[test]
fn observed_batch_write_persists_activity_days_atomically() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    let first_ts = ts("2026-05-05T10:00:00Z")?;
    let later_ts = ts("2026-05-05T12:00:00Z")?;
    let next_day_ts = first_ts + Duration::days(1);

    let summary = store.insert_observed_swaps_batch_with_activity_days_measured(&[
        swap("sig-a", "wallet-a", first_ts),
        swap("sig-a", "wallet-a", later_ts),
        swap("sig-b", "wallet-a", next_day_ts),
    ])?;

    assert_eq!(summary.inserted, vec![true, false, true]);

    let rows = store.load_observed_swaps_since(first_ts - Duration::minutes(1))?;
    assert_eq!(rows.len(), 2);
    drop(store);

    let conn = Connection::open(&db_path)?;
    let activity_rows: i64 = conn.query_row(
        "SELECT COUNT(*) FROM wallet_activity_days WHERE wallet_id = 'wallet-a'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(activity_rows, 2);

    let first_day_last_seen: String = conn.query_row(
        "SELECT last_seen FROM wallet_activity_days
         WHERE wallet_id = 'wallet-a' AND activity_day = '2026-05-05'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(first_day_last_seen, first_ts.to_rfc3339());
    Ok(())
}

#[test]
fn observed_batch_write_rolls_back_when_activity_upsert_fails() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    store.ensure_observed_swap_writer_tables()?;
    drop(store);

    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "CREATE TRIGGER fail_wallet_activity_days_insert
         BEFORE INSERT ON wallet_activity_days
         BEGIN
             SELECT RAISE(ABORT, 'wallet activity blocked');
         END;",
    )?;
    drop(conn);

    let store = SqliteDiscoveryStore::open(&db_path)?;
    let now = ts("2026-05-05T10:00:00Z")?;
    let err = store
        .insert_observed_swaps_batch_with_activity_days(&[swap("sig-rollback", "wallet-a", now)])
        .expect_err("activity-day failure must abort the whole observed swap batch");
    assert!(err.to_string().contains("observed swap batch"));
    drop(store);

    let conn = Connection::open(&db_path)?;
    let observed_rows: i64 =
        conn.query_row("SELECT COUNT(*) FROM observed_swaps", [], |row| row.get(0))?;
    let activity_rows: i64 =
        conn.query_row("SELECT COUNT(*) FROM wallet_activity_days", [], |row| {
            row.get(0)
        })?;
    assert_eq!(observed_rows, 0);
    assert_eq!(activity_rows, 0);
    Ok(())
}
