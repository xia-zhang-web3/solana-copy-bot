use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use rusqlite::Connection;
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

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

fn prepare_observed_schema(store: &SqliteDiscoveryStore) -> Result<()> {
    ensure_discovery_v2_schema(store)?;
    store.ensure_observed_swap_writer_tables()
}

fn prepare_recent_raw_schema(store: &SqliteDiscoveryStore) -> Result<()> {
    ensure_discovery_v2_schema(store)?;
    store.ensure_recent_raw_journal_tables()
}

#[test]
fn observed_batch_write_persists_activity_days_atomically() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    prepare_observed_schema(&store)?;
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
    prepare_observed_schema(&store)?;
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

#[test]
fn observed_read_paths_reject_negative_slots() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    prepare_observed_schema(&store)?;
    drop(store);

    let conn = Connection::open(&db_path)?;
    conn.execute(
        "INSERT INTO observed_swaps(
            signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        (
            "sig-negative-slot",
            "wallet-a",
            "raydium",
            "SOL",
            "TOKEN",
            1.0f64,
            2.0f64,
            -1i64,
            "2026-05-05T10:00:00+00:00",
        ),
    )?;
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let tail_error = store
        .observed_swaps_tail_cursor_read_only()
        .expect_err("negative tail slot must fail closed");
    assert!(tail_error.to_string().contains("negative"));

    let row_error = store
        .load_recent_observed_swaps_since(ts("2026-05-05T09:00:00Z")?, 10)
        .expect_err("negative row slot must fail closed");
    assert!(row_error.to_string().contains("negative"));
    Ok(())
}

#[test]
fn observed_tail_rejects_non_utc_timestamp_offsets() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    prepare_observed_schema(&store)?;
    drop(store);

    let conn = Connection::open(&db_path)?;
    for (signature, ts) in [
        ("sig-canonical", "2026-05-05T10:00:00+00:00"),
        ("sig-non-utc", "2026-05-05T09:59:30-01:00"),
    ] {
        conn.execute(
            "INSERT INTO observed_swaps(
                signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
             ) VALUES (?1, 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 42, ?2)",
            (signature, ts),
        )?;
    }
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = store
        .observed_swaps_tail_cursor_read_only()
        .expect_err("non-UTC observed timestamp offset must fail closed");
    assert!(err.to_string().contains("not canonical UTC"));
    Ok(())
}

#[test]
fn observed_window_reads_reject_non_utc_timestamp_offsets() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    prepare_observed_schema(&store)?;
    drop(store);

    let conn = Connection::open(&db_path)?;
    for (signature, ts) in [
        ("sig-earliest", "2026-05-05T08:30:00+00:00"),
        ("sig-canonical", "2026-05-05T10:00:00+00:00"),
        ("sig-non-utc", "2026-05-05T09:30:00+01:00"),
    ] {
        conn.execute(
            "INSERT INTO observed_swaps(
                signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
             ) VALUES (?1, 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 42, ?2)",
            (signature, ts),
        )?;
    }
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = store
        .load_recent_observed_swaps_in_window(
            ts("2026-05-05T08:00:00Z")?,
            ts("2026-05-05T11:30:00Z")?,
            1,
        )
        .expect_err("non-UTC observed timestamp offset must fail closed in window scan");
    assert!(err.to_string().contains("not canonical UTC"));
    Ok(())
}

#[test]
fn observed_edge_reads_reject_hidden_non_utc_timestamp_offsets() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    prepare_recent_raw_schema(&store)?;
    drop(store);

    let conn = Connection::open(&db_path)?;
    for (signature, ts) in [
        ("sig-earliest", "2026-05-05T08:00:00+00:00"),
        ("sig-hidden-non-utc", "2026-05-05T09:30:00+01:00"),
        ("sig-latest", "2026-05-05T10:00:00+00:00"),
    ] {
        conn.execute(
            "INSERT INTO observed_swaps(
                signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
             ) VALUES (?1, 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 42, ?2)",
            (signature, ts),
        )?;
    }
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let tail_error = store
        .observed_swaps_tail_cursor_read_only()
        .expect_err("hidden non-UTC tail candidate must fail closed before LIMIT");
    assert!(tail_error.to_string().contains("not canonical UTC"));

    let coverage_error = store
        .observed_swaps_coverage_start_read_only()
        .expect_err("hidden non-UTC coverage row must fail closed before LIMIT");
    assert!(coverage_error.to_string().contains("not canonical UTC"));

    let recent_raw_error = store
        .recent_raw_journal_state_read_only()
        .expect_err("hidden non-UTC recent_raw coverage row must fail closed before LIMIT");
    assert!(recent_raw_error.to_string().contains("not canonical UTC"));
    Ok(())
}

#[test]
fn observed_edge_reads_reject_hidden_noncanonical_utc_timestamps() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    prepare_recent_raw_schema(&store)?;
    drop(store);

    let conn = Connection::open(&db_path)?;
    for (signature, ts) in [
        ("sig-earliest", "2026-05-05T08:00:00+00:00"),
        ("sig-hidden-z", "2026-05-05T09:00:00Z"),
        ("sig-hidden-malformed", "not-a-real-timestamp+00:00"),
        ("sig-latest", "2026-05-05T10:00:00+00:00"),
    ] {
        conn.execute(
            "INSERT INTO observed_swaps(
                signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
             ) VALUES (?1, 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 42, ?2)",
            (signature, ts),
        )?;
    }
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = store
        .observed_swaps_tail_cursor_read_only()
        .expect_err("hidden noncanonical UTC timestamp must fail closed before LIMIT");
    assert!(err.to_string().contains("not canonical UTC"));
    Ok(())
}

#[test]
fn observed_edge_reads_reject_hidden_invalid_calendar_timestamps() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    prepare_recent_raw_schema(&store)?;
    drop(store);

    let conn = Connection::open(&db_path)?;
    for (signature, ts) in [
        ("sig-earliest", "2026-02-28T08:00:00+00:00"),
        ("sig-hidden-invalid-date", "2026-02-31T09:00:00+00:00"),
        ("sig-latest", "2026-03-01T10:00:00+00:00"),
    ] {
        conn.execute(
            "INSERT INTO observed_swaps(
                signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
             ) VALUES (?1, 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 42, ?2)",
            (signature, ts),
        )?;
    }
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = store
        .observed_swaps_tail_cursor_read_only()
        .expect_err("hidden invalid calendar timestamp must fail closed before LIMIT");
    assert!(err.to_string().contains("not canonical UTC"));
    Ok(())
}

#[test]
fn observed_timestamp_guard_rejects_relaxed_chrono_timestamp_formats() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    prepare_recent_raw_schema(&store)?;
    drop(store);

    let conn = Connection::open(&db_path)?;
    for (signature, ts) in [
        ("sig-earliest", "2026-05-05T08:00:00+00:00"),
        ("sig-hidden-relaxed", "2026-05-05t09:00:00+00:00"),
        ("sig-latest", "2026-05-05T10:00:00+00:00"),
    ] {
        conn.execute(
            "INSERT INTO observed_swaps(
                signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
             ) VALUES (?1, 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 42, ?2)",
            (signature, ts),
        )?;
    }
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = store
        .observed_swaps_tail_cursor_read_only()
        .expect_err("hidden relaxed timestamp format must fail closed before chrono parsing");
    assert!(err.to_string().contains("not canonical UTC"));
    Ok(())
}

#[test]
fn recent_raw_scan_rejects_non_utc_timestamp_before_limit() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    prepare_recent_raw_schema(&store)?;
    drop(store);

    let conn = Connection::open(&db_path)?;
    for (signature, ts) in [
        ("sig-canonical", "2026-05-05T10:00:00+00:00"),
        ("sig-non-utc", "2026-05-05T09:30:00+01:00"),
    ] {
        conn.execute(
            "INSERT INTO observed_swaps(
                signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
             ) VALUES (?1, 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 42, ?2)",
            (signature, ts),
        )?;
    }
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = store
        .for_each_observed_swap_since_with_budget(
            ts("2026-05-05T08:00:00Z")?,
            1,
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            |_| Ok(()),
        )
        .expect_err("recent_raw scan must fail closed before LIMIT can hide non-UTC ts");
    assert!(err.to_string().contains("not canonical UTC"));
    Ok(())
}

#[test]
fn token_market_context_rejects_non_utc_timestamp_offsets() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    prepare_observed_schema(&store)?;
    drop(store);

    let token = "TokenMarketNonUtc111111111111111111111111111";
    let conn = Connection::open(&db_path)?;
    for (signature, ts) in [
        ("sig-market-canonical", "2026-05-05T11:00:00+00:00"),
        ("sig-market-non-utc", "2026-05-05T09:59:30-01:00"),
    ] {
        conn.execute(
            "INSERT INTO observed_swaps(
                signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
             ) VALUES (?1, 'wallet-a', 'raydium', ?2, ?3, 1.0, 2.0, 42, ?4)",
            (signature, SOL_MINT, token, ts),
        )?;
    }
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let as_of = ts("2026-05-05T11:20:00Z")?;
    let stats_error = store
        .token_market_stats(token, as_of)
        .expect_err("market stats must fail closed on non-UTC observed timestamp");
    assert!(stats_error.to_string().contains("not canonical UTC"));
    let latest_error = store
        .latest_token_sol_price(token, as_of)
        .expect_err("latest price must fail closed on non-UTC observed timestamp");
    assert!(latest_error.to_string().contains("not canonical UTC"));
    let reliable_error = store
        .reliable_token_sol_price_for_stale_close(token, as_of)
        .expect_err("reliable price must fail closed on non-UTC observed timestamp");
    assert!(reliable_error.to_string().contains("not canonical UTC"));
    Ok(())
}

#[test]
fn recent_raw_read_paths_reject_negative_slots() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    prepare_recent_raw_schema(&store)?;
    drop(store);

    let conn = Connection::open(&db_path)?;
    conn.execute(
        "INSERT INTO observed_swaps(
            signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
         ) VALUES ('sig-negative-recent-raw', 'wallet-a', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, -1, '2026-05-05T10:00:00+00:00')",
        [],
    )?;
    conn.execute(
        "INSERT INTO recent_raw_journal_state(
            id, covered_through_cursor_ts, covered_through_cursor_slot,
            covered_through_cursor_signature, updated_at
         ) VALUES (1, '2026-05-05T10:00:00+00:00', -1, 'sig-negative-recent-raw', '2026-05-05T10:00:00+00:00')",
        [],
    )?;
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let state_error = store
        .recent_raw_journal_state_read_only()
        .expect_err("negative recent_raw coverage slot must fail closed");
    assert!(state_error.to_string().contains("negative"));

    let cached_error = store
        .recent_raw_journal_state_cached_read_only_required()
        .expect_err("negative cached recent_raw cursor slot must fail closed");
    assert!(cached_error.to_string().contains("negative"));

    let scan_error = store
        .for_each_observed_swap_since_with_budget(
            ts("2026-05-05T09:00:00Z")?,
            10,
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            |_| Ok(()),
        )
        .expect_err("negative recent_raw scan slot must fail closed");
    assert!(scan_error.to_string().contains("negative"));
    Ok(())
}

#[test]
fn recent_raw_state_rejects_partial_cursor_columns() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let store = SqliteDiscoveryStore::open(&db_path)?;
    prepare_recent_raw_schema(&store)?;
    drop(store);

    let conn = Connection::open(&db_path)?;
    conn.execute(
        "INSERT INTO recent_raw_journal_state(
            id, covered_through_cursor_ts, updated_at
         ) VALUES (1, '2026-05-05T10:00:00+00:00', '2026-05-05T10:00:00+00:00')",
        [],
    )?;
    drop(conn);

    let store = SqliteDiscoveryStore::open_read_only(&db_path)?;
    let err = store
        .recent_raw_journal_state_read_only()
        .expect_err("partial recent_raw cursor must fail closed");
    assert!(err.to_string().contains("partially populated"));
    Ok(())
}
