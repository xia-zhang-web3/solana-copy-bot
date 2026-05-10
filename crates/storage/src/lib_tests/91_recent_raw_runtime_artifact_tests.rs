use super::*;
use chrono::Duration;
use tempfile::tempdir;

#[test]
fn recent_raw_journal_batch_persists_rows_and_updates_state() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let journal_db_path = temp.path().join("recent-raw-journal.db");
    let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
    let now = DateTime::parse_from_rfc3339("2026-03-24T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let swaps = vec![
        recent_raw_journal_swap(
            "journal-sig-a",
            "wallet-a",
            "So11111111111111111111111111111111111111112",
            "token-a",
            1.0,
            10.0,
            100,
            now - Duration::hours(3),
        ),
        recent_raw_journal_swap(
            "journal-sig-b",
            "wallet-b",
            "So11111111111111111111111111111111111111112",
            "token-b",
            1.2,
            12.0,
            101,
            now - Duration::hours(1),
        ),
    ];

    let summary = journal_store.insert_recent_raw_journal_batch(&swaps, now)?;
    assert_eq!(summary.batch_rows, 2);
    assert_eq!(summary.inserted_rows, 2);
    assert_eq!(summary.row_count, 2);
    assert_eq!(summary.last_batch_completed_at, Some(now));
    let state = journal_store.recent_raw_journal_state()?;
    assert_eq!(state.row_count, 2);
    assert_eq!(state.last_batch_rows, 2);
    assert_eq!(state.last_batch_completed_at, Some(now));
    let persisted = journal_store.load_observed_swaps_since(now - Duration::days(1))?;
    assert_eq!(persisted.len(), 2);
    Ok(())
}

#[test]
fn recent_raw_journal_retention_prunes_old_rows_but_keeps_required_horizon() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let journal_db_path = temp.path().join("recent-raw-journal-prune.db");
    let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
    let now = DateTime::parse_from_rfc3339("2026-03-24T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let old_swap = recent_raw_journal_swap(
        "journal-old-sig",
        "wallet-old",
        "So11111111111111111111111111111111111111112",
        "token-old",
        1.0,
        9.0,
        90,
        now - Duration::days(10),
    );
    let fresh_swap = recent_raw_journal_swap(
        "journal-fresh-sig",
        "wallet-fresh",
        "So11111111111111111111111111111111111111112",
        "token-fresh",
        1.0,
        11.0,
        91,
        now - Duration::days(6),
    );
    journal_store.insert_recent_raw_journal_batch(&[old_swap, fresh_swap], now)?;

    let deleted =
        journal_store.prune_recent_raw_journal_before_batch(now - Duration::days(7), 100, now)?;
    assert_eq!(deleted, 1);

    let state = journal_store.recent_raw_journal_state()?;
    assert_eq!(state.row_count, 1);
    assert_eq!(state.last_pruned_rows, 1);
    assert_eq!(state.last_pruned_at, Some(now));
    let persisted = journal_store.load_observed_swaps_since(now - Duration::days(30))?;
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].signature, "journal-fresh-sig");
    Ok(())
}

#[test]
fn recent_raw_journal_state_and_prune_reject_hidden_non_canonical_timestamp() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let journal_db_path = temp.path().join("recent-raw-journal-hidden-bad-ts.db");
    let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
    let now = DateTime::parse_from_rfc3339("2026-03-24T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let valid_swap = recent_raw_journal_swap(
        "journal-valid-sig",
        "wallet-valid",
        "So11111111111111111111111111111111111111112",
        "token-valid",
        1.0,
        9.0,
        90,
        now - Duration::hours(1),
    );
    journal_store.insert_recent_raw_journal_batch(&[valid_swap], now)?;
    journal_store.conn.execute(
        "INSERT INTO observed_swaps(signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts)
         VALUES ('journal-hidden-bad-ts', 'wallet-bad', 'dex', 'token-a', 'token-b', 1.0, 1.0, 1, '2026-03-24T00:00:00Z')",
        [],
    )?;

    let state_error = journal_store
        .recent_raw_journal_state()
        .expect_err("state scan must fail closed on hidden non-canonical timestamp");
    assert!(format!("{state_error:#}").contains("observed_swaps.ts is not canonical UTC"));
    let prune_error = journal_store
        .prune_recent_raw_journal_before_batch(now - Duration::days(1), 100, now)
        .expect_err("prune must fail closed before deleting hidden bad timestamp rows");
    assert!(format!("{prune_error:#}").contains("observed_swaps.ts is not canonical UTC"));
    let row_count: i64 =
        journal_store
            .conn
            .query_row("SELECT COUNT(*) FROM observed_swaps", [], |row| row.get(0))?;
    assert_eq!(row_count, 2);
    Ok(())
}

#[test]
fn recent_raw_journal_recent_load_rejects_hidden_non_canonical_timestamp() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let journal_db_path = temp.path().join("recent-raw-journal-load-hidden-bad-ts.db");
    let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
    let now = DateTime::parse_from_rfc3339("2026-03-24T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    journal_store.insert_recent_raw_journal_batch(
        &[
            recent_raw_journal_swap(
                "journal-load-valid-new",
                "wallet-valid-new",
                "So11111111111111111111111111111111111111112",
                "token-new",
                1.0,
                10.0,
                101,
                now - Duration::minutes(1),
            ),
            recent_raw_journal_swap(
                "journal-load-valid-old",
                "wallet-valid-old",
                "So11111111111111111111111111111111111111112",
                "token-old",
                1.0,
                9.0,
                100,
                now - Duration::minutes(2),
            ),
        ],
        now,
    )?;
    journal_store.conn.execute(
        "INSERT INTO observed_swaps(signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts)
         VALUES ('journal-load-hidden-bad-ts', 'wallet-bad', 'dex', 'token-a', 'token-b', 1.0, 1.0, 1, '2026-03-24T00:00:00Z')",
        [],
    )?;

    let error = journal_store
        .load_recent_observed_swaps_since(now - Duration::days(1), 1)
        .expect_err("recent load must fail closed before LIMIT can hide bad observed evidence");
    assert!(format!("{error:#}").contains("observed_swaps.ts is not canonical UTC"));
    Ok(())
}

#[test]
fn recent_raw_journal_state_rejects_missing_timestamp_guard_index_on_non_empty_table() -> Result<()>
{
    let temp = tempdir().context("failed to create tempdir")?;
    let journal_db_path = temp
        .path()
        .join("recent-raw-journal-missing-timestamp-index.db");
    let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
    let now = DateTime::parse_from_rfc3339("2026-03-24T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let valid_swap = recent_raw_journal_swap(
        "journal-valid-guard-index-sig",
        "wallet-valid",
        "So11111111111111111111111111111111111111112",
        "token-valid",
        1.0,
        9.0,
        90,
        now - Duration::hours(1),
    );
    journal_store.insert_recent_raw_journal_batch(&[valid_swap], now)?;
    journal_store
        .conn
        .execute_batch("DROP INDEX IF EXISTS idx_observed_swaps_non_utc_ts;")?;

    let state_error = journal_store
        .recent_raw_journal_state()
        .expect_err("state scan must require the timestamp guard index");
    assert!(format!("{state_error:#}").contains("run migration 0040 offline"));
    Ok(())
}

#[test]
fn recent_raw_journal_cached_state_rejects_partial_cursor_columns() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let journal_db_path = temp.path().join("recent-raw-journal-partial-cursor.db");
    let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
    journal_store.ensure_recent_raw_journal_tables()?;
    journal_store.conn.execute(
        "INSERT INTO recent_raw_journal_state(
            id, covered_since_ts, covered_through_cursor_ts, covered_through_cursor_slot,
            covered_through_cursor_signature, row_count, last_batch_rows,
            last_batch_completed_at, last_pruned_rows, last_pruned_at, updated_at
         ) VALUES (
            1, NULL, '2026-03-24T12:00:00+00:00', NULL, NULL, 1, 1, NULL, 0, NULL,
            '2026-03-24T12:00:01+00:00'
         )",
        [],
    )?;

    let error = journal_store
        .recent_raw_journal_state_cached()
        .expect_err("partial cached cursor columns must fail closed");
    assert!(format!("{error:#}").contains("cursor columns are partially populated"));
    Ok(())
}

#[test]
fn recent_raw_journal_cached_state_rejects_negative_counters() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let journal_db_path = temp.path().join("recent-raw-journal-negative-counters.db");
    let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
    journal_store.ensure_recent_raw_journal_tables()?;
    journal_store.conn.execute(
        "INSERT INTO recent_raw_journal_state(
            id, covered_since_ts, covered_through_cursor_ts, covered_through_cursor_slot,
            covered_through_cursor_signature, row_count, last_batch_rows,
            last_batch_completed_at, last_pruned_rows, last_pruned_at, updated_at
         ) VALUES (
            1, NULL, NULL, NULL, NULL, -1, 0, NULL, 0, NULL,
            '2026-03-24T12:00:01+00:00'
         )",
        [],
    )?;

    let error = journal_store
        .recent_raw_journal_state_cached()
        .expect_err("negative cached counters must fail closed");
    assert!(format!("{error:#}").contains("row_count must be non-negative"));
    Ok(())
}

#[test]
fn recent_raw_journal_replay_restores_required_window_without_full_history_reread() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let journal_db_path = temp.path().join("recent-raw-journal-replay.db");
    let runtime_db_path = temp.path().join("recent-raw-runtime.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
    let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
    runtime_store.run_migrations(&migration_dir)?;
    let now = DateTime::parse_from_rfc3339("2026-03-24T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let required_window_start = now - Duration::days(7);
    let artifact_runtime_cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::hours(2),
        slot: 120,
        signature: "artifact-runtime-cursor".to_string(),
    };
    journal_store.insert_recent_raw_journal_batch(
        &[
            recent_raw_journal_swap(
                "journal-replay-too-old",
                "wallet-old",
                "So11111111111111111111111111111111111111112",
                "token-old",
                1.0,
                8.0,
                110,
                now - Duration::days(9),
            ),
            recent_raw_journal_swap(
                "journal-replay-window-start",
                "wallet-window",
                "So11111111111111111111111111111111111111112",
                "token-window",
                1.0,
                9.0,
                111,
                required_window_start,
            ),
            recent_raw_journal_swap(
                "journal-replay-recent",
                "wallet-recent",
                "So11111111111111111111111111111111111111112",
                "token-recent",
                1.0,
                10.0,
                121,
                now - Duration::hours(1),
            ),
        ],
        now,
    )?;

    let replay = journal_store.replay_recent_raw_journal_into_runtime_store(
        &runtime_store,
        required_window_start,
        &artifact_runtime_cursor,
        2,
    )?;
    assert!(replay.journal_available);
    assert!(replay.journal_covers_artifact_cursor);
    assert!(replay.raw_coverage_satisfied);
    assert_eq!(replay.replayed_rows, 2);

    let restored = runtime_store.load_observed_swaps_since(now - Duration::days(30))?;
    let restored_signatures = restored
        .iter()
        .map(|swap| swap.signature.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        restored_signatures,
        vec!["journal-replay-window-start", "journal-replay-recent"]
    );
    Ok(())
}

fn recent_raw_journal_swap(
    signature: &str,
    wallet: &str,
    token_in: &str,
    token_out: &str,
    amount_in: f64,
    amount_out: f64,
    slot: u64,
    ts_utc: DateTime<Utc>,
) -> copybot_core_types::SwapEvent {
    copybot_core_types::SwapEvent {
        signature: signature.to_string(),
        wallet: wallet.to_string(),
        dex: "raydium".to_string(),
        token_in: token_in.to_string(),
        token_out: token_out.to_string(),
        amount_in,
        amount_out,
        exact_amounts: None,
        slot,
        ts_utc,
    }
}
