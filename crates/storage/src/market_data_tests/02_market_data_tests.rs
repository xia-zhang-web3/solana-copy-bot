use super::*;

#[test]
fn recent_raw_journal_bulk_deadline_write_returns_bounded_on_expired_deadline() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("recent-raw-bulk-expired-deadline.db");
    let store = SqliteStore::open(Path::new(&db_path))?;
    let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let swaps = (0..512)
        .map(|idx| {
            swap(
                &format!("sig-recent-raw-bulk-deadline-{idx:04}"),
                "wallet-bulk-deadline",
                now + Duration::seconds(idx as i64),
                SOL_MINT,
                "TokenRecentRawBulkDeadline111111111111111",
                10_000 + idx as u64,
            )
        })
        .collect::<Vec<_>>();

    let (summary, time_budget_exhausted) = store
        .insert_recent_raw_journal_batch_bulk_with_deadline_internal(
            &swaps,
            now,
            Instant::now(),
            Some(16),
        )?;
    assert!(
            time_budget_exhausted,
            "expired deadline must return a bounded outcome instead of hanging in optimized recent_raw write path"
        );
    assert_eq!(summary.batch_rows, 0);
    assert_eq!(summary.recent_raw_bulk_statement_count, 0);
    assert!(summary.recent_raw_bulk_deadline_exhausted_before_statement);
    assert!(!summary.recent_raw_bulk_deadline_exhausted_during_execute);

    let cached_state = store.recent_raw_journal_state_cached()?;
    let scanned_state = store.recent_raw_journal_state()?;
    assert_eq!(cached_state, scanned_state);
    assert_eq!(summary.row_count, scanned_state.row_count);
    Ok(())
}

#[test]
fn recent_raw_journal_bulk_deadline_write_preserves_generic_sqlite_errors_as_hard_failures(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("recent-raw-bulk-hard-failure.db");
    let store = SqliteStore::open(Path::new(&db_path))?;
    store.ensure_recent_raw_journal_tables()?;
    store.conn.execute_batch(
        "CREATE TRIGGER recent_raw_bulk_hard_failure
             BEFORE INSERT ON observed_swaps
             BEGIN
                 SELECT RAISE(FAIL, 'synthetic non-timeout recent_raw bulk failure');
             END;",
    )?;
    let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let swaps = vec![swap(
        "sig-recent-raw-bulk-hard-failure",
        "wallet-bulk-hard-failure",
        now,
        SOL_MINT,
        "TokenRecentRawBulkHardFailure1111111111",
        30_000,
    )];

    let error = store
        .insert_recent_raw_journal_batch_bulk_with_deadline_internal(
            &swaps,
            now,
            Instant::now() + StdDuration::from_secs(5),
            Some(16),
        )
        .expect_err("non-timeout sqlite errors must remain hard failures");

    let error = error
        .chain()
        .map(|cause| cause.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        error.contains("failed to bulk insert observed swaps into recent raw journal batch"),
        "unexpected error: {error}"
    );
    assert!(
        error.contains("synthetic non-timeout recent_raw bulk failure"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn recent_raw_journal_cached_read_only_required_uses_cached_state_without_scanning_observed_swaps(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("recent-raw-cached-read-only.db");
    let store = SqliteStore::open(Path::new(&db_path))?;
    let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let swaps = vec![
        swap(
            "sig-recent-raw-cached-read-a",
            "wallet-cached-read",
            now - Duration::minutes(2),
            SOL_MINT,
            "TokenRecentRawCachedReadA11111111111111111111",
            301,
        ),
        swap(
            "sig-recent-raw-cached-read-b",
            "wallet-cached-read",
            now - Duration::minutes(1),
            SOL_MINT,
            "TokenRecentRawCachedReadB11111111111111111111",
            302,
        ),
    ];
    store.insert_recent_raw_journal_batch(&swaps, now)?;
    let expected_state = store.recent_raw_journal_state_cached()?;

    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute("DELETE FROM observed_swaps", [])?;

    let read_only = SqliteStore::open_read_only(Path::new(&db_path))?;
    let cached_state = read_only.recent_raw_journal_state_cached_read_only_required()?;
    assert_eq!(cached_state, expected_state);
    Ok(())
}

#[test]
fn recent_raw_journal_cached_read_only_required_errors_when_cached_state_row_is_missing(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("recent-raw-missing-cached-read-only.db");
    let store = SqliteStore::open(Path::new(&db_path))?;
    let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let swaps = vec![swap(
        "sig-recent-raw-missing-cached",
        "wallet-missing-cached",
        now - Duration::minutes(1),
        SOL_MINT,
        "TokenRecentRawMissingCached111111111111111111",
        401,
    )];
    store.insert_recent_raw_journal_batch(&swaps, now)?;

    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute("DELETE FROM recent_raw_journal_state WHERE id = 1", [])?;

    let read_only = SqliteStore::open_read_only(Path::new(&db_path))?;
    let error = read_only
        .recent_raw_journal_state_cached_read_only_required()
        .expect_err("missing cached state row must fail explicitly");
    assert!(
        error
            .to_string()
            .contains("cached recent raw journal state row id=1 is missing"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn recent_raw_journal_runtime_page_query_is_indexed_bounded_and_not_full_scan() {
    assert!(
        OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY
            .contains("INDEXED BY idx_observed_swaps_ts_slot_signature"),
        "{OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY}"
    );
    assert!(
        OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY.contains("ORDER BY ts ASC, slot ASC, signature ASC"),
        "{OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY}"
    );
    assert!(
        OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY.contains("LIMIT ?4"),
        "{OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY}"
    );
    let upper = OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY.to_ascii_uppercase();
    assert!(
        !upper.contains("COUNT("),
        "{OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY}"
    );
    assert!(
        !upper.contains("MIN("),
        "{OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY}"
    );
    assert!(
        !upper.contains("MAX("),
        "{OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY}"
    );
}

#[test]
fn recent_raw_journal_runtime_page_helper_preserves_cursor_order_and_page_boundary() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("recent-raw-runtime-page-helper.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;
    let base = DateTime::parse_from_rfc3339("2026-04-29T08:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let swaps = vec![
        swap(
            "sig-runtime-page-cursor",
            "wallet-runtime-page",
            base,
            SOL_MINT,
            "TokenRecentRawRuntimePage111111111111",
            10,
        ),
        swap(
            "sig-runtime-page-b",
            "wallet-runtime-page",
            base + Duration::seconds(1),
            SOL_MINT,
            "TokenRecentRawRuntimePage111111111111",
            12,
        ),
        swap(
            "sig-runtime-page-a",
            "wallet-runtime-page",
            base + Duration::seconds(1),
            SOL_MINT,
            "TokenRecentRawRuntimePage111111111111",
            12,
        ),
        swap(
            "sig-runtime-page-c",
            "wallet-runtime-page",
            base + Duration::seconds(2),
            SOL_MINT,
            "TokenRecentRawRuntimePage111111111111",
            13,
        ),
    ];
    store.insert_observed_swaps_batch_with_activity_days(&swaps)?;

    let cursor = DiscoveryRuntimeCursor {
        ts_utc: base,
        slot: 10,
        signature: "sig-runtime-page-cursor".to_string(),
    };
    let (page, time_budget_exhausted) = store.load_observed_swaps_after_cursor_page_read_only(
        &cursor,
        2,
        Instant::now() + StdDuration::from_secs(5),
    )?;

    assert!(!time_budget_exhausted);
    assert_eq!(
        page.iter()
            .map(|swap| swap.signature.as_str())
            .collect::<Vec<_>>(),
        vec!["sig-runtime-page-a", "sig-runtime-page-b"]
    );
    Ok(())
}

#[test]
fn observed_wallet_activity_page_uses_exact_day_counts_with_partial_first_day() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-activity-page-partial-day.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    store.insert_observed_swap(&swap(
        "sig-wallet-a-pre",
        "wallet-a",
        DateTime::parse_from_rfc3339("2026-03-06T08:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
        SOL_MINT,
        "TokenWalletA1111111111111111111111111111111",
        1,
    ))?;
    store.insert_observed_swap(&swap(
        "sig-wallet-a-in-window",
        "wallet-a",
        DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
        SOL_MINT,
        "TokenWalletA1111111111111111111111111111111",
        2,
    ))?;
    store.insert_observed_swap(&swap(
        "sig-wallet-a-next-day",
        "wallet-a",
        DateTime::parse_from_rfc3339("2026-03-07T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
        SOL_MINT,
        "TokenWalletA1111111111111111111111111111111",
        3,
    ))?;
    store.insert_observed_swap(&swap(
        "sig-wallet-b-in-window",
        "wallet-b",
        DateTime::parse_from_rfc3339("2026-03-06T13:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
        SOL_MINT,
        "TokenWalletB1111111111111111111111111111111",
        4,
    ))?;
    store.upsert_wallet_activity_days(&[
        WalletActivityDayRow {
            wallet_id: "wallet-a".to_string(),
            activity_day: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc)
                .date_naive(),
            last_seen: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        },
        WalletActivityDayRow {
            wallet_id: "wallet-a".to_string(),
            activity_day: DateTime::parse_from_rfc3339("2026-03-07T09:00:00Z")
                .expect("ts")
                .with_timezone(&Utc)
                .date_naive(),
            last_seen: DateTime::parse_from_rfc3339("2026-03-07T09:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        },
        WalletActivityDayRow {
            wallet_id: "wallet-b".to_string(),
            activity_day: DateTime::parse_from_rfc3339("2026-03-06T13:00:00Z")
                .expect("ts")
                .with_timezone(&Utc)
                .date_naive(),
            last_seen: DateTime::parse_from_rfc3339("2026-03-06T13:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        },
    ])?;

    let since = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let until = DateTime::parse_from_rfc3339("2026-03-07T23:59:59Z")
        .expect("ts")
        .with_timezone(&Utc);
    let page = store.observed_wallet_activity_page_in_window_with_budget(
        since,
        until,
        None,
        10,
        50,
        Instant::now() + StdDuration::from_secs(5),
    )?;
    assert!(!page.time_budget_exhausted);
    assert_eq!(page.rows_seen, 3);
    assert_eq!(
        page.active_day_count_source,
        Some(ObservedWalletActivityDayCountSource::WalletActivityDays)
    );
    let by_wallet: HashMap<String, ObservedWalletActivityRow> = page
        .rows
        .into_iter()
        .map(|row| (row.wallet_id.clone(), row))
        .collect();
    assert_eq!(by_wallet["wallet-a"].trades, 2);
    assert_eq!(by_wallet["wallet-a"].active_day_count, 2);
    assert_eq!(by_wallet["wallet-a"].first_seen, since + Duration::hours(2));
    assert_eq!(
        by_wallet["wallet-a"].last_seen,
        DateTime::parse_from_rfc3339("2026-03-07T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc)
    );
    assert_eq!(by_wallet["wallet-b"].trades, 1);
    assert_eq!(by_wallet["wallet-b"].active_day_count, 1);
    Ok(())
}

#[test]
fn observed_wallet_activity_page_falls_back_when_wallet_activity_days_is_incomplete() -> Result<()>
{
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-activity-page-fallback.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    store.insert_observed_swap(&swap(
        "sig-wallet-fallback-a-1",
        "wallet-fallback-a",
        DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
        SOL_MINT,
        "TokenFallbackA11111111111111111111111111111",
        1,
    ))?;
    store.insert_observed_swap(&swap(
        "sig-wallet-fallback-a-2",
        "wallet-fallback-a",
        DateTime::parse_from_rfc3339("2026-03-07T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
        SOL_MINT,
        "TokenFallbackA11111111111111111111111111111",
        2,
    ))?;
    store.insert_observed_swap(&swap(
        "sig-wallet-fallback-b-1",
        "wallet-fallback-b",
        DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
        SOL_MINT,
        "TokenFallbackB11111111111111111111111111111",
        3,
    ))?;

    let conn = Connection::open(&db_path)?;
    conn.execute(
        "DELETE FROM wallet_activity_days WHERE wallet_id = 'wallet-fallback-a'",
        [],
    )?;

    let page = store.observed_wallet_activity_page_in_window_with_budget(
        DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
        DateTime::parse_from_rfc3339("2026-03-07T23:59:59Z")
            .expect("ts")
            .with_timezone(&Utc),
        None,
        10,
        50,
        Instant::now() + StdDuration::from_secs(5),
    )?;
    assert!(!page.time_budget_exhausted);
    assert_eq!(
        page.active_day_count_source,
        Some(ObservedWalletActivityDayCountSource::ObservedSwapsFallback)
    );
    let by_wallet: HashMap<String, ObservedWalletActivityRow> = page
        .rows
        .into_iter()
        .map(|row| (row.wallet_id.clone(), row))
        .collect();
    assert_eq!(by_wallet["wallet-fallback-a"].active_day_count, 2);
    assert_eq!(by_wallet["wallet-fallback-b"].active_day_count, 1);
    Ok(())
}
