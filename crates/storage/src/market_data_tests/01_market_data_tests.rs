use super::*;

#[test]
fn observed_swap_window_paged_reader_matches_unpaged_stream_results_stage1() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("observed-swap-window-paged-equivalence.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;
    let now = DateTime::parse_from_rfc3339("2026-04-07T18:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let since = now - Duration::hours(2);
    let until = now;
    let swaps = (0..257usize)
        .map(|idx| {
            swap(
                &format!("sig-window-paged-equivalence-{idx:04}"),
                &format!("wallet-window-{:03}", idx % 7),
                since + Duration::seconds(idx as i64),
                SOL_MINT,
                &format!("TokenWindowEquivalence{idx:04}"),
                40_000 + idx as u64,
            )
        })
        .collect::<Vec<_>>();
    store.insert_observed_swaps_batch_with_activity_days(&swaps)?;

    let mut unpaged_signatures = Vec::new();
    store.for_each_observed_swap_in_window(since, until, |swap| {
        unpaged_signatures.push(swap.signature);
        Ok(())
    })?;

    let mut paged_signatures = Vec::new();
    let summary = store.for_each_observed_swap_in_window_paged_with_budget(
        since,
        until,
        32,
        Instant::now() + StdDuration::from_secs(5),
        |swap| {
            paged_signatures.push(swap.signature);
            Ok(())
        },
    )?;

    assert_eq!(summary.rows_seen, unpaged_signatures.len());
    assert!(!summary.time_budget_exhausted);
    assert_eq!(paged_signatures, unpaged_signatures);
    Ok(())
}

#[test]
fn observed_swap_window_paged_reader_prevents_post_checkpoint_recurrence_stage1() -> Result<()> {
    let unpaged = run_checkpoint_recurrence_scenario(false)?;
    let paged = run_checkpoint_recurrence_scenario(true)?;

    assert!(
            unpaged.writes_before_reader >= 32,
            "clean checkpoint should permit an immediate post-start write baseline before the long reader begins: {unpaged:?}"
        );
    assert!(
        paged.writes_before_reader >= 32,
        "paged scenario should also establish the same clean post-checkpoint baseline: {paged:?}"
    );
    assert!(
            unpaged.max_backlog_frames >= paged.max_backlog_frames.saturating_mul(2),
            "the current single-statement reader should strand materially more WAL frames behind the oldest reader mark than the paged reader: unpaged={unpaged:?} paged={paged:?}"
        );
    assert!(
            unpaged.max_backlog_frames.saturating_sub(paged.max_backlog_frames) >= 5_000,
            "the long reader should create a materially larger checkpoint debt even when scheduler jitter makes raw write counts noisy: unpaged={unpaged:?} paged={paged:?}"
        );
    assert!(
            unpaged.writes_during_reader > 0 && paged.writes_during_reader > 0,
            "both scenarios should continue writing after the clean checkpoint baseline so the recurrence is exercised under active writer load: unpaged={unpaged:?} paged={paged:?}"
        );
    Ok(())
}

#[test]
fn observed_swap_after_cursor_chunked_reader_prevents_post_checkpoint_recurrence_stage1(
) -> Result<()> {
    let legacy = run_cursor_checkpoint_recurrence_scenario(
        CursorCheckpointRecurrenceReader::LegacyAfterCursorSingleStatement,
    )?;
    let chunked = run_cursor_checkpoint_recurrence_scenario(
        CursorCheckpointRecurrenceReader::ChunkedAfterCursor,
    )?;

    assert!(
            legacy.writes_before_reader >= 32 && chunked.writes_before_reader >= 32,
            "both cursor scenarios should establish the same clean post-checkpoint write baseline before the long reader begins: legacy={legacy:?} chunked={chunked:?}"
        );
    assert!(
            legacy.max_backlog_frames
                >= chunked
                    .max_backlog_frames
                    .saturating_mul(15)
                    .saturating_add(9)
                    / 10,
            "the legacy single-statement after-cursor reader should strand materially more WAL frames than the chunked production reader: legacy={legacy:?} chunked={chunked:?}"
        );
    assert!(
            legacy.max_backlog_frames.saturating_sub(chunked.max_backlog_frames) >= 250_000,
            "the chunked after-cursor reader should materially reduce checkpoint debt on the same active-writer workload: legacy={legacy:?} chunked={chunked:?}"
        );
    assert!(
            legacy.writes_during_reader > 0 && chunked.writes_during_reader > 0,
            "both cursor scenarios should continue writing after the clean checkpoint baseline so the recurrence is exercised under active writer load: legacy={legacy:?} chunked={chunked:?}"
        );
    Ok(())
}

#[test]
fn observed_sol_leg_cursor_chunked_reader_prevents_post_checkpoint_recurrence_stage1() -> Result<()>
{
    let legacy = run_cursor_checkpoint_recurrence_scenario(
        CursorCheckpointRecurrenceReader::LegacySolLegSingleStatement,
    )?;
    let chunked =
        run_cursor_checkpoint_recurrence_scenario(CursorCheckpointRecurrenceReader::ChunkedSolLeg)?;

    assert!(
            legacy.writes_before_reader >= 32 && chunked.writes_before_reader >= 32,
            "both SOL-leg cursor scenarios should establish the same clean post-checkpoint write baseline before the long reader begins: legacy={legacy:?} chunked={chunked:?}"
        );
    assert!(
            legacy.max_backlog_frames
                >= chunked
                    .max_backlog_frames
                    .saturating_mul(15)
                    .saturating_add(9)
                    / 10,
            "the legacy single-statement SOL-leg cursor reader should strand materially more WAL frames than the chunked production reader: legacy={legacy:?} chunked={chunked:?}"
        );
    assert!(
            legacy.max_backlog_frames.saturating_sub(chunked.max_backlog_frames) >= 250_000,
            "the chunked SOL-leg reader should materially reduce checkpoint debt on the same active-writer workload: legacy={legacy:?} chunked={chunked:?}"
        );
    assert!(
            legacy.writes_during_reader > 0 && chunked.writes_during_reader > 0,
            "both SOL-leg scenarios should continue writing after the clean checkpoint baseline so the recurrence is exercised under active writer load: legacy={legacy:?} chunked={chunked:?}"
        );
    Ok(())
}

#[test]
fn observed_swap_window_after_cursor_chunked_reader_preserves_resume_order_stage1() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("observed-swap-window-after-cursor-resume-order.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let base = DateTime::parse_from_rfc3339("2026-04-09T12:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    for (idx, signature) in ["sig-a", "sig-b", "sig-c", "sig-d"].into_iter().enumerate() {
        assert!(store.insert_observed_swap(&SwapEvent {
            signature: signature.to_string(),
            wallet: format!("wallet-window-cursor-{idx:02}"),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: format!("TokenWindowCursor{idx:02}"),
            amount_in: 1.0,
            amount_out: 10.0 + idx as f64,
            slot: 10 + idx as u64,
            ts_utc: base + Duration::seconds(idx as i64),
            exact_amounts: None,
        })?);
    }

    let mut first_page = Vec::new();
    let first = store.for_each_observed_swap_in_window_after_cursor_with_budget(
        base,
        base + Duration::seconds(10),
        None,
        2,
        Instant::now() + StdDuration::from_secs(1),
        |swap| {
            first_page.push(swap.signature);
            Ok(())
        },
    )?;
    assert_eq!(first.rows_seen, 2);
    assert!(!first.time_budget_exhausted);
    assert_eq!(first_page, vec!["sig-a".to_string(), "sig-b".to_string()]);

    let cursor = DiscoveryRuntimeCursor {
        ts_utc: base + Duration::seconds(1),
        slot: 11,
        signature: "sig-b".to_string(),
    };
    let mut second_page = Vec::new();
    let second = store.for_each_observed_swap_in_window_after_cursor_with_budget(
        base,
        base + Duration::seconds(10),
        Some(&cursor),
        2,
        Instant::now() + StdDuration::from_secs(1),
        |swap| {
            second_page.push(swap.signature);
            Ok(())
        },
    )?;
    assert_eq!(second.rows_seen, 2);
    assert!(!second.time_budget_exhausted);
    assert_eq!(second_page, vec!["sig-c".to_string(), "sig-d".to_string()]);
    Ok(())
}

#[test]
fn recent_raw_journal_effective_bulk_insert_chunk_rows_honors_sqlite_variable_limit() {
    assert_eq!(
        recent_raw_journal_effective_bulk_insert_chunk_rows(None, 999),
        76
    );
    assert_eq!(
        recent_raw_journal_effective_bulk_insert_chunk_rows(Some(512), 999),
        76
    );
}

#[test]
fn recent_raw_journal_effective_bulk_insert_chunk_rows_uses_hard_cap_for_high_limit() {
    assert_eq!(
        recent_raw_journal_effective_bulk_insert_chunk_rows(None, 32_766),
        RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS
    );
    assert_eq!(
        recent_raw_journal_effective_bulk_insert_chunk_rows(Some(4096), 32_766),
        RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS
    );
}

#[test]
fn recent_raw_journal_effective_bulk_insert_chunk_rows_preserves_forced_small_test_chunk() {
    assert_eq!(
        recent_raw_journal_effective_bulk_insert_chunk_rows(Some(16), 32_766),
        16
    );
    assert_eq!(
        recent_raw_journal_effective_bulk_insert_chunk_rows(Some(0), 32_766),
        1
    );
}

#[test]
fn recent_raw_journal_batch_write_keeps_cached_state_exact() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("recent-raw-cached-state.db");
    let store = SqliteStore::open(Path::new(&db_path))?;
    let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let first_batch = vec![
        swap(
            "sig-recent-raw-state-a",
            "wallet-a",
            now - Duration::minutes(2),
            SOL_MINT,
            "TokenRecentRawStateA111111111111111111111111",
            100,
        ),
        swap(
            "sig-recent-raw-state-b",
            "wallet-a",
            now - Duration::minutes(1),
            SOL_MINT,
            "TokenRecentRawStateB111111111111111111111111",
            101,
        ),
    ];
    let first_summary = store.insert_recent_raw_journal_batch(&first_batch, now)?;
    assert_eq!(first_summary.batch_rows, 2);
    assert_eq!(first_summary.inserted_rows, 2);

    let second_batch = vec![
        first_batch[1].clone(),
        swap(
            "sig-recent-raw-state-c",
            "wallet-a",
            now,
            SOL_MINT,
            "TokenRecentRawStateC111111111111111111111111",
            102,
        ),
    ];
    let second_summary = store.insert_recent_raw_journal_batch(&second_batch, now)?;
    assert_eq!(second_summary.batch_rows, 2);
    assert_eq!(second_summary.inserted_rows, 1);

    let cached_state = store.recent_raw_journal_state_cached()?;
    let scanned_state = store.recent_raw_journal_state()?;
    assert_eq!(cached_state, scanned_state);
    assert_eq!(second_summary.row_count, scanned_state.row_count);
    assert_eq!(
        second_summary.covered_through_cursor,
        scanned_state.covered_through_cursor
    );
    Ok(())
}

#[test]
fn recent_raw_journal_batch_write_with_deadline_returns_bounded_outcome() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("recent-raw-bounded-deadline.db");
    let store = SqliteStore::open(Path::new(&db_path))?;
    let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let swaps = (0..512)
        .map(|idx| {
            swap(
                &format!("sig-recent-raw-deadline-{idx:04}"),
                "wallet-deadline",
                now + Duration::seconds(idx as i64),
                SOL_MINT,
                "TokenRecentRawDeadline111111111111111111111",
                1_000 + idx as u64,
            )
        })
        .collect::<Vec<_>>();

    let (summary, time_budget_exhausted) =
        store.insert_recent_raw_journal_batch_with_deadline(&swaps, now, Instant::now())?;
    assert!(
        time_budget_exhausted,
        "expired deadline must return a bounded outcome instead of hanging in sqlite write path"
    );
    assert_eq!(summary.batch_rows, 0);

    let cached_state = store.recent_raw_journal_state_cached()?;
    let scanned_state = store.recent_raw_journal_state()?;
    assert_eq!(cached_state, scanned_state);
    assert_eq!(summary.row_count, scanned_state.row_count);
    Ok(())
}

#[test]
fn recent_raw_journal_bulk_deadline_write_keeps_cached_state_exact_and_ignores_duplicates(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("recent-raw-bulk-cached-state.db");
    let store = SqliteStore::open(Path::new(&db_path))?;
    let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let existing = swap(
        "sig-recent-raw-bulk-existing",
        "wallet-bulk",
        now,
        SOL_MINT,
        "TokenRecentRawBulkExisting111111111111111111",
        1_000,
    );
    store.insert_recent_raw_journal_batch(std::slice::from_ref(&existing), now)?;

    let mut swaps = vec![existing];
    for idx in 0..130 {
        swaps.push(swap(
            &format!("sig-recent-raw-bulk-{idx:04}"),
            "wallet-bulk",
            now + Duration::seconds(idx as i64 + 1),
            SOL_MINT,
            "TokenRecentRawBulk11111111111111111111111",
            1_001 + idx as u64,
        ));
    }

    let (summary, time_budget_exhausted) = store
        .insert_recent_raw_journal_batch_bulk_with_deadline_internal(
            &swaps,
            now,
            Instant::now() + StdDuration::from_secs(5),
            Some(16),
        )?;
    assert!(!time_budget_exhausted);
    assert_eq!(summary.batch_rows, swaps.len());
    assert_eq!(summary.inserted_rows, swaps.len() - 1);
    assert_eq!(summary.recent_raw_bulk_effective_statement_chunk_rows, 16);
    assert_eq!(summary.recent_raw_bulk_statement_count, 9);
    assert_eq!(summary.recent_raw_bulk_rows_processed, swaps.len());
    assert_eq!(summary.recent_raw_bulk_rows_inserted, swaps.len() - 1);
    assert!(!summary.recent_raw_bulk_deadline_exhausted_before_statement);
    assert!(!summary.recent_raw_bulk_deadline_exhausted_during_execute);

    let cached_state = store.recent_raw_journal_state_cached()?;
    let scanned_state = store.recent_raw_journal_state()?;
    assert_eq!(cached_state, scanned_state);
    assert_eq!(summary.row_count, scanned_state.row_count);
    assert_eq!(
        summary.covered_through_cursor,
        scanned_state.covered_through_cursor
    );
    assert_eq!(scanned_state.row_count, 131);
    assert_eq!(
        scanned_state
            .covered_through_cursor
            .as_ref()
            .map(|cursor| cursor.signature.as_str()),
        Some("sig-recent-raw-bulk-0129")
    );
    Ok(())
}

#[test]
fn recent_raw_journal_bulk_default_path_uses_adaptive_effective_chunk_size() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("recent-raw-bulk-adaptive-default.db");
    let store = SqliteStore::open(Path::new(&db_path))?;
    let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let expected_chunk_rows = recent_raw_journal_effective_bulk_insert_chunk_rows(
        None,
        recent_raw_journal_sqlite_variable_limit(&store.conn),
    );
    assert!(
            expected_chunk_rows > 64,
            "test SQLite variable limit should permit exercising a chunk larger than the old 64-row ceiling; got {expected_chunk_rows}"
        );
    let swaps = (0..(expected_chunk_rows + 8))
        .map(|idx| {
            swap(
                &format!("sig-recent-raw-bulk-adaptive-{idx:04}"),
                "wallet-bulk-adaptive",
                now + Duration::seconds(idx as i64),
                SOL_MINT,
                "TokenRecentRawBulkAdaptive111111111111",
                20_000 + idx as u64,
            )
        })
        .collect::<Vec<_>>();
    let _guard =
        super::install_recent_raw_bulk_write_budget_hook(move |processed_rows, inserted_rows| {
            processed_rows >= expected_chunk_rows && inserted_rows >= expected_chunk_rows
        });

    let (summary, time_budget_exhausted) = store
        .insert_recent_raw_journal_batch_bulk_with_deadline(
            &swaps,
            now,
            Instant::now() + StdDuration::from_secs(5),
        )?;

    assert!(time_budget_exhausted);
    assert_eq!(summary.batch_rows, expected_chunk_rows);
    assert_eq!(summary.inserted_rows, expected_chunk_rows);
    assert_eq!(
        summary.recent_raw_bulk_effective_statement_chunk_rows,
        expected_chunk_rows
    );
    assert_eq!(summary.recent_raw_bulk_statement_count, 1);
    assert_eq!(
        summary.recent_raw_bulk_statement_params_per_row,
        RECENT_RAW_JOURNAL_BULK_INSERT_PARAMS_PER_ROW
    );
    assert_eq!(
        summary.recent_raw_bulk_statement_chunk_row_cap,
        RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS
    );
    assert_eq!(summary.recent_raw_bulk_rows_processed, expected_chunk_rows);
    assert_eq!(summary.recent_raw_bulk_rows_inserted, expected_chunk_rows);
    assert!(summary.recent_raw_bulk_sqlite_variable_limit >= expected_chunk_rows * 13);
    let cached_state = store.recent_raw_journal_state_cached()?;
    let scanned_state = store.recent_raw_journal_state()?;
    assert_eq!(cached_state, scanned_state);
    assert_eq!(summary.row_count, scanned_state.row_count);
    Ok(())
}

#[test]
fn recent_raw_journal_bulk_deadline_write_preserves_partial_chunks_for_budget_retry() -> Result<()>
{
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("recent-raw-bulk-partial-budget.db");
    let store = SqliteStore::open(Path::new(&db_path))?;
    let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let swaps = (0..130)
        .map(|idx| {
            swap(
                &format!("sig-recent-raw-bulk-partial-{idx:04}"),
                "wallet-bulk-partial",
                now + Duration::seconds(idx as i64),
                SOL_MINT,
                "TokenRecentRawBulkPartial111111111111111",
                5_000 + idx as u64,
            )
        })
        .collect::<Vec<_>>();
    let _guard =
        super::install_recent_raw_bulk_write_budget_hook(|processed_rows, inserted_rows| {
            processed_rows >= 16 && inserted_rows >= 16
        });

    let (summary, time_budget_exhausted) = store
        .insert_recent_raw_journal_batch_bulk_with_deadline_internal(
            &swaps,
            now,
            Instant::now() + StdDuration::from_secs(5),
            Some(16),
        )?;
    assert!(time_budget_exhausted);
    assert_eq!(summary.batch_rows, 16);
    assert_eq!(summary.inserted_rows, 16);
    assert_eq!(summary.recent_raw_bulk_effective_statement_chunk_rows, 16);
    assert_eq!(summary.recent_raw_bulk_statement_count, 1);
    assert_eq!(summary.recent_raw_bulk_rows_processed, 16);
    assert_eq!(summary.recent_raw_bulk_rows_inserted, 16);
    assert!(!summary.recent_raw_bulk_deadline_exhausted_before_statement);
    assert!(!summary.recent_raw_bulk_deadline_exhausted_during_execute);

    let cached_state = store.recent_raw_journal_state_cached()?;
    let scanned_state = store.recent_raw_journal_state()?;
    assert_eq!(cached_state, scanned_state);
    assert_eq!(summary.row_count, 16);
    assert_eq!(summary.row_count, scanned_state.row_count);
    assert_eq!(
        scanned_state
            .covered_through_cursor
            .as_ref()
            .map(|cursor| cursor.signature.as_str()),
        Some("sig-recent-raw-bulk-partial-0015")
    );
    Ok(())
}
