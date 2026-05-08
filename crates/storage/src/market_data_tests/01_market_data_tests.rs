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
