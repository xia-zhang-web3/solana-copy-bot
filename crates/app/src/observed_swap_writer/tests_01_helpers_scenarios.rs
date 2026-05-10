use super::*;

fn seed_recent_raw_journal_prune_backlog(
    journal_store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<()> {
    let stale_rows = (0..16_384usize)
        .map(|idx| SwapEvent {
            wallet: format!("wallet-journal-stale-{:03}", idx % 16),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("token-journal-stale-{idx:04}"),
            amount_in: 1.0,
            amount_out: 5.0,
            signature: format!("sig-journal-stale-{idx:05}"),
            slot: 1_000 + idx as u64,
            ts_utc: now - ChronoDuration::days(14) + ChronoDuration::seconds(idx as i64),
            exact_amounts: None,
        })
        .collect::<Vec<_>>();
    journal_store.insert_recent_raw_journal_batch(&stale_rows, now - ChronoDuration::days(14))?;
    Ok(())
}

pub(super) fn run_recent_raw_journal_backpressure_scenario(
    skip_prune_while_backlogged: bool,
    write_coalesce_max_batches: usize,
    overflow_capacity_batches: usize,
) -> Result<RecentRawJournalBackpressureSummary> {
    let _contention_guard = sqlite_contention_delta_test_guard();
    let unique = format!(
        "copybot-app-recent-raw-journal-backpressure-{}-{}",
        std::process::id(),
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(Utc::now().timestamp_micros() * 1000)
    );
    let runtime_db_path = std::env::temp_dir().join(format!("{unique}.db"));
    let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
    let runtime_store = prepare_observed_writer_store_for_test(Path::new(&runtime_db_path))?;
    let journal_store = prepare_recent_raw_journal_store_for_test(Path::new(&journal_db_path))?;
    let scenario_now = DateTime::parse_from_rfc3339("2026-04-08T09:30:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    seed_recent_raw_journal_prune_backlog(&journal_store, scenario_now)?;
    journal_store.checkpoint_wal_truncate()?;
    drop(journal_store);

    let writer = ObservedSwapWriter::start_with_config(
        runtime_db_path
            .to_str()
            .context("runtime sqlite path must be valid utf-8")?
            .to_string(),
        ObservedSwapWriterConfig::for_test(
            512,
            1,
            Some(ObservedSwapRecentRawJournalConfig {
                sqlite_path: journal_db_path
                    .to_str()
                    .context("journal sqlite path must be valid utf-8")?
                    .to_string(),
                retention_days: 8,
                writer_queue_capacity_batches: 16,
                write_coalesce_max_batches,
                overflow_capacity_batches,
                skip_prune_while_backlogged,
                skip_startup_prune: true,
            }),
        ),
    )?;
    runtime_store.checkpoint_wal_truncate()?;

    let contention_before = sqlite_contention_snapshot();
    let runtime = Builder::new_current_thread().enable_all().build()?;
    for idx in 0..64usize {
        runtime.block_on(async {
            writer
                .enqueue(&recent_raw_journal_backpressure_swap(idx, scenario_now))
                .await
        })?;
        std::thread::sleep(StdDuration::from_millis(1));
    }

    let baseline_started = Instant::now();
    let baseline_rows_persisted = loop {
        let rows = runtime_store
            .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
            .len();
        if rows >= 32 {
            break rows;
        }
        if baseline_started.elapsed() > StdDuration::from_secs(5) {
            anyhow::bail!("writer failed to establish clean post-checkpoint throughput before recent_raw journal backpressure scenario");
        }
        std::thread::sleep(StdDuration::from_millis(10));
    };

    let max_pending_requests = Arc::new(AtomicUsize::new(0));
    let max_journal_queue_depth_batches = Arc::new(AtomicUsize::new(0));
    let max_journal_overflow_depth_batches = Arc::new(AtomicUsize::new(0));
    for idx in 64..4_096usize {
        runtime.block_on(async {
            writer
                .enqueue(&recent_raw_journal_backpressure_swap(idx, scenario_now))
                .await
        })?;
        if idx % 8 == 0 {
            std::thread::sleep(StdDuration::from_millis(1));
        }
        let snapshot = writer.snapshot();
        max_pending_requests.fetch_max(snapshot.pending_requests, Ordering::Relaxed);
        max_journal_queue_depth_batches
            .fetch_max(snapshot.journal_queue_depth_batches, Ordering::Relaxed);
        max_journal_overflow_depth_batches
            .fetch_max(snapshot.journal_overflow_depth_batches, Ordering::Relaxed);
    }
    let snapshot_after_load = writer.snapshot();

    let drain_started = Instant::now();
    while drain_started.elapsed() < StdDuration::from_millis(500) {
        let snapshot = writer.snapshot();
        max_pending_requests.fetch_max(snapshot.pending_requests, Ordering::Relaxed);
        max_journal_queue_depth_batches
            .fetch_max(snapshot.journal_queue_depth_batches, Ordering::Relaxed);
        max_journal_overflow_depth_batches
            .fetch_max(snapshot.journal_overflow_depth_batches, Ordering::Relaxed);
        std::thread::sleep(StdDuration::from_millis(10));
    }

    let persisted_rows_after_load = runtime_store
        .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
        .len();
    let runtime_wal_bytes_after_load =
        std::fs::metadata(format!("{}-wal", runtime_db_path.display()))
            .map(|metadata| metadata.len())
            .unwrap_or(0);
    writer.shutdown()?;
    let contention_after = sqlite_contention_snapshot();

    let _ = std::fs::remove_file(runtime_db_path);
    let _ = std::fs::remove_file(journal_db_path);
    Ok(RecentRawJournalBackpressureSummary {
        baseline_rows_persisted,
        pending_requests_after_load: snapshot_after_load.pending_requests,
        journal_queue_depth_after_load: snapshot_after_load.journal_queue_depth_batches,
        journal_overflow_depth_after_load: snapshot_after_load.journal_overflow_depth_batches,
        max_pending_requests: max_pending_requests.load(Ordering::Relaxed),
        max_journal_queue_depth_batches: max_journal_queue_depth_batches.load(Ordering::Relaxed),
        max_journal_overflow_depth_batches: max_journal_overflow_depth_batches
            .load(Ordering::Relaxed),
        persisted_rows_after_load,
        runtime_wal_bytes_after_load,
        sqlite_write_retry_delta: contention_after
            .write_retry_total
            .saturating_sub(contention_before.write_retry_total),
        sqlite_busy_error_delta: contention_after
            .busy_error_total
            .saturating_sub(contention_before.busy_error_total),
    })
}
