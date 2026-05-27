use super::*;

#[test]
fn recent_raw_journal_startup_prune_waits_for_live_write_then_prunes_idle_stage1() -> Result<()> {
    let _phase_guard = super::recent_raw_journal_phase_test_guard();
    let unique = format!(
        "copybot-app-recent-raw-journal-deferred-startup-prune-{}-{}",
        std::process::id(),
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(Utc::now().timestamp_micros() * 1000)
    );
    let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
    let journal_now = Utc::now();
    let stale_swap = SwapEvent {
        wallet: "wallet-journal-startup-prune-stale".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "token-journal-startup-prune-stale".to_string(),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: "sig-recent-raw-journal-startup-prune-stale".to_string(),
        slot: 450,
        ts_utc: journal_now - ChronoDuration::days(10),
        exact_amounts: None,
    };
    let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
    ensure_discovery_v2_schema(&journal_store)?;
    journal_store
        .insert_recent_raw_journal_batch(std::slice::from_ref(&stale_swap), stale_swap.ts_utc)?;
    journal_store.checkpoint_wal_truncate()?;
    drop(journal_store);

    let (journal_sender, journal_receiver) =
        std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(8);
    let (startup_sender, startup_receiver) = std_mpsc::channel::<std::result::Result<(), String>>();
    let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
    let config = ObservedSwapRecentRawJournalConfig {
        sqlite_path: journal_db_path
            .to_str()
            .context("journal sqlite path must be valid utf-8")?
            .to_string(),
        retention_days: 8,
        writer_queue_capacity_batches: 8,
        write_coalesce_max_batches:
            super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES,
        overflow_capacity_batches: 32,
        skip_prune_while_backlogged: true,
        skip_startup_prune: false,
    };
    let writer_handle = thread::spawn(move || {
        super::recent_raw_journal_writer_loop(journal_receiver, startup_sender, config, telemetry)
    });
    startup_receiver
        .recv_timeout(StdDuration::from_secs(2))
        .context("recent_raw journal writer did not signal startup before pruning")?
        .map_err(|error| anyhow!(error))?;

    let journal_store_after_startup = SqliteStore::open(Path::new(&journal_db_path))?;
    let rows_after_startup = journal_store_after_startup
        .load_observed_swaps_since(journal_now - ChronoDuration::days(30))?;
    assert_eq!(
            rows_after_startup.len(),
            1,
            "startup must only open/ensure journal tables and must not prune before the writer can receive live rows"
        );
    assert_eq!(
        rows_after_startup[0].signature,
        "sig-recent-raw-journal-startup-prune-stale"
    );
    drop(journal_store_after_startup);

    let fresh_swap = SwapEvent {
        wallet: "wallet-journal-startup-prune-fresh".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "token-journal-startup-prune-fresh".to_string(),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: "sig-recent-raw-journal-startup-prune-fresh".to_string(),
        slot: 451,
        ts_utc: journal_now - ChronoDuration::seconds(30),
        exact_amounts: None,
    };
    journal_sender.send(super::RecentRawJournalWriteRequest {
        inserted_swaps: vec![fresh_swap],
    })?;
    drop(journal_sender);

    let live_write_started = Instant::now();
    loop {
        let verify_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let rows =
            verify_store.load_observed_swaps_since(journal_now - ChronoDuration::days(30))?;
        if rows
            .iter()
            .any(|row| row.signature == "sig-recent-raw-journal-startup-prune-fresh")
        {
            break;
        }
        if live_write_started.elapsed() > StdDuration::from_secs(2) {
            anyhow::bail!(
                "recent_raw journal writer did not persist first live row after startup signal"
            );
        }
        std::thread::sleep(StdDuration::from_millis(10));
    }

    writer_handle
        .join()
        .map_err(|payload| anyhow!(super::panic_payload_to_string(payload.as_ref())))??;
    let journal_store_after_write = SqliteStore::open(Path::new(&journal_db_path))?;
    let rows_after_write = journal_store_after_write
        .load_observed_swaps_since(journal_now - ChronoDuration::days(30))?;
    assert_eq!(
        rows_after_write.len(),
        1,
        "idle recent_raw writer should prune stale rows after the first live write"
    );
    assert!(rows_after_write
        .iter()
        .any(|row| row.signature == "sig-recent-raw-journal-startup-prune-fresh"));
    let journal_state = journal_store_after_write.recent_raw_journal_state()?;
    assert_eq!(
        journal_state.row_count, 1,
        "journal state must reflect committed hot writer rows after stale rows are pruned"
    );
    assert!(
        journal_state.last_pruned_at.is_some(),
        "hot writer should mark retention prune after pruning stale rows"
    );

    remove_sqlite_test_files(&journal_db_path);
    Ok(())
}

#[test]
fn recent_raw_journal_writer_phase_telemetry_orders_write_and_prune_stage1() -> Result<()> {
    let _phase_guard = super::recent_raw_journal_phase_test_guard();
    super::clear_recent_raw_journal_phase_events_for_test();
    let unique = format!(
        "copybot-app-recent-raw-journal-phase-order-{}-{}",
        std::process::id(),
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(Utc::now().timestamp_micros() * 1000)
    );
    let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
    let prepare_store = SqliteStore::open(Path::new(&journal_db_path))?;
    ensure_discovery_v2_schema(&prepare_store)?;
    prepare_store.ensure_recent_raw_journal_tables()?;
    drop(prepare_store);
    let (journal_sender, journal_receiver) =
        std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(8);
    let (startup_sender, startup_receiver) = std_mpsc::channel::<std::result::Result<(), String>>();
    let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
    let config = ObservedSwapRecentRawJournalConfig {
        sqlite_path: journal_db_path
            .to_str()
            .context("journal sqlite path must be valid utf-8")?
            .to_string(),
        retention_days: 365,
        writer_queue_capacity_batches: 8,
        write_coalesce_max_batches: 4,
        overflow_capacity_batches: 8,
        skip_prune_while_backlogged: false,
        skip_startup_prune: true,
    };
    let writer_handle = thread::spawn(move || {
        super::recent_raw_journal_writer_loop(journal_receiver, startup_sender, config, telemetry)
    });
    startup_receiver
        .recv_timeout(StdDuration::from_secs(2))
        .context("recent_raw journal writer did not signal startup")?
        .map_err(|error| anyhow!(error))?;

    let scenario_now = Utc::now();
    journal_sender.send(recent_raw_journal_write_request_for_test(
        0,
        3,
        scenario_now,
    ))?;
    drop(journal_sender);
    writer_handle
        .join()
        .map_err(|payload| anyhow!(super::panic_payload_to_string(payload.as_ref())))??;

    let events = super::recent_raw_journal_phase_events_for_test();
    let expected = vec![
        super::RECENT_RAW_JOURNAL_PHASE_BATCH_COLLECTED,
        super::RECENT_RAW_JOURNAL_PHASE_WRITE_START,
        super::RECENT_RAW_JOURNAL_PHASE_WRITE_END,
        super::RECENT_RAW_JOURNAL_PHASE_PRUNE_CHECK_START,
        super::RECENT_RAW_JOURNAL_PHASE_PRUNE_CHECK_END,
        super::RECENT_RAW_JOURNAL_PHASE_PRUNE_START,
        super::RECENT_RAW_JOURNAL_PHASE_PRUNE_END,
        super::RECENT_RAW_JOURNAL_PHASE_BATCH_DONE,
    ];
    assert_eq!(
        events, expected,
        "recent_raw journal phase telemetry must expose exact write/prune order"
    );
    let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
    let journal_state = journal_store.recent_raw_journal_state()?;
    assert_eq!(
        journal_state.row_count, 3,
        "phase telemetry must not replace commit-only journal state advancement"
    );
    remove_sqlite_test_files(&journal_db_path);
    Ok(())
}

#[test]
fn recent_raw_journal_hot_writer_prunes_when_idle_with_skip_backlogged_enabled_stage1() -> Result<()>
{
    let _phase_guard = super::recent_raw_journal_phase_test_guard();
    super::clear_recent_raw_journal_phase_events_for_test();
    let unique = format!(
        "copybot-app-recent-raw-journal-phase-skip-prune-{}-{}",
        std::process::id(),
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(Utc::now().timestamp_micros() * 1000)
    );
    let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
    let prepare_store = SqliteStore::open(Path::new(&journal_db_path))?;
    ensure_discovery_v2_schema(&prepare_store)?;
    prepare_store.ensure_recent_raw_journal_tables()?;
    drop(prepare_store);
    let (journal_sender, journal_receiver) =
        std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(8);
    let (startup_sender, startup_receiver) = std_mpsc::channel::<std::result::Result<(), String>>();
    let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
    let config = ObservedSwapRecentRawJournalConfig {
        sqlite_path: journal_db_path
            .to_str()
            .context("journal sqlite path must be valid utf-8")?
            .to_string(),
        retention_days: 365,
        writer_queue_capacity_batches: 8,
        write_coalesce_max_batches: 4,
        overflow_capacity_batches: 8,
        skip_prune_while_backlogged: true,
        skip_startup_prune: true,
    };
    let writer_handle = thread::spawn(move || {
        super::recent_raw_journal_writer_loop(journal_receiver, startup_sender, config, telemetry)
    });
    startup_receiver
        .recv_timeout(StdDuration::from_secs(2))
        .context("recent_raw journal writer did not signal startup")?
        .map_err(|error| anyhow!(error))?;

    let scenario_now = DateTime::parse_from_rfc3339("2026-04-29T18:55:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    journal_sender.send(recent_raw_journal_write_request_for_test(
        0,
        3,
        scenario_now,
    ))?;
    drop(journal_sender);
    writer_handle
        .join()
        .map_err(|payload| anyhow!(super::panic_payload_to_string(payload.as_ref())))??;

    let events = super::recent_raw_journal_phase_events_for_test();
    assert!(
        events.contains(&super::RECENT_RAW_JOURNAL_PHASE_PRUNE_START),
        "idle hot writer should enter prune_start when no backlog is present: {events:?}"
    );
    assert!(
        events.contains(&super::RECENT_RAW_JOURNAL_PHASE_PRUNE_END),
        "idle hot writer should finish bounded prune when no backlog is present: {events:?}"
    );
    let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
    let journal_state = journal_store.recent_raw_journal_state()?;
    assert_eq!(journal_state.row_count, 3);
    remove_sqlite_test_files(&journal_db_path);
    Ok(())
}

#[test]
fn recent_raw_journal_write_deadline_exhaustion_fails_closed_without_unproven_state_stage1(
) -> Result<()> {
    let unique = format!(
        "copybot-app-recent-raw-journal-write-deadline-{}-{}",
        std::process::id(),
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(Utc::now().timestamp_micros() * 1000)
    );
    let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
    let store = SqliteStore::open(Path::new(&journal_db_path))?;
    ensure_discovery_v2_schema(&store)?;
    store.ensure_recent_raw_journal_tables()?;
    let completed_at = Utc::now();
    let swaps = (0..4usize)
        .map(|idx| SwapEvent {
            wallet: format!("wallet-journal-deadline-{idx}"),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("token-journal-deadline-{idx}"),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: format!("sig-recent-raw-journal-deadline-{idx}"),
            slot: 1_000 + idx as u64,
            ts_utc: completed_at + ChronoDuration::milliseconds(idx as i64),
            exact_amounts: None,
        })
        .collect::<Vec<_>>();
    let telemetry = ObservedSwapWriterTelemetry::default();

    let error = super::write_recent_raw_journal_batch_with_deadline_attempts(
        &telemetry,
        &swaps,
        || Instant::now(),
        |_suffix, _deadline| Ok((recent_raw_journal_write_summary_for_test(0, 0), true)),
    )
    .expect_err("expired recent_raw journal write deadline must fail closed");
    let error_message = format!("{error:#}");
    assert!(
        error_message.contains(super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE_EXHAUSTED),
        "deadline failure must expose stable fail-closed reason; error={error_message}"
    );

    let rows = store.load_observed_swaps_since(completed_at - ChronoDuration::seconds(1))?;
    assert_eq!(
        rows.len(),
        0,
        "rows not proven committed before deadline exhaustion must not appear in the journal"
    );
    let journal_state = store.recent_raw_journal_state_cached()?;
    assert_eq!(journal_state.row_count, 0);
    assert_eq!(journal_state.last_batch_rows, 0);
    assert!(journal_state.covered_through_cursor.is_none());
    std::thread::sleep(StdDuration::from_millis(50));
    let delayed_rows =
        store.load_observed_swaps_since(completed_at - ChronoDuration::seconds(1))?;
    let delayed_state = store.recent_raw_journal_state_cached()?;
    assert_eq!(
            delayed_rows.len(),
            0,
            "deadline path must not leave any detached writer that can commit rows after fail-closed return"
        );
    assert_eq!(
            delayed_state.row_count, 0,
            "deadline path must not leave any detached writer that can advance journal state after fail-closed return"
        );

    remove_sqlite_test_files(&journal_db_path);
    Ok(())
}
