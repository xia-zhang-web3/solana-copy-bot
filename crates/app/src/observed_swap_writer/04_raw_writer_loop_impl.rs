fn observed_swap_writer_loop(
    sqlite_path: String,
    mut receiver: mpsc::Receiver<ObservedSwapWriteRequest>,
    journal_sender: Option<std_mpsc::SyncSender<RecentRawJournalWriteRequest>>,
    journal_startup_receiver: Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
    config: ObservedSwapWriterConfig,
    telemetry: Arc<ObservedSwapWriterTelemetry>,
    terminal_failure_message: Arc<Mutex<Option<String>>>,
) -> Result<()> {
    let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
        format!("failed to open sqlite db for observed swap writer: {sqlite_path}")
    })?;
    let journal_overflow_capacity_batches = config
        .recent_raw_journal
        .as_ref()
        .map(|journal| journal.overflow_capacity_batches)
        .unwrap_or(0);
    let journal_overflow_row_capacity = config
        .recent_raw_journal
        .as_ref()
        .map(|journal| recent_raw_journal_overflow_row_capacity(config.batch_max_size, journal))
        .unwrap_or(0);
    let journal_overflow_drain_coalesce_max_batches = config
        .recent_raw_journal
        .as_ref()
        .map(recent_raw_journal_adaptive_write_coalesce_max_batches)
        .unwrap_or(1);
    let mut journal_overflow = VecDeque::with_capacity(journal_overflow_capacity_batches);
    let mut journal_startup_receiver = journal_startup_receiver;

    loop {
        poll_observed_swap_writer_downstream_startups(&mut journal_startup_receiver)?;

        let first_request = match receiver.try_recv() {
            Ok(request) => request,
            Err(mpsc::error::TryRecvError::Empty) => {
                if let Some(journal_sender) = journal_sender.as_ref() {
                    if !journal_overflow.is_empty() && journal_startup_receiver.is_none() {
                        drain_recent_raw_journal_overflow_nonblocking(
                            journal_sender,
                            &mut journal_overflow,
                            journal_overflow_drain_coalesce_max_batches,
                            &telemetry,
                        )?;
                        if journal_overflow.is_empty() {
                            continue;
                        }
                        thread::sleep(OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_POLL);
                        continue;
                    }
                }
                if observed_swap_writer_downstream_startup_pending(&journal_startup_receiver) {
                    thread::sleep(OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_POLL);
                    continue;
                }
                match receiver.blocking_recv() {
                    Some(request) => request,
                    None => break,
                }
            }
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        };
        let batch = collect_observed_swap_write_batch(
            first_request,
            &mut receiver,
            config.batch_max_size,
        );
        let (swaps, replies, queued_at) = unpack_observed_swap_write_batch(batch);

        if let Some(message) = load_terminal_failure_message(&terminal_failure_message) {
            send_observed_swap_write_error_replies(replies, &message);
            telemetry.note_batch_completed(&queued_at);
            return Err(anyhow!(message))
                .context("observed swap writer stopping after downstream terminal failure");
        }

        let batch_started = Instant::now();
        let mut retryable_lock_started_at: Option<Instant> = None;
        let mut last_retryable_lock_log_at: Option<Instant> = None;
        let mut retryable_lock_attempts = 0u64;
        loop {
            if let Some(message) = load_terminal_failure_message(&terminal_failure_message) {
                send_observed_swap_write_error_replies(replies, &message);
                telemetry.note_batch_completed(&queued_at);
                return Err(anyhow!(message)).context(
                    "observed swap writer stopping after downstream terminal failure while raw batch was pending",
                );
            }

            let raw_batch_started = Instant::now();
            match store.insert_observed_swaps_batch_with_activity_days_measured(&swaps) {
                Ok(batch_metrics) => {
                    telemetry
                        .note_raw_batch_completed(elapsed_ms_ceil(raw_batch_started.elapsed()));
                    telemetry.note_observed_swaps_insert_completed(
                        batch_metrics.observed_swaps_insert_ms,
                    );
                    telemetry.note_wallet_activity_days_completed(
                        batch_metrics.wallet_activity_days_upsert_ms,
                    );
                    if let Some(retryable_lock_started_at) = retryable_lock_started_at {
                        info!(
                            batch_swaps = swaps.len(),
                            retryable_lock_attempts,
                            retryable_lock_elapsed_ms =
                                elapsed_ms_ceil(retryable_lock_started_at.elapsed()),
                            "observed swap writer recovered after retryable sqlite lock pressure"
                        );
                    }
                    let results = batch_metrics.inserted;
                    for (reply_tx, inserted) in replies.into_iter().zip(results.iter().copied()) {
                        if let Some(reply_tx) = reply_tx {
                            let _ = reply_tx.send(Ok(inserted));
                        }
                    }
                    telemetry.note_batch_completed(&queued_at);
                    let inserted_swaps: Vec<SwapEvent> = swaps
                        .iter()
                        .zip(results.iter())
                        .filter_map(|(swap, inserted)| inserted.then_some(swap.clone()))
                        .collect();
                    if let Some(journal_sender) = journal_sender.as_ref() {
                        if !inserted_swaps.is_empty() {
                            let journal_enqueue_started = Instant::now();
                            if let Err(error) = enqueue_recent_raw_journal_request(
                                journal_sender,
                                &mut journal_overflow,
                                journal_overflow_capacity_batches,
                                journal_overflow_row_capacity,
                                journal_overflow_drain_coalesce_max_batches,
                                RecentRawJournalWriteRequest { inserted_swaps },
                                &telemetry,
                            ) {
                                telemetry.note_journal_enqueue_wait_completed(elapsed_ms_ceil(
                                    journal_enqueue_started.elapsed(),
                                ));
                                telemetry.note_worker_busy_completed(elapsed_ms_ceil(
                                    batch_started.elapsed(),
                                ));
                                return Err(error).context(
                                    "observed swap writer stopping after recent raw journal enqueue failure",
                                );
                            }
                            telemetry.note_journal_enqueue_wait_completed(elapsed_ms_ceil(
                                journal_enqueue_started.elapsed(),
                            ));
                        }
                    }
                    telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
                    break;
                }
                Err(error) => {
                    telemetry
                        .note_raw_batch_completed(elapsed_ms_ceil(raw_batch_started.elapsed()));
                    if is_retryable_sqlite_anyhow_error(&error) {
                        retryable_lock_attempts = retryable_lock_attempts.saturating_add(1);
                        let retryable_lock_started_at =
                            *retryable_lock_started_at.get_or_insert_with(Instant::now);
                        let should_log = last_retryable_lock_log_at
                            .map(|logged_at| {
                                logged_at.elapsed()
                                    >= OBSERVED_SWAP_WRITER_RETRYABLE_LOCK_LOG_INTERVAL
                            })
                            .unwrap_or(true);
                        if should_log {
                            let contention = sqlite_contention_snapshot();
                            warn!(
                                error = %error,
                                batch_swaps = swaps.len(),
                                retryable_lock_attempts,
                                retryable_lock_elapsed_ms =
                                    elapsed_ms_ceil(retryable_lock_started_at.elapsed()),
                                sqlite_write_retry_total = contention.write_retry_total,
                                sqlite_busy_error_total = contention.busy_error_total,
                                "observed swap writer raw batch blocked by retryable sqlite lock; keeping writer alive and retrying"
                            );
                            last_retryable_lock_log_at = Some(Instant::now());
                        }
                        thread::sleep(OBSERVED_SWAP_WRITER_RETRYABLE_LOCK_BACKOFF);
                        continue;
                    }

                    telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
                    let message = format!("{error:#}");
                    warn!(
                        error = %error,
                        batch_swaps = swaps.len(),
                        "failed to insert observed swap batch with activity days"
                    );
                    send_observed_swap_write_error_replies(replies, &message);
                    telemetry.note_batch_completed(&queued_at);
                    return Err(anyhow!(message))
                        .context("observed swap writer stopping after raw batch insert failure");
                }
            }
        }
    }

    flush_observed_swap_writer_downstream_overflow_on_shutdown(
        journal_sender.as_ref(),
        &mut journal_overflow,
        &telemetry,
    )?;

    Ok(())
}
