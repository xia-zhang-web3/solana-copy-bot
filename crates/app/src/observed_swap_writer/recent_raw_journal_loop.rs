#[cfg(test)]
pub(in crate::observed_swap_writer) fn clear_recent_raw_journal_phase_events_for_test() {
    if let Ok(mut events) = RECENT_RAW_JOURNAL_PHASE_EVENTS_FOR_TEST.lock() {
        events.clear();
    }
}

#[cfg(test)]
pub(in crate::observed_swap_writer) fn recent_raw_journal_phase_events_for_test(
) -> Vec<&'static str> {
    RECENT_RAW_JOURNAL_PHASE_EVENTS_FOR_TEST
        .lock()
        .map(|events| events.clone())
        .unwrap_or_default()
}

use super::*;

pub(in crate::observed_swap_writer) fn recent_raw_journal_writer_loop(
    receiver: std_mpsc::Receiver<RecentRawJournalWriteRequest>,
    startup_sender: std_mpsc::Sender<std::result::Result<(), String>>,
    config: ObservedSwapRecentRawJournalConfig,
    telemetry: Arc<ObservedSwapWriterTelemetry>,
) -> Result<()> {
    let journal_path = Path::new(&config.sqlite_path);
    if let Some(parent) = journal_path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed creating recent raw journal parent directory {}",
                parent.display()
            )
        })?;
    }
    let store = SqliteStore::open(journal_path).with_context(|| {
        format!(
            "failed to open sqlite db for recent raw journal writer: {}",
            journal_path.display()
        )
    })?;
    match store.ensure_recent_raw_journal_tables() {
        Ok(()) => {
            if !config.skip_startup_prune {
                debug!(
                    retention_days = config.retention_days,
                    "recent raw journal startup prune deferred until post-write bounded prune"
                );
            }
            let _ = startup_sender.send(Ok(()));
        }
        Err(error) => {
            let _ = startup_sender.send(Err(format!("{error:#}")));
            return Err(error);
        }
    }

    let adaptive_coalesce_max_batches =
        recent_raw_journal_adaptive_write_coalesce_max_batches(&config);
    let adaptive_coalesce_max_rows = recent_raw_journal_adaptive_write_coalesce_max_rows(&config);
    while let Ok(request) = receiver.recv() {
        let collected_batch = collect_recent_raw_journal_write_batch(
            &receiver,
            request,
            config.write_coalesce_max_batches,
            adaptive_coalesce_max_batches,
            adaptive_coalesce_max_rows,
            &telemetry,
        );
        let inserted_swaps = collected_batch.inserted_swaps;
        let journal_batch_started = Instant::now();
        let contention_before = sqlite_contention_snapshot();
        let completed_at = Utc::now();
        telemetry.note_journal_writer_inflight_started(inserted_swaps.len());
        log_recent_raw_journal_phase(
            RECENT_RAW_JOURNAL_PHASE_BATCH_COLLECTED,
            None,
            inserted_swaps.len(),
            collected_batch.request_batches,
            collected_batch.coalesce_elapsed_ms,
            collected_batch.coalesce_limit_rows,
            collected_batch.coalesce_elapsed_ms,
            &telemetry,
        );
        let write_result = (|| -> Result<()> {
            write_recent_raw_journal_batch_with_deadline(
                &store,
                &config,
                &telemetry,
                &inserted_swaps,
                collected_batch.request_batches,
                collected_batch.coalesce_elapsed_ms,
                collected_batch.coalesce_limit_rows,
                completed_at,
            )
        })();
        telemetry.note_journal_writer_inflight_finished();
        write_result?;
        match maybe_prune_recent_raw_journal(&store, &config, &telemetry, completed_at) {
            Ok(Some(deleted_rows)) if deleted_rows > 0 => {
                info!(
                    retention_days = config.retention_days,
                    deleted_rows, "recent raw journal retention prune applied"
                );
            }
            Ok(_) => {}
            Err(error) => {
                warn!(error = %error, "recent raw journal retention prune failed");
            }
        }
        let contention_after = sqlite_contention_snapshot();
        telemetry.note_journal_sqlite_contention_delta(contention_before, contention_after);
        telemetry
            .note_journal_batch_write_completed(elapsed_ms_ceil(journal_batch_started.elapsed()));
    }

    Ok(())
}
