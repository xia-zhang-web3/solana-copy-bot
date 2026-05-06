    fn remove_sqlite_test_files(db_path: &Path) {
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
    }

    #[derive(Debug, Clone, Copy)]
    struct RecentRawJournalBackpressureSummary {
        baseline_rows_persisted: usize,
        pending_requests_after_load: usize,
        journal_queue_depth_after_load: usize,
        journal_overflow_depth_after_load: usize,
        max_pending_requests: usize,
        max_journal_queue_depth_batches: usize,
        max_journal_overflow_depth_batches: usize,
        persisted_rows_after_load: usize,
        runtime_wal_bytes_after_load: u64,
        sqlite_write_retry_delta: u64,
        sqlite_busy_error_delta: u64,
    }

    fn recent_raw_journal_backpressure_swap(idx: usize, ts: DateTime<Utc>) -> SwapEvent {
        SwapEvent {
            wallet: format!("wallet-journal-backpressure-{:03}", idx % 32),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("token-journal-backpressure-{:03}", idx % 64),
            amount_in: 1.0,
            amount_out: 10.0 + idx as f64,
            signature: format!("sig-journal-backpressure-{idx:05}"),
            slot: 10_000 + idx as u64,
            ts_utc: ts + ChronoDuration::milliseconds(idx as i64),
            exact_amounts: None,
        }
    }

    fn recent_raw_journal_write_request_for_test(
        start_idx: usize,
        rows: usize,
        ts: DateTime<Utc>,
    ) -> super::RecentRawJournalWriteRequest {
        super::RecentRawJournalWriteRequest {
            inserted_swaps: (start_idx..start_idx.saturating_add(rows))
                .map(|idx| recent_raw_journal_backpressure_swap(idx, ts))
                .collect(),
        }
    }

    fn recent_raw_journal_write_summary_for_test(
        processed_rows: usize,
        inserted_rows: usize,
    ) -> RecentRawJournalWriteSummary {
        RecentRawJournalWriteSummary {
            batch_rows: processed_rows,
            inserted_rows,
            recent_raw_bulk_rows_processed: processed_rows,
            recent_raw_bulk_rows_inserted: inserted_rows,
            ..RecentRawJournalWriteSummary::default()
        }
    }
