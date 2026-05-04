impl ObservedSwapWriterTelemetry {
    fn note_enqueued(&self) {
        self.pending_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn note_batch_completed(&self, queued_at: &[Instant]) {
        if queued_at.is_empty() {
            return;
        }
        let now = Instant::now();
        if let Ok(mut samples) = self.write_latency_ms_samples.lock() {
            for queued_at in queued_at {
                let latency_ms = now
                    .duration_since(*queued_at)
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64;
                if samples.len() >= OBSERVED_SWAP_WRITER_LATENCY_SAMPLE_CAPACITY {
                    let _ = samples.pop_front();
                }
                samples.push_back(latency_ms);
            }
            self.last_write_latency_ms_p95
                .store(percentile_from_deque(&samples, 0.95), Ordering::Relaxed);
        }
        let _ =
            self.pending_requests
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(queued_at.len()))
                });
    }

    fn note_raw_batch_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.raw_batch_write_ms_samples,
            &self.last_raw_batch_write_ms_p95,
            duration_ms,
        );
    }

    fn note_discovery_scoring_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.discovery_scoring_ms_samples,
            &self.last_discovery_scoring_ms_p95,
            duration_ms,
        );
    }

    fn note_journal_enqueue_wait_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.journal_enqueue_wait_ms_samples,
            &self.last_journal_enqueue_wait_ms_p95,
            duration_ms,
        );
    }

    fn note_journal_batch_write_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.journal_batch_write_ms_samples,
            &self.last_journal_batch_write_ms_p95,
            duration_ms,
        );
    }

    fn note_observed_swaps_insert_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.observed_swaps_insert_ms_samples,
            &self.last_observed_swaps_insert_ms_p95,
            duration_ms,
        );
    }

    fn note_wallet_activity_days_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.wallet_activity_days_ms_samples,
            &self.last_wallet_activity_days_ms_p95,
            duration_ms,
        );
    }

    fn note_worker_busy_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.worker_busy_ms_samples,
            &self.last_worker_busy_ms_p95,
            duration_ms,
        );
    }

    fn note_aggregate_queue_enqueued(&self) {
        self.aggregate_queue_depth_batches
            .fetch_add(1, Ordering::Relaxed);
    }

    fn note_aggregate_queue_dequeued(&self) {
        let _ = self.aggregate_queue_depth_batches.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(1)),
        );
    }

    fn note_aggregate_overflow_enqueued(&self) {
        self.aggregate_overflow_depth_batches
            .fetch_add(1, Ordering::Relaxed);
    }

    fn note_aggregate_overflow_dequeued(&self) {
        let _ = self.aggregate_overflow_depth_batches.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(1)),
        );
    }

    fn note_journal_queue_enqueued(&self, rows: usize) {
        self.journal_queue_enqueued_batches
            .fetch_add(1, Ordering::Relaxed);
        self.journal_queue_enqueued_rows
            .fetch_add(rows, Ordering::Relaxed);
    }

    fn note_journal_queue_dequeued(&self, rows: usize) {
        self.journal_queue_dequeued_batches
            .fetch_add(1, Ordering::Relaxed);
        self.journal_queue_dequeued_rows
            .fetch_add(rows, Ordering::Relaxed);
    }

    fn note_journal_overflow_enqueued(&self, rows: usize) {
        self.journal_overflow_depth_batches
            .fetch_add(1, Ordering::Relaxed);
        self.journal_overflow_row_debt
            .fetch_add(rows, Ordering::Relaxed);
    }

    fn note_journal_overflow_dequeued(&self, rows: usize) {
        let _ = self.journal_overflow_depth_batches.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(1)),
        );
        let _ = self.journal_overflow_row_debt.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(rows)),
        );
    }

    fn note_journal_overflow_rows_coalesced(&self, rows: usize) {
        self.journal_overflow_row_debt
            .fetch_add(rows, Ordering::Relaxed);
    }

    fn note_journal_writer_inflight_started(&self, rows: usize) {
        self.journal_writer_inflight_rows
            .store(rows, Ordering::Relaxed);
    }

    fn note_journal_writer_inflight_finished(&self) {
        self.journal_writer_inflight_rows
            .store(0, Ordering::Relaxed);
    }

    fn note_journal_sqlite_contention_delta(
        &self,
        before: SqliteContentionSnapshot,
        after: SqliteContentionSnapshot,
    ) {
        self.journal_sqlite_write_retry_total.fetch_add(
            after
                .write_retry_total
                .saturating_sub(before.write_retry_total),
            Ordering::Relaxed,
        );
        self.journal_sqlite_busy_error_total.fetch_add(
            after
                .busy_error_total
                .saturating_sub(before.busy_error_total),
            Ordering::Relaxed,
        );
    }

    fn set_aggregate_gap_active(&self, active: bool) {
        self.aggregate_gap_active.store(active, Ordering::Relaxed);
    }

    fn aggregate_gap_active(&self) -> bool {
        self.aggregate_gap_active.load(Ordering::Relaxed)
    }

    fn set_aggregate_worker_busy(&self, busy: bool) {
        self.aggregate_worker_busy.store(busy, Ordering::Relaxed);
    }

    fn aggregate_worker_busy(&self) -> bool {
        self.aggregate_worker_busy.load(Ordering::Relaxed)
    }
}
