impl ObservedSwapWriterTelemetry {
    fn note_phase_sample(
        &self,
        samples_lock: &Mutex<VecDeque<u64>>,
        last_p95: &AtomicU64,
        duration_ms: u64,
    ) {
        if let Ok(mut samples) = samples_lock.lock() {
            if samples.len() >= OBSERVED_SWAP_WRITER_LATENCY_SAMPLE_CAPACITY {
                let _ = samples.pop_front();
            }
            samples.push_back(duration_ms);
            last_p95.store(percentile_from_deque(&samples, 0.95), Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> ObservedSwapWriterSnapshot {
        let write_latency_ms_p95 = self
            .write_latency_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| self.last_write_latency_ms_p95.load(Ordering::Relaxed));
        let raw_batch_write_ms_p95 = self
            .raw_batch_write_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| self.last_raw_batch_write_ms_p95.load(Ordering::Relaxed));
        let observed_swaps_insert_ms_p95 = self
            .observed_swaps_insert_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| {
                self.last_observed_swaps_insert_ms_p95
                    .load(Ordering::Relaxed)
            });
        let wallet_activity_days_ms_p95 = self
            .wallet_activity_days_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| {
                self.last_wallet_activity_days_ms_p95
                    .load(Ordering::Relaxed)
            });
        let journal_enqueue_wait_ms_p95 = self
            .journal_enqueue_wait_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| {
                self.last_journal_enqueue_wait_ms_p95
                    .load(Ordering::Relaxed)
            });
        let journal_batch_write_ms_p95 = self
            .journal_batch_write_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| self.last_journal_batch_write_ms_p95.load(Ordering::Relaxed));
        let worker_busy_ms_p95 = self
            .worker_busy_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| self.last_worker_busy_ms_p95.load(Ordering::Relaxed));
        ObservedSwapWriterSnapshot {
            pending_requests: self.pending_requests.load(Ordering::Relaxed),
            write_latency_ms_p95,
            raw_batch_write_ms_p95,
            observed_swaps_insert_ms_p95,
            wallet_activity_days_ms_p95,
            journal_enqueue_wait_ms_p95,
            journal_batch_write_ms_p95,
            worker_busy_ms_p95,
            journal_queue_depth_batches: self.journal_queue_depth_batches(),
            journal_queue_row_debt: self.journal_queue_row_debt(),
            journal_queue_capacity_batches: self
                .journal_queue_capacity_batches
                .load(Ordering::Relaxed),
            journal_overflow_depth_batches: self
                .journal_overflow_depth_batches
                .load(Ordering::Relaxed),
            journal_overflow_row_debt: self.journal_overflow_row_debt.load(Ordering::Relaxed),
            journal_overflow_capacity_batches: self
                .journal_overflow_capacity_batches
                .load(Ordering::Relaxed),
            journal_overflow_row_debt_capacity: self
                .journal_overflow_row_debt_capacity
                .load(Ordering::Relaxed),
            journal_writer_inflight_rows: self.journal_writer_inflight_rows.load(Ordering::Relaxed),
            journal_sqlite_write_retry_total: self
                .journal_sqlite_write_retry_total
                .load(Ordering::Relaxed),
            journal_sqlite_busy_error_total: self
                .journal_sqlite_busy_error_total
                .load(Ordering::Relaxed),
        }
    }

    fn journal_queue_depth_batches(&self) -> usize {
        let enqueued = self.journal_queue_enqueued_batches.load(Ordering::Relaxed);
        let dequeued = self.journal_queue_dequeued_batches.load(Ordering::Relaxed);
        let depth = enqueued.saturating_sub(dequeued);
        let capacity = self.journal_queue_capacity_batches.load(Ordering::Relaxed);
        if capacity == 0 {
            depth
        } else {
            depth.min(capacity)
        }
    }

    fn journal_queue_row_debt(&self) -> usize {
        self.journal_queue_enqueued_rows
            .load(Ordering::Relaxed)
            .saturating_sub(self.journal_queue_dequeued_rows.load(Ordering::Relaxed))
    }
}
