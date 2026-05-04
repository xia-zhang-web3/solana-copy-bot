impl IngestionTelemetry {
    pub(super) fn maybe_report(
        &self,
        report_seconds: u64,
        ws_to_fetch_queue_depth: usize,
        fetch_to_output_queue_depth: usize,
        reorder_buffer_size: usize,
    ) {
        let report_seconds = report_seconds.max(5);
        let now_ms = Utc::now().timestamp_millis();
        let last = self.last_report_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < (report_seconds as i64 * 1_000) {
            return;
        }
        if self
            .last_report_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let fetch_samples = self
            .fetch_latency_ms_samples
            .lock()
            .ok()
            .map(|values| values.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        let lag_samples = self
            .ingestion_lag_ms_samples
            .lock()
            .ok()
            .map(|values| values.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        let hold_samples = self
            .reorder_hold_ms_samples
            .lock()
            .ok()
            .map(|values| values.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        let parse_rejected_by_reason = self
            .parse_rejected_by_reason
            .lock()
            .ok()
            .map(|values| values.clone())
            .unwrap_or_default();
        let parse_fallback_by_reason = self
            .parse_fallback_by_reason
            .lock()
            .ok()
            .map(|values| values.clone())
            .unwrap_or_default();
        let ingestion_lag_ms_p95 = percentile(&lag_samples, 0.95);
        self.last_ingestion_lag_p95
            .store(ingestion_lag_ms_p95, Ordering::Relaxed);
        let yellowstone_output_queue_depth = self
            .last_yellowstone_output_queue_depth
            .load(Ordering::Relaxed);
        let yellowstone_output_queue_capacity = self
            .last_yellowstone_output_queue_capacity
            .load(Ordering::Relaxed);
        let yellowstone_output_oldest_age_ms = self
            .last_yellowstone_output_oldest_age_ms
            .load(Ordering::Relaxed);
        let yellowstone_output_queue_fill_ratio = if yellowstone_output_queue_capacity == 0 {
            0.0
        } else {
            yellowstone_output_queue_depth as f64 / yellowstone_output_queue_capacity as f64
        };

        info!(
            ws_notifications_seen = self.ws_notifications_seen.load(Ordering::Relaxed),
            ws_notifications_enqueued = self.ws_notifications_enqueued.load(Ordering::Relaxed),
            ws_notifications_backpressured =
                self.ws_notifications_backpressured.load(Ordering::Relaxed),
            ws_notifications_dropped = self.ws_notifications_dropped.load(Ordering::Relaxed),
            ws_notifications_replaced_oldest = self
                .ws_notifications_replaced_oldest
                .load(Ordering::Relaxed),
            reconnect_count = self.reconnect_count.load(Ordering::Relaxed),
            stream_gap_detected = self.stream_gap_detected.load(Ordering::Relaxed),
            parse_rejected_total = self.parse_rejected_total.load(Ordering::Relaxed),
            parse_rejected_by_reason = ?parse_rejected_by_reason,
            parse_fallback_by_reason = ?parse_fallback_by_reason,
            grpc_message_total = self.grpc_message_total.load(Ordering::Relaxed),
            grpc_transaction_updates_total = self.grpc_transaction_updates_total.load(Ordering::Relaxed),
            grpc_decode_errors = self.grpc_decode_errors.load(Ordering::Relaxed),
            ws_to_fetch_queue_depth,
            fetch_to_output_queue_depth,
            yellowstone_output_queue_depth,
            yellowstone_output_queue_capacity,
            yellowstone_output_queue_fill_ratio,
            yellowstone_output_oldest_age_ms,
            fetch_concurrency_inflight = self.fetch_inflight.load(Ordering::Relaxed),
            fetch_success = self.fetch_success.load(Ordering::Relaxed),
            fetch_failed = self.fetch_failed.load(Ordering::Relaxed),
            fetch_no_swap = self.fetch_no_swap.load(Ordering::Relaxed),
            fetch_retry_attempts = self.fetch_retry_attempts.load(Ordering::Relaxed),
            fetch_retry_exhausted = self.fetch_retry_exhausted.load(Ordering::Relaxed),
            fetch_retry_terminal = self.fetch_retry_terminal.load(Ordering::Relaxed),
            prefetch_stale_dropped = self.prefetch_stale_dropped.load(Ordering::Relaxed),
            rpc_429 = self.rpc_429.load(Ordering::Relaxed),
            rpc_5xx = self.rpc_5xx.load(Ordering::Relaxed),
            fetch_latency_ms_p50 = percentile(&fetch_samples, 0.50),
            fetch_latency_ms_p95 = percentile(&fetch_samples, 0.95),
            fetch_latency_ms_p99 = percentile(&fetch_samples, 0.99),
            ingestion_lag_ms_p50 = percentile(&lag_samples, 0.50),
            ingestion_lag_ms_p95,
            ingestion_lag_ms_p99 = percentile(&lag_samples, 0.99),
            reorder_hold_ms_p95 = percentile(&hold_samples, 0.95),
            reorder_buffer_size,
            reorder_buffer_max = self.max_reorder_buffer_size.load(Ordering::Relaxed),
            "ingestion pipeline metrics"
        );
    }
}
