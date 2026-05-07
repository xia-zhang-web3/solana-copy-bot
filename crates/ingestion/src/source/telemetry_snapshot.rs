use super::*;

impl IngestionTelemetry {
    pub(in crate::source) fn snapshot(&self) -> IngestionRuntimeSnapshot {
        let ingestion_lag_ms_p95 = match self.ingestion_lag_ms_samples.lock() {
            Ok(values) => percentile(&values.iter().copied().collect::<Vec<_>>(), 0.95),
            Err(_) => self.last_ingestion_lag_p95.load(Ordering::Relaxed),
        };
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
        IngestionRuntimeSnapshot {
            ts_utc: Utc::now(),
            ws_notifications_enqueued: self.ws_notifications_enqueued.load(Ordering::Relaxed),
            ws_notifications_replaced_oldest: self
                .ws_notifications_replaced_oldest
                .load(Ordering::Relaxed),
            grpc_message_total: self.grpc_message_total.load(Ordering::Relaxed),
            grpc_transaction_updates_total: self
                .grpc_transaction_updates_total
                .load(Ordering::Relaxed),
            parse_rejected_total: self.parse_rejected_total.load(Ordering::Relaxed),
            grpc_decode_errors: self.grpc_decode_errors.load(Ordering::Relaxed),
            rpc_429: self.rpc_429.load(Ordering::Relaxed),
            rpc_5xx: self.rpc_5xx.load(Ordering::Relaxed),
            ingestion_lag_ms_p95,
            yellowstone_output_queue_depth,
            yellowstone_output_queue_capacity,
            yellowstone_output_queue_fill_ratio,
            yellowstone_output_oldest_age_ms,
        }
    }
}
