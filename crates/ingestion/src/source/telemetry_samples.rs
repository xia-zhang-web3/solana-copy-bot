use super::*;

impl IngestionTelemetry {
    pub(in crate::source) fn push_fetch_latency(&self, value: u64) {
        if let Ok(mut guard) = self.fetch_latency_ms_samples.lock() {
            push_sample(&mut guard, value, TELEMETRY_SAMPLE_CAPACITY);
        }
    }

    pub(in crate::source) fn push_ingestion_lag(&self, value: u64) {
        if let Ok(mut guard) = self.ingestion_lag_ms_samples.lock() {
            push_sample(&mut guard, value, TELEMETRY_SAMPLE_CAPACITY);
        }
    }

    pub(in crate::source) fn push_reorder_hold(&self, value: u64) {
        if let Ok(mut guard) = self.reorder_hold_ms_samples.lock() {
            push_sample(&mut guard, value, TELEMETRY_SAMPLE_CAPACITY);
        }
    }

    pub(in crate::source) fn note_reorder_buffer_size(&self, size: usize) {
        let _ = self
            .max_reorder_buffer_size
            .fetch_max(size, Ordering::Relaxed);
    }

    pub(in crate::source) fn note_yellowstone_output_queue_metrics(
        &self,
        depth: usize,
        capacity: usize,
        oldest_age_ms: u64,
    ) {
        self.last_yellowstone_output_queue_depth
            .store(depth as u64, Ordering::Relaxed);
        self.last_yellowstone_output_queue_capacity
            .store(capacity as u64, Ordering::Relaxed);
        self.last_yellowstone_output_oldest_age_ms
            .store(oldest_age_ms, Ordering::Relaxed);
    }

    pub(in crate::source) fn note_parse_rejected(&self, error: &anyhow::Error) {
        self.parse_rejected_total.fetch_add(1, Ordering::Relaxed);
        let reason = classify_parse_reject_reason(error);
        if let Ok(mut guard) = self.parse_rejected_by_reason.lock() {
            let entry = guard.entry(reason).or_insert(0);
            *entry = entry.saturating_add(1);
        }
    }

    pub(in crate::source) fn note_parse_fallback(&self, reason: &'static str) {
        if let Ok(mut guard) = self.parse_fallback_by_reason.lock() {
            let entry = guard.entry(reason).or_insert(0);
            *entry = entry.saturating_add(1);
        }
    }
}
