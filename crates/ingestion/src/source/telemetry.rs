use chrono::Utc;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use tracing::info;

use super::core::{percentile, push_sample};
use super::{IngestionRuntimeSnapshot, TELEMETRY_SAMPLE_CAPACITY};

#[derive(Debug)]
pub(super) struct IngestionTelemetry {
    pub(super) ws_notifications_seen: AtomicU64,
    pub(super) ws_notifications_enqueued: AtomicU64,
    pub(super) ws_notifications_backpressured: AtomicU64,
    pub(super) ws_notifications_dropped: AtomicU64,
    pub(super) ws_notifications_replaced_oldest: AtomicU64,
    pub(super) reconnect_count: AtomicU64,
    pub(super) stream_gap_detected: AtomicU64,
    pub(super) parse_rejected_total: AtomicU64,
    pub(super) parse_rejected_by_reason: Mutex<BTreeMap<&'static str, u64>>,
    pub(super) parse_fallback_by_reason: Mutex<BTreeMap<&'static str, u64>>,
    pub(super) grpc_message_total: AtomicU64,
    pub(super) grpc_transaction_updates_total: AtomicU64,
    pub(super) grpc_decode_errors: AtomicU64,
    pub(super) fetch_inflight: AtomicU64,
    pub(super) fetch_success: AtomicU64,
    pub(super) fetch_failed: AtomicU64,
    pub(super) fetch_no_swap: AtomicU64,
    pub(super) fetch_retry_attempts: AtomicU64,
    pub(super) fetch_retry_exhausted: AtomicU64,
    pub(super) fetch_retry_terminal: AtomicU64,
    pub(super) prefetch_stale_dropped: AtomicU64,
    pub(super) rpc_429: AtomicU64,
    pub(super) rpc_5xx: AtomicU64,
    pub(super) last_ingestion_lag_p95: AtomicU64,
    pub(super) fetch_latency_ms_samples: Mutex<VecDeque<u64>>,
    pub(super) ingestion_lag_ms_samples: Mutex<VecDeque<u64>>,
    pub(super) reorder_hold_ms_samples: Mutex<VecDeque<u64>>,
    pub(super) max_reorder_buffer_size: AtomicUsize,
    pub(super) last_report_ms: AtomicI64,
}

impl Default for IngestionTelemetry {
    fn default() -> Self {
        Self {
            ws_notifications_seen: AtomicU64::new(0),
            ws_notifications_enqueued: AtomicU64::new(0),
            ws_notifications_backpressured: AtomicU64::new(0),
            ws_notifications_dropped: AtomicU64::new(0),
            ws_notifications_replaced_oldest: AtomicU64::new(0),
            reconnect_count: AtomicU64::new(0),
            stream_gap_detected: AtomicU64::new(0),
            parse_rejected_total: AtomicU64::new(0),
            parse_rejected_by_reason: Mutex::new(BTreeMap::new()),
            parse_fallback_by_reason: Mutex::new(BTreeMap::new()),
            grpc_message_total: AtomicU64::new(0),
            grpc_transaction_updates_total: AtomicU64::new(0),
            grpc_decode_errors: AtomicU64::new(0),
            fetch_inflight: AtomicU64::new(0),
            fetch_success: AtomicU64::new(0),
            fetch_failed: AtomicU64::new(0),
            fetch_no_swap: AtomicU64::new(0),
            fetch_retry_attempts: AtomicU64::new(0),
            fetch_retry_exhausted: AtomicU64::new(0),
            fetch_retry_terminal: AtomicU64::new(0),
            prefetch_stale_dropped: AtomicU64::new(0),
            rpc_429: AtomicU64::new(0),
            rpc_5xx: AtomicU64::new(0),
            last_ingestion_lag_p95: AtomicU64::new(0),
            fetch_latency_ms_samples: Mutex::new(VecDeque::with_capacity(
                TELEMETRY_SAMPLE_CAPACITY,
            )),
            ingestion_lag_ms_samples: Mutex::new(VecDeque::with_capacity(
                TELEMETRY_SAMPLE_CAPACITY,
            )),
            reorder_hold_ms_samples: Mutex::new(VecDeque::with_capacity(TELEMETRY_SAMPLE_CAPACITY)),
            max_reorder_buffer_size: AtomicUsize::new(0),
            last_report_ms: AtomicI64::new(0),
        }
    }
}

impl IngestionTelemetry {
    pub(super) fn push_fetch_latency(&self, value: u64) {
        if let Ok(mut guard) = self.fetch_latency_ms_samples.lock() {
            push_sample(&mut guard, value, TELEMETRY_SAMPLE_CAPACITY);
        }
    }

    pub(super) fn push_ingestion_lag(&self, value: u64) {
        if let Ok(mut guard) = self.ingestion_lag_ms_samples.lock() {
            push_sample(&mut guard, value, TELEMETRY_SAMPLE_CAPACITY);
        }
    }

    pub(super) fn push_reorder_hold(&self, value: u64) {
        if let Ok(mut guard) = self.reorder_hold_ms_samples.lock() {
            push_sample(&mut guard, value, TELEMETRY_SAMPLE_CAPACITY);
        }
    }

    pub(super) fn note_reorder_buffer_size(&self, size: usize) {
        let _ = self
            .max_reorder_buffer_size
            .fetch_max(size, Ordering::Relaxed);
    }

    pub(super) fn note_parse_rejected(&self, error: &anyhow::Error) {
        self.parse_rejected_total.fetch_add(1, Ordering::Relaxed);
        let reason = classify_parse_reject_reason(error);
        if let Ok(mut guard) = self.parse_rejected_by_reason.lock() {
            let entry = guard.entry(reason).or_insert(0);
            *entry = entry.saturating_add(1);
        }
    }

    pub(super) fn note_parse_fallback(&self, reason: &'static str) {
        if let Ok(mut guard) = self.parse_fallback_by_reason.lock() {
            let entry = guard.entry(reason).or_insert(0);
            *entry = entry.saturating_add(1);
        }
    }

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

    pub(super) fn snapshot(&self) -> IngestionRuntimeSnapshot {
        let ingestion_lag_ms_p95 = match self.ingestion_lag_ms_samples.lock() {
            Ok(values) => percentile(&values.iter().copied().collect::<Vec<_>>(), 0.95),
            Err(_) => self.last_ingestion_lag_p95.load(Ordering::Relaxed),
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
        }
    }
}

pub(super) fn classify_parse_reject_reason(error: &anyhow::Error) -> &'static str {
    let lowered = error.to_string().to_ascii_lowercase();
    if lowered.contains("missing slot") {
        return "missing_slot";
    }
    if lowered.contains("missing status") {
        return "missing_status";
    }
    if lowered.contains("missing signer") {
        return "missing_signer";
    }
    if lowered.contains("missing program ids") {
        return "missing_program_ids";
    }
    if lowered.contains("missing transaction signature") {
        return "missing_signature";
    }
    if lowered.contains("timestamp") {
        return "invalid_timestamp";
    }
    if lowered.contains("balance") {
        return "invalid_balance_inference";
    }
    if lowered.contains("account key") {
        return "invalid_account_keys";
    }
    "other"
}
