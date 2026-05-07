use std::collections::VecDeque;
use std::time::Instant as StdInstant;

use crate::APP_CONSUMER_LOOP_LATENCY_SAMPLE_CAPACITY;

#[derive(Debug, Default)]
pub(crate) struct AppConsumerLoopTelemetry {
    swaps_seen: u64,
    follow_rejected: u64,
    processing_ms_samples: VecDeque<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct AppConsumerLoopTelemetrySnapshot {
    pub(crate) swaps_seen: u64,
    pub(crate) follow_rejected: u64,
    pub(crate) follow_rejected_ratio: f64,
    pub(crate) processing_ms_p95: u64,
}

impl AppConsumerLoopTelemetry {
    pub(crate) fn note_swap_seen(&mut self) {
        self.swaps_seen = self.swaps_seen.saturating_add(1);
    }

    pub(crate) fn note_follow_rejected(&mut self) {
        self.follow_rejected = self.follow_rejected.saturating_add(1);
    }

    pub(crate) fn note_processing_duration(&mut self, duration_ms: u64) {
        if self.processing_ms_samples.len() >= APP_CONSUMER_LOOP_LATENCY_SAMPLE_CAPACITY {
            let _ = self.processing_ms_samples.pop_front();
        }
        self.processing_ms_samples.push_back(duration_ms);
    }

    pub(crate) fn note_processing_started_at(&mut self, started_at: StdInstant) {
        let duration_ms = started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        self.note_processing_duration(duration_ms);
    }

    pub(crate) fn snapshot_and_reset(&mut self) -> AppConsumerLoopTelemetrySnapshot {
        let swaps_seen = self.swaps_seen;
        let follow_rejected = self.follow_rejected;
        let processing_ms_p95 = percentile_u64_deque(&self.processing_ms_samples, 0.95);
        let follow_rejected_ratio = if swaps_seen == 0 {
            0.0
        } else {
            follow_rejected as f64 / swaps_seen as f64
        };
        self.swaps_seen = 0;
        self.follow_rejected = 0;
        self.processing_ms_samples.clear();
        AppConsumerLoopTelemetrySnapshot {
            swaps_seen,
            follow_rejected,
            follow_rejected_ratio,
            processing_ms_p95,
        }
    }
}

fn percentile_u64_deque(values: &VecDeque<u64>, q: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    let idx = ((sorted.len() - 1) as f64 * q.clamp(0.0, 1.0)).round() as usize;
    sorted[idx]
}
