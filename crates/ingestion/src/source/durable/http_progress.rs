//! Bounded recovery observation. Values do not authorize admissions or execution.
use super::DurableIngressTelemetry;
use std::time::{Duration, Instant};
pub(super) fn refused(error: &anyhow::Error, secrets: &[&str]) {
    let details =
        crate::source::durable::transport_diagnostics::ErrorDetails::error(error, secrets);
    tracing::warn!(class = ?details.class, error_message = %details.message,
        causes = ?details.causes, "confirmed HTTP recovery refused");
}
pub(super) struct Progress {
    from: u64,
    anchor: u64,
    began: Instant,
    last_report: Option<Instant>,
}
impl Progress {
    pub fn new(from: u64, anchor: u64) -> Self {
        Self {
            from,
            anchor,
            began: Instant::now(),
            last_report: None,
        }
    }
    pub fn note(&mut self, telemetry: &DurableIngressTelemetry, recovered: u64, completed: bool) {
        let s = telemetry.snapshot();
        telemetry.processing.http_progress(
            self.from,
            self.anchor,
            recovered,
            s.last_received_block_slot,
            completed,
        );
        if !completed
            && self
                .last_report
                .is_some_and(|at| at.elapsed() < Duration::from_secs(5))
        {
            return;
        }
        self.last_report = Some(Instant::now());
        let memory = &s.processing;
        tracing::info!(
            from_slot = self.from,
            live_anchor_slot = self.anchor,
            recovered_slot = recovered,
            recovery_completed = completed,
            live_received_slot = s.last_received_block_slot,
            durable_slot = s.last_durably_stored_parent_slot,
            live_backlog_slots = s
                .last_received_block_slot
                .saturating_sub(s.last_durably_stored_parent_slot),
            catchup_age_ms = self.began.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            input_queue_count = memory.input_queue_count,
            input_queue_bytes = memory.input_queue_bytes,
            block_cache_count = memory.block_cache_count,
            block_cache_encoded_bytes = memory.block_cache_encoded_bytes,
            "confirmed HTTP recovery progress"
        );
    }
}
