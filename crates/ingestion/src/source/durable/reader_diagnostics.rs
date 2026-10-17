//! Measurement only. Reader records never wait for locks, exporter or filesystem.
use super::{
    continuity_telemetry::{diagnostic_time, origin_unix_ms, ReaderIdentity},
    diagnostic_clock::ClockStamp,
    reader_error_witness::ErrorWitness,
};
use serde::Serialize;
use std::sync::{
    atomic::{AtomicU64, Ordering::Relaxed},
    Arc, Mutex, OnceLock,
};
use tokio::sync::mpsc;
const CAP: usize = 32;
#[derive(Debug, Clone, Serialize)]
pub struct ReaderDiagnosticEvent {
    pub schema: &'static str,
    pub sequence: u64,
    pub origin_unix_ms: u64,
    pub observed_ns: u64,
    pub clock: ClockStamp,
    pub reader: Option<ReaderIdentity>,
    pub identity_availability: &'static str,
    pub kind: &'static str,
    pub outcome: &'static str,
    pub error: Option<ErrorWitness>,
    pub task_finished_at_abort_request: Option<bool>,
}
#[derive(Debug, Clone, Serialize)]
pub struct ReaderDiagnosticSnapshot {
    pub schema: &'static str,
    pub origin_unix_ms: u64,
    pub sampled_ns: u64,
    pub sampled_clock: ClockStamp,
    pub attempted: u64,
    pub retained_total: u64,
    pub retention_lost: u64,
    pub observer_panics: u64,
    pub export_enqueued: u64,
    pub export_completed: u64,
    pub export_failed: u64,
    pub export_queue_dropped: u64,
    pub exporter_dropped: u64,
    pub export_pending: u64,
    pub journal_available: bool,
    pub events: Vec<ReaderDiagnosticEvent>,
}
#[derive(Default)]
pub(crate) struct ReaderDiagnostics {
    pub(super) events: Mutex<Vec<ReaderDiagnosticEvent>>,
    sender: OnceLock<mpsc::Sender<ReaderDiagnosticEvent>>,
    attempted: AtomicU64,
    retained: AtomicU64,
    lost: AtomicU64,
    panics: AtomicU64,
    enqueued: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    queue_dropped: AtomicU64,
    exporter_dropped: AtomicU64,
}
struct ExportLifetime(Arc<ReaderDiagnostics>);
impl Drop for ExportLifetime {
    fn drop(&mut self) {
        self.0.exporter_dropped.fetch_add(1, Relaxed);
    }
}
impl ReaderDiagnostics {
    pub(super) fn exporter(self: &Arc<Self>) {
        self.sender.get_or_init(|| {
            let (tx, mut rx) = mpsc::channel::<ReaderDiagnosticEvent>(CAP);
            let journal = self.clone();
            tokio::spawn(async move {
                let _lifetime = ExportLifetime(journal.clone());
                while let Some(event) = rx.recv().await {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let encoded = serde_json::to_string(&event)?;
                        tracing::info!(reader_terminal_json = %encoded, "durable reader diagnostic event");
                        Ok::<(), serde_json::Error>(())
                    }));
                    let mut ok = matches!(result, Ok(Ok(())));
                    #[cfg(test)]
                    if super::measurement_export::enabled() {
                        let encoded=serde_json::to_string(&event);
                        ok = match encoded {
                            Ok(encoded) => tokio::task::spawn_blocking(move || super::measurement_export::export(&encoded)).await.unwrap_or(false) && ok,
                            Err(_) => false,
                        };
                    }
                    journal.export_result(ok);
                }
            });
            tx
        });
    }
    pub(super) fn export_result(&self, ok: bool) {
        if ok {
            self.completed.fetch_add(1, Relaxed);
        } else {
            self.failed.fetch_add(1, Relaxed);
        }
    }
    pub(super) fn observer_failed(&self) {
        self.panics.fetch_add(1, Relaxed);
    }
    pub(super) fn record(&self, mut event: ReaderDiagnosticEvent) {
        event.sequence = self.attempted.fetch_add(1, Relaxed) + 1;
        match self.events.try_lock() {
            Ok(mut events) if events.len() < CAP => {
                events.push(event.clone());
                self.retained.fetch_add(1, Relaxed);
            }
            _ => {
                self.lost.fetch_add(1, Relaxed);
            }
        }
        if self
            .sender
            .get()
            .is_some_and(|tx| tx.try_send(event).is_ok())
        {
            self.enqueued.fetch_add(1, Relaxed);
        } else {
            self.queue_dropped.fetch_add(1, Relaxed);
        }
    }
    pub(crate) fn snapshot(&self) -> ReaderDiagnosticSnapshot {
        let events = self.events.try_lock().ok().map(|e| e.clone());
        let enqueued = self.enqueued.load(Relaxed);
        let completed = self.completed.load(Relaxed);
        let failed = self.failed.load(Relaxed);
        let (sampled_ns, sampled_clock) = diagnostic_time();
        ReaderDiagnosticSnapshot {
            schema: "reader_diagnostic_snapshot_v1",
            origin_unix_ms: origin_unix_ms(),
            sampled_ns,
            sampled_clock,
            attempted: self.attempted.load(Relaxed),
            retained_total: self.retained.load(Relaxed),
            retention_lost: self.lost.load(Relaxed),
            observer_panics: self.panics.load(Relaxed),
            export_enqueued: enqueued,
            export_completed: completed,
            export_failed: failed,
            export_queue_dropped: self.queue_dropped.load(Relaxed),
            exporter_dropped: self.exporter_dropped.load(Relaxed),
            export_pending: enqueued.saturating_sub(completed.saturating_add(failed)),
            journal_available: events.is_some(),
            events: events.unwrap_or_default(),
        }
    }
}
pub(super) fn event(
    reader: Option<ReaderIdentity>,
    kind: &'static str,
    outcome: &'static str,
) -> ReaderDiagnosticEvent {
    let (observed_ns, clock) = diagnostic_time();
    ReaderDiagnosticEvent {
        schema: "reader_diagnostic_event_v1",
        sequence: 0,
        origin_unix_ms: origin_unix_ms(),
        observed_ns,
        clock,
        identity_availability: if reader.is_some() {
            "PRESENT"
        } else {
            "UNAVAILABLE_SCOPE_NOT_OBSERVED"
        },
        reader,
        kind,
        outcome,
        error: None,
        task_finished_at_abort_request: None,
    }
}
